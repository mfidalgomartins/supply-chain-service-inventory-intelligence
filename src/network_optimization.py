"""Deterministic multi-echelon inventory and sourcing optimization."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix

from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
from src.settings import NetworkOptimizationSettings, load_settings

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


class NetworkOptimizationError(RuntimeError):
    """Raised when the constrained network has no publishable optimum."""


@dataclass(frozen=True)
class _Variables:
    flow: dict[tuple[str, str], int]
    ending: dict[tuple[str, str], int]
    shortage: dict[tuple[str, str], int]
    order: dict[tuple[str, str], int]
    size: int


class _ConstraintBuilder:
    def __init__(self, variable_count: int) -> None:
        self.variable_count = variable_count
        self.row_indices: list[int] = []
        self.column_indices: list[int] = []
        self.values: list[float] = []
        self.lower: list[float] = []
        self.upper: list[float] = []

    def add(self, coefficients: dict[int, float], lower: float, upper: float) -> None:
        row = len(self.lower)
        for column, value in coefficients.items():
            if value:
                self.row_indices.append(row)
                self.column_indices.append(column)
                self.values.append(value)
        self.lower.append(lower)
        self.upper.append(upper)

    def linear_constraint(self) -> LinearConstraint:
        matrix = coo_matrix(
            (self.values, (self.row_indices, self.column_indices)),
            shape=(len(self.lower), self.variable_count),
        ).tocsr()
        return LinearConstraint(matrix, np.asarray(self.lower), np.asarray(self.upper))


def _required_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} fields missing: {missing}")


def _validate_inputs(
    nodes: pd.DataFrame,
    lanes: pd.DataFrame,
    sources: pd.DataFrame,
    requirements: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _required_columns(
        nodes,
        {"node_id", "node_type", "storage_capacity_units"},
        "network_nodes",
    )
    _required_columns(
        lanes,
        {
            "lane_id",
            "source_node_id",
            "destination_node_id",
            "lane_type",
            "lead_time_days",
            "unit_transport_cost",
            "daily_capacity_units",
            "enabled",
        },
        "network_lanes",
    )
    _required_columns(
        sources,
        {
            "product_id",
            "supplier_id",
            "is_primary",
            "unit_purchase_cost",
            "minimum_order_qty",
            "max_horizon_units",
            "source_lead_time_days",
            "enabled",
        },
        "product_sources",
    )
    _required_columns(
        requirements,
        {
            "product_id",
            "warehouse_id",
            "demand_units",
            "starting_inventory_units",
            "target_service_level",
            "unit_cost",
            "lost_margin_per_unit",
        },
        "network_requirements",
    )
    if nodes["node_id"].duplicated().any():
        raise ValueError("network_nodes.node_id must be unique")
    if lanes["lane_id"].duplicated().any():
        raise ValueError("network_lanes.lane_id must be unique")
    if sources.duplicated(["product_id", "supplier_id"]).any():
        raise ValueError("product_sources must be unique by product_id and supplier_id")
    if requirements.duplicated(["product_id", "warehouse_id"]).any():
        raise ValueError("Network requirements must be unique by product_id and warehouse_id")
    if not set(nodes["node_type"]).issubset({"supplier", "gateway", "regional_dc"}):
        raise ValueError("network_nodes contains an invalid node_type")
    node_ids = set(nodes["node_id"])
    referenced_nodes = set(lanes["source_node_id"]) | set(lanes["destination_node_id"])
    if referenced_nodes - node_ids:
        raise ValueError("network_lanes reference unknown nodes")
    supplier_ids = set(nodes.loc[nodes["node_type"] == "supplier", "node_id"])
    warehouse_ids = set(nodes.loc[nodes["node_type"] != "supplier", "node_id"])
    if set(sources["supplier_id"]) - supplier_ids:
        raise ValueError("product_sources reference non-supplier nodes")
    if set(requirements["warehouse_id"]) - warehouse_ids:
        raise ValueError("Network requirements reference non-warehouse nodes")

    numeric_non_negative = [
        (nodes, ["storage_capacity_units"]),
        (lanes, ["unit_transport_cost", "daily_capacity_units"]),
        (
            sources,
            ["unit_purchase_cost", "minimum_order_qty", "max_horizon_units"],
        ),
        (
            requirements,
            [
                "demand_units",
                "starting_inventory_units",
                "unit_cost",
                "lost_margin_per_unit",
            ],
        ),
    ]
    for frame, columns in numeric_non_negative:
        values = frame[columns].to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError(f"Network inputs must be finite and non-negative: {columns}")
    if (lanes["lead_time_days"] <= 0).any() or (lanes["daily_capacity_units"] <= 0).any():
        raise ValueError("Network lane lead times and capacities must be positive")
    if (
        (sources["minimum_order_qty"] <= 0).any()
        or (sources["source_lead_time_days"] <= 0).any()
        or (sources["max_horizon_units"] < sources["minimum_order_qty"]).any()
    ):
        raise ValueError("Product source MOQ, capacity, or lead time is invalid")
    if not set(lanes["enabled"]).issubset({0, 1}) or not set(sources["enabled"]).issubset({0, 1}):
        raise ValueError("Network enabled flags must contain only 0 and 1")
    if not set(sources["is_primary"]).issubset({0, 1}):
        raise ValueError("Product source is_primary must contain only 0 and 1")
    primary_counts = sources[sources["enabled"].eq(1)].groupby("product_id")["is_primary"].sum()
    if not primary_counts.eq(1).all():
        raise ValueError("Every product requires exactly one enabled primary source")
    if not requirements["target_service_level"].between(0, 1).all():
        raise ValueError("target_service_level must be in [0, 1]")
    products = set(requirements["product_id"])
    expected_pairs = {(product, warehouse) for product in products for warehouse in warehouse_ids}
    observed_pairs = set(zip(requirements["product_id"], requirements["warehouse_id"], strict=True))
    if observed_pairs != expected_pairs:
        raise ValueError("Requirements must cover every product and warehouse node")
    active_lanes = lanes[lanes["enabled"].eq(1)].copy()
    active_sources = sources[sources["enabled"].eq(1)].copy()
    if set(products) - set(active_sources["product_id"]):
        raise ValueError("Every required product needs an enabled source")
    inbound_suppliers = set(
        active_lanes.loc[active_lanes["lane_type"] == "inbound", "source_node_id"]
    )
    if set(active_sources["supplier_id"]) - inbound_suppliers:
        raise ValueError("Every enabled product source needs an enabled inbound lane")
    return active_lanes, active_sources


def _variables(
    products: list[str],
    lanes: pd.DataFrame,
    sources: pd.DataFrame,
    warehouses: list[str],
) -> _Variables:
    cursor = 0
    flow: dict[tuple[str, str], int] = {}
    eligible_sources = set(zip(sources["product_id"], sources["supplier_id"], strict=True))
    for product_id in products:
        for lane in lanes.sort_values("lane_id").itertuples(index=False):
            if (
                lane.lane_type == "inbound"
                and (
                    product_id,
                    lane.source_node_id,
                )
                not in eligible_sources
            ):
                continue
            flow[(product_id, lane.lane_id)] = cursor
            cursor += 1
    ending = {}
    shortage = {}
    for product_id in products:
        for warehouse_id in warehouses:
            ending[(product_id, warehouse_id)] = cursor
            cursor += 1
            shortage[(product_id, warehouse_id)] = cursor
            cursor += 1
    order = {}
    for source in sources.sort_values(["product_id", "supplier_id"]).itertuples(index=False):
        order[(source.product_id, source.supplier_id)] = cursor
        cursor += 1
    return _Variables(flow=flow, ending=ending, shortage=shortage, order=order, size=cursor)


def solve_network_plan(
    nodes: pd.DataFrame,
    lanes: pd.DataFrame,
    sources: pd.DataFrame,
    requirements: pd.DataFrame,
    settings: NetworkOptimizationSettings,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Solve and independently reconcile a constrained multi-commodity network."""
    active_lanes, active_sources = _validate_inputs(nodes, lanes, sources, requirements)
    active_lanes = active_lanes[active_lanes["lead_time_days"] <= settings.horizon_days].copy()
    if active_lanes.empty:
        raise NetworkOptimizationError("Network optimization is infeasible: no usable lanes")
    products = sorted(requirements["product_id"].astype(str).unique())
    warehouses = sorted(nodes.loc[nodes["node_type"] != "supplier", "node_id"].astype(str))
    variables = _variables(products, active_lanes, active_sources, warehouses)
    objective = np.zeros(variables.size, dtype=float)
    lower_bounds = np.zeros(variables.size, dtype=float)
    upper_bounds = np.full(variables.size, np.inf, dtype=float)
    integrality = np.ones(variables.size, dtype=int)
    source_lookup = active_sources.set_index(["product_id", "supplier_id"])
    lane_lookup = active_lanes.set_index("lane_id")
    requirement_lookup = requirements.set_index(["product_id", "warehouse_id"])

    for (product_id, lane_id), index in variables.flow.items():
        lane = lane_lookup.loc[lane_id]
        objective[index] = float(lane["unit_transport_cost"])
        if lane["lane_type"] == "inbound":
            objective[index] += float(
                source_lookup.loc[(product_id, lane["source_node_id"]), "unit_purchase_cost"]
            )
    holding_factor = settings.annual_holding_cost_rate * settings.horizon_days / 365
    for key, index in variables.ending.items():
        objective[index] = float(requirement_lookup.loc[key, "unit_cost"]) * holding_factor
    for key, index in variables.shortage.items():
        objective[index] = (
            float(requirement_lookup.loc[key, "lost_margin_per_unit"])
            * settings.shortage_penalty_multiplier
        )
    for index in variables.order.values():
        objective[index] = settings.ordering_cost
        upper_bounds[index] = 1

    constraints = _ConstraintBuilder(variables.size)
    for product_id in products:
        for warehouse_id in warehouses:
            requirement = requirement_lookup.loc[(product_id, warehouse_id)]
            coefficients = {
                variables.ending[(product_id, warehouse_id)]: -1.0,
                variables.shortage[(product_id, warehouse_id)]: 1.0,
            }
            for lane in active_lanes.itertuples(index=False):
                flow_index = variables.flow.get((product_id, lane.lane_id))
                if flow_index is None:
                    continue
                if lane.destination_node_id == warehouse_id:
                    coefficients[flow_index] = coefficients.get(flow_index, 0.0) + 1.0
                if lane.source_node_id == warehouse_id:
                    coefficients[flow_index] = coefficients.get(flow_index, 0.0) - 1.0
            right_hand_side = float(
                requirement["demand_units"] - requirement["starting_inventory_units"]
            )
            constraints.add(coefficients, right_hand_side, right_hand_side)
            allowed_shortage = np.floor(
                (1.0 - float(requirement["target_service_level"]))
                * float(requirement["demand_units"])
                + 1e-9
            )
            constraints.add(
                {variables.shortage[(product_id, warehouse_id)]: 1.0},
                0.0,
                float(allowed_shortage),
            )

    for source in active_sources.itertuples(index=False):
        flow_indices = [
            variables.flow[(source.product_id, lane.lane_id)]
            for lane in active_lanes.itertuples(index=False)
            if lane.lane_type == "inbound"
            and lane.source_node_id == source.supplier_id
            and (source.product_id, lane.lane_id) in variables.flow
        ]
        order_index = variables.order[(source.product_id, source.supplier_id)]
        capacity_coefficients = dict.fromkeys(flow_indices, 1.0)
        capacity_coefficients[order_index] = -float(source.max_horizon_units)
        constraints.add(capacity_coefficients, -np.inf, 0.0)
        moq_coefficients = dict.fromkeys(flow_indices, 1.0)
        moq_coefficients[order_index] = -float(source.minimum_order_qty)
        constraints.add(moq_coefficients, 0.0, np.inf)

    for lane in active_lanes.itertuples(index=False):
        coefficients = {
            index: 1.0
            for (product_id, lane_id), index in variables.flow.items()
            if lane_id == lane.lane_id
        }
        constraints.add(
            coefficients,
            0.0,
            float(lane.daily_capacity_units * settings.horizon_days),
        )
    for warehouse_id in warehouses:
        capacity = float(
            nodes.loc[nodes["node_id"] == warehouse_id, "storage_capacity_units"].item()
        )
        constraints.add(
            {variables.ending[(product_id, warehouse_id)]: 1.0 for product_id in products},
            0.0,
            capacity,
        )

    result = milp(
        objective,
        integrality=integrality,
        bounds=Bounds(lower_bounds, upper_bounds),
        constraints=constraints.linear_constraint(),
        options={
            "disp": False,
            "time_limit": float(settings.time_limit_seconds),
            "mip_rel_gap": float(settings.mip_relative_gap),
        },
    )
    if not result.success or result.x is None:
        status = {1: "limit reached", 2: "infeasible", 3: "unbounded"}.get(
            result.status, "solver error"
        )
        raise NetworkOptimizationError(f"Network optimization is {status}: {result.message}")
    rounded = np.rint(result.x)
    if float(np.max(np.abs(result.x - rounded))) > 1e-5:
        raise NetworkOptimizationError("Network solver returned a non-integral solution")
    solution = rounded.astype(np.int64)

    flow_rows: list[dict] = []
    for (product_id, lane_id), index in sorted(variables.flow.items()):
        flow_units = int(solution[index])
        if flow_units == 0:
            continue
        lane = lane_lookup.loc[lane_id]
        purchase_cost = 0.0
        if lane["lane_type"] == "inbound":
            purchase_cost = float(
                source_lookup.loc[(product_id, lane["source_node_id"]), "unit_purchase_cost"]
            )
        transport_cost = float(lane["unit_transport_cost"])
        flow_rows.append(
            {
                "product_id": product_id,
                "lane_id": lane_id,
                "source_node_id": lane["source_node_id"],
                "destination_node_id": lane["destination_node_id"],
                "lane_type": lane["lane_type"],
                "flow_units": flow_units,
                "lead_time_days": int(lane["lead_time_days"]),
                "unit_purchase_cost": purchase_cost,
                "unit_transport_cost": transport_cost,
                "total_procurement_cost": flow_units * purchase_cost,
                "total_transport_cost": flow_units * transport_cost,
            }
        )
    flows = pd.DataFrame(flow_rows).sort_values(["product_id", "lane_id"], ignore_index=True)

    plan_rows: list[dict] = []
    for requirement in requirements.sort_values(["product_id", "warehouse_id"]).itertuples(
        index=False
    ):
        key = (requirement.product_id, requirement.warehouse_id)
        inbound = int(
            flows.loc[
                (flows["product_id"] == requirement.product_id)
                & (flows["destination_node_id"] == requirement.warehouse_id),
                "flow_units",
            ].sum()
        )
        outbound = int(
            flows.loc[
                (flows["product_id"] == requirement.product_id)
                & (flows["source_node_id"] == requirement.warehouse_id),
                "flow_units",
            ].sum()
        )
        ending = int(solution[variables.ending[key]])
        shortage = int(solution[variables.shortage[key]])
        demand = int(requirement.demand_units)
        fulfilled = demand - shortage
        balance_error = (
            int(requirement.starting_inventory_units) + inbound - outbound - fulfilled - ending
        )
        plan_rows.append(
            {
                "product_id": requirement.product_id,
                "warehouse_id": requirement.warehouse_id,
                "demand_units": demand,
                "starting_inventory_units": int(requirement.starting_inventory_units),
                "inbound_units": inbound,
                "outbound_units": outbound,
                "fulfilled_units": fulfilled,
                "shortage_units": shortage,
                "ending_inventory_units": ending,
                "target_service_level": float(requirement.target_service_level),
                "achieved_service_level": fulfilled / demand if demand else 1.0,
                "ending_inventory_value": ending * float(requirement.unit_cost),
                "shortage_cost": shortage
                * float(requirement.lost_margin_per_unit)
                * settings.shortage_penalty_multiplier,
                "balance_error_units": balance_error,
            }
        )
    plan = pd.DataFrame(plan_rows)
    if plan["balance_error_units"].abs().max() > 0:
        raise NetworkOptimizationError("Network solution failed independent flow reconciliation")

    utilization_rows: list[dict] = []

    def add_utilization(
        constraint_type: str, constraint_id: str, capacity: float, used: float
    ) -> None:
        slack = capacity - used
        utilization_rows.append(
            {
                "constraint_type": constraint_type,
                "constraint_id": constraint_id,
                "capacity_units": capacity,
                "used_units": used,
                "slack_units": slack,
                "utilization_rate": used / capacity if capacity > 0 else 0.0,
                "binding": bool(abs(slack) <= 1e-6),
            }
        )

    for lane in active_lanes.itertuples(index=False):
        add_utilization(
            "lane_capacity",
            lane.lane_id,
            float(lane.daily_capacity_units * settings.horizon_days),
            float(flows.loc[flows["lane_id"] == lane.lane_id, "flow_units"].sum()),
        )
    for source in active_sources.itertuples(index=False):
        add_utilization(
            "source_capacity",
            f"{source.product_id}|{source.supplier_id}",
            float(source.max_horizon_units),
            float(
                flows.loc[
                    (flows["product_id"] == source.product_id)
                    & (flows["source_node_id"] == source.supplier_id)
                    & (flows["lane_type"] == "inbound"),
                    "flow_units",
                ].sum()
            ),
        )
    for warehouse_id in warehouses:
        add_utilization(
            "warehouse_capacity",
            warehouse_id,
            float(nodes.loc[nodes["node_id"] == warehouse_id, "storage_capacity_units"].item()),
            float(plan.loc[plan["warehouse_id"] == warehouse_id, "ending_inventory_units"].sum()),
        )
    for row in plan.itertuples(index=False):
        allowed_shortage = float(
            np.floor((1.0 - row.target_service_level) * row.demand_units + 1e-9)
        )
        add_utilization(
            "service_shortage",
            f"{row.product_id}|{row.warehouse_id}",
            allowed_shortage,
            float(row.shortage_units),
        )
    utilization = pd.DataFrame(utilization_rows).sort_values(
        ["constraint_type", "constraint_id"], ignore_index=True
    )
    if (utilization["slack_units"] < -1e-6).any():
        raise NetworkOptimizationError("Network solution violates a capacity constraint")

    procurement_cost = float(flows["total_procurement_cost"].sum())
    transport_cost = float(flows["total_transport_cost"].sum())
    holding_cost = float(
        (
            plan["ending_inventory_value"]
            * settings.annual_holding_cost_rate
            * settings.horizon_days
            / 365
        ).sum()
    )
    shortage_cost = float(plan["shortage_cost"].sum())
    ordering_cost = float(sum(solution[index] for index in variables.order.values())) * float(
        settings.ordering_cost
    )
    total_demand = int(plan["demand_units"].sum())
    total_fulfilled = int(plan["fulfilled_units"].sum())
    summary = pd.DataFrame(
        [
            {
                "solver_status": "optimal",
                "solver_message": str(result.message),
                "objective_value": round(float(result.fun), 4),
                "mip_gap": float(getattr(result, "mip_gap", 0.0) or 0.0),
                "mip_node_count": int(getattr(result, "mip_node_count", 0) or 0),
                "horizon_days": settings.horizon_days,
                "total_demand_units": total_demand,
                "total_fulfilled_units": total_fulfilled,
                "total_shortage_units": int(plan["shortage_units"].sum()),
                "weighted_service_level": total_fulfilled / total_demand if total_demand else 1.0,
                "total_ending_inventory_units": int(plan["ending_inventory_units"].sum()),
                "total_procurement_cost": round(procurement_cost, 2),
                "total_transport_cost": round(transport_cost, 2),
                "total_holding_cost": round(holding_cost, 2),
                "total_shortage_cost": round(shortage_cost, 2),
                "total_ordering_cost": round(ordering_cost, 2),
                "max_flow_balance_error": int(plan["balance_error_units"].abs().max()),
            }
        ]
    )
    return plan, flows, utilization, summary


def build_network_requirements(
    daily: pd.DataFrame,
    inventory: pd.DataFrame,
    products: pd.DataFrame,
    nodes: pd.DataFrame,
    settings: NetworkOptimizationSettings,
) -> pd.DataFrame:
    """Build a no-future-data planning requirement at product-warehouse grain."""
    _required_columns(
        daily,
        {"date", "product_id", "warehouse_id", "units_demanded"},
        "daily_product_warehouse_metrics",
    )
    _required_columns(
        inventory,
        {"snapshot_date", "product_id", "warehouse_id", "available_units"},
        "inventory_snapshots",
    )
    _required_columns(
        products,
        {"product_id", "unit_cost", "unit_price", "target_service_level"},
        "products",
    )
    working_daily = daily.copy()
    working_daily["date"] = pd.to_datetime(working_daily["date"], errors="raise")
    as_of_date = working_daily["date"].max()
    if pd.isna(as_of_date):
        raise ValueError("Demand history has no planning as-of date")
    lookback_start = as_of_date - pd.Timedelta(days=settings.demand_lookback_days - 1)
    history = working_daily[
        (working_daily["date"] >= lookback_start) & (working_daily["date"] <= as_of_date)
    ]
    demand = (
        history.groupby(["product_id", "warehouse_id"], as_index=False)["units_demanded"]
        .mean()
        .rename(columns={"units_demanded": "mean_daily_demand"})
    )
    demand["demand_units"] = np.ceil(demand["mean_daily_demand"] * settings.horizon_days).astype(
        int
    )

    warehouse_ids = (
        nodes.loc[nodes["node_type"].isin({"gateway", "regional_dc"}), "node_id"]
        .astype(str)
        .sort_values()
        .tolist()
    )
    product_ids = sorted(products["product_id"].astype(str).unique())
    grid = pd.MultiIndex.from_product(
        [product_ids, warehouse_ids], names=["product_id", "warehouse_id"]
    ).to_frame(index=False)
    requirements = grid.merge(
        demand[["product_id", "warehouse_id", "demand_units"]],
        on=["product_id", "warehouse_id"],
        how="left",
        validate="one_to_one",
    )
    requirements["demand_units"] = requirements["demand_units"].fillna(0).astype(int)

    working_inventory = inventory.copy()
    working_inventory["snapshot_date"] = pd.to_datetime(
        working_inventory["snapshot_date"], errors="raise"
    )
    latest_inventory = (
        working_inventory[working_inventory["snapshot_date"] <= as_of_date]
        .sort_values("snapshot_date")
        .groupby(["product_id", "warehouse_id"], as_index=False)
        .tail(1)
    )
    requirements = requirements.merge(
        latest_inventory[["product_id", "warehouse_id", "available_units"]],
        on=["product_id", "warehouse_id"],
        how="left",
        validate="one_to_one",
    )
    requirements["starting_inventory_units"] = (
        requirements.pop("available_units").fillna(0).clip(lower=0).astype(int)
    )
    requirements = requirements.merge(
        products[["product_id", "unit_cost", "unit_price", "target_service_level"]],
        on="product_id",
        how="left",
        validate="many_to_one",
    )
    if requirements[["unit_cost", "unit_price", "target_service_level"]].isna().any().any():
        raise ValueError("Network requirements contain products without economic parameters")
    requirements["lost_margin_per_unit"] = (
        requirements["unit_price"] - requirements["unit_cost"]
    ).clip(lower=0)
    return requirements[
        [
            "product_id",
            "warehouse_id",
            "demand_units",
            "starting_inventory_units",
            "target_service_level",
            "unit_cost",
            "lost_margin_per_unit",
        ]
    ].sort_values(["product_id", "warehouse_id"], ignore_index=True)


def run_network_optimization(
    *,
    settings: NetworkOptimizationSettings | None = None,
    config_path: Path | None = None,
    nodes_path: Path = DATA_RAW / "network_nodes.csv",
    lanes_path: Path = DATA_RAW / "network_lanes.csv",
    sources_path: Path = DATA_RAW / "product_sources.csv",
    daily_path: Path = DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
    inventory_path: Path = DATA_RAW / "inventory_snapshots.csv",
    products_path: Path = DATA_RAW / "products.csv",
    output_dir: Path = OUTPUT_TABLES_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build, solve, validate, and materialize the current network plan."""
    if settings is not None and config_path is not None:
        raise ValueError("Provide settings or config_path, not both")
    optimization_settings = settings or load_settings(config_path).network_optimization
    if not optimization_settings.enabled:
        raise ValueError("network_optimization is disabled")
    nodes = pd.read_csv(nodes_path)
    lanes = pd.read_csv(lanes_path)
    sources = pd.read_csv(sources_path)
    daily = pd.read_csv(daily_path, parse_dates=["date"])
    inventory = pd.read_csv(inventory_path, parse_dates=["snapshot_date"])
    products = pd.read_csv(products_path)
    requirements = build_network_requirements(
        daily, inventory, products, nodes, optimization_settings
    )
    plan, flows, utilization, summary = solve_network_plan(
        nodes,
        lanes,
        sources,
        requirements,
        optimization_settings,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    plan.to_csv(output_dir / "network_optimization_plan.csv", index=False)
    flows.to_csv(output_dir / "network_flow_plan.csv", index=False)
    utilization.to_csv(output_dir / "network_constraint_utilization.csv", index=False)
    summary.to_csv(output_dir / "network_optimization_summary.csv", index=False)
    print(
        "Network optimization complete. "
        f"Service: {summary.iloc[0]['weighted_service_level']:.3%}; "
        f"objective: {summary.iloc[0]['objective_value']:,.2f}; "
        f"flows: {len(flows)}"
    )
    return plan, flows, utilization, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize constrained multi-echelon inventory")
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    run_network_optimization(config_path=args.config)


if __name__ == "__main__":
    main()
