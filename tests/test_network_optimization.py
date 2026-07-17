"""Multi-echelon MILP feasibility, conservation, and economic routing."""

from __future__ import annotations

import pandas as pd
import pytest
from src.network_optimization import (
    NetworkOptimizationError,
    build_network_requirements,
    run_network_optimization,
    solve_network_plan,
)
from src.settings import NetworkOptimizationSettings


def _settings() -> NetworkOptimizationSettings:
    return NetworkOptimizationSettings(
        enabled=True,
        horizon_days=30,
        demand_lookback_days=60,
        annual_holding_cost_rate=0.18,
        shortage_penalty_multiplier=2.5,
        ordering_cost=35.0,
        time_limit_seconds=10,
        mip_relative_gap=0.0,
    )


def _network(source_one_capacity: int = 80) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    nodes = pd.DataFrame(
        [
            {"node_id": "S1", "node_type": "supplier", "storage_capacity_units": 0},
            {"node_id": "S2", "node_type": "supplier", "storage_capacity_units": 0},
            {"node_id": "G", "node_type": "gateway", "storage_capacity_units": 1_000},
            {"node_id": "R", "node_type": "regional_dc", "storage_capacity_units": 1_000},
        ]
    )
    lanes = pd.DataFrame(
        [
            {
                "lane_id": "S1-G",
                "source_node_id": "S1",
                "destination_node_id": "G",
                "lane_type": "inbound",
                "lead_time_days": 5,
                "unit_transport_cost": 0.10,
                "daily_capacity_units": 100,
                "enabled": 1,
            },
            {
                "lane_id": "S2-G",
                "source_node_id": "S2",
                "destination_node_id": "G",
                "lane_type": "inbound",
                "lead_time_days": 6,
                "unit_transport_cost": 0.20,
                "daily_capacity_units": 100,
                "enabled": 1,
            },
            {
                "lane_id": "G-R",
                "source_node_id": "G",
                "destination_node_id": "R",
                "lane_type": "transfer",
                "lead_time_days": 2,
                "unit_transport_cost": 0.30,
                "daily_capacity_units": 100,
                "enabled": 1,
            },
        ]
    )
    sources = pd.DataFrame(
        [
            {
                "product_id": "P1",
                "supplier_id": "S1",
                "is_primary": 1,
                "unit_purchase_cost": 2.0,
                "minimum_order_qty": 30,
                "max_horizon_units": source_one_capacity,
                "source_lead_time_days": 5,
                "enabled": 1,
            },
            {
                "product_id": "P1",
                "supplier_id": "S2",
                "is_primary": 0,
                "unit_purchase_cost": 3.0,
                "minimum_order_qty": 10,
                "max_horizon_units": 100,
                "source_lead_time_days": 6,
                "enabled": 1,
            },
        ]
    )
    return nodes, lanes, sources


def _requirements(total_demand: int = 90) -> pd.DataFrame:
    regional_demand = total_demand - 20
    return pd.DataFrame(
        [
            {
                "product_id": "P1",
                "warehouse_id": "G",
                "demand_units": 20,
                "starting_inventory_units": 0,
                "target_service_level": 1.0,
                "unit_cost": 2.0,
                "lost_margin_per_unit": 5.0,
            },
            {
                "product_id": "P1",
                "warehouse_id": "R",
                "demand_units": regional_demand,
                "starting_inventory_units": 0,
                "target_service_level": 1.0,
                "unit_cost": 2.0,
                "lost_margin_per_unit": 5.0,
            },
        ]
    )


def test_solver_enforces_flow_conservation_service_capacity_and_moq() -> None:
    nodes, lanes, sources = _network(source_one_capacity=80)

    plan, flows, utilization, summary = solve_network_plan(
        nodes, lanes, sources, _requirements(), _settings()
    )

    assert summary.iloc[0]["solver_status"] == "optimal"
    assert plan["balance_error_units"].abs().max() == 0
    assert (plan["achieved_service_level"] >= plan["target_service_level"]).all()
    assert plan["shortage_units"].sum() == 0
    source_flows = flows[flows["lane_type"] == "inbound"].set_index("source_node_id")
    assert source_flows.loc["S1", "flow_units"] == 80
    assert source_flows.loc["S2", "flow_units"] == 10
    assert flows.loc[flows["lane_id"] == "G-R", "flow_units"].item() == 70
    assert (flows["flow_units"] % 1 == 0).all()
    assert (utilization["used_units"] <= utilization["capacity_units"] + 1e-9).all()


def test_solver_uses_lowest_total_cost_eligible_source() -> None:
    nodes, lanes, sources = _network(source_one_capacity=500)

    _, flows, _, _ = solve_network_plan(nodes, lanes, sources, _requirements(), _settings())

    inbound = flows[flows["lane_type"] == "inbound"].set_index("source_node_id")
    assert inbound.loc["S1", "flow_units"] == 90
    assert "S2" not in inbound.index


def test_solver_reports_infeasible_service_plan() -> None:
    nodes, lanes, sources = _network(source_one_capacity=30)
    sources.loc[sources["supplier_id"] == "S2", "max_horizon_units"] = 20

    with pytest.raises(NetworkOptimizationError, match="infeasible"):
        solve_network_plan(nodes, lanes, sources, _requirements(total_demand=100), _settings())


@pytest.mark.parametrize(
    ("table", "column"),
    [("lanes", "lane_id"), ("sources", "supplier_id"), ("requirements", "demand_units")],
)
def test_solver_rejects_invalid_model_inputs(table: str, column: str) -> None:
    nodes, lanes, sources = _network()
    requirements = _requirements()
    target = {"lanes": lanes, "sources": sources, "requirements": requirements}[table]
    if column == "demand_units":
        target.loc[0, column] = -1
    else:
        target.loc[1, column] = target.loc[0, column]

    with pytest.raises(ValueError):
        solve_network_plan(nodes, lanes, sources, requirements, _settings())


def test_runner_builds_lagged_requirements_and_materializes_outputs(tmp_path) -> None:
    nodes, lanes, sources = _network(source_one_capacity=80)
    dates = pd.date_range("2025-01-01", periods=10, freq="D")
    daily = pd.DataFrame(
        [
            {
                "date": date,
                "product_id": "P1",
                "warehouse_id": warehouse_id,
                "units_demanded": daily_demand,
            }
            for date in dates
            for warehouse_id, daily_demand in (("G", 2), ("R", 7))
        ]
    )
    inventory = pd.DataFrame(
        [
            {
                "snapshot_date": dates[-1],
                "product_id": "P1",
                "warehouse_id": warehouse_id,
                "available_units": 0,
            }
            for warehouse_id in ("G", "R")
        ]
    )
    products = pd.DataFrame(
        [
            {
                "product_id": "P1",
                "unit_cost": 2.0,
                "unit_price": 7.0,
                "target_service_level": 1.0,
            }
        ]
    )

    requirements = build_network_requirements(daily, inventory, products, nodes, _settings())
    assert requirements.set_index("warehouse_id")["demand_units"].to_dict() == {
        "G": 60,
        "R": 210,
    }

    sources["max_horizon_units"] = [250, 100]
    paths = {}
    for name, frame in {
        "nodes": nodes,
        "lanes": lanes,
        "sources": sources,
        "daily": daily,
        "inventory": inventory,
        "products": products,
    }.items():
        path = tmp_path / f"{name}.csv"
        frame.to_csv(path, index=False)
        paths[name] = path

    plan, flows, utilization, summary = run_network_optimization(
        settings=_settings(),
        nodes_path=paths["nodes"],
        lanes_path=paths["lanes"],
        sources_path=paths["sources"],
        daily_path=paths["daily"],
        inventory_path=paths["inventory"],
        products_path=paths["products"],
        output_dir=tmp_path,
    )

    assert not plan.empty and not flows.empty and not utilization.empty
    assert summary.iloc[0]["weighted_service_level"] == 1.0
    for file_name in (
        "network_optimization_plan.csv",
        "network_flow_plan.csv",
        "network_constraint_utilization.csv",
        "network_optimization_summary.csv",
    ):
        assert (tmp_path / file_name).exists()
