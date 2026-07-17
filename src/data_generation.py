"""Seeded synthetic-data generator for the multi-warehouse FMCG network.

Deliberately embeds the structural patterns the downstream analysis is built
to detect — two unreliable suppliers, a slow network-wide service drift, and
ABC inventory skew — so the findings exercise real analytical logic instead
of describing random noise. Writes the raw-layer CSVs to data/raw/."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

from src.config import DATA_RAW, END_DATE, RANDOM_SEED, START_DATE


@dataclass(frozen=True)
class SimulationConfig:
    """Configuration for synthetic supply chain data generation."""

    seed: int = RANDOM_SEED
    start_date: str = START_DATE
    end_date: str = END_DATE
    n_products: int = 120
    n_suppliers: int = 12

    def __post_init__(self) -> None:
        if pd.Timestamp(self.start_date) > pd.Timestamp(self.end_date):
            raise ValueError("start_date must be on or before end_date")
        if self.n_products < 1:
            raise ValueError("n_products must be at least 1")
        if self.n_suppliers < 1:
            raise ValueError("n_suppliers must be at least 1")


def _bounded(value: float, low: float, high: float) -> float:
    return float(np.clip(value, low, high))


def build_suppliers(cfg: SimulationConfig, rng: np.random.Generator) -> pd.DataFrame:
    """Create supplier master with intentional risk profile diversity."""
    regions = ["Iberia", "Central Europe", "UK", "Nordics", "East Europe"]
    records: list[dict] = []

    for idx in range(1, cfg.n_suppliers + 1):
        supplier_id = f"SUP-{idx:03d}"

        if idx <= 3:
            reliability = _bounded(rng.normal(0.74, 0.05), 0.60, 0.86)
            avg_lt = int(np.clip(round(rng.normal(22, 3.5)), 15, 30))
            lt_var = _bounded(rng.normal(0.42, 0.08), 0.25, 0.65)
            moq = int(np.clip(round(rng.normal(750, 130)), 500, 1000))
        elif idx <= 8:
            reliability = _bounded(rng.normal(0.86, 0.04), 0.77, 0.93)
            avg_lt = int(np.clip(round(rng.normal(14, 2.5)), 8, 22))
            lt_var = _bounded(rng.normal(0.24, 0.05), 0.12, 0.38)
            moq = int(np.clip(round(rng.normal(420, 90)), 240, 640))
        else:
            reliability = _bounded(rng.normal(0.95, 0.02), 0.90, 0.99)
            avg_lt = int(np.clip(round(rng.normal(8, 1.8)), 4, 14))
            lt_var = _bounded(rng.normal(0.12, 0.03), 0.06, 0.20)
            moq = int(np.clip(round(rng.normal(180, 50)), 80, 320))

        records.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": f"Supplier {idx}",
                "supplier_region": rng.choice(regions, p=[0.33, 0.27, 0.12, 0.13, 0.15]),
                "reliability_score": round(reliability, 3),
                "average_lead_time_days": int(avg_lt),
                "lead_time_variability": round(lt_var, 3),
                "minimum_order_qty": int(moq),
            }
        )

    return pd.DataFrame(records)


def build_warehouses() -> pd.DataFrame:
    """Create warehouse network with regional demand and execution differences."""
    return pd.DataFrame(
        [
            {
                "warehouse_id": "WH-LIS",
                "warehouse_name": "Lisbon Distribution Center",
                "region": "Portugal South",
                "storage_capacity_units": 1_850_000,
            },
            {
                "warehouse_id": "WH-PORTO",
                "warehouse_name": "Porto Distribution Center",
                "region": "Portugal North",
                "storage_capacity_units": 1_420_000,
            },
            {
                "warehouse_id": "WH-MAD",
                "warehouse_name": "Madrid Regional Hub",
                "region": "Spain Central",
                "storage_capacity_units": 1_650_000,
            },
            {
                "warehouse_id": "WH-LYON",
                "warehouse_name": "Lyon EU Gateway",
                "region": "France South-East",
                "storage_capacity_units": 1_120_000,
            },
        ]
    )


def build_products_and_classification(
    cfg: SimulationConfig, suppliers: pd.DataFrame, rng: np.random.Generator
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create product master + ABC/criticality classification + hidden simulation attributes."""
    categories = [
        "Beverages",
        "Snacks",
        "Personal Care",
        "Household",
        "Frozen",
        "Dairy",
        "Pet Care",
        "Health",
    ]

    category_cost = {
        "Beverages": (2.2, 18.0),
        "Snacks": (1.5, 12.0),
        "Personal Care": (3.5, 28.0),
        "Household": (4.0, 35.0),
        "Frozen": (2.8, 20.0),
        "Dairy": (1.8, 10.0),
        "Pet Care": (4.5, 42.0),
        "Health": (8.0, 70.0),
    }

    category_shelf_life = {
        "Beverages": (180, 540),
        "Snacks": (120, 360),
        "Personal Care": (365, 1095),
        "Household": (365, 1095),
        "Frozen": (300, 720),
        "Dairy": (12, 45),
        "Pet Care": (210, 720),
        "Health": (365, 1460),
    }

    abc_mix = ["A"] * int(cfg.n_products * 0.20) + ["B"] * int(cfg.n_products * 0.30)
    abc_mix += ["C"] * (cfg.n_products - len(abc_mix))

    supplier_weights = suppliers["reliability_score"].to_numpy()
    supplier_weights = supplier_weights / supplier_weights.sum()

    chronic_profiles = np.array(["normal"] * cfg.n_products, dtype=object)
    overstock_count = min(cfg.n_products, max(1, int(cfg.n_products * 0.12)))
    overstock_idx = rng.choice(np.arange(cfg.n_products), size=overstock_count, replace=False)
    remaining = np.setdiff1d(np.arange(cfg.n_products), overstock_idx)
    stockout_count = min(len(remaining), max(1, int(cfg.n_products * 0.12)))
    stockout_idx = rng.choice(remaining, size=stockout_count, replace=False)
    chronic_profiles[overstock_idx] = "chronic_overstock"
    chronic_profiles[stockout_idx] = "chronic_stockout"

    product_rows: list[dict] = []
    class_rows: list[dict] = []
    sim_rows: list[dict] = []

    for idx in range(cfg.n_products):
        product_id = f"SKU-{idx + 1:04d}"
        category = categories[idx % len(categories)]
        abc = abc_mix[idx]

        if chronic_profiles[idx] == "chronic_stockout":
            supplier = suppliers.iloc[int(rng.integers(0, min(3, len(suppliers))))]
        else:
            supplier = suppliers.iloc[
                int(rng.choice(np.arange(len(suppliers)), p=supplier_weights))
            ]

        c_min, c_max = category_cost[category]
        unit_cost = round(float(rng.uniform(c_min, c_max)), 2)
        markup = float(rng.uniform(1.25, 2.4))
        unit_price = round(unit_cost * markup, 2)

        sl_min, sl_max = category_shelf_life[category]
        shelf_life = int(rng.integers(sl_min, sl_max + 1))

        lead_time_days = int(
            max(
                1,
                round(
                    rng.normal(
                        supplier["average_lead_time_days"],
                        supplier["average_lead_time_days"] * 0.08,
                    )
                ),
            )
        )

        if abc == "A":
            target_service = _bounded(rng.normal(0.98, 0.01), 0.95, 0.995)
            base_demand = _bounded(rng.normal(85, 18), 35, 160)
            demand_cv = _bounded(rng.normal(0.22, 0.05), 0.12, 0.38)
            target_cover_days = int(np.clip(round(rng.normal(16, 3)), 10, 24))
        elif abc == "B":
            target_service = _bounded(rng.normal(0.95, 0.015), 0.90, 0.985)
            base_demand = _bounded(rng.normal(42, 12), 12, 85)
            demand_cv = _bounded(rng.normal(0.30, 0.06), 0.18, 0.48)
            target_cover_days = int(np.clip(round(rng.normal(25, 4)), 16, 36))
        else:
            target_service = _bounded(rng.normal(0.90, 0.02), 0.84, 0.95)
            base_demand = _bounded(rng.normal(13, 6), 2, 35)
            demand_cv = _bounded(rng.normal(0.45, 0.08), 0.25, 0.70)
            target_cover_days = int(np.clip(round(rng.normal(38, 7)), 24, 56))

        if chronic_profiles[idx] == "chronic_overstock":
            planning_bias = _bounded(rng.normal(1.45, 0.10), 1.25, 1.70)
        elif chronic_profiles[idx] == "chronic_stockout":
            planning_bias = _bounded(rng.normal(0.78, 0.08), 0.55, 0.92)
        else:
            planning_bias = _bounded(rng.normal(1.0, 0.12), 0.80, 1.25)

        if abc == "A" or category in {"Health", "Dairy", "Frozen"}:
            criticality = "High"
        elif abc == "B":
            criticality = "Medium"
        else:
            criticality = "Low"

        product_rows.append(
            {
                "product_id": product_id,
                "product_name": f"{category} Product {idx + 1}",
                "category": category,
                "unit_cost": unit_cost,
                "unit_price": unit_price,
                "shelf_life_days": shelf_life,
                "supplier_id": supplier["supplier_id"],
                "lead_time_days": lead_time_days,
                "target_service_level": round(target_service, 3),
            }
        )

        class_rows.append(
            {"product_id": product_id, "abc_class": abc, "criticality_level": criticality}
        )

        sim_rows.append(
            {
                "product_id": product_id,
                "base_daily_demand": round(base_demand, 3),
                "demand_cv": round(demand_cv, 3),
                "target_cover_days": target_cover_days,
                "planning_bias": round(planning_bias, 3),
                "chronic_profile": chronic_profiles[idx],
            }
        )

    return pd.DataFrame(product_rows), pd.DataFrame(class_rows), pd.DataFrame(sim_rows)


def seasonality_index(date: pd.Timestamp) -> float:
    """Multiplicative demand factor: FMCG-style monthly curve (summer and
    pre-holiday peaks) with a midweek uplift and weekend trough."""
    month_factor = {
        1: 0.93,
        2: 0.95,
        3: 1.00,
        4: 1.04,
        5: 1.02,
        6: 1.08,
        7: 1.12,
        8: 1.10,
        9: 1.03,
        10: 1.05,
        11: 1.11,
        12: 1.24,
    }[date.month]
    weekday_factor = 0.92 if date.weekday() in (5, 6) else 1.0
    return round(month_factor * weekday_factor, 3)


def build_intervention_assignments(
    products: pd.DataFrame,
    product_classification: pd.DataFrame,
    warehouses: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Assign units to a randomized policy pilot and a supplier DiD cohort."""
    product_columns = {"product_id", "category", "supplier_id"}
    if not product_columns.issubset(products.columns):
        raise ValueError("Products are missing intervention-assignment fields")
    if not {"product_id", "abc_class"}.issubset(product_classification.columns):
        raise ValueError("Product classification is missing assignment fields")
    if "warehouse_id" not in warehouses.columns:
        raise ValueError("Warehouses are missing intervention-assignment fields")

    eligible = products[sorted(product_columns)].merge(
        product_classification[["product_id", "abc_class"]],
        on="product_id",
        validate="one_to_one",
    )
    eligible = eligible.merge(warehouses[["warehouse_id"]], how="cross")
    eligible["unit_id"] = eligible["product_id"] + "|" + eligible["warehouse_id"]
    eligible["stratum"] = eligible["warehouse_id"] + "|" + eligible["abc_class"]
    eligible = eligible.sort_values(["stratum", "unit_id"]).reset_index(drop=True)
    eligible["treatment_flag"] = 0
    for indices in eligible.groupby("stratum", sort=True).groups.values():
        group_indices = np.asarray(list(indices), dtype=int)
        treated_indices = rng.permutation(group_indices)[: len(group_indices) // 2]
        eligible.loc[treated_indices, "treatment_flag"] = 1

    common = {
        "assignment_date": "2025-04-01",
        "intervention_date": "2025-07-01",
        "outcome_metric": "fill_rate",
        "status": "completed",
    }
    rct = eligible[
        [
            "unit_id",
            "product_id",
            "warehouse_id",
            "supplier_id",
            "stratum",
            "treatment_flag",
        ]
    ].copy()
    rct.insert(0, "experiment_id", "EXP-RCT-001")
    rct.insert(1, "design", "randomized_controlled_trial")
    rct.insert(2, "assignment_method", "stratified_randomization")
    rct["treatment_group"] = np.where(
        rct["treatment_flag"].eq(1), "policy_reset", "business_as_usual"
    )
    for column, value in common.items():
        rct[column] = value

    did = eligible[eligible["supplier_id"].isin({"SUP-001", "SUP-002", "SUP-003"})][
        ["unit_id", "product_id", "warehouse_id", "supplier_id", "category"]
    ].copy()
    did.insert(0, "experiment_id", "EXP-DID-001")
    did.insert(1, "design", "difference_in_differences")
    did.insert(2, "assignment_method", "matched_supplier_comparison")
    did["stratum"] = did.pop("category")
    did["treatment_flag"] = did["supplier_id"].eq("SUP-002").astype(int)
    did["treatment_group"] = np.where(
        did["treatment_flag"].eq(1), "supplier_recovery", "comparison_suppliers"
    )
    for column, value in common.items():
        did[column] = value

    columns = [
        "experiment_id",
        "design",
        "assignment_method",
        "unit_id",
        "product_id",
        "warehouse_id",
        "supplier_id",
        "stratum",
        "treatment_group",
        "treatment_flag",
        "assignment_date",
        "intervention_date",
        "outcome_metric",
        "status",
    ]
    return (
        pd.concat([rct[columns], did[columns]], ignore_index=True)
        .sort_values(["experiment_id", "unit_id"])
        .reset_index(drop=True)
    )


def build_network_tables(
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build a constrained supplier-gateway-regional planning network."""
    gateway_id = "WH-LYON"
    if gateway_id not in set(warehouses["warehouse_id"]):
        raise ValueError(f"Network gateway is missing: {gateway_id}")
    supplier_nodes = pd.DataFrame(
        {
            "node_id": suppliers["supplier_id"],
            "node_type": "supplier",
            "region": suppliers["supplier_region"],
            "storage_capacity_units": 0,
        }
    )
    warehouse_nodes = warehouses[["warehouse_id", "region", "storage_capacity_units"]].rename(
        columns={"warehouse_id": "node_id"}
    )
    warehouse_nodes["node_type"] = np.where(
        warehouse_nodes["node_id"].eq(gateway_id), "gateway", "regional_dc"
    )
    nodes = pd.concat(
        [
            supplier_nodes,
            warehouse_nodes[["node_id", "node_type", "region", "storage_capacity_units"]],
        ],
        ignore_index=True,
    ).sort_values("node_id", ignore_index=True)

    lane_rows: list[dict] = []
    for supplier in suppliers.sort_values("supplier_id").itertuples(index=False):
        lane_rows.append(
            {
                "lane_id": f"LANE-{supplier.supplier_id}-{gateway_id}",
                "source_node_id": supplier.supplier_id,
                "destination_node_id": gateway_id,
                "lane_type": "inbound",
                "lead_time_days": int(supplier.average_lead_time_days),
                "unit_transport_cost": round(
                    0.06 + (1.0 - float(supplier.reliability_score)) * 0.15, 4
                ),
                "daily_capacity_units": int(
                    max(1_000, round(25_000 * float(supplier.reliability_score)))
                ),
                "enabled": 1,
            }
        )
    transfer_profiles = {
        "WH-LIS": (3, 0.11, 45_000),
        "WH-PORTO": (3, 0.12, 35_000),
        "WH-MAD": (2, 0.09, 50_000),
    }
    for destination in sorted(set(warehouses["warehouse_id"]) - {gateway_id}):
        if destination not in transfer_profiles:
            raise ValueError(f"Missing transfer profile for {destination}")
        lead_time, unit_cost, capacity = transfer_profiles[destination]
        lane_rows.append(
            {
                "lane_id": f"LANE-{gateway_id}-{destination}",
                "source_node_id": gateway_id,
                "destination_node_id": destination,
                "lane_type": "transfer",
                "lead_time_days": lead_time,
                "unit_transport_cost": unit_cost,
                "daily_capacity_units": capacity,
                "enabled": 1,
            }
        )
    lanes = pd.DataFrame(lane_rows).sort_values("lane_id", ignore_index=True)

    supplier_lookup = suppliers.set_index("supplier_id")
    ranked_suppliers = suppliers.sort_values(
        ["reliability_score", "supplier_id"], ascending=[False, True]
    )["supplier_id"].tolist()
    source_rows: list[dict] = []
    for product in products.sort_values("product_id").itertuples(index=False):
        primary = str(product.supplier_id)
        if primary not in supplier_lookup.index:
            raise ValueError(f"Unknown primary source for {product.product_id}: {primary}")
        alternate = next((supplier for supplier in ranked_suppliers if supplier != primary), None)
        if alternate is None:
            raise ValueError(f"No alternate source available for {product.product_id}")
        for supplier_id, is_primary in ((primary, 1), (alternate, 0)):
            supplier = supplier_lookup.loc[supplier_id]
            premium = 1.0 if is_primary else float(rng.uniform(1.05, 1.12))
            minimum_order_qty = int(supplier["minimum_order_qty"])
            source_rows.append(
                {
                    "product_id": product.product_id,
                    "supplier_id": supplier_id,
                    "is_primary": is_primary,
                    "unit_purchase_cost": round(float(product.unit_cost) * premium, 2),
                    "minimum_order_qty": minimum_order_qty,
                    "max_horizon_units": max(10_000, minimum_order_qty * 20),
                    "source_lead_time_days": int(supplier["average_lead_time_days"]),
                    "enabled": 1,
                }
            )
    sources = pd.DataFrame(source_rows).sort_values(
        ["product_id", "is_primary"], ascending=[True, False], ignore_index=True
    )
    return nodes, lanes, sources


def intervention_policy_levels(
    reorder_point: int,
    order_up_to: int,
    is_treated: bool,
    current_date: pd.Timestamp,
    intervention_date: pd.Timestamp,
) -> tuple[int, int]:
    """Return policy levels after the randomized treatment becomes active."""
    if not is_treated or current_date < intervention_date:
        return reorder_point, order_up_to
    return int(round(reorder_point * 1.20)), int(round(order_up_to * 1.15))


def supplier_recovery_profile(
    reliability_score: float,
    lead_time_variability: float,
    is_active: bool,
) -> tuple[float, float]:
    """Apply the simulated supplier-recovery mechanism when active."""
    if not is_active:
        return reliability_score, lead_time_variability
    return min(0.98, reliability_score + 0.18), max(0.05, lead_time_variability * 0.50)


def simulate_actual_arrival(
    current_date: pd.Timestamp,
    planned_lead_time_days: int,
    reliability_score: float,
    lead_time_variability: float,
    rng: np.random.Generator,
) -> tuple[pd.Timestamp, int]:
    """Simulate an arrival where supplier reliability controls on-time performance."""
    if planned_lead_time_days < 1:
        raise ValueError("planned_lead_time_days must be at least 1")
    if not 0.0 <= reliability_score <= 1.0:
        raise ValueError("reliability_score must be between 0 and 1")
    if lead_time_variability < 0.0:
        raise ValueError("lead_time_variability must be non-negative")

    expected_arrival = current_date + timedelta(days=planned_lead_time_days)
    variability_days = max(1.0, planned_lead_time_days * lead_time_variability)
    is_late = int(rng.random() > reliability_score)

    if is_late:
        delay_days = max(
            1,
            int(
                round(
                    abs(
                        rng.normal(
                            variability_days * 0.60,
                            max(1.0, variability_days * 0.35),
                        )
                    )
                )
            ),
        )
        return expected_arrival + timedelta(days=delay_days), is_late

    early_days = min(
        planned_lead_time_days - 1,
        int(round(abs(rng.normal(0.0, max(0.5, variability_days * 0.15))))),
    )
    return expected_arrival - timedelta(days=max(0, early_days)), is_late


def simulate_operations(
    cfg: SimulationConfig,
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    sim_attrs: pd.DataFrame,
    rng: np.random.Generator,
    intervention_assignments: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Simulate daily demand, inventory states, and purchase order execution."""
    dates = pd.date_range(cfg.start_date, cfg.end_date, freq="D")
    assignments = (
        intervention_assignments.copy()
        if intervention_assignments is not None
        else pd.DataFrame(
            columns=[
                "experiment_id",
                "unit_id",
                "supplier_id",
                "treatment_flag",
                "intervention_date",
            ]
        )
    )
    required_assignment_columns = {
        "experiment_id",
        "unit_id",
        "supplier_id",
        "treatment_flag",
        "intervention_date",
    }
    if not required_assignment_columns.issubset(assignments.columns):
        raise ValueError("Intervention assignments are missing simulation fields")
    assignments["intervention_date"] = pd.to_datetime(
        assignments["intervention_date"], errors="raise"
    )
    rct = assignments[assignments["experiment_id"] == "EXP-RCT-001"]
    rct_treated_units = set(rct.loc[rct["treatment_flag"].eq(1), "unit_id"])
    rct_intervention_date = rct["intervention_date"].min() if not rct.empty else pd.Timestamp.max
    did = assignments[assignments["experiment_id"] == "EXP-DID-001"]
    did_treated_suppliers = set(did.loc[did["treatment_flag"].eq(1), "supplier_id"])
    did_intervention_date = did["intervention_date"].min() if not did.empty else pd.Timestamp.max

    warehouse_profile = {
        "WH-LIS": {"demand_factor": 1.15, "planning_factor": 1.12, "volatility_factor": 0.95},
        "WH-PORTO": {"demand_factor": 0.95, "planning_factor": 1.03, "volatility_factor": 1.00},
        "WH-MAD": {"demand_factor": 1.05, "planning_factor": 0.93, "volatility_factor": 1.08},
        "WH-LYON": {"demand_factor": 0.84, "planning_factor": 0.90, "volatility_factor": 1.12},
    }

    product_lookup = products.set_index("product_id").to_dict(orient="index")
    supplier_lookup = suppliers.set_index("supplier_id").to_dict(orient="index")
    sim_lookup = sim_attrs.set_index("product_id").to_dict(orient="index")

    demand_rows: list[dict] = []
    inventory_rows: list[dict] = []
    po_rows: list[dict] = []

    po_counter = 0

    for product in products["product_id"]:
        p = product_lookup[product]
        s = supplier_lookup[p["supplier_id"]]
        sa = sim_lookup[product]

        for _, wh_row in warehouses.iterrows():
            wh_id = wh_row["warehouse_id"]
            wh_prof = warehouse_profile[wh_id]
            unit_id = f"{product}|{wh_id}"

            local_demand_mean = (
                sa["base_daily_demand"] * wh_prof["demand_factor"] * float(rng.uniform(0.92, 1.12))
            )
            local_demand_cv = _bounded(sa["demand_cv"] * wh_prof["volatility_factor"], 0.08, 0.90)
            policy_bias = sa["planning_bias"] * wh_prof["planning_factor"]

            reorder_point = int(local_demand_mean * (p["lead_time_days"] + 2) * policy_bias)
            order_up_to = int(
                local_demand_mean * (p["lead_time_days"] + sa["target_cover_days"]) * policy_bias
            )

            on_hand = max(40, int(order_up_to * rng.uniform(0.65, 1.05)))
            open_orders: list[dict] = []

            for current_date in dates:
                arrivals_today = [
                    o for o in open_orders if o["actual_arrival_date"] == current_date
                ]
                if arrivals_today:
                    on_hand += int(sum(o["received_units"] for o in arrivals_today))
                open_orders = [o for o in open_orders if o["actual_arrival_date"] > current_date]

                seasonality = seasonality_index(current_date)
                promo_probability = 0.04 + (0.04 if current_date.month in (11, 12) else 0.0)
                promo_probability += 0.03 if p["category"] in {"Snacks", "Beverages"} else 0.0
                promo_flag = int(rng.random() < min(0.22, promo_probability))
                promo_lift = (
                    _bounded(rng.normal(1.22, 0.09), 1.05, 1.45) if promo_flag == 1 else 1.0
                )

                demand_mean = local_demand_mean * seasonality * promo_lift
                demand_std = max(1.0, demand_mean * local_demand_cv)
                units_demanded = int(max(0, round(rng.normal(demand_mean, demand_std))))

                if sa["chronic_profile"] == "chronic_stockout":
                    units_demanded = int(
                        round(units_demanded * _bounded(rng.normal(1.08, 0.04), 1.0, 1.18))
                    )

                units_fulfilled = int(min(on_hand, units_demanded))
                units_lost_sales = int(units_demanded - units_fulfilled)
                stockout_flag = int(units_lost_sales > 0)

                on_hand -= units_fulfilled
                # Open-order visibility is based on quantities ordered. Using
                # eventual receipt quantities here would leak future supplier
                # underfill into the replenishment decision.
                on_order_units = int(sum(o["ordered_units"] for o in open_orders))
                inventory_position = on_hand + on_order_units
                active_reorder_point, active_order_up_to = intervention_policy_levels(
                    reorder_point,
                    order_up_to,
                    unit_id in rct_treated_units,
                    current_date,
                    rct_intervention_date,
                )

                if inventory_position <= active_reorder_point:
                    moq_multiplier = 1.4 if sa["chronic_profile"] == "chronic_overstock" else 1.0
                    effective_moq = int(round(s["minimum_order_qty"] * moq_multiplier))
                    ordered_units = int(max(active_order_up_to - inventory_position, effective_moq))

                    planned_lt = int(p["lead_time_days"])
                    expected_arrival = current_date + timedelta(days=planned_lt)
                    recovery_active = (
                        p["supplier_id"] in did_treated_suppliers
                        and current_date >= did_intervention_date
                    )
                    reliability_score, lead_time_variability = supplier_recovery_profile(
                        float(s["reliability_score"]),
                        float(s["lead_time_variability"]),
                        recovery_active,
                    )
                    actual_arrival, late_flag = simulate_actual_arrival(
                        current_date=current_date,
                        planned_lead_time_days=planned_lt,
                        reliability_score=reliability_score,
                        lead_time_variability=lead_time_variability,
                        rng=rng,
                    )

                    receipt_fill_rate = _bounded(rng.normal(0.985, 0.015), 0.9, 1.0)
                    if reliability_score < 0.80:
                        receipt_fill_rate = _bounded(rng.normal(0.93, 0.06), 0.78, 1.0)
                    received_units = int(max(1, round(ordered_units * receipt_fill_rate)))

                    po_counter += 1
                    po_id = f"PO-{po_counter:08d}"

                    po_entry = {
                        "po_id": po_id,
                        "supplier_id": p["supplier_id"],
                        "product_id": product,
                        "warehouse_id": wh_id,
                        "order_date": current_date,
                        "expected_arrival_date": expected_arrival,
                        "actual_arrival_date": actual_arrival,
                        "ordered_units": ordered_units,
                        "received_units": received_units,
                        "late_delivery_flag": late_flag,
                    }
                    po_rows.append(po_entry)
                    open_orders.append(po_entry)

                    on_order_units = int(sum(o["ordered_units"] for o in open_orders))

                reserved_units = int(
                    min(on_hand, round(local_demand_mean * rng.uniform(0.04, 0.22)))
                )
                available_units = int(max(0, on_hand - reserved_units))
                inventory_value = round(on_hand * p["unit_cost"], 2)
                days_of_supply = round(available_units / max(1.0, local_demand_mean), 2)

                demand_rows.append(
                    {
                        "date": current_date,
                        "warehouse_id": wh_id,
                        "product_id": product,
                        "region": wh_row["region"],
                        "units_demanded": units_demanded,
                        "units_fulfilled": units_fulfilled,
                        "units_lost_sales": units_lost_sales,
                        "stockout_flag": stockout_flag,
                        "promo_flag": promo_flag,
                        "seasonality_index": seasonality,
                    }
                )

                inventory_rows.append(
                    {
                        "snapshot_date": current_date,
                        "warehouse_id": wh_id,
                        "product_id": product,
                        "on_hand_units": int(on_hand),
                        "on_order_units": int(on_order_units),
                        "reserved_units": int(reserved_units),
                        "available_units": int(available_units),
                        "inventory_value": inventory_value,
                        "days_of_supply": days_of_supply,
                    }
                )

    demand_history = pd.DataFrame(demand_rows)
    inventory_snapshots = pd.DataFrame(inventory_rows)
    purchase_orders = pd.DataFrame(po_rows)
    purchase_orders = purchase_orders.loc[
        purchase_orders["actual_arrival_date"] <= pd.Timestamp(cfg.end_date)
    ].reset_index(drop=True)

    return demand_history, inventory_snapshots, purchase_orders


def write_raw_tables(
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    inventory_snapshots: pd.DataFrame,
    demand_history: pd.DataFrame,
    purchase_orders: pd.DataFrame,
    product_classification: pd.DataFrame,
    intervention_assignments: pd.DataFrame,
    network_nodes: pd.DataFrame,
    network_lanes: pd.DataFrame,
    product_sources: pd.DataFrame,
) -> None:
    """Persist required raw tables to /data/raw/."""
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    products.to_csv(DATA_RAW / "products.csv", index=False)
    suppliers.to_csv(DATA_RAW / "suppliers.csv", index=False)
    warehouses.to_csv(DATA_RAW / "warehouses.csv", index=False)
    inventory_snapshots.to_csv(DATA_RAW / "inventory_snapshots.csv", index=False)
    demand_history.to_csv(DATA_RAW / "demand_history.csv", index=False)
    purchase_orders.to_csv(DATA_RAW / "purchase_orders.csv", index=False)
    product_classification.to_csv(DATA_RAW / "product_classification.csv", index=False)
    intervention_assignments.to_csv(DATA_RAW / "intervention_assignments.csv", index=False)
    network_nodes.to_csv(DATA_RAW / "network_nodes.csv", index=False)
    network_lanes.to_csv(DATA_RAW / "network_lanes.csv", index=False)
    product_sources.to_csv(DATA_RAW / "product_sources.csv", index=False)


def print_summary(
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    warehouses: pd.DataFrame,
    inventory_snapshots: pd.DataFrame,
    demand_history: pd.DataFrame,
    purchase_orders: pd.DataFrame,
    product_classification: pd.DataFrame,
) -> None:
    """Print concise dataset quality and scale summary."""
    stockout_rate = demand_history["stockout_flag"].mean()
    fill_rate = demand_history["units_fulfilled"].sum() / max(
        1, demand_history["units_demanded"].sum()
    )

    demand_with_inventory = demand_history.merge(
        inventory_snapshots,
        left_on=["date", "warehouse_id", "product_id"],
        right_on=["snapshot_date", "warehouse_id", "product_id"],
        how="inner",
    )
    excess_flag = (demand_with_inventory["days_of_supply"] > 35).astype(int)

    late_rate = purchase_orders["late_delivery_flag"].mean() if not purchase_orders.empty else 0.0

    sku_dos = (
        inventory_snapshots.groupby("product_id", as_index=False)["days_of_supply"]
        .mean()
        .rename(columns={"days_of_supply": "avg_days_of_supply"})
    )
    sku_stockout = (
        demand_history.groupby("product_id", as_index=False)["stockout_flag"]
        .mean()
        .rename(columns={"stockout_flag": "stockout_day_rate"})
    )
    sku_behavior = sku_dos.merge(sku_stockout, on="product_id", how="inner")
    chronic_overstock_count = int((sku_behavior["avg_days_of_supply"] >= 35).sum())
    chronic_stockout_count = int((sku_behavior["stockout_day_rate"] >= 0.12).sum())

    print("Synthetic data generation complete.")
    print(
        f"Rows | products: {len(products):,}, suppliers: {len(suppliers):,}, warehouses: {len(warehouses):,}, "
        f"inventory_snapshots: {len(inventory_snapshots):,}, demand_history: {len(demand_history):,}, "
        f"purchase_orders: {len(purchase_orders):,}, product_classification: {len(product_classification):,}"
    )
    print(
        f"Health | fill_rate: {fill_rate:.3f}, stockout_day_rate: {stockout_rate:.3f}, "
        f"late_delivery_rate: {late_rate:.3f}, excess_inventory_day_rate: {excess_flag.mean():.3f}"
    )
    print(f"Coverage | {demand_history['date'].min()} to {demand_history['date'].max()}")
    print(
        "Signal | "
        f"chronic_overstock_skus: {chronic_overstock_count}, "
        f"chronic_stockout_skus: {chronic_stockout_count}"
    )


def generate_all_tables() -> None:
    cfg = SimulationConfig()
    rng = np.random.default_rng(cfg.seed)

    suppliers = build_suppliers(cfg, rng)
    warehouses = build_warehouses()
    products, product_classification, sim_attrs = build_products_and_classification(
        cfg, suppliers, rng
    )
    intervention_assignments = build_intervention_assignments(
        products,
        product_classification,
        warehouses,
        np.random.default_rng(cfg.seed + 10_001),
    )
    network_nodes, network_lanes, product_sources = build_network_tables(
        products,
        suppliers,
        warehouses,
        np.random.default_rng(cfg.seed + 20_003),
    )

    demand_history, inventory_snapshots, purchase_orders = simulate_operations(
        cfg=cfg,
        products=products,
        suppliers=suppliers,
        warehouses=warehouses,
        sim_attrs=sim_attrs,
        rng=rng,
        intervention_assignments=intervention_assignments,
    )

    write_raw_tables(
        products=products,
        suppliers=suppliers,
        warehouses=warehouses,
        inventory_snapshots=inventory_snapshots,
        demand_history=demand_history,
        purchase_orders=purchase_orders,
        product_classification=product_classification,
        intervention_assignments=intervention_assignments,
        network_nodes=network_nodes,
        network_lanes=network_lanes,
        product_sources=product_sources,
    )

    print_summary(
        products=products,
        suppliers=suppliers,
        warehouses=warehouses,
        inventory_snapshots=inventory_snapshots,
        demand_history=demand_history,
        purchase_orders=purchase_orders,
        product_classification=product_classification,
    )


if __name__ == "__main__":
    generate_all_tables()
