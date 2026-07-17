"""Arrival simulation invariants: supplier reliability controls the realised
on-time rate, arrival dates agree with late flags, and invalid inputs fail."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from src.data_generation import (
    SimulationConfig,
    build_intervention_assignments,
    build_network_tables,
    intervention_policy_levels,
    simulate_actual_arrival,
    simulate_operations,
    supplier_recovery_profile,
)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start_date": "2026-02-01", "end_date": "2026-01-01"},
        {"n_products": 0},
        {"n_suppliers": 0},
    ],
)
def test_simulation_config_rejects_invalid_ranges(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        SimulationConfig(**kwargs)


def test_supplier_reliability_controls_on_time_rate() -> None:
    rng = np.random.default_rng(42)
    current_date = pd.Timestamp("2026-01-01")
    reliability = 0.82

    arrivals = [
        simulate_actual_arrival(current_date, 14, reliability, 0.25, rng) for _ in range(10_000)
    ]
    on_time_rate = 1.0 - np.mean([late_flag for _, late_flag in arrivals])

    assert on_time_rate == pytest.approx(reliability, abs=0.02)


def test_arrival_date_and_late_flag_are_consistent() -> None:
    rng = np.random.default_rng(7)
    current_date = pd.Timestamp("2026-01-01")
    expected_arrival = current_date + pd.Timedelta(days=10)

    arrivals = [simulate_actual_arrival(current_date, 10, 0.70, 0.30, rng) for _ in range(1_000)]

    assert all((actual > expected_arrival) == bool(late_flag) for actual, late_flag in arrivals)


@pytest.mark.parametrize(
    ("lead_time", "reliability", "variability"),
    [(0, 0.8, 0.2), (10, -0.1, 0.2), (10, 1.1, 0.2), (10, 0.8, -0.1)],
)
def test_arrival_simulation_rejects_invalid_inputs(
    lead_time: int,
    reliability: float,
    variability: float,
) -> None:
    with pytest.raises(ValueError):
        simulate_actual_arrival(
            pd.Timestamp("2026-01-01"),
            lead_time,
            reliability,
            variability,
            np.random.default_rng(1),
        )


def test_open_order_position_uses_ordered_not_future_received_quantity() -> None:
    cfg = SimulationConfig(start_date="2026-01-01", end_date="2026-03-31", n_products=1)
    products = pd.DataFrame(
        [
            {
                "product_id": "SKU-0001",
                "category": "Beverages",
                "supplier_id": "SUP-001",
                "lead_time_days": 7,
                "unit_cost": 5.0,
            }
        ]
    )
    suppliers = pd.DataFrame(
        [
            {
                "supplier_id": "SUP-001",
                "reliability_score": 0.65,
                "lead_time_variability": 0.30,
                "minimum_order_qty": 120,
            }
        ]
    )
    warehouses = pd.DataFrame([{"warehouse_id": "WH-LIS", "region": "Portugal South"}])
    sim_attrs = pd.DataFrame(
        [
            {
                "product_id": "SKU-0001",
                "base_daily_demand": 35.0,
                "demand_cv": 0.20,
                "target_cover_days": 14,
                "planning_bias": 1.0,
                "chronic_profile": "normal",
            }
        ]
    )

    _, inventory, purchase_orders = simulate_operations(
        cfg,
        products,
        suppliers,
        warehouses,
        sim_attrs,
        np.random.default_rng(9),
    )
    underfilled = purchase_orders[
        purchase_orders["received_units"] < purchase_orders["ordered_units"]
    ]
    assert not underfilled.empty

    order_date = underfilled.iloc[0]["order_date"]
    open_orders = purchase_orders[
        (purchase_orders["order_date"] <= order_date)
        & (purchase_orders["actual_arrival_date"] > order_date)
    ]
    snapshot = inventory.loc[inventory["snapshot_date"] == order_date].iloc[0]

    assert snapshot["on_order_units"] == open_orders["ordered_units"].sum()
    assert snapshot["on_order_units"] != open_orders["received_units"].sum()


def _pilot_masters() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    products = pd.DataFrame(
        [
            {
                "product_id": f"SKU-{idx:04d}",
                "category": ["Beverages", "Health"][idx % 2],
                "supplier_id": f"SUP-{(idx % 3) + 1:03d}",
                "unit_cost": 4.0 + idx / 10,
                "lead_time_days": 7 + idx % 3,
            }
            for idx in range(1, 25)
        ]
    )
    classifications = pd.DataFrame(
        {
            "product_id": products["product_id"],
            "abc_class": ["A", "B", "C"] * 8,
            "criticality_level": ["High", "Medium", "Low"] * 8,
        }
    )
    warehouses = pd.DataFrame(
        [
            {
                "warehouse_id": warehouse_id,
                "region": region,
                "storage_capacity_units": capacity,
            }
            for warehouse_id, region, capacity in [
                ("WH-LIS", "Portugal South", 100_000),
                ("WH-PORTO", "Portugal North", 80_000),
                ("WH-MAD", "Spain Central", 90_000),
                ("WH-LYON", "France South-East", 120_000),
            ]
        ]
    )
    return products, classifications, warehouses


def test_intervention_assignment_is_pre_outcome_stratified_and_deterministic() -> None:
    products, classifications, warehouses = _pilot_masters()

    first = build_intervention_assignments(
        products,
        classifications,
        warehouses,
        np.random.default_rng(20260714),
    )
    second = build_intervention_assignments(
        products,
        classifications,
        warehouses,
        np.random.default_rng(20260714),
    )

    pd.testing.assert_frame_equal(first, second)
    assert not first.duplicated(["experiment_id", "unit_id"]).any()
    assert (
        pd.to_datetime(first["assignment_date"]) < pd.to_datetime(first["intervention_date"])
    ).all()
    rct = first[first["design"] == "randomized_controlled_trial"]
    balance = rct.groupby("stratum")["treatment_flag"].agg(["sum", "count"])
    assert ((2 * balance["sum"] - balance["count"]).abs() <= 1).all()
    assert set(rct["treatment_group"]) == {"business_as_usual", "policy_reset"}

    did = first[first["design"] == "difference_in_differences"]
    assert set(did.loc[did["treatment_flag"] == 1, "supplier_id"]) == {"SUP-002"}
    assert set(did.loc[did["treatment_flag"] == 0, "supplier_id"]) == {
        "SUP-001",
        "SUP-003",
    }


def test_operational_treatments_activate_only_on_or_after_intervention() -> None:
    intervention_date = pd.Timestamp("2025-07-01")

    assert intervention_policy_levels(100, 160, True, intervention_date, intervention_date) == (
        120,
        184,
    )
    assert intervention_policy_levels(
        100, 160, True, intervention_date - pd.Timedelta(days=1), intervention_date
    ) == (100, 160)
    assert intervention_policy_levels(100, 160, False, intervention_date, intervention_date) == (
        100,
        160,
    )
    assert supplier_recovery_profile(0.74, 0.42, True) == pytest.approx((0.92, 0.21))
    assert supplier_recovery_profile(0.74, 0.42, False) == pytest.approx((0.74, 0.42))


def test_network_tables_define_constrained_multi_echelon_routes() -> None:
    products, _, warehouses = _pilot_masters()
    suppliers = pd.DataFrame(
        [
            {
                "supplier_id": f"SUP-{idx:03d}",
                "supplier_region": "EU",
                "minimum_order_qty": 100 * idx,
                "average_lead_time_days": 5 + idx,
                "reliability_score": 0.8 + idx / 20,
            }
            for idx in range(1, 4)
        ]
    )

    nodes, lanes, sources = build_network_tables(
        products,
        suppliers,
        warehouses,
        np.random.default_rng(19),
    )

    assert not nodes["node_id"].duplicated().any()
    assert set(nodes["node_type"]) == {"supplier", "gateway", "regional_dc"}
    assert not lanes["lane_id"].duplicated().any()
    assert (lanes["daily_capacity_units"] > 0).all()
    assert (lanes["unit_transport_cost"] >= 0).all()
    assert set(lanes.loc[lanes["lane_type"] == "transfer", "source_node_id"]) == {"WH-LYON"}
    assert sources.groupby("product_id").size().eq(2).all()
    assert sources.groupby("product_id")["is_primary"].sum().eq(1).all()
    assert (sources["minimum_order_qty"] > 0).all()
    assert (sources["max_horizon_units"] >= sources["minimum_order_qty"]).all()


def test_randomized_policy_assignment_changes_post_intervention_replenishment() -> None:
    cfg = SimulationConfig(start_date="2025-06-01", end_date="2025-10-31", n_products=1)
    products = pd.DataFrame(
        [
            {
                "product_id": "SKU-0001",
                "category": "Beverages",
                "supplier_id": "SUP-001",
                "lead_time_days": 10,
                "unit_cost": 5.0,
            }
        ]
    )
    suppliers = pd.DataFrame(
        [
            {
                "supplier_id": "SUP-001",
                "reliability_score": 0.82,
                "lead_time_variability": 0.25,
                "minimum_order_qty": 120,
            }
        ]
    )
    warehouses = pd.DataFrame([{"warehouse_id": "WH-LIS", "region": "Portugal South"}])
    sim_attrs = pd.DataFrame(
        [
            {
                "product_id": "SKU-0001",
                "base_daily_demand": 45.0,
                "demand_cv": 0.25,
                "target_cover_days": 12,
                "planning_bias": 0.9,
                "chronic_profile": "normal",
            }
        ]
    )
    assignment = pd.DataFrame(
        [
            {
                "experiment_id": "EXP-RCT-001",
                "unit_id": "SKU-0001|WH-LIS",
                "supplier_id": "SUP-001",
                "treatment_flag": 1,
                "intervention_date": "2025-07-01",
            }
        ]
    )

    treated_demand, _, treated_orders = simulate_operations(
        cfg,
        products,
        suppliers,
        warehouses,
        sim_attrs,
        np.random.default_rng(112),
        intervention_assignments=assignment,
    )
    control_demand, _, control_orders = simulate_operations(
        cfg,
        products,
        suppliers,
        warehouses,
        sim_attrs,
        np.random.default_rng(112),
        intervention_assignments=assignment.assign(treatment_flag=0),
    )

    assert treated_orders["ordered_units"].sum() > control_orders["ordered_units"].sum()
    assert treated_demand["units_lost_sales"].sum() <= control_demand["units_lost_sales"].sum()
