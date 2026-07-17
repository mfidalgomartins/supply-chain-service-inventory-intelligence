"""Inventory-policy simulation invariants and economic trade-offs."""

from __future__ import annotations

import numpy as np
import pytest
from src.inventory_policy import PolicyParameters, policy_levels, simulate_policy


def test_policy_levels_increase_with_safety_and_cycle_stock() -> None:
    lean = PolicyParameters("lean", 0.75, 7)
    resilient = PolicyParameters("resilient", 1.25, 21)

    lean_levels = policy_levels(20.0, 5.0, 10, 1.64, lean)
    resilient_levels = policy_levels(20.0, 5.0, 10, 1.64, resilient)

    assert resilient_levels[0] > lean_levels[0]
    assert resilient_levels[1] > lean_levels[1]


def test_policy_simulation_conserves_inventory_flow() -> None:
    result = simulate_policy(
        demand=np.array([8, 12, 10, 14, 9, 11] * 10),
        starting_on_hand=40,
        mean_daily_demand=10.0,
        demand_std=2.0,
        lead_time_days=5,
        service_z=1.64,
        minimum_order_qty=30,
        unit_cost=4.0,
        unit_margin=3.0,
        annual_holding_cost_rate=0.18,
        ordering_cost=35.0,
        policy=PolicyParameters("balanced", 1.0, 14),
    )

    assert result.balance_error_units == 0
    assert result.units_fulfilled + result.units_lost == result.units_demanded
    assert 0 <= result.fill_rate <= 1
    assert result.economic_cost_proxy == (
        result.lost_margin_proxy + result.holding_cost_proxy + result.ordering_cost_proxy
    )


def test_policy_warmup_initializes_state_but_is_excluded_from_kpis() -> None:
    result = simulate_policy(
        demand=np.array([20, 20, 2, 3, 4]),
        starting_on_hand=50,
        mean_daily_demand=3.0,
        demand_std=1.0,
        lead_time_days=2,
        service_z=1.64,
        minimum_order_qty=5,
        unit_cost=2.0,
        unit_margin=1.0,
        annual_holding_cost_rate=0.18,
        ordering_cost=10.0,
        policy=PolicyParameters("warmup", 1.0, 3),
        warmup_days=2,
    )

    assert result.units_demanded == 9
    assert result.units_fulfilled + result.units_lost == 9
    assert (
        result.evaluation_starting_on_hand + result.received_units - result.units_fulfilled
        == result.ending_on_hand
    )
    assert result.balance_error_units == 0


@pytest.mark.parametrize(
    ("mean_demand", "demand_std", "lead_time", "policy"),
    [
        (-1.0, 1.0, 5, PolicyParameters("invalid", 1.0, 7)),
        (1.0, 1.0, 0, PolicyParameters("invalid", 1.0, 7)),
        (1.0, 1.0, 5, PolicyParameters("invalid", 0.0, 7)),
    ],
)
def test_policy_levels_reject_invalid_inputs(mean_demand, demand_std, lead_time, policy) -> None:
    with pytest.raises(ValueError):
        policy_levels(mean_demand, demand_std, lead_time, 1.64, policy)


@pytest.mark.parametrize(
    ("demand", "ordering_cost"),
    [(np.array([]), 1.0), (np.array([-1]), 1.0), (np.array([1, 2]), -1.0)],
)
def test_policy_simulation_rejects_invalid_inputs(demand: np.ndarray, ordering_cost: float) -> None:
    with pytest.raises(ValueError):
        simulate_policy(
            demand=demand,
            starting_on_hand=2,
            mean_daily_demand=1.5,
            demand_std=0.5,
            lead_time_days=2,
            service_z=1.64,
            minimum_order_qty=1,
            unit_cost=1.0,
            unit_margin=1.0,
            annual_holding_cost_rate=0.18,
            ordering_cost=ordering_cost,
            policy=PolicyParameters("test", 1.0, 2),
        )


def test_policy_simulation_rejects_warmup_without_evaluation_days() -> None:
    with pytest.raises(ValueError, match="warmup_days"):
        simulate_policy(
            demand=np.array([1, 2]),
            starting_on_hand=2,
            mean_daily_demand=1.5,
            demand_std=0.5,
            lead_time_days=2,
            service_z=1.64,
            minimum_order_qty=1,
            unit_cost=1.0,
            unit_margin=1.0,
            annual_holding_cost_rate=0.18,
            ordering_cost=1.0,
            policy=PolicyParameters("test", 1.0, 2),
            warmup_days=2,
        )
