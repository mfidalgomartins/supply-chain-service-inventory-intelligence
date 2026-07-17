"""Monte Carlo runs are deterministic, bounded, and flow-conserving."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from src.inventory_policy import PolicyParameters
from src.monte_carlo import _moving_block_bootstrap, simulate_monte_carlo_policy


def _simulation() -> pd.DataFrame:
    return simulate_monte_carlo_policy(
        demand_history=np.array([5, 8, 10, 12, 7, 9]),
        lead_time_history=np.array([3, 4, 5]),
        starting_on_hand=20,
        mean_daily_demand=8.5,
        demand_std=2.0,
        service_z=1.64,
        minimum_order_qty=20,
        unit_cost=3.0,
        unit_margin=2.0,
        annual_holding_cost_rate=0.18,
        ordering_cost=25.0,
        policy=PolicyParameters("test", 1.0, 10),
        simulations=20,
        warmup_days=10,
        horizon_days=30,
        seed=123,
    )


def test_monte_carlo_is_deterministic_and_conserves_flow() -> None:
    first = _simulation()
    second = _simulation()

    pd.testing.assert_frame_equal(first, second)
    assert (first["balance_error_units"] == 0).all()
    assert ((first["fill_rate"] >= 0) & (first["fill_rate"] <= 1)).all()
    assert (first["units_fulfilled"] + first["units_lost"] == first["units_demanded"]).all()


def test_moving_block_bootstrap_preserves_within_block_order() -> None:
    history = np.arange(10)
    sampled = _moving_block_bootstrap(
        np.random.default_rng(7), history, simulations=3, total_days=12, block_days=4
    )

    assert sampled.shape == (3, 12)
    for row in sampled:
        for start in range(0, 12, 4):
            assert (np.diff(row[start : start + 4]) % len(history) == 1).all()


@pytest.mark.parametrize(
    ("demand_history", "lead_time_history", "simulations"),
    [
        (np.array([]), np.array([1, 2]), 2),
        (np.array([1, 2]), np.array([0]), 2),
        (np.array([1, 2]), np.array([1, 2]), 0),
    ],
)
def test_monte_carlo_rejects_invalid_histories_and_run_sizes(
    demand_history: np.ndarray,
    lead_time_history: np.ndarray,
    simulations: int,
) -> None:
    with pytest.raises(ValueError):
        simulate_monte_carlo_policy(
            demand_history=demand_history,
            lead_time_history=lead_time_history,
            starting_on_hand=2,
            mean_daily_demand=1.5,
            demand_std=0.5,
            service_z=1.64,
            minimum_order_qty=1,
            unit_cost=1.0,
            unit_margin=1.0,
            annual_holding_cost_rate=0.18,
            ordering_cost=1.0,
            policy=PolicyParameters("test", 1.0, 2),
            simulations=simulations,
            warmup_days=2,
            horizon_days=2,
            seed=1,
        )
