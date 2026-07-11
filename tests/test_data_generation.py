"""Arrival simulation invariants: supplier reliability must control the
realised on-time rate, arrival dates must agree with late flags, and invalid
inputs must be rejected rather than silently coerced."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from src.data_generation import simulate_actual_arrival


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
