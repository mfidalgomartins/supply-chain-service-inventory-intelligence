from __future__ import annotations

import pandas as pd

from src.metrics import classify_risk, compute_risk_score, min_max_scale


def test_min_max_scale_constant_series_returns_half():
    s = pd.Series([5, 5, 5])
    scaled = min_max_scale(s)
    assert (scaled == 0.5).all()


def test_risk_score_higher_for_worse_profile():
    df = pd.DataFrame(
        {
            "stockout_day_rate": [0.02, 0.30],
            "service_level": [0.98, 0.78],
            "excess_inventory_rate": [0.05, 0.35],
            "slow_moving_rate": [0.03, 0.28],
            "lead_time_cv": [0.10, 0.45],
            "demand_cv": [0.15, 0.50],
        }
    )
    scores = compute_risk_score(df)
    assert scores.iloc[1] > scores.iloc[0]


def test_classify_risk_thresholds():
    assert classify_risk(10) == "Low"
    assert classify_risk(25) == "Medium"
    assert classify_risk(40) == "High"
    assert classify_risk(60) == "Critical"
