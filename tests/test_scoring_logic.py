"""Scoring primitives: tier boundaries at exactly 35/55/75, linear scores
clipped to 0-100, and Low-tier entities mapped to monitor-only actions."""

from __future__ import annotations

import pandas as pd
import pytest
from src.scoring import assign_risk_tier, linear_score, prepare_daily_input, recommended_action


def test_assign_risk_tier_boundaries():
    scores = pd.Series([35.0, 35.1, 55.0, 55.1, 75.0, 75.1])
    tiers = assign_risk_tier(scores).tolist()
    assert tiers == ["Low", "Medium", "Medium", "High", "High", "Critical"]


def test_linear_score_clips_to_0_100():
    vals = pd.Series([-1.0, 0.0, 0.5, 1.0, 2.0])
    out = linear_score(vals, good=0.0, bad=1.0)
    assert out.tolist() == [0.0, 0.0, 50.0, 100.0, 100.0]


def test_recommended_action_low_tier_is_monitor_only():
    assert recommended_action("Service Risk", "Low", "sku_warehouse") == "monitor only"


def test_recommended_action_rejects_unsupported_routes() -> None:
    with pytest.raises(ValueError, match="Unsupported action route"):
        recommended_action("Unknown Risk", "High", "sku_warehouse")


def test_zero_demand_rows_have_zero_weight() -> None:
    daily = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02"],
            "abc_class": ["A", "C"],
            "criticality_level": ["High", "Low"],
            "units_demanded": [0, 10],
            "available_units": [10, 10],
            "units_fulfilled": [0, 10],
            "days_of_supply": [20.0, 20.0],
            "supplier_id": ["SUP-001", "SUP-001"],
        }
    )
    supplier_scores = pd.DataFrame({"supplier_id": ["SUP-001"], "supplier_risk_score_base": [40.0]})

    prepared = prepare_daily_input(daily, supplier_scores)

    assert prepared.loc[0, "demand_weight"] == 0
    assert prepared.loc[0, "weighted_criticality"] == 0
    assert prepared.loc[0, "weighted_dos_cap"] == 0
