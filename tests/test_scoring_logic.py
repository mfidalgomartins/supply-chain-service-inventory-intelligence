from __future__ import annotations

import pandas as pd

from src.scoring import assign_risk_tier, linear_score, recommended_action


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
