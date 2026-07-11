"""Impact aggregation semantics: balance metrics (inventory, trapped working
capital) must be averaged across dates, and the overall summary must never
annualise a balance as if it were a flow."""

from __future__ import annotations

import pandas as pd
import pytest
from src.impact_analysis import aggregate_impact, build_overall_summary


def _impact_daily() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"]),
            "category": ["A", "A", "A", "A"],
            "units_demanded": [10, 20, 10, 20],
            "units_lost_sales": [1, 2, 1, 2],
            "lost_sales_revenue": [10.0, 20.0, 10.0, 20.0],
            "lost_sales_margin_proxy": [4.0, 8.0, 4.0, 8.0],
            "supplier_delay_impact_proxy": [2.0, 4.0, 2.0, 4.0],
            "excess_inventory_value_proxy": [10.0, 30.0, 20.0, 40.0],
            "trapped_working_capital_proxy": [15.0, 35.0, 25.0, 45.0],
            "slow_moving_value_proxy": [20.0, 40.0, 30.0, 50.0],
        }
    )


def test_aggregate_impact_averages_balance_metrics_across_dates() -> None:
    out = aggregate_impact(_impact_daily(), ["category"]).iloc[0]

    assert out["trapped_working_capital_proxy_average"] == pytest.approx(60.0)
    assert out["excess_inventory_value_proxy_average"] == pytest.approx(50.0)
    assert out["opportunity_wc_release_12m_proxy"] == pytest.approx(15.0)


def test_overall_summary_does_not_annualize_working_capital_balance() -> None:
    summary = build_overall_summary(_impact_daily(), annualization_factor=365 / 2)
    values = dict(zip(summary["metric"], summary["value"], strict=False))

    assert values["trapped_working_capital_proxy_average"] == pytest.approx(60.0)
    assert values["opportunity_wc_release_12m_proxy"] == pytest.approx(15.0)
    assert "trapped_working_capital_proxy_annualized" not in values
