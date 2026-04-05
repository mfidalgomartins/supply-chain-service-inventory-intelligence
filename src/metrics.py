from __future__ import annotations

import numpy as np
import pandas as pd


def safe_divide(numerator: pd.Series | float, denominator: pd.Series | float) -> pd.Series | float:
    """Safely divide values while preserving numeric semantics and avoiding inf."""
    if isinstance(numerator, pd.Series) or isinstance(denominator, pd.Series):
        num = numerator if isinstance(numerator, pd.Series) else pd.Series(numerator, index=denominator.index)
        den = denominator if isinstance(denominator, pd.Series) else pd.Series(denominator, index=numerator.index)
        return np.where(den == 0, 0.0, num / den)
    return 0.0 if denominator == 0 else numerator / denominator


def min_max_scale(series: pd.Series) -> pd.Series:
    minimum = series.min()
    maximum = series.max()
    if maximum == minimum:
        return pd.Series(0.5, index=series.index)
    return (series - minimum) / (maximum - minimum)


def compute_risk_score(df: pd.DataFrame) -> pd.Series:
    """Interpretable composite risk score for service vs inventory imbalance."""
    stockout_component = min_max_scale(df["stockout_day_rate"])
    excess_component = min_max_scale(df["excess_inventory_rate"])
    dual_failure_component = min_max_scale(df["stockout_day_rate"] * df["excess_inventory_rate"])
    slow_move_component = min_max_scale(df["slow_moving_rate"])
    lead_time_risk_component = min_max_scale(df["lead_time_cv"])
    demand_volatility_component = min_max_scale(df["demand_cv"])

    score = (
        0.35 * stockout_component
        + 0.35 * excess_component
        + 0.15 * dual_failure_component
        + 0.05 * slow_move_component
        + 0.05 * lead_time_risk_component
        + 0.05 * demand_volatility_component
    )
    return (score * 100).round(2)


def classify_risk(score: float) -> str:
    if score >= 50:
        return "Critical"
    if score >= 35:
        return "High"
    if score >= 20:
        return "Medium"
    return "Low"
