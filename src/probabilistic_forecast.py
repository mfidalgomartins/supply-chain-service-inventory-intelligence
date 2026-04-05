from __future__ import annotations

from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    from src.config import DATA_PROCESSED, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


FORECAST_HORIZON_DAYS = 30
HISTORY_DAYS = 180
MIN_HISTORY_DAYS = 60
Z10 = -1.2815515655446004
Z90 = 1.2815515655446004


@dataclass(frozen=True)
class LaneForecastConfig:
    ewma_span: int = 28
    residual_floor_rate: float = 0.18


def _load() -> pd.DataFrame:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    cols = [
        "date",
        "product_id",
        "warehouse_id",
        "supplier_id",
        "category",
        "abc_class",
        "units_demanded",
    ]
    return daily[cols].copy()


def _forecast_one_lane(lane: pd.DataFrame, cfg: LaneForecastConfig) -> tuple[pd.DataFrame, dict]:
    lane = lane.sort_values("date").copy()
    lane = lane.iloc[-HISTORY_DAYS:] if len(lane) > HISTORY_DAYS else lane

    if len(lane) < MIN_HISTORY_DAYS:
        return pd.DataFrame(), {}

    demand = lane["units_demanded"].astype(float).clip(lower=0.0)

    # Robust base-level estimate with exponentially weighted smoothing.
    level = demand.ewm(span=cfg.ewma_span, adjust=False).mean()
    level_last = float(max(level.iloc[-1], 0.1))

    dow_stats = lane.assign(dow=lane["date"].dt.dayofweek).groupby("dow", as_index=False).agg(avg_demand=("units_demanded", "mean"))
    overall_avg = float(max(demand.mean(), 0.1))
    dow_stats["dow_factor"] = (dow_stats["avg_demand"] / overall_avg).clip(lower=0.5, upper=1.8)
    dow_factor_map = {int(r.dow): float(r.dow_factor) for r in dow_stats.itertuples(index=False)}

    expected_hist = level.shift(1).fillna(level.iloc[0])
    residual = demand - expected_hist
    residual_std = float(np.nan_to_num(residual.std(ddof=1), nan=overall_avg * cfg.residual_floor_rate))
    residual_std = max(residual_std, overall_avg * cfg.residual_floor_rate, 0.25)

    last_date = lane["date"].max()
    horizon_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=FORECAST_HORIZON_DAYS, freq="D")

    rows: list[dict] = []
    for i, fc_date in enumerate(horizon_dates, start=1):
        dow = int(fc_date.dayofweek)
        dow_factor = dow_factor_map.get(dow, 1.0)

        mean = max(level_last * dow_factor, 0.05)
        sigma = residual_std * np.sqrt(1.0 + i / 7.0)

        q10 = max(0.0, mean + Z10 * sigma)
        q50 = max(0.0, mean)
        q90 = max(q50, mean + Z90 * sigma)

        rows.append(
            {
                "forecast_date": fc_date,
                "days_ahead": i,
                "forecast_q10": q10,
                "forecast_q50": q50,
                "forecast_q90": q90,
                "forecast_mean": q50,
                "forecast_sigma": sigma,
            }
        )

    lane_key = {
        "product_id": lane["product_id"].iloc[0],
        "warehouse_id": lane["warehouse_id"].iloc[0],
        "supplier_id": lane["supplier_id"].iloc[0],
        "category": lane["category"].iloc[0],
        "abc_class": lane["abc_class"].iloc[0],
    }

    forecast_df = pd.DataFrame(rows)
    for k, v in lane_key.items():
        forecast_df[k] = v

    summary = {
        **lane_key,
        "history_days_used": int(len(lane)),
        "historical_daily_mean": float(demand.mean()),
        "historical_daily_std": float(demand.std(ddof=1)),
        "forecast_daily_mean": float(forecast_df["forecast_q50"].mean()),
        "forecast_daily_std": float(forecast_df["forecast_sigma"].mean()),
        "forecast_30d_q10": float(forecast_df["forecast_q10"].sum()),
        "forecast_30d_q50": float(forecast_df["forecast_q50"].sum()),
        "forecast_30d_q90": float(forecast_df["forecast_q90"].sum()),
        "forecast_cv": float((forecast_df["forecast_sigma"] / forecast_df["forecast_mean"].clip(lower=0.1)).mean()),
        "uncertainty_band_rate": float((forecast_df["forecast_q90"].sum() - forecast_df["forecast_q10"].sum()) / max(forecast_df["forecast_q50"].sum(), 0.1)),
        "forecast_model": "ewma_dow_normal",
    }

    return forecast_df, summary


def _run_forecast(daily: pd.DataFrame, cfg: LaneForecastConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    group_cols = ["product_id", "warehouse_id", "supplier_id", "category", "abc_class"]
    daily_rows: list[pd.DataFrame] = []
    summary_rows: list[dict] = []

    for _, lane in daily.groupby(group_cols, sort=False):
        fcast, summary = _forecast_one_lane(lane, cfg)
        if not fcast.empty:
            daily_rows.append(fcast)
            summary_rows.append(summary)

    forecast_daily = pd.concat(daily_rows, ignore_index=True) if daily_rows else pd.DataFrame()
    forecast_summary = pd.DataFrame(summary_rows).sort_values("uncertainty_band_rate", ascending=False)

    return forecast_daily, forecast_summary


def _plot(summary: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    plot = summary.copy()
    plot["uncertainty_exposure"] = plot["uncertainty_band_rate"] * plot["forecast_30d_q50"]
    plot = plot.sort_values("uncertainty_exposure", ascending=False).head(20)
    plot["lane"] = plot["product_id"] + " | " + plot["warehouse_id"]

    plt.figure(figsize=(12, 8))
    sns.barplot(data=plot, y="lane", x="uncertainty_exposure", hue="abc_class", dodge=False, palette="Blues")
    plt.title("Forecast Upgrade: Lanes with Highest Demand Uncertainty Exposure (30D)")
    plt.xlabel("Uncertainty Exposure Proxy (Band Rate x Forecast Volume)")
    plt.ylabel("Product | Warehouse")
    plt.legend(title="ABC", loc="lower right")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "forecast_uncertainty_top_lanes.png", dpi=180)
    plt.close()


def _write_summary(summary: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if summary.empty:
        lines = [
            "# Probabilistic Forecast Summary",
            "",
            "No lanes met minimum history requirements for probabilistic forecast generation.",
        ]
    else:
        top = summary.sort_values("uncertainty_band_rate", ascending=False).iloc[0]
        lines = [
            "# Probabilistic Forecast Summary",
            "",
            "This layer upgrades lane demand inputs from static rolling means to a probabilistic forecast distribution used by policy simulation.",
            "",
            "## Highest Uncertainty Lane",
            f"- Lane: **{top['product_id']} | {top['warehouse_id']}**",
            f"- Forecast 30D p50 demand: **{top['forecast_30d_q50']:.1f} units**",
            f"- Forecast uncertainty band rate: **{top['uncertainty_band_rate']:.2f}**",
            f"- Forecast CV proxy: **{top['forecast_cv']:.2f}**",
            "",
            "## Governance Use",
            "- Use p50 demand for base planning and p90 demand to stress safety-stock policy in critical lanes.",
            "- Lanes with high uncertainty band and high commercial exposure should receive tighter planning cadence.",
        ]

    (OUTPUT_REPORTS_DIR / "probabilistic_forecast_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_probabilistic_forecast() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    daily = _load()
    cfg = LaneForecastConfig()
    forecast_daily, forecast_summary = _run_forecast(daily, cfg)

    forecast_daily.to_csv(OUTPUT_TABLES_DIR / "demand_forecast_lane_daily.csv", index=False)
    forecast_summary.to_csv(OUTPUT_TABLES_DIR / "demand_forecast_lane_summary.csv", index=False)

    if not forecast_summary.empty:
        _plot(forecast_summary)

    _write_summary(forecast_summary)

    print("Probabilistic demand forecast complete.")
    print(f"Forecast daily rows: {len(forecast_daily):,}")
    print(f"Forecast summary rows: {len(forecast_summary):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_probabilistic_forecast()
