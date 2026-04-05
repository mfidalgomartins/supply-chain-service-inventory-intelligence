from __future__ import annotations

from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT, RANDOM_SEED
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT, RANDOM_SEED


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


@dataclass(frozen=True)
class StressConfig:
    iterations: int = 600
    lanes_to_simulate: int = 180
    lookback_days: int = 180


CFG = StressConfig()


def _prepare_lane_baseline() -> pd.DataFrame:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    products = pd.read_csv(DATA_RAW / "products.csv")[["product_id", "lead_time_days", "unit_price"]]
    suppliers = pd.read_csv(DATA_RAW / "suppliers.csv")[["supplier_id", "lead_time_variability"]]

    cutoff = daily["date"].max() - pd.Timedelta(days=CFG.lookback_days - 1)
    recent = daily[daily["date"] >= cutoff].copy()

    lane = (
        recent.groupby(["supplier_id", "warehouse_id", "category", "product_id"], as_index=False)
        .agg(
            demand_mean=("units_demanded", "mean"),
            demand_std=("units_demanded", "std"),
            avg_available_units=("available_units", "mean"),
            avg_on_order_units=("on_order_units", "mean"),
            baseline_fill_rate=("fill_rate", "mean"),
            baseline_stockout_rate=("stockout_flag", "mean"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
            units_demanded=("units_demanded", "sum"),
        )
        .merge(products, on="product_id", how="left")
        .merge(suppliers, on="supplier_id", how="left")
    )

    lane["demand_mean"] = lane["demand_mean"].clip(lower=0.2)
    lane["demand_std"] = lane["demand_std"].fillna(lane["demand_mean"] * 0.35).clip(lower=0.3)
    lane["lead_time_days"] = lane["lead_time_days"].fillna(14).clip(lower=1)
    lane["lead_time_variability"] = lane["lead_time_variability"].fillna(0.25).clip(lower=0.05)

    lane["available_position"] = (lane["avg_available_units"] + lane["avg_on_order_units"]).clip(lower=1.0)

    lane = lane.sort_values(["lost_sales_revenue", "units_demanded"], ascending=[False, False]).head(CFG.lanes_to_simulate)
    return lane.reset_index(drop=True)


def _simulate_lane(row: pd.Series, rng: np.random.Generator) -> dict[str, float]:
    mean_d = float(row["demand_mean"])
    std_d = float(row["demand_std"])
    mean_lt = float(row["lead_time_days"])
    lt_var = float(row["lead_time_variability"])
    buffer_units = float(row["available_position"])

    lt_std = max(1.0, mean_lt * lt_var)

    sampled_lt = np.clip(rng.normal(mean_lt, lt_std, CFG.iterations), 1.0, 75.0)
    demand_during_lt = rng.normal(mean_d * sampled_lt, std_d * np.sqrt(sampled_lt), CFG.iterations)
    demand_during_lt = np.clip(demand_during_lt, 0.0, None)

    stockout = demand_during_lt > buffer_units
    shortage_units = np.clip(demand_during_lt - buffer_units, 0.0, None)

    service = np.ones_like(demand_during_lt, dtype=float)
    positive_demand = demand_during_lt > 0
    service[positive_demand] = np.minimum(buffer_units / demand_during_lt[positive_demand], 1.0)
    end_units = np.clip(buffer_units - demand_during_lt, 0.0, None)
    end_dos = end_units / max(mean_d, 1e-9)

    severe_event = (stockout.astype(int) + (end_dos > 55).astype(int)) >= 1

    return {
        "prob_stockout": float(stockout.mean()),
        "expected_service_level": float(service.mean()),
        "p05_service": float(np.quantile(service, 0.05)),
        "p50_service": float(np.quantile(service, 0.50)),
        "p95_demand_during_lt": float(np.quantile(demand_during_lt, 0.95)),
        "expected_shortage_units": float(shortage_units.mean()),
        "expected_end_dos": float(end_dos.mean()),
        "severe_event_probability": float(severe_event.mean()),
    }


def _run_monte_carlo(lanes: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RANDOM_SEED + 17)
    sim_rows: list[dict] = []

    for _, row in lanes.iterrows():
        sim = _simulate_lane(row, rng)
        sim_rows.append({
            **row.to_dict(),
            **sim,
        })

    out = pd.DataFrame(sim_rows)

    out["stress_risk_score"] = (
        100.0
        * (
            0.45 * out["prob_stockout"].clip(0, 1)
            + 0.25 * (1.0 - out["expected_service_level"]).clip(0, 1)
            + 0.20 * out["severe_event_probability"].clip(0, 1)
            + 0.10 * (out["expected_shortage_units"] / np.maximum(out["p95_demand_during_lt"], 1e-9)).clip(0, 1)
        )
    ).clip(0, 100)

    return out.sort_values(["stress_risk_score", "lost_sales_revenue"], ascending=[False, False]).reset_index(drop=True)


def _aggregate_segments(stress: pd.DataFrame) -> pd.DataFrame:
    seg = (
        stress.groupby(["supplier_id", "warehouse_id", "category"], as_index=False)
        .agg(
            lane_count=("product_id", "count"),
            avg_prob_stockout=("prob_stockout", "mean"),
            avg_expected_service=("expected_service_level", "mean"),
            avg_severe_event_probability=("severe_event_probability", "mean"),
            avg_stress_risk_score=("stress_risk_score", "mean"),
            total_lost_sales_revenue=("lost_sales_revenue", "sum"),
            total_units_demanded=("units_demanded", "sum"),
        )
    )

    seg["segment_priority_proxy"] = (
        seg["avg_stress_risk_score"]
        * np.log1p(seg["total_lost_sales_revenue"]).clip(lower=1)
        / np.log1p(seg["total_lost_sales_revenue"]).max()
    )

    return seg.sort_values(["segment_priority_proxy", "total_lost_sales_revenue"], ascending=[False, False])


def _create_charts(stress: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    top = stress.head(20).copy()
    top["lane_id"] = top["product_id"] + " | " + top["warehouse_id"] + " | " + top["supplier_id"]

    plt.figure(figsize=(12, 8))
    sns.barplot(data=top, y="lane_id", x="prob_stockout", color="#9B2C2C")
    plt.xlabel("Simulated Stockout Probability")
    plt.ylabel("Supplier | Warehouse | SKU")
    plt.title("Monte Carlo Stress Test: Top Lane Stockout Probabilities")
    plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "stress_monte_carlo_top_lane_stockout_probability.png", dpi=180)
    plt.close()

    plt.figure(figsize=(10, 6))
    sns.histplot(stress["expected_service_level"], bins=25, color="#1F4E79")
    plt.xlabel("Expected Service Level Under Stress")
    plt.ylabel("Lane Count")
    plt.title("Monte Carlo Stress Test: Lane Service Distribution Under Demand and Lead-Time Uncertainty")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "stress_monte_carlo_service_distribution.png", dpi=180)
    plt.close()


def _write_summary(stress: pd.DataFrame, segment: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    top_lane = stress.iloc[0]
    top_seg = segment.iloc[0]

    lines = [
        "# Monte Carlo Stress Test Summary",
        "",
        "Demand and lead-time uncertainty were simulated at lane level to estimate downside service risk and intervention priority under volatility.",
        "",
        "## Key Results",
        f"- Lanes simulated: **{len(stress):,}**.",
        f"- Highest-stress lane: **{top_lane['supplier_id']} | {top_lane['warehouse_id']} | {top_lane['product_id']}**.",
        f"- This lane has simulated stockout probability **{top_lane['prob_stockout']:.1%}** and expected service **{top_lane['expected_service_level']:.1%}**.",
        f"- Highest-stress segment: **{top_seg['supplier_id']} | {top_seg['warehouse_id']} | {top_seg['category']}** with priority proxy **{top_seg['segment_priority_proxy']:.2f}**.",
        "",
        "## Interpretation",
        "- Lanes with high stockout probability and high lost-sales exposure should receive immediate safety-stock and supplier-reliability interventions.",
        "- Segment-level stress rankings provide a practical weekly exception list for S&OE and procurement governance.",
    ]

    (OUTPUT_REPORTS_DIR / "monte_carlo_stress_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_monte_carlo_stress() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    lanes = _prepare_lane_baseline()
    stress = _run_monte_carlo(lanes)
    segment = _aggregate_segments(stress)

    stress.to_csv(OUTPUT_TABLES_DIR / "stress_monte_carlo_lane_results.csv", index=False)
    segment.to_csv(OUTPUT_TABLES_DIR / "stress_monte_carlo_segment_results.csv", index=False)

    _create_charts(stress)
    _write_summary(stress, segment)

    print("Monte Carlo stress testing complete.")
    print(f"Lane rows: {len(stress):,}")
    print(f"Segment rows: {len(segment):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_monte_carlo_stress()
