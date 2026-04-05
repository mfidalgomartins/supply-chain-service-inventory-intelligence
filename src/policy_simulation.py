from __future__ import annotations

from dataclasses import dataclass
from math import erf

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


@dataclass(frozen=True)
class PolicyScenario:
    scenario_name: str
    reorder_z: float
    cycle_stock_days: float


SCENARIOS: tuple[PolicyScenario, ...] = (
    PolicyScenario("Lean Cash", reorder_z=0.8, cycle_stock_days=7.0),
    PolicyScenario("Balanced Baseline", reorder_z=1.2, cycle_stock_days=10.0),
    PolicyScenario("Service Protect", reorder_z=1.7, cycle_stock_days=14.0),
    PolicyScenario("Peak Guard", reorder_z=2.1, cycle_stock_days=18.0),
)


def _normal_cdf(x: np.ndarray | float) -> np.ndarray | float:
    arr = np.asarray(x, dtype=float)
    erf_vec = np.vectorize(erf)
    return 0.5 * (1.0 + erf_vec(arr / np.sqrt(2.0)))


def _build_baseline() -> pd.DataFrame:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    products = pd.read_csv(DATA_RAW / "products.csv")[
        ["product_id", "unit_cost", "unit_price", "lead_time_days", "target_service_level"]
    ]

    cutoff = daily["date"].max() - pd.Timedelta(days=119)
    recent = daily[daily["date"] >= cutoff].copy()

    baseline = (
        recent.groupby(["product_id", "warehouse_id", "supplier_id", "category", "abc_class"], as_index=False)
        .agg(
            demand_mean=("units_demanded", "mean"),
            demand_std=("units_demanded", "std"),
            baseline_fill_rate=("fill_rate", "mean"),
            baseline_days_of_supply=("days_of_supply", "mean"),
            baseline_inventory_value=("inventory_value", "mean"),
            baseline_lost_sales=("lost_sales_revenue", "sum"),
        )
        .merge(products, on="product_id", how="left")
    )

    forecast_path = OUTPUT_TABLES_DIR / "demand_forecast_lane_summary.csv"
    if forecast_path.exists():
        forecast = pd.read_csv(forecast_path)[
            [
                "product_id",
                "warehouse_id",
                "supplier_id",
                "category",
                "abc_class",
                "forecast_daily_mean",
                "forecast_daily_std",
            ]
        ]
        baseline = baseline.merge(
            forecast,
            on=["product_id", "warehouse_id", "supplier_id", "category", "abc_class"],
            how="left",
        )
        baseline["forecast_enriched_flag"] = baseline["forecast_daily_mean"].notna().astype(int)
        baseline["demand_mean"] = np.where(
            baseline["forecast_daily_mean"].notna(),
            baseline["forecast_daily_mean"],
            baseline["demand_mean"],
        )
        baseline["demand_std"] = np.where(
            baseline["forecast_daily_std"].notna(),
            baseline["forecast_daily_std"],
            baseline["demand_std"],
        )
    else:
        baseline["forecast_enriched_flag"] = 0

    baseline["demand_mean"] = baseline["demand_mean"].clip(lower=0.1)
    baseline["demand_std"] = baseline["demand_std"].fillna(baseline["demand_mean"] * 0.35).clip(lower=0.1)
    baseline["lead_time_days"] = baseline["lead_time_days"].fillna(14).clip(lower=1)
    baseline["target_service_level"] = baseline["target_service_level"].fillna(0.95).clip(0.70, 0.999)

    baseline["annual_demand_units"] = baseline["demand_mean"] * 365.0
    return baseline


def _simulate_scenario(base: pd.DataFrame, scenario: PolicyScenario) -> pd.DataFrame:
    out = base.copy()

    lt = out["lead_time_days"].to_numpy(dtype=float)
    mean = out["demand_mean"].to_numpy(dtype=float)
    std = out["demand_std"].to_numpy(dtype=float)

    sigma_lt = np.maximum(0.1, std * np.sqrt(lt))
    safety_stock = scenario.reorder_z * sigma_lt

    reorder_point = mean * lt + safety_stock
    order_up_to = reorder_point + mean * scenario.cycle_stock_days
    expected_on_hand_units = np.maximum(order_up_to - (mean * lt * 0.50), 0.0)

    z = (reorder_point - mean * lt) / np.maximum(sigma_lt, 1e-9)
    estimated_service_level = np.clip(_normal_cdf(z), 0.01, 0.999)

    expected_dos = expected_on_hand_units / np.maximum(mean, 1e-9)
    expected_inventory_value = expected_on_hand_units * out["unit_cost"].to_numpy(dtype=float)

    annual_demand = out["annual_demand_units"].to_numpy(dtype=float)
    expected_lost_units = annual_demand * (1.0 - estimated_service_level)
    expected_lost_sales = expected_lost_units * out["unit_price"].to_numpy(dtype=float)

    out["scenario_name"] = scenario.scenario_name
    out["reorder_z"] = scenario.reorder_z
    out["cycle_stock_days"] = scenario.cycle_stock_days
    out["reorder_point_units"] = reorder_point
    out["order_up_to_units"] = order_up_to
    out["estimated_service_level"] = estimated_service_level
    out["expected_days_of_supply"] = expected_dos
    out["expected_inventory_value"] = expected_inventory_value
    out["expected_lost_sales_value_annual"] = expected_lost_sales
    out["service_delta_vs_baseline"] = out["estimated_service_level"] - out["baseline_fill_rate"]
    out["capital_delta_vs_baseline"] = out["expected_inventory_value"] - out["baseline_inventory_value"]

    return out


def _build_frontier(scenario_results: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        scenario_results.groupby(["scenario_name", "reorder_z", "cycle_stock_days"], as_index=False)
        .agg(
            weighted_service_num=("estimated_service_level", lambda s: float(np.sum(s * scenario_results.loc[s.index, "annual_demand_units"]))),
            weighted_service_den=("annual_demand_units", "sum"),
            total_expected_inventory_value=("expected_inventory_value", "sum"),
            total_expected_lost_sales_annual=("expected_lost_sales_value_annual", "sum"),
            baseline_inventory_value=("baseline_inventory_value", "sum"),
            baseline_lost_sales=("baseline_lost_sales", "sum"),
        )
    )

    grouped["weighted_service_level"] = grouped["weighted_service_num"] / np.maximum(grouped["weighted_service_den"], 1e-9)
    grouped.drop(columns=["weighted_service_num", "weighted_service_den"], inplace=True)

    grouped["inventory_delta_vs_baseline"] = grouped["total_expected_inventory_value"] - grouped["baseline_inventory_value"]
    grouped["lost_sales_delta_vs_baseline"] = grouped["total_expected_lost_sales_annual"] - grouped["baseline_lost_sales"]

    grouped = grouped.sort_values("total_expected_inventory_value").reset_index(drop=True)

    best_service = -1.0
    pareto_flags: list[int] = []
    for row in grouped.itertuples(index=False):
        if row.weighted_service_level > best_service + 1e-9:
            pareto_flags.append(1)
            best_service = row.weighted_service_level
        else:
            pareto_flags.append(0)

    grouped["pareto_frontier_flag"] = pareto_flags
    return grouped


def _plot_frontier(frontier: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(10, 6))
    plot = frontier.copy()

    sns.scatterplot(
        data=plot,
        x="total_expected_inventory_value",
        y="weighted_service_level",
        size="total_expected_lost_sales_annual",
        sizes=(200, 1200),
        hue="pareto_frontier_flag",
        palette={0: "#6B7280", 1: "#0D5C7A"},
        legend=False,
    )

    for row in plot.itertuples(index=False):
        plt.text(
            row.total_expected_inventory_value,
            row.weighted_service_level,
            row.scenario_name,
            fontsize=9,
            ha="center",
            va="bottom",
        )

    plt.title("Policy Frontier: Service Level vs Inventory Value Across Reorder Policies")
    plt.xlabel("Total Expected Inventory Value")
    plt.ylabel("Weighted Service Level")
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1%}"))
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "policy_frontier_service_vs_inventory.png", dpi=180)
    plt.close()


def _write_summary(frontier: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    best_service = frontier.sort_values("weighted_service_level", ascending=False).iloc[0]
    min_capital = frontier.sort_values("total_expected_inventory_value", ascending=True).iloc[0]
    best_balanced = frontier.assign(
        balance_score=(frontier["weighted_service_level"] * 100.0) - (frontier["total_expected_inventory_value"] / frontier["total_expected_inventory_value"].max() * 35.0)
    ).sort_values("balance_score", ascending=False).iloc[0]

    lines = [
        "# Policy Simulation Summary",
        "",
        "This simulation estimates service-vs-capital outcomes under alternative reorder policies using forecasted demand and lead-time assumptions.",
        "",
        "## Decision Readout",
        f"- Highest service policy: **{best_service['scenario_name']}** at **{best_service['weighted_service_level']:.2%}** weighted service level.",
        f"- Lowest capital policy: **{min_capital['scenario_name']}** with expected inventory value **EUR {min_capital['total_expected_inventory_value']:,.0f}**.",
        f"- Best balanced policy (service-capital score): **{best_balanced['scenario_name']}**.",
        "",
        "## Interpretation",
        "- Policies with higher safety stock improve service but increase working-capital intensity.",
        "- Leaner policies reduce inventory but increase projected service leakage.",
        "- Leadership should select policy by segment (not one-size-fits-all), using criticality and margin protection priorities.",
        "",
        "## Caveat",
        "- This is an analytical simulation layer for policy trade-off planning, not an execution engine replacing MRP/APS logic.",
    ]

    (OUTPUT_REPORTS_DIR / "policy_simulation_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_policy_simulation() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    baseline = _build_baseline()
    scenario_frames = [_simulate_scenario(baseline, s) for s in SCENARIOS]
    scenario_results = pd.concat(scenario_frames, ignore_index=True)

    frontier = _build_frontier(scenario_results)

    scenario_results.to_csv(OUTPUT_TABLES_DIR / "policy_simulation_sku_scenarios.csv", index=False)
    frontier.to_csv(OUTPUT_TABLES_DIR / "policy_simulation_frontier.csv", index=False)

    _plot_frontier(frontier)
    _write_summary(frontier)

    print("Policy simulation complete.")
    print(f"SKU scenario rows: {len(scenario_results):,}")
    print(f"Frontier scenarios: {len(frontier):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")
    print(f"Chart written to: {OUTPUT_CHARTS_DIR / 'policy_frontier_service_vs_inventory.png'}")


if __name__ == "__main__":
    run_policy_simulation()
