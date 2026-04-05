from __future__ import annotations

from dataclasses import dataclass
from itertools import product

import matplotlib.pyplot as plt
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
class SensitivityConfig:
    margin_rates: tuple[float, ...] = (0.20, 0.30, 0.35, 0.40, 0.50)
    wc_rates: tuple[float, ...] = (0.15, 0.20, 0.25, 0.30, 0.35)
    slow_weights: tuple[float, ...] = (0.30, 0.50, 0.70)


CFG = SensitivityConfig()


def _build_base_components() -> tuple[float, float, float, int]:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    products = pd.read_csv(DATA_RAW / "products.csv")[["product_id", "unit_cost", "unit_price"]]

    margin = products.copy()
    margin["gross_margin_rate"] = (
        (margin["unit_price"] - margin["unit_cost"]) / margin["unit_price"].where(margin["unit_price"] > 0, 1.0)
    ).clip(lower=0, upper=0.90)

    df = daily.merge(margin[["product_id", "gross_margin_rate"]], on="product_id", how="left")
    df["gross_margin_rate"] = df["gross_margin_rate"].fillna(0.30)

    df["dos_cap"] = df["abc_class"].map({"A": 20.0, "B": 30.0}).fillna(45.0)
    df["excess_inventory_proxy"] = (
        df["inventory_value"] * ((df["days_of_supply"] - df["dos_cap"]).clip(lower=0.0) / df["days_of_supply"].clip(lower=1e-9))
    )
    df["slow_moving_proxy"] = ((df["available_units"] > 0) & (df["units_fulfilled"] == 0)).astype(int) * df["inventory_value"]
    df["slow_non_excess_proxy"] = (df["slow_moving_proxy"] - df["excess_inventory_proxy"]).clip(lower=0.0)
    df["lost_sales_margin_proxy"] = df["lost_sales_revenue"] * df["gross_margin_rate"]

    days = int(df["date"].nunique())
    annualization_factor = 365.0 / max(days, 1)

    annual_lost_margin = float(df["lost_sales_margin_proxy"].sum() * annualization_factor)
    annual_excess = float(df["excess_inventory_proxy"].sum() * annualization_factor)
    annual_slow_non_excess = float(df["slow_non_excess_proxy"].sum() * annualization_factor)

    return annual_lost_margin, annual_excess, annual_slow_non_excess, days


def _run_scenarios(annual_lost_margin: float, annual_excess: float, annual_slow_non_excess: float) -> pd.DataFrame:
    rows: list[dict] = []

    for margin_rate, wc_rate, slow_weight in product(CFG.margin_rates, CFG.wc_rates, CFG.slow_weights):
        trapped_wc = annual_excess + slow_weight * annual_slow_non_excess
        opportunity_margin = annual_lost_margin * margin_rate
        opportunity_wc = trapped_wc * wc_rate
        total_opp = opportunity_margin + opportunity_wc

        rows.append(
            {
                "recoverable_margin_rate": margin_rate,
                "releasable_wc_rate": wc_rate,
                "slow_moving_incremental_weight": slow_weight,
                "annual_lost_sales_margin_proxy": annual_lost_margin,
                "annual_excess_inventory_proxy": annual_excess,
                "annual_slow_non_excess_proxy": annual_slow_non_excess,
                "annual_trapped_wc_proxy_scenario": trapped_wc,
                "opportunity_margin_recovery_12m_proxy": opportunity_margin,
                "opportunity_wc_release_12m_proxy": opportunity_wc,
                "opportunity_total_12m_proxy": total_opp,
            }
        )

    out = pd.DataFrame(rows).sort_values("opportunity_total_12m_proxy", ascending=False).reset_index(drop=True)
    out["scenario_rank"] = out.index + 1
    return out


def _build_tornado(scenarios: pd.DataFrame) -> pd.DataFrame:
    factors = ["recoverable_margin_rate", "releasable_wc_rate", "slow_moving_incremental_weight"]
    rows: list[dict] = []

    for factor in factors:
        grouped = scenarios.groupby(factor, as_index=False)["opportunity_total_12m_proxy"].mean()
        low = grouped.iloc[0]
        high = grouped.iloc[-1]

        rows.append(
            {
                "factor": factor,
                "low_setting": float(low[factor]),
                "high_setting": float(high[factor]),
                "low_opportunity": float(low["opportunity_total_12m_proxy"]),
                "high_opportunity": float(high["opportunity_total_12m_proxy"]),
                "swing": float(high["opportunity_total_12m_proxy"] - low["opportunity_total_12m_proxy"]),
            }
        )

    return pd.DataFrame(rows).sort_values("swing", ascending=False)


def _plot(scenarios: pd.DataFrame, tornado: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    heat = (
        scenarios[scenarios["slow_moving_incremental_weight"] == 0.50]
        .pivot(index="recoverable_margin_rate", columns="releasable_wc_rate", values="opportunity_total_12m_proxy")
        .sort_index(ascending=True)
    )

    plt.figure(figsize=(9, 6))
    sns.heatmap(heat, cmap="YlGnBu", annot=True, fmt=",.0f", cbar_kws={"label": "12M Opportunity Proxy"})
    plt.title("Assumption Sensitivity Heatmap (Slow-Moving Weight = 50%)")
    plt.xlabel("Releasable Working Capital Rate")
    plt.ylabel("Recoverable Lost Margin Rate")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "sensitivity_opportunity_heatmap.png", dpi=180)
    plt.close()

    plt.figure(figsize=(9, 4.8))
    sns.barplot(data=tornado, y="factor", x="swing", color="#0B5C78")
    plt.title("Assumption Influence on 12M Opportunity Proxy (High - Low)")
    plt.xlabel("Opportunity Swing")
    plt.ylabel("Assumption Factor")
    plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"EUR {x:,.0f}"))
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "sensitivity_opportunity_tornado.png", dpi=180)
    plt.close()


def _write_summary(scenarios: pd.DataFrame, tornado: pd.DataFrame, analysis_days: int) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    best = scenarios.iloc[0]
    worst = scenarios.iloc[-1]
    top_factor = tornado.iloc[0]

    lines = [
        "# Opportunity Sensitivity Summary",
        "",
        "This analysis stress-tests the financial opportunity proxy under alternate assumption settings to quantify model sensitivity and governance robustness.",
        "",
        "## Scope",
        f"- Analysis window: **{analysis_days} days**.",
        f"- Scenarios tested: **{len(scenarios)}** (margin x WC x slow-moving weight grid).",
        "",
        "## Range of Outcomes",
        f"- Highest scenario: **EUR {best['opportunity_total_12m_proxy']:,.0f}** "
        f"(margin={best['recoverable_margin_rate']:.0%}, wc={best['releasable_wc_rate']:.0%}, slow_w={best['slow_moving_incremental_weight']:.0%}).",
        f"- Lowest scenario: **EUR {worst['opportunity_total_12m_proxy']:,.0f}** "
        f"(margin={worst['recoverable_margin_rate']:.0%}, wc={worst['releasable_wc_rate']:.0%}, slow_w={worst['slow_moving_incremental_weight']:.0%}).",
        "",
        "## Dominant Assumption Driver",
        f"- Largest modeled swing comes from **{top_factor['factor']}** with approx. **EUR {top_factor['swing']:,.0f}** spread across tested bounds.",
        "",
        "## Governance Use",
        "- Use baseline assumptions for default prioritization, but include sensitivity range in executive communication to avoid false precision.",
        "- Escalate decisions where business cases only hold under optimistic parameter settings.",
    ]

    (OUTPUT_REPORTS_DIR / "sensitivity_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_sensitivity_analysis() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    annual_lost_margin, annual_excess, annual_slow_non_excess, analysis_days = _build_base_components()
    scenarios = _run_scenarios(annual_lost_margin, annual_excess, annual_slow_non_excess)
    tornado = _build_tornado(scenarios)

    scenarios.to_csv(OUTPUT_TABLES_DIR / "sensitivity_opportunity_grid.csv", index=False)
    tornado.to_csv(OUTPUT_TABLES_DIR / "sensitivity_opportunity_tornado.csv", index=False)

    _plot(scenarios, tornado)
    _write_summary(scenarios, tornado, analysis_days)

    print("Sensitivity analysis complete.")
    print(f"Scenarios: {len(scenarios):,}")
    print(f"Factors: {len(tornado):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_sensitivity_analysis()
