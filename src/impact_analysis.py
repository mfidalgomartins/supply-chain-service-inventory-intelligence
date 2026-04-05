from __future__ import annotations

from dataclasses import dataclass

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
DOCS_DIR = PROJECT_ROOT / "docs"


@dataclass(frozen=True)
class ImpactAssumptions:
    dos_cap_a: float = 20.0
    dos_cap_b: float = 30.0
    dos_cap_c: float = 45.0
    slow_moving_incremental_weight: float = 0.50
    recoverable_lost_margin_rate_12m: float = 0.35
    releasable_trapped_wc_rate_12m: float = 0.25
    supplier_delay_weight_otd_gap: float = 0.45
    supplier_delay_weight_avg_delay: float = 0.35
    supplier_delay_weight_lt_variability: float = 0.20
    supplier_delay_norm_days: float = 7.0
    supplier_lt_var_norm: float = 10.0


ASSUMPTIONS = ImpactAssumptions()


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    products = pd.read_csv(DATA_RAW / "products.csv")
    suppliers = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")
    return daily, products, suppliers


def build_supplier_delay_factor(suppliers: pd.DataFrame) -> pd.DataFrame:
    out = suppliers.copy()

    otd_gap = (1.0 - out["on_time_delivery_rate"]).clip(0, 1)
    avg_delay_norm = (out["average_delay_days"] / ASSUMPTIONS.supplier_delay_norm_days).clip(0, 1)
    lt_var_norm = (out["lead_time_variability"] / ASSUMPTIONS.supplier_lt_var_norm).clip(0, 1)

    out["supplier_delay_factor"] = (
        ASSUMPTIONS.supplier_delay_weight_otd_gap * otd_gap
        + ASSUMPTIONS.supplier_delay_weight_avg_delay * avg_delay_norm
        + ASSUMPTIONS.supplier_delay_weight_lt_variability * lt_var_norm
    ).clip(0, 1)

    return out[["supplier_id", "supplier_name", "supplier_delay_factor", "on_time_delivery_rate", "average_delay_days", "lead_time_variability"]]


def enrich_daily(daily: pd.DataFrame, products: pd.DataFrame, suppliers: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    margin = products[["product_id", "product_name", "unit_cost", "unit_price"]].copy()
    margin["unit_gross_margin"] = (margin["unit_price"] - margin["unit_cost"]).clip(lower=0)
    margin["gross_margin_rate"] = np.where(
        margin["unit_price"] > 0,
        margin["unit_gross_margin"] / margin["unit_price"],
        0.0,
    )

    supplier_delay = build_supplier_delay_factor(suppliers)

    out = daily.merge(margin[["product_id", "product_name", "gross_margin_rate"]], on="product_id", how="left")
    out = out.merge(supplier_delay[["supplier_id", "supplier_delay_factor"]], on="supplier_id", how="left")

    out["gross_margin_rate"] = out["gross_margin_rate"].fillna(0.30).clip(0, 0.90)
    out["supplier_delay_factor"] = out["supplier_delay_factor"].fillna(0.25)

    out["dos_cap"] = np.select(
        [out["abc_class"] == "A", out["abc_class"] == "B"],
        [ASSUMPTIONS.dos_cap_a, ASSUMPTIONS.dos_cap_b],
        default=ASSUMPTIONS.dos_cap_c,
    )

    out["excess_dos_units"] = (out["days_of_supply"] - out["dos_cap"]).clip(lower=0)
    out["excess_inventory_ratio"] = np.where(
        out["days_of_supply"] > 0,
        out["excess_dos_units"] / out["days_of_supply"],
        0.0,
    ).clip(0, 1)

    out["excess_inventory_value_proxy"] = out["inventory_value"] * out["excess_inventory_ratio"]

    out["slow_moving_flag"] = ((out["available_units"] > 0) & (out["units_fulfilled"] == 0)).astype(int)
    out["slow_moving_value_proxy"] = out["inventory_value"] * out["slow_moving_flag"]

    # Avoid full double counting between excess and slow-moving exposures.
    out["slow_moving_non_excess_proxy"] = (out["slow_moving_value_proxy"] - out["excess_inventory_value_proxy"]).clip(lower=0)
    out["trapped_working_capital_proxy"] = (
        out["excess_inventory_value_proxy"]
        + ASSUMPTIONS.slow_moving_incremental_weight * out["slow_moving_non_excess_proxy"]
    )

    out["lost_sales_margin_proxy"] = out["lost_sales_revenue"] * out["gross_margin_rate"]
    out["supplier_delay_impact_proxy"] = out["lost_sales_revenue"] * out["supplier_delay_factor"]

    analysis_days = int(out["date"].nunique())
    annualization_factor = 365.0 / max(analysis_days, 1)

    out["lost_sales_revenue_annualized"] = out["lost_sales_revenue"] * annualization_factor
    out["lost_sales_margin_proxy_annualized"] = out["lost_sales_margin_proxy"] * annualization_factor
    out["excess_inventory_value_proxy_annualized"] = out["excess_inventory_value_proxy"] * annualization_factor
    out["trapped_working_capital_proxy_annualized"] = out["trapped_working_capital_proxy"] * annualization_factor
    out["slow_moving_value_proxy_annualized"] = out["slow_moving_value_proxy"] * annualization_factor
    out["supplier_delay_impact_proxy_annualized"] = out["supplier_delay_impact_proxy"] * annualization_factor

    out["opportunity_margin_recovery_12m_proxy"] = (
        out["lost_sales_margin_proxy_annualized"] * ASSUMPTIONS.recoverable_lost_margin_rate_12m
    )
    out["opportunity_wc_release_12m_proxy"] = (
        out["trapped_working_capital_proxy_annualized"] * ASSUMPTIONS.releasable_trapped_wc_rate_12m
    )
    out["opportunity_total_12m_proxy"] = (
        out["opportunity_margin_recovery_12m_proxy"] + out["opportunity_wc_release_12m_proxy"]
    )

    return out, annualization_factor


def aggregate_impact(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    out = (
        df.groupby(group_cols, as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_lost_sales=("units_lost_sales", "sum"),
            lost_sales_revenue_observed=("lost_sales_revenue", "sum"),
            lost_sales_margin_proxy_observed=("lost_sales_margin_proxy", "sum"),
            excess_inventory_value_proxy_observed=("excess_inventory_value_proxy", "sum"),
            trapped_working_capital_proxy_observed=("trapped_working_capital_proxy", "sum"),
            slow_moving_value_proxy_observed=("slow_moving_value_proxy", "sum"),
            supplier_delay_impact_proxy_observed=("supplier_delay_impact_proxy", "sum"),
            lost_sales_revenue_annualized=("lost_sales_revenue_annualized", "sum"),
            lost_sales_margin_proxy_annualized=("lost_sales_margin_proxy_annualized", "sum"),
            excess_inventory_value_proxy_annualized=("excess_inventory_value_proxy_annualized", "sum"),
            trapped_working_capital_proxy_annualized=("trapped_working_capital_proxy_annualized", "sum"),
            slow_moving_value_proxy_annualized=("slow_moving_value_proxy_annualized", "sum"),
            supplier_delay_impact_proxy_annualized=("supplier_delay_impact_proxy_annualized", "sum"),
            opportunity_margin_recovery_12m_proxy=("opportunity_margin_recovery_12m_proxy", "sum"),
            opportunity_wc_release_12m_proxy=("opportunity_wc_release_12m_proxy", "sum"),
            opportunity_total_12m_proxy=("opportunity_total_12m_proxy", "sum"),
        )
    )

    out["stockout_rate"] = np.where(
        out["units_demanded"] > 0,
        out["units_lost_sales"] / out["units_demanded"],
        0.0,
    )

    total_opp = float(out["opportunity_total_12m_proxy"].sum())
    out["opportunity_share"] = np.where(total_opp > 0, out["opportunity_total_12m_proxy"] / total_opp, 0.0)
    out["opportunity_rank"] = out["opportunity_total_12m_proxy"].rank(method="first", ascending=False).astype(int)

    return out.sort_values("opportunity_total_12m_proxy", ascending=False)


def build_overall_summary(df: pd.DataFrame, annualization_factor: float) -> pd.DataFrame:
    observed = {
        "lost_sales_revenue_observed": df["lost_sales_revenue"].sum(),
        "lost_sales_margin_proxy_observed": df["lost_sales_margin_proxy"].sum(),
        "excess_inventory_value_proxy_observed": df["excess_inventory_value_proxy"].sum(),
        "trapped_working_capital_proxy_observed": df["trapped_working_capital_proxy"].sum(),
        "slow_moving_value_proxy_observed": df["slow_moving_value_proxy"].sum(),
        "supplier_delay_impact_proxy_observed": df["supplier_delay_impact_proxy"].sum(),
    }

    annualized = {
        k.replace("_observed", "_annualized"): v * annualization_factor for k, v in observed.items()
    }

    opportunity = {
        "opportunity_margin_recovery_12m_proxy": annualized["lost_sales_margin_proxy_annualized"]
        * ASSUMPTIONS.recoverable_lost_margin_rate_12m,
        "opportunity_wc_release_12m_proxy": annualized["trapped_working_capital_proxy_annualized"]
        * ASSUMPTIONS.releasable_trapped_wc_rate_12m,
    }
    opportunity["opportunity_total_12m_proxy"] = (
        opportunity["opportunity_margin_recovery_12m_proxy"] + opportunity["opportunity_wc_release_12m_proxy"]
    )

    rows = [
        ("analysis_days", float(df["date"].nunique()), "Observed period length (days)"),
        ("annualization_factor", annualization_factor, "365 / analysis_days"),
        (
            "lost_sales_revenue_observed",
            observed["lost_sales_revenue_observed"],
            "Observed lost sales value from stockout unmet demand",
        ),
        (
            "excess_inventory_value_proxy_observed",
            observed["excess_inventory_value_proxy_observed"],
            "Proxy value of inventory above ABC DOS policy caps",
        ),
        (
            "trapped_working_capital_proxy_observed",
            observed["trapped_working_capital_proxy_observed"],
            "Proxy of inefficient inventory capital (excess + incremental slow-moving)",
        ),
        (
            "slow_moving_value_proxy_observed",
            observed["slow_moving_value_proxy_observed"],
            "Observed inventory value in slow-moving days",
        ),
        (
            "supplier_delay_impact_proxy_observed",
            observed["supplier_delay_impact_proxy_observed"],
            "Proxy lost-sales value associated with supplier delay severity",
        ),
        (
            "lost_sales_revenue_annualized",
            annualized["lost_sales_revenue_annualized"],
            "Annualized observed lost sales value from stockouts",
        ),
        (
            "excess_inventory_value_proxy_annualized",
            annualized["excess_inventory_value_proxy_annualized"],
            "Annualized proxy value of inventory above DOS policy caps",
        ),
        (
            "trapped_working_capital_proxy_annualized",
            annualized["trapped_working_capital_proxy_annualized"],
            "Annualized inefficient working-capital proxy",
        ),
        (
            "slow_moving_value_proxy_annualized",
            annualized["slow_moving_value_proxy_annualized"],
            "Annualized slow-moving inventory value proxy",
        ),
        (
            "supplier_delay_impact_proxy_annualized",
            annualized["supplier_delay_impact_proxy_annualized"],
            "Annualized supplier delay impact proxy",
        ),
        (
            "opportunity_total_12m_proxy",
            opportunity["opportunity_total_12m_proxy"],
            "Estimated 12-month value pool: margin recovery + WC release",
        ),
    ]

    return pd.DataFrame(rows, columns=["metric", "value", "definition"])


def build_opportunity_priority_view(
    sku: pd.DataFrame,
    warehouse: pd.DataFrame,
    supplier: pd.DataFrame,
    category: pd.DataFrame,
) -> pd.DataFrame:
    def top_block(
        df: pd.DataFrame,
        entity_type: str,
        id_col: str,
        name_col: str | None = None,
        extra_id_col: str | None = None,
    ) -> pd.DataFrame:
        top = df.nsmallest(15, "opportunity_rank").copy()
        top["entity_type"] = entity_type
        if extra_id_col:
            top["entity_id"] = top[id_col].astype(str) + "|" + top[extra_id_col].astype(str)
            if name_col:
                top["entity_name"] = top[name_col].astype(str) + " @ " + top[extra_id_col].astype(str)
            else:
                top["entity_name"] = top["entity_id"]
        else:
            top["entity_id"] = top[id_col]
            top["entity_name"] = top[name_col] if name_col else top[id_col]
        return top[
            [
                "entity_type",
                "entity_id",
                "entity_name",
                "opportunity_rank",
                "opportunity_total_12m_proxy",
                "opportunity_margin_recovery_12m_proxy",
                "opportunity_wc_release_12m_proxy",
                "lost_sales_revenue_annualized",
                "trapped_working_capital_proxy_annualized",
                "supplier_delay_impact_proxy_annualized",
                "opportunity_share",
            ]
        ]

    blocks = [
        top_block(sku, "SKU", "product_id", "product_name", extra_id_col="warehouse_id"),
        top_block(warehouse, "Warehouse", "warehouse_id", "warehouse_name"),
        top_block(supplier, "Supplier", "supplier_id", "supplier_name"),
        top_block(category, "Category", "category", "category"),
    ]

    return pd.concat(blocks, ignore_index=True).sort_values(
        ["entity_type", "opportunity_rank"], ascending=[True, True]
    )


def create_charts(
    overall: pd.DataFrame,
    sku: pd.DataFrame,
    warehouse: pd.DataFrame,
    supplier: pd.DataFrame,
    category: pd.DataFrame,
) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    overall_plot = overall[overall["metric"].str.contains("_annualized|opportunity_total_12m_proxy")].copy()
    overall_plot = overall_plot[~overall_plot["metric"].eq("annualization_factor")]

    plt.figure(figsize=(10, 5))
    sns.barplot(data=overall_plot, x="metric", y="value", color="#2F855A")
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("EUR")
    plt.title("Annualized Financial Exposure and 12M Opportunity Proxy")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "impact_01_overall_exposure.png", dpi=180)
    plt.close()

    top_sku = sku.head(15).copy()
    top_sku["sku_label"] = top_sku["product_id"] + " | " + top_sku["warehouse_id"]
    plt.figure(figsize=(11, 7))
    sns.barplot(data=top_sku, y="sku_label", x="opportunity_total_12m_proxy", color="#1F4E79")
    plt.xlabel("12M Opportunity Proxy (EUR)")
    plt.ylabel("SKU | Warehouse")
    plt.title("Top SKU-Warehouse Opportunities by Estimated 12M Value")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "impact_02_top_sku_opportunity.png", dpi=180)
    plt.close()

    plt.figure(figsize=(9, 5))
    sns.barplot(data=warehouse, x="warehouse_name", y="opportunity_total_12m_proxy", color="#C05621")
    plt.xticks(rotation=20, ha="right")
    plt.ylabel("12M Opportunity Proxy (EUR)")
    plt.title("Warehouse Opportunity Prioritization")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "impact_03_warehouse_opportunity.png", dpi=180)
    plt.close()

    top_supplier = supplier.head(12).copy()
    plt.figure(figsize=(10, 6))
    sns.barplot(data=top_supplier, y="supplier_name", x="supplier_delay_impact_proxy_annualized", color="#7B341E")
    plt.xlabel("Annualized Supplier Delay Impact Proxy (EUR)")
    plt.ylabel("Supplier")
    plt.title("Supplier Delay Impact Proxy (Lost-Sales Linkage)")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "impact_04_supplier_delay_proxy.png", dpi=180)
    plt.close()

    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=category,
        x="excess_inventory_value_proxy_annualized",
        y="lost_sales_revenue_annualized",
        size="opportunity_total_12m_proxy",
        sizes=(80, 800),
        legend=False,
        color="#2D3748",
    )
    for row in category.itertuples(index=False):
        plt.text(row.excess_inventory_value_proxy_annualized, row.lost_sales_revenue_annualized, row.category, fontsize=8)
    plt.xlabel("Annualized Excess Inventory Value Proxy (EUR)")
    plt.ylabel("Annualized Lost Sales Revenue (EUR)")
    plt.title("Category Trade-off: Overstock Exposure vs Service Failure")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "impact_05_category_tradeoff_scatter.png", dpi=180)
    plt.close()


def write_assumptions_log(annualization_factor: float) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Impact Assumptions Log",
        "",
        "This document separates observed metrics from proxy estimates used for business-impact prioritization.",
        "",
        "## Observed Metrics",
        "- Lost sales revenue: directly observed from `daily_product_warehouse_metrics.lost_sales_revenue`.",
        "- Inventory value: directly observed from `daily_product_warehouse_metrics.inventory_value`.",
        "- Slow-moving days: observed when `available_units > 0` and `units_fulfilled = 0`.",
        "",
        "## Proxy Formulas",
        "- Excess inventory value proxy: `inventory_value * max(days_of_supply - dos_cap, 0) / max(days_of_supply, 1)` where DOS caps are A=20, B=30, C=45 days.",
        "- Trapped working-capital proxy: `excess_inventory_value_proxy + 0.50 * max(slow_moving_value_proxy - excess_inventory_value_proxy, 0)`.",
        "- Lost sales margin proxy: `lost_sales_revenue * gross_margin_rate` where `gross_margin_rate = (unit_price - unit_cost) / unit_price` from product master.",
        "- Supplier delay impact proxy: `lost_sales_revenue * supplier_delay_factor`.",
        "- Supplier delay factor: `0.45*(1-OTD) + 0.35*min(avg_delay_days/7,1) + 0.20*min(lead_time_variability/10,1)`.",
        "",
        "## 12-Month Opportunity Assumptions",
        f"- Annualization factor: `{annualization_factor:.6f}` (365 / observed_days).",
        f"- Recoverable margin from service interventions: `{ASSUMPTIONS.recoverable_lost_margin_rate_12m:.0%}` of annualized lost-sales margin proxy.",
        f"- Releasable working capital from inventory actions: `{ASSUMPTIONS.releasable_trapped_wc_rate_12m:.0%}` of annualized trapped WC proxy.",
        "",
        "## Caveats",
        "- Proxy values are prioritization signals, not accounting-recognized P&L outcomes.",
        "- Excess and slow-moving exposures are behavior-based estimates and may over/understate liquidation reality by SKU lifecycle.",
        "- Supplier delay impact is associative, not a causal decomposition.",
        "- Opportunity estimates should be validated with planner constraints, contract terms, and implementation feasibility.",
    ]

    (DOCS_DIR / "impact_assumptions.md").write_text("\n".join(lines), encoding="utf-8")


def write_executive_narrative(
    overall: pd.DataFrame,
    sku: pd.DataFrame,
    warehouse: pd.DataFrame,
    supplier: pd.DataFrame,
    category: pd.DataFrame,
) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    m = dict(zip(overall["metric"], overall["value"]))

    top_sku = sku.iloc[0]
    top_wh = warehouse.iloc[0]
    top_supplier = supplier.iloc[0]
    top_category = category.iloc[0]

    lines = [
        "# Executive Financial Impact Narrative",
        "",
        "The current operating model shows a dual-value leakage pattern: material revenue loss from stockouts and substantial capital lock-up in inefficient inventory.",
        "",
        "## Financial Impact Snapshot",
        f"- Observed lost sales exposure: **EUR {m['lost_sales_revenue_observed']:,.0f}**.",
        f"- Observed excess inventory value proxy: **EUR {m['excess_inventory_value_proxy_observed']:,.0f}**.",
        f"- Observed trapped working-capital proxy: **EUR {m['trapped_working_capital_proxy_observed']:,.0f}**.",
        f"- Observed slow-moving concentration proxy: **EUR {m['slow_moving_value_proxy_observed']:,.0f}**.",
        f"- Supplier delay impact proxy: **EUR {m['supplier_delay_impact_proxy_observed']:,.0f}**.",
        f"- Estimated 12-month opportunity proxy (margin recovery + WC release): **EUR {m['opportunity_total_12m_proxy']:,.0f}**.",
        "",
        "## Prioritization by Business Value",
        f"- Highest SKU opportunity: **{top_sku['product_id']} ({top_sku['product_name']})** with estimated 12M proxy value **EUR {top_sku['opportunity_total_12m_proxy']:,.0f}**.",
        f"- Highest warehouse opportunity: **{top_wh['warehouse_id']} ({top_wh['warehouse_name']})** with estimated 12M proxy value **EUR {top_wh['opportunity_total_12m_proxy']:,.0f}**.",
        f"- Highest supplier opportunity: **{top_supplier['supplier_id']} ({top_supplier['supplier_name']})** with estimated 12M proxy value **EUR {top_supplier['opportunity_total_12m_proxy']:,.0f}**.",
        f"- Highest category opportunity: **{top_category['category']}** with estimated 12M proxy value **EUR {top_category['opportunity_total_12m_proxy']:,.0f}**.",
        "",
        "## Decision Note",
        "Values above are intentionally conservative proxies to prioritize interventions. Final budget cases should be refined with lane-level constraints, procurement terms, and execution timelines.",
    ]

    (OUTPUT_REPORTS_DIR / "impact_executive_narrative.md").write_text("\n".join(lines), encoding="utf-8")


def run_impact_analysis() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    daily, products, suppliers = load_inputs()
    enriched, annualization_factor = enrich_daily(daily, products, suppliers)

    sku = aggregate_impact(enriched, ["product_id", "product_name", "warehouse_id", "category", "supplier_id"])
    warehouse = aggregate_impact(enriched, ["warehouse_id", "region"])
    supplier = aggregate_impact(enriched, ["supplier_id"])
    category = aggregate_impact(enriched, ["category"])

    warehouse_names = pd.read_csv(DATA_RAW / "warehouses.csv")[["warehouse_id", "warehouse_name"]]
    supplier_names = suppliers[["supplier_id", "supplier_name", "on_time_delivery_rate", "average_delay_days", "lead_time_variability"]]

    warehouse = warehouse.merge(warehouse_names, on="warehouse_id", how="left")
    supplier = supplier.merge(supplier_names, on="supplier_id", how="left")

    overall = build_overall_summary(enriched, annualization_factor)
    opportunity = build_opportunity_priority_view(sku, warehouse, supplier, category)

    sku.to_csv(OUTPUT_TABLES_DIR / "impact_by_sku.csv", index=False)
    warehouse.to_csv(OUTPUT_TABLES_DIR / "impact_by_warehouse.csv", index=False)
    supplier.to_csv(OUTPUT_TABLES_DIR / "impact_by_supplier.csv", index=False)
    category.to_csv(OUTPUT_TABLES_DIR / "impact_by_category.csv", index=False)
    overall.to_csv(OUTPUT_TABLES_DIR / "impact_overall_summary.csv", index=False)
    opportunity.to_csv(OUTPUT_TABLES_DIR / "impact_opportunity_priority.csv", index=False)

    create_charts(overall, sku, warehouse, supplier, category)
    write_assumptions_log(annualization_factor)
    write_executive_narrative(overall, sku, warehouse, supplier, category)

    print("Impact analysis complete.")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")
    print(f"Charts written to: {OUTPUT_CHARTS_DIR}")
    print(f"Assumptions log: {DOCS_DIR / 'impact_assumptions.md'}")
    print(f"Executive narrative: {OUTPUT_REPORTS_DIR / 'impact_executive_narrative.md'}")


if __name__ == "__main__":
    run_impact_analysis()
