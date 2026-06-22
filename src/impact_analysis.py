from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT  # type: ignore[no-redef]


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


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
    daily = pd.read_csv(
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"]
    )
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

    return out[
        [
            "supplier_id",
            "supplier_name",
            "supplier_delay_factor",
            "on_time_delivery_rate",
            "average_delay_days",
            "lead_time_variability",
        ]
    ]


def enrich_daily(
    daily: pd.DataFrame, products: pd.DataFrame, suppliers: pd.DataFrame
) -> tuple[pd.DataFrame, float]:
    margin = products[["product_id", "product_name", "unit_cost", "unit_price"]].copy()
    margin["unit_gross_margin"] = (margin["unit_price"] - margin["unit_cost"]).clip(lower=0)
    margin["gross_margin_rate"] = np.where(
        margin["unit_price"] > 0,
        margin["unit_gross_margin"] / margin["unit_price"],
        0.0,
    )

    supplier_delay = build_supplier_delay_factor(suppliers)

    out = daily.merge(
        margin[["product_id", "product_name", "gross_margin_rate"]], on="product_id", how="left"
    )
    out = out.merge(
        supplier_delay[["supplier_id", "supplier_delay_factor"]], on="supplier_id", how="left"
    )

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

    out["slow_moving_flag"] = ((out["available_units"] > 0) & (out["units_fulfilled"] == 0)).astype(
        int
    )
    out["slow_moving_value_proxy"] = out["inventory_value"] * out["slow_moving_flag"]

    # Avoid full double counting between excess and slow-moving exposures.
    out["slow_moving_non_excess_proxy"] = (
        out["slow_moving_value_proxy"] - out["excess_inventory_value_proxy"]
    ).clip(lower=0)
    out["trapped_working_capital_proxy"] = (
        out["excess_inventory_value_proxy"]
        + ASSUMPTIONS.slow_moving_incremental_weight * out["slow_moving_non_excess_proxy"]
    )

    out["lost_sales_margin_proxy"] = out["lost_sales_revenue"] * out["gross_margin_rate"]
    out["supplier_delay_impact_proxy"] = out["lost_sales_revenue"] * out["supplier_delay_factor"]

    analysis_days = int(out["date"].nunique())
    annualization_factor = 365.0 / max(analysis_days, 1)

    out["lost_sales_revenue_annualized"] = out["lost_sales_revenue"] * annualization_factor
    out["lost_sales_margin_proxy_annualized"] = (
        out["lost_sales_margin_proxy"] * annualization_factor
    )
    out["supplier_delay_impact_proxy_annualized"] = (
        out["supplier_delay_impact_proxy"] * annualization_factor
    )

    return out, annualization_factor


def aggregate_impact(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    analysis_days = int(df["date"].nunique())
    annualization_factor = 365.0 / max(analysis_days, 1)

    flows = df.groupby(group_cols, as_index=False).agg(
        units_demanded=("units_demanded", "sum"),
        units_lost_sales=("units_lost_sales", "sum"),
        lost_sales_revenue_observed=("lost_sales_revenue", "sum"),
        lost_sales_margin_proxy_observed=("lost_sales_margin_proxy", "sum"),
        supplier_delay_impact_proxy_observed=("supplier_delay_impact_proxy", "sum"),
    )

    daily_balances = (
        df.groupby([*group_cols, "date"], as_index=False)
        .agg(
            excess_inventory_value_proxy=("excess_inventory_value_proxy", "sum"),
            trapped_working_capital_proxy=("trapped_working_capital_proxy", "sum"),
            slow_moving_value_proxy=("slow_moving_value_proxy", "sum"),
        )
        .groupby(group_cols, as_index=False)
        .agg(
            excess_inventory_value_proxy_average=("excess_inventory_value_proxy", "mean"),
            trapped_working_capital_proxy_average=("trapped_working_capital_proxy", "mean"),
            slow_moving_value_proxy_average=("slow_moving_value_proxy", "mean"),
        )
    )

    out = flows.merge(daily_balances, on=group_cols, how="left", validate="one_to_one")
    out["lost_sales_revenue_annualized"] = out["lost_sales_revenue_observed"] * annualization_factor
    out["lost_sales_margin_proxy_annualized"] = (
        out["lost_sales_margin_proxy_observed"] * annualization_factor
    )
    out["supplier_delay_impact_proxy_annualized"] = (
        out["supplier_delay_impact_proxy_observed"] * annualization_factor
    )
    out["opportunity_margin_recovery_12m_proxy"] = (
        out["lost_sales_margin_proxy_annualized"] * ASSUMPTIONS.recoverable_lost_margin_rate_12m
    )
    out["opportunity_wc_release_12m_proxy"] = (
        out["trapped_working_capital_proxy_average"] * ASSUMPTIONS.releasable_trapped_wc_rate_12m
    )
    out["opportunity_total_12m_proxy"] = (
        out["opportunity_margin_recovery_12m_proxy"] + out["opportunity_wc_release_12m_proxy"]
    )

    out["stockout_rate"] = np.where(
        out["units_demanded"] > 0,
        out["units_lost_sales"] / out["units_demanded"],
        0.0,
    )

    total_opp = float(out["opportunity_total_12m_proxy"].sum())
    out["opportunity_share"] = np.where(
        total_opp > 0, out["opportunity_total_12m_proxy"] / total_opp, 0.0
    )
    out["opportunity_rank"] = (
        out["opportunity_total_12m_proxy"].rank(method="first", ascending=False).astype(int)
    )

    return out.sort_values("opportunity_total_12m_proxy", ascending=False)


def build_overall_summary(df: pd.DataFrame, annualization_factor: float) -> pd.DataFrame:
    flows = {
        "lost_sales_revenue_observed": df["lost_sales_revenue"].sum(),
        "lost_sales_margin_proxy_observed": df["lost_sales_margin_proxy"].sum(),
        "supplier_delay_impact_proxy_observed": df["supplier_delay_impact_proxy"].sum(),
    }

    daily_balances = (
        df.groupby("date", as_index=False)
        .agg(
            excess_inventory_value_proxy=("excess_inventory_value_proxy", "sum"),
            trapped_working_capital_proxy=("trapped_working_capital_proxy", "sum"),
            slow_moving_value_proxy=("slow_moving_value_proxy", "sum"),
        )
        .mean(numeric_only=True)
    )
    balances = {
        "excess_inventory_value_proxy_average": daily_balances["excess_inventory_value_proxy"],
        "trapped_working_capital_proxy_average": daily_balances["trapped_working_capital_proxy"],
        "slow_moving_value_proxy_average": daily_balances["slow_moving_value_proxy"],
    }

    annualized = {
        k.replace("_observed", "_annualized"): v * annualization_factor for k, v in flows.items()
    }

    opportunity = {
        "opportunity_margin_recovery_12m_proxy": annualized["lost_sales_margin_proxy_annualized"]
        * ASSUMPTIONS.recoverable_lost_margin_rate_12m,
        "opportunity_wc_release_12m_proxy": balances["trapped_working_capital_proxy_average"]
        * ASSUMPTIONS.releasable_trapped_wc_rate_12m,
    }
    opportunity["opportunity_total_12m_proxy"] = (
        opportunity["opportunity_margin_recovery_12m_proxy"]
        + opportunity["opportunity_wc_release_12m_proxy"]
    )

    rows = [
        ("analysis_days", float(df["date"].nunique()), "Observed period length (days)"),
        ("annualization_factor", annualization_factor, "365 / analysis_days"),
        (
            "lost_sales_revenue_observed",
            flows["lost_sales_revenue_observed"],
            "Observed lost sales value from stockout unmet demand",
        ),
        (
            "lost_sales_margin_proxy_observed",
            flows["lost_sales_margin_proxy_observed"],
            "Observed lost-sales margin proxy",
        ),
        (
            "supplier_delay_impact_proxy_observed",
            flows["supplier_delay_impact_proxy_observed"],
            "Proxy lost-sales value associated with supplier delay severity",
        ),
        (
            "excess_inventory_value_proxy_average",
            balances["excess_inventory_value_proxy_average"],
            "Average daily inventory value above ABC DOS policy caps",
        ),
        (
            "trapped_working_capital_proxy_average",
            balances["trapped_working_capital_proxy_average"],
            "Average daily inefficient inventory capital proxy",
        ),
        (
            "slow_moving_value_proxy_average",
            balances["slow_moving_value_proxy_average"],
            "Average daily inventory value exposed on slow-moving days",
        ),
        (
            "lost_sales_revenue_annualized",
            annualized["lost_sales_revenue_annualized"],
            "Annualized observed lost sales value from stockouts",
        ),
        (
            "lost_sales_margin_proxy_annualized",
            annualized["lost_sales_margin_proxy_annualized"],
            "Annualized lost-sales margin proxy",
        ),
        (
            "supplier_delay_impact_proxy_annualized",
            annualized["supplier_delay_impact_proxy_annualized"],
            "Annualized supplier delay impact proxy",
        ),
        (
            "opportunity_margin_recovery_12m_proxy",
            opportunity["opportunity_margin_recovery_12m_proxy"],
            "Recoverable annual lost-sales margin under the scenario rate",
        ),
        (
            "opportunity_wc_release_12m_proxy",
            opportunity["opportunity_wc_release_12m_proxy"],
            "Releasable average trapped working capital under the scenario rate",
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
                top["entity_name"] = (
                    top[name_col].astype(str) + " @ " + top[extra_id_col].astype(str)
                )
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
                "trapped_working_capital_proxy_average",
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


def run_impact_analysis() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    daily, products, suppliers = load_inputs()
    enriched, annualization_factor = enrich_daily(daily, products, suppliers)

    sku = aggregate_impact(
        enriched, ["product_id", "product_name", "warehouse_id", "category", "supplier_id"]
    )
    warehouse = aggregate_impact(enriched, ["warehouse_id", "region"])
    supplier = aggregate_impact(enriched, ["supplier_id"])
    category = aggregate_impact(enriched, ["category"])

    warehouse_names = pd.read_csv(DATA_RAW / "warehouses.csv")[["warehouse_id", "warehouse_name"]]
    supplier_names = suppliers[
        [
            "supplier_id",
            "supplier_name",
            "on_time_delivery_rate",
            "average_delay_days",
            "lead_time_variability",
        ]
    ]

    warehouse = warehouse.merge(warehouse_names, on="warehouse_id", how="left")
    supplier = supplier.merge(supplier_names, on="supplier_id", how="left")

    overall = build_overall_summary(enriched, annualization_factor)
    opportunity = build_opportunity_priority_view(sku, warehouse, supplier, category)

    overall.to_csv(OUTPUT_TABLES_DIR / "impact_overall_summary.csv", index=False)
    opportunity.to_csv(OUTPUT_TABLES_DIR / "impact_opportunity_priority.csv", index=False)

    print("Impact analysis complete.")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_impact_analysis()
