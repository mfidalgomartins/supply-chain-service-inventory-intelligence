from __future__ import annotations

import numpy as np
import pandas as pd

try:
    from src.config import DATA_PROCESSED
except ModuleNotFoundError:
    from config import DATA_PROCESSED



def _minmax_0_100(series: pd.Series) -> pd.Series:
    s_min = float(series.min())
    s_max = float(series.max())
    if s_max == s_min:
        return pd.Series(50.0, index=series.index)
    return ((series - s_min) / (s_max - s_min) * 100.0).clip(0, 100)



def load_intermediate() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    supplier = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")
    product = pd.read_csv(DATA_PROCESSED / "product_inventory_profile.csv")
    warehouse = pd.read_csv(DATA_PROCESSED / "warehouse_service_profile.csv")
    return daily, supplier, product, warehouse



def build_supplier_performance_summary(base: pd.DataFrame) -> pd.DataFrame:
    out = base.copy()

    # Formula: risk proxy = weighted delivery failure, delay, lead-time volatility, and under-receipt.
    delay_scaled = (out["average_delay_days"] / 10.0).clip(0, 1)
    lead_var_scaled = (out["lead_time_variability"] / 8.0).clip(0, 1)
    underfill_scaled = (1.0 - out["received_vs_ordered_fill_rate"]).clip(0, 1)
    on_time_gap = (1.0 - out["on_time_delivery_rate"]).clip(0, 1)

    out["supplier_service_risk_proxy"] = (
        100.0
        * (
            0.40 * on_time_gap
            + 0.20 * delay_scaled
            + 0.20 * lead_var_scaled
            + 0.20 * underfill_scaled
        )
    ).round(2)

    return out[
        [
            "supplier_id",
            "supplier_name",
            "on_time_delivery_rate",
            "average_delay_days",
            "lead_time_variability",
            "received_vs_ordered_fill_rate",
            "supplier_service_risk_proxy",
        ]
    ]



def build_product_inventory_profile(base: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    out = base.copy()

    # Recalculate selected proxies from daily behavior to avoid over-reliance on one-pass aggregates.
    behavior = (
        daily.assign(
            slow_moving_day=lambda df: ((df["available_units"] > 0) & (df["units_fulfilled"] == 0)).astype(int),
            abc_dos_cap=lambda df: np.select(
                [df["abc_class"] == "A", df["abc_class"] == "B"],
                [20.0, 30.0],
                default=45.0,
            ),
            excess_day=lambda df: (df["days_of_supply"] > df["abc_dos_cap"]).astype(int),
        )
        .groupby("product_id", as_index=False)
        .agg(
            slow_moving_inventory_proxy=("slow_moving_day", "mean"),
            excess_inventory_proxy=("excess_day", "mean"),
            stockout_frequency=("stockout_flag", "mean"),
            fill_rate_average=("fill_rate", "mean"),
            lost_sales_exposure=("lost_sales_revenue", "sum"),
        )
    )

    out = out.drop(columns=[
        "slow_moving_inventory_proxy",
        "excess_inventory_proxy",
        "stockout_frequency",
        "fill_rate_average",
        "lost_sales_exposure",
    ], errors="ignore").merge(behavior, on="product_id", how="left")

    out["slow_moving_inventory_proxy"] = out["slow_moving_inventory_proxy"].fillna(0.0)
    out["excess_inventory_proxy"] = out["excess_inventory_proxy"].fillna(0.0)

    # Formula: working capital risk proxy combines excess-days behavior, slow movement, DOS stretch, and value-at-risk.
    value_scaled = (out["average_inventory_value"] / out["average_inventory_value"].quantile(0.95)).clip(0, 1)
    dos_scaled = (out["average_days_of_supply"] / 60.0).clip(0, 1)
    out["working_capital_risk_proxy"] = (
        100.0
        * (
            0.45 * out["excess_inventory_proxy"]
            + 0.25 * out["slow_moving_inventory_proxy"]
            + 0.20 * dos_scaled
            + 0.10 * value_scaled
        )
    ).round(2)

    return out[
        [
            "product_id",
            "product_name",
            "category",
            "abc_class",
            "average_inventory_units",
            "average_inventory_value",
            "average_days_of_supply",
            "stockout_frequency",
            "fill_rate_average",
            "lost_sales_exposure",
            "slow_moving_inventory_proxy",
            "excess_inventory_proxy",
            "working_capital_risk_proxy",
        ]
    ]



def build_warehouse_service_profile(base: pd.DataFrame) -> pd.DataFrame:
    out = base.copy()

    # Formula: warehouse service risk blends lost-service pressure with capacity and DOS stress.
    out["warehouse_service_risk_proxy"] = (
        100.0
        * (
            0.40 * out["stockout_rate"].clip(0, 1)
            + 0.30 * (1.0 - out["fill_rate"]).clip(0, 1)
            + 0.20 * out["capacity_pressure_proxy"].clip(0, 1)
            + 0.10 * (out["average_days_of_supply"] / 50.0).clip(0, 1)
        )
    ).round(2)

    return out[
        [
            "warehouse_id",
            "warehouse_name",
            "region",
            "fill_rate",
            "stockout_rate",
            "lost_sales_value",
            "average_days_of_supply",
            "inventory_value",
            "capacity_pressure_proxy",
            "warehouse_service_risk_proxy",
        ]
    ]



def build_sku_risk_table(
    daily: pd.DataFrame,
    supplier_summary: pd.DataFrame,
    product_profile: pd.DataFrame,
) -> pd.DataFrame:
    sku = (
        daily.assign(
            abc_dos_cap=lambda df: np.select(
                [df["abc_class"] == "A", df["abc_class"] == "B"],
                [20.0, 30.0],
                default=45.0,
            ),
            excess_day=lambda df: (df["days_of_supply"] > df["abc_dos_cap"]).astype(int),
            slow_moving_day=lambda df: ((df["available_units"] > 0) & (df["units_fulfilled"] == 0)).astype(int),
        )
        .groupby(["product_id", "warehouse_id", "supplier_id"], as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_fulfilled=("units_fulfilled", "sum"),
            units_lost_sales=("units_lost_sales", "sum"),
            service_gap_units=("service_gap_units", "sum"),
            stockout_frequency=("stockout_flag", "mean"),
            excess_inventory_rate=("excess_day", "mean"),
            slow_moving_rate=("slow_moving_day", "mean"),
            average_days_of_supply=("days_of_supply", "mean"),
            average_inventory_value=("inventory_value", "mean"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
        )
    )

    sku["fill_rate"] = np.where(
        sku["units_demanded"] == 0,
        1.0,
        sku["units_fulfilled"] / sku["units_demanded"],
    )
    sku["lost_sales_rate"] = np.where(
        sku["units_demanded"] == 0,
        0.0,
        sku["units_lost_sales"] / sku["units_demanded"],
    )
    sku["service_gap_rate"] = np.where(
        sku["units_demanded"] == 0,
        0.0,
        sku["service_gap_units"] / sku["units_demanded"],
    )

    supplier_risk_map = supplier_summary[["supplier_id", "supplier_service_risk_proxy"]]
    product_wc_map = product_profile[["product_id", "working_capital_risk_proxy"]]

    sku = sku.merge(supplier_risk_map, on="supplier_id", how="left")
    sku = sku.merge(product_wc_map, on="product_id", how="left")

    sku["supplier_service_risk_proxy"] = sku["supplier_service_risk_proxy"].fillna(0.0)
    sku["working_capital_risk_proxy"] = sku["working_capital_risk_proxy"].fillna(0.0)

    # Component score formulas (0-100 scale).
    sku["service_risk_score"] = (
        100.0
        * (
            0.65 * (sku["service_gap_rate"] / 0.25).clip(0, 1)
            + 0.35 * ((1.0 - sku["fill_rate"]) / 0.15).clip(0, 1)
        )
    )

    sku["stockout_risk_score"] = (
        100.0
        * (
            0.70 * (sku["stockout_frequency"] / 0.25).clip(0, 1)
            + 0.30 * (sku["lost_sales_rate"] / 0.20).clip(0, 1)
        )
    )

    sku["excess_inventory_score"] = (
        100.0
        * (
            0.70 * (sku["excess_inventory_rate"] / 0.40).clip(0, 1)
            + 0.30 * (sku["average_days_of_supply"] / 60.0).clip(0, 1)
        )
    )

    sku["supplier_risk_score"] = sku["supplier_service_risk_proxy"].clip(0, 100)

    value_scaled = _minmax_0_100(sku["average_inventory_value"]) / 100.0
    sku["working_capital_risk_score"] = (
        100.0
        * (
            0.55 * (sku["working_capital_risk_proxy"] / 100.0)
            + 0.25 * (sku["slow_moving_rate"] / 0.30).clip(0, 1)
            + 0.20 * value_scaled
        )
    )

    lost_revenue_scaled = _minmax_0_100(sku["lost_sales_revenue"])

    # Governance priority: multi-objective prioritization for service + working capital governance.
    sku["governance_priority_score"] = (
        0.26 * sku["service_risk_score"]
        + 0.22 * sku["stockout_risk_score"]
        + 0.18 * sku["excess_inventory_score"]
        + 0.16 * sku["supplier_risk_score"]
        + 0.10 * sku["working_capital_risk_score"]
        + 0.08 * lost_revenue_scaled
    ).round(2)

    sku["risk_tier"] = pd.cut(
        sku["governance_priority_score"],
        bins=[-0.001, 35, 55, 75, 1000],
        labels=["Low", "Medium", "High", "Critical"],
    ).astype(str)

    driver_cols = {
        "service_risk_score": "Service Risk",
        "stockout_risk_score": "Stockout Risk",
        "excess_inventory_score": "Excess Inventory",
        "supplier_risk_score": "Supplier Risk",
        "working_capital_risk_score": "Working Capital",
    }

    comp_matrix = sku[list(driver_cols.keys())]
    sku["main_risk_driver"] = comp_matrix.idxmax(axis=1).map(driver_cols)

    action_map = {
        "Service Risk": "Increase safety stock and tighten replenishment cycle for affected SKU-location.",
        "Stockout Risk": "Raise reorder point and protect promotional allocation to reduce lost sales.",
        "Excess Inventory": "Reduce order-up-to level and trigger markdown/transfer for overstock exposure.",
        "Supplier Risk": "Escalate supplier performance plan and evaluate secondary sourcing options.",
        "Working Capital": "Rebalance network inventory and enforce stricter MOQ/order frequency controls.",
    }
    sku["recommended_action"] = sku["main_risk_driver"].map(action_map)

    return sku[
        [
            "product_id",
            "warehouse_id",
            "supplier_id",
            "service_risk_score",
            "stockout_risk_score",
            "excess_inventory_score",
            "supplier_risk_score",
            "working_capital_risk_score",
            "governance_priority_score",
            "risk_tier",
            "main_risk_driver",
            "recommended_action",
        ]
    ].sort_values("governance_priority_score", ascending=False)



def save_outputs(
    daily: pd.DataFrame,
    supplier_summary: pd.DataFrame,
    product_profile: pd.DataFrame,
    warehouse_profile: pd.DataFrame,
    sku_risk_table: pd.DataFrame,
) -> None:
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    daily.to_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", index=False)
    supplier_summary.to_csv(DATA_PROCESSED / "supplier_performance_summary.csv", index=False)
    product_profile.to_csv(DATA_PROCESSED / "product_inventory_profile.csv", index=False)
    warehouse_profile.to_csv(DATA_PROCESSED / "warehouse_service_profile.csv", index=False)
    sku_risk_table.to_csv(DATA_PROCESSED / "sku_risk_table.csv", index=False)



def run_feature_engineering() -> None:
    daily, supplier_base, product_base, warehouse_base = load_intermediate()

    supplier_summary = build_supplier_performance_summary(supplier_base)
    product_profile = build_product_inventory_profile(product_base, daily)
    warehouse_profile = build_warehouse_service_profile(warehouse_base)
    sku_risk_table = build_sku_risk_table(daily, supplier_summary, product_profile)

    save_outputs(
        daily=daily,
        supplier_summary=supplier_summary,
        product_profile=product_profile,
        warehouse_profile=warehouse_profile,
        sku_risk_table=sku_risk_table,
    )

    print("Feature engineering complete.")
    print(f"daily_product_warehouse_metrics rows: {len(daily):,}")
    print(f"supplier_performance_summary rows: {len(supplier_summary):,}")
    print(f"product_inventory_profile rows: {len(product_profile):,}")
    print(f"warehouse_service_profile rows: {len(warehouse_profile):,}")
    print(f"sku_risk_table rows: {len(sku_risk_table):,}")


if __name__ == "__main__":
    run_feature_engineering()
