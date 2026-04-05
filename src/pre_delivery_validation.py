from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re

import duckdb
import numpy as np
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


DOCS_DIR = PROJECT_ROOT / "docs"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
OUTPUT_DASHBOARD_DIR = PROJECT_ROOT / "outputs" / "dashboard"
SQL_DIR = PROJECT_ROOT / "sql"


@dataclass
class CheckResult:
    check_name: str
    layer: str
    method: str
    status: str
    severity: str
    observed: str
    expected: str
    details: str


def _fmt_float(value: float) -> str:
    if np.isnan(value):
        return "nan"
    return f"{value:,.6f}"


def _add_check(results: list[CheckResult], **kwargs) -> None:
    results.append(CheckResult(**kwargs))


def _run_sql_checks() -> tuple[pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect(database=":memory:")

    raw_tables = {
        "products": DATA_RAW / "products.csv",
        "suppliers": DATA_RAW / "suppliers.csv",
        "warehouses": DATA_RAW / "warehouses.csv",
        "inventory_snapshots": DATA_RAW / "inventory_snapshots.csv",
        "demand_history": DATA_RAW / "demand_history.csv",
        "purchase_orders": DATA_RAW / "purchase_orders.csv",
        "product_classification": DATA_RAW / "product_classification.csv",
    }
    for name, path in raw_tables.items():
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE);"
        )

    sql_raw = (SQL_DIR / "04_validation_queries.sql").read_text(encoding="utf-8")
    sql_raw_df = con.execute(sql_raw).df()
    sql_raw_df["method"] = "SQL"
    sql_raw_df["layer"] = "raw"

    processed_tables = {
        "daily_product_warehouse_metrics": DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
        "sku_risk_table": DATA_PROCESSED / "sku_risk_table.csv",
        "dashboard_monthly_sku_fact": OUTPUT_TABLES_DIR / "dashboard_monthly_sku_fact.csv",
    }

    for name, path in processed_tables.items():
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE);"
        )

    sql_processed = """
    WITH
    daily_duplicate_keys AS (
      SELECT COUNT(*) AS issue_count
      FROM (
        SELECT date, warehouse_id, product_id, COUNT(*) AS row_count
        FROM daily_product_warehouse_metrics
        GROUP BY 1,2,3
        HAVING COUNT(*) > 1
      ) d
    ),
    daily_fill_rate_bounds AS (
      SELECT COUNT(*) AS issue_count
      FROM daily_product_warehouse_metrics
      WHERE fill_rate < 0 OR fill_rate > 1
    ),
    daily_stockout_logic AS (
      SELECT COUNT(*) AS issue_count
      FROM daily_product_warehouse_metrics
      WHERE (stockout_flag = 1 AND units_lost_sales = 0)
         OR (stockout_flag = 0 AND units_lost_sales > 0)
         OR (units_fulfilled + units_lost_sales <> units_demanded)
    ),
    sku_score_bounds AS (
      SELECT COUNT(*) AS issue_count
      FROM sku_risk_table
      WHERE service_risk_score < 0 OR service_risk_score > 100
         OR stockout_risk_score < 0 OR stockout_risk_score > 100
         OR excess_inventory_score < 0 OR excess_inventory_score > 100
         OR supplier_risk_score < 0 OR supplier_risk_score > 100
         OR working_capital_risk_score < 0 OR working_capital_risk_score > 100
         OR governance_priority_score < 0 OR governance_priority_score > 100
    ),
    dashboard_expected_grain AS (
      SELECT
        COUNT(*) AS observed_rows,
        COUNT(DISTINCT month) * COUNT(DISTINCT product_id) * COUNT(DISTINCT warehouse_id) AS expected_rows
      FROM dashboard_monthly_sku_fact
    )
    SELECT 'daily_duplicate_keys' AS check_name, issue_count,
           CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM daily_duplicate_keys
    UNION ALL
    SELECT 'daily_fill_rate_bounds', issue_count,
           CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM daily_fill_rate_bounds
    UNION ALL
    SELECT 'daily_stockout_logic', issue_count,
           CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM daily_stockout_logic
    UNION ALL
    SELECT 'sku_score_bounds', issue_count,
           CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM sku_score_bounds
    UNION ALL
    SELECT 'dashboard_expected_grain',
           CASE WHEN observed_rows = expected_rows THEN 0 ELSE ABS(observed_rows - expected_rows) END AS issue_count,
           CASE WHEN observed_rows = expected_rows THEN 'PASS' ELSE 'FAIL' END
    FROM dashboard_expected_grain
    ORDER BY check_name
    """

    sql_processed_df = con.execute(sql_processed).df()
    sql_processed_df["method"] = "SQL"
    sql_processed_df["layer"] = "processed"

    con.close()
    return sql_raw_df, sql_processed_df


def _python_validation_checks() -> list[CheckResult]:
    results: list[CheckResult] = []

    products = pd.read_csv(DATA_RAW / "products.csv")
    suppliers_raw = pd.read_csv(DATA_RAW / "suppliers.csv")
    warehouses = pd.read_csv(DATA_RAW / "warehouses.csv")
    inventory = pd.read_csv(DATA_RAW / "inventory_snapshots.csv", parse_dates=["snapshot_date"])
    demand = pd.read_csv(DATA_RAW / "demand_history.csv", parse_dates=["date"])
    po = pd.read_csv(DATA_RAW / "purchase_orders.csv", parse_dates=["order_date", "expected_arrival_date", "actual_arrival_date"])

    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    sku_risk = pd.read_csv(DATA_PROCESSED / "sku_risk_table.csv")
    supplier_perf = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")

    impact_overall = pd.read_csv(OUTPUT_TABLES_DIR / "impact_overall_summary.csv")
    impact_sku = pd.read_csv(OUTPUT_TABLES_DIR / "impact_by_sku.csv")
    impact_warehouse = pd.read_csv(OUTPUT_TABLES_DIR / "impact_by_warehouse.csv")
    impact_supplier = pd.read_csv(OUTPUT_TABLES_DIR / "impact_by_supplier.csv")
    impact_category = pd.read_csv(OUTPUT_TABLES_DIR / "impact_by_category.csv")
    kpi_overall = pd.read_csv(OUTPUT_REPORTS_DIR / "kpi_overall_service_health.csv").iloc[0]

    dashboard_fact = pd.read_csv(OUTPUT_TABLES_DIR / "dashboard_monthly_sku_fact.csv")
    html_path = OUTPUT_DASHBOARD_DIR / "index.html"

    # 1) Row count sanity
    expected_dense_rows = (
        demand["date"].nunique() * demand["warehouse_id"].nunique() * demand["product_id"].nunique()
    )
    _add_check(
        results,
        check_name="rowcount_dense_demand_history",
        layer="raw",
        method="Python",
        status="PASS" if len(demand) == expected_dense_rows else "FAIL",
        severity="HIGH",
        observed=str(len(demand)),
        expected=str(expected_dense_rows),
        details="Demand history should be dense daily grain (date x warehouse x product).",
    )

    expected_daily_rows = len(demand)
    _add_check(
        results,
        check_name="rowcount_daily_equals_demand_history",
        layer="processed",
        method="Python",
        status="PASS" if len(daily) == expected_daily_rows else "FAIL",
        severity="HIGH",
        observed=str(len(daily)),
        expected=str(expected_daily_rows),
        details="Processed daily metrics should preserve full transactional row coverage.",
    )

    expected_sku_rows = products["product_id"].nunique() * warehouses["warehouse_id"].nunique()
    _add_check(
        results,
        check_name="rowcount_sku_risk_expected_grain",
        layer="processed",
        method="Python",
        status="PASS" if len(sku_risk) == expected_sku_rows else "FAIL",
        severity="HIGH",
        observed=str(len(sku_risk)),
        expected=str(expected_sku_rows),
        details="SKU risk table should be one row per product-warehouse (single supplier per SKU master).",
    )

    # 2) Duplicates and key integrity
    dup_demand = int(demand.duplicated(["date", "warehouse_id", "product_id"]).sum())
    dup_inventory = int(inventory.duplicated(["snapshot_date", "warehouse_id", "product_id"]).sum())
    dup_po = int(po.duplicated(["po_id"]).sum())
    dup_daily = int(daily.duplicated(["date", "warehouse_id", "product_id"]).sum())

    for name, observed in [
        ("duplicates_demand_history", dup_demand),
        ("duplicates_inventory_snapshots", dup_inventory),
        ("duplicates_purchase_orders", dup_po),
        ("duplicates_daily_product_warehouse_metrics", dup_daily),
    ]:
        _add_check(
            results,
            check_name=name,
            layer="raw" if "daily" not in name else "processed",
            method="Python",
            status="PASS" if observed == 0 else "FAIL",
            severity="HIGH",
            observed=str(observed),
            expected="0",
            details="Duplicate key integrity check.",
        )

    # 3) Null critical columns
    critical_null_checks = {
        "products_critical_nulls": (products, ["product_id", "category", "unit_cost", "unit_price", "supplier_id"]),
        "suppliers_critical_nulls": (suppliers_raw, ["supplier_id", "reliability_score", "average_lead_time_days"]),
        "daily_critical_nulls": (daily, ["date", "warehouse_id", "product_id", "units_demanded", "units_fulfilled", "fill_rate"]),
        "sku_risk_critical_nulls": (
            sku_risk,
            [
                "product_id",
                "warehouse_id",
                "supplier_id",
                "service_risk_score",
                "stockout_risk_score",
                "governance_priority_score",
                "risk_tier",
            ],
        ),
    }

    for check_name, (df, cols) in critical_null_checks.items():
        null_count = int(df[cols].isna().sum().sum())
        _add_check(
            results,
            check_name=check_name,
            layer="raw" if check_name.startswith(("products", "suppliers")) else "processed",
            method="Python",
            status="PASS" if null_count == 0 else "FAIL",
            severity="HIGH",
            observed=str(null_count),
            expected="0",
            details=f"Critical column null check for {', '.join(cols)}.",
        )

    # 4) Impossible negative values
    negative_count = int(
        (inventory[["on_hand_units", "on_order_units", "reserved_units", "available_units", "inventory_value"]] < 0)
        .sum()
        .sum()
        + (demand[["units_demanded", "units_fulfilled", "units_lost_sales"]] < 0).sum().sum()
        + (po[["ordered_units", "received_units"]] < 0).sum().sum()
    )
    _add_check(
        results,
        check_name="impossible_negative_values",
        layer="raw",
        method="Python",
        status="PASS" if negative_count == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(negative_count),
        expected="0",
        details="No negative units or values allowed in operational raw tables.",
    )

    # 5) Fill rate logic, stockout logic, lost sales logic
    demand_fill_rate = np.where(demand["units_demanded"] > 0, demand["units_fulfilled"] / demand["units_demanded"], 1.0)
    fill_out_of_bounds = int(((demand_fill_rate < 0) | (demand_fill_rate > 1)).sum())
    demand_balance_issues = int((demand["units_fulfilled"] + demand["units_lost_sales"] != demand["units_demanded"]).sum())
    stockout_logic_issues = int(
        (((demand["stockout_flag"] == 1) & (demand["units_lost_sales"] == 0))
         | ((demand["stockout_flag"] == 0) & (demand["units_lost_sales"] > 0))).sum()
    )

    _add_check(
        results,
        check_name="fill_rate_logic_bounds_raw",
        layer="raw",
        method="Python",
        status="PASS" if fill_out_of_bounds == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(fill_out_of_bounds),
        expected="0",
        details="Derived fill rate must stay within [0,1].",
    )
    _add_check(
        results,
        check_name="demand_balance_units",
        layer="raw",
        method="Python",
        status="PASS" if demand_balance_issues == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(demand_balance_issues),
        expected="0",
        details="Units fulfilled + lost sales must equal units demanded.",
    )
    _add_check(
        results,
        check_name="stockout_flag_logic",
        layer="raw",
        method="Python",
        status="PASS" if stockout_logic_issues == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(stockout_logic_issues),
        expected="0",
        details="Stockout flag must align to lost sales > 0.",
    )

    # Lost sales revenue consistency
    demand_with_price = demand.merge(products[["product_id", "unit_price"]], on="product_id", how="left")
    expected_lost_revenue = demand_with_price["units_lost_sales"] * demand_with_price["unit_price"]
    daily_with_price = daily.merge(products[["product_id", "unit_price"]], on="product_id", how="left")
    expected_daily_lost_revenue = daily_with_price["units_lost_sales"] * daily_with_price["unit_price"]

    lost_rev_mismatch_raw = int((np.abs(expected_lost_revenue - (demand_with_price["units_lost_sales"] * demand_with_price["unit_price"])) > 0.01).sum())
    lost_rev_mismatch_daily = int((np.abs(expected_daily_lost_revenue - daily_with_price["lost_sales_revenue"]) > 0.11).sum())

    _add_check(
        results,
        check_name="lost_sales_revenue_consistency_daily",
        layer="processed",
        method="Python",
        status="PASS" if lost_rev_mismatch_daily == 0 else "FAIL",
        severity="HIGH",
        observed=str(lost_rev_mismatch_daily),
        expected="0",
        details="lost_sales_revenue should reconcile to units_lost_sales * unit_price within rounding tolerance.",
    )

    # 6) Inventory value consistency
    inv_join = inventory.merge(products[["product_id", "unit_cost"]], on="product_id", how="left")
    inv_expected = inv_join["on_hand_units"] * inv_join["unit_cost"]
    inv_mismatch = int((np.abs(inv_join["inventory_value"] - inv_expected) > 0.11).sum())
    available_logic_issues = int((inv_join["available_units"] != (inv_join["on_hand_units"] - inv_join["reserved_units"])).sum())

    _add_check(
        results,
        check_name="inventory_value_consistency_raw",
        layer="raw",
        method="Python",
        status="PASS" if inv_mismatch == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(inv_mismatch),
        expected="0",
        details="Inventory value should reconcile to on-hand units * unit cost.",
    )
    _add_check(
        results,
        check_name="available_units_consistency_raw",
        layer="raw",
        method="Python",
        status="PASS" if available_logic_issues == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(available_logic_issues),
        expected="0",
        details="available_units should equal on_hand_units - reserved_units.",
    )

    # 7) Working capital calculation consistency (impact layer)
    daily_wc = daily.copy()
    daily_wc["dos_cap"] = np.select(
        [daily_wc["abc_class"] == "A", daily_wc["abc_class"] == "B"],
        [20.0, 30.0],
        default=45.0,
    )
    daily_wc["excess_proxy"] = daily_wc["inventory_value"] * (
        (daily_wc["days_of_supply"] - daily_wc["dos_cap"]).clip(lower=0) / daily_wc["days_of_supply"].clip(lower=1e-9)
    )
    daily_wc["slow_proxy"] = np.where(
        (daily_wc["available_units"] > 0) & (daily_wc["units_fulfilled"] == 0),
        daily_wc["inventory_value"],
        0.0,
    )
    daily_wc["trapped_proxy"] = daily_wc["excess_proxy"] + 0.5 * (daily_wc["slow_proxy"] - daily_wc["excess_proxy"]).clip(lower=0)

    overall_map = dict(zip(impact_overall["metric"], impact_overall["value"]))
    trapped_diff = abs(daily_wc["trapped_proxy"].sum() - overall_map["trapped_working_capital_proxy_observed"])
    excess_diff = abs(daily_wc["excess_proxy"].sum() - overall_map["excess_inventory_value_proxy_observed"])

    _add_check(
        results,
        check_name="working_capital_proxy_overall_consistency",
        layer="impact",
        method="Python",
        status="PASS" if trapped_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(trapped_diff),
        expected="<= 1.000000",
        details="Recomputed trapped WC proxy should match impact summary.",
    )
    _add_check(
        results,
        check_name="excess_inventory_proxy_overall_consistency",
        layer="impact",
        method="Python",
        status="PASS" if excess_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(excess_diff),
        expected="<= 1.000000",
        details="Recomputed excess inventory proxy should match impact summary.",
    )

    # 8) Supplier delay calculations
    supplier_calc = supplier_perf.copy()
    supplier_calc["supplier_delay_factor"] = (
        0.45 * (1 - supplier_calc["on_time_delivery_rate"]).clip(0, 1)
        + 0.35 * (supplier_calc["average_delay_days"] / 7.0).clip(0, 1)
        + 0.20 * (supplier_calc["lead_time_variability"] / 10.0).clip(0, 1)
    )

    supplier_delay_observed = (
        daily.groupby("supplier_id", as_index=False)["lost_sales_revenue"].sum()
        .merge(supplier_calc[["supplier_id", "supplier_delay_factor"]], on="supplier_id", how="left")
    )
    supplier_delay_observed["expected_supplier_delay_impact_proxy_observed"] = (
        supplier_delay_observed["lost_sales_revenue"] * supplier_delay_observed["supplier_delay_factor"]
    )

    supplier_delay_compare = impact_supplier.merge(
        supplier_delay_observed[["supplier_id", "expected_supplier_delay_impact_proxy_observed"]],
        on="supplier_id",
        how="left",
    )
    supplier_delay_max_abs_diff = float(
        np.abs(
            supplier_delay_compare["supplier_delay_impact_proxy_observed"]
            - supplier_delay_compare["expected_supplier_delay_impact_proxy_observed"]
        ).max()
    )

    _add_check(
        results,
        check_name="supplier_delay_proxy_consistency",
        layer="impact",
        method="Python",
        status="PASS" if supplier_delay_max_abs_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(supplier_delay_max_abs_diff),
        expected="<= 1.000000",
        details="Supplier delay impact proxy should reconcile to weighted delay factor formula.",
    )

    # 9) Aggregation correctness
    def _metric(df: pd.DataFrame, col: str) -> float:
        return float(df[col].sum())

    overall_lost = float(overall_map["lost_sales_revenue_observed"])
    agg_checks = {
        "aggregation_lost_sales_sku_to_overall": abs(_metric(impact_sku, "lost_sales_revenue_observed") - overall_lost),
        "aggregation_lost_sales_warehouse_to_overall": abs(_metric(impact_warehouse, "lost_sales_revenue_observed") - overall_lost),
        "aggregation_lost_sales_supplier_to_overall": abs(_metric(impact_supplier, "lost_sales_revenue_observed") - overall_lost),
        "aggregation_lost_sales_category_to_overall": abs(_metric(impact_category, "lost_sales_revenue_observed") - overall_lost),
    }

    for name, diff in agg_checks.items():
        _add_check(
            results,
            check_name=name,
            layer="impact",
            method="Python",
            status="PASS" if diff <= 1.0 else "FAIL",
            severity="HIGH",
            observed=_fmt_float(diff),
            expected="<= 1.000000",
            details="Aggregated impact totals should reconcile to overall summary.",
        )

    sensitivity_grid = pd.read_csv(OUTPUT_TABLES_DIR / "sensitivity_opportunity_grid.csv")
    baseline_sensitivity = sensitivity_grid[
        (np.isclose(sensitivity_grid["recoverable_margin_rate"], 0.35))
        & (np.isclose(sensitivity_grid["releasable_wc_rate"], 0.25))
        & (np.isclose(sensitivity_grid["slow_moving_incremental_weight"], 0.50))
    ]
    observed_opportunity = float(overall_map.get("opportunity_total_12m_proxy", 0.0))
    expected_opportunity = float(baseline_sensitivity["opportunity_total_12m_proxy"].iloc[0]) if not baseline_sensitivity.empty else np.nan
    opportunity_diff = abs(expected_opportunity - observed_opportunity) if not np.isnan(expected_opportunity) else np.nan
    _add_check(
        results,
        check_name="impact_opportunity_formula_consistency",
        layer="impact",
        method="Python",
        status="PASS" if not np.isnan(opportunity_diff) and opportunity_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed="nan" if np.isnan(opportunity_diff) else _fmt_float(opportunity_diff),
        expected="<= 1.000000",
        details="12M opportunity proxy must reconcile to the baseline sensitivity scenario (35% margin, 25% WC release, 50% slow-moving weight).",
    )

    # 10) Denominator correctness
    denom_issues_raw = int(((demand["units_demanded"] == 0) & ((demand["units_fulfilled"] > 0) | (demand["units_lost_sales"] > 0))).sum())
    denom_issues_daily = int(((daily["units_demanded"] == 0) & ((daily["units_fulfilled"] > 0) | (daily["units_lost_sales"] > 0))).sum())

    _add_check(
        results,
        check_name="denominator_zero_demand_with_activity_raw",
        layer="raw",
        method="Python",
        status="PASS" if denom_issues_raw == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(denom_issues_raw),
        expected="0",
        details="No fulfilled or lost units allowed when demand denominator is zero.",
    )
    _add_check(
        results,
        check_name="denominator_zero_demand_with_activity_daily",
        layer="processed",
        method="Python",
        status="PASS" if denom_issues_daily == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(denom_issues_daily),
        expected="0",
        details="Processed table denominator sanity should match raw logic.",
    )

    # 11) Scoring consistency
    score_formula = (
        0.24 * sku_risk["service_risk_score"]
        + 0.22 * sku_risk["stockout_risk_score"]
        + 0.18 * sku_risk["excess_inventory_score"]
        + 0.16 * sku_risk["supplier_risk_score"]
        + 0.14 * sku_risk["working_capital_risk_score"]
        + 0.06 * np.minimum(sku_risk["service_risk_score"], sku_risk["excess_inventory_score"])
    )
    score_diff = float(np.abs(score_formula - sku_risk["governance_priority_score"]).max())

    def tier_from_score(s: float) -> str:
        if s > 75:
            return "Critical"
        if s > 55:
            return "High"
        if s > 35:
            return "Medium"
        return "Low"

    tier_mismatches = int((sku_risk["governance_priority_score"].apply(tier_from_score) != sku_risk["risk_tier"]).sum())

    driver_cols = {
        "Service Risk": "service_risk_score",
        "Stockout Risk": "stockout_risk_score",
        "Excess Inventory": "excess_inventory_score",
        "Supplier Risk": "supplier_risk_score",
        "Working Capital": "working_capital_risk_score",
    }
    max_driver = sku_risk[list(driver_cols.values())].idxmax(axis=1).map({v: k for k, v in driver_cols.items()})
    driver_mismatches = int((max_driver != sku_risk["main_risk_driver"]).sum())

    _add_check(
        results,
        check_name="scoring_formula_consistency",
        layer="scoring",
        method="Python",
        status="PASS" if score_diff <= 0.05 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(score_diff),
        expected="<= 0.050000",
        details="Governance score must reconcile to declared weighted formula.",
    )
    _add_check(
        results,
        check_name="scoring_tier_consistency",
        layer="scoring",
        method="Python",
        status="PASS" if tier_mismatches == 0 else "FAIL",
        severity="HIGH",
        observed=str(tier_mismatches),
        expected="0",
        details="risk_tier must align with governance_priority_score thresholds.",
    )
    _add_check(
        results,
        check_name="scoring_main_driver_consistency",
        layer="scoring",
        method="Python",
        status="PASS" if driver_mismatches == 0 else "FAIL",
        severity="MEDIUM",
        observed=str(driver_mismatches),
        expected="0",
        details="main_risk_driver should match max risk component.",
    )

    non_low_monitor = int(
        (
            sku_risk["risk_tier"].isin(["Medium", "High", "Critical"])
            & (sku_risk["recommended_action"].str.lower().str.strip() == "monitor only")
        ).sum()
    )
    _add_check(
        results,
        check_name="scoring_action_policy_non_low_not_monitor_only",
        layer="scoring",
        method="Python",
        status="PASS" if non_low_monitor == 0 else "FAIL",
        severity="HIGH",
        observed=str(non_low_monitor),
        expected="0",
        details="Non-low risk tiers must map to active intervention actions.",
    )

    base_top = set(
        sku_risk.sort_values("governance_priority_score", ascending=False)
        .head(25)
        .apply(lambda x: f"{x['product_id']}|{x['warehouse_id']}|{x['supplier_id']}", axis=1)
        .tolist()
    )
    service_bias_score = (
        0.30 * sku_risk["service_risk_score"]
        + 0.19 * sku_risk["stockout_risk_score"]
        + 0.17 * sku_risk["excess_inventory_score"]
        + 0.16 * sku_risk["supplier_risk_score"]
        + 0.12 * sku_risk["working_capital_risk_score"]
        + 0.06 * np.minimum(sku_risk["service_risk_score"], sku_risk["excess_inventory_score"])
    )
    wc_bias_score = (
        0.20 * sku_risk["service_risk_score"]
        + 0.20 * sku_risk["stockout_risk_score"]
        + 0.21 * sku_risk["excess_inventory_score"]
        + 0.16 * sku_risk["supplier_risk_score"]
        + 0.17 * sku_risk["working_capital_risk_score"]
        + 0.06 * np.minimum(sku_risk["service_risk_score"], sku_risk["excess_inventory_score"])
    )
    service_top = set(
        sku_risk.assign(tmp=service_bias_score)
        .sort_values("tmp", ascending=False)
        .head(25)
        .apply(lambda x: f"{x['product_id']}|{x['warehouse_id']}|{x['supplier_id']}", axis=1)
        .tolist()
    )
    wc_top = set(
        sku_risk.assign(tmp=wc_bias_score)
        .sort_values("tmp", ascending=False)
        .head(25)
        .apply(lambda x: f"{x['product_id']}|{x['warehouse_id']}|{x['supplier_id']}", axis=1)
        .tolist()
    )
    overlap_service = len(base_top & service_top) / 25.0
    overlap_wc = len(base_top & wc_top) / 25.0
    min_overlap = min(overlap_service, overlap_wc)
    stability_status = "PASS" if min_overlap >= 0.65 else ("WARN" if min_overlap >= 0.50 else "FAIL")
    _add_check(
        results,
        check_name="scoring_top25_stability_under_weight_perturbation",
        layer="scoring",
        method="Python",
        status=stability_status,
        severity="MEDIUM",
        observed=f"min_overlap={min_overlap:.3f}",
        expected=">= 0.650",
        details="Top governance queue should remain reasonably stable under small weighting perturbations.",
    )

    # 12) Chart-to-data consistency
    required_chart_files = [
        "viz_01_service_level_trend.png",
        "viz_02_stockout_rate_trend.png",
        "viz_03_fill_rate_by_warehouse.png",
        "viz_04_fill_rate_by_category.png",
        "viz_05_lost_sales_by_region.png",
        "viz_06_inventory_value_concentration_by_category.png",
        "viz_07_days_of_supply_distribution.png",
        "viz_08_supplier_otd_comparison.png",
        "viz_09_lead_time_variability_comparison.png",
        "viz_10_service_vs_inventory_scatter.png",
        "viz_11_top_governance_priority_skus.png",
        "viz_12_excess_inventory_exposure_ranking.png",
        "viz_13_slow_moving_inventory_ranking.png",
        "viz_14_warehouse_service_vs_working_capital_quadrant.png",
        "viz_15_supplier_risk_heatmap.png",
        "policy_frontier_service_vs_inventory.png",
        "policy_optimizer_budget_tradeoff.png",
        "forecast_uncertainty_top_lanes.png",
        "stress_monte_carlo_top_lane_stockout_probability.png",
        "stress_monte_carlo_service_distribution.png",
        "supplier_lane_top_risk_lanes.png",
        "po_cohort_top_risk_cohorts.png",
        "intervention_backlog_by_owner.png",
        "anomaly_alert_timeline.png",
        "sensitivity_opportunity_heatmap.png",
        "sensitivity_opportunity_tornado.png",
    ]
    missing_charts = [f for f in required_chart_files if not (OUTPUT_CHARTS_DIR / f).exists()]
    small_charts = [f for f in required_chart_files if (OUTPUT_CHARTS_DIR / f).exists() and (OUTPUT_CHARTS_DIR / f).stat().st_size < 20_000]

    _add_check(
        results,
        check_name="charts_required_files_present",
        layer="visualization",
        method="Python",
        status="PASS" if len(missing_charts) == 0 else "FAIL",
        severity="HIGH",
        observed=str(len(missing_charts)),
        expected="0",
        details="All required executive chart PNG files should exist.",
    )
    _add_check(
        results,
        check_name="charts_file_size_sanity",
        layer="visualization",
        method="Python",
        status="PASS" if len(small_charts) == 0 else "WARN",
        severity="LOW",
        observed=str(len(small_charts)),
        expected="0",
        details="Very small PNG files may indicate rendering issues.",
    )

    # Upgrade output presence checks
    required_upgrade_tables = [
        "source_adapter_readiness.csv",
        "source_refresh_manifest.csv",
        "data_contract_check_results.csv",
        "data_contract_table_profile.csv",
        "demand_forecast_lane_daily.csv",
        "demand_forecast_lane_summary.csv",
        "policy_simulation_sku_scenarios.csv",
        "policy_simulation_frontier.csv",
        "policy_optimizer_lane_selection.csv",
        "policy_optimizer_budget_summary.csv",
        "stress_monte_carlo_lane_results.csv",
        "stress_monte_carlo_segment_results.csv",
        "supplier_lane_diagnostics.csv",
        "supplier_lane_supplier_summary.csv",
        "po_cohort_diagnostics.csv",
        "po_cohort_lane_summary.csv",
        "intervention_register.csv",
        "intervention_summary_by_owner.csv",
        "anomaly_alerts.csv",
        "anomaly_alerts_summary.csv",
        "sensitivity_opportunity_grid.csv",
        "sensitivity_opportunity_tornado.csv",
        "dashboard_sku_risk_baseline.csv",
        "dashboard_official_snapshot.csv",
        "dashboard_build_manifest.csv",
        "dashboard_release_manifest.csv",
        "ci_sql_validation_checks.csv",
    ]
    missing_upgrade_tables = [t for t in required_upgrade_tables if not (OUTPUT_TABLES_DIR / t).exists()]

    _add_check(
        results,
        check_name="upgrade_outputs_required_tables_present",
        layer="analytics",
        method="Python",
        status="PASS" if len(missing_upgrade_tables) == 0 else "FAIL",
        severity="HIGH",
        observed=str(len(missing_upgrade_tables)),
        expected="0",
        details="Required upgrade tables (adapter, forecast, policy, stress, lane diagnostics, intervention, alerts, SQL gate) must exist.",
    )

    contract_checks_path = OUTPUT_TABLES_DIR / "data_contract_check_results.csv"
    if contract_checks_path.exists():
        contract_checks = pd.read_csv(contract_checks_path)
        contract_fail = int((contract_checks["status"] == "FAIL").sum())
        contract_warn = int((contract_checks["status"] == "WARN").sum())
    else:
        contract_fail = 1
        contract_warn = 0
    _add_check(
        results,
        check_name="data_contract_blocking_failures",
        layer="analytics",
        method="Python",
        status="PASS" if contract_fail == 0 else "FAIL",
        severity="HIGH",
        observed=str(contract_fail),
        expected="0",
        details="Data contract validation must have zero FAIL checks.",
    )
    _add_check(
        results,
        check_name="data_contract_warning_sanity",
        layer="analytics",
        method="Python",
        status="PASS" if contract_warn == 0 else "WARN",
        severity="MEDIUM",
        observed=str(contract_warn),
        expected="0",
        details="Contract warnings should remain zero for release quality discipline.",
    )

    pipeline_log_exists = (OUTPUT_TABLES_DIR / "pipeline_run_log.csv").exists()
    _add_check(
        results,
        check_name="pipeline_orchestration_log_present",
        layer="analytics",
        method="Python",
        status="PASS" if pipeline_log_exists else "WARN",
        severity="LOW",
        observed="1" if pipeline_log_exists else "0",
        expected="1",
        details="Optional run-log from one-command orchestration should exist for production-style traceability.",
    )

    intervention_path = OUTPUT_TABLES_DIR / "intervention_register.csv"
    intervention_sorted = True
    intervention_open_first = True
    if intervention_path.exists():
        intervention = pd.read_csv(intervention_path)
        if not intervention.empty:
            rank_sorted = intervention["intervention_rank"].is_monotonic_increasing
            status_priority = intervention["intervention_status"].map({"Open": 0, "In Progress": 1, "Monitor": 2, "Closed": 3}).fillna(9)
            open_first = status_priority.is_monotonic_increasing
            intervention_sorted = bool(rank_sorted)
            intervention_open_first = bool(open_first)

    _add_check(
        results,
        check_name="intervention_rank_order_consistency",
        layer="analytics",
        method="Python",
        status="PASS" if intervention_sorted else "FAIL",
        severity="MEDIUM",
        observed="1" if intervention_sorted else "0",
        expected="1",
        details="Intervention register ranks must be monotonic and stable after sorting logic.",
    )
    _add_check(
        results,
        check_name="intervention_status_priority_consistency",
        layer="analytics",
        method="Python",
        status="PASS" if intervention_open_first else "FAIL",
        severity="HIGH",
        observed="1" if intervention_open_first else "0",
        expected="1",
        details="Open interventions should be prioritized before Monitor/Closed items in ranking order.",
    )

    # 13) Dashboard metric consistency and structure
    dashboard_daily_totals = {
        "units_demanded": float(daily["units_demanded"].sum()),
        "units_fulfilled": float(daily["units_fulfilled"].sum()),
        "units_lost_sales": float(daily["units_lost_sales"].sum()),
        "lost_sales_revenue": float(daily["lost_sales_revenue"].sum()),
        "inventory_value": float(daily["inventory_value"].sum()),
    }

    dashboard_fact_totals = {
        "units_demanded": float(dashboard_fact["units_demanded"].sum()),
        "units_fulfilled": float(dashboard_fact["units_fulfilled"].sum()),
        "units_lost_sales": float(dashboard_fact["units_lost_sales"].sum()),
        "lost_sales_revenue": float(dashboard_fact["lost_sales_revenue"].sum()),
        "inventory_value": float(dashboard_fact["inventory_value"].sum()),
    }

    dashboard_total_diff = sum(abs(dashboard_daily_totals[k] - dashboard_fact_totals[k]) for k in dashboard_daily_totals)
    _add_check(
        results,
        check_name="dashboard_metric_reconciliation",
        layer="dashboard",
        method="Python",
        status="PASS" if dashboard_total_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(dashboard_total_diff),
        expected="<= 1.000000",
        details="Dashboard embedded fact should reconcile to processed daily totals.",
    )

    required_html_tokens = [
        "filter-region",
        "filter-warehouse",
        "filter-category",
        "filter-supplier",
        "filter-abc",
        "filter-start",
        "filter-end",
        "toggle-theme",
        "data-theme",
        "assump-margin-rate",
        "assump-wc-rate",
        "assump-slow-weight",
        "chart-service-trend",
        "chart-stockout-trend",
        "chart-lost-sales-trend",
        "chart-top-governance",
        "detail-table",
    ]
    html_text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    missing_tokens = [t for t in required_html_tokens if t not in html_text]

    _add_check(
        results,
        check_name="dashboard_required_components_present",
        layer="dashboard",
        method="Python",
        status="PASS" if len(missing_tokens) == 0 else "FAIL",
        severity="HIGH",
        observed=str(len(missing_tokens)),
        expected="0",
        details="Dashboard must contain required filters, charts, and drilldown table structure.",
    )

    forbidden_presentational_tokens = [
        "Data Refresh:",
        "Dashboard Version:",
        "Model Grain:",
        "Dataset Fingerprint:",
        "Generated:",
    ]
    forbidden_hits = [t for t in forbidden_presentational_tokens if t in html_text]
    _add_check(
        results,
        check_name="dashboard_no_visible_technical_metadata",
        layer="dashboard",
        method="Python",
        status="PASS" if len(forbidden_hits) == 0 else "FAIL",
        severity="HIGH",
        observed=str(len(forbidden_hits)),
        expected="0",
        details="Executive-facing dashboard should not expose technical build metadata in the visible layout.",
    )

    style_match = re.search(r"<style>(.*?)</style>", html_text, flags=re.DOTALL | re.IGNORECASE)
    style_text = style_match.group(1) if style_match else ""
    absolute_position_count = style_text.count("position:absolute") + style_text.count("position: absolute")
    _add_check(
        results,
        check_name="dashboard_layout_no_absolute_positioning",
        layer="dashboard",
        method="Python",
        status="PASS" if absolute_position_count == 0 else "WARN",
        severity="MEDIUM",
        observed=str(absolute_position_count),
        expected="0",
        details="Avoiding absolute positioning reduces overlap risk across responsive screen widths.",
    )

    responsive_tokens = ["@media (max-width: 1280px)", "@media (max-width: 820px)", "grid-template-columns"]
    missing_responsive_tokens = [t for t in responsive_tokens if t not in html_text]
    _add_check(
        results,
        check_name="dashboard_responsive_rule_presence",
        layer="dashboard",
        method="Python",
        status="PASS" if len(missing_responsive_tokens) == 0 else "FAIL",
        severity="HIGH",
        observed=str(len(missing_responsive_tokens)),
        expected="0",
        details="Dashboard must retain explicit responsive layout rules for mobile/tablet/desktop safety.",
    )

    dashboard_bytes = html_path.stat().st_size if html_path.exists() else 0
    payload_status = "PASS" if dashboard_bytes <= 8_500_000 else ("WARN" if dashboard_bytes <= 10_500_000 else "FAIL")
    _add_check(
        results,
        check_name="dashboard_payload_size_sanity",
        layer="dashboard",
        method="Python",
        status=payload_status,
        severity="MEDIUM",
        observed=str(dashboard_bytes),
        expected="<= 8500000 bytes (warn up to 10500000)",
        details="Large HTML payloads degrade browser reliability and increase silent rendering failures.",
    )

    forbidden_frontend_logic_tokens = [
        "function riskTier(",
        "function recommendedAction(",
        "0.24 * serviceRiskScore",
        "0.55 * fillGapScore",
    ]
    frontend_logic_hits = sum(t in html_text for t in forbidden_frontend_logic_tokens)
    _add_check(
        results,
        check_name="dashboard_frontend_governance_logic_forbidden",
        layer="dashboard",
        method="Python",
        status="PASS" if frontend_logic_hits == 0 else "FAIL",
        severity="HIGH",
        observed=str(frontend_logic_hits),
        expected="0",
        details="Critical risk-scoring logic must remain in governed backend datasets, not browser-side logic.",
    )

    dashboard_snapshot_path = OUTPUT_TABLES_DIR / "dashboard_official_snapshot.csv"
    if dashboard_snapshot_path.exists():
        dashboard_snapshot = pd.read_csv(dashboard_snapshot_path).iloc[0]
        fill_diff_snapshot = abs(float(dashboard_snapshot["overall_fill_rate"]) - float(kpi_overall["overall_fill_rate"]))
    else:
        fill_diff_snapshot = np.nan
    _add_check(
        results,
        check_name="dashboard_official_snapshot_present_and_reconciled",
        layer="dashboard",
        method="Python",
        status="PASS" if dashboard_snapshot_path.exists() and (fill_diff_snapshot <= 0.0005) else "FAIL",
        severity="HIGH",
        observed="nan" if np.isnan(fill_diff_snapshot) else _fmt_float(fill_diff_snapshot),
        expected="<= 0.000500",
        details="Dashboard official snapshot file must exist and reconcile to KPI baseline metrics.",
    )

    # 14) Cross-output consistency (reports vs governed KPI outputs)
    executive_summary_text = (OUTPUT_REPORTS_DIR / "executive_summary.md").read_text(encoding="utf-8")

    fill_match = re.search(r"Overall fill rate is \*\*(\d+\.\d+)%\*\*", executive_summary_text)
    stockout_match = re.search(r"\*\*(\d+\.\d+)%\*\* unit stockout rate", executive_summary_text)
    lost_sales_match = re.search(r"Lost sales exposure is \*\*EUR ([\d\.]+)M\*\*", executive_summary_text)

    summary_fill = float(fill_match.group(1)) / 100.0 if fill_match else np.nan
    summary_stockout = float(stockout_match.group(1)) / 100.0 if stockout_match else np.nan
    summary_lost = float(lost_sales_match.group(1)) * 1_000_000 if lost_sales_match else np.nan

    fill_summary_diff = abs(summary_fill - float(kpi_overall["overall_fill_rate"])) if not np.isnan(summary_fill) else np.nan
    stockout_summary_diff = abs(summary_stockout - float(kpi_overall["overall_stockout_rate"])) if not np.isnan(summary_stockout) else np.nan
    lost_summary_diff = abs(summary_lost - float(kpi_overall["total_lost_sales_revenue"])) if not np.isnan(summary_lost) else np.nan

    _add_check(
        results,
        check_name="executive_summary_fill_rate_consistency",
        layer="reporting",
        method="Python",
        status="PASS" if not np.isnan(fill_summary_diff) and fill_summary_diff <= 0.001 else "FAIL",
        severity="HIGH",
        observed="nan" if np.isnan(fill_summary_diff) else _fmt_float(fill_summary_diff),
        expected="<= 0.001000",
        details="Executive summary fill-rate narrative must reconcile to KPI baseline output.",
    )
    _add_check(
        results,
        check_name="executive_summary_stockout_rate_consistency",
        layer="reporting",
        method="Python",
        status="PASS" if not np.isnan(stockout_summary_diff) and stockout_summary_diff <= 0.001 else "FAIL",
        severity="HIGH",
        observed="nan" if np.isnan(stockout_summary_diff) else _fmt_float(stockout_summary_diff),
        expected="<= 0.001000",
        details="Executive summary stockout-rate narrative must reconcile to KPI baseline output.",
    )
    _add_check(
        results,
        check_name="executive_summary_lost_sales_consistency",
        layer="reporting",
        method="Python",
        status="PASS" if not np.isnan(lost_summary_diff) and lost_summary_diff <= 200_000 else "WARN",
        severity="MEDIUM",
        observed="nan" if np.isnan(lost_summary_diff) else _fmt_float(lost_summary_diff),
        expected="<= 200000.000000",
        details="Executive summary lost-sales statement should stay aligned (allowing rounded million formatting).",
    )

    # 15) Overclaiming risk in written conclusions (heuristic)
    causal_terms = re.compile(r"\b(caused|proves|guarantees|certainly|always|directly attributable)\b", re.IGNORECASE)
    mitigation_terms = re.compile(r"\b(proxy|estimated|signal|directional|assumption|caveat|likely)\b", re.IGNORECASE)

    text_sources = {
        "executive_kpi": (OUTPUT_REPORTS_DIR / "executive_kpi_diagnostic_analysis.md").read_text(encoding="utf-8"),
        "impact_narrative": (OUTPUT_REPORTS_DIR / "impact_executive_narrative.md").read_text(encoding="utf-8"),
    }

    total_causal_hits = 0
    total_mitigation_hits = 0
    for text in text_sources.values():
        total_causal_hits += len(causal_terms.findall(text))
        total_mitigation_hits += len(mitigation_terms.findall(text))

    overclaim_status = "PASS"
    if total_causal_hits > 0 and total_mitigation_hits < total_causal_hits:
        overclaim_status = "WARN"

    _add_check(
        results,
        check_name="written_conclusion_overclaiming_risk",
        layer="reporting",
        method="Python",
        status=overclaim_status,
        severity="MEDIUM",
        observed=f"causal_hits={total_causal_hits}, mitigation_hits={total_mitigation_hits}",
        expected="mitigation_hits >= causal_hits",
        details="Heuristic check for causal overclaiming without proxy/assumption qualifiers.",
    )

    return results


def _compute_release_state_matrix(checks_df: pd.DataFrame) -> pd.DataFrame:
    blocker_fail = checks_df[
        (checks_df["status"] == "FAIL") & (checks_df["severity"].isin(["BLOCKER", "CRITICAL"]))
    ]
    high_fail = checks_df[
        (checks_df["status"] == "FAIL") & (checks_df["severity"] == "HIGH")
    ]
    high_warn = checks_df[
        (checks_df["status"] == "WARN") & (checks_df["severity"].isin(["BLOCKER", "CRITICAL", "HIGH"]))
    ]
    analytical_fail = checks_df[
        (checks_df["status"] == "FAIL") & (checks_df["layer"].isin(["impact", "scoring", "reporting", "dashboard"]))
    ]
    technical_fail = checks_df[
        (checks_df["status"] == "FAIL") & (checks_df["layer"].isin(["raw", "processed", "dashboard", "scoring"]))
    ]

    technically_valid = technical_fail.empty
    analytically_acceptable = technically_valid and analytical_fail.empty and high_warn.empty
    decision_support_only = analytically_acceptable
    screening_grade_only = technically_valid and (not analytically_acceptable)
    not_committee_grade = True  # synthetic data + proxy-based financial assumptions
    publish_blocked = (not technically_valid) or (not blocker_fail.empty) or (not high_fail.empty) or (not high_warn.empty)

    rows = [
        {
            "state_name": "technically_valid",
            "state_label": "Technically Valid",
            "status": "PASS" if technically_valid else "FAIL",
            "criteria": "No FAIL checks in raw/processed/scoring/dashboard integrity controls.",
            "implication": "Foundational data and metric logic are internally coherent.",
        },
        {
            "state_name": "analytically_acceptable",
            "state_label": "Analytically Acceptable",
            "status": "PASS" if analytically_acceptable else "FAIL",
            "criteria": "Technical validity plus no analytical FAIL and no high-severity WARN.",
            "implication": "Interpretations and prioritization outputs are fit for controlled internal analysis.",
        },
        {
            "state_name": "decision_support_only",
            "state_label": "Decision-Support Only",
            "status": "PASS" if decision_support_only else "FAIL",
            "criteria": "Analytically acceptable with caveated proxy economics.",
            "implication": "Suitable for leadership prioritization and directional planning discussions.",
        },
        {
            "state_name": "screening_grade_only",
            "state_label": "Screening-Grade Only",
            "status": "PASS" if screening_grade_only else "FAIL",
            "criteria": "Technically valid but analytical rigor still below decision-support quality.",
            "implication": "Use for triage/scoping only; no executive decision framing.",
        },
        {
            "state_name": "not_committee_grade",
            "state_label": "Not Committee-Grade",
            "status": "PASS" if not_committee_grade else "FAIL",
            "criteria": "Synthetic data + proxy assumptions prevent audit-grade committee sign-off.",
            "implication": "Do not represent as statutory/committee-grade evidence.",
        },
        {
            "state_name": "publish_blocked",
            "state_label": "Publish-Blocked",
            "status": "PASS" if publish_blocked else "FAIL",
            "criteria": "Any blocker/high failure or high-severity warning blocks release publication.",
            "implication": "When PASS here, release cannot be promoted.",
        },
    ]

    if publish_blocked:
        release_classification = "publish-blocked"
    elif decision_support_only:
        release_classification = "decision-support only"
    elif screening_grade_only:
        release_classification = "screening-grade only"
    else:
        release_classification = "not-classified"

    matrix_df = pd.DataFrame(rows)
    matrix_df["release_classification"] = release_classification
    matrix_df["blocker_fail_count"] = len(blocker_fail)
    matrix_df["high_fail_count"] = len(high_fail)
    matrix_df["high_warn_count"] = len(high_warn)
    return matrix_df


def _build_report(
    checks_df: pd.DataFrame,
    sql_raw_df: pd.DataFrame,
    sql_processed_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    release_matrix: pd.DataFrame,
) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    total = len(checks_df)
    passed = int((checks_df["status"] == "PASS").sum())
    failed = int((checks_df["status"] == "FAIL").sum())
    warned = int((checks_df["status"] == "WARN").sum())

    severity_weight = {"CRITICAL": 35, "HIGH": 20, "MEDIUM": 10, "LOW": 5}
    penalty = 0
    for row in checks_df.itertuples(index=False):
        if row.status == "FAIL":
            penalty += severity_weight.get(row.severity, 10)
        elif row.status == "WARN":
            penalty += int(0.4 * severity_weight.get(row.severity, 10))

    confidence_score = max(0, 100 - penalty)
    if confidence_score >= 90:
        confidence_band = "High"
    elif confidence_score >= 75:
        confidence_band = "Moderate-High"
    elif confidence_score >= 60:
        confidence_band = "Moderate"
    else:
        confidence_band = "Low"

    sql_failed = int(((sql_raw_df["status"] == "FAIL").sum() + (sql_processed_df["status"] == "FAIL").sum()))
    release_classification = str(release_matrix["release_classification"].iloc[0])
    publish_blocked = release_classification == "publish-blocked"

    # Issues and fixes summaries
    issues_lines = [
        "## 2) Issues Found",
        "",
    ]
    if issues_df.empty:
        issues_lines.append("- No FAIL/WARN issues detected in the pre-delivery validation suite.")
    else:
        for row in issues_df.itertuples(index=False):
            issues_lines.append(
                f"- [{row.status}] `{row.check_name}` ({row.layer}/{row.method}, severity {row.severity}): {row.details} (observed={row.observed}, expected={row.expected})."
            )

    fixes_applied = [
        {
            "fix_id": "FIX-001",
            "status": "APPLIED",
            "description": "Reworded KPI narrative line from 'supplier-attributed lost sales' to 'supplier-linked proxy lost sales' to reduce causal overclaim risk.",
            "file": "outputs/reports/executive_kpi_diagnostic_analysis.md",
        }
    ]
    fixes_df = pd.DataFrame(fixes_applied)
    fixes_df.to_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_fixes_applied.csv", index=False)

    unresolved_caveats = [
        "Impact opportunity values remain proxy estimates; 35% recoverable margin and 25% releasable working-capital assumptions materially affect estimated value pools.",
        "Supplier delay impact is an association proxy (delay severity x lost sales), not a causal attribution model.",
        "Dashboard metrics aggregate inventory value over time windows; for finance close processes, point-in-time inventory snapshots should be validated separately.",
    ]

    lines = [
        "# Validation Report",
        "",
        f"Generated at: {generated_at}",
        "",
        "Formal pre-delivery QA for the Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System.",
        "",
        "## 1) Validation Report",
        "",
        f"- Total checks: **{total}**",
        f"- Passed: **{passed}**",
        f"- Failed: **{failed}**",
        f"- Warnings: **{warned}**",
        f"- SQL check failures: **{sql_failed}**",
        f"- Confidence score: **{confidence_score}/100** ({confidence_band})",
        f"- Release classification: **{release_classification}**",
        f"- Publish blocked: **{'Yes' if publish_blocked else 'No'}**",
        "",
        "### Confirmed vs Estimated",
        "- Confirmed (data-integrity/logic): key uniqueness, nulls, non-negativity, fill-rate and stockout logic, reconciliation of aggregates, scoring formula coherence, chart-file generation, dashboard reconciliation.",
        "- Estimated/proxy: excess-inventory value, trapped working-capital value, supplier-delay impact, and 12-month opportunity estimates.",
        "",
        "### Release-State Matrix",
        "| State | Status | Criteria | Implication |",
        "|---|---|---|---|",
    ]

    for row in release_matrix.itertuples(index=False):
        lines.append(f"| {row.state_label} | {row.status} | {row.criteria} | {row.implication} |")

    lines.extend([
        "",
        "### Check Matrix",
        "| Check | Layer | Method | Severity | Status | Observed | Expected |",
        "|---|---|---|---|---|---:|---:|",
    ])

    for row in checks_df.itertuples(index=False):
        lines.append(
            f"| {row.check_name} | {row.layer} | {row.method} | {row.severity} | {row.status} | {row.observed} | {row.expected} |"
        )

    lines.extend(["", *issues_lines, "", "## 3) Fixes Applied", ""])
    for fix in fixes_applied:
        lines.append(f"- [{fix['status']}] {fix['fix_id']}: {fix['description']} ({fix['file']}).")

    lines.extend(["", "## 4) Unresolved Caveats", ""])
    for caveat in unresolved_caveats:
        lines.append(f"- {caveat}")

    lines.extend([
        "",
        "## 5) Final Confidence Assessment",
        "",
        f"- Delivery confidence: **{confidence_band}** ({confidence_score}/100).",
        f"- Release class: **{release_classification}**.",
        "- Recommendation: suitable for leadership review only when release class is decision-support only and proxy caveats remain explicit.",
        "- Governance note: committee-grade publication remains blocked by synthetic-data and proxy-finance constraints.",
        "",
        "## Supporting Outputs",
        "- `/outputs/tables/validation_pre_delivery_checks.csv`",
        "- `/outputs/tables/validation_pre_delivery_issues.csv`",
        "- `/outputs/tables/validation_pre_delivery_sql_raw.csv`",
        "- `/outputs/tables/validation_pre_delivery_sql_processed.csv`",
        "- `/outputs/tables/validation_pre_delivery_fixes_applied.csv`",
        "- `/outputs/tables/validation_release_state_matrix.csv`",
    ])

    return "\n".join(lines)


def run_pre_delivery_validation() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    sql_raw_df, sql_processed_df = _run_sql_checks()
    python_results = _python_validation_checks()

    checks_df = pd.DataFrame([r.__dict__ for r in python_results])

    # Integrate SQL checks into unified output
    sql_raw_checks = sql_raw_df.assign(
        layer=sql_raw_df["layer"],
        method=sql_raw_df["method"],
        severity="HIGH",
        observed=sql_raw_df["issue_count"].astype(str),
        expected="0",
        details="SQL validation query result.",
    )[["check_name", "layer", "method", "status", "severity", "observed", "expected", "details"]]

    sql_processed_checks = sql_processed_df.assign(
        layer=sql_processed_df["layer"],
        method=sql_processed_df["method"],
        severity="HIGH",
        observed=sql_processed_df["issue_count"].astype(str),
        expected="0",
        details="SQL validation query result.",
    )[["check_name", "layer", "method", "status", "severity", "observed", "expected", "details"]]

    unified_df = pd.concat([checks_df, sql_raw_checks, sql_processed_checks], ignore_index=True)
    issues_df = unified_df[unified_df["status"].isin(["FAIL", "WARN"])].copy()
    release_matrix = _compute_release_state_matrix(unified_df)

    unified_df.to_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_checks.csv", index=False)
    issues_df.to_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_issues.csv", index=False)
    sql_raw_df.to_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_sql_raw.csv", index=False)
    sql_processed_df.to_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_sql_processed.csv", index=False)
    release_matrix.to_csv(OUTPUT_TABLES_DIR / "validation_release_state_matrix.csv", index=False)

    report = _build_report(unified_df, sql_raw_df, sql_processed_df, issues_df, release_matrix)
    (DOCS_DIR / "validation_report.md").write_text(report, encoding="utf-8")

    release_lines = [
        "# Release Readiness",
        "",
        f"- Generated at: **{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}**",
        f"- Release classification: **{release_matrix['release_classification'].iloc[0]}**",
        "",
        "## State Overview",
        "| State | Status |",
        "|---|---|",
    ]
    for row in release_matrix.itertuples(index=False):
        release_lines.append(f"| {row.state_label} | {row.status} |")
    (OUTPUT_REPORTS_DIR / "release_readiness.md").write_text("\n".join(release_lines), encoding="utf-8")

    print("Pre-delivery validation completed.")
    print(f"Checks: {len(unified_df)}")
    print(f"Failures: {(unified_df['status'] == 'FAIL').sum()}")
    print(f"Warnings: {(unified_df['status'] == 'WARN').sum()}")


if __name__ == "__main__":
    run_pre_delivery_validation()
