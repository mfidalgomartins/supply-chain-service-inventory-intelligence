"""Analytical release gate: reconciles service, inventory, impact, scoring,
and dashboard metrics against each other, emits
validation_pre_delivery_checks.csv, and classifies the run into the release
state matrix (technically_valid through publish_allowed). Any FAIL or
high-severity WARN downstream blocks publication via ci_quality_gate."""

from __future__ import annotations

import re
from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT  # type: ignore[no-redef]


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_DASHBOARD_FILE = PROJECT_ROOT / "index.html"
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
    po = pd.read_csv(
        DATA_RAW / "purchase_orders.csv",
        parse_dates=["order_date", "expected_arrival_date", "actual_arrival_date"],
    )

    daily = pd.read_csv(
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"]
    )
    sku_risk = pd.read_csv(DATA_PROCESSED / "sku_risk_table.csv")
    supplier_perf = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")

    impact_overall = pd.read_csv(OUTPUT_TABLES_DIR / "impact_overall_summary.csv")
    kpi_overall = {
        "overall_fill_rate": float(
            daily["units_fulfilled"].sum() / max(1.0, daily["units_demanded"].sum())
        ),
        "overall_stockout_rate": float(
            daily["units_lost_sales"].sum() / max(1.0, daily["units_demanded"].sum())
        ),
        "total_lost_sales_revenue": float(daily["lost_sales_revenue"].sum()),
    }

    dashboard_fact = pd.read_csv(OUTPUT_TABLES_DIR / "dashboard_monthly_sku_fact.csv")
    html_path = OUTPUT_DASHBOARD_FILE

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

    po_horizon_issues = int(
        (
            (po["order_date"] < demand["date"].min())
            | (po["actual_arrival_date"] > demand["date"].max())
        ).sum()
    )
    _add_check(
        results,
        check_name="purchase_order_execution_within_analysis_horizon",
        layer="raw",
        method="Python",
        status="PASS" if po_horizon_issues == 0 else "FAIL",
        severity="CRITICAL",
        observed=str(po_horizon_issues),
        expected="0",
        details="Completed purchase-order execution records must remain inside the analysis horizon.",
    )

    # 3) Null critical columns
    critical_null_checks = {
        "products_critical_nulls": (
            products,
            ["product_id", "category", "unit_cost", "unit_price", "supplier_id"],
        ),
        "suppliers_critical_nulls": (
            suppliers_raw,
            ["supplier_id", "reliability_score", "average_lead_time_days"],
        ),
        "daily_critical_nulls": (
            daily,
            [
                "date",
                "warehouse_id",
                "product_id",
                "units_demanded",
                "units_fulfilled",
                "fill_rate",
            ],
        ),
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
        (
            inventory[
                [
                    "on_hand_units",
                    "on_order_units",
                    "reserved_units",
                    "available_units",
                    "inventory_value",
                ]
            ]
            < 0
        )
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
    demand_fill_rate = np.where(
        demand["units_demanded"] > 0, demand["units_fulfilled"] / demand["units_demanded"], 1.0
    )
    fill_out_of_bounds = int(((demand_fill_rate < 0) | (demand_fill_rate > 1)).sum())
    demand_balance_issues = int(
        (demand["units_fulfilled"] + demand["units_lost_sales"] != demand["units_demanded"]).sum()
    )
    stockout_logic_issues = int(
        (
            ((demand["stockout_flag"] == 1) & (demand["units_lost_sales"] == 0))
            | ((demand["stockout_flag"] == 0) & (demand["units_lost_sales"] > 0))
        ).sum()
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
    daily_with_price = daily.merge(
        products[["product_id", "unit_price"]], on="product_id", how="left"
    )
    expected_daily_lost_revenue = (
        daily_with_price["units_lost_sales"] * daily_with_price["unit_price"]
    )

    lost_rev_mismatch_daily = int(
        (np.abs(expected_daily_lost_revenue - daily_with_price["lost_sales_revenue"]) > 0.11).sum()
    )

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
    available_logic_issues = int(
        (
            inv_join["available_units"] != (inv_join["on_hand_units"] - inv_join["reserved_units"])
        ).sum()
    )

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
        (daily_wc["days_of_supply"] - daily_wc["dos_cap"]).clip(lower=0)
        / daily_wc["days_of_supply"].clip(lower=1e-9)
    )
    daily_wc["slow_proxy"] = np.where(
        (daily_wc["available_units"] > 0) & (daily_wc["units_fulfilled"] == 0),
        daily_wc["inventory_value"],
        0.0,
    )
    daily_wc["trapped_proxy"] = daily_wc["excess_proxy"] + 0.5 * (
        daily_wc["slow_proxy"] - daily_wc["excess_proxy"]
    ).clip(lower=0)

    overall_map = dict(zip(impact_overall["metric"], impact_overall["value"], strict=False))
    average_daily_wc = daily_wc.groupby("date")[["trapped_proxy", "excess_proxy"]].sum().mean()
    trapped_diff = abs(
        average_daily_wc["trapped_proxy"] - overall_map["trapped_working_capital_proxy_average"]
    )
    excess_diff = abs(
        average_daily_wc["excess_proxy"] - overall_map["excess_inventory_value_proxy_average"]
    )

    _add_check(
        results,
        check_name="working_capital_proxy_overall_consistency",
        layer="impact",
        method="Python",
        status="PASS" if trapped_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(trapped_diff),
        expected="<= 1.000000",
        details="Average daily trapped WC proxy should match the impact summary.",
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
        details="Average daily excess inventory proxy should match the impact summary.",
    )

    # 8) Supplier delay calculations
    supplier_calc = supplier_perf.copy()
    supplier_calc["supplier_delay_factor"] = (
        0.45 * (1 - supplier_calc["on_time_delivery_rate"]).clip(0, 1)
        + 0.35 * (supplier_calc["average_delay_days"] / 7.0).clip(0, 1)
        + 0.20 * (supplier_calc["lead_time_variability"] / 10.0).clip(0, 1)
    )

    supplier_delay_by_supplier = (
        daily.groupby("supplier_id", as_index=False)["lost_sales_revenue"]
        .sum()
        .merge(
            supplier_calc[["supplier_id", "supplier_delay_factor"]], on="supplier_id", how="left"
        )
    )
    supplier_delay_by_supplier["supplier_delay_impact_proxy_observed"] = (
        supplier_delay_by_supplier["lost_sales_revenue"]
        * supplier_delay_by_supplier["supplier_delay_factor"]
    )
    supplier_delay_diff = abs(
        float(supplier_delay_by_supplier["supplier_delay_impact_proxy_observed"].sum())
        - float(overall_map["supplier_delay_impact_proxy_observed"])
    )

    _add_check(
        results,
        check_name="supplier_delay_proxy_consistency",
        layer="impact",
        method="Python",
        status="PASS" if supplier_delay_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(supplier_delay_diff),
        expected="<= 1.000000",
        details="Supplier delay impact proxy should reconcile to the declared weighted delay factor.",
    )

    # 9) Aggregation correctness
    overall_lost = float(overall_map["lost_sales_revenue_observed"])
    lost_sales_diff = abs(float(daily["lost_sales_revenue"].sum()) - overall_lost)
    _add_check(
        results,
        check_name="impact_lost_sales_overall_consistency",
        layer="impact",
        method="Python",
        status="PASS" if lost_sales_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(lost_sales_diff),
        expected="<= 1.000000",
        details="Impact summary lost sales must reconcile to the canonical daily fact.",
    )

    opportunity_value = float(overall_map.get("opportunity_total_12m_proxy", 0.0))
    _add_check(
        results,
        check_name="impact_opportunity_non_negative",
        layer="impact",
        method="Python",
        status="PASS" if opportunity_value >= 0.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(opportunity_value),
        expected=">= 0.000000",
        details="12M opportunity proxy should be non-negative for a valid value pool.",
    )

    # 10) Denominator correctness
    denom_issues_raw = int(
        (
            (demand["units_demanded"] == 0)
            & ((demand["units_fulfilled"] > 0) | (demand["units_lost_sales"] > 0))
        ).sum()
    )
    denom_issues_daily = int(
        (
            (daily["units_demanded"] == 0)
            & ((daily["units_fulfilled"] > 0) | (daily["units_lost_sales"] > 0))
        ).sum()
    )

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

    tier_mismatches = int(
        (
            sku_risk["governance_priority_score"].apply(tier_from_score) != sku_risk["risk_tier"]
        ).sum()
    )

    driver_cols = {
        "Service Risk": "service_risk_score",
        "Stockout Risk": "stockout_risk_score",
        "Excess Inventory": "excess_inventory_score",
        "Supplier Risk": "supplier_risk_score",
        "Working Capital": "working_capital_risk_score",
    }
    max_driver = (
        sku_risk[list(driver_cols.values())]
        .idxmax(axis=1)
        .map({v: k for k, v in driver_cols.items()})
    )
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
    stability_status = (
        "PASS" if min_overlap >= 0.65 else ("WARN" if min_overlap >= 0.50 else "FAIL")
    )
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

    # Curated output presence checks
    required_upgrade_tables = [
        "data_contract_check_results.csv",
        "data_contract_table_profile.csv",
        "scoring_sku_risk_table.csv",
        "impact_overall_summary.csv",
        "impact_opportunity_priority.csv",
        "dashboard_sku_risk_baseline.csv",
        "dashboard_official_snapshot.csv",
        "ci_sql_validation_checks.csv",
    ]
    missing_upgrade_tables = [
        t for t in required_upgrade_tables if not (OUTPUT_TABLES_DIR / t).exists()
    ]

    _add_check(
        results,
        check_name="upgrade_outputs_required_tables_present",
        layer="analytics",
        method="Python",
        status="PASS" if len(missing_upgrade_tables) == 0 else "FAIL",
        severity="HIGH",
        observed=str(len(missing_upgrade_tables)),
        expected="0",
        details="Required curated release tables (contracts, scoring, impact, dashboard, and SQL gate) must exist.",
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

    # 13) Dashboard metric consistency and structure
    dashboard_flow_totals = {
        "units_demanded": float(daily["units_demanded"].sum()),
        "units_fulfilled": float(daily["units_fulfilled"].sum()),
        "units_lost_sales": float(daily["units_lost_sales"].sum()),
        "lost_sales_revenue": float(daily["lost_sales_revenue"].sum()),
    }

    dashboard_fact_flow_totals = {
        "units_demanded": float(dashboard_fact["units_demanded"].sum()),
        "units_fulfilled": float(dashboard_fact["units_fulfilled"].sum()),
        "units_lost_sales": float(dashboard_fact["units_lost_sales"].sum()),
        "lost_sales_revenue": float(dashboard_fact["lost_sales_revenue"].sum()),
    }

    daily_inventory_average = float(daily.groupby("date")["inventory_value"].sum().mean())
    dashboard_monthly_inventory = dashboard_fact.groupby("month", as_index=False).agg(
        inventory_value=("inventory_value", "sum"),
        observation_days=("observation_days", "max"),
    )
    dashboard_inventory_average = float(
        np.average(
            dashboard_monthly_inventory["inventory_value"],
            weights=dashboard_monthly_inventory["observation_days"],
        )
    )
    dashboard_total_diff = sum(
        abs(dashboard_flow_totals[k] - dashboard_fact_flow_totals[k]) for k in dashboard_flow_totals
    ) + abs(daily_inventory_average - dashboard_inventory_average)
    _add_check(
        results,
        check_name="dashboard_metric_reconciliation",
        layer="dashboard",
        method="Python",
        status="PASS" if dashboard_total_diff <= 1.0 else "FAIL",
        severity="HIGH",
        observed=_fmt_float(dashboard_total_diff),
        expected="<= 1.000000",
        details="Dashboard flows and average daily inventory balance should reconcile to the processed daily fact.",
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
        "chart-trend",
        "chart-value-trend",
        "chart-bottlenecks",
        "chart-category-capital",
        "chart-tradeoff",
        "chart-supplier",
        "chart-governance",
        "detail-table",
        "aria-sort",
        "https://cdn.plot.ly/plotly-3.5.0.min.js",
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
    absolute_position_count = style_text.count("position:absolute") + style_text.count(
        "position: absolute"
    )
    _add_check(
        results,
        check_name="dashboard_layout_no_absolute_positioning",
        layer="dashboard",
        method="Python",
        status="PASS" if absolute_position_count <= 2 else "WARN",
        severity="MEDIUM",
        observed=str(absolute_position_count),
        expected="<= 2",
        details="Only the keyboard skip-link and screen-reader-only utility should use absolute positioning.",
    )

    # Responsiveness is asserted by intent, not exact breakpoint pixels: at least two
    # max-width media queries (tablet + mobile) plus responsive grid columns must exist.
    responsive_breakpoint_count = html_text.count("@media (max-width:")
    has_grid_columns = "grid-template-columns" in html_text
    responsive_ok = responsive_breakpoint_count >= 2 and has_grid_columns
    _add_check(
        results,
        check_name="dashboard_responsive_rule_presence",
        layer="dashboard",
        method="Python",
        status="PASS" if responsive_ok else "FAIL",
        severity="HIGH",
        observed=f"breakpoints={responsive_breakpoint_count}, grid_columns={has_grid_columns}",
        expected="breakpoints>=2, grid_columns=True",
        details="Dashboard must retain explicit responsive layout rules for mobile/tablet/desktop safety.",
    )

    dashboard_bytes = html_path.stat().st_size if html_path.exists() else 0
    payload_status = (
        "PASS"
        if dashboard_bytes <= 2_000_000
        else ("WARN" if dashboard_bytes <= 3_000_000 else "FAIL")
    )
    _add_check(
        results,
        check_name="dashboard_payload_size_sanity",
        layer="dashboard",
        method="Python",
        status=payload_status,
        severity="MEDIUM",
        observed=str(dashboard_bytes),
        expected="<= 2000000 bytes (warn up to 3000000)",
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
        fill_diff_snapshot = abs(
            float(dashboard_snapshot["overall_fill_rate"]) - float(kpi_overall["overall_fill_rate"])
        )
        stockout_diff_snapshot = abs(
            float(dashboard_snapshot["overall_stockout_rate"])
            - float(kpi_overall["overall_stockout_rate"])
        )
        trapped_wc_diff_snapshot = abs(
            float(dashboard_snapshot["trapped_working_capital_proxy_average"])
            - float(overall_map["trapped_working_capital_proxy_average"])
        )
    else:
        fill_diff_snapshot = np.nan
        stockout_diff_snapshot = np.nan
        trapped_wc_diff_snapshot = np.nan
    _add_check(
        results,
        check_name="dashboard_official_snapshot_present_and_reconciled",
        layer="dashboard",
        method="Python",
        status=(
            "PASS"
            if dashboard_snapshot_path.exists()
            and fill_diff_snapshot <= 0.0005
            and stockout_diff_snapshot <= 0.0005
            and trapped_wc_diff_snapshot <= 1.0
            else "FAIL"
        ),
        severity="HIGH",
        observed=(
            "nan"
            if np.isnan(fill_diff_snapshot)
            or np.isnan(stockout_diff_snapshot)
            or np.isnan(trapped_wc_diff_snapshot)
            else (
                f"fill_diff={_fmt_float(fill_diff_snapshot)}, "
                f"stockout_diff={_fmt_float(stockout_diff_snapshot)}, "
                f"trapped_wc_diff={_fmt_float(trapped_wc_diff_snapshot)}"
            )
        ),
        expected="fill_diff <= 0.000500, stockout_diff <= 0.000500, trapped_wc_diff <= 1.000000",
        details="Dashboard snapshot must reconcile service KPIs and average daily trapped WC to governed outputs.",
    )

    return results


def _compute_release_state_matrix(checks_df: pd.DataFrame) -> pd.DataFrame:
    """Fold the check results into the four release states (technically_valid
    through publish_allowed). Blocker/critical failures invalidate everything;
    high-severity failures or warnings block publication while leaving the
    technical layer valid."""
    blocker_fail = checks_df[
        (checks_df["status"] == "FAIL") & (checks_df["severity"].isin(["BLOCKER", "CRITICAL"]))
    ]
    high_fail = checks_df[(checks_df["status"] == "FAIL") & (checks_df["severity"] == "HIGH")]
    high_warn = checks_df[
        (checks_df["status"] == "WARN")
        & (checks_df["severity"].isin(["BLOCKER", "CRITICAL", "HIGH"]))
    ]
    analytical_fail = checks_df[
        (checks_df["status"] == "FAIL")
        & (checks_df["layer"].isin(["impact", "scoring", "reporting", "dashboard"]))
    ]
    technical_fail = checks_df[
        (checks_df["status"] == "FAIL")
        & (checks_df["layer"].isin(["raw", "processed", "dashboard", "scoring"]))
    ]

    technically_valid = technical_fail.empty
    analytically_acceptable = technically_valid and analytical_fail.empty and high_warn.empty
    decision_support_ready = analytically_acceptable
    publish_blocked = (
        (checks_df["status"] == "FAIL").any()
        or (not blocker_fail.empty)
        or (not high_fail.empty)
        or (not high_warn.empty)
    )
    publish_allowed = not publish_blocked

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
            "state_name": "decision_support_ready",
            "state_label": "Decision-Support Ready",
            "status": "PASS" if decision_support_ready else "FAIL",
            "criteria": "Analytically acceptable with caveated proxy economics.",
            "implication": "Suitable for leadership prioritization and directional planning discussions.",
        },
        {
            "state_name": "publish_allowed",
            "state_label": "Publish Allowed",
            "status": "PASS" if publish_allowed else "FAIL",
            "criteria": "No failures or high-severity warnings remain.",
            "implication": "When PASS, release artefacts may be promoted.",
        },
    ]

    if publish_blocked:
        release_classification = "publish-blocked"
    elif decision_support_ready:
        release_classification = "decision-support ready"
    else:
        release_classification = "not-classified"

    matrix_df = pd.DataFrame(rows)
    matrix_df["release_classification"] = release_classification
    matrix_df["blocker_fail_count"] = len(blocker_fail)
    matrix_df["high_fail_count"] = len(high_fail)
    matrix_df["high_warn_count"] = len(high_warn)
    return matrix_df


def run_pre_delivery_validation() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

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
    release_matrix = _compute_release_state_matrix(unified_df)

    unified_df.to_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_checks.csv", index=False)
    release_matrix.to_csv(OUTPUT_TABLES_DIR / "validation_release_state_matrix.csv", index=False)

    print("Pre-delivery validation completed.")
    print(f"Checks: {len(unified_df)}")
    print(f"Failures: {(unified_df['status'] == 'FAIL').sum()}")
    print(f"Warnings: {(unified_df['status'] == 'WARN').sum()}")


if __name__ == "__main__":
    run_pre_delivery_validation()
