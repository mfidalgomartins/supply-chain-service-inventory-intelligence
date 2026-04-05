from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb
import pandas as pd

try:
    from src.config import DOCS_DIR, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DOCS_DIR, PROJECT_ROOT


@dataclass(frozen=True)
class TableSpec:
    name: str
    file_name: str
    grain: str
    primary_key: list[str]
    date_columns: list[str]
    useful_dimensions: list[str]
    useful_metrics: list[str]
    likely_pitfalls: list[str]
    non_negative_columns: list[str]
    bounded_columns: dict[str, tuple[float, float]]
    categorical_domains: dict[str, set[Any]]


RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
SQL_DIR = PROJECT_ROOT / "sql"


TABLE_SPECS: list[TableSpec] = [
    TableSpec(
        name="products",
        file_name="products.csv",
        grain="1 row per product_id",
        primary_key=["product_id"],
        date_columns=[],
        useful_dimensions=["category", "supplier_id", "shelf_life_days"],
        useful_metrics=["unit_cost", "unit_price", "lead_time_days", "target_service_level"],
        likely_pitfalls=[
            "target_service_level is policy target, not realized performance",
            "lead_time_days is static master value; use PO realized lead time for execution analytics",
        ],
        non_negative_columns=["unit_cost", "unit_price", "shelf_life_days", "lead_time_days"],
        bounded_columns={"target_service_level": (0.0, 1.0)},
        categorical_domains={},
    ),
    TableSpec(
        name="suppliers",
        file_name="suppliers.csv",
        grain="1 row per supplier_id",
        primary_key=["supplier_id"],
        date_columns=[],
        useful_dimensions=["supplier_region", "supplier_name"],
        useful_metrics=["reliability_score", "average_lead_time_days", "lead_time_variability", "minimum_order_qty"],
        likely_pitfalls=[
            "reliability_score is prior/master signal and should not replace observed OTD from transactions",
            "minimum_order_qty can drive overstock and should be used with demand context",
        ],
        non_negative_columns=["average_lead_time_days", "lead_time_variability", "minimum_order_qty"],
        bounded_columns={"reliability_score": (0.0, 1.0)},
        categorical_domains={},
    ),
    TableSpec(
        name="warehouses",
        file_name="warehouses.csv",
        grain="1 row per warehouse_id",
        primary_key=["warehouse_id"],
        date_columns=[],
        useful_dimensions=["region", "warehouse_name"],
        useful_metrics=["storage_capacity_units"],
        likely_pitfalls=[
            "capacity is static and should not be interpreted as effective usable capacity without utilization constraints",
        ],
        non_negative_columns=["storage_capacity_units"],
        bounded_columns={},
        categorical_domains={},
    ),
    TableSpec(
        name="inventory_snapshots",
        file_name="inventory_snapshots.csv",
        grain="1 row per snapshot_date + warehouse_id + product_id",
        primary_key=["snapshot_date", "warehouse_id", "product_id"],
        date_columns=["snapshot_date"],
        useful_dimensions=["warehouse_id", "product_id", "snapshot_date"],
        useful_metrics=["on_hand_units", "on_order_units", "reserved_units", "available_units", "inventory_value", "days_of_supply"],
        likely_pitfalls=[
            "days_of_supply can be unstable for low-demand SKUs and should be winsorized/segmented",
            "inventory_value is point-in-time and should be averaged for working-capital interpretation",
        ],
        non_negative_columns=["on_hand_units", "on_order_units", "reserved_units", "available_units", "inventory_value", "days_of_supply"],
        bounded_columns={},
        categorical_domains={},
    ),
    TableSpec(
        name="demand_history",
        file_name="demand_history.csv",
        grain="1 row per date + warehouse_id + product_id",
        primary_key=["date", "warehouse_id", "product_id"],
        date_columns=["date"],
        useful_dimensions=["warehouse_id", "product_id", "region", "promo_flag"],
        useful_metrics=["units_demanded", "units_fulfilled", "units_lost_sales", "stockout_flag", "seasonality_index"],
        likely_pitfalls=[
            "stockout_flag is event-level binary and should not be summed without denominator",
            "promo_flag should be controlled for when comparing service rates",
            "seasonality_index is a modeled driver and should not be treated as observed KPI",
        ],
        non_negative_columns=["units_demanded", "units_fulfilled", "units_lost_sales", "seasonality_index"],
        bounded_columns={},
        categorical_domains={"stockout_flag": {0, 1}, "promo_flag": {0, 1}},
    ),
    TableSpec(
        name="purchase_orders",
        file_name="purchase_orders.csv",
        grain="1 row per po_id",
        primary_key=["po_id"],
        date_columns=["order_date", "expected_arrival_date", "actual_arrival_date"],
        useful_dimensions=["supplier_id", "warehouse_id", "product_id"],
        useful_metrics=["ordered_units", "received_units", "late_delivery_flag"],
        likely_pitfalls=[
            "late_delivery_flag is not volume-weighted; combine with ordered_units for impact",
            "received_units timing can lag and should be aligned with arrival-date cohorts",
        ],
        non_negative_columns=["ordered_units", "received_units"],
        bounded_columns={},
        categorical_domains={"late_delivery_flag": {0, 1}},
    ),
    TableSpec(
        name="product_classification",
        file_name="product_classification.csv",
        grain="1 row per product_id",
        primary_key=["product_id"],
        date_columns=[],
        useful_dimensions=["abc_class", "criticality_level"],
        useful_metrics=[],
        likely_pitfalls=[
            "ABC class is static in this dataset and should be refreshed if demand mix changes",
        ],
        non_negative_columns=[],
        bounded_columns={},
        categorical_domains={"abc_class": {"A", "B", "C"}, "criticality_level": {"Low", "Medium", "High"}},
    ),
]


def _load_tables() -> dict[str, pd.DataFrame]:
    date_map = {
        "inventory_snapshots": ["snapshot_date"],
        "demand_history": ["date"],
        "purchase_orders": ["order_date", "expected_arrival_date", "actual_arrival_date"],
    }
    tables: dict[str, pd.DataFrame] = {}
    for spec in TABLE_SPECS:
        parse_dates = date_map.get(spec.name, None)
        tables[spec.name] = pd.read_csv(RAW_DIR / spec.file_name, parse_dates=parse_dates)
    return tables


def _profile_table_summary(tables: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict] = []
    null_rows: list[dict] = []
    cardinality_rows: list[dict] = []
    date_rows: list[dict] = []
    distribution_rows: list[dict] = []

    for spec in TABLE_SPECS:
        df = tables[spec.name]
        row_count = len(df)
        col_count = len(df.columns)

        pk_distinct = df[spec.primary_key].drop_duplicates().shape[0]
        duplicate_key_rows = max(row_count - pk_distinct, 0)

        total_cells = max(row_count * col_count, 1)
        total_null_cells = int(df.isna().sum().sum())
        overall_null_rate = total_null_cells / total_cells

        impossible_count = 0
        for col in spec.non_negative_columns:
            if col in df.columns:
                impossible_count += int((df[col] < 0).sum())
        for col, (low, high) in spec.bounded_columns.items():
            if col in df.columns:
                impossible_count += int(((df[col] < low) | (df[col] > high)).sum())
        for col, domain in spec.categorical_domains.items():
            if col in df.columns:
                impossible_count += int((~df[col].isin(domain)).sum())

        summary_rows.append(
            {
                "table_name": spec.name,
                "grain": spec.grain,
                "likely_primary_key": ", ".join(spec.primary_key),
                "row_count": row_count,
                "column_count": col_count,
                "duplicate_key_rows": duplicate_key_rows,
                "overall_null_rate": round(overall_null_rate, 6),
                "impossible_value_rows": impossible_count,
                "useful_dimensions": "; ".join(spec.useful_dimensions),
                "useful_metrics": "; ".join(spec.useful_metrics),
                "likely_analytical_pitfalls": "; ".join(spec.likely_pitfalls),
            }
        )

        for col in df.columns:
            null_rows.append(
                {
                    "table_name": spec.name,
                    "column_name": col,
                    "null_count": int(df[col].isna().sum()),
                    "null_rate": round(float(df[col].isna().mean()), 6),
                }
            )

            if (
                pd.api.types.is_object_dtype(df[col])
                or isinstance(df[col].dtype, pd.CategoricalDtype)
                or col.endswith("_id")
            ):
                non_null = df[col].dropna()
                nunique = int(non_null.nunique())
                cardinality_rows.append(
                    {
                        "table_name": spec.name,
                        "column_name": col,
                        "distinct_values": nunique,
                        "cardinality_ratio": round((nunique / max(len(non_null), 1)), 6),
                        "top_value": None if non_null.empty else non_null.value_counts().index[0],
                        "top_value_share": 0.0 if non_null.empty else round(float(non_null.value_counts(normalize=True).iloc[0]), 6),
                    }
                )

            if pd.api.types.is_numeric_dtype(df[col]):
                col_series = df[col].dropna()
                if len(col_series) == 0:
                    continue
                p50 = float(col_series.quantile(0.50))
                p95 = float(col_series.quantile(0.95))
                p99 = float(col_series.quantile(0.99))
                skew = float(col_series.skew()) if len(col_series) > 2 else 0.0
                distribution_rows.append(
                    {
                        "table_name": spec.name,
                        "column_name": col,
                        "min_value": float(col_series.min()),
                        "p50_value": p50,
                        "p95_value": p95,
                        "p99_value": p99,
                        "max_value": float(col_series.max()),
                        "mean_value": float(col_series.mean()),
                        "std_dev": float(col_series.std(ddof=1)) if len(col_series) > 1 else 0.0,
                        "skewness": skew,
                        "p99_to_p50_ratio": 0.0 if p50 == 0 else float(p99 / p50),
                    }
                )

        for date_col in spec.date_columns:
            series = df[date_col]
            min_dt = pd.to_datetime(series.min())
            max_dt = pd.to_datetime(series.max())
            distinct_dates = int(series.nunique())
            expected_days = int((max_dt - min_dt).days + 1)
            missing_days = max(expected_days - distinct_dates, 0)
            date_rows.append(
                {
                    "table_name": spec.name,
                    "date_column": date_col,
                    "min_date": min_dt.date().isoformat(),
                    "max_date": max_dt.date().isoformat(),
                    "distinct_dates": distinct_dates,
                    "expected_days": expected_days,
                    "missing_days": missing_days,
                }
            )

    return (
        pd.DataFrame(summary_rows),
        pd.DataFrame(null_rows),
        pd.DataFrame(cardinality_rows),
        pd.DataFrame(date_rows),
        pd.DataFrame(distribution_rows),
    )


def _fk_and_join_risks(tables: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    fk_rows: list[dict] = []
    join_risk_rows: list[dict] = []

    fk_specs = [
        ("products", "supplier_id", "suppliers", "supplier_id"),
        ("inventory_snapshots", "product_id", "products", "product_id"),
        ("inventory_snapshots", "warehouse_id", "warehouses", "warehouse_id"),
        ("demand_history", "product_id", "products", "product_id"),
        ("demand_history", "warehouse_id", "warehouses", "warehouse_id"),
        ("purchase_orders", "supplier_id", "suppliers", "supplier_id"),
        ("purchase_orders", "product_id", "products", "product_id"),
        ("purchase_orders", "warehouse_id", "warehouses", "warehouse_id"),
        ("product_classification", "product_id", "products", "product_id"),
    ]

    for child_table, child_col, parent_table, parent_col in fk_specs:
        child = tables[child_table]
        parent_keys = set(tables[parent_table][parent_col].dropna().astype(str).unique())
        child_vals = child[child_col].dropna().astype(str)
        missing_mask = ~child_vals.isin(parent_keys)
        missing_count = int(missing_mask.sum())
        fk_rows.append(
            {
                "child_table": child_table,
                "child_column": child_col,
                "parent_table": parent_table,
                "parent_column": parent_col,
                "missing_fk_rows": missing_count,
                "missing_fk_rate": round(missing_count / max(len(child_vals), 1), 6),
                "status": "PASS" if missing_count == 0 else "FAIL",
            }
        )

    demand_keys = tables["demand_history"][["date", "warehouse_id", "product_id"]].drop_duplicates()
    inv_keys = tables["inventory_snapshots"][["snapshot_date", "warehouse_id", "product_id"]].drop_duplicates()
    inv_keys = inv_keys.rename(columns={"snapshot_date": "date"})

    demand_without_inventory = demand_keys.merge(inv_keys, on=["date", "warehouse_id", "product_id"], how="left", indicator=True)
    demand_missing_count = int((demand_without_inventory["_merge"] == "left_only").sum())

    inventory_without_demand = inv_keys.merge(demand_keys, on=["date", "warehouse_id", "product_id"], how="left", indicator=True)
    inventory_missing_count = int((inventory_without_demand["_merge"] == "left_only").sum())

    join_risk_rows.append(
        {
            "risk_name": "demand_rows_without_inventory_snapshot",
            "issue_count": demand_missing_count,
            "severity": "High" if demand_missing_count > 0 else "Info",
            "details": "Left join drops service rows if inventory snapshot missing on same day-SKU-warehouse key.",
        }
    )
    join_risk_rows.append(
        {
            "risk_name": "inventory_rows_without_demand_row",
            "issue_count": inventory_missing_count,
            "severity": "Medium" if inventory_missing_count > 0 else "Info",
            "details": "Inventory-only rows can distort DOS averages if naively joined to demand facts.",
        }
    )

    demand_region_check = tables["demand_history"].merge(
        tables["warehouses"][["warehouse_id", "region"]].rename(columns={"region": "warehouse_region"}),
        on="warehouse_id",
        how="left",
    )
    region_mismatch_count = int((demand_region_check["region"] != demand_region_check["warehouse_region"]).sum())
    join_risk_rows.append(
        {
            "risk_name": "demand_region_vs_warehouse_region_mismatch",
            "issue_count": region_mismatch_count,
            "severity": "High" if region_mismatch_count > 0 else "Info",
            "details": "Region attribute mismatch can cause wrong regional service rollups.",
        }
    )

    po_mismatch = tables["purchase_orders"].merge(
        tables["products"][["product_id", "supplier_id"]].rename(columns={"supplier_id": "master_supplier_id"}),
        on="product_id",
        how="left",
    )
    po_supplier_mismatch_count = int((po_mismatch["supplier_id"] != po_mismatch["master_supplier_id"]).sum())
    join_risk_rows.append(
        {
            "risk_name": "po_supplier_vs_product_master_supplier_mismatch",
            "issue_count": po_supplier_mismatch_count,
            "severity": "High" if po_supplier_mismatch_count > 0 else "Info",
            "details": "Supplier attribution for lead-time and service impact can be wrong if PO supplier differs from product master supplier.",
        }
    )

    return pd.DataFrame(fk_rows), pd.DataFrame(join_risk_rows)


def _time_series_and_signal_checks(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    issues: list[dict] = []

    products_n = tables["products"]["product_id"].nunique()
    warehouses_n = tables["warehouses"]["warehouse_id"].nunique()

    demand = tables["demand_history"]
    inv = tables["inventory_snapshots"]

    demand_days = demand["date"].nunique()
    inv_days = inv["snapshot_date"].nunique()

    expected_demand_rows = products_n * warehouses_n * demand_days
    expected_inv_rows = products_n * warehouses_n * inv_days

    issues.append(
        {
            "issue_name": "demand_time_series_row_completeness",
            "observed_rows": int(len(demand)),
            "expected_rows": int(expected_demand_rows),
            "gap_rows": int(expected_demand_rows - len(demand)),
            "severity": "High" if len(demand) != expected_demand_rows else "Info",
            "details": "Checks full daily cube coverage by date-warehouse-product for demand history.",
        }
    )
    issues.append(
        {
            "issue_name": "inventory_time_series_row_completeness",
            "observed_rows": int(len(inv)),
            "expected_rows": int(expected_inv_rows),
            "gap_rows": int(expected_inv_rows - len(inv)),
            "severity": "High" if len(inv) != expected_inv_rows else "Info",
            "details": "Checks full daily cube coverage by date-warehouse-product for inventory snapshots.",
        }
    )

    supplier_po_counts = (
        tables["purchase_orders"].groupby("supplier_id")["po_id"].count().rename("po_count").reset_index()
    )
    supplier_signal = tables["suppliers"][["supplier_id"]].merge(supplier_po_counts, on="supplier_id", how="left").fillna(0)

    zero_po_suppliers = int((supplier_signal["po_count"] == 0).sum())
    low_po_suppliers = int((supplier_signal["po_count"] < 100).sum())

    issues.append(
        {
            "issue_name": "missing_supplier_signals_zero_po",
            "observed_rows": zero_po_suppliers,
            "expected_rows": 0,
            "gap_rows": zero_po_suppliers,
            "severity": "Medium" if zero_po_suppliers > 0 else "Info",
            "details": "Suppliers with zero transactions have no observed lead-time or lateness signal.",
        }
    )
    issues.append(
        {
            "issue_name": "weak_supplier_signal_low_po_count",
            "observed_rows": low_po_suppliers,
            "expected_rows": 0,
            "gap_rows": low_po_suppliers,
            "severity": "Low" if low_po_suppliers > 0 else "Info",
            "details": "Supplier comparisons are noisy when PO volume is too low.",
        }
    )

    dos = inv["days_of_supply"].dropna()
    inv_value = inv["inventory_value"].dropna()

    dos_p50 = float(dos.quantile(0.50))
    dos_p99 = float(dos.quantile(0.99))
    dos_skew_ratio = 0.0 if dos_p50 == 0 else dos_p99 / dos_p50

    value_p50 = float(inv_value.quantile(0.50))
    value_p99 = float(inv_value.quantile(0.99))
    value_skew_ratio = 0.0 if value_p50 == 0 else value_p99 / value_p50

    issues.append(
        {
            "issue_name": "skewed_days_of_supply_distribution",
            "observed_rows": round(dos_skew_ratio, 3),
            "expected_rows": 8.0,
            "gap_rows": round(dos_skew_ratio - 8.0, 3),
            "severity": "Medium" if dos_skew_ratio > 8.0 else "Info",
            "details": "High p99/p50 DOS ratio indicates heavy right tail and potential overstock outliers.",
        }
    )

    issues.append(
        {
            "issue_name": "skewed_inventory_value_distribution",
            "observed_rows": round(value_skew_ratio, 3),
            "expected_rows": 15.0,
            "gap_rows": round(value_skew_ratio - 15.0, 3),
            "severity": "Medium" if value_skew_ratio > 15.0 else "Info",
            "details": "Inventory value concentration can bias averages; use weighted and percentile views.",
        }
    )

    return pd.DataFrame(issues)


def _build_quality_issues(
    table_summary: pd.DataFrame,
    fk_checks: pd.DataFrame,
    join_risks: pd.DataFrame,
    ts_signal: pd.DataFrame,
    sql_checks: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict] = []

    for row in table_summary.itertuples():
        if row.duplicate_key_rows > 0:
            rows.append(
                {
                    "issue_category": "duplicates",
                    "severity": "High",
                    "object_name": row.table_name,
                    "issue": "Duplicate rows on likely primary key",
                    "issue_count": int(row.duplicate_key_rows),
                    "recommended_rule": "Deduplicate on key with deterministic latest-record rule before KPI rollups.",
                }
            )
        if row.impossible_value_rows > 0:
            rows.append(
                {
                    "issue_category": "impossible_values",
                    "severity": "High",
                    "object_name": row.table_name,
                    "issue": "Impossible numeric/domain values detected",
                    "issue_count": int(row.impossible_value_rows),
                    "recommended_rule": "Filter invalid records and quarantine for root-cause analysis.",
                }
            )

    for row in fk_checks.itertuples():
        if row.missing_fk_rows > 0:
            rows.append(
                {
                    "issue_category": "foreign_key",
                    "severity": "High",
                    "object_name": f"{row.child_table}.{row.child_column}",
                    "issue": f"Missing parent key in {row.parent_table}.{row.parent_column}",
                    "issue_count": int(row.missing_fk_rows),
                    "recommended_rule": "Use referential integrity filters; route orphan rows to exception table.",
                }
            )

    for row in join_risks.itertuples():
        if row.issue_count > 0:
            rows.append(
                {
                    "issue_category": "join_risk",
                    "severity": row.severity,
                    "object_name": row.risk_name,
                    "issue": row.details,
                    "issue_count": int(row.issue_count),
                    "recommended_rule": "Apply pre-join row reconciliation and explicit anti-join exception reporting.",
                }
            )

    for row in ts_signal.itertuples():
        if row.severity != "Info":
            rows.append(
                {
                    "issue_category": "time_series_or_signal",
                    "severity": row.severity,
                    "object_name": row.issue_name,
                    "issue": row.details,
                    "issue_count": row.observed_rows,
                    "recommended_rule": "Use coverage thresholds and minimum sample gates in KPI dashboards.",
                }
            )

    for row in sql_checks.itertuples():
        if row.status in {"FAIL", "WARN"} and row.issue_count > 0:
            rows.append(
                {
                    "issue_category": "sql_validation",
                    "severity": "High" if row.status == "FAIL" else "Low",
                    "object_name": row.check_name,
                    "issue": "Validation check did not pass",
                    "issue_count": int(row.issue_count),
                    "recommended_rule": "Block executive reporting until FAIL checks are cleared; monitor WARN checks.",
                }
            )

    if not rows:
        return pd.DataFrame(
            [
                {
                    "issue_category": "none",
                    "severity": "Info",
                    "object_name": "dataset",
                    "issue": "No blocking quality issues detected.",
                    "issue_count": 0,
                    "recommended_rule": "Proceed with analysis using documented caveats.",
                }
            ]
        )

    out = pd.DataFrame(rows)
    severity_rank = {"High": 1, "Medium": 2, "Low": 3, "Info": 4}
    out["_severity_rank"] = out["severity"].map(severity_rank).fillna(99)
    out = out.sort_values(["_severity_rank", "issue_count"], ascending=[True, False]).drop(columns=["_severity_rank"])
    return out


def _run_sql_validation(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    sql_path = SQL_DIR / "05_data_profile_validation.sql"
    con = duckdb.connect(database=":memory:")
    try:
        for name, df in tables.items():
            con.register(f"tmp_{name}", df)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmp_{name};")

        checks = con.execute(sql_path.read_text()).df()
        return checks
    finally:
        con.close()


def _df_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no rows)"
    cols = list(df.columns)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    rows = []
    for _, row in df.iterrows():
        rows.append("| " + " | ".join(str(row[c]) for c in cols) + " |")
    return "\n".join([header, sep, *rows])


def _write_report(
    table_summary: pd.DataFrame,
    quality_issues: pd.DataFrame,
    join_risks: pd.DataFrame,
    ts_signal: pd.DataFrame,
    sql_checks: pd.DataFrame,
) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    fail_count = int((sql_checks["status"] == "FAIL").sum())
    warn_count = int((sql_checks["status"] == "WARN").sum())

    explicit_flags = [
        "Potential join risks are explicitly quantified in outputs/tables/data_profile_join_risks.csv.",
        "Time-series continuity is checked at date-level and full cube completeness for demand/inventory.",
        "Inventory skew is flagged using p99/p50 ratios for days_of_supply and inventory_value.",
        "Missing supplier signal risk is checked through zero/low PO coverage by supplier.",
        "Fields not to use naively: stockout_flag (binary event), target_service_level (policy target), reliability_score (prior score), late_delivery_flag (not volume-weighted), days_of_supply (unstable at low demand).",
    ]

    recommended_rules = [
        "Enforce key uniqueness on all table grains before any KPI aggregation.",
        "Reject or quarantine rows with impossible values (negative units/value, invalid flags).",
        "Use anti-join exception tables before fact-to-fact joins (demand vs inventory).",
        "For supplier scorecards, require minimum PO count threshold before ranking suppliers.",
        "Winsorize or percentile-cap days_of_supply when computing portfolio averages.",
        "For service KPIs, use weighted rates with denominators; do not average row-level flags directly.",
    ]

    analytical_focus = [
        "Service-risk segmentation by warehouse and ABC class with denominator-controlled fill-rate trends.",
        "Inventory working-capital concentration: top decile SKUs by average inventory value and DOS outliers.",
        "Supplier execution diagnostics: on-time rate, lead-time variability, and downstream lost-sales coupling.",
        "Dual-failure lens: SKU-warehouse combinations showing both high stockout-day rate and excess DOS.",
        "Promo-adjusted service analysis separating baseline vs promo periods.",
    ]

    lines = [
        "# Data Profile and Quality Audit",
        "",
        "Project: Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System",
        "",
        "## 1) Data Profile Summary",
        "",
        _df_to_markdown(
            table_summary[
                [
                    "table_name",
                    "grain",
                    "likely_primary_key",
                    "row_count",
                    "duplicate_key_rows",
                    "overall_null_rate",
                    "impossible_value_rows",
                ]
            ]
        ),
        "",
        "## 2) Data Quality Issues",
        "",
        _df_to_markdown(quality_issues.head(25)),
        "",
        "SQL validation status summary:",
        f"- FAIL checks: {fail_count}",
        f"- WARN checks: {warn_count}",
        f"- PASS checks: {int((sql_checks['status'] == 'PASS').sum())}",
        "",
        "## 3) Risks to Downstream Interpretation",
        "",
    ]

    for flag in explicit_flags:
        lines.append(f"- {flag}")

    lines.extend(["", "Top join/time-series risk indicators:", "", _df_to_markdown(join_risks), "", _df_to_markdown(ts_signal), ""])

    lines.append("## 4) Recommended Cleaning / Handling Rules")
    lines.append("")
    for rule in recommended_rules:
        lines.append(f"- {rule}")

    lines.extend(["", "## 5) Recommended Analytical Focus Areas", ""])
    for focus in analytical_focus:
        lines.append(f"- {focus}")

    lines.extend(
        [
            "",
            "## Appendix: Table-Level Modeling Notes",
            "",
            _df_to_markdown(
                table_summary[
                    [
                        "table_name",
                        "useful_dimensions",
                        "useful_metrics",
                        "likely_analytical_pitfalls",
                    ]
                ]
            ),
        ]
    )

    (DOCS_DIR / "data_profile.md").write_text("\n".join(lines), encoding="utf-8")


def run_audit() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    tables = _load_tables()

    table_summary, null_profile, cardinality_profile, date_coverage, distribution_profile = _profile_table_summary(tables)
    fk_checks, join_risks = _fk_and_join_risks(tables)
    ts_signal = _time_series_and_signal_checks(tables)
    sql_checks = _run_sql_validation(tables)

    quality_issues = _build_quality_issues(
        table_summary=table_summary,
        fk_checks=fk_checks,
        join_risks=join_risks,
        ts_signal=ts_signal,
        sql_checks=sql_checks,
    )

    table_summary.to_csv(OUTPUT_TABLES_DIR / "data_profile_table_summary.csv", index=False)
    null_profile.to_csv(OUTPUT_TABLES_DIR / "data_profile_column_nulls.csv", index=False)
    cardinality_profile.to_csv(OUTPUT_TABLES_DIR / "data_profile_cardinality.csv", index=False)
    date_coverage.to_csv(OUTPUT_TABLES_DIR / "data_profile_date_coverage.csv", index=False)
    distribution_profile.to_csv(OUTPUT_TABLES_DIR / "data_profile_distribution_summary.csv", index=False)
    fk_checks.to_csv(OUTPUT_TABLES_DIR / "data_profile_fk_checks.csv", index=False)
    join_risks.to_csv(OUTPUT_TABLES_DIR / "data_profile_join_risks.csv", index=False)
    ts_signal.to_csv(OUTPUT_TABLES_DIR / "data_profile_time_series_signal_checks.csv", index=False)
    sql_checks.to_csv(OUTPUT_TABLES_DIR / "data_profile_sql_validation_checks.csv", index=False)
    quality_issues.to_csv(OUTPUT_TABLES_DIR / "data_profile_quality_issues.csv", index=False)

    _write_report(
        table_summary=table_summary,
        quality_issues=quality_issues,
        join_risks=join_risks,
        ts_signal=ts_signal,
        sql_checks=sql_checks,
    )

    print("Data profiling audit complete.")
    print(f"Output tables saved to: {OUTPUT_TABLES_DIR}")
    print(f"Audit report saved to: {DOCS_DIR / 'data_profile.md'}")
    print(f"High-severity issues identified: {(quality_issues['severity'] == 'High').sum()}")


if __name__ == "__main__":
    run_audit()
