from __future__ import annotations

import duckdb
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


SQL_COHORT = """
WITH po AS (
    SELECT
        supplier_id,
        warehouse_id,
        product_id,
        DATE_TRUNC('month', order_date) AS cohort_month,
        ordered_units,
        received_units,
        late_delivery_flag,
        CAST((actual_arrival_date - expected_arrival_date) AS DOUBLE) AS delay_days,
        CAST((actual_arrival_date - order_date) AS DOUBLE) AS realized_lead_time_days
    FROM purchase_orders
),
po_cohort AS (
    SELECT
        supplier_id,
        warehouse_id,
        cohort_month,
        COUNT(*) AS po_count,
        SUM(ordered_units) AS ordered_units,
        SUM(received_units) AS received_units,
        CASE WHEN SUM(ordered_units) = 0 THEN 1.0 ELSE CAST(SUM(received_units) AS DOUBLE) / CAST(SUM(ordered_units) AS DOUBLE) END AS receipt_fill_rate,
        AVG(CASE WHEN late_delivery_flag = 0 THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(GREATEST(delay_days, 0.0)) AS average_delay_days,
        COALESCE(STDDEV_SAMP(realized_lead_time_days), 0.0) AS lead_time_variability,
        SUM(CASE WHEN late_delivery_flag = 1 THEN received_units ELSE 0 END) AS late_received_units
    FROM po
    GROUP BY supplier_id, warehouse_id, cohort_month
),
downstream AS (
    SELECT
        supplier_id,
        warehouse_id,
        DATE_TRUNC('month', date) AS cohort_month,
        SUM(units_demanded) AS units_demanded,
        SUM(units_lost_sales) AS units_lost_sales,
        SUM(lost_sales_revenue) AS lost_sales_revenue,
        CASE WHEN SUM(units_demanded) = 0 THEN 0.0 ELSE CAST(SUM(units_lost_sales) AS DOUBLE) / CAST(SUM(units_demanded) AS DOUBLE) END AS downstream_stockout_rate,
        CASE WHEN SUM(units_demanded) = 0 THEN 1.0 ELSE CAST(SUM(units_demanded - units_lost_sales) AS DOUBLE) / CAST(SUM(units_demanded) AS DOUBLE) END AS downstream_fill_rate
    FROM daily_product_warehouse_metrics
    GROUP BY supplier_id, warehouse_id, cohort_month
)
SELECT
    p.supplier_id,
    s.supplier_name,
    p.warehouse_id,
    w.warehouse_name,
    p.cohort_month,
    p.po_count,
    p.ordered_units,
    p.received_units,
    p.receipt_fill_rate,
    p.on_time_delivery_rate,
    p.average_delay_days,
    p.lead_time_variability,
    p.late_received_units,
    COALESCE(d.units_demanded, 0) AS downstream_units_demanded,
    COALESCE(d.units_lost_sales, 0) AS downstream_units_lost_sales,
    COALESCE(d.lost_sales_revenue, 0.0) AS downstream_lost_sales_revenue,
    COALESCE(d.downstream_stockout_rate, 0.0) AS downstream_stockout_rate,
    COALESCE(d.downstream_fill_rate, 1.0) AS downstream_fill_rate,
    100.0 * (
        0.30 * LEAST((1.0 - p.on_time_delivery_rate) / 0.45, 1.0)
      + 0.20 * LEAST(p.average_delay_days / 8.0, 1.0)
      + 0.15 * LEAST(p.lead_time_variability / 10.0, 1.0)
      + 0.20 * LEAST(COALESCE(d.downstream_stockout_rate, 0.0) / 0.18, 1.0)
      + 0.15 * LEAST(COALESCE(d.lost_sales_revenue, 0.0) / 2000000.0, 1.0)
    ) AS cohort_risk_score
FROM po_cohort p
LEFT JOIN downstream d
  ON p.supplier_id = d.supplier_id
 AND p.warehouse_id = d.warehouse_id
 AND p.cohort_month = d.cohort_month
LEFT JOIN suppliers s
  ON p.supplier_id = s.supplier_id
LEFT JOIN warehouses w
  ON p.warehouse_id = w.warehouse_id
ORDER BY cohort_risk_score DESC, downstream_lost_sales_revenue DESC
"""


def _load_tables(con: duckdb.DuckDBPyConnection) -> None:
    for table_name, path in {
        "purchase_orders": DATA_RAW / "purchase_orders.csv",
        "suppliers": DATA_RAW / "suppliers.csv",
        "warehouses": DATA_RAW / "warehouses.csv",
        "daily_product_warehouse_metrics": DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
    }.items():
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE);
            """
        )


def _plot(df: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    top = df.head(20).copy()
    top["cohort_label"] = (
        top["supplier_id"] + " | " + top["warehouse_id"] + " | " + pd.to_datetime(top["cohort_month"]).dt.strftime("%Y-%m")
    )

    plt.figure(figsize=(12, 8))
    sns.barplot(data=top, y="cohort_label", x="cohort_risk_score", color="#7B341E")
    plt.xlabel("Cohort Risk Score")
    plt.ylabel("Supplier | Warehouse | Cohort Month")
    plt.title("PO Cohort Diagnostics: Highest-Risk Supplier-Warehouse Cohorts")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "po_cohort_top_risk_cohorts.png", dpi=180)
    plt.close()


def _write_summary(df: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    top = df.iloc[0]
    lines = [
        "# PO Cohort Diagnostics Summary",
        "",
        "This analysis decomposes supplier risk by monthly PO cohorts at supplier-warehouse lane level.",
        "",
        "## Highest-Risk Cohort",
        f"- Cohort: **{top['supplier_id']} | {top['warehouse_id']} | {pd.to_datetime(top['cohort_month']).strftime('%Y-%m')}**",
        f"- Cohort risk score: **{top['cohort_risk_score']:.2f}**",
        f"- OTD: **{top['on_time_delivery_rate']:.2%}**, Avg delay: **{top['average_delay_days']:.2f} days**",
        f"- Downstream stockout rate: **{top['downstream_stockout_rate']:.2%}**",
        f"- Downstream lost-sales exposure: **EUR {top['downstream_lost_sales_revenue']:,.0f}**",
        "",
        "## Use in Operations",
        "- Use cohort outputs to separate persistent supplier risk from temporary shocks.",
        "- Prioritize corrective plans on cohorts with both weak PO execution and high downstream commercial impact.",
    ]

    (OUTPUT_REPORTS_DIR / "po_cohort_diagnostics_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_po_cohort_diagnostics() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        _load_tables(con)
        cohort = con.execute(SQL_COHORT).df()
    finally:
        con.close()

    cohort.to_csv(OUTPUT_TABLES_DIR / "po_cohort_diagnostics.csv", index=False)

    lane_summary = (
        cohort.groupby(["supplier_id", "warehouse_id"], as_index=False)
        .agg(
            cohort_count=("cohort_month", "nunique"),
            avg_cohort_risk_score=("cohort_risk_score", "mean"),
            total_downstream_lost_sales=("downstream_lost_sales_revenue", "sum"),
        )
        .sort_values(["avg_cohort_risk_score", "total_downstream_lost_sales"], ascending=[False, False])
    )
    lane_summary.to_csv(OUTPUT_TABLES_DIR / "po_cohort_lane_summary.csv", index=False)

    _plot(cohort)
    _write_summary(cohort)

    print("PO cohort diagnostics complete.")
    print(f"Cohort rows: {len(cohort):,}")
    print(f"Lane rows: {len(lane_summary):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_po_cohort_diagnostics()
