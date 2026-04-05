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


SQL = """
WITH lane AS (
    SELECT
        d.supplier_id,
        s.supplier_name,
        d.warehouse_id,
        w.warehouse_name,
        d.category,
        SUM(d.units_demanded) AS units_demanded,
        SUM(d.units_fulfilled) AS units_fulfilled,
        SUM(d.units_lost_sales) AS units_lost_sales,
        SUM(d.lost_sales_revenue) AS lost_sales_revenue,
        AVG(d.days_of_supply) AS avg_days_of_supply,
        AVG(d.fill_rate) AS avg_row_fill_rate,
        AVG(d.stockout_flag) AS stockout_day_rate,
        AVG(
            CASE
                WHEN d.abc_class = 'A' AND d.days_of_supply > 20 THEN 1.0
                WHEN d.abc_class = 'B' AND d.days_of_supply > 30 THEN 1.0
                WHEN d.abc_class NOT IN ('A', 'B') AND d.days_of_supply > 45 THEN 1.0
                ELSE 0.0
            END
        ) AS excess_day_rate
    FROM daily_product_warehouse_metrics d
    LEFT JOIN supplier_performance_summary s
        ON d.supplier_id = s.supplier_id
    LEFT JOIN warehouses w
        ON d.warehouse_id = w.warehouse_id
    GROUP BY d.supplier_id, s.supplier_name, d.warehouse_id, w.warehouse_name, d.category
),
joined AS (
    SELECT
        lane.*,
        sp.on_time_delivery_rate,
        sp.average_delay_days,
        sp.lead_time_variability,
        sp.received_vs_ordered_fill_rate,
        sp.supplier_service_risk_proxy
    FROM lane
    LEFT JOIN supplier_performance_summary sp
      ON lane.supplier_id = sp.supplier_id
),
scored AS (
    SELECT
        *,
        CASE WHEN units_demanded = 0 THEN 1.0 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS downstream_fill_rate,
        CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS downstream_stockout_rate
    FROM joined
)
SELECT
    supplier_id,
    supplier_name,
    warehouse_id,
    warehouse_name,
    category,
    units_demanded,
    units_fulfilled,
    units_lost_sales,
    downstream_fill_rate,
    downstream_stockout_rate,
    stockout_day_rate,
    lost_sales_revenue,
    avg_days_of_supply,
    excess_day_rate,
    on_time_delivery_rate,
    average_delay_days,
    lead_time_variability,
    received_vs_ordered_fill_rate,
    supplier_service_risk_proxy,
    100.0 * (
        0.32 * LEAST(downstream_stockout_rate / 0.18, 1.0)
      + 0.20 * LEAST((1.0 - downstream_fill_rate) / 0.15, 1.0)
      + 0.20 * LEAST(COALESCE(supplier_service_risk_proxy, 0.0) / 100.0, 1.0)
      + 0.16 * LEAST(excess_day_rate / 0.40, 1.0)
      + 0.12 * LEAST(lost_sales_revenue / 2500000.0, 1.0)
    ) AS supplier_lane_risk_score
FROM scored
ORDER BY supplier_lane_risk_score DESC, lost_sales_revenue DESC
"""


def _load_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE daily_product_warehouse_metrics AS
        SELECT * FROM read_csv_auto('{(DATA_PROCESSED / 'daily_product_warehouse_metrics.csv').as_posix()}', HEADER=TRUE);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE supplier_performance_summary AS
        SELECT * FROM read_csv_auto('{(DATA_PROCESSED / 'supplier_performance_summary.csv').as_posix()}', HEADER=TRUE);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE warehouses AS
        SELECT * FROM read_csv_auto('{(DATA_RAW / 'warehouses.csv').as_posix()}', HEADER=TRUE);
        """
    )


def _create_chart(df: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    top = df.head(25).copy()
    top["lane"] = top["supplier_id"] + " | " + top["warehouse_id"] + " | " + top["category"]

    plt.figure(figsize=(12, 9))
    sns.barplot(data=top, y="lane", x="supplier_lane_risk_score", color="#0D5C7A")
    plt.xlabel("Supplier-Lane Risk Score")
    plt.ylabel("Supplier | Warehouse | Category")
    plt.title("Supplier Lane Diagnostics: Highest-Risk Supplier-Warehouse-Category Lanes")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "supplier_lane_top_risk_lanes.png", dpi=180)
    plt.close()


def _write_summary(df: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    top = df.iloc[0]
    top5_lost = float(df.head(5)["lost_sales_revenue"].sum())

    lines = [
        "# Supplier Lane Diagnostics Summary",
        "",
        "Lane diagnostics isolate where supplier execution risk intersects with warehouse-category service failure.",
        "",
        "## Key Signals",
        f"- Highest-risk lane: **{top['supplier_id']} | {top['warehouse_id']} | {top['category']}**.",
        f"- Lane risk score: **{top['supplier_lane_risk_score']:.2f}**.",
        f"- Downstream stockout rate: **{top['downstream_stockout_rate']:.2%}**.",
        f"- Downstream lost-sales value: **EUR {top['lost_sales_revenue']:,.0f}**.",
        f"- Top-5 lane lost-sales concentration: **EUR {top5_lost:,.0f}**.",
        "",
        "## Operational Use",
        "- Use this output as weekly procurement + operations exception list.",
        "- Prioritize lanes that combine high supplier-lane risk score and high lost-sales value.",
    ]

    (OUTPUT_REPORTS_DIR / "supplier_lane_diagnostics_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_supplier_lane_diagnostics() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        _load_tables(con)
        df = con.execute(SQL).df()
    finally:
        con.close()

    df.to_csv(OUTPUT_TABLES_DIR / "supplier_lane_diagnostics.csv", index=False)

    summary = (
        df.groupby("supplier_id", as_index=False)
        .agg(
            lane_count=("warehouse_id", "count"),
            avg_lane_risk_score=("supplier_lane_risk_score", "mean"),
            total_lane_lost_sales=("lost_sales_revenue", "sum"),
        )
        .sort_values(["avg_lane_risk_score", "total_lane_lost_sales"], ascending=[False, False])
    )
    summary.to_csv(OUTPUT_TABLES_DIR / "supplier_lane_supplier_summary.csv", index=False)

    _create_chart(df)
    _write_summary(df)

    print("Supplier lane diagnostics complete.")
    print(f"Lane rows: {len(df):,}")
    print(f"Supplier rows: {len(summary):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_supplier_lane_diagnostics()
