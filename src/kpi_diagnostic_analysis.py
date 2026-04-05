from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"


PROCESSED_TABLES = {
    "daily_product_warehouse_metrics": DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
    "supplier_performance_summary": DATA_PROCESSED / "supplier_performance_summary.csv",
    "product_inventory_profile": DATA_PROCESSED / "product_inventory_profile.csv",
    "warehouse_service_profile": DATA_PROCESSED / "warehouse_service_profile.csv",
    "sku_risk_table": DATA_PROCESSED / "sku_risk_table.csv",
}

RAW_TABLES = {
    "products": DATA_RAW / "products.csv",
    "suppliers": DATA_RAW / "suppliers.csv",
    "warehouses": DATA_RAW / "warehouses.csv",
    "purchase_orders": DATA_RAW / "purchase_orders.csv",
}


SQL_QUERIES: dict[str, str] = {
    "kpi_overall_service_health": """
        WITH base AS (
            SELECT
                SUM(units_demanded) AS total_units_demanded,
                SUM(units_fulfilled) AS total_units_fulfilled,
                SUM(units_lost_sales) AS total_units_lost_sales,
                SUM(lost_sales_revenue) AS total_lost_sales_revenue,
                SUM(service_gap_units) AS total_service_gap_units
            FROM daily_product_warehouse_metrics
        )
        SELECT
            total_units_demanded,
            total_units_fulfilled,
            total_units_lost_sales,
            CASE
                WHEN total_units_demanded = 0 THEN 1.0
                ELSE CAST(total_units_fulfilled AS DOUBLE) / CAST(total_units_demanded AS DOUBLE)
            END AS overall_fill_rate,
            CASE
                WHEN total_units_demanded = 0 THEN 0.0
                ELSE CAST(total_units_lost_sales AS DOUBLE) / CAST(total_units_demanded AS DOUBLE)
            END AS overall_stockout_rate,
            total_lost_sales_revenue,
            total_service_gap_units
        FROM base
    """,
    "kpi_service_over_time_monthly": """
        WITH monthly AS (
            SELECT
                DATE_TRUNC('month', date) AS month,
                SUM(units_demanded) AS units_demanded,
                SUM(units_fulfilled) AS units_fulfilled,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                AVG(days_of_supply) AS avg_days_of_supply,
                AVG(CASE WHEN promo_flag = 1 THEN 1.0 ELSE 0.0 END) AS promo_mix_rate
            FROM daily_product_warehouse_metrics
            GROUP BY 1
        )
        SELECT
            month,
            units_demanded,
            units_fulfilled,
            units_lost_sales,
            CASE WHEN units_demanded = 0 THEN 1.0 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS fill_rate,
            CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate,
            lost_sales_revenue,
            avg_days_of_supply,
            promo_mix_rate
        FROM monthly
        ORDER BY month
    """,
    "kpi_service_by_region": """
        WITH region_agg AS (
            SELECT
                region,
                SUM(units_demanded) AS units_demanded,
                SUM(units_fulfilled) AS units_fulfilled,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                AVG(fill_rate) AS avg_row_fill_rate,
                AVG(stockout_flag) AS stockout_day_rate
            FROM daily_product_warehouse_metrics
            GROUP BY region
        )
        SELECT
            region,
            units_demanded,
            units_fulfilled,
            units_lost_sales,
            CASE WHEN units_demanded = 0 THEN 1.0 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS fill_rate,
            CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate,
            stockout_day_rate,
            lost_sales_revenue,
            avg_row_fill_rate
        FROM region_agg
        ORDER BY lost_sales_revenue DESC
    """,
    "kpi_service_by_warehouse": """
        SELECT
            warehouse_id,
            warehouse_name,
            region,
            fill_rate,
            stockout_rate,
            lost_sales_value,
            average_days_of_supply,
            inventory_value,
            capacity_pressure_proxy,
            warehouse_service_risk_proxy
        FROM warehouse_service_profile
        ORDER BY warehouse_service_risk_proxy DESC, lost_sales_value DESC
    """,
    "kpi_service_by_category": """
        WITH category_agg AS (
            SELECT
                category,
                SUM(units_demanded) AS units_demanded,
                SUM(units_fulfilled) AS units_fulfilled,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                AVG(fill_rate) AS avg_row_fill_rate,
                AVG(stockout_flag) AS stockout_day_rate,
                AVG(days_of_supply) AS avg_days_of_supply,
                AVG(inventory_value) AS avg_inventory_value
            FROM daily_product_warehouse_metrics
            GROUP BY category
        )
        SELECT
            category,
            units_demanded,
            units_fulfilled,
            units_lost_sales,
            CASE WHEN units_demanded = 0 THEN 1.0 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS fill_rate,
            CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate,
            stockout_day_rate,
            lost_sales_revenue,
            avg_days_of_supply,
            avg_inventory_value,
            avg_row_fill_rate
        FROM category_agg
        ORDER BY lost_sales_revenue DESC
    """,
    "kpi_inventory_efficiency_summary": """
        WITH base AS (
            SELECT
                *,
                CASE
                    WHEN abc_class = 'A' THEN 20.0
                    WHEN abc_class = 'B' THEN 30.0
                    ELSE 45.0
                END AS dos_cap,
                CASE
                    WHEN available_units > 0 AND units_fulfilled = 0 THEN 1 ELSE 0
                END AS slow_moving_flag
            FROM daily_product_warehouse_metrics
        )
        SELECT
            AVG(days_of_supply) AS avg_days_of_supply,
            QUANTILE_CONT(days_of_supply, 0.50) AS median_days_of_supply,
            QUANTILE_CONT(days_of_supply, 0.90) AS p90_days_of_supply,
            QUANTILE_CONT(days_of_supply, 0.99) AS p99_days_of_supply,
            AVG(inventory_value) AS avg_inventory_value,
            QUANTILE_CONT(inventory_value, 0.95) AS p95_inventory_value,
            CAST(SUM(CASE WHEN days_of_supply > dos_cap THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS excess_inventory_day_rate,
            CAST(SUM(slow_moving_flag) AS DOUBLE) / COUNT(*) AS slow_moving_day_rate
        FROM base
    """,
    "kpi_days_of_supply_distribution": """
        WITH bucketed AS (
            SELECT
                CASE
                    WHEN days_of_supply < 5 THEN '00_<5'
                    WHEN days_of_supply < 10 THEN '01_5_to_10'
                    WHEN days_of_supply < 20 THEN '02_10_to_20'
                    WHEN days_of_supply < 35 THEN '03_20_to_35'
                    WHEN days_of_supply < 50 THEN '04_35_to_50'
                    ELSE '05_50_plus'
                END AS dos_bucket,
                days_of_supply
            FROM daily_product_warehouse_metrics
        )
        SELECT
            dos_bucket,
            COUNT(*) AS row_count,
            CAST(COUNT(*) AS DOUBLE) / SUM(COUNT(*)) OVER () AS row_share,
            AVG(days_of_supply) AS avg_dos_in_bucket
        FROM bucketed
        GROUP BY dos_bucket
        ORDER BY dos_bucket
    """,
    "kpi_inventory_value_concentration": """
        WITH sku_value AS (
            SELECT
                product_id,
                category,
                AVG(inventory_value) AS avg_inventory_value
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, category
        ),
        ranked AS (
            SELECT
                product_id,
                category,
                avg_inventory_value,
                ROW_NUMBER() OVER (ORDER BY avg_inventory_value DESC) AS value_rank,
                SUM(avg_inventory_value) OVER () AS total_inventory_value,
                SUM(avg_inventory_value) OVER (ORDER BY avg_inventory_value DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    AS cumulative_inventory_value,
                COUNT(*) OVER () AS sku_count
            FROM sku_value
        )
        SELECT
            product_id,
            category,
            avg_inventory_value,
            value_rank,
            CAST(value_rank AS DOUBLE) / sku_count AS rank_percentile,
            cumulative_inventory_value,
            total_inventory_value,
            cumulative_inventory_value / NULLIF(total_inventory_value, 0.0) AS cumulative_value_share
        FROM ranked
        ORDER BY value_rank
    """,
    "kpi_excess_and_slow_moving_exposure": """
        WITH base AS (
            SELECT
                category,
                product_id,
                CASE WHEN abc_class = 'A' THEN 20.0 WHEN abc_class = 'B' THEN 30.0 ELSE 45.0 END AS dos_cap,
                days_of_supply,
                inventory_value,
                available_units,
                units_fulfilled
            FROM daily_product_warehouse_metrics
        )
        SELECT
            category,
            AVG(CASE WHEN days_of_supply > dos_cap THEN 1.0 ELSE 0.0 END) AS excess_inventory_day_rate,
            AVG(CASE WHEN available_units > 0 AND units_fulfilled = 0 THEN 1.0 ELSE 0.0 END) AS slow_moving_day_rate,
            AVG(inventory_value) AS avg_inventory_value,
            SUM(CASE WHEN days_of_supply > dos_cap THEN inventory_value ELSE 0.0 END) AS excess_inventory_value_proxy
        FROM base
        GROUP BY category
        ORDER BY excess_inventory_value_proxy DESC
    """,
    "kpi_stockout_concentration_multilevel": """
        WITH warehouse_view AS (
            SELECT
                'warehouse' AS level,
                warehouse_id AS segment,
                SUM(units_demanded) AS units_demanded,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue
            FROM daily_product_warehouse_metrics
            GROUP BY warehouse_id
        ),
        region_view AS (
            SELECT
                'region' AS level,
                region AS segment,
                SUM(units_demanded) AS units_demanded,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue
            FROM daily_product_warehouse_metrics
            GROUP BY region
        ),
        category_view AS (
            SELECT
                'category' AS level,
                category AS segment,
                SUM(units_demanded) AS units_demanded,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue
            FROM daily_product_warehouse_metrics
            GROUP BY category
        )
        SELECT
            level,
            segment,
            units_demanded,
            units_lost_sales,
            CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate,
            lost_sales_revenue
        FROM (
            SELECT * FROM warehouse_view
            UNION ALL
            SELECT * FROM region_view
            UNION ALL
            SELECT * FROM category_view
        ) t
        ORDER BY lost_sales_revenue DESC
    """,
    "kpi_stockout_pattern_assessment": """
        WITH monthly_sku_wh AS (
            SELECT
                product_id,
                warehouse_id,
                DATE_TRUNC('month', date) AS month,
                SUM(units_demanded) AS units_demanded,
                SUM(units_lost_sales) AS units_lost_sales
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, warehouse_id, month
        ),
        sku_wh_summary AS (
            SELECT
                product_id,
                warehouse_id,
                CASE
                    WHEN SUM(units_demanded) = 0 THEN 0.0
                    ELSE CAST(SUM(units_lost_sales) AS DOUBLE) / CAST(SUM(units_demanded) AS DOUBLE)
                END AS stockout_rate,
                AVG(CASE WHEN units_lost_sales > 0 THEN 1.0 ELSE 0.0 END) AS stockout_active_month_ratio
            FROM monthly_sku_wh
            GROUP BY product_id, warehouse_id
        ),
        classified AS (
            SELECT
                product_id,
                warehouse_id,
                stockout_rate,
                stockout_active_month_ratio,
                CASE
                    WHEN stockout_rate >= 0.08 AND stockout_active_month_ratio >= 0.50 THEN 'Systematic'
                    WHEN stockout_rate >= 0.08 AND stockout_active_month_ratio < 0.50 THEN 'Episodic High-Intensity'
                    WHEN stockout_rate < 0.08 AND stockout_active_month_ratio >= 0.35 THEN 'Frequent Low-Intensity'
                    ELSE 'Low/Contained'
                END AS stockout_pattern
            FROM sku_wh_summary
        )
        SELECT
            stockout_pattern,
            COUNT(*) AS sku_warehouse_count,
            AVG(stockout_rate) AS avg_stockout_rate,
            AVG(stockout_active_month_ratio) AS avg_active_month_ratio
        FROM classified
        GROUP BY stockout_pattern
        ORDER BY avg_stockout_rate DESC
    """,
    "kpi_lost_sales_by_sku_warehouse": """
        WITH sku_wh AS (
            SELECT
                product_id,
                warehouse_id,
                region,
                category,
                SUM(units_demanded) AS units_demanded,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, warehouse_id, region, category
        )
        SELECT
            product_id,
            warehouse_id,
            region,
            category,
            units_demanded,
            units_lost_sales,
            CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate,
            lost_sales_revenue
        FROM sku_wh
        ORDER BY lost_sales_revenue DESC
        LIMIT 50
    """,
    "kpi_supplier_risk_with_downstream_impact": """
        WITH downstream AS (
            SELECT
                supplier_id,
                SUM(units_demanded) AS units_demanded,
                SUM(units_fulfilled) AS units_fulfilled,
                SUM(units_lost_sales) AS units_lost_sales,
                SUM(lost_sales_revenue) AS lost_sales_revenue
            FROM daily_product_warehouse_metrics
            GROUP BY supplier_id
        )
        SELECT
            s.supplier_id,
            s.supplier_name,
            s.on_time_delivery_rate,
            s.average_delay_days,
            s.lead_time_variability,
            s.received_vs_ordered_fill_rate,
            s.supplier_service_risk_proxy,
            d.units_demanded,
            d.units_lost_sales,
            CASE WHEN d.units_demanded = 0 THEN 0.0 ELSE CAST(d.units_lost_sales AS DOUBLE) / CAST(d.units_demanded AS DOUBLE) END AS downstream_stockout_rate,
            CASE WHEN d.units_demanded = 0 THEN 1.0 ELSE CAST(d.units_fulfilled AS DOUBLE) / CAST(d.units_demanded AS DOUBLE) END AS downstream_fill_rate,
            d.lost_sales_revenue AS downstream_lost_sales_revenue
        FROM supplier_performance_summary s
        LEFT JOIN downstream d
            ON s.supplier_id = d.supplier_id
        ORDER BY s.supplier_service_risk_proxy DESC, downstream_lost_sales_revenue DESC
    """,
    "kpi_service_vs_working_capital_tradeoff_summary": """
        WITH sku_wh AS (
            SELECT
                product_id,
                warehouse_id,
                category,
                abc_class,
                SUM(units_demanded) AS units_demanded,
                SUM(units_fulfilled) AS units_fulfilled,
                SUM(units_lost_sales) AS units_lost_sales,
                AVG(days_of_supply) AS avg_days_of_supply,
                AVG(inventory_value) AS avg_inventory_value,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                CASE
                    WHEN abc_class = 'A' THEN 20.0
                    WHEN abc_class = 'B' THEN 30.0
                    ELSE 45.0
                END AS dos_cap
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, warehouse_id, category, abc_class
        ),
        scored AS (
            SELECT
                *,
                CASE WHEN units_demanded = 0 THEN 1.0 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS fill_rate,
                CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate
            FROM sku_wh
        ),
        zoned AS (
            SELECT
                *,
                CASE
                    WHEN avg_days_of_supply > dos_cap AND fill_rate < 0.95 THEN 'Overstocked and Under-Serving'
                    WHEN avg_days_of_supply <= 8 AND stockout_rate >= 0.08 THEN 'Understocked Revenue Exposure'
                    WHEN fill_rate >= 0.97 AND avg_days_of_supply BETWEEN 8 AND dos_cap THEN 'Balanced Efficient'
                    WHEN avg_days_of_supply > dos_cap AND fill_rate >= 0.97 THEN 'Service Protected but Capital Heavy'
                    ELSE 'Transitional / Monitor'
                END AS tradeoff_zone
            FROM scored
        )
        SELECT
            tradeoff_zone,
            COUNT(*) AS sku_warehouse_count,
            AVG(fill_rate) AS avg_fill_rate,
            AVG(stockout_rate) AS avg_stockout_rate,
            AVG(avg_days_of_supply) AS avg_days_of_supply,
            SUM(lost_sales_revenue) AS total_lost_sales_revenue,
            SUM(avg_inventory_value) AS total_avg_inventory_value
        FROM zoned
        GROUP BY tradeoff_zone
        ORDER BY total_lost_sales_revenue DESC
    """,
    "kpi_tradeoff_unbalanced_segments": """
        WITH sku_wh AS (
            SELECT
                product_id,
                warehouse_id,
                category,
                abc_class,
                supplier_id,
                SUM(units_demanded) AS units_demanded,
                SUM(units_fulfilled) AS units_fulfilled,
                SUM(units_lost_sales) AS units_lost_sales,
                AVG(days_of_supply) AS avg_days_of_supply,
                AVG(inventory_value) AS avg_inventory_value,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                CASE WHEN abc_class = 'A' THEN 20.0 WHEN abc_class = 'B' THEN 30.0 ELSE 45.0 END AS dos_cap
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, warehouse_id, category, abc_class, supplier_id
        ),
        scored AS (
            SELECT
                *,
                CASE WHEN units_demanded = 0 THEN 1.0 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS fill_rate,
                CASE WHEN units_demanded = 0 THEN 0.0 ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE) END AS stockout_rate
            FROM sku_wh
        )
        SELECT
            product_id,
            warehouse_id,
            supplier_id,
            category,
            abc_class,
            fill_rate,
            stockout_rate,
            avg_days_of_supply,
            dos_cap,
            avg_inventory_value,
            lost_sales_revenue,
            CASE
                WHEN avg_days_of_supply > dos_cap AND fill_rate < 0.95 THEN 'Overstocked and Under-Serving'
                WHEN avg_days_of_supply <= 8 AND stockout_rate >= 0.08 THEN 'Understocked Revenue Exposure'
                WHEN avg_days_of_supply > dos_cap AND fill_rate >= 0.97 THEN 'Service Protected but Capital Heavy'
                ELSE 'Other'
            END AS imbalance_type
        FROM scored
        WHERE (avg_days_of_supply > dos_cap AND fill_rate < 0.95)
           OR (avg_days_of_supply <= 8 AND stockout_rate >= 0.08)
           OR (avg_days_of_supply > dos_cap AND fill_rate >= 0.97)
        ORDER BY lost_sales_revenue DESC, avg_inventory_value DESC
        LIMIT 80
    """,
    "kpi_action_priority_skus": """
        WITH impact AS (
            SELECT
                product_id,
                warehouse_id,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                AVG(inventory_value) AS avg_inventory_value,
                AVG(days_of_supply) AS avg_days_of_supply,
                AVG(fill_rate) AS avg_fill_rate
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, warehouse_id
        )
        SELECT
            s.product_id,
            s.warehouse_id,
            s.supplier_id,
            s.governance_priority_score,
            s.risk_tier,
            s.main_risk_driver,
            s.recommended_action,
            i.lost_sales_revenue,
            i.avg_inventory_value,
            i.avg_days_of_supply,
            i.avg_fill_rate
        FROM sku_risk_table s
        LEFT JOIN impact i
            ON s.product_id = i.product_id
           AND s.warehouse_id = i.warehouse_id
        ORDER BY s.governance_priority_score DESC, i.lost_sales_revenue DESC
        LIMIT 40
    """,
    "kpi_action_priority_warehouses": """
        SELECT
            warehouse_id,
            warehouse_name,
            region,
            warehouse_service_risk_proxy,
            fill_rate,
            stockout_rate,
            lost_sales_value,
            average_days_of_supply,
            inventory_value,
            capacity_pressure_proxy
        FROM warehouse_service_profile
        ORDER BY warehouse_service_risk_proxy DESC, lost_sales_value DESC
    """,
    "kpi_action_priority_suppliers": """
        WITH downstream AS (
            SELECT
                supplier_id,
                SUM(lost_sales_revenue) AS downstream_lost_sales_revenue,
                SUM(units_demanded) AS units_demanded,
                SUM(units_lost_sales) AS units_lost_sales
            FROM daily_product_warehouse_metrics
            GROUP BY supplier_id
        )
        SELECT
            s.supplier_id,
            s.supplier_name,
            s.supplier_service_risk_proxy,
            s.on_time_delivery_rate,
            s.average_delay_days,
            s.lead_time_variability,
            s.received_vs_ordered_fill_rate,
            d.downstream_lost_sales_revenue,
            CASE WHEN d.units_demanded = 0 THEN 0.0 ELSE CAST(d.units_lost_sales AS DOUBLE) / CAST(d.units_demanded AS DOUBLE) END AS downstream_stockout_rate
        FROM supplier_performance_summary s
        LEFT JOIN downstream d
            ON s.supplier_id = d.supplier_id
        ORDER BY s.supplier_service_risk_proxy DESC, downstream_lost_sales_revenue DESC
    """,
    "kpi_action_priority_segments": """
        WITH impact AS (
            SELECT
                product_id,
                warehouse_id,
                SUM(lost_sales_revenue) AS lost_sales_revenue,
                AVG(inventory_value) AS avg_inventory_value
            FROM daily_product_warehouse_metrics
            GROUP BY product_id, warehouse_id
        ),
        enriched AS (
            SELECT
                s.product_id,
                s.warehouse_id,
                p.category,
                w.region,
                s.governance_priority_score,
                s.risk_tier,
                s.main_risk_driver,
                i.lost_sales_revenue,
                i.avg_inventory_value
            FROM sku_risk_table s
            INNER JOIN products p
                ON s.product_id = p.product_id
            INNER JOIN warehouses w
                ON s.warehouse_id = w.warehouse_id
            LEFT JOIN impact i
                ON s.product_id = i.product_id
               AND s.warehouse_id = i.warehouse_id
        )
        SELECT
            category,
            region,
            COUNT(*) AS sku_warehouse_count,
            AVG(governance_priority_score) AS avg_governance_priority_score,
            SUM(CASE WHEN risk_tier IN ('High', 'Critical') THEN 1 ELSE 0 END) AS high_or_critical_count,
            SUM(lost_sales_revenue) AS total_lost_sales_revenue,
            SUM(avg_inventory_value) AS total_avg_inventory_value,
            AVG(CASE WHEN main_risk_driver = 'Service Risk' THEN 1.0 ELSE 0.0 END) AS service_risk_driver_share,
            AVG(CASE WHEN main_risk_driver = 'Excess Inventory' THEN 1.0 ELSE 0.0 END) AS excess_inventory_driver_share,
            AVG(CASE WHEN main_risk_driver = 'Supplier Risk' THEN 1.0 ELSE 0.0 END) AS supplier_risk_driver_share
        FROM enriched
        GROUP BY category, region
        ORDER BY avg_governance_priority_score DESC, total_lost_sales_revenue DESC
    """,
}


def _load_tables(con: duckdb.DuckDBPyConnection) -> None:
    for table_name, path in {**PROCESSED_TABLES, **RAW_TABLES}.items():
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE);
            """
        )


def _run_queries(con: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for name, sql in SQL_QUERIES.items():
        outputs[name] = con.execute(sql).df()
    return outputs


def _save_outputs(outputs: dict[str, pd.DataFrame]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in outputs.items():
        df.to_csv(REPORT_DIR / f"{name}.csv", index=False)


def _build_executive_report(outputs: dict[str, pd.DataFrame]) -> str:
    overall = outputs["kpi_overall_service_health"].iloc[0]

    by_region = outputs["kpi_service_by_region"].copy()
    by_warehouse = outputs["kpi_service_by_warehouse"].copy()
    by_category = outputs["kpi_service_by_category"].copy()

    inventory_summary = outputs["kpi_inventory_efficiency_summary"].iloc[0]
    dos_buckets = outputs["kpi_days_of_supply_distribution"].copy()
    inv_concentration = outputs["kpi_inventory_value_concentration"].copy()
    excess_slow = outputs["kpi_excess_and_slow_moving_exposure"].copy()

    stockout_patterns = outputs["kpi_stockout_pattern_assessment"].copy()
    top_lost = outputs["kpi_lost_sales_by_sku_warehouse"].copy()

    supplier_risk = outputs["kpi_supplier_risk_with_downstream_impact"].copy()
    tradeoff_summary = outputs["kpi_service_vs_working_capital_tradeoff_summary"].copy()
    tradeoff_imbalanced = outputs["kpi_tradeoff_unbalanced_segments"].copy()

    top_skus = outputs["kpi_action_priority_skus"].copy()
    top_wh = outputs["kpi_action_priority_warehouses"].copy()
    top_suppliers = outputs["kpi_action_priority_suppliers"].copy()
    top_segments = outputs["kpi_action_priority_segments"].copy()

    worst_region = by_region.sort_values("fill_rate").iloc[0]
    best_region = by_region.sort_values("fill_rate", ascending=False).iloc[0]
    worst_wh = by_warehouse.sort_values("warehouse_service_risk_proxy", ascending=False).iloc[0]
    worst_category = by_category.sort_values("lost_sales_revenue", ascending=False).iloc[0]

    top20_share = inv_concentration[inv_concentration["rank_percentile"] <= 0.20]["avg_inventory_value"].sum() / max(
        inv_concentration["avg_inventory_value"].sum(), 1
    )

    systematic_stockout = stockout_patterns.loc[
        stockout_patterns["stockout_pattern"] == "Systematic", "sku_warehouse_count"
    ]
    systematic_count = int(systematic_stockout.iloc[0]) if not systematic_stockout.empty else 0

    risky_supplier = top_suppliers.iloc[0]
    risky_sku = top_skus.iloc[0]

    dual_failure = tradeoff_summary.loc[
        tradeoff_summary["tradeoff_zone"] == "Overstocked and Under-Serving"
    ]
    dual_failure_count = int(dual_failure["sku_warehouse_count"].iloc[0]) if not dual_failure.empty else 0
    dual_failure_lost = float(dual_failure["total_lost_sales_revenue"].iloc[0]) if not dual_failure.empty else 0.0

    balanced_zone = tradeoff_summary.loc[
        tradeoff_summary["tradeoff_zone"] == "Balanced Efficient"
    ]
    balanced_count = int(balanced_zone["sku_warehouse_count"].iloc[0]) if not balanced_zone.empty else 0

    inventory_heavy_category = excess_slow.sort_values("excess_inventory_value_proxy", ascending=False).iloc[0]
    segment_priority = top_segments.iloc[0]

    lines = [
        "# Executive KPI and Diagnostic Analysis",
        "",
        "Project: Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System",
        "",
        "## Descriptive Analysis",
        "",
        "### 1) Overall Service Level Health",
        f"- Overall fill rate: **{overall['overall_fill_rate']:.2%}**.",
        f"- Overall stockout rate (unit-based): **{overall['overall_stockout_rate']:.2%}**.",
        f"- Lost sales exposure: **EUR {overall['total_lost_sales_revenue']:,.0f}**.",
        f"- Total service gap volume: **{overall['total_service_gap_units']:,.0f} units** versus policy targets.",
        f"- Service dispersion by region: best region **{best_region['region']} ({best_region['fill_rate']:.2%})**, weakest region **{worst_region['region']} ({worst_region['fill_rate']:.2%})**.",
        f"- Most pressured warehouse: **{worst_wh['warehouse_id']}** with risk proxy **{worst_wh['warehouse_service_risk_proxy']:.2f}**, fill rate **{worst_wh['fill_rate']:.2%}**, and lost sales value **EUR {worst_wh['lost_sales_value']:,.0f}**.",
        f"- Category with highest lost-sales value: **{worst_category['category']}** at **EUR {worst_category['lost_sales_revenue']:,.0f}**.",
        "",
        "### 2) Inventory Efficiency",
        f"- Average days of supply: **{inventory_summary['avg_days_of_supply']:.2f} days** (median **{inventory_summary['median_days_of_supply']:.2f}**, p90 **{inventory_summary['p90_days_of_supply']:.2f}**, p99 **{inventory_summary['p99_days_of_supply']:.2f}**).",
        f"- Excess inventory day-rate proxy: **{inventory_summary['excess_inventory_day_rate']:.2%}**.",
        f"- Slow-moving day-rate proxy: **{inventory_summary['slow_moving_day_rate']:.2%}**.",
        f"- Inventory concentration: top 20% of SKUs hold **{top20_share:.2%}** of average inventory value.",
        f"- Category with largest excess-value proxy: **{inventory_heavy_category['category']}** (proxy **EUR {inventory_heavy_category['excess_inventory_value_proxy']:,.0f}**).",
        "",
        "### 3) Stockout and Lost Sales Risk",
        f"- Systematic stockout footprint: **{systematic_count} SKU-warehouse combinations** classified as systematic.",
        f"- Highest-loss SKU-warehouse in current ranking: **{top_lost.iloc[0]['product_id']} @ {top_lost.iloc[0]['warehouse_id']}** with **EUR {top_lost.iloc[0]['lost_sales_revenue']:,.0f}** lost sales.",
        "",
        "## Diagnostic Analysis",
        "",
        "### 4) Supplier-Driven Risk",
        f"- Most critical supplier risk profile: **{risky_supplier['supplier_id']} ({risky_supplier['supplier_name']})** with supplier risk score **{risky_supplier['supplier_service_risk_proxy']:.2f}**.",
        f"- This supplier shows on-time delivery **{risky_supplier['on_time_delivery_rate']:.2%}**, average delay **{risky_supplier['average_delay_days']:.2f} days**, lead-time variability **{risky_supplier['lead_time_variability']:.2f}**.",
        f"- Downstream propagation signal: supplier-linked proxy lost sales **EUR {risky_supplier['downstream_lost_sales_revenue']:,.0f}**, downstream stockout rate **{risky_supplier['downstream_stockout_rate']:.2%}**.",
        "",
        "### 5) Service Level vs Working Capital Trade-off",
        f"- Dual-failure zone (overstocked yet under-serving): **{dual_failure_count} SKU-warehouse combinations**, lost sales **EUR {dual_failure_lost:,.0f}**.",
        f"- Balanced efficient zone count: **{balanced_count} SKU-warehouse combinations**.",
        "- This indicates simultaneous value leakage: revenue at risk from service failures and capital lock-up in high-DOS positions.",
        "",
        "### 6) Action Prioritization",
        f"- Highest-priority SKU-location: **{risky_sku['product_id']} @ {risky_sku['warehouse_id']}** (priority score **{risky_sku['governance_priority_score']:.2f}**, driver **{risky_sku['main_risk_driver']}**).",
        f"- Highest-risk warehouse: **{top_wh.iloc[0]['warehouse_id']}** (risk proxy **{top_wh.iloc[0]['warehouse_service_risk_proxy']:.2f}**).",
        f"- Highest-risk supplier: **{risky_supplier['supplier_id']}** (risk proxy **{risky_supplier['supplier_service_risk_proxy']:.2f}**).",
        f"- Most urgent intervention segment: **{segment_priority['category']} | {segment_priority['region']}** with avg governance priority **{segment_priority['avg_governance_priority_score']:.2f}** and lost-sales exposure **EUR {segment_priority['total_lost_sales_revenue']:,.0f}**.",
        "",
        "## Recommended Immediate Interventions (30-60 day)",
        "1. Service recovery on high-priority SKU-locations where stockout risk and service-gap scores jointly exceed threshold; tune reorder points and protect promotional allocation.",
        "2. Supplier corrective action plans for top-risk suppliers with poor OTD and high downstream lost-sales coupling; enforce SLA recovery cadence.",
        "3. Working-capital release program for capital-heavy categories/segments with high excess-DOS proxies, using transfer/markdown and tighter order-up-to controls.",
        "",
        "## Notes",
        "- KPI files are saved as CSV in /outputs/reports/ for traceability and dashboard ingestion.",
        "- This report separates descriptive performance status from diagnostic root-cause patterns for decision governance.",
    ]

    return "\n".join(lines)


def run_analysis() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        _load_tables(con)
        outputs = _run_queries(con)
    finally:
        con.close()

    _save_outputs(outputs)

    report_text = _build_executive_report(outputs)
    report_path = REPORT_DIR / "executive_kpi_diagnostic_analysis.md"
    report_path.write_text(report_text, encoding="utf-8")

    print("KPI and diagnostic analysis complete.")
    print(f"CSV outputs: {len(outputs)} files written to {REPORT_DIR}")
    print(f"Executive report: {report_path}")


if __name__ == "__main__":
    run_analysis()
