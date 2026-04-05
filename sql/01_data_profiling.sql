CREATE OR REPLACE TABLE profiling_row_counts AS
SELECT 'dim_products' AS table_name, COUNT(*) AS row_count FROM dim_products
UNION ALL
SELECT 'dim_suppliers', COUNT(*) FROM dim_suppliers
UNION ALL
SELECT 'dim_warehouses', COUNT(*) FROM dim_warehouses
UNION ALL
SELECT 'fact_sales_daily', COUNT(*) FROM fact_sales_daily
UNION ALL
SELECT 'fact_inventory_daily', COUNT(*) FROM fact_inventory_daily
UNION ALL
SELECT 'fact_purchase_orders', COUNT(*) FROM fact_purchase_orders;

CREATE OR REPLACE TABLE profiling_date_coverage AS
SELECT
    (SELECT MIN(date) FROM fact_sales_daily) AS min_sales_date,
    (SELECT MAX(date) FROM fact_sales_daily) AS max_sales_date,
    (SELECT COUNT(DISTINCT date) FROM fact_sales_daily) AS sales_days,
    (SELECT MIN(date) FROM fact_inventory_daily) AS min_inventory_date,
    (SELECT MAX(date) FROM fact_inventory_daily) AS max_inventory_date,
    (SELECT COUNT(DISTINCT date) FROM fact_inventory_daily) AS inventory_days;

CREATE OR REPLACE TABLE profiling_data_quality AS
SELECT
    SUM(CASE WHEN demand_qty IS NULL THEN 1 ELSE 0 END) AS null_demand_qty,
    SUM(CASE WHEN fulfilled_qty IS NULL THEN 1 ELSE 0 END) AS null_fulfilled_qty,
    SUM(CASE WHEN lost_sales_qty IS NULL THEN 1 ELSE 0 END) AS null_lost_sales_qty,
    SUM(CASE WHEN net_revenue < 0 THEN 1 ELSE 0 END) AS negative_revenue_rows,
    SUM(CASE WHEN fulfilled_qty + lost_sales_qty != demand_qty THEN 1 ELSE 0 END) AS demand_balance_mismatch_rows
FROM fact_sales_daily;

CREATE OR REPLACE TABLE profiling_demand_distribution AS
SELECT
    category,
    abc_class,
    COUNT(*) AS row_count,
    ROUND(AVG(demand_qty), 2) AS avg_daily_demand,
    ROUND(STDDEV_SAMP(demand_qty), 2) AS std_daily_demand,
    ROUND(SUM(lost_sales_qty) * 1.0 / NULLIF(SUM(demand_qty), 0), 4) AS lost_sales_rate
FROM fact_sales_daily
GROUP BY 1,2
ORDER BY category, abc_class;
