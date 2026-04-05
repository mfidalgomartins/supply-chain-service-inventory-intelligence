CREATE OR REPLACE TABLE diagnostic_sku_location AS
WITH base AS (
    SELECT
        s.product_id,
        s.warehouse_id,
        MAX(s.category) AS category,
        MAX(s.abc_class) AS abc_class,
        SUM(s.demand_qty) AS demand_qty,
        SUM(s.fulfilled_qty) AS fulfilled_qty,
        SUM(s.lost_sales_qty) AS lost_sales_qty,
        SUM(s.lost_revenue) AS lost_revenue,
        AVG(i.inventory_value) AS avg_inventory_value,
        AVG(i.days_of_supply) AS avg_days_of_supply,
        SUM(CASE WHEN i.excess_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS excess_day_rate,
        SUM(CASE WHEN s.stockout_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS stockout_day_rate
    FROM fact_sales_daily s
    JOIN fact_inventory_daily i
      ON s.date = i.date
     AND s.product_id = i.product_id
     AND s.warehouse_id = i.warehouse_id
    GROUP BY 1,2
)
SELECT
    product_id,
    warehouse_id,
    category,
    abc_class,
    ROUND(fulfilled_qty * 1.0 / NULLIF(demand_qty, 0), 4) AS service_level,
    ROUND(stockout_day_rate, 4) AS stockout_day_rate,
    ROUND(excess_day_rate, 4) AS excess_day_rate,
    ROUND(avg_days_of_supply, 2) AS avg_days_of_supply,
    ROUND(avg_inventory_value, 2) AS avg_inventory_value,
    ROUND(lost_revenue, 2) AS lost_revenue,
    CASE
        WHEN stockout_day_rate >= 0.18 AND excess_day_rate >= 0.22 THEN 'Dual Failure: Stockout + Excess'
        WHEN stockout_day_rate >= 0.18 THEN 'Service Risk'
        WHEN excess_day_rate >= 0.22 THEN 'Working Capital Risk'
        ELSE 'Balanced / Monitor'
    END AS tradeoff_zone
FROM base;

CREATE OR REPLACE TABLE diagnostic_supplier_reliability AS
WITH po AS (
    SELECT
        supplier_id,
        COUNT(*) AS po_count,
        AVG(realized_lead_time_days) AS avg_realized_lead_time,
        STDDEV_SAMP(realized_lead_time_days) AS std_realized_lead_time,
        AVG(ABS(realized_lead_time_days - expected_lead_time_days)) AS avg_lead_time_deviation
    FROM fact_purchase_orders
    GROUP BY 1
),
sales_impact AS (
    SELECT
        p.supplier_id,
        SUM(s.lost_revenue) AS lost_revenue,
        SUM(s.demand_qty) AS demand_qty,
        SUM(s.fulfilled_qty) AS fulfilled_qty
    FROM fact_sales_daily s
    JOIN dim_products p USING (product_id)
    GROUP BY 1
)
SELECT
    po.supplier_id,
    po.po_count,
    ROUND(po.avg_realized_lead_time, 2) AS avg_realized_lead_time,
    ROUND(po.std_realized_lead_time, 2) AS std_realized_lead_time,
    ROUND(po.avg_lead_time_deviation, 2) AS avg_lead_time_deviation,
    ROUND(si.fulfilled_qty * 1.0 / NULLIF(si.demand_qty, 0), 4) AS downstream_service_level,
    ROUND(si.lost_revenue, 2) AS downstream_lost_revenue
FROM po
LEFT JOIN sales_impact si USING (supplier_id)
ORDER BY avg_lead_time_deviation DESC;

CREATE OR REPLACE TABLE diagnostic_quadrant_summary AS
SELECT
    tradeoff_zone,
    COUNT(*) AS sku_location_count,
    ROUND(AVG(service_level), 4) AS avg_service_level,
    ROUND(AVG(stockout_day_rate), 4) AS avg_stockout_day_rate,
    ROUND(AVG(excess_day_rate), 4) AS avg_excess_day_rate,
    ROUND(SUM(lost_revenue), 2) AS total_lost_revenue,
    ROUND(SUM(avg_inventory_value), 2) AS total_avg_inventory_value
FROM diagnostic_sku_location
GROUP BY 1
ORDER BY total_lost_revenue DESC;
