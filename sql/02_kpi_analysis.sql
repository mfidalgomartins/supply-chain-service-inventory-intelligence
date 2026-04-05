CREATE OR REPLACE TABLE kpi_monthly_service_inventory AS
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', date) AS month,
        SUM(demand_qty) AS demand_qty,
        SUM(fulfilled_qty) AS fulfilled_qty,
        SUM(lost_sales_qty) AS lost_sales_qty,
        SUM(net_revenue) AS revenue,
        SUM(gross_margin) AS gross_margin,
        SUM(lost_revenue) AS lost_revenue
    FROM fact_sales_daily
    GROUP BY 1
),
monthly_inventory AS (
    SELECT
        DATE_TRUNC('month', date) AS month,
        AVG(inventory_value) AS avg_inventory_value,
        AVG(days_of_supply) AS avg_days_of_supply,
        SUM(CASE WHEN excess_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS excess_day_rate
    FROM fact_inventory_daily
    GROUP BY 1
)
SELECT
    s.month,
    s.demand_qty,
    s.fulfilled_qty,
    s.lost_sales_qty,
    ROUND(s.fulfilled_qty * 1.0 / NULLIF(s.demand_qty, 0), 4) AS service_level,
    ROUND(s.revenue, 2) AS revenue,
    ROUND(s.gross_margin, 2) AS gross_margin,
    ROUND(s.lost_revenue, 2) AS lost_revenue,
    ROUND(i.avg_inventory_value, 2) AS avg_inventory_value,
    ROUND(i.avg_days_of_supply, 2) AS avg_days_of_supply,
    ROUND(i.excess_day_rate, 4) AS excess_day_rate,
    ROUND(365 * (i.avg_inventory_value / NULLIF(s.gross_margin, 0)), 2) AS proxy_days_inventory_on_margin
FROM monthly_sales s
LEFT JOIN monthly_inventory i USING (month)
ORDER BY s.month;

CREATE OR REPLACE TABLE kpi_warehouse_tradeoff AS
WITH warehouse_sales AS (
    SELECT
        warehouse_id,
        SUM(demand_qty) AS demand_qty,
        SUM(fulfilled_qty) AS fulfilled_qty,
        SUM(lost_sales_qty) AS lost_sales_qty,
        SUM(net_revenue) AS revenue,
        SUM(gross_margin) AS gross_margin
    FROM fact_sales_daily
    GROUP BY 1
),
warehouse_inventory AS (
    SELECT
        warehouse_id,
        AVG(inventory_value) AS avg_inventory_value,
        AVG(days_of_supply) AS avg_days_of_supply,
        SUM(CASE WHEN excess_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS excess_day_rate
    FROM fact_inventory_daily
    GROUP BY 1
)
SELECT
    ws.warehouse_id,
    ROUND(ws.fulfilled_qty * 1.0 / NULLIF(ws.demand_qty, 0), 4) AS service_level,
    ROUND(ws.lost_sales_qty * 1.0 / NULLIF(ws.demand_qty, 0), 4) AS stockout_rate,
    ROUND(ws.revenue, 2) AS revenue,
    ROUND(ws.gross_margin, 2) AS gross_margin,
    ROUND(wi.avg_inventory_value, 2) AS avg_inventory_value,
    ROUND(wi.avg_days_of_supply, 2) AS avg_days_of_supply,
    ROUND(wi.excess_day_rate, 4) AS excess_day_rate,
    ROUND(wi.avg_inventory_value / NULLIF(ws.revenue, 0), 4) AS inventory_to_revenue_ratio
FROM warehouse_sales ws
LEFT JOIN warehouse_inventory wi USING (warehouse_id)
ORDER BY service_level ASC;

CREATE OR REPLACE TABLE kpi_abc_policy_gap AS
WITH sales_abc AS (
    SELECT
        abc_class,
        SUM(demand_qty) AS demand_qty,
        SUM(fulfilled_qty) AS fulfilled_qty,
        SUM(lost_sales_qty) AS lost_sales_qty,
        SUM(lost_revenue) AS lost_revenue
    FROM fact_sales_daily
    GROUP BY 1
),
inventory_abc AS (
    SELECT
        s.abc_class,
        AVG(i.inventory_value) AS avg_inventory_value,
        AVG(i.days_of_supply) AS avg_days_of_supply,
        SUM(CASE WHEN i.excess_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS excess_day_rate
    FROM fact_inventory_daily i
    JOIN fact_sales_daily s
        ON i.date = s.date
       AND i.product_id = s.product_id
       AND i.warehouse_id = s.warehouse_id
    GROUP BY 1
)
SELECT
    sa.abc_class,
    ROUND(sa.fulfilled_qty * 1.0 / NULLIF(sa.demand_qty, 0), 4) AS service_level,
    ROUND(sa.lost_sales_qty * 1.0 / NULLIF(sa.demand_qty, 0), 4) AS lost_unit_rate,
    ROUND(sa.lost_revenue, 2) AS lost_revenue,
    ROUND(ia.avg_inventory_value, 2) AS avg_inventory_value,
    ROUND(ia.avg_days_of_supply, 2) AS avg_days_of_supply,
    ROUND(ia.excess_day_rate, 4) AS excess_day_rate
FROM sales_abc sa
LEFT JOIN inventory_abc ia USING (abc_class)
ORDER BY abc_class;
