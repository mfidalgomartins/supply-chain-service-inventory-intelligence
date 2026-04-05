-- Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System
-- File: 03_kpi_queries.sql
-- Purpose: KPI query library for supply chain leadership, finance, and operations.

-- ============================================================
-- KPI 01: Overall Fill Rate
-- ============================================================
WITH demand_rollup AS (
    SELECT
        SUM(units_demanded) AS units_demanded,
        SUM(units_fulfilled) AS units_fulfilled
    FROM demand_history
)
SELECT
    units_demanded,
    units_fulfilled,
    CASE WHEN units_demanded = 0 THEN 1.0
         ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE)
    END AS overall_fill_rate
FROM demand_rollup;

-- ============================================================
-- KPI 02: Stockout Rate (Unit-Level)
-- ============================================================
WITH stockout_rollup AS (
    SELECT
        SUM(units_lost_sales) AS units_lost_sales,
        SUM(units_demanded) AS units_demanded
    FROM demand_history
)
SELECT
    units_lost_sales,
    units_demanded,
    CASE WHEN units_demanded = 0 THEN 0.0
         ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE)
    END AS stockout_rate
FROM stockout_rollup;

-- ============================================================
-- KPI 03: Lost Sales Exposure (Units and Revenue)
-- ============================================================
WITH lost_sales AS (
    SELECT
        d.product_id,
        p.category,
        SUM(d.units_lost_sales) AS lost_units,
        SUM(CAST(d.units_lost_sales AS DOUBLE) * p.unit_price) AS lost_revenue
    FROM demand_history d
    INNER JOIN products p
        ON d.product_id = p.product_id
    GROUP BY d.product_id, p.category
)
SELECT
    category,
    SUM(lost_units) AS lost_units,
    SUM(lost_revenue) AS lost_revenue
FROM lost_sales
GROUP BY category
ORDER BY lost_revenue DESC;

-- ============================================================
-- KPI 04: Inventory Value by Category
-- ============================================================
WITH category_inventory AS (
    SELECT
        p.category,
        AVG(i.inventory_value) AS avg_inventory_value,
        MAX(i.inventory_value) AS peak_inventory_value
    FROM inventory_snapshots i
    INNER JOIN products p
        ON i.product_id = p.product_id
    GROUP BY p.category
)
SELECT
    category,
    avg_inventory_value,
    peak_inventory_value
FROM category_inventory
ORDER BY avg_inventory_value DESC;

-- ============================================================
-- KPI 05: Days of Supply Distribution
-- ============================================================
WITH dos_stats AS (
    SELECT
        MIN(days_of_supply) AS dos_min,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY days_of_supply) AS dos_p25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_of_supply) AS dos_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_of_supply) AS dos_p75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_of_supply) AS dos_p90,
        MAX(days_of_supply) AS dos_max,
        AVG(days_of_supply) AS dos_avg
    FROM inventory_snapshots
)
SELECT *
FROM dos_stats;

-- ============================================================
-- KPI 06: Supplier On-Time Delivery Rate
-- ============================================================
WITH supplier_on_time AS (
    SELECT
        po.supplier_id,
        s.supplier_name,
        COUNT(*) AS po_count,
        AVG(CASE WHEN po.late_delivery_flag = 0 THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(CASE WHEN po.ordered_units = 0 THEN 1.0
                 ELSE CAST(po.received_units AS DOUBLE) / CAST(po.ordered_units AS DOUBLE)
            END) AS in_full_rate
    FROM purchase_orders po
    INNER JOIN suppliers s
        ON po.supplier_id = s.supplier_id
    GROUP BY po.supplier_id, s.supplier_name
)
SELECT *
FROM supplier_on_time
ORDER BY on_time_delivery_rate ASC, po_count DESC;

-- ============================================================
-- KPI 07: Warehouse Service Comparison
-- ============================================================
WITH warehouse_service AS (
    SELECT
        warehouse_id,
        region,
        total_units_demanded,
        total_units_fulfilled,
        total_units_lost_sales,
        warehouse_fill_rate,
        warehouse_lost_sales_rate,
        avg_inventory_value,
        avg_days_of_supply
    FROM warehouse_service_profile
)
SELECT *
FROM warehouse_service
ORDER BY warehouse_fill_rate ASC;

-- ============================================================
-- KPI 08: Service Level vs Inventory Trade-off (Quadrant Summary)
-- ============================================================
WITH sku_wh_tradeoff AS (
    SELECT
        s.product_id,
        s.warehouse_id,
        s.avg_fill_rate,
        s.stockout_day_rate,
        i.avg_days_of_supply,
        i.excess_day_rate,
        i.avg_inventory_value,
        s.total_lost_revenue,
        CASE
            WHEN s.avg_fill_rate < 0.95 AND i.excess_day_rate >= 0.20 THEN 'Dual Failure: Low Service + Excess Inventory'
            WHEN s.avg_fill_rate < 0.95 THEN 'Service Risk'
            WHEN i.excess_day_rate >= 0.20 THEN 'Working Capital Risk'
            ELSE 'Balanced'
        END AS tradeoff_zone
    FROM service_risk_base s
    INNER JOIN inventory_risk_base i
        ON s.product_id = i.product_id
       AND s.warehouse_id = i.warehouse_id
)
SELECT
    tradeoff_zone,
    COUNT(*) AS sku_warehouse_count,
    AVG(avg_fill_rate) AS avg_fill_rate,
    AVG(stockout_day_rate) AS avg_stockout_day_rate,
    AVG(excess_day_rate) AS avg_excess_day_rate,
    SUM(total_lost_revenue) AS total_lost_revenue,
    SUM(avg_inventory_value) AS total_avg_inventory_value
FROM sku_wh_tradeoff
GROUP BY tradeoff_zone
ORDER BY total_lost_revenue DESC;

-- ============================================================
-- KPI 09: Top Risk SKUs (Composite Service + Inventory Pressure)
-- ============================================================
WITH risk_components AS (
    SELECT
        s.product_id,
        s.warehouse_id,
        s.category,
        s.abc_class,
        s.avg_fill_rate,
        s.stockout_day_rate,
        i.excess_day_rate,
        i.slow_moving_day_rate,
        i.avg_days_of_supply,
        s.total_lost_revenue,
        i.avg_inventory_value,
        -- Interpretable weighted composite score
        (
            0.40 * s.stockout_day_rate +
            0.30 * i.excess_day_rate +
            0.15 * i.slow_moving_day_rate +
            0.10 * (1.0 - s.avg_fill_rate) +
            0.05 * LEAST(i.avg_days_of_supply / 60.0, 1.0)
        ) * 100.0 AS risk_score
    FROM service_risk_base s
    INNER JOIN inventory_risk_base i
        ON s.product_id = i.product_id
       AND s.warehouse_id = i.warehouse_id
),
ranked AS (
    SELECT
        rc.*,
        ROW_NUMBER() OVER (ORDER BY rc.risk_score DESC, rc.total_lost_revenue DESC) AS risk_rank
    FROM risk_components rc
)
SELECT *
FROM ranked
WHERE risk_rank <= 25
ORDER BY risk_rank;

-- ============================================================
-- KPI 10: Working Capital Concentration
-- ============================================================
WITH sku_inventory_value AS (
    SELECT
        i.product_id,
        p.category,
        pc.abc_class,
        AVG(i.inventory_value) AS avg_inventory_value
    FROM inventory_snapshots i
    INNER JOIN products p
        ON i.product_id = p.product_id
    LEFT JOIN product_classification pc
        ON i.product_id = pc.product_id
    GROUP BY i.product_id, p.category, pc.abc_class
),
portfolio AS (
    SELECT
        product_id,
        category,
        abc_class,
        avg_inventory_value,
        SUM(avg_inventory_value) OVER () AS total_portfolio_value,
        ROW_NUMBER() OVER (ORDER BY avg_inventory_value DESC) AS value_rank,
        COUNT(*) OVER () AS sku_count
    FROM sku_inventory_value
),
concentration AS (
    SELECT
        product_id,
        category,
        abc_class,
        avg_inventory_value,
        total_portfolio_value,
        value_rank,
        CAST(value_rank AS DOUBLE) / CAST(sku_count AS DOUBLE) AS rank_percentile,
        SUM(avg_inventory_value) OVER (ORDER BY avg_inventory_value DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS cumulative_inventory_value
    FROM portfolio
)
SELECT
    product_id,
    category,
    abc_class,
    avg_inventory_value,
    cumulative_inventory_value,
    total_portfolio_value,
    cumulative_inventory_value / NULLIF(total_portfolio_value, 0.0) AS cumulative_value_share,
    rank_percentile
FROM concentration
WHERE rank_percentile <= 0.20
ORDER BY avg_inventory_value DESC;
