-- Supply Chain Service Level and Inventory Intelligence
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
WITH category_daily_inventory AS (
    SELECT
        p.category,
        i.snapshot_date,
        SUM(i.inventory_value) AS inventory_value
    FROM inventory_snapshots i
    INNER JOIN products p
        ON i.product_id = p.product_id
    GROUP BY p.category, i.snapshot_date
)
SELECT
    category,
    AVG(inventory_value) AS avg_daily_inventory_value,
    MAX(inventory_value) AS peak_daily_inventory_value
FROM category_daily_inventory
GROUP BY category
ORDER BY avg_daily_inventory_value DESC;

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
        CASE WHEN SUM(po.ordered_units) = 0 THEN 1.0
             ELSE CAST(SUM(po.received_units) AS DOUBLE) / CAST(SUM(po.ordered_units) AS DOUBLE)
        END AS in_full_rate
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
        fill_rate,
        stockout_rate,
        lost_sales_value,
        average_days_of_supply,
        inventory_value,
        capacity_pressure_proxy
    FROM warehouse_service_profile
)
SELECT *
FROM warehouse_service
ORDER BY fill_rate ASC;

-- ============================================================
-- KPI 08: Service Level vs Inventory Trade-off (Quadrant Summary)
-- ============================================================
WITH sku_wh_tradeoff AS (
    SELECT
        product_id,
        warehouse_id,
        fill_rate,
        stockout_rate,
        dos_stretch,
        excess_day_rate,
        lost_sales_revenue,
        CASE
            WHEN fill_rate < 0.95 AND excess_day_rate >= 0.20 THEN 'Dual Failure: Low Service + Excess Inventory'
            WHEN fill_rate < 0.95 THEN 'Service Risk'
            WHEN excess_day_rate >= 0.20 THEN 'Working Capital Risk'
            ELSE 'Balanced'
        END AS tradeoff_zone
    FROM sku_risk_table
)
SELECT
    tradeoff_zone,
    COUNT(*) AS sku_warehouse_count,
    AVG(fill_rate) AS avg_fill_rate,
    AVG(stockout_rate) AS avg_stockout_rate,
    AVG(excess_day_rate) AS avg_excess_day_rate,
    AVG(dos_stretch) AS avg_dos_stretch,
    SUM(lost_sales_revenue) AS total_lost_revenue
FROM sku_wh_tradeoff
GROUP BY tradeoff_zone
ORDER BY total_lost_revenue DESC;

-- ============================================================
-- KPI 09: Top Governance Priority SKU-Locations
-- ============================================================
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY governance_priority_score DESC, lost_sales_revenue DESC
        ) AS priority_rank
    FROM sku_risk_table
)
SELECT *
FROM ranked
WHERE priority_rank <= 25
ORDER BY priority_rank;

-- ============================================================
-- KPI 10: Working Capital Concentration
-- ============================================================
WITH sku_daily_inventory AS (
    SELECT
        i.product_id,
        p.category,
        pc.abc_class,
        i.snapshot_date,
        SUM(i.inventory_value) AS inventory_value
    FROM inventory_snapshots i
    INNER JOIN products p
        ON i.product_id = p.product_id
    LEFT JOIN product_classification pc
        ON i.product_id = pc.product_id
    GROUP BY i.product_id, p.category, pc.abc_class, i.snapshot_date
),
sku_inventory_value AS (
    SELECT
        product_id,
        category,
        abc_class,
        AVG(inventory_value) AS avg_inventory_value
    FROM sku_daily_inventory
    GROUP BY product_id, category, abc_class
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
