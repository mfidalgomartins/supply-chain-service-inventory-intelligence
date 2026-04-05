-- Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System
-- File: 02_intermediate_views.sql
-- Purpose: intermediate analytical views at auditable business grain.

-- ============================================================
-- View 1: daily_product_warehouse_metrics
-- Grain: date + warehouse_id + product_id
-- ============================================================
CREATE OR REPLACE VIEW daily_product_warehouse_metrics AS
WITH base_join AS (
    SELECT
        d.date,
        d.warehouse_id,
        w.region,
        d.product_id,
        p.category,
        p.supplier_id,
        pc.abc_class,
        pc.criticality_level,
        d.units_demanded,
        d.units_fulfilled,
        d.units_lost_sales,
        d.stockout_flag,
        d.promo_flag,
        i.on_hand_units,
        i.on_order_units,
        i.available_units,
        i.days_of_supply,
        i.inventory_value,
        p.unit_price,
        p.target_service_level
    FROM demand_history d
    INNER JOIN inventory_snapshots i
        ON d.date = i.snapshot_date
       AND d.warehouse_id = i.warehouse_id
       AND d.product_id = i.product_id
    INNER JOIN products p
        ON d.product_id = p.product_id
    INNER JOIN warehouses w
        ON d.warehouse_id = w.warehouse_id
    LEFT JOIN product_classification pc
        ON d.product_id = pc.product_id
),
calculated AS (
    SELECT
        date,
        warehouse_id,
        region,
        product_id,
        category,
        supplier_id,
        abc_class,
        criticality_level,
        units_demanded,
        units_fulfilled,
        units_lost_sales,
        stockout_flag,
        CASE
            WHEN units_demanded = 0 THEN 1.0
            ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE)
        END AS fill_rate,
        promo_flag,
        on_hand_units,
        on_order_units,
        available_units,
        days_of_supply,
        inventory_value,
        CAST(units_lost_sales AS DOUBLE) * unit_price AS lost_sales_revenue,
        GREATEST((target_service_level * units_demanded) - units_fulfilled, 0.0) AS service_gap_units
    FROM base_join
)
SELECT *
FROM calculated;

-- ============================================================
-- View 2: supplier_performance_summary
-- Grain: supplier_id
-- ============================================================
CREATE OR REPLACE VIEW supplier_performance_summary AS
WITH po_base AS (
    SELECT
        po.supplier_id,
        po.po_id,
        po.ordered_units,
        po.received_units,
        po.late_delivery_flag,
        CAST((po.actual_arrival_date - po.expected_arrival_date) AS DOUBLE) AS delay_days,
        CAST((po.actual_arrival_date - po.order_date) AS DOUBLE) AS actual_lead_time_days
    FROM purchase_orders po
),
po_agg AS (
    SELECT
        supplier_id,
        AVG(CASE WHEN late_delivery_flag = 0 THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
        AVG(GREATEST(delay_days, 0.0)) AS average_delay_days,
        COALESCE(STDDEV_SAMP(actual_lead_time_days), 0.0) AS lead_time_variability,
        CASE
            WHEN SUM(ordered_units) = 0 THEN 1.0
            ELSE CAST(SUM(received_units) AS DOUBLE) / CAST(SUM(ordered_units) AS DOUBLE)
        END AS received_vs_ordered_fill_rate
    FROM po_base
    GROUP BY supplier_id
),
risk_scored AS (
    SELECT
        supplier_id,
        on_time_delivery_rate,
        average_delay_days,
        lead_time_variability,
        received_vs_ordered_fill_rate,
        100.0 * (
            0.40 * (1.0 - on_time_delivery_rate) +
            0.20 * LEAST(average_delay_days / 10.0, 1.0) +
            0.20 * LEAST(lead_time_variability / 8.0, 1.0) +
            0.20 * (1.0 - received_vs_ordered_fill_rate)
        ) AS supplier_service_risk_proxy
    FROM po_agg
)
SELECT
    s.supplier_id,
    s.supplier_name,
    COALESCE(r.on_time_delivery_rate, 1.0) AS on_time_delivery_rate,
    COALESCE(r.average_delay_days, 0.0) AS average_delay_days,
    COALESCE(r.lead_time_variability, 0.0) AS lead_time_variability,
    COALESCE(r.received_vs_ordered_fill_rate, 1.0) AS received_vs_ordered_fill_rate,
    COALESCE(r.supplier_service_risk_proxy, 0.0) AS supplier_service_risk_proxy
FROM suppliers s
LEFT JOIN risk_scored r
    ON s.supplier_id = r.supplier_id;

-- ============================================================
-- View 3: product_inventory_profile
-- Grain: product_id
-- ============================================================
CREATE OR REPLACE VIEW product_inventory_profile AS
WITH sku_daily AS (
    SELECT
        d.product_id,
        p.product_name,
        d.category,
        d.abc_class,
        d.on_hand_units,
        d.inventory_value,
        d.days_of_supply,
        d.stockout_flag,
        d.fill_rate,
        d.lost_sales_revenue,
        d.available_units,
        d.units_fulfilled,
        CASE
            WHEN d.abc_class = 'A' THEN 20.0
            WHEN d.abc_class = 'B' THEN 30.0
            ELSE 45.0
        END AS dos_policy_cap
    FROM daily_product_warehouse_metrics d
    INNER JOIN products p
        ON d.product_id = p.product_id
),
sku_agg AS (
    SELECT
        product_id,
        MAX(product_name) AS product_name,
        MAX(category) AS category,
        MAX(abc_class) AS abc_class,
        AVG(on_hand_units) AS average_inventory_units,
        AVG(inventory_value) AS average_inventory_value,
        AVG(days_of_supply) AS average_days_of_supply,
        AVG(CASE WHEN stockout_flag = 1 THEN 1.0 ELSE 0.0 END) AS stockout_frequency,
        AVG(fill_rate) AS fill_rate_average,
        SUM(lost_sales_revenue) AS lost_sales_exposure,
        AVG(CASE WHEN available_units > 0 AND units_fulfilled = 0 THEN 1.0 ELSE 0.0 END) AS slow_moving_inventory_proxy,
        AVG(CASE WHEN days_of_supply > dos_policy_cap THEN 1.0 ELSE 0.0 END) AS excess_inventory_proxy
    FROM sku_daily
    GROUP BY product_id
),
risk_calc AS (
    SELECT
        *,
        100.0 * (
            0.45 * excess_inventory_proxy +
            0.30 * slow_moving_inventory_proxy +
            0.15 * LEAST(average_days_of_supply / 60.0, 1.0) +
            0.10 * LEAST(average_inventory_value / 50000.0, 1.0)
        ) AS working_capital_risk_proxy
    FROM sku_agg
)
SELECT *
FROM risk_calc;

-- ============================================================
-- View 4: warehouse_service_profile
-- Grain: warehouse_id
-- ============================================================
CREATE OR REPLACE VIEW warehouse_service_profile AS
WITH warehouse_daily AS (
    SELECT
        d.date,
        d.warehouse_id,
        MAX(w.warehouse_name) AS warehouse_name,
        MAX(d.region) AS region,
        SUM(d.units_demanded) AS units_demanded,
        SUM(d.units_fulfilled) AS units_fulfilled,
        SUM(d.units_lost_sales) AS units_lost_sales,
        SUM(d.lost_sales_revenue) AS lost_sales_value,
        AVG(d.days_of_supply) AS average_days_of_supply,
        SUM(d.inventory_value) AS inventory_value,
        SUM(d.on_hand_units + d.on_order_units) AS inventory_units_plus_pipeline,
        MAX(w.storage_capacity_units) AS storage_capacity_units
    FROM daily_product_warehouse_metrics d
    INNER JOIN warehouses w
        ON d.warehouse_id = w.warehouse_id
    GROUP BY d.date, d.warehouse_id
),
warehouse_agg AS (
    SELECT
        warehouse_id,
        MAX(warehouse_name) AS warehouse_name,
        MAX(region) AS region,
        CASE
            WHEN SUM(units_demanded) = 0 THEN 1.0
            ELSE CAST(SUM(units_fulfilled) AS DOUBLE) / CAST(SUM(units_demanded) AS DOUBLE)
        END AS fill_rate,
        CASE
            WHEN SUM(units_demanded) = 0 THEN 0.0
            ELSE CAST(SUM(units_lost_sales) AS DOUBLE) / CAST(SUM(units_demanded) AS DOUBLE)
        END AS stockout_rate,
        SUM(lost_sales_value) AS lost_sales_value,
        AVG(average_days_of_supply) AS average_days_of_supply,
        AVG(inventory_value) AS inventory_value,
        AVG(CAST(inventory_units_plus_pipeline AS DOUBLE) / NULLIF(CAST(storage_capacity_units AS DOUBLE), 0.0)) AS capacity_pressure_proxy
    FROM warehouse_daily
    GROUP BY warehouse_id
),
risk_calc AS (
    SELECT
        *,
        100.0 * (
            0.40 * stockout_rate +
            0.30 * (1.0 - fill_rate) +
            0.20 * LEAST(capacity_pressure_proxy, 1.0) +
            0.10 * LEAST(average_days_of_supply / 50.0, 1.0)
        ) AS warehouse_service_risk_proxy
    FROM warehouse_agg
)
SELECT *
FROM risk_calc;

-- ============================================================
-- View 5: inventory_risk_base
-- Grain: product_id + warehouse_id
-- Purpose: normalized base signals for overstock / working-capital diagnostics.
-- ============================================================
CREATE OR REPLACE VIEW inventory_risk_base AS
WITH sku_wh AS (
    SELECT
        product_id,
        warehouse_id,
        category,
        abc_class,
        AVG(days_of_supply) AS avg_days_of_supply,
        AVG(inventory_value) AS avg_inventory_value,
        AVG(CASE WHEN available_units > 0 AND units_fulfilled = 0 THEN 1.0 ELSE 0.0 END) AS slow_moving_day_rate,
        AVG(
            CASE
                WHEN abc_class = 'A' AND days_of_supply > 20 THEN 1.0
                WHEN abc_class = 'B' AND days_of_supply > 30 THEN 1.0
                WHEN abc_class NOT IN ('A', 'B') AND days_of_supply > 45 THEN 1.0
                ELSE 0.0
            END
        ) AS excess_inventory_day_rate
    FROM daily_product_warehouse_metrics
    GROUP BY product_id, warehouse_id, category, abc_class
),
scored AS (
    SELECT
        *,
        CASE
            WHEN abc_class = 'A' THEN avg_days_of_supply / 20.0
            WHEN abc_class = 'B' THEN avg_days_of_supply / 30.0
            ELSE avg_days_of_supply / 45.0
        END AS dos_stretch_ratio
    FROM sku_wh
)
SELECT
    product_id,
    warehouse_id,
    category,
    abc_class,
    avg_days_of_supply,
    avg_inventory_value,
    excess_inventory_day_rate,
    slow_moving_day_rate,
    dos_stretch_ratio,
    100.0 * (
        0.45 * LEAST(dos_stretch_ratio / 2.25, 1.0) +
        0.35 * LEAST(excess_inventory_day_rate / 0.40, 1.0) +
        0.20 * LEAST(slow_moving_day_rate / 0.12, 1.0)
    ) AS inventory_risk_score_base
FROM scored;

-- ============================================================
-- View 6: service_risk_base
-- Grain: product_id + warehouse_id
-- Purpose: normalized base signals for service failure diagnostics.
-- ============================================================
CREATE OR REPLACE VIEW service_risk_base AS
WITH sku_wh AS (
    SELECT
        product_id,
        warehouse_id,
        category,
        supplier_id,
        criticality_level,
        SUM(units_demanded) AS units_demanded,
        SUM(units_fulfilled) AS units_fulfilled,
        SUM(units_lost_sales) AS units_lost_sales,
        SUM(service_gap_units) AS service_gap_units,
        SUM(lost_sales_revenue) AS lost_sales_revenue,
        AVG(stockout_flag) AS stockout_day_rate
    FROM daily_product_warehouse_metrics
    GROUP BY product_id, warehouse_id, category, supplier_id, criticality_level
),
scored AS (
    SELECT
        *,
        CASE
            WHEN units_demanded = 0 THEN 1.0
            ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE)
        END AS fill_rate,
        CASE
            WHEN units_demanded = 0 THEN 0.0
            ELSE CAST(units_lost_sales AS DOUBLE) / CAST(units_demanded AS DOUBLE)
        END AS stockout_rate,
        CASE
            WHEN units_demanded = 0 THEN 0.0
            ELSE CAST(service_gap_units AS DOUBLE) / CAST(units_demanded AS DOUBLE)
        END AS service_gap_rate,
        CASE
            WHEN criticality_level = 'High' THEN 1.0
            WHEN criticality_level = 'Medium' THEN 0.6
            ELSE 0.3
        END AS criticality_weight
    FROM sku_wh
)
SELECT
    product_id,
    warehouse_id,
    category,
    supplier_id,
    criticality_level,
    units_demanded,
    units_fulfilled,
    units_lost_sales,
    fill_rate,
    stockout_rate,
    stockout_day_rate,
    service_gap_rate,
    lost_sales_revenue,
    100.0 * (
        0.35 * LEAST((1.0 - fill_rate) / 0.15, 1.0) +
        0.30 * LEAST(service_gap_rate / 0.20, 1.0) +
        0.20 * LEAST(criticality_weight / 0.85, 1.0) +
        0.15 * LEAST(stockout_rate / 0.18, 1.0)
    ) AS service_risk_score_base
FROM scored;
