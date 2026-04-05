-- Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System
-- File: 01_schema.sql
-- Purpose: Define raw-layer physical structures and key constraints.

-- ============================================================
-- RAW TABLES
-- ============================================================

-- Grain: 1 row per product_id
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    unit_cost DECIMAL(12, 2) NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    shelf_life_days INTEGER NOT NULL,
    supplier_id VARCHAR NOT NULL,
    lead_time_days INTEGER NOT NULL,
    target_service_level DECIMAL(6, 4) NOT NULL
);

-- Grain: 1 row per supplier_id
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id VARCHAR PRIMARY KEY,
    supplier_name VARCHAR NOT NULL,
    supplier_region VARCHAR NOT NULL,
    reliability_score DECIMAL(6, 4) NOT NULL,
    average_lead_time_days INTEGER NOT NULL,
    lead_time_variability DECIMAL(8, 4) NOT NULL,
    minimum_order_qty INTEGER NOT NULL
);

-- Grain: 1 row per warehouse_id
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id VARCHAR PRIMARY KEY,
    warehouse_name VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    storage_capacity_units BIGINT NOT NULL
);

-- Grain: 1 row per snapshot_date + warehouse_id + product_id
CREATE TABLE IF NOT EXISTS inventory_snapshots (
    snapshot_date DATE NOT NULL,
    warehouse_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    on_hand_units BIGINT NOT NULL,
    on_order_units BIGINT NOT NULL,
    reserved_units BIGINT NOT NULL,
    available_units BIGINT NOT NULL,
    inventory_value DECIMAL(18, 2) NOT NULL,
    days_of_supply DECIMAL(12, 2) NOT NULL,
    CONSTRAINT inventory_snapshots_pk PRIMARY KEY (snapshot_date, warehouse_id, product_id)
);

-- Grain: 1 row per date + warehouse_id + product_id
CREATE TABLE IF NOT EXISTS demand_history (
    date DATE NOT NULL,
    warehouse_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    units_demanded BIGINT NOT NULL,
    units_fulfilled BIGINT NOT NULL,
    units_lost_sales BIGINT NOT NULL,
    stockout_flag SMALLINT NOT NULL,
    promo_flag SMALLINT NOT NULL,
    seasonality_index DECIMAL(8, 4) NOT NULL,
    CONSTRAINT demand_history_pk PRIMARY KEY (date, warehouse_id, product_id)
);

-- Grain: 1 row per po_id
CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id VARCHAR PRIMARY KEY,
    supplier_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    warehouse_id VARCHAR NOT NULL,
    order_date DATE NOT NULL,
    expected_arrival_date DATE NOT NULL,
    actual_arrival_date DATE NOT NULL,
    ordered_units BIGINT NOT NULL,
    received_units BIGINT NOT NULL,
    late_delivery_flag SMALLINT NOT NULL
);

-- Grain: 1 row per product_id
CREATE TABLE IF NOT EXISTS product_classification (
    product_id VARCHAR PRIMARY KEY,
    abc_class VARCHAR NOT NULL,
    criticality_level VARCHAR NOT NULL
);

-- ============================================================
-- ANALYTICS INDEXES (helpful for Postgres; DuckDB will ignore if unsupported)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_wh_prod_date
    ON inventory_snapshots (warehouse_id, product_id, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_demand_history_wh_prod_date
    ON demand_history (warehouse_id, product_id, date);

CREATE INDEX IF NOT EXISTS idx_purchase_orders_supplier_date
    ON purchase_orders (supplier_id, order_date);

CREATE INDEX IF NOT EXISTS idx_purchase_orders_prod_wh_date
    ON purchase_orders (product_id, warehouse_id, order_date);
