-- Supply Chain Service Level and Inventory Intelligence
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
    target_service_level DECIMAL(6, 4) NOT NULL,
    CONSTRAINT products_cost_non_negative CHECK (unit_cost >= 0),
    CONSTRAINT products_price_non_negative CHECK (unit_price >= 0),
    CONSTRAINT products_shelf_life_positive CHECK (shelf_life_days > 0),
    CONSTRAINT products_lead_time_positive CHECK (lead_time_days > 0),
    CONSTRAINT products_service_target_valid CHECK (target_service_level BETWEEN 0 AND 1)
);

-- Grain: 1 row per supplier_id
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id VARCHAR PRIMARY KEY,
    supplier_name VARCHAR NOT NULL,
    supplier_region VARCHAR NOT NULL,
    reliability_score DECIMAL(6, 4) NOT NULL,
    average_lead_time_days INTEGER NOT NULL,
    lead_time_variability DECIMAL(8, 4) NOT NULL,
    minimum_order_qty INTEGER NOT NULL,
    CONSTRAINT suppliers_reliability_valid CHECK (reliability_score BETWEEN 0 AND 1),
    CONSTRAINT suppliers_lead_time_positive CHECK (average_lead_time_days > 0),
    CONSTRAINT suppliers_variability_non_negative CHECK (lead_time_variability >= 0),
    CONSTRAINT suppliers_moq_positive CHECK (minimum_order_qty > 0)
);

-- Grain: 1 row per warehouse_id
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id VARCHAR PRIMARY KEY,
    warehouse_name VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    storage_capacity_units BIGINT NOT NULL,
    CONSTRAINT warehouses_capacity_positive CHECK (storage_capacity_units > 0)
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
    CONSTRAINT inventory_units_non_negative CHECK (
        on_hand_units >= 0 AND on_order_units >= 0 AND reserved_units >= 0 AND available_units >= 0
    ),
    CONSTRAINT inventory_value_non_negative CHECK (inventory_value >= 0),
    CONSTRAINT inventory_dos_non_negative CHECK (days_of_supply >= 0),
    CONSTRAINT inventory_reserved_valid CHECK (reserved_units <= on_hand_units),
    CONSTRAINT inventory_available_consistent CHECK (available_units = on_hand_units - reserved_units),
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
    seasonality_index DECIMAL(8, 4) NOT NULL CHECK (seasonality_index > 0),
    CONSTRAINT demand_units_non_negative CHECK (
        units_demanded >= 0 AND units_fulfilled >= 0 AND units_lost_sales >= 0
    ),
    CONSTRAINT demand_units_reconcile CHECK (units_fulfilled + units_lost_sales = units_demanded),
    CONSTRAINT demand_stockout_flag_valid CHECK (stockout_flag IN (0, 1)),
    CONSTRAINT demand_promo_flag_valid CHECK (promo_flag IN (0, 1)),
    CONSTRAINT demand_stockout_consistent CHECK (
        (stockout_flag = 1 AND units_lost_sales > 0)
        OR (stockout_flag = 0 AND units_lost_sales = 0)
    ),
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
    late_delivery_flag SMALLINT NOT NULL,
    CONSTRAINT purchase_orders_dates_valid CHECK (
        expected_arrival_date >= order_date AND actual_arrival_date >= order_date
    ),
    CONSTRAINT purchase_orders_units_valid CHECK (
        ordered_units > 0 AND received_units >= 0 AND received_units <= ordered_units
    ),
    CONSTRAINT purchase_orders_late_flag_valid CHECK (late_delivery_flag IN (0, 1)),
    CONSTRAINT purchase_orders_late_flag_consistent CHECK (
        (late_delivery_flag = 1 AND actual_arrival_date > expected_arrival_date)
        OR (late_delivery_flag = 0 AND actual_arrival_date <= expected_arrival_date)
    )
);

-- Grain: 1 row per product_id
CREATE TABLE IF NOT EXISTS product_classification (
    product_id VARCHAR PRIMARY KEY,
    abc_class VARCHAR NOT NULL,
    criticality_level VARCHAR NOT NULL,
    CONSTRAINT classification_abc_valid CHECK (abc_class IN ('A', 'B', 'C')),
    CONSTRAINT classification_criticality_valid CHECK (
        criticality_level IN ('High', 'Medium', 'Low')
    )
);

-- Grain: 1 row per experiment_id + unit_id
CREATE TABLE IF NOT EXISTS intervention_assignments (
    experiment_id VARCHAR NOT NULL,
    design VARCHAR NOT NULL,
    assignment_method VARCHAR NOT NULL,
    unit_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    warehouse_id VARCHAR NOT NULL,
    supplier_id VARCHAR NOT NULL,
    stratum VARCHAR NOT NULL,
    treatment_group VARCHAR NOT NULL,
    treatment_flag SMALLINT NOT NULL,
    assignment_date DATE NOT NULL,
    intervention_date DATE NOT NULL,
    outcome_metric VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    CONSTRAINT intervention_assignments_pk PRIMARY KEY (experiment_id, unit_id),
    CONSTRAINT intervention_design_valid CHECK (
        design IN ('randomized_controlled_trial', 'difference_in_differences')
    ),
    CONSTRAINT intervention_treatment_valid CHECK (treatment_flag IN (0, 1)),
    CONSTRAINT intervention_timing_valid CHECK (assignment_date < intervention_date),
    CONSTRAINT intervention_status_valid CHECK (status IN ('designed', 'active', 'completed'))
);

-- Grain: 1 row per physical or supplier node
CREATE TABLE IF NOT EXISTS network_nodes (
    node_id VARCHAR PRIMARY KEY,
    node_type VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    storage_capacity_units BIGINT NOT NULL,
    CONSTRAINT network_node_type_valid CHECK (
        node_type IN ('supplier', 'gateway', 'regional_dc')
    ),
    CONSTRAINT network_node_capacity_non_negative CHECK (storage_capacity_units >= 0)
);

-- Grain: 1 row per directed transport lane
CREATE TABLE IF NOT EXISTS network_lanes (
    lane_id VARCHAR PRIMARY KEY,
    source_node_id VARCHAR NOT NULL,
    destination_node_id VARCHAR NOT NULL,
    lane_type VARCHAR NOT NULL,
    lead_time_days INTEGER NOT NULL,
    unit_transport_cost DECIMAL(12, 4) NOT NULL,
    daily_capacity_units BIGINT NOT NULL,
    enabled SMALLINT NOT NULL,
    CONSTRAINT network_lane_nodes_distinct CHECK (source_node_id <> destination_node_id),
    CONSTRAINT network_lane_type_valid CHECK (lane_type IN ('inbound', 'transfer')),
    CONSTRAINT network_lane_lead_time_positive CHECK (lead_time_days > 0),
    CONSTRAINT network_lane_cost_non_negative CHECK (unit_transport_cost >= 0),
    CONSTRAINT network_lane_capacity_positive CHECK (daily_capacity_units > 0),
    CONSTRAINT network_lane_enabled_valid CHECK (enabled IN (0, 1))
);

-- Grain: 1 row per eligible product_id + supplier_id
CREATE TABLE IF NOT EXISTS product_sources (
    product_id VARCHAR NOT NULL,
    supplier_id VARCHAR NOT NULL,
    is_primary SMALLINT NOT NULL,
    unit_purchase_cost DECIMAL(12, 2) NOT NULL,
    minimum_order_qty BIGINT NOT NULL,
    max_horizon_units BIGINT NOT NULL,
    source_lead_time_days INTEGER NOT NULL,
    enabled SMALLINT NOT NULL,
    CONSTRAINT product_sources_pk PRIMARY KEY (product_id, supplier_id),
    CONSTRAINT product_source_primary_valid CHECK (is_primary IN (0, 1)),
    CONSTRAINT product_source_cost_non_negative CHECK (unit_purchase_cost >= 0),
    CONSTRAINT product_source_moq_positive CHECK (minimum_order_qty > 0),
    CONSTRAINT product_source_capacity_valid CHECK (max_horizon_units >= minimum_order_qty),
    CONSTRAINT product_source_lead_time_positive CHECK (source_lead_time_days > 0),
    CONSTRAINT product_source_enabled_valid CHECK (enabled IN (0, 1))
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

CREATE INDEX IF NOT EXISTS idx_intervention_assignments_design
    ON intervention_assignments (design, intervention_date, treatment_flag);

CREATE INDEX IF NOT EXISTS idx_network_lanes_nodes
    ON network_lanes (source_node_id, destination_node_id);

CREATE INDEX IF NOT EXISTS idx_product_sources_supplier
    ON product_sources (supplier_id, product_id);
