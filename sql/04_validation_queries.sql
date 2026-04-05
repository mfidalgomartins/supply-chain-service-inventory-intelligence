-- Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System
-- File: 04_validation_queries.sql
-- Purpose: SQL QA checks for data quality, metric integrity, and logical consistency.

-- ============================================================
-- Validation Summary (single output table of checks)
-- ============================================================
WITH
-- 1) Duplicate key checks
inventory_duplicate_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT snapshot_date, warehouse_id, product_id, COUNT(*) AS row_count
        FROM inventory_snapshots
        GROUP BY snapshot_date, warehouse_id, product_id
        HAVING COUNT(*) > 1
    ) d
),
demand_duplicate_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT date, warehouse_id, product_id, COUNT(*) AS row_count
        FROM demand_history
        GROUP BY date, warehouse_id, product_id
        HAVING COUNT(*) > 1
    ) d
),
po_duplicate_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT po_id, COUNT(*) AS row_count
        FROM purchase_orders
        GROUP BY po_id
        HAVING COUNT(*) > 1
    ) d
),

-- 2) Null checks in critical columns
critical_nulls AS (
    SELECT
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) +
        SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) +
        SUM(CASE WHEN unit_cost IS NULL THEN 1 ELSE 0 END) +
        SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS issue_count
    FROM products
),

-- 3) Impossible negative values
negative_values AS (
    SELECT
        (
            SELECT COUNT(*) FROM inventory_snapshots
            WHERE on_hand_units < 0 OR on_order_units < 0 OR reserved_units < 0 OR available_units < 0 OR inventory_value < 0
        )
        +
        (
            SELECT COUNT(*) FROM demand_history
            WHERE units_demanded < 0 OR units_fulfilled < 0 OR units_lost_sales < 0
        )
        +
        (
            SELECT COUNT(*) FROM purchase_orders
            WHERE ordered_units < 0 OR received_units < 0
        )
        AS issue_count
),

-- 4) Fill rate outside [0, 1]
fill_rate_out_of_bounds AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT
            date,
            warehouse_id,
            product_id,
            CASE WHEN units_demanded = 0 THEN NULL
                 ELSE CAST(units_fulfilled AS DOUBLE) / CAST(units_demanded AS DOUBLE)
            END AS fill_rate
        FROM demand_history
    ) f
    WHERE fill_rate IS NOT NULL
      AND (fill_rate < 0 OR fill_rate > 1)
),

-- 5) Stockout logic inconsistencies
stockout_logic_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM demand_history
    WHERE (stockout_flag = 1 AND units_lost_sales = 0)
       OR (stockout_flag = 0 AND units_lost_sales > 0)
       OR (units_fulfilled + units_lost_sales <> units_demanded)
),

-- 6) Inventory value consistency (inventory_value ~= on_hand_units * unit_cost)
inventory_value_mismatch AS (
    SELECT COUNT(*) AS issue_count
    FROM inventory_snapshots i
    INNER JOIN products p
        ON i.product_id = p.product_id
    WHERE ABS(i.inventory_value - (CAST(i.on_hand_units AS DOUBLE) * p.unit_cost)) > 0.05
),

-- 7) PO date inconsistencies
po_date_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM purchase_orders
    WHERE expected_arrival_date < order_date
       OR actual_arrival_date < order_date
),

-- 8) Denominator sanity checks
zero_demand_with_activity AS (
    SELECT COUNT(*) AS issue_count
    FROM demand_history
    WHERE units_demanded = 0
      AND (units_fulfilled > 0 OR units_lost_sales > 0)
),
invalid_available_units AS (
    SELECT COUNT(*) AS issue_count
    FROM inventory_snapshots
    WHERE available_units <> (on_hand_units - reserved_units)
)

SELECT 'duplicate_keys_inventory_snapshots' AS check_name, issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM inventory_duplicate_keys
UNION ALL
SELECT 'duplicate_keys_demand_history', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM demand_duplicate_keys
UNION ALL
SELECT 'duplicate_keys_purchase_orders', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM po_duplicate_keys
UNION ALL
SELECT 'critical_null_fields', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM critical_nulls
UNION ALL
SELECT 'impossible_negative_values', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM negative_values
UNION ALL
SELECT 'fill_rate_out_of_bounds', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fill_rate_out_of_bounds
UNION ALL
SELECT 'stockout_logic_inconsistencies', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM stockout_logic_issues
UNION ALL
SELECT 'inventory_value_consistency', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM inventory_value_mismatch
UNION ALL
SELECT 'po_date_inconsistencies', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM po_date_issues
UNION ALL
SELECT 'zero_demand_denominator_sanity', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM zero_demand_with_activity
UNION ALL
SELECT 'available_units_consistency', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM invalid_available_units
ORDER BY check_name;


-- ============================================================
-- Optional detailed drill-down snippets for failures
-- ============================================================

-- Duplicate demand rows (if any)
-- SELECT date, warehouse_id, product_id, COUNT(*) AS row_count
-- FROM demand_history
-- GROUP BY 1,2,3
-- HAVING COUNT(*) > 1
-- ORDER BY row_count DESC;

-- Inventory value mismatches (if any)
-- SELECT i.snapshot_date, i.warehouse_id, i.product_id,
--        i.inventory_value,
--        CAST(i.on_hand_units AS DOUBLE) * p.unit_cost AS expected_inventory_value
-- FROM inventory_snapshots i
-- JOIN products p ON i.product_id = p.product_id
-- WHERE ABS(i.inventory_value - (CAST(i.on_hand_units AS DOUBLE) * p.unit_cost)) > 0.05
-- ORDER BY ABS(i.inventory_value - (CAST(i.on_hand_units AS DOUBLE) * p.unit_cost)) DESC
-- LIMIT 100;
