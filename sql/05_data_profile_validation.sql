-- File: 05_data_profile_validation.sql
-- Purpose: formal data profiling quality checks for core supply chain datasets.

WITH
products_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT product_id, COUNT(*) AS cnt
        FROM products
        GROUP BY product_id
        HAVING COUNT(*) > 1
    ) t
),
suppliers_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT supplier_id, COUNT(*) AS cnt
        FROM suppliers
        GROUP BY supplier_id
        HAVING COUNT(*) > 1
    ) t
),
warehouses_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT warehouse_id, COUNT(*) AS cnt
        FROM warehouses
        GROUP BY warehouse_id
        HAVING COUNT(*) > 1
    ) t
),
inventory_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT snapshot_date, warehouse_id, product_id, COUNT(*) AS cnt
        FROM inventory_snapshots
        GROUP BY snapshot_date, warehouse_id, product_id
        HAVING COUNT(*) > 1
    ) t
),
demand_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT date, warehouse_id, product_id, COUNT(*) AS cnt
        FROM demand_history
        GROUP BY date, warehouse_id, product_id
        HAVING COUNT(*) > 1
    ) t
),
po_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT po_id, COUNT(*) AS cnt
        FROM purchase_orders
        GROUP BY po_id
        HAVING COUNT(*) > 1
    ) t
),
classification_pk_duplicates AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT product_id, COUNT(*) AS cnt
        FROM product_classification
        GROUP BY product_id
        HAVING COUNT(*) > 1
    ) t
),

fk_products_supplier_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM products p
    LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id
    WHERE s.supplier_id IS NULL
),
fk_inventory_product_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM inventory_snapshots i
    LEFT JOIN products p ON i.product_id = p.product_id
    WHERE p.product_id IS NULL
),
fk_inventory_warehouse_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM inventory_snapshots i
    LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
    WHERE w.warehouse_id IS NULL
),
fk_demand_product_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM demand_history d
    LEFT JOIN products p ON d.product_id = p.product_id
    WHERE p.product_id IS NULL
),
fk_demand_warehouse_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM demand_history d
    LEFT JOIN warehouses w ON d.warehouse_id = w.warehouse_id
    WHERE w.warehouse_id IS NULL
),
fk_po_supplier_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM purchase_orders po
    LEFT JOIN suppliers s ON po.supplier_id = s.supplier_id
    WHERE s.supplier_id IS NULL
),
fk_po_product_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM purchase_orders po
    LEFT JOIN products p ON po.product_id = p.product_id
    WHERE p.product_id IS NULL
),
fk_po_warehouse_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM purchase_orders po
    LEFT JOIN warehouses w ON po.warehouse_id = w.warehouse_id
    WHERE w.warehouse_id IS NULL
),
fk_classification_product_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM product_classification pc
    LEFT JOIN products p ON pc.product_id = p.product_id
    WHERE p.product_id IS NULL
),

nulls_critical_fields AS (
    SELECT
        (
            SELECT SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN supplier_id IS NULL THEN 1 ELSE 0 END)
            FROM products
        )
        +
        (
            SELECT SUM(CASE WHEN supplier_id IS NULL THEN 1 ELSE 0 END)
            FROM suppliers
        )
        +
        (
            SELECT SUM(CASE WHEN warehouse_id IS NULL THEN 1 ELSE 0 END)
            FROM warehouses
        )
        +
        (
            SELECT SUM(CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN warehouse_id IS NULL THEN 1 ELSE 0 END)
            FROM inventory_snapshots
        )
        +
        (
            SELECT SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN warehouse_id IS NULL THEN 1 ELSE 0 END)
            FROM demand_history
        )
        +
        (
            SELECT SUM(CASE WHEN po_id IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN supplier_id IS NULL THEN 1 ELSE 0 END)
                 + SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
            FROM purchase_orders
        ) AS issue_count
),

impossible_negative_values AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM products
            WHERE unit_cost < 0 OR unit_price < 0 OR shelf_life_days < 0 OR lead_time_days < 0
        )
        +
        (
            SELECT COUNT(*)
            FROM suppliers
            WHERE reliability_score < 0 OR reliability_score > 1
               OR average_lead_time_days < 0 OR lead_time_variability < 0 OR minimum_order_qty < 0
        )
        +
        (
            SELECT COUNT(*)
            FROM inventory_snapshots
            WHERE on_hand_units < 0 OR on_order_units < 0 OR reserved_units < 0
               OR available_units < 0 OR inventory_value < 0 OR days_of_supply < 0
        )
        +
        (
            SELECT COUNT(*)
            FROM demand_history
            WHERE units_demanded < 0 OR units_fulfilled < 0 OR units_lost_sales < 0
        )
        +
        (
            SELECT COUNT(*)
            FROM purchase_orders
            WHERE ordered_units < 0 OR received_units < 0
        ) AS issue_count
),

stockout_logic_inconsistencies AS (
    SELECT COUNT(*) AS issue_count
    FROM demand_history
    WHERE (units_fulfilled + units_lost_sales) <> units_demanded
       OR (stockout_flag = 1 AND units_lost_sales = 0)
       OR (stockout_flag = 0 AND units_lost_sales > 0)
),

inventory_value_consistency AS (
    SELECT COUNT(*) AS issue_count
    FROM inventory_snapshots i
    JOIN products p ON i.product_id = p.product_id
    WHERE ABS(i.inventory_value - (CAST(i.on_hand_units AS DOUBLE) * p.unit_cost)) > 0.05
),

po_date_inconsistencies AS (
    SELECT COUNT(*) AS issue_count
    FROM purchase_orders
    WHERE expected_arrival_date < order_date
       OR actual_arrival_date < order_date
),

po_receipt_overrun AS (
    SELECT COUNT(*) AS issue_count
    FROM purchase_orders
    WHERE received_units > ordered_units
),

region_mapping_mismatch AS (
    SELECT COUNT(*) AS issue_count
    FROM demand_history d
    JOIN warehouses w ON d.warehouse_id = w.warehouse_id
    WHERE d.region <> w.region
),

date_coverage_demand AS (
    SELECT
        GREATEST(
            DATE_DIFF('day', MIN(date), MAX(date)) + 1 - COUNT(DISTINCT date),
            0
        ) AS issue_count
    FROM demand_history
),

date_coverage_inventory AS (
    SELECT
        GREATEST(
            DATE_DIFF('day', MIN(snapshot_date), MAX(snapshot_date)) + 1 - COUNT(DISTINCT snapshot_date),
            0
        ) AS issue_count
    FROM inventory_snapshots
),

supplier_signal_missing AS (
    SELECT COUNT(*) AS issue_count
    FROM (
        SELECT s.supplier_id, COUNT(po.po_id) AS po_count
        FROM suppliers s
        LEFT JOIN purchase_orders po
            ON s.supplier_id = po.supplier_id
        GROUP BY s.supplier_id
        HAVING COUNT(po.po_id) = 0
    ) t
)

SELECT 'pk_duplicates_products' AS check_name, issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM products_pk_duplicates
UNION ALL
SELECT 'pk_duplicates_suppliers', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM suppliers_pk_duplicates
UNION ALL
SELECT 'pk_duplicates_warehouses', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouses_pk_duplicates
UNION ALL
SELECT 'pk_duplicates_inventory_snapshots', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM inventory_pk_duplicates
UNION ALL
SELECT 'pk_duplicates_demand_history', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM demand_pk_duplicates
UNION ALL
SELECT 'pk_duplicates_purchase_orders', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM po_pk_duplicates
UNION ALL
SELECT 'pk_duplicates_product_classification', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM classification_pk_duplicates
UNION ALL
SELECT 'fk_products_supplier_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_products_supplier_missing
UNION ALL
SELECT 'fk_inventory_product_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_inventory_product_missing
UNION ALL
SELECT 'fk_inventory_warehouse_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_inventory_warehouse_missing
UNION ALL
SELECT 'fk_demand_product_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_demand_product_missing
UNION ALL
SELECT 'fk_demand_warehouse_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_demand_warehouse_missing
UNION ALL
SELECT 'fk_po_supplier_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_po_supplier_missing
UNION ALL
SELECT 'fk_po_product_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_po_product_missing
UNION ALL
SELECT 'fk_po_warehouse_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_po_warehouse_missing
UNION ALL
SELECT 'fk_product_classification_product_missing', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fk_classification_product_missing
UNION ALL
SELECT 'nulls_critical_fields', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM nulls_critical_fields
UNION ALL
SELECT 'impossible_negative_values', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM impossible_negative_values
UNION ALL
SELECT 'stockout_logic_inconsistencies', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM stockout_logic_inconsistencies
UNION ALL
SELECT 'inventory_value_consistency', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM inventory_value_consistency
UNION ALL
SELECT 'po_date_inconsistencies', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM po_date_inconsistencies
UNION ALL
SELECT 'po_receipt_overrun', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM po_receipt_overrun
UNION ALL
SELECT 'region_mapping_mismatch', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM region_mapping_mismatch
UNION ALL
SELECT 'date_coverage_demand_missing_days', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM date_coverage_demand
UNION ALL
SELECT 'date_coverage_inventory_missing_days', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END
FROM date_coverage_inventory
UNION ALL
SELECT 'supplier_signal_missing_zero_po', issue_count,
       CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'WARN' END
FROM supplier_signal_missing
ORDER BY check_name;
