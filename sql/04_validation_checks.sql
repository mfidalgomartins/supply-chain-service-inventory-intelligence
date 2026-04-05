CREATE OR REPLACE TABLE validation_checks AS
WITH checks AS (
    SELECT 'demand_balance_integrity' AS check_name,
           SUM(CASE WHEN fulfilled_qty + lost_sales_qty != demand_qty THEN 1 ELSE 0 END) AS issue_count,
           COUNT(*) AS total_rows
    FROM fact_sales_daily

    UNION ALL

    SELECT 'inventory_non_negative',
           SUM(CASE WHEN on_hand_qty < 0 THEN 1 ELSE 0 END),
           COUNT(*)
    FROM fact_inventory_daily

    UNION ALL

    SELECT 'service_level_bounds',
           SUM(CASE WHEN demand_qty = 0 THEN 0
                    WHEN fulfilled_qty * 1.0 / demand_qty < 0 OR fulfilled_qty * 1.0 / demand_qty > 1 THEN 1
                    ELSE 0 END),
           COUNT(*)
    FROM fact_sales_daily

    UNION ALL

    SELECT 'po_positive_quantities',
           SUM(CASE WHEN ordered_qty <= 0 OR received_qty < 0 THEN 1 ELSE 0 END),
           COUNT(*)
    FROM fact_purchase_orders
)
SELECT
    check_name,
    issue_count,
    total_rows,
    ROUND(issue_count * 1.0 / NULLIF(total_rows, 0), 6) AS issue_rate,
    CASE WHEN issue_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM checks
ORDER BY check_name;
