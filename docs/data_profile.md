# Data Profile and Quality Audit

Project: Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

## 1) Data Profile Summary

| table_name | grain | likely_primary_key | row_count | duplicate_key_rows | overall_null_rate | impossible_value_rows |
| --- | --- | --- | --- | --- | --- | --- |
| products | 1 row per product_id | product_id | 120 | 0 | 0.0 | 0 |
| suppliers | 1 row per supplier_id | supplier_id | 12 | 0 | 0.0 | 0 |
| warehouses | 1 row per warehouse_id | warehouse_id | 4 | 0 | 0.0 | 0 |
| inventory_snapshots | 1 row per snapshot_date + warehouse_id + product_id | snapshot_date, warehouse_id, product_id | 350880 | 0 | 0.0 | 0 |
| demand_history | 1 row per date + warehouse_id + product_id | date, warehouse_id, product_id | 350880 | 0 | 0.0 | 0 |
| purchase_orders | 1 row per po_id | po_id | 13369 | 0 | 0.0 | 0 |
| product_classification | 1 row per product_id | product_id | 120 | 0 | 0.0 | 0 |

## 2) Data Quality Issues

| issue_category | severity | object_name | issue | issue_count | recommended_rule |
| --- | --- | --- | --- | --- | --- |
| time_series_or_signal | Medium | skewed_days_of_supply_distribution | High p99/p50 DOS ratio indicates heavy right tail and potential overstock outliers. | 9.368 | Use coverage thresholds and minimum sample gates in KPI dashboards. |

SQL validation status summary:
- FAIL checks: 0
- WARN checks: 0
- PASS checks: 26

## 3) Risks to Downstream Interpretation

- Potential join risks are explicitly quantified in outputs/tables/data_profile_join_risks.csv.
- Time-series continuity is checked at date-level and full cube completeness for demand/inventory.
- Inventory skew is flagged using p99/p50 ratios for days_of_supply and inventory_value.
- Missing supplier signal risk is checked through zero/low PO coverage by supplier.
- Fields not to use naively: stockout_flag (binary event), target_service_level (policy target), reliability_score (prior score), late_delivery_flag (not volume-weighted), days_of_supply (unstable at low demand).

Top join/time-series risk indicators:

| risk_name | issue_count | severity | details |
| --- | --- | --- | --- |
| demand_rows_without_inventory_snapshot | 0 | Info | Left join drops service rows if inventory snapshot missing on same day-SKU-warehouse key. |
| inventory_rows_without_demand_row | 0 | Info | Inventory-only rows can distort DOS averages if naively joined to demand facts. |
| demand_region_vs_warehouse_region_mismatch | 0 | Info | Region attribute mismatch can cause wrong regional service rollups. |
| po_supplier_vs_product_master_supplier_mismatch | 0 | Info | Supplier attribution for lead-time and service impact can be wrong if PO supplier differs from product master supplier. |

| issue_name | observed_rows | expected_rows | gap_rows | severity | details |
| --- | --- | --- | --- | --- | --- |
| demand_time_series_row_completeness | 350880.0 | 350880.0 | 0.0 | Info | Checks full daily cube coverage by date-warehouse-product for demand history. |
| inventory_time_series_row_completeness | 350880.0 | 350880.0 | 0.0 | Info | Checks full daily cube coverage by date-warehouse-product for inventory snapshots. |
| missing_supplier_signals_zero_po | 0.0 | 0.0 | 0.0 | Info | Suppliers with zero transactions have no observed lead-time or lateness signal. |
| weak_supplier_signal_low_po_count | 0.0 | 0.0 | 0.0 | Info | Supplier comparisons are noisy when PO volume is too low. |
| skewed_days_of_supply_distribution | 9.368 | 8.0 | 1.368 | Medium | High p99/p50 DOS ratio indicates heavy right tail and potential overstock outliers. |
| skewed_inventory_value_distribution | 12.36 | 15.0 | -2.64 | Info | Inventory value concentration can bias averages; use weighted and percentile views. |

## 4) Recommended Cleaning / Handling Rules

- Enforce key uniqueness on all table grains before any KPI aggregation.
- Reject or quarantine rows with impossible values (negative units/value, invalid flags).
- Use anti-join exception tables before fact-to-fact joins (demand vs inventory).
- For supplier scorecards, require minimum PO count threshold before ranking suppliers.
- Winsorize or percentile-cap days_of_supply when computing portfolio averages.
- For service KPIs, use weighted rates with denominators; do not average row-level flags directly.

## 5) Recommended Analytical Focus Areas

- Service-risk segmentation by warehouse and ABC class with denominator-controlled fill-rate trends.
- Inventory working-capital concentration: top decile SKUs by average inventory value and DOS outliers.
- Supplier execution diagnostics: on-time rate, lead-time variability, and downstream lost-sales coupling.
- Dual-failure lens: SKU-warehouse combinations showing both high stockout-day rate and excess DOS.
- Promo-adjusted service analysis separating baseline vs promo periods.

## Appendix: Table-Level Modeling Notes

| table_name | useful_dimensions | useful_metrics | likely_analytical_pitfalls |
| --- | --- | --- | --- |
| products | category; supplier_id; shelf_life_days | unit_cost; unit_price; lead_time_days; target_service_level | target_service_level is policy target, not realized performance; lead_time_days is static master value; use PO realized lead time for execution analytics |
| suppliers | supplier_region; supplier_name | reliability_score; average_lead_time_days; lead_time_variability; minimum_order_qty | reliability_score is prior/master signal and should not replace observed OTD from transactions; minimum_order_qty can drive overstock and should be used with demand context |
| warehouses | region; warehouse_name | storage_capacity_units | capacity is static and should not be interpreted as effective usable capacity without utilization constraints |
| inventory_snapshots | warehouse_id; product_id; snapshot_date | on_hand_units; on_order_units; reserved_units; available_units; inventory_value; days_of_supply | days_of_supply can be unstable for low-demand SKUs and should be winsorized/segmented; inventory_value is point-in-time and should be averaged for working-capital interpretation |
| demand_history | warehouse_id; product_id; region; promo_flag | units_demanded; units_fulfilled; units_lost_sales; stockout_flag; seasonality_index | stockout_flag is event-level binary and should not be summed without denominator; promo_flag should be controlled for when comparing service rates; seasonality_index is a modeled driver and should not be treated as observed KPI |
| purchase_orders | supplier_id; warehouse_id; product_id | ordered_units; received_units; late_delivery_flag | late_delivery_flag is not volume-weighted; combine with ordered_units for impact; received_units timing can lag and should be aligned with arrival-date cohorts |
| product_classification | abc_class; criticality_level |  | ABC class is static in this dataset and should be refreshed if demand mix changes |