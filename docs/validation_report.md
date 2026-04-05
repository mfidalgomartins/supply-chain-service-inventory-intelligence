# Validation Report

Generated at: 2026-04-05 20:50 UTC

Formal pre-delivery QA for the Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System.

## 1) Validation Report

- Total checks: **69**
- Passed: **69**
- Failed: **0**
- Warnings: **0**
- SQL check failures: **0**
- Confidence score: **100/100** (High)
- Release classification: **decision-support only**
- Publish blocked: **No**

### Confirmed vs Estimated
- Confirmed (data-integrity/logic): key uniqueness, nulls, non-negativity, fill-rate and stockout logic, reconciliation of aggregates, scoring formula coherence, chart-file generation, dashboard reconciliation.
- Estimated/proxy: excess-inventory value, trapped working-capital value, supplier-delay impact, and 12-month opportunity estimates.

### Release-State Matrix
| State | Status | Criteria | Implication |
|---|---|---|---|
| Technically Valid | PASS | No FAIL checks in raw/processed/scoring/dashboard integrity controls. | Foundational data and metric logic are internally coherent. |
| Analytically Acceptable | PASS | Technical validity plus no analytical FAIL and no high-severity WARN. | Interpretations and prioritization outputs are fit for controlled internal analysis. |
| Decision-Support Only | PASS | Analytically acceptable with caveated proxy economics. | Suitable for leadership prioritization and directional planning discussions. |
| Screening-Grade Only | FAIL | Technically valid but analytical rigor still below decision-support quality. | Use for triage/scoping only; no executive decision framing. |
| Not Committee-Grade | PASS | Synthetic data + proxy assumptions prevent audit-grade committee sign-off. | Do not represent as statutory/committee-grade evidence. |
| Publish-Blocked | FAIL | Any blocker/high failure or high-severity warning blocks release publication. | When PASS here, release cannot be promoted. |

### Check Matrix
| Check | Layer | Method | Severity | Status | Observed | Expected |
|---|---|---|---|---|---:|---:|
| rowcount_dense_demand_history | raw | Python | HIGH | PASS | 350880 | 350880 |
| rowcount_daily_equals_demand_history | processed | Python | HIGH | PASS | 350880 | 350880 |
| rowcount_sku_risk_expected_grain | processed | Python | HIGH | PASS | 480 | 480 |
| duplicates_demand_history | raw | Python | HIGH | PASS | 0 | 0 |
| duplicates_inventory_snapshots | raw | Python | HIGH | PASS | 0 | 0 |
| duplicates_purchase_orders | raw | Python | HIGH | PASS | 0 | 0 |
| duplicates_daily_product_warehouse_metrics | processed | Python | HIGH | PASS | 0 | 0 |
| products_critical_nulls | raw | Python | HIGH | PASS | 0 | 0 |
| suppliers_critical_nulls | raw | Python | HIGH | PASS | 0 | 0 |
| daily_critical_nulls | processed | Python | HIGH | PASS | 0 | 0 |
| sku_risk_critical_nulls | processed | Python | HIGH | PASS | 0 | 0 |
| impossible_negative_values | raw | Python | CRITICAL | PASS | 0 | 0 |
| fill_rate_logic_bounds_raw | raw | Python | CRITICAL | PASS | 0 | 0 |
| demand_balance_units | raw | Python | CRITICAL | PASS | 0 | 0 |
| stockout_flag_logic | raw | Python | CRITICAL | PASS | 0 | 0 |
| lost_sales_revenue_consistency_daily | processed | Python | HIGH | PASS | 0 | 0 |
| inventory_value_consistency_raw | raw | Python | CRITICAL | PASS | 0 | 0 |
| available_units_consistency_raw | raw | Python | CRITICAL | PASS | 0 | 0 |
| working_capital_proxy_overall_consistency | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| excess_inventory_proxy_overall_consistency | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| supplier_delay_proxy_consistency | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| aggregation_lost_sales_sku_to_overall | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| aggregation_lost_sales_warehouse_to_overall | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| aggregation_lost_sales_supplier_to_overall | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| aggregation_lost_sales_category_to_overall | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| impact_opportunity_formula_consistency | impact | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| denominator_zero_demand_with_activity_raw | raw | Python | CRITICAL | PASS | 0 | 0 |
| denominator_zero_demand_with_activity_daily | processed | Python | CRITICAL | PASS | 0 | 0 |
| scoring_formula_consistency | scoring | Python | HIGH | PASS | 0.000000 | <= 0.050000 |
| scoring_tier_consistency | scoring | Python | HIGH | PASS | 0 | 0 |
| scoring_main_driver_consistency | scoring | Python | MEDIUM | PASS | 0 | 0 |
| scoring_action_policy_non_low_not_monitor_only | scoring | Python | HIGH | PASS | 0 | 0 |
| scoring_top25_stability_under_weight_perturbation | scoring | Python | MEDIUM | PASS | min_overlap=0.880 | >= 0.650 |
| charts_required_files_present | visualization | Python | HIGH | PASS | 0 | 0 |
| charts_file_size_sanity | visualization | Python | LOW | PASS | 0 | 0 |
| upgrade_outputs_required_tables_present | analytics | Python | HIGH | PASS | 0 | 0 |
| data_contract_blocking_failures | analytics | Python | HIGH | PASS | 0 | 0 |
| data_contract_warning_sanity | analytics | Python | MEDIUM | PASS | 0 | 0 |
| pipeline_orchestration_log_present | analytics | Python | LOW | PASS | 1 | 1 |
| intervention_rank_order_consistency | analytics | Python | MEDIUM | PASS | 1 | 1 |
| intervention_status_priority_consistency | analytics | Python | HIGH | PASS | 1 | 1 |
| dashboard_metric_reconciliation | dashboard | Python | HIGH | PASS | 0.000000 | <= 1.000000 |
| dashboard_required_components_present | dashboard | Python | HIGH | PASS | 0 | 0 |
| dashboard_no_visible_technical_metadata | dashboard | Python | HIGH | PASS | 0 | 0 |
| dashboard_layout_no_absolute_positioning | dashboard | Python | MEDIUM | PASS | 0 | 0 |
| dashboard_responsive_rule_presence | dashboard | Python | HIGH | PASS | 0 | 0 |
| dashboard_payload_size_sanity | dashboard | Python | MEDIUM | PASS | 5982889 | <= 8500000 bytes (warn up to 10500000) |
| dashboard_frontend_governance_logic_forbidden | dashboard | Python | HIGH | PASS | 0 | 0 |
| dashboard_official_snapshot_present_and_reconciled | dashboard | Python | HIGH | PASS | 0.000000 | <= 0.000500 |
| executive_summary_fill_rate_consistency | reporting | Python | HIGH | PASS | 0.000009 | <= 0.001000 |
| executive_summary_stockout_rate_consistency | reporting | Python | HIGH | PASS | 0.000009 | <= 0.001000 |
| executive_summary_lost_sales_consistency | reporting | Python | MEDIUM | PASS | 292.050000 | <= 200000.000000 |
| written_conclusion_overclaiming_risk | reporting | Python | MEDIUM | PASS | causal_hits=0, mitigation_hits=23 | mitigation_hits >= causal_hits |
| available_units_consistency | raw | SQL | HIGH | PASS | 0.0 | 0 |
| critical_null_fields | raw | SQL | HIGH | PASS | 0.0 | 0 |
| duplicate_keys_demand_history | raw | SQL | HIGH | PASS | 0.0 | 0 |
| duplicate_keys_inventory_snapshots | raw | SQL | HIGH | PASS | 0.0 | 0 |
| duplicate_keys_purchase_orders | raw | SQL | HIGH | PASS | 0.0 | 0 |
| fill_rate_out_of_bounds | raw | SQL | HIGH | PASS | 0.0 | 0 |
| impossible_negative_values | raw | SQL | HIGH | PASS | 0.0 | 0 |
| inventory_value_consistency | raw | SQL | HIGH | PASS | 0.0 | 0 |
| po_date_inconsistencies | raw | SQL | HIGH | PASS | 0.0 | 0 |
| stockout_logic_inconsistencies | raw | SQL | HIGH | PASS | 0.0 | 0 |
| zero_demand_denominator_sanity | raw | SQL | HIGH | PASS | 0.0 | 0 |
| daily_duplicate_keys | processed | SQL | HIGH | PASS | 0 | 0 |
| daily_fill_rate_bounds | processed | SQL | HIGH | PASS | 0 | 0 |
| daily_stockout_logic | processed | SQL | HIGH | PASS | 0 | 0 |
| dashboard_expected_grain | processed | SQL | HIGH | PASS | 0 | 0 |
| sku_score_bounds | processed | SQL | HIGH | PASS | 0 | 0 |

## 2) Issues Found

- No FAIL/WARN issues detected in the pre-delivery validation suite.

## 3) Fixes Applied

- [APPLIED] FIX-001: Reworded KPI narrative line from 'supplier-attributed lost sales' to 'supplier-linked proxy lost sales' to reduce causal overclaim risk. (outputs/reports/executive_kpi_diagnostic_analysis.md).

## 4) Unresolved Caveats

- Impact opportunity values remain proxy estimates; 35% recoverable margin and 25% releasable working-capital assumptions materially affect estimated value pools.
- Supplier delay impact is an association proxy (delay severity x lost sales), not a causal attribution model.
- Dashboard metrics aggregate inventory value over time windows; for finance close processes, point-in-time inventory snapshots should be validated separately.

## 5) Final Confidence Assessment

- Delivery confidence: **High** (100/100).
- Release class: **decision-support only**.
- Recommendation: suitable for leadership review only when release class is decision-support only and proxy caveats remain explicit.
- Governance note: committee-grade publication remains blocked by synthetic-data and proxy-finance constraints.

## Supporting Outputs
- `/outputs/tables/validation_pre_delivery_checks.csv`
- `/outputs/tables/validation_pre_delivery_issues.csv`
- `/outputs/tables/validation_pre_delivery_sql_raw.csv`
- `/outputs/tables/validation_pre_delivery_sql_processed.csv`
- `/outputs/tables/validation_pre_delivery_fixes_applied.csv`
- `/outputs/tables/validation_release_state_matrix.csv`