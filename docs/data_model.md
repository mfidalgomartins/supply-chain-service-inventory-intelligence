# Data Model

## Overview
The project uses a layered data model:
1. Raw operational tables (`/data/raw/`)
2. SQL intermediate analytical tables (`/data/processed/`)
3. Scored governance outputs (`/data/processed/` and `/outputs/tables/`)
4. Dashboard-serving fact/dim extracts (`/outputs/tables/`)

## Source Tables (Raw Layer)

| Table | File | Grain | Primary Key | Core Foreign Keys |
|---|---|---|---|---|
| products | `/data/raw/products.csv` | 1 row per SKU | `product_id` | `supplier_id -> suppliers.supplier_id` |
| suppliers | `/data/raw/suppliers.csv` | 1 row per supplier | `supplier_id` | None |
| warehouses | `/data/raw/warehouses.csv` | 1 row per warehouse | `warehouse_id` | None |
| inventory_snapshots | `/data/raw/inventory_snapshots.csv` | 1 row per `snapshot_date + warehouse_id + product_id` | composite | `warehouse_id -> warehouses`, `product_id -> products` |
| demand_history | `/data/raw/demand_history.csv` | 1 row per `date + warehouse_id + product_id` | composite | `warehouse_id -> warehouses`, `product_id -> products` |
| purchase_orders | `/data/raw/purchase_orders.csv` | 1 row per PO | `po_id` | `supplier_id -> suppliers`, `product_id -> products`, `warehouse_id -> warehouses` |
| product_classification | `/data/raw/product_classification.csv` | 1 row per SKU | `product_id` | `product_id -> products` |

Latest run row counts:
- products: 120
- suppliers: 12
- warehouses: 4
- inventory_snapshots: 350,880
- demand_history: 350,880
- purchase_orders: 13,369
- product_classification: 120

Date coverage:
- `2024-01-01` to `2025-12-31` (731 days)

## Join Design

Primary analytical join path:
- `demand_history` INNER JOIN `inventory_snapshots`
  - Keys: `date = snapshot_date`, `warehouse_id`, `product_id`
- Then enrich with:
  - `products` on `product_id`
  - `warehouses` on `warehouse_id`
  - `product_classification` on `product_id` (left join)

Supplier-performance path:
- `purchase_orders` grouped by `supplier_id`
- Joined to `suppliers` for full supplier master coverage (left join from supplier master)

Important modeling note:
- `daily_product_warehouse_metrics` is the canonical fact table for downstream KPI, scoring, impact, and dashboard layers.

## Intermediate Analytical Tables
Built via `sql/02_intermediate_views.sql` and materialized by `src/data_preparation.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| daily_product_warehouse_metrics | `/data/processed/daily_product_warehouse_metrics.csv` | `date + warehouse_id + product_id` | Canonical daily fact combining demand, inventory, commercial value, and policy context |
| supplier_performance_summary | `/data/processed/supplier_performance_summary.csv` | `supplier_id` | Supplier execution profile (OTD, delay, variability, underfill, risk proxy) |
| product_inventory_profile | `/data/processed/product_inventory_profile.csv` | `product_id` | SKU-level service/inventory behavior and working-capital proxy |
| warehouse_service_profile | `/data/processed/warehouse_service_profile.csv` | `warehouse_id` | Warehouse-level service performance and capacity pressure profile |
| inventory_risk_base (SQL view) | `sql/02_intermediate_views.sql` | `product_id + warehouse_id` | Base normalized overstock/working-capital diagnostics (not persisted by default) |
| service_risk_base (SQL view) | `sql/02_intermediate_views.sql` | `product_id + warehouse_id` | Base normalized service-failure diagnostics (not persisted by default) |

## Scoring and Priority Outputs
Built via `src/scoring.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| sku_risk_table | `/data/processed/sku_risk_table.csv` | `product_id + warehouse_id + supplier_id + category + region` | Primary intervention queue for SKU-location actions |
| supplier_risk_table | `/data/processed/supplier_risk_table.csv` | `supplier_id` | Supplier governance ranking |
| segment_risk_table | `/data/processed/segment_risk_table.csv` | `category + region` | Segment-level risk concentration and governance |
| governance_priority_master | `/data/processed/governance_priority_master.csv` | mixed entity list | Unified governance queue across SKU, supplier, and segment entities |

Mirrored reporting extracts are written to `/outputs/tables/` with `scoring_*.csv` names.

## Impact Outputs
Built via `src/impact_analysis.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| impact_by_sku | `/outputs/tables/impact_by_sku.csv` | `product_id + product_name + warehouse_id + category + supplier_id` | SKU-level financial exposure and opportunity proxy |
| impact_by_warehouse | `/outputs/tables/impact_by_warehouse.csv` | `warehouse_id + region` | Warehouse-level financial exposure and opportunity proxy |
| impact_by_supplier | `/outputs/tables/impact_by_supplier.csv` | `supplier_id` | Supplier-linked exposure and opportunity proxy |
| impact_by_category | `/outputs/tables/impact_by_category.csv` | `category` | Category-level value concentration and trade-off exposure |
| impact_overall_summary | `/outputs/tables/impact_overall_summary.csv` | metric row | Overall observed and annualized proxy metrics |
| impact_opportunity_priority | `/outputs/tables/impact_opportunity_priority.csv` | mixed entity list | Top prioritized opportunities across entity types |

## Policy Simulation Outputs
Built via `src/policy_simulation.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| policy_simulation_sku_scenarios | `/outputs/tables/policy_simulation_sku_scenarios.csv` | `scenario_name + product_id + warehouse_id` | Forecast-driven policy simulation by lane under alternative reorder assumptions |
| policy_simulation_frontier | `/outputs/tables/policy_simulation_frontier.csv` | `scenario_name` | Service vs inventory frontier summary for policy selection |

## Source Adapter and Forecast Outputs
Built via `src/source_adapter.py` and `src/probabilistic_forecast.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| source_adapter_readiness | `/outputs/tables/source_adapter_readiness.csv` | `table_name` | Readiness and schema-compatibility checks for external source promotion |
| source_refresh_manifest | `/outputs/tables/source_refresh_manifest.csv` | `artifact_path` | Artifact freshness manifest for refresh traceability |
| demand_forecast_lane_daily | `/outputs/tables/demand_forecast_lane_daily.csv` | `forecast_date + product_id + warehouse_id` | 30-day lane forecast distribution (q10/q50/q90) |
| demand_forecast_lane_summary | `/outputs/tables/demand_forecast_lane_summary.csv` | `product_id + warehouse_id + supplier_id` | Lane-level forecast uncertainty summary used by policy simulation |

## Policy Optimizer Outputs
Built via `src/policy_optimizer.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| policy_optimizer_lane_selection | `/outputs/tables/policy_optimizer_lane_selection.csv` | `budget_uplift + product_id + warehouse_id + supplier_id` | Lane-level scenario selection under capital budget constraints |
| policy_optimizer_budget_summary | `/outputs/tables/policy_optimizer_budget_summary.csv` | `budget_uplift` | Portfolio-level service and lost-sales improvement across budget tiers |

## Assumption Sensitivity Outputs
Built via `src/sensitivity_analysis.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| sensitivity_opportunity_grid | `/outputs/tables/sensitivity_opportunity_grid.csv` | `recoverable_margin_rate + releasable_wc_rate + slow_moving_incremental_weight` | Sensitivity grid for 12M opportunity proxy under alternate assumptions |
| sensitivity_opportunity_tornado | `/outputs/tables/sensitivity_opportunity_tornado.csv` | `factor` | Assumption influence summary (high-low opportunity swing) |

## Monte Carlo Stress Outputs
Built via `src/monte_carlo_stress.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| stress_monte_carlo_lane_results | `/outputs/tables/stress_monte_carlo_lane_results.csv` | `supplier_id + warehouse_id + category + product_id` | Simulated lane-level downside service risk under demand/lead-time uncertainty |
| stress_monte_carlo_segment_results | `/outputs/tables/stress_monte_carlo_segment_results.csv` | `supplier_id + warehouse_id + category` | Segment-level stress concentration for governance queueing |

## Supplier Lane Diagnostics Outputs
Built via `src/supplier_lane_diagnostics.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| supplier_lane_diagnostics | `/outputs/tables/supplier_lane_diagnostics.csv` | `supplier_id + warehouse_id + category` | Lane-level supplier diagnostics linking execution risk with downstream service outcomes |
| supplier_lane_supplier_summary | `/outputs/tables/supplier_lane_supplier_summary.csv` | `supplier_id` | Roll-up view of supplier lane risk concentration |

## PO Cohort, Intervention, and Alert Outputs
Built via `src/po_cohort_diagnostics.py`, `src/intervention_tracker.py`, and `src/anomaly_alerts.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| po_cohort_diagnostics | `/outputs/tables/po_cohort_diagnostics.csv` | `supplier_id + warehouse_id + cohort_month` | Monthly cohort-level supplier execution and downstream service linkage |
| po_cohort_lane_summary | `/outputs/tables/po_cohort_lane_summary.csv` | `supplier_id + warehouse_id` | Persistent cohort risk roll-up at lane level |
| intervention_register | `/outputs/tables/intervention_register.csv` | `intervention_id` | Action register with owners, due dates, expected value proxy, and closure evidence requirements |
| intervention_summary_by_owner | `/outputs/tables/intervention_summary_by_owner.csv` | `owner_function + intervention_status` | Backlog concentration by owner and status |
| intervention_summary_by_driver | `/outputs/tables/intervention_summary_by_driver.csv` | `main_risk_driver + intervention_type` | Root-driver concentration for governance cadence |
| anomaly_alerts | `/outputs/tables/anomaly_alerts.csv` | `alert_id` | Point-in-time operational spikes with severity and recommended immediate actions |
| anomaly_alerts_summary | `/outputs/tables/anomaly_alerts_summary.csv` | `entity_type + entity_id + severity` | Alert persistence and severity concentration |

## SQL Quality Gate Output
Built via `src/sql_quality_gate.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| ci_sql_validation_checks | `/outputs/tables/ci_sql_validation_checks.csv` | `check_name` | SQL quality-gate results used by CI and release controls |
| pipeline_run_log | `/outputs/tables/pipeline_run_log.csv` | `run_timestamp_utc + script_name` | Orchestration trace log with step-level runtime and status |
| validation_release_state_matrix | `/outputs/tables/validation_release_state_matrix.csv` | `state_name` | Explicit release-state governance matrix (technical, analytical, decision-support, screening, committee, publish gate) |

## Data Contract Outputs
Built via `src/data_contracts.py` using `contracts/table_contracts.json`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| data_contract_check_results | `/outputs/tables/data_contract_check_results.csv` | `table_name + check_name` | Contract-level pass/fail checks for required columns, uniqueness, nulls, and non-negative constraints |
| data_contract_table_profile | `/outputs/tables/data_contract_table_profile.csv` | `table_name` | Row/column counts, file sizes, and SHA256 signatures for traceability |

## Dashboard Data Model
Built via `src/executive_dashboard.py`.

Dashboard fact/dim exports:
- `/outputs/tables/dashboard_monthly_sku_fact.csv`
  - Grain: `month + region + warehouse_id + product_id + supplier_id + category + abc_class`
  - Contains aggregated demand, fulfillment, lost sales, inventory, DOS, excess/slow-moving/trapped WC proxies.
- `/outputs/tables/dashboard_supplier_dim.csv`
  - Grain: `supplier_id`
- `/outputs/tables/dashboard_warehouse_dim.csv`
  - Grain: `warehouse_id`
- `/outputs/tables/dashboard_sku_risk_baseline.csv`
  - Grain: `product_id + warehouse_id + supplier_id`
  - Governed risk/priority/action outputs consumed directly by dashboard drill-downs (no browser-side score calculation).
- `/outputs/tables/dashboard_official_snapshot.csv`
  - Grain: single row
  - Reconciled headline KPI snapshot used for dashboard consistency checks.
- `/outputs/tables/dashboard_build_manifest.csv`
  - Grain: single row
  - Dashboard version, data hash, and row-volume metadata for traceability.
- `/outputs/tables/dashboard_release_manifest.csv`
  - Grain: single row
  - Final HTML size/hash and build metadata for release discipline.

Runtime HTML payload:
- `/outputs/dashboard/index.html` embeds JSON with:
  - monthly fact records
  - product dimension map
  - supplier/warehouse dimensions
  - SKU risk baseline
  - official KPI snapshot
  - refresh/version metadata

## Lineage Summary
`data/raw/*.csv`
-> source adapter readiness checks (`source_adapter_readiness`)
-> SQL views (`daily_product_warehouse_metrics`, `supplier_performance_summary`, `product_inventory_profile`, `warehouse_service_profile`)
-> probabilistic forecast enrichment (`demand_forecast_lane_*`)
-> feature engineering and scoring (`sku_risk_table`, `supplier_risk_table`, `segment_risk_table`, `governance_priority_master`)
-> KPI diagnostics and impact outputs
-> assumption sensitivity analysis
-> policy simulation + policy optimizer + Monte Carlo stress + supplier/PO diagnostics
-> intervention tracker + anomaly alerts
-> visualizations + dashboard + validation + CI quality gates.
