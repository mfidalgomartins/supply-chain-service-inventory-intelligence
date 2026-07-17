# Data Model

## Overview
The project uses a layered data model:
1. Canonical raw operational tables (`/data/raw/`)
2. Governed Parquet persistence (`/data/lake/`)
3. SQL intermediate analytical tables (`/data/processed/`)
4. Scored and advanced decision outputs (`/data/processed/` and `/outputs/tables/`)
5. Dashboard-serving fact/dim extracts (`/outputs/tables/`)

All CSV layers are deterministic pipeline outputs and are intentionally excluded
from version control.

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
| intervention_assignments | `/data/raw/intervention_assignments.csv` | 1 row per experiment-unit assignment | `experiment_id + unit_id` | `product_id -> products`, `warehouse_id -> warehouses`, `supplier_id -> suppliers` |
| network_nodes | `/data/raw/network_nodes.csv` | 1 row per physical node | `node_id` | None |
| network_lanes | `/data/raw/network_lanes.csv` | 1 row per directed lane | `lane_id` | source/destination node IDs -> `network_nodes` |
| product_sources | `/data/raw/product_sources.csv` | 1 row per eligible product-supplier pair | `product_id + supplier_id` | `product_id -> products`, `supplier_id -> suppliers` |

Latest run row counts:
- products: 120
- suppliers: 12
- warehouses: 4
- inventory_snapshots: 350,880
- demand_history: 350,880
- purchase_orders: 13,446
- product_classification: 120
- intervention_assignments: 648
- network_nodes: 16
- network_lanes: 15
- product_sources: 240

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
- `inventory_snapshots.on_order_units` is the sum of ordered quantities on open
  POs. Eventual under-receipt is not visible to the replenishment decision.
- Raw extracts load through `sql/01_schema.sql`; data contracts then add
  cross-table reference, domain, range, null, and fingerprint checks.

## Intermediate Analytical Tables
Built via `sql/02_intermediate_views.sql` and materialized by `src/data_preparation.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| daily_product_warehouse_metrics | `/data/processed/daily_product_warehouse_metrics.csv` | `date + warehouse_id + product_id` | Canonical daily fact combining demand, inventory, commercial value, and policy context |
| supplier_performance_summary | `/data/processed/supplier_performance_summary.csv` | `supplier_id` | Supplier execution profile (OTD, delay, variability, underfill) |
| product_inventory_profile | `/data/processed/product_inventory_profile.csv` | `product_id` | SKU-level service/inventory behavior and working-capital proxy |
| warehouse_service_profile | `/data/processed/warehouse_service_profile.csv` | `warehouse_id` | Warehouse-level service performance and capacity pressure profile |

## Scoring and Priority Outputs
Built via `src/scoring.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| sku_risk_table | `/data/processed/sku_risk_table.csv` | `product_id + warehouse_id + supplier_id + category + region` | Primary intervention queue for SKU-location actions |
| supplier_risk_table | `/data/processed/supplier_risk_table.csv` | `supplier_id` | Supplier governance ranking |
| segment_risk_table | `/data/processed/segment_risk_table.csv` | `category + region` | Segment-level risk concentration and governance |
| governance_priority_master | `/data/processed/governance_priority_master.csv` | mixed entity list | Unified governance queue across SKU, supplier, and segment entities |

## Impact Outputs
Built via `src/impact_analysis.py`. Curated impact summaries are stored in `/outputs/tables/`:
- `impact_overall_summary.csv` (portfolio-level exposure snapshot)
- `impact_opportunity_priority.csv` (top business-value priorities)

## Advanced Decision Outputs

| Table | Grain | Purpose |
|---|---|---|
| `policy_backtest_folds` | `product_id + warehouse_id + fold_start + policy_id` | Leakage-safe observed-demand policy evaluation |
| `policy_backtest_recommendations` | `product_id + warehouse_id` | Evidence-ranked replenishment policy |
| `policy_backtest_abc_summary` | `abc_class + policy_id` | Portfolio policy comparison |
| `monte_carlo_policy_scenarios` | `product_id + warehouse_id + scenario_id` | Probabilistic service-capital trade-off |
| `monte_carlo_recommendations` | `product_id + warehouse_id` | Constraint-aware efficient-frontier selection |
| `monte_carlo_portfolio_summary` | `metric` | Selected scenario portfolio summary |
| `action_register` | `action_id` | Intervention lifecycle, score migration, and benefit proxy |
| `action_kpi_timeseries` | `action_id + month + period` | Pre/post operating evidence; mid-month changes retain both periods |
| `causal_effect_estimates` | `experiment_id` | RCT/DiD estimand, inference, economic point estimate, and evidence status |
| `causal_diagnostics` | `experiment_id + diagnostic` | Design, support, balance, randomization, pre-trend, and placebo checks |
| `causal_cohort_timeseries` | `experiment_id + date + treatment_flag` | Weighted cohort outcome evidence by period |
| `network_optimization_plan` | `product_id + warehouse_id` | Service-constrained node plan and flow reconciliation |
| `network_flow_plan` | `product_id + lane_id` | Sourcing/transfer decisions and cost components |
| `network_constraint_utilization` | `constraint_type + constraint_id` | Capacity use, slack, and binding state |
| `network_optimization_summary` | `solver_status` | Objective, solver diagnostics, system service, cost, and balance |

## Ingestion and Storage Lineage

`source_readiness_checks` records schema, drift, key, and freshness decisions;
`source_schema_registry` retains the latest accepted canonical fingerprint.
`ingestion_manifest` records adapter, row count, and source/canonical hashes for
each raw table. `storage_manifest` records the Parquet path, refresh mode,
compression, business key, watermark bounds, row counts, and both source and
stored hashes.

The Parquet lake mirrors raw, processed, and analytics layers. Dated facts use
key-based incremental upserts; compact dimensions and summaries use
deterministic full replacement.

After all release gates pass, `data_catalog` profiles every contracted asset
with content/schema hashes and business watermarks. `data_lineage` contains the
validated parent-child graph, and `object_publication_manifest` maps each logical
asset to an immutable content-addressed object. None is self-referential.

## SQL Quality Gate Output
Built via `src/sql_quality_gate.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| ci_sql_validation_checks | `/outputs/tables/ci_sql_validation_checks.csv` | `check_name` | SQL quality-gate results used by CI and release controls |

## Release Governance Output
Built via `src/pre_delivery_validation.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| validation_pre_delivery_checks | `/outputs/tables/validation_pre_delivery_checks.csv` | `check_name` | Analytical reconciliation checks across service, inventory, impact, scoring, and dashboard layers |
| validation_release_state_matrix | `/outputs/tables/validation_release_state_matrix.csv` | `state_name` | Release-state governance matrix: technically_valid, analytically_acceptable, decision_support_ready, publish_allowed |

## Data Contract Outputs
Built via `src/data_contracts.py` using `configs/table_contracts.json`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| data_contract_check_results | `/outputs/tables/data_contract_check_results.csv` | `table_name + check_name` | Contract-level pass/fail checks for required columns, uniqueness, nulls, numeric ranges, categorical domains, and key references |
| data_contract_table_profile | `/outputs/tables/data_contract_table_profile.csv` | `table_name` | Row/column counts, file sizes, and SHA256 signatures for traceability |

## Dashboard Data Model
Built via `src/executive_dashboard.py`.

Dashboard fact/dim exports:
- `/outputs/tables/dashboard_monthly_sku_fact.csv`
  - Grain: `month + region + warehouse_id + product_id + supplier_id + category + abc_class`
  - Contains monthly demand, fulfillment, and lost-sales flows plus average
    daily inventory, DOS, excess, slow-moving, and trapped-WC balances.
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

Runtime HTML payload:
- `/index.html` embeds the dashboard data as JSON and loads the pinned Plotly
  JavaScript bundle from a CDN. JSON is escaped for safe embedding in a script
  element. The payload includes:
  - monthly fact records
  - product dimension map
  - supplier/warehouse dimensions
  - SKU risk baseline
  - official KPI snapshot
  - refresh/version metadata

## Lineage Summary
configured ingestion -> source readiness -> `data/raw/*.csv` + `data/lake/raw/*.parquet`
-> SQL views (`daily_product_warehouse_metrics`, `supplier_performance_summary`, `product_inventory_profile`, `warehouse_service_profile`)
-> policy scoring (`sku_risk_table`, `supplier_risk_table`, `segment_risk_table`, `governance_priority_master`)
-> impact + backtest + simulation + action + causal + network-optimization outputs
-> `data/lake/processed/*.parquet` + `data/lake/analytics/*.parquet`
-> dashboard + validation + CI quality gates
-> catalog + validated lineage + immutable object manifest
-> atomic `latest` promotion for decision-support review.
