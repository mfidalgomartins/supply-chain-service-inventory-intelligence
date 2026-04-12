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
Built via `src/impact_analysis.py`. Curated impact summaries are stored in `/outputs/tables/`:
- `impact_overall_summary.csv` (portfolio-level exposure snapshot)
- `impact_opportunity_priority.csv` (top business-value priorities)

No additional diagnostic layers are generated beyond the core impact summaries in this release.

## SQL Quality Gate Output
Built via `src/sql_quality_gate.py`.

| Table | File | Grain | Purpose |
|---|---|---|---|
| ci_sql_validation_checks | `/outputs/tables/ci_sql_validation_checks.csv` | `check_name` | SQL quality-gate results used by CI and release controls |
| validation_release_state_matrix | `/outputs/tables/validation_release_state_matrix.csv` | `state_name` | Explicit release-state governance matrix (technical, analytical, decision-support, screening, committee, publish gate) |

## Data Contract Outputs
Built via `src/data_contracts.py` using `configs/table_contracts.json`.

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
-> SQL views (`daily_product_warehouse_metrics`, `supplier_performance_summary`, `product_inventory_profile`, `warehouse_service_profile`)
-> feature engineering and scoring (`sku_risk_table`, `supplier_risk_table`, `segment_risk_table`, `governance_priority_master`)
-> KPI diagnostics + impact outputs
-> visualizations + dashboard + validation + CI quality gates
-> curated impact summaries and governance outputs for decision-support review.
