# Methodology

## Analytical Objective
Build an operational decision system that quantifies where the company is simultaneously:
- under-serving demand (service failures and lost sales), and
- over-invested in inventory (excess and slow-moving stock),
then prioritizes interventions by expected operational and financial value.

Core business question:
> Is the company balancing service level and inventory efficiently, or losing sales while tying up too much working capital?

## Project Scope
Included:
- End-to-end synthetic data generation at daily grain for a multi-warehouse distribution network.
- SQL-based analytical modeling for canonical daily and entity-level views.
- Python-based scoring, impact estimation, and dashboard packaging.
- Formal pre-delivery validation (SQL + Python checks) before executive outputs.

Excluded:
- Causal ML models for driver attribution and treatment-effect estimation.
- Causal attribution claims for supplier delays and commercial outcomes.
- ERP transaction posting logic and accounting treatment.

## Data Generation Logic
Implementation: `src/data_generation.py`

Design choices:
- Reproducible seed: `RANDOM_SEED = 42`.
- Coverage window: `2024-01-01` to `2025-12-31` (731 days).
- Network scale: 120 products, 12 suppliers, 4 warehouses.
- Daily operational simulation across product-warehouse combinations.

Embedded operational realism:
- Heterogeneous supplier reliability, lead times, variability, and MOQ constraints.
- ABC and criticality segmentation.
- Category-level cost/price and shelf-life variation.
- Seasonality and promotion lift in demand.
- Warehouse-specific demand/planning profiles.
- Chronic profiles for deliberate overstock and stockout behavior.
- Purchase-order creation with late-delivery and under-receipt patterns.

Current generated volume:
- `products`: 120
- `suppliers`: 12
- `warehouses`: 4
- `inventory_snapshots`: 350,880
- `demand_history`: 350,880
- `purchase_orders`: 13,610
- `product_classification`: 120

## Analytical Workflow
1. Raw generation
- Script: `src/data_generation.py`
- Output: `/data/raw/*.csv`

2. SQL transformations
- Schema: `sql/01_schema.sql`
- Intermediate views: `sql/02_intermediate_views.sql`
- Core outputs materialized by `src/data_preparation.py`:
  - `daily_product_warehouse_metrics`
  - `supplier_performance_summary`
  - `product_inventory_profile`
  - `warehouse_service_profile`

3. Governance scoring layer
- Script: `src/scoring.py`
- Computes the canonical SKU-location, supplier, and segment risk tables from
  the daily fact and supplier execution profile.
- Produces:
  - `/data/processed/sku_risk_table.csv`
  - `/data/processed/supplier_risk_table.csv`
  - `/data/processed/segment_risk_table.csv`
  - `/data/processed/governance_priority_master.csv`

4. Data contract enforcement
- Contract spec: `configs/table_contracts.json`
- Script: `src/data_contracts.py`
- Output: `/outputs/tables/data_contract_check_results.csv`, `/outputs/tables/data_contract_table_profile.csv`
- Purpose: enforce required columns, grain uniqueness, critical nulls,
  non-negative rules, categorical domains, value ranges, and key references
  before reporting.

5. Impact analysis
- Script: `src/impact_analysis.py`
- Core outputs are curated as CSV tables in `/outputs/tables/` and consumed by the dashboard.

6. Publication artefacts
- Scripts: `src/build_charts.py`, `src/build_report.py`
- Outputs: `/outputs/graphs/*.png` and
  `/outputs/reports/service_inventory_intelligence_report.pdf`

7. Executive dashboard
- Script: `src/executive_dashboard.py`
- Output: `/index.html` (publishable GitHub Pages entry point) and dashboard fact/dim extracts in `/outputs/tables/`.

8. Pre-delivery QA
- Script: `src/pre_delivery_validation.py`
- Output: validation checks (`/outputs/tables/validation_pre_delivery_checks.csv`) and release-state matrix (`/outputs/tables/validation_release_state_matrix.csv`).

9. SQL and CI quality gates
- Scripts: `src/sql_quality_gate.py`, `src/ci_quality_gate.py`
- CI workflow: `.github/workflows/analytics-ci.yml`
- Output: SQL gate checks (`/outputs/tables/ci_sql_validation_checks.csv`),
  smoke tests for the KPI query library (`sql/03_kpi_queries.sql`), and release
  gating status with explicit states:
  - technically valid
  - analytically acceptable
  - decision-support ready
  - publish-blocked
- Governance reference: `/docs/release_governance.md`

## Key Assumptions
Operational policy assumptions:
- ABC DOS caps: A=20 days, B=30 days, C=45 days.
- Stockout persistence evaluated monthly (active month has lost sales > 0).

Scoring assumptions (`src/scoring.py`):
- Fixed policy-anchored thresholds for component normalization.
- Linear scaling for operational rates and log scaling for concentration shares.
- Governance priority is a weighted multi-objective score (service, stockout, excess, supplier, working capital, dual imbalance).

Impact assumptions (`src/impact_analysis.py`):
- Trapped WC proxy gives incremental weight of 0.50 to non-overlapping slow-moving value.
- 12M opportunity proxy uses:
  - recoverable lost margin rate = 35%
  - releasable trapped WC rate = 25%
- Lost-sales margin is annualized as a flow. Inventory and trapped working
  capital are average daily balances and are not summed or annualized through
  time.
- Supplier delay impact is an associative severity proxy, not causal attribution.

## Caveats
- Synthetic data is policy-realistic but does not represent any specific company ledger.
- Composite scores support prioritization and governance sequencing, not root-cause proof.
- Opportunity estimates are directional proxies; they should be converted into business cases with planner and procurement constraints.
- Inventory proxies depend on DOS behavior and may differ from liquidation/markdown outcomes.

## Validation Approach
Implementation: `src/pre_delivery_validation.py`

Validation dimensions:
- Row-count and grain sanity.
- Duplicate keys and critical null checks.
- Negative/impossible value checks.
- Fill-rate, stockout, and lost-sales arithmetic consistency.
- Working-capital proxy recomputation against reported outputs.
- Supplier delay factor recomputation checks.
- Aggregation reconciliation across SKU, warehouse, supplier, category, and overall totals.
- Governance score formula and tier consistency checks.
- Dashboard metric reconciliation checks.
- Output presence checks for core KPI, scoring, impact, and dashboard artifacts.

Latest status:
- Generated on each pipeline run; see `/outputs/tables/validation_pre_delivery_checks.csv` and `/outputs/tables/validation_release_state_matrix.csv` for current counts and release class.
