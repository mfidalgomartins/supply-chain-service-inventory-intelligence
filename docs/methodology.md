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
- Configurable synthetic or ERP/WMS ingestion at daily grain for a multi-warehouse distribution network.
- SQL-based analytical modeling for canonical daily and entity-level views.
- Python-based scoring, impact estimation, temporal backtesting, Monte Carlo
  simulation, registered RCT/DiD evaluation, multi-echelon optimization, and action measurement.
- Formal pre-delivery validation (SQL + Python checks) before executive outputs.

Excluded:
- Individualized uplift/causal-ML models and unregistered treatment-effect searches.
- Causal attribution for action-register pre/post movement, supplier associations,
  or commercial outcomes outside the registered RCT/DiD designs.
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
- Open-order inventory positions use ordered quantities; realized supplier
  underfill affects stock only when the receipt arrives.

Current generated volume:
- `products`: 120
- `suppliers`: 12
- `warehouses`: 4
- `inventory_snapshots`: 350,880
- `demand_history`: 350,880
- `purchase_orders`: 13,446
- `product_classification`: 120
- `intervention_assignments`: 648
- `network_nodes`: 16
- `network_lanes`: 15
- `product_sources`: 240

## Analytical Workflow
1. Ingestion and governed storage
- Scripts: `src/ingestion.py`, `src/storage.py`
- Output: `/data/raw/*.csv`
- Adapter choices: deterministic synthetic, canonical directory, or split ERP/WMS exports.
- External extracts pass required-schema, drift, business-key, freshness, and
  full relationship contracts before canonical write; raw, processed, and
  analytical contracts are mirrored to compressed Parquet.

2. SQL transformations
- Schema: `sql/01_schema.sql`
- Intermediate views: `sql/02_intermediate_views.sql`
- Raw CSVs are loaded into schema-defined DuckDB tables before transformation,
  enforcing types, primary keys, and operational `CHECK` constraints.
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

6. Policy validation and probabilistic optimization
- Scripts: `src/backtesting.py`, `src/monte_carlo.py`
- Walk-forward folds estimate policy inputs only from dates before evaluation.
- Training demand initializes simulated policy state before backtest KPIs begin.
- Monte Carlo resamples seven-day empirical demand blocks and supplier lead
  times after a warm-up, then selects non-dominated service-capital scenarios
  under explicit constraints.

7. Action tracking
- Script: `src/action_tracking.py`
- Implemented and closed actions receive equal-length pre/post measurement;
  incomplete post windows remain pending and other lifecycle states remain unmeasured.
- Benefit and score migration are observational, not causal attribution.

8. Registered intervention evaluation
- Script: `src/causal_evaluation.py`
- RCT estimates use assigned-unit changes, Welch uncertainty, and stratified
  randomization inference.
- DiD estimates require common windows, support, pre-trend, baseline-balance,
  and pre-period placebo diagnostics.
- Evidence and attribution labels are governed separately from economic point estimates.

9. Multi-echelon planning
- Script: `src/network_optimization.py`
- A sparse integer MILP allocates eligible supplier and inter-warehouse flows
  subject to MOQ, source, lane, storage, flow-balance, and service constraints.
- Only an independently reconciled optimal solution is materialized.

10. Publication artefacts
- Scripts: `src/build_charts.py`, `src/build_report.py`
- Outputs: `/outputs/graphs/*.png` and
  `/outputs/reports/service_inventory_intelligence_report.pdf`

11. Executive dashboard
- Script: `src/executive_dashboard.py`
- Output: `/index.html` (publishable GitHub Pages entry point) and dashboard fact/dim extracts in `/outputs/tables/`.

12. Pre-delivery QA
- Script: `src/pre_delivery_validation.py`
- Output: validation checks (`/outputs/tables/validation_pre_delivery_checks.csv`) and release-state matrix (`/outputs/tables/validation_release_state_matrix.csv`).

13. SQL, CI, lineage, and publication gates
- Scripts: `src/sql_quality_gate.py`, `src/ci_quality_gate.py`
- Orchestration: `src/orchestration.py`, `src/data_catalog.py`, `src/object_store.py`
- CI workflow: `.github/workflows/analytics-ci.yml`
- Output: SQL gate checks (`/outputs/tables/ci_sql_validation_checks.csv`),
  smoke tests for the KPI query library (`sql/03_kpi_queries.sql`), and release
  gating status with explicit states:
  - technically valid
  - analytically acceptable
  - decision-support ready
  - publish-blocked
- Successful runs then validate the catalog/lineage graph, publish immutable
  objects, and promote the latest pointer as the final operation.
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
- Supplier delay impact is an associative severity proxy; it does not attribute causation.

Policy assumptions (`src/backtesting.py`, `src/monte_carlo.py`):
- Reorder point combines expected lead-time demand and safety stock.
- Order-up-to level adds configured cycle-stock coverage.
- Backtests replay observed evaluation demand as a counterfactual, not a forecast.
- Monte Carlo demand uses circular moving blocks to retain short-run dependence;
  uncertainty remains limited to empirical demand and lead-time histories.
- Economic cost combines lost-margin, holding-cost, and ordering-cost proxies.

Action assumptions (`src/action_tracking.py`):
- Pre/post windows have equal length and complete daily coverage.
- Implemented actions remain pending until both windows are complete.
- Lost-margin recovery and inventory release may be negative.
- Concurrent demand, mix, seasonality, or operational changes can explain movement.

Causal assumptions (`src/causal_evaluation.py`):
- Assignment occurs before the intervention and is analyzed at the assigned unit.
- RCT inference preserves treatment counts inside the registered strata.
- DiD interpretation depends on parallel pre-trends, placebo, balance, common
  support, and no unmeasured time-varying differential shock.
- An inconclusive estimate remains inconclusive even when its point estimate is favorable.

Network assumptions (`src/network_optimization.py`):
- Horizon demand is derived from lagged observed history.
- Supplier/product eligibility, costs, MOQ, and horizon capacities are fixed inputs.
- Shortage penalties are decision weights, not audited lost-sales bookings.
- The deterministic horizon does not represent every disruption scenario; Monte
  Carlo remains the separate uncertainty layer.

## Caveats
- Synthetic data is policy-realistic but does not represent any specific company ledger.
- Composite scores support prioritization and governance sequencing; they do not prove root cause.
- Opportunity estimates are directional proxies; they should be converted into business cases with planner and procurement constraints.
- Inventory proxies depend on DOS behavior and may differ from liquidation/markdown outcomes.
- Synthetic interventions demonstrate identification behavior; live causal use
  requires preregistration, stable assignment, and operational compliance monitoring.
- The network plan is decision support and does not post purchase or transfer orders.

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
- PNG integrity/dimension checks and PDF page/section checks before release.
- Backtest time-separation and inventory-conservation checks.
- Monte Carlo probability, conservation, service-confidence, and frontier checks.
- Action-benefit reconciliation and attribution-label checks.
- Source freshness, schema-drift, registry, and business-key readiness checks.
- Causal design/claim compatibility and supported-claim diagnostic checks.
- MILP solver, optimality-gap, service, capacity, and flow-balance checks.
- Parquet/source hash, business-key, contract-coverage, and manifest checks.
- Catalog producer/parent/cycle validation plus content-addressed object hashes.

Latest status:
- Generated on each pipeline run; see `/outputs/tables/validation_pre_delivery_checks.csv` and `/outputs/tables/validation_release_state_matrix.csv` for current counts and release class.
