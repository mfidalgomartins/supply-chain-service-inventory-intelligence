# Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

## 1) Project Overview
This project is an end-to-end supply chain analytics system designed to emulate serious internal decision support for Operations and Finance leadership.

It combines:
- **SQL** for relational modeling, KPI logic, and validation checks
- **Python** for synthetic data generation, feature engineering, scoring, impact estimation, and QA
- a fully self-contained **HTML executive dashboard** for leadership review

The work is structured to answer not only _what happened_, but also _where value is leaking_, _why_, and _what should be acted on first_.

## 2) Business Problem
Distribution businesses often fail in both directions at once:
- revenue loss from stockouts and missed demand
- cash lock-up in excess and slow-moving inventory

Without integrated visibility, teams optimize locally (service, purchasing, finance, warehousing) and miss portfolio-level trade-offs.

## 3) Core Business Question
> Is the company balancing service level and inventory efficiently, or is it simultaneously losing sales through stockouts while tying up too much working capital in excess and slow-moving stock?

## 4) Why This Matters Operationally and Financially
Operationally:
- lower service reliability weakens customer trust and on-shelf availability
- unstable supplier performance propagates into replenishment volatility
- warehouse performance diverges by region and category

Financially:
- stockouts create immediate top-line and margin leakage
- excess inventory increases carrying cost and markdown risk
- inefficient inventory positioning traps working capital that could be redeployed

## 5) Project Architecture
The system is organized in modular layers:
1. **Data generation layer** (`src/data_generation.py`) produces multi-table synthetic operations data.
2. **Source adapter layer** (`src/source_adapter.py`) introduces real-source readiness checks, schema templates, and refresh-manifest traceability.
3. **SQL modeling layer** (`sql/`) defines schema, intermediate views, KPI queries, and validation logic.
4. **Analytical modeling layer** (`src/data_preparation.py`, `src/feature_engineering.py`) builds processed decision tables.
5. **Data-contract layer** (`configs/table_contracts.json`, `src/data_contracts.py`) enforces schema/grain/null/non-negative contracts before downstream analytics.
6. **Probabilistic forecast layer** (`src/probabilistic_forecast.py`) replaces static lane demand assumptions with forecast distributions.
7. **Scoring layer** (`src/scoring.py`) computes interpretable risk and governance priority scores.
8. **Impact layer** (`src/impact_analysis.py`) estimates business-value exposure and opportunity proxies.
9. **Sensitivity layer** (`src/sensitivity_analysis.py`) stress-tests business-value outputs under assumption ranges.
10. **Visualization layer** (`src/visualization.py`) generates publication-quality PNG charts.
11. **Policy simulation layer** (`src/policy_simulation.py`) builds service-vs-working-capital policy frontiers under alternate reorder assumptions.
12. **Policy optimizer layer** (`src/policy_optimizer.py`) allocates limited inventory capital to lane-level policy upgrades.
13. **Monte Carlo stress layer** (`src/monte_carlo_stress.py`) stress-tests lane service under demand and lead-time uncertainty.
14. **Supplier lane + PO cohort diagnostics** (`src/supplier_lane_diagnostics.py`, `src/po_cohort_diagnostics.py`) quantifies persistent execution risk by lane and cohort.
15. **Intervention tracker layer** (`src/intervention_tracker.py`) converts risk scores into owner-based action registers with due dates.
16. **Anomaly alert layer** (`src/anomaly_alerts.py`) flags sudden warehouse and supplier instability spikes.
17. **Executive dashboard layer** (`src/executive_dashboard.py`) creates a single self-contained HTML dashboard with version stamp and dataset fingerprint.
18. **Pre-delivery + CI quality-gate layer** (`src/pre_delivery_validation.py`, `src/sql_quality_gate.py`, `src/ci_quality_gate.py`) enforces SQL/Python data-quality and release checks.

## 6) Dataset Design
Synthetic data models a multi-warehouse distribution network with daily granularity and realistic operational variation.

Core raw tables:
- `products`
- `suppliers`
- `warehouses`
- `inventory_snapshots`
- `demand_history`
- `purchase_orders`
- `product_classification`

Processed analytical tables:
- `daily_product_warehouse_metrics`
- `supplier_performance_summary`
- `product_inventory_profile`
- `warehouse_service_profile`
- `sku_risk_table`
- `supplier_risk_table`
- `segment_risk_table`

## 7) Methodology
1. Generate reproducible synthetic operational data with embedded trade-offs and risk patterns.
2. Build warehouse-style SQL transformations for entity-level and daily metrics.
3. Engineer interpretable analytics features in Python.
4. Compute KPI and diagnostic views by region, warehouse, category, supplier, and SKU.
5. Apply transparent governance scoring (no black-box ML).
6. Estimate financial impact using explicit proxy formulas and assumptions.
7. Validate logic, reconciliation, and narrative risk before final delivery.

## 8) KPI Framework
Primary KPI families:
- service health: fill rate, stockout rate, lost sales value
- inventory efficiency: days of supply distribution, excess exposure, slow-moving exposure
- working-capital stress: inventory concentration and trapped-capital proxies
- supplier execution: on-time delivery, delay, lead-time variability
- trade-off diagnostics: service vs inventory policy imbalance zones

## 9) Scoring Framework
Implemented in `src/scoring.py` and documented in [`docs/scoring_framework.md`](docs/scoring_framework.md).

Required component scores (0-100):
- `service_risk_score`
- `stockout_risk_score`
- `excess_inventory_score`
- `supplier_risk_score`
- `working_capital_risk_score`
- `governance_priority_score`

Additional decision fields:
- `risk_tier` (`Low` / `Medium` / `High` / `Critical`)
- `main_risk_driver`
- `recommended_action`

Design principle: interpretable weighted logic aligned to operational policy thresholds, not statistical opacity.

## 10) Dashboard Overview
A single self-contained executive dashboard is generated at:
- `outputs/dashboard/index.html`

Audience-ready sections include:
- executive header + filters + methodology panel
- financial assumption sliders for recoverable margin, releasable working capital, and slow-moving incremental weighting
- KPI scorecards
- automatic leadership callouts
- trend analysis (service, stockout, lost sales, inventory)
- comparative diagnostics (warehouse/category/region/supplier)
- trade-off scatter/quadrant views
- risk prioritization panels
- sortable drill-down table with actions
- narrative panel for leadership decisions

## 11) Key Findings (Latest Run)
From the latest generated outputs:
- overall fill rate: **92.27%**
- stockout rate: **7.73%**
- lost sales exposure: **EUR 34.78M**
- trapped working-capital proxy: **EUR 321.37M**
- estimated 12M opportunity proxy (margin recovery + WC release): **EUR 42.79M**
- most pressured warehouse: **WH-LYON**
- highest supplier risk exposure: **SUP-002**
- largest loss-concentration category: **Health**
- policy optimizer best tested budget: **+15% capital**, **+4.53pp service uplift**, **EUR 10.38M lost-sales recovery**
- intervention register backlog: **480 items** with **43 open high-priority interventions**
- anomaly watchlist: **428 alerts** with top critical spike at **SUP-011 delay-duration**
- sensitivity range (assumption grid): opportunity spans approx. **EUR 20.45M to EUR 64.35M**

These values are scenario outputs from synthetic data and are intended for methodological demonstration and governance prioritization logic.

## 12) Business Recommendations
1. Prioritize top governance-score SKU-warehouse combinations for immediate replenishment policy intervention.
2. Launch supplier corrective plans on high-risk suppliers with weak OTD and high downstream stockout linkage.
3. Execute category-level working-capital release actions where excess and slow-moving exposure are concentrated.
4. Manage service-vs-capital trade-offs through explicit policy bands (target fill rate + DOS guardrails).
5. Operationalize a weekly governance cadence using score movement and intervention closure metrics.

## 13) Validation and Caveats
Formal pre-delivery validation is documented in:
- [`docs/validation_report.md`](docs/validation_report.md)

Current QA status (latest run):
- generated by `src/pre_delivery_validation.py` and `src/ci_quality_gate.py`
- includes SQL/Python integrity checks, scoring stability checks, dashboard governance checks, report-to-metric reconciliation, and release-state classification
- outputs:
  - `outputs/tables/validation_pre_delivery_checks.csv`
  - `outputs/tables/validation_release_state_matrix.csv`
  - `outputs/reports/release_readiness.md`

Important caveats:
- financial impact outputs are **proxy estimates**, not accounting-recognized P&L values
- supplier delay impact is associative, not causal attribution
- assumption choices (e.g., recoverable margin %, releasable WC %) materially affect opportunity estimates

## 14) How to Run the Project
```bash
cd /Users/miguelfidalgo/Documents/supply-chain-service-level-inventory-intelligence-system
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

# 1) Generate synthetic raw data
.venv/bin/python src/data_generation.py

# 2) Build processed analytical tables (SQL + Python)
.venv/bin/python src/data_preparation.py
.venv/bin/python src/feature_engineering.py

# 2b) Enforce data contracts
.venv/bin/python src/data_contracts.py

# 3) Build scoring outputs
.venv/bin/python src/scoring.py

# 4) Run KPI + diagnostic analysis
.venv/bin/python src/kpi_diagnostic_analysis.py

# 5) Run impact analysis
.venv/bin/python src/impact_analysis.py

# 6) Run assumption sensitivity analysis
.venv/bin/python src/sensitivity_analysis.py

# 7) Generate visualization suite
.venv/bin/python src/visualization.py

# 8) Run policy simulation and stress diagnostics
.venv/bin/python src/probabilistic_forecast.py
.venv/bin/python src/policy_simulation.py
.venv/bin/python src/policy_optimizer.py
.venv/bin/python src/monte_carlo_stress.py
.venv/bin/python src/supplier_lane_diagnostics.py
.venv/bin/python src/po_cohort_diagnostics.py
.venv/bin/python src/intervention_tracker.py
.venv/bin/python src/anomaly_alerts.py

# 9) Build executive dashboard
.venv/bin/python src/executive_dashboard.py

# 10) Run source adapter readiness + formal quality gates
.venv/bin/python src/source_adapter.py
.venv/bin/python src/sql_quality_gate.py
.venv/bin/python src/pre_delivery_validation.py
.venv/bin/python src/ci_quality_gate.py

# 11) Optional tests
.venv/bin/python -m pytest -q

# One-command full run
.venv/bin/python src/run_pipeline.py
```

## 15) Repository Structure
```text
supply-chain-service-inventory-intelligence/
├── README.md
├── requirements.txt
├── .gitignore
├── src/
├── data/
│   ├── raw/
│   └── processed/
├── sql/
├── docs/
├── tests/
├── outputs/
│   ├── charts/
│   ├── dashboard/
│   ├── reports/
│   └── tables/
├── notebooks/
│   └── supply_chain_service_level_inventory_intelligence.ipynb
├── configs/
│   └── table_contracts.json
└── .github/workflows/
    └── analytics-ci.yml
```

## 16) Future Improvements
1. Add closed-loop intervention outcomes (before/after score migration and realized P&L impact tracking).
2. Extend policy optimizer to multi-period capital budgets with seasonal constraints and MOQ/transport cost penalties.
3. Add probabilistic forecast model benchmarking (ETS vs Croston vs baseline EWMA) with backtest diagnostics.
4. Introduce shipment-level transportation and lane-cost data for end-to-end service-cost optimization.
5. Wire source adapter to cloud object storage + orchestration metadata (e.g., dbt/Airflow run status integration).
6. Add near-real-time alert routing integration (Slack/Teams webhook and ticket auto-creation).

## 17) Repository Governance Notes
- `src/run_pipeline.py` is the authoritative execution path for reproducible builds.
- `outputs/dashboard/index.html` is the single official dashboard artifact for release review.
- `docs/metric_dictionary.md` is the authoritative metric source; `docs/metric_definitions.md` is a legacy pointer only.
- Legacy wrappers are retained only for backward compatibility and kept minimal to avoid logic divergence.
- Legacy entry-point names (`src/analyze.py`, `src/build_dashboard.py`, `src/generate_data.py`, `src/validate.py`, `src/run_sql_analysis.py`) are now compatibility wrappers that route to authoritative modules to prevent logic divergence.

---
This project is intentionally built as an internal-grade analytics product, not a notebook-only demo, to demonstrate senior-level analytics engineering, operational reasoning, and executive communication.
