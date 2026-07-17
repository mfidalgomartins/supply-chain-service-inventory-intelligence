# Advanced Decision Intelligence

## Purpose

The advanced layer converts the governed risk queue into testable inventory
policies, probabilistic service-capital decisions, registered intervention
evidence, constrained network plans, and an auditable action register. Evidence
labels are determined by design and diagnostics, not by whether an estimate is favorable.

Configuration is versioned in `configs/pipeline.json`. Action events are
maintained in `configs/action_events.json`.

## Policy Backtesting

`src/backtesting.py` runs leakage-safe walk-forward evaluation for every
SKU-location and configured policy.

- Training uses only dates before each evaluation fold.
- Reorder point is expected lead-time demand plus safety stock.
- Order-up-to level adds the configured cycle-stock days.
- Evaluation uses the observed future demand sequence, not future demand in
  policy estimation.
- The training sequence initializes inventory and outstanding-order state; this
  warm-up is excluded from evaluation KPIs.
- Selection first rewards service-target attainment, then fold wins, economic
  cost, and inventory value.
- Every simulation enforces the inventory-flow identity: starting stock plus
  receipts minus fulfilled units equals ending stock.

Outputs:

| Table | Grain | Role |
|---|---|---|
| `policy_backtest_folds.csv` | SKU-location × fold × policy | Full observed-versus-counterfactual evidence |
| `policy_backtest_recommendations.csv` | SKU-location | Selected policy and evidence status |
| `policy_backtest_abc_summary.csv` | ABC class × policy | Portfolio comparison |

Backtest fill rate and inventory are counterfactual simulation results. The
`actual_*` columns are observed comparators from the same evaluation window.

## Monte Carlo Optimization

`src/monte_carlo.py` stress-tests the highest-priority SKU-locations using
moving-block resampling of empirical demand and realized supplier lead-time histories.

- Random streams are deterministic by base seed, entity, and scenario.
- Seven-day circular demand blocks preserve short-run serial dependence that
  independent daily sampling would discard.
- A warm-up period initializes inventory state and is excluded from reported
  KPIs.
- Each scenario reports expected fill rate, P10/P50/P90 service, probability of
  meeting target, P90 inventory, P95 lost margin, and total cost proxy.
- Dominated service-capital policies are removed from the efficient frontier.
- The selector minimizes expected total cost among frontier policies that meet
  service-confidence and inventory-capital constraints. Any relaxed constraint
  is explicit in `selection_reason`.
- Flow conservation is checked in every simulation run.

Outputs:

| Table | Grain | Role |
|---|---|---|
| `monte_carlo_policy_scenarios.csv` | SKU-location × scenario | Full uncertainty distribution summary |
| `monte_carlo_recommendations.csv` | SKU-location | Constraint-aware frontier selection |
| `monte_carlo_portfolio_summary.csv` | Metric | Selected portfolio totals and rates |

These are scenario estimates, not forecasts or guaranteed outcomes.

## Action Tracking

`src/action_tracking.py` joins intervention events to governed operating data.
Implemented or closed actions receive equal-length pre/post measurement windows,
score and tier migration, KPI movement, and a realized-benefit proxy. Planned,
in-progress, and cancelled actions remain in the register but are marked
`not_eligible_status` and do not receive benefit values.

Implemented actions whose full post window is not yet observable remain in the
register as `measurement_pending`. For mid-month implementation dates, the
monthly evidence table emits separate pre and post rows for that month.
Targets are limited to governed service, lost-sales, lost-margin, and inventory
metrics with explicit improvement direction.

Supplier execution inputs are recomputed from receipts observed inside each
window, so pre-period scores cannot use post-period purchase-order outcomes.
When a supplier has no receipts in a window, documented neutral defaults are
used instead of treating missing execution evidence as perfect performance.

The benefit proxy reconciles as:

```text
observed_total_benefit_proxy
  = observed_lost_margin_recovery_proxy
  + observed_inventory_release_proxy
```

Negative values are retained. Pre/post changes are explicitly labelled
`observational_not_causal`; they may reflect seasonality, mix, demand, or other
concurrent changes.

## Registered Causal Evaluation

`src/causal_evaluation.py` evaluates completed experiments at their assigned
intervention-unit grain. Assignment must be unique, precede the intervention,
and meet configured treatment/control support.

The registered designs are deliberately narrow:

- The stratified randomized pilot estimates the average treatment effect on the
  treated from unit-level post-minus-pre changes. It reports Welch uncertainty
  and a deterministic, within-stratum randomization p-value.
- The supplier-recovery DiD compares common-window changes for treated and
  control cohorts. Differential pre-trends, a pre-period placebo, baseline
  balance, and sample support are explicit diagnostics.

`causal_supported` is available only to a valid randomized design with detectable
evidence and clean diagnostics. DiD can reach `quasi_causal_supported`. Failed
identification produces `insufficient_evidence` and `not_causal`; a valid design
with an imprecise effect remains explicitly inconclusive. Economic value is a
separate point estimate and inherits the evidence status—it is not audited P&L.

| Table | Grain | Role |
|---|---|---|
| `causal_effect_estimates.csv` | experiment | Estimand, effect, uncertainty, value, and evidence label |
| `causal_diagnostics.csv` | experiment × diagnostic | Assignment, support, balance, pre-trend, placebo, and inference gates |
| `causal_cohort_timeseries.csv` | experiment × date × treatment group | Weighted pre/post outcome evidence |

## Multi-Echelon Network Optimization

`src/network_optimization.py` solves a deterministic, integer multi-echelon
planning problem with SciPy's HiGHS-backed MILP solver. Lagged observed demand
sets the planning requirement; no future operating data enters the model.

The objective includes procurement, lane transport, ending-inventory holding,
shortage, and order-activation costs. Constraints enforce:

- product-source eligibility and source capacity;
- minimum order quantities linked to binary order decisions;
- enabled lanes and horizon-adjusted lane capacity;
- warehouse storage capacity and node-level flow conservation;
- product-location service targets and non-negative integer physical flows.

The runner independently reconciles the returned solution. Non-optimal,
infeasible, unbalanced, over-capacity, or service-violating solutions fail before
materialization.

| Table | Grain | Role |
|---|---|---|
| `network_optimization_plan.csv` | product × warehouse | Demand, inbound/outbound flow, service, ending inventory, and balance |
| `network_flow_plan.csv` | product × lane | Sourcing and transfer quantities with cost components |
| `network_constraint_utilization.csv` | constraint × entity | Capacity, use, slack, utilization, and binding status |
| `network_optimization_summary.csv` | run | Solver status, gap, objective, system service, cost, and reconciliation |

## Real-Data Adapters

`src/ingestion.py` supports three adapters:

- `synthetic`: deterministic portfolio dataset.
- `directory`: 11 canonical tables from one CSV or Parquet directory.
- `erp_wms`: ERP tables (`products`, `suppliers`, `purchase_orders`,
  `product_classification`, intervention assignments, nodes, lanes, and product
  sources) and WMS tables (`warehouses`, `inventory_snapshots`, `demand_history`)
  from separate directories.

Example configuration:

```json
{
  "adapter": {
    "type": "erp_wms",
    "erp_path": "/exports/erp",
    "wms_path": "/exports/wms",
    "source_path": null,
    "file_format": "parquet",
    "column_mapping": {
      "products": {"sku_code": "product_id"},
      "demand_history": {"demand_date": "date"}
    }
  }
}
```

Before a canonical file is replaced, `src/source_readiness.py` validates required
schema, business keys, source freshness, and schema drift against the last
successful registry. Missing/type-changing columns and blocker-level staleness
fail; additive drift is recorded without silently changing the canonical model.
The registry is promoted only on a successful readiness decision.

The adapter then validates full table contracts and cross-table references.
Environment overrides are available for deployment:
`PIPELINE_CONFIG`, `INGESTION_ADAPTER`, `SOURCE_DATA_PATH`, `ERP_EXPORT_PATH`,
`WMS_EXPORT_PATH`, and `PIPELINE_AS_OF_DATE`.

## Parquet and Incremental Processing

`src/storage.py` mirrors governed raw, processed, and analytical CSV contracts
to a Zstandard-compressed Parquet lake under `data/lake/`.

- Large dated facts use key-based incremental upserts.
- Dimensions and compact analytical tables use deterministic full replacement.
- A write is skipped only when both the source hash and existing Parquet hash
  match the prior manifest; an untrusted file is rebuilt from its source.
- Null or duplicate business keys fail before the manifest is published.
- Schema drift and duplicate business keys fail the run.
- `storage_manifest.csv` records source and Parquet hashes, row counts, refresh
  mode, compression, keys, and watermark bounds.

CSV remains the portable contract and publication boundary. Parquet is the
efficient persisted processing layer; both are validated against current source
hashes before release.

## Orchestration and Governed Publication

`src/orchestration.py` validates the stage graph, executes each stage in an
isolated Python process, verifies stage-owned outputs, and records structured
events. Run identifiers are derived from governed configuration and source bytes
and are independent of checkout location. Deterministic failures are never
retried. Explicit transient stage or object-store failures receive bounded
exponential backoff.

After all release gates pass, `src/data_catalog.py` profiles each contracted
asset, validates its lineage graph, and publishes immutable content-addressed
objects through `src/object_store.py`. `LocalObjectStore` is the zero-credential
default. `S3ObjectStore` supplies checksums, SHA-256 metadata, conditional writes,
and server-side encryption without serializing credentials. `pointers/latest.json`
is the final write; failed runs cannot promote it.

Operational outputs:

| File | Purpose |
|---|---|
| `data_catalog.csv` | Producer, path, row/column count, schema/content hashes, watermark, and run ID |
| `data_lineage.csv` | Validated parent-child asset edges |
| `object_publication_manifest.csv` | Logical asset to immutable object mapping |
| `pipeline_run_events.jsonl` | Ordered stage, retry, publication, and outcome events |
| `pipeline_run_summary.json` | Final status, durations, failure stage, and published manifest identity |

## Execution

Run the complete dependency-ordered pipeline:

```bash
python -m src
```

Run individual capabilities:

```bash
python -m src.ingestion
python -m src.storage --layer raw
python -m src.backtesting
python -m src.monte_carlo
python -m src.action_tracking
python -m src.causal_evaluation
python -m src.network_optimization
python -m src.storage --layer downstream
```

The release gate verifies temporal separation, inventory conservation,
probability bounds, frontier selection, action-benefit reconciliation, source
readiness, causal claim discipline, network feasibility, and Parquet hash integrity.
