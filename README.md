# Supply Chain Service Level and Inventory Intelligence

A reproducible decision-support pipeline that identifies where stockouts,
supplier instability, and excess inventory create the largest service and
working-capital exposure across a multi-warehouse network.

**[Open the live dashboard](https://mfidalgomartins.github.io/supply-chain-service-inventory-intelligence/)**
· **[Read the analytical report](https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/blob/main/outputs/reports/service_inventory_intelligence_report.pdf)**

![Opportunity by category](outputs/graphs/01_opportunity_by_category.png)

The reproducible synthetic dataset covers **120 products, 12 suppliers, 4
warehouses, and 731 daily observations from 2024-01-01 to 2025-12-31**. The
current scenario reports a **95.96% fill rate**, **€20.3M observed lost-sales
exposure**, and a **€1.64M directional 12-month opportunity proxy**.

## Decisions Supported
- Prioritize SKU-location interventions by service, stockout, supplier, and inventory risk.
- Identify suppliers and warehouses associated with the largest downstream exposure.
- Find excess inventory that can be reviewed without applying broad stock reductions.
- Quantify lost-sales margin and releasable working-capital scenarios.

## Analytical Flow
```text
reproducible synthetic data
  -> SQL analytical views
  -> policy-based risk scoring
  -> impact estimates
  -> dashboard, publication charts, and analytical report
  -> contract, SQL, analytical, and publication gates
```

The priority score is policy-based and fully documented. Financial values are
labelled as proxy estimates and are not presented as audited P&L or causal
attribution.

## Run Locally
Requires Python 3.12 or newer.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python src/run_pipeline.py
.venv/bin/python -m pytest -q
```

The repository intentionally does not version pipeline-generated CSV files.
Running the pipeline rebuilds `data/raw/`, `data/processed/`, and
`outputs/tables/`, then refreshes the tracked dashboard, charts, and PDF report.

## Quality Controls
- Data contracts cover required columns, grain, nulls, ranges, domains, and key references.
- SQL and Python checks reconcile service, inventory, impact, scoring, and dashboard metrics.
- CI runs the full pipeline, unit tests, release gates, and a dashboard freshness check.
- The release gate blocks publication on any failure or high-severity warning.

## Documentation
- [Methodology](docs/methodology.md)
- [Data model](docs/data_model.md)
- [Metric dictionary](docs/metric_dictionary.md)
- [Scoring framework](docs/scoring_framework.md)
- [Release governance](docs/release_governance.md)

## Repository Layout
```text
configs/   data-contract definitions
docs/      methodology and analytical documentation
sql/       schema, analytical views, KPI queries, and validation queries
src/       generation, transformation, scoring, publication, and gates
tests/     focused unit tests for critical logic
outputs/   tracked publication charts and analytical report
index.html GitHub Pages dashboard
```

## Limitations
- The dataset is synthetic and does not represent a specific company.
- Composite scores support prioritization; they do not prove root cause.
- Financial opportunity metrics are directional scenario estimates.
- The static dashboard loads the pinned Plotly JavaScript bundle from a CDN.

## Stack
Python, pandas, NumPy, DuckDB, SQL, JavaScript, HTML, CSS.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for the validation and pull-request
workflow.
