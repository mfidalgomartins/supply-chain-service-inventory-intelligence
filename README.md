# Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

**One-line:** Executive-grade supply chain analytics that quantifies service leakage vs inventory drag and prioritizes action by business impact.

## Business problem
Distribution networks often miss both sides of the trade‑off at once: stockouts lose revenue while excess inventory traps working capital. Teams optimize locally and lose portfolio visibility.

## What the system does
- Generates a realistic multi‑warehouse dataset with service, inventory, and supplier dynamics.
- Builds governed KPI and risk tables (SQL + Python) for service, inventory efficiency, and capital exposure.
- Produces a scored governance queue and executive dashboard for decision review.

## Decisions supported
- Where to intervene first across SKU‑warehouse combinations.
- Which suppliers and warehouses are driving service leakage.
- Where inventory policy is over‑protective or under‑protective.
- How much working capital is tied up in slow/excess stock.

## Project architecture
- **Data generation** → `src/data_generation.py`
- **SQL modeling + validation** → `sql/`
- **Analytics tables + scoring** → `src/data_preparation.py`, `src/feature_engineering.py`, `src/scoring.py`
- **Impact + dashboard** → `src/impact_analysis.py`, `src/executive_dashboard.py`
- **Quality gates** → `src/pre_delivery_validation.py`, `src/sql_quality_gate.py`, `src/ci_quality_gate.py`

## Repository structure
```
├── src/
├── data/
├── sql/
├── docs/
├── outputs/
├── tests/
└── configs/
```

## Core outputs
- `outputs/dashboard/index.html` — single-file executive dashboard
- `outputs/tables/sku_risk_table.csv` — primary governance queue
- `outputs/tables/impact_overall_summary.csv` — portfolio impact snapshot
- `docs/validation_report.md` — release QA summary

## Why this project is strong
- End‑to‑end system with governance, scoring, and release checks (not a notebook demo).
- Clear decision framing for operations and finance.
- Transparent, interpretable scoring and impact proxies.

## How to run
```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python src/run_pipeline.py
```

## Limitations
- Synthetic data; results are methodological, not company‑specific.
- Financial impact metrics are proxy estimates, not audited P&L.

## Tools
Python, SQL, DuckDB, pandas, Plotly, HTML, CSS.
