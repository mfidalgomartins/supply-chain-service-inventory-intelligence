# Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

A decision‑grade supply chain analytics system built to expose the trade‑off between service performance and inventory capital. It models how stockouts, supplier instability, and excess inventory interact—and turns that into a ranked action list leadership can use.

Live dashboard: https://mfidalgomartins.github.io/supply-chain-service-inventory-intelligence/executive-supply-chain-command-center.html

## The problem this solves
Most distribution teams fail in both directions at once: service breaks create lost sales while excess stock traps cash. Without an integrated view, planning, procurement, and finance optimize locally and miss the portfolio trade‑off.

## What the system delivers
A governed pipeline that generates realistic operational data, builds KPI and risk tables, scores each SKU‑warehouse position, and surfaces where action will yield the highest service and capital impact. The output is a concise executive dashboard plus a set of scored tables used for prioritization.

## Decisions it supports
- Which SKUs and warehouses should be acted on first.
- Which suppliers are driving downstream service leakage.
- Where inventory policy is too aggressive or too conservative.
- How much working capital is tied up in slow or excess stock.

## Architecture (short)
- Data generation → SQL modeling → analytics tables → scoring → impact → dashboard → QA gates.
- Core code lives in `src/`, SQL logic in `sql/`, outputs under `outputs/`.

## Repository map
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
- `outputs/dashboard/executive-supply-chain-command-center.html` — presentation-ready dashboard
- `outputs/tables/sku_risk_table.csv` — ranked governance queue
- `outputs/tables/impact_overall_summary.csv` — portfolio exposure snapshot
- `docs/validation_report.md` — release QA summary

## Why this is above a typical portfolio project
It is a full decision‑support system with governance, scoring, and release discipline—not a notebook. The logic is interpretable, the outputs are curated for leadership, and the QA gates are explicit.

## Run it
```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python src/run_pipeline.py
```

## Limitations
- Synthetic data; results are methodological, not company‑specific.
- Financial impact metrics are proxy estimates, not audited P&L.

Tools: Python, SQL, DuckDB, pandas, Plotly, HTML, CSS.
