# Metric Definitions (Legacy Pointer)

This file is kept only as a compatibility pointer.

The authoritative metric specification for the current production pipeline is:
- `docs/metric_dictionary.md`

Why this changed:
- older definitions in this file referred to a deprecated prototype stack (`fact_*`, `dim_*`, and `risk_score` logic),
- the active repository now uses `daily_product_warehouse_metrics`, policy-threshold scoring, and validated proxy-impact formulas.

Use `docs/metric_dictionary.md` for all implementation, QA, interview walkthroughs, and executive references.
