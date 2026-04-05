# Executive KPI and Diagnostic Analysis

Project: Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

## Descriptive Analysis

### 1) Overall Service Level Health
- Overall fill rate: **92.27%**.
- Overall stockout rate (unit-based): **7.73%**.
- Lost sales exposure: **EUR 34,779,708**.
- Total service gap volume: **961,872 units** versus policy targets.
- Service dispersion by region: best region **Portugal South (95.04%)**, weakest region **France South-East (89.88%)**.
- Most pressured warehouse: **WH-LYON** with risk proxy **13.64**, fill rate **89.88%**, and lost sales value **EUR 9,493,608**.
- Category with highest lost-sales value: **Health** at **EUR 14,800,055**.

### 2) Inventory Efficiency
- Average days of supply: **23.97 days** (median **15.83**, p90 **50.53**, p99 **148.29**).
- Excess inventory day-rate proxy: **15.82%**.
- Slow-moving day-rate proxy: **1.27%**.
- Inventory concentration: top 20% of SKUs hold **52.54%** of average inventory value.
- Category with largest excess-value proxy: **Household** (proxy **EUR 273,539,117**).

### 3) Stockout and Lost Sales Risk
- Systematic stockout footprint: **119 SKU-warehouse combinations** classified as systematic.
- Highest-loss SKU-warehouse in current ranking: **SKU-0016 @ WH-MAD** with **EUR 2,420,289** lost sales.

## Diagnostic Analysis

### 4) Supplier-Driven Risk
- Most critical supplier risk profile: **SUP-002 (Supplier 2)** with supplier risk score **54.62**.
- This supplier shows on-time delivery **41.17%**, average delay **4.80 days**, lead-time variability **9.12**.
- Downstream propagation signal: supplier-linked proxy lost sales **EUR 13,544,812**, downstream stockout rate **20.00%**.

### 5) Service Level vs Working Capital Trade-off
- Dual-failure zone (overstocked yet under-serving): **16 SKU-warehouse combinations**, lost sales **EUR 134,281**.
- Balanced efficient zone count: **187 SKU-warehouse combinations**.
- This indicates simultaneous value leakage: revenue at risk from service failures and capital lock-up in high-DOS positions.

### 6) Action Prioritization
- Highest-priority SKU-location: **SKU-0016 @ WH-LIS** (priority score **63.95**, driver **Service Risk**).
- Highest-risk warehouse: **WH-LYON** (risk proxy **13.64**).
- Highest-risk supplier: **SUP-002** (risk proxy **54.62**).
- Most urgent intervention segment: **Frozen | France South-East** with avg governance priority **38.88** and lost-sales exposure **EUR 743,756**.

## Recommended Immediate Interventions (30-60 day)
1. Service recovery on high-priority SKU-locations where stockout risk and service-gap scores jointly exceed threshold; tune reorder points and protect promotional allocation.
2. Supplier corrective action plans for top-risk suppliers with poor OTD and high downstream lost-sales coupling; enforce SLA recovery cadence.
3. Working-capital release program for capital-heavy categories/segments with high excess-DOS proxies, using transfer/markdown and tighter order-up-to controls.

## Notes
- KPI files are saved as CSV in /outputs/reports/ for traceability and dashboard ingestion.
- This report separates descriptive performance status from diagnostic root-cause patterns for decision governance.