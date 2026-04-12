# Scoring Framework

## Purpose
The scoring layer creates a transparent intervention-priority system that ranks:
- SKU-warehouse combinations
- Suppliers
- Category-region segments

Implementation: `src/scoring.py`

Output tables:
- `/data/processed/sku_risk_table.csv`
- `/data/processed/supplier_risk_table.csv`
- `/data/processed/segment_risk_table.csv`
- `/data/processed/governance_priority_master.csv`

## Entity Grains
- SKU level: `product_id + warehouse_id + supplier_id + category + region`
- Supplier level: `supplier_id`
- Segment level: `category + region`

## Scoring Design Principles
- Interpretable policy thresholds instead of black-box ML.
- 0-100 normalized component scores.
- Same component logic across entities for comparability.
- Service and working-capital trade-off explicitly represented via dual-imbalance logic.

## Normalization Logic
### Linear policy scaling
For operational ratios/rates:
- `linear_score = clip((value - good_threshold) / (bad_threshold - good_threshold), 0, 1) * 100`

### Log-share scaling
For concentration effects (`lost_sales_share`, `inventory_value_share`):
- `log_share_score = clip((ln(value) - ln(low_share)) / (ln(high_share) - ln(low_share)), 0, 1) * 100`

## Threshold Anchors
From `Thresholds` dataclass in `src/scoring.py`.

| Component | Good | Bad |
|---|---:|---:|
| Fill gap (`1 - fill_rate`) | 0.01 | 0.15 |
| Service gap rate | 0.00 | 0.20 |
| Stockout rate | 0.01 | 0.18 |
| Stockout active month ratio | 0.10 | 0.65 |
| DOS stretch (`avg_dos / avg_dos_cap`) | 1.00 | 2.25 |
| Excess day rate | 0.05 | 0.40 |
| Slow-moving rate | 0.01 | 0.12 |
| Criticality index | 0.45 | 0.85 |
| Supplier OTD gap | 0.05 | 0.45 |
| Supplier delay days | 0.50 | 5.00 |
| Supplier lead-time variability | 1.50 | 10.00 |
| Supplier underfill | 0.00 | 0.08 |
| Lost-sales share (log) | 0.0005 | 0.08 |
| Inventory-value share (log) | 0.0010 | 0.12 |

## Score Definitions and Formulas
### 1) `service_risk_score`
Measures service risk against policy targets.

Formula:
- `0.35 * fill_gap_score`
- `+ 0.30 * service_gap_score`
- `+ 0.20 * criticality_score`
- `+ 0.15 * lost_share_score`

### 2) `stockout_risk_score`
Measures unit stockout intensity and persistence.

Formula:
- `0.55 * stockout_rate_score`
- `+ 0.30 * stockout_persistence_score`
- `+ 0.15 * lost_share_score`

### 3) `excess_inventory_score`
Measures overstock pressure versus ABC policy and concentration.

Formula:
- `0.45 * dos_stretch_score`
- `+ 0.35 * excess_day_score`
- `+ 0.20 * inventory_share_score`

### 4) `supplier_risk_score`
Supplier base score from execution quality, then demand-weighted exposure for non-supplier entities.

Supplier base formula:
- `0.45 * otd_gap_score`
- `+ 0.20 * delay_score`
- `+ 0.20 * lead_time_variability_score`
- `+ 0.15 * underfill_score`

### 5) `working_capital_risk_score`
Measures capital lock-up risk from DOS stretch, slow movement, and concentration.

Formula:
- `0.45 * dos_stretch_score`
- `+ 0.30 * slow_moving_score`
- `+ 0.25 * inventory_share_score`

### 6) `governance_priority_score`
Final intervention ranking score balancing service, supplier, and capital trade-offs.

Formula:
- `0.24 * service_risk_score`
- `+ 0.22 * stockout_risk_score`
- `+ 0.18 * excess_inventory_score`
- `+ 0.16 * supplier_risk_score`
- `+ 0.14 * working_capital_risk_score`
- `+ 0.06 * dual_imbalance_score`

Where:
- `dual_imbalance_score = min(service_risk_score, excess_inventory_score)`

## Supporting Component Logic
Derived fields used before scoring:
- `fill_rate = units_fulfilled / units_demanded` (safe division)
- `stockout_rate = units_lost_sales / units_demanded` (safe division)
- `service_gap_rate = service_gap_units / units_demanded`
- `stockout_active_month_ratio = mean(monthly_stockout_flag)`
- `criticality_index = demand-weighted criticality (High=1.0, Medium=0.6, Low=0.3)`
- `dos_stretch = average_days_of_supply / demand-weighted_abc_dos_cap`
- `lost_sales_share = entity_lost_sales / company_lost_sales`
- `inventory_value_share = entity_inventory_value / company_inventory_value`

## Risk Tiers
Tier assignment from `governance_priority_score`:
- `Low`: `<= 35`
- `Medium`: `> 35 and <= 55`
- `High`: `> 55 and <= 75`
- `Critical`: `> 75`

## Main Risk Driver
`main_risk_driver` is the highest of:
- `service_risk_score`
- `stockout_risk_score`
- `excess_inventory_score`
- `supplier_risk_score`
- `working_capital_risk_score`

Driver labels:
- Service Risk
- Stockout Risk
- Excess Inventory
- Supplier Risk
- Working Capital

## Recommended Action Mapping
Implemented in `recommended_action()` in `src/scoring.py` with entity-specific routing.

Low tier default:
- `monitor only`

Examples by driver:
- Service Risk:
  - SKU: `review reorder point and raise safety stock`
  - Supplier: `review service target by SKU class and stabilize replenishment cadence`
  - Segment: `review planning assumptions and rebalance stock across warehouses`
- Stockout Risk:
  - SKU: `expedite replenishment and rebalance stock across warehouses`
  - Supplier: `expedite replenishment on constrained supplier lanes`
  - Segment: `raise safety stock for critical SKUs and protect promotion allocation`
- Excess Inventory:
  - SKU: `reduce safety stock and review assortment strategy`
  - Supplier: `review MOQ and order cadence to reduce overstock propagation`
  - Segment: `rebalance stock across warehouses and reduce order-up-to levels`
- Supplier Risk:
  - SKU: `investigate supplier reliability and qualify backup source`
  - Supplier: `investigate supplier reliability and execute corrective action plan`
  - Segment: `review sourcing mix and supplier dependency for this segment`
- Working Capital:
  - SKU: `review planning assumptions and tighten order-up-to policy`
  - Supplier: `review planning assumptions and align procurement with demand volatility`
  - Segment: `review assortment strategy and release cash from slow-moving inventory`

## Governance Usage Guidance
- Use `governance_priority_score` as a weekly intervention queue.
- Route ownership using `main_risk_driver`.
- Track score migration over time; close actions only after sustained tier reduction and KPI improvement.
- Treat scores as prioritization logic, not causal proof.

## Operationalization Layer
Governance actions should be logged outside this repo (S&OP or ticketing tools) using the score table as the weekly intake list.
