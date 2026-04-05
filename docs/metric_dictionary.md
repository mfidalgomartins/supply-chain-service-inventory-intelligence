# Metric Dictionary

Project: Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

## Scope
This dictionary documents the production metrics used by the analytical layer, scoring layer, KPI reporting, and executive dashboard.

Primary implementation references:
- `sql/02_intermediate_views.sql`
- `src/feature_engineering.py`
- `src/scoring.py`
- `src/impact_analysis.py`
- `src/probabilistic_forecast.py`
- `src/policy_optimizer.py`
- `src/intervention_tracker.py`
- `src/anomaly_alerts.py`
- `src/sensitivity_analysis.py`

## Service Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `fill_rate` | Share of demanded units fulfilled. | `units_fulfilled / units_demanded`; if denominator is 0, set to `1.0`. | Daily SKU-warehouse and aggregated levels | Core service KPI. |
| `stockout_rate` | Share of demanded units not fulfilled. | `units_lost_sales / units_demanded`; if denominator is 0, set to `0.0`. | Aggregated entity levels | Unit-based stockout rate. |
| `stockout_flag` | Daily stockout event indicator. | `1` if `units_lost_sales > 0`, else `0`. | Daily SKU-warehouse | Event-rate metrics should use denominators. |
| `service_gap_units` | Unit shortfall vs target service policy. | `max(target_service_level * units_demanded - units_fulfilled, 0)` | Daily SKU-warehouse | Policy-gap metric, not realized lost sales. |
| `service_gap_rate` | Service gap normalized by demand. | `service_gap_units / units_demanded`; if denominator is 0, `0.0`. | Scoring entities | Used in `service_risk_score`. |
| `lost_sales_revenue` | Value of unmet demand at selling price. | `units_lost_sales * unit_price` | Daily SKU-warehouse and aggregated | Observed revenue exposure proxy. |

## Inventory Efficiency Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `days_of_supply` | Stock coverage in days. | `available_units / expected_daily_demand` (simulated and persisted in raw snapshots). | Daily SKU-warehouse | Already generated in source data. |
| `average_days_of_supply` | Mean DOS across period. | `avg(days_of_supply)` | Product, warehouse, segment | Use percentile context due right skew. |
| `excess_day` | Indicator that DOS exceeds ABC policy cap. | `1` if `days_of_supply > dos_cap`, else `0`; caps: A=20, B=30, C=45. | Daily SKU-warehouse | Binary input to multiple proxies/scores. |
| `excess_day_rate` | Share of days above ABC DOS cap. | `avg(excess_day)` | Scoring entities | Behavioral overstock signal. |
| `slow_moving_day` | Inventory present with no fulfillment. | `1` if `available_units > 0 and units_fulfilled = 0`, else `0`. | Daily SKU-warehouse | Slow-moving inventory behavior. |
| `slow_moving_rate` | Share of slow-moving days. | `avg(slow_moving_day)` | Scoring entities | Used for WC risk. |
| `excess_inventory_proxy` | Value of inventory above ABC DOS caps. | `inventory_value * max(days_of_supply - dos_cap, 0) / max(days_of_supply, 1e-9)` | Daily SKU-warehouse (impact/dashboard layers) | Proxy, not realizable liquidation value. |
| `slow_moving_value_proxy` | Value exposed on slow-moving days. | `inventory_value * slow_moving_flag` | Daily SKU-warehouse | Can overlap with excess exposure. |

## Supplier Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `on_time_delivery_rate` | Share of POs arriving on or before expected date. | `avg(case when late_delivery_flag = 0 then 1 else 0 end)` | Supplier | From PO execution history. |
| `average_delay_days` | Mean positive delay days vs expected arrival. | `avg(max(actual_arrival_date - expected_arrival_date, 0))` | Supplier | Zero when early/on-time. |
| `lead_time_variability` | Variability of realized PO lead time. | `stddev(actual_arrival_date - order_date)` | Supplier | Higher value implies instability. |
| `received_vs_ordered_fill_rate` | PO receipt completeness. | `sum(received_units) / sum(ordered_units)`; if denominator 0, `1.0` | Supplier | Underfill signal in supplier risk. |
| `supplier_service_risk_proxy` | Interpretable supplier risk proxy from delivery behavior. | `100 * (0.40*(1-OTD) + 0.20*min(avg_delay/10,1) + 0.20*min(lt_var/8,1) + 0.20*(1-po_fill))` | Supplier | Used in intermediate and feature layers. |

## Working Capital and Financial Exposure Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `trapped_working_capital_proxy` | Proxy of inefficient capital tied in inventory. | `excess_inventory_value_proxy + 0.50 * max(slow_moving_value_proxy - excess_inventory_value_proxy, 0)` | Daily SKU-warehouse and aggregated | Avoids full double counting of slow-moving over excess. |
| `working_capital_at_risk` | Working-capital exposure shown in executive views. | Aggregated `trapped_working_capital_proxy` over selected scope | Dashboard/filter scope, impact outputs | Proxy estimate, not accounting balance sheet line item. |
| `gross_margin_rate` | Product-level gross margin ratio. | `(unit_price - unit_cost) / unit_price`, floor at 0 | Product | Used to convert lost sales to margin proxy. |
| `lost_sales_margin_proxy` | Margin value associated with lost sales. | `lost_sales_revenue * gross_margin_rate` | Daily SKU-warehouse and aggregated | Proxy for recoverable margin opportunity. |

## Governance and Scoring Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `service_risk_score` | Service-policy risk (0-100). | `0.35*fill_gap_score + 0.30*service_gap_score + 0.20*criticality_score + 0.15*lost_share_score` | SKU, supplier, segment | Component scores normalized via policy thresholds. |
| `stockout_risk_score` | Stockout severity and persistence risk (0-100). | `0.55*stockout_rate_score + 0.30*stockout_persistence_score + 0.15*lost_share_score` | SKU, supplier, segment | Includes monthly persistence behavior. |
| `excess_inventory_score` | Overstock policy risk (0-100). | `0.45*dos_stretch_score + 0.35*excess_day_score + 0.20*inventory_share_score` | SKU, supplier, segment | Captures DOS stretch and excess-day behavior. |
| `supplier_risk_score` | Supplier-driven execution risk (0-100). | Base supplier score: `0.45*otd_gap + 0.20*delay + 0.20*lt_var + 0.15*underfill` after normalization | SKU, supplier, segment | Demand-weighted exposure for non-supplier entities. |
| `working_capital_risk_score` | Capital-efficiency risk (0-100). | `0.45*dos_stretch_score + 0.30*slow_moving_score + 0.25*inventory_share_score` | SKU, supplier, segment | Uses normalized inventory behavior and concentration. |
| `governance_priority_score` | Composite intervention priority score (0-100). | `0.24*service + 0.22*stockout + 0.18*excess + 0.16*supplier + 0.14*working_capital + 0.06*min(service,excess)` | SKU, supplier, segment | Primary weekly governance ranking metric. |
| `risk_tier` | Priority classification band. | `Low <=35`, `Medium (35,55]`, `High (55,75]`, `Critical >75` | Scoring entities | Used for intervention queueing. |
| `main_risk_driver` | Dominant risk component for routing ownership. | `argmax(service, stockout, excess, supplier, working_capital)` | Scoring entities | Drives recommended action mapping. |

## Policy Simulation and Stress Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `estimated_service_level` | Forecasted service under policy scenario. | `Phi((reorder_point - demand_mean*lead_time) / (demand_std*sqrt(lead_time)))` | Scenario x SKU x warehouse | Policy simulation output from `src/policy_simulation.py`. |
| `expected_inventory_value` | Forecasted inventory value under scenario policy. | `expected_on_hand_units * unit_cost` | Scenario x SKU x warehouse | Used to build service-vs-capital frontier. |
| `prob_stockout` | Simulated stockout probability under uncertainty. | `mean(demand_during_lead_time > available_position)` across Monte Carlo iterations | Supplier x warehouse x category x SKU | Monte Carlo downside risk metric. |
| `expected_service_level` (stress) | Mean simulated service under uncertainty. | `mean(min(available_position / demand_during_lead_time, 1))` | Supplier x warehouse x category x SKU | Captures resilience under volatility. |
| `supplier_lane_risk_score` | Composite lane-level supplier risk score. | Weighted combination of downstream stockout, fill gap, supplier risk proxy, excess day rate, and lost-sales concentration | Supplier x warehouse x category | Used for lane-level procurement and S&OE exception management. |

## Forecast, Optimization, and Operational Governance Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `forecast_q10`, `forecast_q50`, `forecast_q90` | Probabilistic demand forecast band by lane-date. | EWMA level x day-of-week seasonality with normal residual spread; q10/q50/q90 from z-scores | Forecast date + SKU + warehouse | Generated in `src/probabilistic_forecast.py`. |
| `uncertainty_band_rate` | Relative forecast uncertainty width. | `(sum(q90) - sum(q10)) / sum(q50)` over forecast horizon | SKU + warehouse + supplier | Higher values indicate unstable demand signal. |
| `benefit_proxy` (optimizer) | Policy upgrade benefit proxy under capital constraint. | `0.70*lost_sales_improvement + 0.30*(service_improvement*1,000,000)` weighted by ABC priority | Scenario + SKU + warehouse | Decision ranking signal, not accounting value. |
| `benefit_cost_ratio` | Efficiency of policy upgrade per unit capital. | `benefit_proxy / max(delta_inventory, 1)` | Scenario + SKU + warehouse | Used for greedy budget allocation. |
| `service_uplift` (optimizer) | Incremental weighted service level versus baseline. | `weighted_selected_service - weighted_base_service` | Budget scenario | Evaluates marginal policy return by capital tier. |
| `lost_sales_improvement` (optimizer) | Lost-sales reduction under selected policy set. | `base_lost_sales - selected_lost_sales` | Budget scenario | Reported as directional economic benefit proxy. |
| `expected_value_proxy` (intervention) | Combined value signal for intervention prioritization. | `expected_margin_recovery_proxy + expected_wc_release_proxy` | Intervention ID | Used for owner backlog sequencing. |
| `required_score_reduction` | Score movement needed to meet target tier. | `max(governance_priority_score - target_governance_score, 0)` | Intervention ID | Used for closure criteria. |
| `z_score` (alerts) | Magnitude of metric spike versus rolling baseline. | `(metric_value - baseline_mean) / baseline_std` | Alert ID | Supports severity assignment (`Medium/High/Critical`). |

## Sensitivity Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `opportunity_total_12m_proxy` (sensitivity) | Total 12M value under a specific assumption set. | `(annual_lost_sales_margin_proxy * recoverable_margin_rate) + (annual_trapped_wc_proxy_scenario * releasable_wc_rate)` | Assumption scenario row | Used to communicate uncertainty band, not a single-point commitment. |
| `annual_trapped_wc_proxy_scenario` | Working-capital proxy under configurable slow-moving overlap weight. | `annual_excess_inventory_proxy + slow_moving_incremental_weight * annual_slow_non_excess_proxy` | Assumption scenario row | Prevents false precision from fixed overlap assumptions. |
| `swing` (tornado) | Modeled impact range of each assumption factor. | `high_opportunity - low_opportunity` | Assumption factor | Used to identify assumptions that most influence executive value claims. |

## Handling Rules for KPI Construction
- Do not average binary flags (`stockout_flag`, `slow_moving_day`, `excess_day`) without stating the denominator and interpretation.
- Prefer demand-weighted service metrics for executive comparisons.
- Treat value concentration metrics (`lost_sales_share`, `inventory_value_share`) as relative diagnostics, not absolute performance.
- Label all financial proxy metrics explicitly as proxy estimates.
