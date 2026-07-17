# Metric Dictionary

Project: Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System

## Scope
This dictionary documents the production metrics used by the analytical layer, scoring layer, KPI reporting, and executive dashboard.

Primary implementation references:
- `sql/02_intermediate_views.sql`
- `src/scoring.py`
- `src/impact_analysis.py`
- `src/causal_evaluation.py`
- `src/network_optimization.py`

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
| `on_order_units` | Quantity ordered on POs that have not arrived by the snapshot date. | `sum(ordered_units for open POs)` | Daily SKU-warehouse | Uses order-time information; eventual receipt underfill is recognized on arrival. |
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

## Working Capital and Financial Exposure Metrics
| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `trapped_working_capital_proxy` | Proxy of inefficient capital tied in inventory. | `excess_inventory_value_proxy + 0.50 * max(slow_moving_value_proxy - excess_inventory_value_proxy, 0)` | Daily SKU-warehouse | Avoids full double counting of slow-moving over excess. |
| `trapped_working_capital_proxy_average` | Average daily inefficient inventory balance. | Mean by day of summed `trapped_working_capital_proxy` across the selected scope. | Dashboard/filter scope, impact outputs | Balance metric; do not sum or annualize across dates. |
| `working_capital_at_risk` | Working-capital exposure shown in executive views. | `trapped_working_capital_proxy_average` | Dashboard/filter scope, impact outputs | Proxy estimate, not accounting balance sheet line item. |
| `gross_margin_rate` | Product-level gross margin ratio. | `(unit_price - unit_cost) / unit_price`, floor at 0 | Product | Used to convert lost sales to margin proxy. |
| `lost_sales_margin_proxy` | Margin value associated with lost sales. | `lost_sales_revenue * gross_margin_rate` | Daily SKU-warehouse and aggregated | Proxy for recoverable margin opportunity. |
| `opportunity_total_12m_proxy` | Total 12M value proxy under current assumptions. | `(annual_lost_sales_margin_proxy * recoverable_margin_rate) + (average_trapped_wc_proxy * releasable_wc_rate)` | Executive scope | Uses annualized margin flow plus releasable average WC balance; directional, not booking. |

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

## Policy, Simulation, and Action Metrics

| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `reorder_point` | Inventory position that triggers replenishment. | `ceil(mean_demand * lead_time + safety_factor * z * demand_std * sqrt(lead_time))` | Policy evaluation | Estimated from training history in backtests. |
| `order_up_to` | Inventory position after replenishment. | `reorder_point + mean_demand * cycle_stock_days` | Policy evaluation | MOQ is the lower bound on order quantity. |
| `economic_cost_proxy` | Policy comparison cost. | `lost_margin_proxy + holding_cost_proxy + ordering_cost_proxy` | Fold or simulation | Decision proxy, not accounting cost. |
| `target_success_rate` | Share of temporal folds meeting service policy. | `mean(fill_rate >= target - tolerance)` | SKU-location policy | Counterfactual backtest evidence. |
| `probability_target_met` | Simulated probability of meeting service target. | `mean(simulated_fill_rate >= target)` | SKU-location scenario | Empirical Monte Carlo estimate. |
| `is_frontier` | Non-dominated service-capital scenario. | No other scenario has at least as much service and no more inventory, with one strict improvement. | SKU-location scenario | Evaluated before recommendation selection. |
| `priority_score_improvement` | Observed reduction in intervention priority. | `pre_priority_score - post_priority_score` | Action | Positive means lower measured risk. |
| `observed_total_benefit_proxy` | Observed pre/post value movement. | `lost_margin_recovery_proxy + inventory_release_proxy` | Action | May be negative; explicitly non-causal. |

## Causal Evidence Metrics

| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `effect_estimate` | Assigned-unit average treatment effect on the treated. | Treated mean post-minus-pre change minus the control mean change. | Experiment | Causal only for a valid registered design with compatible evidence status. |
| `standard_error` | Cluster-level uncertainty of the change contrast. | Welch standard error from treated and control unit changes. | Experiment | The intervention unit, not the daily row, is the inference unit. |
| `randomization_p_value` | Exact-design approximation under reassignment. | Share of within-stratum permutations with an absolute effect at least as large as observed. | RCT experiment | Deterministic seed; not emitted for DiD. |
| `economic_value_estimate` | Point-estimate value mapped from the registered operational outcome. | Outcome-specific effect × treated post-period exposure. | Experiment | Directional and governed by `evidence_status`; not audited P&L. |
| `evidence_status` | Identification and inference result. | Design-specific decision from assignment, diagnostics, and significance. | Experiment | Values include supported, inconclusive, and insufficient-evidence states. |

## Network Optimization Metrics

| Metric | Definition | Formula | Grain | Notes |
|---|---|---|---|---|
| `achieved_service_level` | Planned share of horizon demand fulfilled. | `fulfilled_units / demand_units`; zero-demand rows use `1.0`. | Product-location | Must meet the contracted target. |
| `balance_error_units` | Independent physical-flow reconciliation residual. | `start + inbound - outbound - fulfilled - ending`. | Product-location | Must equal zero. |
| `slack_units` | Remaining capacity or shortage allowance. | `capacity_units - used_units`. | Constraint-entity | Negative slack blocks publication. |
| `objective_value` | Minimum modeled network cost. | Procurement + transport + holding + shortage + order activation. | Optimization run | Decision objective, not an accounting total. |
| `mip_gap` | Solver-reported relative optimality gap. | Incumbent-bound gap from HiGHS. | Optimization run | Must not exceed the configured tolerance. |
| `weighted_service_level` | Portfolio demand-weighted planned service. | `sum(fulfilled_units) / sum(demand_units)`. | Optimization run | Complements product-location service constraints. |

## Handling Rules for KPI Construction
- Do not average binary flags (`stockout_flag`, `slow_moving_day`, `excess_day`) without stating the denominator and interpretation.
- Prefer demand-weighted service metrics for executive comparisons.
- Treat value concentration metrics (`lost_sales_share`, `inventory_value_share`) as relative diagnostics, not absolute performance.
- Label all financial proxy metrics explicitly as proxy estimates.
- Aggregate inventory and working-capital balances across entities, then average across dates. Do not sum balances through time.
