# Impact Assumptions Log

This document separates observed metrics from proxy estimates used for business-impact prioritization.

## Observed Metrics
- Lost sales revenue: directly observed from `daily_product_warehouse_metrics.lost_sales_revenue`.
- Inventory value: directly observed from `daily_product_warehouse_metrics.inventory_value`.
- Slow-moving days: observed when `available_units > 0` and `units_fulfilled = 0`.

## Proxy Formulas
- Excess inventory value proxy: `inventory_value * max(days_of_supply - dos_cap, 0) / max(days_of_supply, 1)` where DOS caps are A=20, B=30, C=45 days.
- Trapped working-capital proxy: `excess_inventory_value_proxy + 0.50 * max(slow_moving_value_proxy - excess_inventory_value_proxy, 0)`.
- Lost sales margin proxy: `lost_sales_revenue * gross_margin_rate` where `gross_margin_rate = (unit_price - unit_cost) / unit_price` from product master.
- Supplier delay impact proxy: `lost_sales_revenue * supplier_delay_factor`.
- Supplier delay factor: `0.45*(1-OTD) + 0.35*min(avg_delay_days/7,1) + 0.20*min(lead_time_variability/10,1)`.

## 12-Month Opportunity Assumptions
- Annualization factor: `0.499316` (365 / observed_days).
- Recoverable margin from service interventions: `35%` of annualized lost-sales margin proxy.
- Releasable working capital from inventory actions: `25%` of annualized trapped WC proxy.

## Caveats
- Proxy values are prioritization signals, not accounting-recognized P&L outcomes.
- Excess and slow-moving exposures are behavior-based estimates and may over/understate liquidation reality by SKU lifecycle.
- Supplier delay impact is associative, not a causal decomposition.
- Opportunity estimates should be validated with planner constraints, contract terms, and implementation feasibility.