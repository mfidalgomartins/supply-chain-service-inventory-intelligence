# Executive Summary

## Executive Interpretation
Service performance is not broken everywhere, but it is materially unbalanced. The network is delivering a solid baseline in many lanes while still carrying concentrated failure pockets that are expensive both operationally and financially.

Observed operating facts (latest run):
- Overall fill rate is **92.27%** with a **7.73%** unit stockout rate.
- Lost sales exposure is **EUR 34.78M** over the observed period.
- Service gap versus policy targets is **961,872 units**.
- Stockouts are not only episodic: **119 of 480 SKU-warehouse combinations** are in the "Systematic" stockout pattern.

This is the central operating message for leadership: the company is achieving acceptable aggregate service, but the miss is concentrated in specific suppliers, warehouses, categories, and SKU-location combinations where intervention value is high.

## What The Company Is Doing Well
1. A meaningful share of the network is performing efficiently.
- **187 SKU-warehouse combinations** sit in the "Balanced Efficient" zone (high fill with policy-aligned DOS).

2. Operational controls are preventing broad network collapse.
- **256 SKU-warehouse combinations** are classified as "Low/Contained" stockout patterns.

3. Service is not uniformly weak.
- Best-performing regions and categories remain stable enough to provide templates for replication of planning and replenishment practices.

## Where Service Levels Are Failing
1. Warehouse concentration of service risk is clear.
- **WH-LYON** is the most pressured node (fill rate **89.88%**, lost sales **EUR 9.49M**, highest warehouse service risk proxy).
- **WH-MAD** also carries high lost-sales burden (**EUR 11.05M**) with elevated service risk.

2. Category-level service leakage is concentrated.
- **Health** has the highest lost-sales value (**EUR 14.80M**) and the weakest category fill profile among top-value categories.
- **Pet Care** is the second largest lost-sales pocket (**EUR 7.38M**).

3. Understocking remains a direct revenue issue.
- The "Understocked Revenue Exposure" zone accounts for **EUR 16.63M** lost sales, the largest trade-off zone by lost value.

## Where Inventory Is Inefficient
Observed inventory behavior shows excess is not random; it is structural in specific segments.

- Average DOS is **23.97 days**, but tail risk is high (p90 **50.53**, p99 **148.29**).
- Excess-inventory day rate is **15.82%** and slow-moving day rate is **1.27%**.
- Largest excess-value concentration is in:
  - **Household** (excess proxy ~**EUR 273.5M**)
  - **Health** (excess proxy ~**EUR 223.1M**)

Management implication: inventory policy is over-protecting parts of the portfolio where demand quality does not justify the capital tie-up.

## How Supplier Performance Contributes To Risk
Supplier instability is materially linked to downstream service failure.

- **SUP-002** is the highest-risk supplier (risk proxy **54.62**) with weak execution profile:
  - On-time delivery rate **41.17%**
  - Average delay **4.80 days**
  - Lead-time variability **9.12 days**
- This supplier alone is associated with **EUR 13.54M** downstream lost-sales exposure and a **20.00%** downstream stockout rate.

This is a procurement-and-operations issue, not only a planning issue.

## Working Capital Exposure And Financial Consequences
### Observed values
- Lost sales exposure: **EUR 34.78M**
- Excess inventory value proxy: **EUR 310.54M**
- Trapped working-capital proxy: **EUR 321.37M**
- Slow-moving value proxy: **EUR 30.19M**

### Estimated/proxy impacts
- Supplier delay impact proxy: **EUR 20.21M**
- 12-month opportunity proxy (margin recovery + WC release): **EUR 42.79M**

Important distinction: the opportunity figure is an estimate using explicit assumptions (recoverable lost margin rate and releasable WC rate). It is suitable for prioritization, not budget booking.

## What Deserves Intervention First
1. **Supplier-led service recovery**
- Immediate corrective plan for SUP-002 and other top-risk suppliers (OTD recovery targets, delay variance containment, backup sourcing lanes).

2. **Warehouse execution focus on WH-LYON and WH-MAD**
- Reorder-point and allocation recalibration for high-lost-sales SKUs.
- Weekly exception governance on stockout persistence, not just average fill rate.

3. **SKU-level critical fixes**
- Top governance-priority SKU-locations are dominated by **SKU-0016** across multiple warehouses, indicating systemic planning/sourcing imbalance rather than local noise.

4. **Category working-capital release program**
- Household and Health should be targeted for DOS-cap enforcement, transfer/markdown governance, and order-up-to discipline.

## Trade-Offs Management Must Actively Manage
Leadership should govern four active trade-offs, not one:
1. Fill-rate protection vs avoidable capital lock-up.
2. Supplier resilience spending vs short-term procurement cost minimization.
3. Promotional service protection vs baseline inventory discipline.
4. Network-level rebalancing speed vs local warehouse optimization preferences.

The current state shows both sides of the trade-off failing in pockets: understocked revenue loss and overstocked capital drag.

## Uncertainty And Caveats
- Financial impact outputs are proxy estimates, not accounting-recognized outcomes.
- Supplier delay impact is associative, not causal attribution.
- Results are sensitive to assumption choices (especially recoverable margin and releasable WC rates).
- Because data is synthetic, decision logic is robust for demonstration, while absolute euro values should be treated as scenario-level magnitudes.
