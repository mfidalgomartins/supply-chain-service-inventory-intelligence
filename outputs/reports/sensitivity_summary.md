# Opportunity Sensitivity Summary

This analysis stress-tests the financial opportunity proxy under alternate assumption settings to quantify model sensitivity and governance robustness.

## Scope
- Analysis window: **731 days**.
- Scenarios tested: **75** (margin x WC x slow-moving weight grid).

## Range of Outcomes
- Highest scenario: **EUR 60,745,948** (margin=50%, wc=35%, slow_w=70%).
- Lowest scenario: **EUR 25,275,265** (margin=20%, wc=15%, slow_w=30%).

## Dominant Assumption Driver
- Largest modeled swing comes from **releasable_wc_rate** with approx. **EUR 32,093,446** spread across tested bounds.

## Governance Use
- Use baseline assumptions for default prioritization, but include sensitivity range in executive communication to avoid false precision.
- Escalate decisions where business cases only hold under optimistic parameter settings.