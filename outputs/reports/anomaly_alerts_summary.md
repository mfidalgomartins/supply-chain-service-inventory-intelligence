# Anomaly Alerts Summary

This layer detects sudden operational spikes in warehouse service failure and supplier execution instability.

## Current Alert Book
- Total alerts: **428**
- Most frequent severity: **Medium** (213 alerts)

## Highest-Z Alert
- supplier `SUP-011` | metric `avg_delay_days`
- z-score: **7.62**, severity: **Critical**
- diagnosis: Supplier delay-duration spike vs 8-week baseline

## Governance Use
- Route Critical/High alerts to daily operations stand-up.
- Link repeated alerts to intervention ownership and closure evidence.