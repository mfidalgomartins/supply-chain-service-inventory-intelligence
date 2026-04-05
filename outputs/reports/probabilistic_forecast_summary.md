# Probabilistic Forecast Summary

This layer upgrades lane demand inputs from static rolling means to a probabilistic forecast distribution used by policy simulation.

## Highest Uncertainty Lane
- Lane: **SKU-0098 | WH-LYON**
- Forecast 30D p50 demand: **492.6 units**
- Forecast uncertainty band rate: **2.76**
- Forecast CV proxy: **1.40**

## Governance Use
- Use p50 demand for base planning and p90 demand to stress safety-stock policy in critical lanes.
- Lanes with high uncertainty band and high commercial exposure should receive tighter planning cadence.