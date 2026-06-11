# Release Governance

## Purpose
Define enforceable release states for the published analytics artefacts.

## State Definitions
| State | Meaning | Gate Outcome |
|---|---|---|
| Technically Valid | Core data integrity, schema logic, and scoring mechanics are coherent. | Required for any release |
| Analytically Acceptable | Technical validity plus no analytical failures and no high-severity warnings. | Required for decision-support release |
| Decision-Support Ready | Suitable for operational prioritization with explicit proxy caveats. | Release allowed |
| Publish Allowed | No failures or high-severity warnings remain. | Release may be promoted |

## Enforcement
- `src/pre_delivery_validation.py` computes validation checks and emits `/outputs/tables/validation_release_state_matrix.csv`.
- `src/ci_quality_gate.py` blocks release if classification is `publish-blocked`.
- CI requires the dashboard, 14 publication charts, and analytical report, then
  verifies that the tracked `/index.html` matches a fresh pipeline build.
- `outputs/tables/validation_pre_delivery_checks.csv` and `outputs/tables/validation_release_state_matrix.csv` are the canonical release status outputs.

## Current Policy
- Medium/low warnings are recorded and surfaced.
- All failures and high/blocker warnings are release-blocking.
- Synthetic-data and financial-proxy limitations remain explicit in the
  methodology and dashboard.
