# Release Governance

## Purpose
Define enforceable release states for analytics deliverables so technical validity is not confused with executive-grade evidence.

## State Definitions
| State | Meaning | Gate Outcome |
|---|---|---|
| Technically Valid | Core data integrity, schema logic, and scoring mechanics are coherent. | Required for any release |
| Analytically Acceptable | Technical validity plus no analytical failures and no high-severity warnings. | Required for decision-support release |
| Decision-Support Only | Suitable for operational prioritization with caveated proxy economics. | Release allowed |
| Screening-Grade Only | Useful for triage/scoping; not reliable enough for executive framing. | Release restricted |
| Not Committee-Grade | Synthetic/proxy constraints prevent audit-committee use. | Must remain explicitly disclosed |
| Publish-Blocked | Blocker/high failures or high-severity warnings detected. | Release denied |

## Enforcement
- `src/pre_delivery_validation.py` computes validation checks and emits `/outputs/tables/validation_release_state_matrix.csv`.
- `src/ci_quality_gate.py` blocks release if classification is `publish-blocked`.
- `outputs/reports/release_readiness.md` is the canonical generated status snapshot for each run.

## Current Policy
- Medium/low warnings are recorded and surfaced.
- High/blocker warnings and all failures are release-blocking.
- Committee-grade status remains blocked by design for synthetic/proxy outputs.
