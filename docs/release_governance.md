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
- The final gate verifies the exact 14-chart set, PNG integrity and minimum
  dimensions, PDF pagination and required sections, and all required documents.
- Advanced gates verify temporal separation, inventory-flow conservation,
  simulation probability bounds, efficient-frontier selection, action-benefit
  reconciliation, source readiness, causal-claim discipline, multi-echelon
  feasibility, and Parquet/source hash integrity.
- `src/orchestration.py` validates stage dependencies and stage-owned outputs.
  It writes failure telemetry before returning a failed status and does not retry
  deterministic contract, model, or publication defects.
- After the CI gate passes, `src/data_catalog.py` rejects missing producers,
  missing parents, duplicate producers, and lineage cycles. Publication then
  verifies catalogued content hashes and writes immutable objects.
- `pointers/latest.json` is promoted only after every stage, release check,
  catalog check, and immutable manifest write succeeds.
- CI verifies that the tracked `/index.html`, 14 charts, and analytical PDF
  match a fresh build on Python 3.12, 3.13, and 3.14.
- `.github/workflows/scheduled-analytics.yml` executes the same entry point
  weekly or manually with non-overlapping runs and uploads operational evidence.
- `outputs/tables/validation_pre_delivery_checks.csv` and `outputs/tables/validation_release_state_matrix.csv` are the canonical release status outputs.

## Current Policy
- Medium/low warnings are recorded and surfaced.
- All failures and high/blocker warnings are release-blocking.
- Synthetic-data and financial-proxy limitations remain explicit in the
  methodology and dashboard.
- Inconclusive causal evidence is publishable only with an inconclusive label;
  unsupported identification is labelled `not_causal`.
- Non-optimal or infeasible network solutions are not publishable plans.
