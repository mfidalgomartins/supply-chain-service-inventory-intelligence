# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-07-14

### Added
- Governed source-readiness gates for all 11 ERP/WMS contracts, including
  ownership, watermark SLAs, business keys, stable schema fingerprints, drift
  classification, and fail-before-overwrite behavior.
- Registered causal evaluation for a stratified inventory-policy RCT and a
  supplier-recovery DiD cohort, with cluster-level inference, randomization,
  pre-trend, placebo, balance, support, and attribution-label controls.
- Sparse multi-echelon HiGHS MILP for eligible sourcing and transfer flows under
  MOQ, source, lane, storage, service, integrality, and conservation constraints.
- Dependency-validated orchestration with stage-owned outputs, checkout-stable
  content-derived run IDs, bounded transient retries, JSONL telemetry, and final
  summaries.
- Validated data catalog and lineage graph plus immutable content-addressed
  publication through local or S3-compatible object storage.
- Weekly/manual scheduled workflow with concurrency control and
  operational-evidence uploads.
- Configurable synthetic, canonical-directory, and split ERP/WMS ingestion for
  CSV or Parquet exports, with column mapping and contract-first validation.
- Compressed Parquet lake with key-based incremental upserts, idempotent hash
  skips, schema-drift protection, watermarks, and a governed manifest.
- Leakage-safe walk-forward backtesting of reorder-point and safety-stock
  policies across the full SKU-location network.
- Deterministic Monte Carlo service-capital optimization using empirical demand
  blocks and lead-time histories, warm-up state, explicit constraints, and
  frontier selection.
- Lifecycle-aware action register with score migration, equal-window KPI
  comparison, benefit reconciliation, and explicit non-causal labels.
- Release checks and data contracts for every persisted raw, processed, and
  advanced analytical output.
- Schema-enforced DuckDB loading with operational `CHECK` constraints at the
  raw-to-analytics boundary.
- Publication guards for the exact chart set, PNG integrity and dimensions,
  PDF pagination, and required report sections.
- Tests for open-order information timing, zero-demand score weighting, schema
  enforcement, and SQL identifier validation.

### Changed
- Pipeline configuration moved to schema version 2 with validated source,
  causal, network, orchestration, and object-store policy.
- The canonical pipeline now treats causal evaluation and network optimization
  as required upstream dependencies of downstream storage and release validation.
- CI operational artifacts now include run telemetry, catalog, lineage,
  publication manifest, and the promoted pointer.
- Open-order inventory positions now use ordered quantities, removing future
  knowledge of supplier underfill from replenishment decisions.
- Dashboard markup moved to a static template while preserving byte-identical
  generated HTML and the existing visual design.
- Pipeline stages now execute as Python modules; GitHub Actions cover Python
  3.12-3.14 with read-only permissions and commit-pinned actions.
- Runtime and development dependencies were separated, and `pre-commit` is pinned.
- CI freshness checks now cover the dashboard, all 14 charts, and the PDF report.
- Pillow was upgraded to 12.3.0 to incorporate its July 2026 security fixes.

### Fixed
- Run identifiers no longer depend on the absolute checkout path.
- Transient S3 throttling/service failures are classified for retry while
  deterministic object conflicts and integrity failures remain fail-fast.
- Parquet skips now verify the stored-file hash before reuse; tampered or
  corrupted files are rebuilt, compression changes force replacement, and null
  business keys are rejected.
- Dashboard data embedded in JavaScript now escapes script-breaking characters
  from external ERP/WMS dimensions.
- Walk-forward policy simulations now initialize inventory and outstanding
  orders on the training window before evaluation begins.
- Action tracking supports pending measurement windows, validates target
  direction and supplier scope, and preserves both periods for mid-month changes.
- Raw validation now rejects warehouse-region mismatches and non-positive
  seasonality indices.

## [1.2.0] - 2026-07-09

### Added
- Unified Apple-inspired design system across the dashboard and publication
  charts (San Francisco typography, system-blue accent, refined materials
  and motion), verified against WCAG AA contrast in both light and dark
  themes with a dedicated 26-check regression test.
- Branded SVG favicon and Open Graph/Twitter/theme-color metadata on the
  dashboard for correct link previews and mobile browser chrome.
- A bordered `note_box()` style in the PDF report for supplementary asides,
  replacing subsections that previously stranded a single paragraph on an
  otherwise-blank page.

### Changed
- Tightened the analytical report's prose and made the recovery-rate
  assumptions behind the headline value pool (35% margin, 25% working capital)
  explicit in the narrative and sensitivity analysis.
- Re-typeset the report's pagination: findings no longer force onto a fresh
  page regardless of remaining space, and two subsections that each
  stranded a single paragraph on a near-blank page now share space with
  adjacent content. Net effect: 32 pages to 29, entirely from removed blank
  space, no content cut.
- Chart typography now renders in a Helvetica/Arial-first font stack
  (graceful fallback to the prior default) to match the report's body
  typeface; applied the equivalent SF-Pro-first stack to the dashboard's
  chart text for cross-surface consistency.
- Segment-risk heatmap recoloured to the shared blue palette, replacing a
  mismatched orange colormap left over from an earlier palette.

### Fixed
- Two charts rendered in-bar percentage labels underneath a dashed
  reference line (a z-order bug), making values such as "83%" and "97.0%"
  visually unreadable; corrected for both instances and as a general rule
  for any chart combining a reference line with in-bar labels.
- Replaced two hardcoded report figures (a euro value and four warehouse
  fill rates/inventory values) with values read from the already-loaded
  source data, so they can no longer silently drift from the dataset.
- Cover page and table-of-contents composition: the cover's content block
  is vertically centred instead of top-loaded, and the TOC no longer spills
  onto a near-empty page with an awkward mid-section split.

## [1.1.0] - 2026-06-18

### Added
- `pyproject.toml` centralising ruff (lint + format), mypy, pytest and coverage
  configuration, plus a pinned `requirements-dev.txt` toolchain.
- End-to-end, in-process pipeline integration test and orchestrator unit tests,
  raising measured coverage from 16% to ~98% with an enforced 95% floor.
- Subresource Integrity (SHA-384) hash on the published Plotly bundle, with a
  governance test that blocks publication if it is missing.
- Per-step pipeline timings persisted to `outputs/pipeline_timings.json` and
  uploaded as a CI artifact.
- CI stages for linting, format checking, type checking and dependency auditing.

### Changed
- Dashboard JSON serialisation now rounds floats to a fixed precision so the
  published `index.html` is byte-identical across Python and NumPy versions,
  fixing a reproducibility gap in the dashboard freshness check.
- `build_charts` exposes a `main()` entry point for in-process execution.

### Fixed
- Removed dead code and an ambiguous variable name; resolved all ruff and mypy
  findings across `src/` and `tests/`.

## [1.0.0] - 2026-06-11

### Added
- Initial public release: reproducible synthetic data generation, SQL analytical
  views, policy-based risk scoring, impact estimates, publication charts, PDF
  report, executive dashboard, and contract/SQL/analytical/publication gates.

[Unreleased]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v2.0.0
[1.2.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.2.0
[1.1.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.1.0
[1.0.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.0.0
