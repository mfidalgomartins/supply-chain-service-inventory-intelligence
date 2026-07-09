# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- Rewrote the analytical report's prose end to end, removing a repeated
  antithesis pattern ("X, not Y") used throughout, including verbatim
  duplicate phrasings, in favour of varied, evidence-led sentences. The
  recovery-rate assumptions behind the headline value pool (35% margin, 25%
  working capital) are now stated explicitly in the text, with the correct
  sensitivity relationship, instead of only appearing in a data table.
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

[1.2.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.2.0
[1.1.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.1.0
[1.0.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.0.0
