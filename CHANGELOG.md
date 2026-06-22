# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.1.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.1.0
[1.0.0]: https://github.com/mfidalgomartins/supply-chain-service-inventory-intelligence/releases/tag/v1.0.0
