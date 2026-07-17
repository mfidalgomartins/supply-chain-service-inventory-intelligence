# Contributing

## Local Validation

Use Python 3.12 or newer and run the complete publication pipeline before
opening a pull request:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/mypy
.venv/bin/pip-audit -r requirements-dev.txt
.venv/bin/python -m src
.venv/bin/python -m pytest
git diff --check
```

Or run the same steps via `make setup && make check`.

These are the same gates CI enforces. `pytest` fails below 95% line coverage.

Optionally, install the local pre-commit hooks (ruff check, ruff format, mypy)
so formatting and lint issues are caught before you commit:

```bash
make pre-commit-install
```

The pipeline regenerates ignored CSV and Parquet layers and refreshes the tracked
publication artefacts: `index.html`, `outputs/graphs/`, and
`outputs/reports/service_inventory_intelligence_report.pdf`.

## Change Expectations

- Keep metric definitions aligned with `docs/metric_dictionary.md`.
- Add focused tests for changes to scoring, impact, contracts, or release gates.
- Keep `configs/pipeline.json` versioned and validate every new parameter.
- Keep all canonical source policies owned, freshness-governed, and schema-versioned.
- Preserve temporal separation in backtests and inventory conservation in simulators.
- Do not label backtests, scenarios, or unregistered pre/post outputs as causal effects.
- Add independent feasibility assertions for every new optimization constraint.
- Preserve the explicit distinction between observed values, proxy estimates,
  and causal claims.
- Use conventional commits in the form `type(scope): imperative description`.
- Record user-facing changes in `CHANGELOG.md`.
