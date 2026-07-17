# Strategic Expansions Implementation Plan

> **Execution rule:** Apply each task test-first. Run the targeted test to confirm
> the expected failure, implement the narrow behavior, rerun it, then run the
> affected regression slice before proceeding.

**Goal:** Deliver governed source onboarding, credible causal evaluation,
constraint-based multi-echelon planning, and observable scheduled publication in
the existing reproducible pipeline without changing the visual design.

**Architecture:** Extend the current versioned configuration and canonical pipeline.
Each capability owns explicit contracts and outputs; the orchestration layer owns
execution metadata, lineage, and immutable publication. SciPy/HiGHS supplies the
deterministic MILP and statistical distributions; Boto3 supplies the optional S3
backend.

**Stack:** Python 3.12+, pandas, NumPy, DuckDB, SciPy, Boto3, pytest, Ruff, mypy.

---

## Task 1: Versioned strategic configuration

**Files:**

- Modify: `configs/pipeline.json`
- Modify: `src/settings.py`
- Modify: `tests/test_settings.py`

1. Add failing tests for version 2 parsing, defaults, environment overrides, and
   invalid SLA, causal, optimizer, retry, and object-store values.
2. Run `python -m pytest tests/test_settings.py -q` and confirm failures are due to
   missing version-2 settings.
3. Add frozen settings dataclasses and centralized validation without compatibility
   aliases or untyped dictionaries at call sites.
4. Rerun the targeted tests and `ruff check src/settings.py tests/test_settings.py`.

## Task 2: Source readiness contracts

**Files:**

- Create: `src/source_readiness.py`
- Create: `tests/test_source_readiness.py`
- Modify: `src/ingestion.py`
- Modify: `tests/test_ingestion.py`
- Modify: `configs/table_contracts.json`

1. Add failing tests for stable fingerprints, additive and breaking drift,
   mapped-column comparison, fresh/stale watermarks, synthetic as-of handling, and
   fail-before-overwrite behavior.
2. Run targeted tests and confirm the source-readiness API is absent.
3. Implement schema normalization/fingerprinting, drift classification, freshness
   evaluation, and readiness aggregation with deterministic output ordering.
4. Integrate the gate into external and synthetic ingestion before canonical
   publication. Preserve the last successful registry on a failed run.
5. Add the three readiness output contracts and regression tests for manifests.
6. Run `python -m pytest tests/test_source_readiness.py tests/test_ingestion.py -q`.

## Task 3: Synthetic intervention and network inputs

**Files:**

- Modify: `src/data_generation.py`
- Modify: `tests/test_data_generation.py`
- Modify: `src/warehouse.py`
- Modify: `src/storage.py`
- Modify: `configs/table_contracts.json`

1. Add failing tests for deterministic stratified assignment, pre-outcome assignment,
   treatment/control support, supplier recovery timing, lane uniqueness, sourcing
   eligibility, and capacity/unit validity.
2. Add canonical `intervention_assignments`, `network_nodes`, `network_lanes`, and
   `product_sources` tables to the seeded generator and raw-table registry.
3. Embed the policy-pilot and supplier-recovery mechanisms inside the operational
   simulation while preserving demand/inventory conservation.
4. Add schema-defined DuckDB tables, relationships, storage specs, and contracts.
5. Run generator, contract, warehouse, and storage test slices.

## Task 4: Causal estimators and validity gates

**Files:**

- Create: `src/causal_evaluation.py`
- Create: `tests/test_causal_evaluation.py`
- Modify: `configs/table_contracts.json`

1. Add failing tests for RCT effect recovery, no-effect inference, duplicate or
   post-outcome assignment rejection, minimum support, DiD effect recovery,
   differential pre-trend rejection, placebo diagnostics, and deterministic output.
2. Implement cohort panel preparation with explicit unit, treatment, outcome, and
   common-window validation.
3. Implement cluster-level RCT change estimates and deterministic randomization
   inference.
4. Implement DiD change estimates, cluster uncertainty, pre-trend, and placebo
   diagnostics.
5. Add evidence-status logic that separates statistical results, economic impact,
   and allowed attribution language.
6. Materialize estimates, diagnostics, and cohort timeseries; add contracts.
7. Run `python -m pytest tests/test_causal_evaluation.py -q` and the action-tracking
   regression slice.

## Task 5: Multi-echelon MILP

**Files:**

- Create: `src/network_optimization.py`
- Create: `tests/test_network_optimization.py`
- Modify: `configs/table_contracts.json`

1. Add failing small-network tests that prove flow conservation, MOQ activation,
   sourcing eligibility, lane/source/warehouse capacity, service constraints,
   cost-optimal routing, integrality, and infeasibility handling.
2. Build validated model inputs from canonical network tables and lagged demand.
3. Create sparse objective, bounds, integrality vector, and linear constraints for
   SciPy `milp`.
4. Reject non-optimal solver states and validate the returned solution independently
   before materialization.
5. Produce plan, flow, utilization, and solver-summary tables with stable ordering.
6. Add contracts and run the targeted optimizer tests.

## Task 6: Object-store contract

**Files:**

- Create: `src/object_store.py`
- Create: `tests/test_object_store.py`
- Modify: `requirements.txt`
- Modify: `requirements-dev.txt`

1. Add failing tests for content-addressed local writes, overwrite rejection, hash-
   verified reads, atomic pointer promotion, prefix isolation, and S3 request metadata.
2. Implement an `ObjectStore` protocol and immutable object descriptor.
3. Implement `LocalObjectStore` with atomic filesystem replacement and SHA-256
   verification.
4. Implement `S3ObjectStore` using injected Boto3 clients, conditional writes where
   supported, checksums, server-side encryption, and no credential serialization.
5. Pin compatible SciPy and Boto3 releases; install and run dependency audit.
6. Run object-store tests and mypy for the new module.

## Task 7: Catalog and lineage

**Files:**

- Create: `src/data_catalog.py`
- Create: `tests/test_data_catalog.py`
- Modify: `configs/table_contracts.json`

1. Add failing tests for asset profiling, schema/content hashes, watermark extraction,
   missing producers/parents, cycles, deterministic lineage, and manifest coverage.
2. Implement typed asset and lineage declarations plus graph validation.
3. Profile final stage outputs and materialize `data_catalog.csv` and
   `data_lineage.csv`.
4. Add contracts and verify every pipeline-produced table has one producer.

## Task 8: Observable orchestration

**Files:**

- Create: `src/orchestration.py`
- Create: `tests/test_orchestration.py`
- Modify: `src/run_pipeline.py`
- Modify: `tests/test_run_pipeline.py`
- Modify: `src/__main__.py`

1. Add failing tests for dependency order, stable run IDs, stage output ownership,
   transient retry bounds, non-retryable failures, event sequencing, failed-run
   summaries, idempotent publication, and latest-pointer promotion only after gates.
2. Implement typed stage definitions and topological validation.
3. Implement JSONL event emission and final JSON run summaries without logging
   secrets or environment dumps.
4. Implement retry classification and bounded exponential backoff with injected
   sleep for tests.
5. Add catalog generation and object publication as terminal stages.
6. Keep `run_pipeline.py` as the compatibility entry point over the new orchestrator.
7. Run orchestration and existing runner tests.

## Task 9: Pipeline, contracts, and release gates

**Files:**

- Modify: `src/run_pipeline.py`
- Modify: `src/pre_delivery_validation.py`
- Modify: `src/ci_quality_gate.py`
- Modify: `src/data_contracts.py`
- Modify: `src/storage.py`
- Modify: `tests/test_pipeline_integration.py`
- Add or modify focused gate tests as required

1. Add failing integration assertions for all new stages and outputs.
2. Insert source readiness before canonical persistence, causal evaluation after
   action tracking, optimization after policy analytics, and catalog/publication
   after downstream validation.
3. Expand storage and contract coverage to every new persistent table.
4. Add release checks for source readiness, causal claim discipline, MILP feasibility,
   lineage completeness, telemetry outcome, and object-manifest integrity.
5. Run the full integration test and affected release-gate tests.

## Task 10: Scheduled execution and CI evidence

**Files:**

- Create: `.github/workflows/scheduled-analytics.yml`
- Modify: `.github/workflows/analytics-ci.yml`
- Modify: `.github/dependabot.yml`

1. Add a weekly and manual scheduled workflow that calls the canonical pipeline once,
   uses least-privilege permissions, and uploads telemetry/catalog evidence.
2. Keep pull-request CI unscheduled and retain the Python 3.12-3.14 quality matrix.
3. Add new operational artefacts to CI uploads without weakening tracked-publication
   freshness checks.
4. Validate workflow syntax, action pinning, timeouts, and concurrency behavior.

## Task 11: Production and portfolio documentation

**Files:**

- Modify: `README.md`
- Modify: `docs/data_model.md`
- Modify: `docs/methodology.md`
- Modify: `docs/metric_dictionary.md`
- Modify: `docs/advanced_analytics.md`
- Modify: `docs/release_governance.md`
- Modify: `SECURITY.md`
- Modify: `CHANGELOG.md`
- Modify: `Makefile`

1. Document the production data path, causal estimands and caveats, MILP formulation,
   operator commands, local/S3 deployment, failure semantics, lineage, and telemetry.
2. Replace obsolete statements that causal evaluation or multi-echelon capability is
   out of scope while preserving honest limitations.
3. Add concise run targets for tests, scheduled-mode dry runs, and focused capabilities.
4. Search for contradictory, generic, repetitive, or unsupported claims and remove
   them.

## Task 12: Complete verification and requirement audit

1. Run targeted red-green records throughout implementation.
2. Run `ruff check src tests`.
3. Run `ruff format --check src tests`.
4. Run `mypy`.
5. Run `python -m compileall -q src tests`.
6. Run `python -m pytest` and confirm coverage remains at or above 95%.
7. Run `python -m src` from the integrated final state.
8. Run `python -m src.sql_quality_gate`, `python -m src.pre_delivery_validation`, and
   `python -m src.ci_quality_gate` independently.
9. Run `python -m pip check` and `pip-audit -r requirements-dev.txt`.
10. Compare dashboard/template/graph/report visual hashes or rendered output against
    the approved baseline and investigate every unexpected change.
11. Re-read the approved design and this plan; map each requirement to direct code,
    test, output, and runtime evidence before declaring completion.
