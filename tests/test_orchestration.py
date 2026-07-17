"""Dependency-aware execution, telemetry, retries, and guarded publication."""

from __future__ import annotations

import json

import pytest
from src.data_catalog import AssetSpec
from src.object_store import LocalObjectStore
from src.orchestration import (
    PipelineExecutionError,
    PipelineStage,
    TransientStageError,
    compute_run_id,
    orchestrate,
    topological_order,
)
from src.settings import ObjectStoreSettings, OrchestrationSettings


def _settings(tmp_path, *, max_retries: int = 2) -> OrchestrationSettings:
    return OrchestrationSettings(
        max_retries=max_retries,
        retry_base_seconds=0.25,
        object_store=ObjectStoreSettings(
            backend="local",
            local_root=str(tmp_path / "objects"),
            s3_bucket=None,
            s3_prefix="runs",
            s3_endpoint_url=None,
            s3_region="eu-west-1",
            s3_server_side_encryption="AES256",
        ),
    )


def test_topological_order_is_stable_and_rejects_invalid_graphs() -> None:
    stages = [
        PipelineStage("publish", "publish", dependencies=("transform",)),
        PipelineStage("ingest", "ingest"),
        PipelineStage("transform", "transform", dependencies=("ingest",)),
    ]

    assert [stage.name for stage in topological_order(stages)] == [
        "ingest",
        "transform",
        "publish",
    ]
    with pytest.raises(ValueError, match="unknown dependencies"):
        topological_order([PipelineStage("a", "a", dependencies=("missing",))])
    with pytest.raises(ValueError, match="cycle"):
        topological_order(
            [
                PipelineStage("a", "a", dependencies=("b",)),
                PipelineStage("b", "b", dependencies=("a",)),
            ]
        )


def test_run_id_is_content_derived_and_order_independent(tmp_path) -> None:
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("one", encoding="utf-8")
    second.write_text("two", encoding="utf-8")

    run_id = compute_run_id([first, second])
    assert run_id == compute_run_id([second, first])
    first.write_text("changed", encoding="utf-8")
    assert compute_run_id([first, second]) != run_id


def test_run_id_does_not_depend_on_checkout_location(tmp_path) -> None:
    first_root = tmp_path / "checkout-a"
    second_root = tmp_path / "checkout-b"
    first_root.mkdir()
    second_root.mkdir()
    for root in (first_root, second_root):
        (root / "pipeline.json").write_text('{"version": 2}', encoding="utf-8")
        (root / "products.csv").write_text("id\n1\n", encoding="utf-8")

    assert compute_run_id(list(first_root.iterdir())) == compute_run_id(list(second_root.iterdir()))


def test_run_id_includes_effective_non_file_context(tmp_path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")

    first = compute_run_id(
        [source], context={"adapter_type": "synthetic", "as_of_date": "2025-12-31"}
    )
    same = compute_run_id(
        [source], context={"as_of_date": "2025-12-31", "adapter_type": "synthetic"}
    )
    changed = compute_run_id(
        [source], context={"adapter_type": "synthetic", "as_of_date": "2026-01-01"}
    )

    assert first == same
    assert first != changed


def test_orchestration_retries_only_transient_failures_and_publishes_after_gates(
    tmp_path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    output = tmp_path / "stage-output.txt"
    attempts = 0
    delays: list[float] = []

    def runner(stage: PipelineStage) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TransientStageError("object service unavailable")
        output.write_text(stage.name, encoding="utf-8")

    summary = orchestrate(
        stages=[PipelineStage("quality_gate", "quality", outputs=(output,))],
        runner=runner,
        settings=_settings(tmp_path),
        run_id="run-success",
        catalog_specs=[AssetSpec("source", source, "source", "ingestion")],
        store=LocalObjectStore(tmp_path / "objects"),
        output_dir=tmp_path,
        event_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
        sleep=delays.append,
    )

    assert attempts == 3
    assert delays == [0.25, 0.5]
    assert summary["status"] == "succeeded"
    assert summary["published_asset_count"] == 3
    events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert [event["event_type"] for event in events].count("stage_retry") == 2
    assert events[-1]["event_type"] == "pipeline_succeeded"
    assert (tmp_path / "objects/pointers/latest.json").exists()


def test_non_transient_failure_is_not_retried_or_published(tmp_path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    attempts = 0

    def runner(stage: PipelineStage) -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError(f"contract failure in {stage.name}")

    with pytest.raises(PipelineExecutionError, match="contract failure"):
        orchestrate(
            stages=[PipelineStage("contract_gate", "contract")],
            runner=runner,
            settings=_settings(tmp_path),
            run_id="run-failed",
            catalog_specs=[AssetSpec("source", source, "source", "ingestion")],
            store=LocalObjectStore(tmp_path / "objects"),
            output_dir=tmp_path,
            event_path=tmp_path / "events.jsonl",
            summary_path=tmp_path / "summary.json",
            sleep=lambda _: None,
        )

    assert attempts == 1
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["status"] == "failed"
    assert summary["failed_stage"] == "contract_gate"
    assert not (tmp_path / "objects/pointers/latest.json").exists()


def test_missing_owned_output_fails_stage(tmp_path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(PipelineExecutionError, match="did not produce owned outputs"):
        orchestrate(
            stages=[PipelineStage("stage", "module", outputs=(tmp_path / "missing.csv",))],
            runner=lambda _: None,
            settings=_settings(tmp_path),
            run_id="run-missing",
            catalog_specs=[AssetSpec("source", source, "source", "ingestion")],
            store=LocalObjectStore(tmp_path / "objects"),
            output_dir=tmp_path,
            event_path=tmp_path / "events.jsonl",
            summary_path=tmp_path / "summary.json",
            sleep=lambda _: None,
        )


def test_publication_retries_transient_pointer_failure_idempotently(tmp_path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    delegate = LocalObjectStore(tmp_path / "objects")

    class FlakyStore:
        attempts = 0

        def put_immutable(self, key, payload, *, content_type):
            return delegate.put_immutable(key, payload, content_type=content_type)

        def read_verified(self, key, expected_sha256):
            return delegate.read_verified(key, expected_sha256)

        def promote_pointer(self, key, payload):
            self.attempts += 1
            if self.attempts == 1:
                raise TransientStageError("temporary object-store outage")
            return delegate.promote_pointer(key, payload)

    delays: list[float] = []
    summary = orchestrate(
        stages=[PipelineStage("gate", "gate")],
        runner=lambda _: None,
        settings=_settings(tmp_path),
        run_id="run-publication-retry",
        catalog_specs=[AssetSpec("source", source, "source", "ingestion")],
        store=FlakyStore(),
        output_dir=tmp_path,
        event_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
        sleep=delays.append,
    )

    events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert summary["status"] == "succeeded"
    assert delays == [0.25]
    assert [event["event_type"] for event in events].count("publication_retry") == 1
