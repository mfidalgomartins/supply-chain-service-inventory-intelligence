"""Dependency-aware pipeline execution with retries, telemetry, and publication."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from src.data_catalog import (
    AssetSpec,
    materialize_catalog,
    publish_catalogued_run,
)
from src.object_store import ObjectStore, TransientObjectStoreError
from src.settings import OrchestrationSettings


class TransientStageError(RuntimeError):
    """Explicitly retryable I/O or service failure."""


class PipelineExecutionError(RuntimeError):
    """Raised after failed-run telemetry and summary have been persisted."""


@dataclass(frozen=True)
class PipelineStage:
    name: str
    module: str
    args: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    outputs: tuple[Path, ...] = ()
    env: dict[str, str] | None = None

    @property
    def label(self) -> str:
        return " ".join((self.module, *self.args))


def topological_order(stages: list[PipelineStage]) -> list[PipelineStage]:
    names = [stage.name for stage in stages]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError("Pipeline stages have duplicate names: " + ", ".join(duplicates))
    by_name = {stage.name: stage for stage in stages}
    for stage in stages:
        missing = sorted(set(stage.dependencies) - set(by_name))
        if missing:
            raise ValueError(f"Stage {stage.name} has unknown dependencies: {', '.join(missing)}")

    ordered: list[PipelineStage] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError(f"Pipeline dependency graph contains a cycle through {name}")
        if name in visited:
            return
        visiting.add(name)
        stage = by_name[name]
        for dependency in stage.dependencies:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)
        ordered.append(stage)

    for stage in stages:
        visit(stage.name)
    return ordered


def compute_run_id(paths: list[Path], *, context: dict[str, object] | None = None) -> str:
    """Derive an idempotent run identifier from governed input bytes."""
    if not paths:
        raise ValueError("At least one identity path is required")
    digest = hashlib.sha256()
    if context:
        digest.update(b"effective-context\0")
        digest.update(json.dumps(context, sort_keys=True, separators=(",", ":")).encode())
        digest.update(b"\0")
    identities: list[tuple[str, int, bytes]] = []
    for path in (item.resolve() for item in paths):
        if not path.is_file():
            raise FileNotFoundError(f"Run identity input is missing: {path}")
        payload = path.read_bytes()
        identities.append((path.name, len(payload), hashlib.sha256(payload).digest()))
    for name, size, content_digest in sorted(identities):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(str(size).encode())
        digest.update(b"\0")
        digest.update(content_digest)
    return f"run-{digest.hexdigest()[:20]}"


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def orchestrate(
    *,
    stages: list[PipelineStage],
    runner: Callable[[PipelineStage], None],
    settings: OrchestrationSettings,
    run_id: str,
    catalog_specs: list[AssetSpec],
    store: ObjectStore,
    output_dir: Path,
    event_path: Path,
    summary_path: Path,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Execute all gates and promote the object-store run only after success."""
    ordered = topological_order(stages)
    if not run_id.strip():
        raise ValueError("run_id must not be empty")
    event_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.write_text("", encoding="utf-8")
    sequence = 0

    def emit(event_type: str, **fields: object) -> None:
        nonlocal sequence
        sequence += 1
        event = {
            "sequence": sequence,
            "timestamp": _timestamp(),
            "run_id": run_id,
            "event_type": event_type,
            **fields,
        }
        with event_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")

    pipeline_started_at = _timestamp()
    pipeline_start = time.perf_counter()
    completed: list[str] = []
    timings: list[dict] = []
    emit("pipeline_started", stage_count=len(ordered))

    try:
        for stage in ordered:
            stage_start = time.perf_counter()
            attempt = 0
            while True:
                attempt += 1
                emit("stage_started", stage=stage.name, attempt=attempt, command=stage.label)
                try:
                    runner(stage)
                    missing_outputs = [
                        path.as_posix() for path in stage.outputs if not path.exists()
                    ]
                    if missing_outputs:
                        raise ValueError(
                            f"Stage {stage.name} did not produce owned outputs: "
                            + ", ".join(missing_outputs)
                        )
                except TransientStageError as exc:
                    if attempt > settings.max_retries:
                        raise
                    delay = settings.retry_base_seconds * (2 ** (attempt - 1))
                    emit(
                        "stage_retry",
                        stage=stage.name,
                        attempt=attempt,
                        delay_seconds=delay,
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                    sleep(delay)
                    continue
                duration = time.perf_counter() - stage_start
                timings.append(
                    {"stage": stage.name, "command": stage.label, "seconds": round(duration, 6)}
                )
                completed.append(stage.name)
                emit(
                    "stage_succeeded",
                    stage=stage.name,
                    attempt=attempt,
                    duration_seconds=round(duration, 6),
                )
                break

        catalog, lineage = materialize_catalog(catalog_specs, run_id=run_id, output_dir=output_dir)
        publication_attempt = 0
        while True:
            publication_attempt += 1
            emit("publication_started", attempt=publication_attempt)
            try:
                publication_manifest, manifest_descriptor = publish_catalogued_run(
                    catalog,
                    lineage,
                    store=store,
                    run_id=run_id,
                    output_dir=output_dir,
                )
            except (TransientStageError, TransientObjectStoreError) as exc:
                if publication_attempt > settings.max_retries:
                    raise
                delay = settings.retry_base_seconds * (2 ** (publication_attempt - 1))
                emit(
                    "publication_retry",
                    attempt=publication_attempt,
                    delay_seconds=delay,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                sleep(delay)
                continue
            break
        emit(
            "publication_succeeded",
            attempt=publication_attempt,
            asset_count=len(publication_manifest),
            manifest_object_key=manifest_descriptor.key,
            manifest_sha256=manifest_descriptor.sha256,
        )
    except Exception as exc:
        failed_stage = (
            ordered[len(completed)].name if len(completed) < len(ordered) else "publication"
        )
        duration = time.perf_counter() - pipeline_start
        emit(
            "pipeline_failed",
            failed_stage=failed_stage,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        summary = {
            "run_id": run_id,
            "status": "failed",
            "started_at": pipeline_started_at,
            "finished_at": _timestamp(),
            "duration_seconds": round(duration, 6),
            "stage_count": len(ordered),
            "completed_stage_count": len(completed),
            "failed_stage": failed_stage,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "stages": timings,
        }
        _write_json(summary_path, summary)
        raise PipelineExecutionError(str(exc)) from exc

    duration = time.perf_counter() - pipeline_start
    summary = {
        "run_id": run_id,
        "status": "succeeded",
        "started_at": pipeline_started_at,
        "finished_at": _timestamp(),
        "duration_seconds": round(duration, 6),
        "stage_count": len(ordered),
        "completed_stage_count": len(completed),
        "failed_stage": None,
        "published_asset_count": len(publication_manifest),
        "manifest_object_key": manifest_descriptor.key,
        "manifest_sha256": manifest_descriptor.sha256,
        "stages": timings,
    }
    _write_json(summary_path, summary)
    emit(
        "pipeline_succeeded",
        duration_seconds=round(duration, 6),
        published_asset_count=len(publication_manifest),
    )
    return summary
