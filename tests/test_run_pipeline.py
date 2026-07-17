"""Unit tests for production pipeline wiring and subprocess execution."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from src import run_pipeline
from src.orchestration import PipelineExecutionError


def test_steps_cover_every_pipeline_stage_in_order() -> None:
    labels = [step.label for step in run_pipeline.STEPS]
    assert labels == [
        "src.ingestion",
        "src.storage --layer raw",
        "src.data_preparation",
        "src.scoring",
        "src.impact_analysis",
        "src.backtesting",
        "src.monte_carlo",
        "src.action_tracking",
        "src.causal_evaluation",
        "src.network_optimization",
        "src.storage --layer downstream",
        "src.data_contracts",
        "src.build_charts",
        "src.build_report",
        "src.executive_dashboard",
        "src.sql_quality_gate",
        "src.pre_delivery_validation",
        "src.ci_quality_gate",
    ]
    assert run_pipeline.STEPS[-1].dependencies == ("pre_delivery_validation",)
    assert run_pipeline.STEPS[3].outputs == (
        run_pipeline.PROJECT_ROOT / "data/processed/governance_priority_master.csv",
    )
    assert run_pipeline.STEPS[10].dependencies == (
        "impact_analysis",
        "monte_carlo",
        "action_tracking",
        "causal_evaluation",
        "network_optimization",
    )


def test_subprocess_runner_merges_stage_environment(monkeypatch) -> None:
    calls: list[tuple[list[str], dict]] = []

    def fake_run(cmd, check, cwd, env):
        calls.append((cmd, env))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_pipeline.subprocess, "run", fake_run)

    for stage in run_pipeline.STEPS:
        run_pipeline._run_stage(stage)

    invoked = [" ".join(cmd[2:]) for cmd, _ in calls]
    assert invoked == [step.label for step in run_pipeline.STEPS]

    # The chart step receives the matplotlib backend overrides merged over the
    # process environment; other steps receive the unmodified environment.
    env_by_script = {" ".join(cmd[2:]): env for cmd, env in calls}
    assert env_by_script["src.build_charts"]["MPLBACKEND"] == "Agg"
    assert (
        env_by_script["src.build_charts"]["XDG_CACHE_HOME"]
        == run_pipeline.PLOT_ENV["XDG_CACHE_HOME"]
    )
    assert env_by_script["src.scoring"].get("XDG_CACHE_HOME") == os.environ.get("XDG_CACHE_HOME")


def test_identity_paths_include_config_and_governed_external_exports(tmp_path) -> None:
    config = tmp_path / "pipeline.json"
    erp = tmp_path / "erp"
    wms = tmp_path / "wms"
    config.write_text("{}", encoding="utf-8")
    erp.mkdir()
    wms.mkdir()
    (erp / "products.csv").write_text("id\n1\n", encoding="utf-8")
    (wms / "inventory.csv").write_text("id\n1\n", encoding="utf-8")

    paths = run_pipeline._identity_paths(
        adapter_type="erp_wms",
        config_path=config,
        source_path=None,
        erp_path=str(erp),
        wms_path=str(wms),
    )

    assert paths == [config, erp / "products.csv", wms / "inventory.csv"]


def test_identity_paths_include_config_referenced_action_events(tmp_path) -> None:
    events = tmp_path / "events.json"
    events.write_text("[]", encoding="utf-8")
    config = tmp_path / "pipeline.json"
    config.write_text(
        '{"action_tracking": {"events_file": "' + events.as_posix() + '"}}',
        encoding="utf-8",
    )

    paths = run_pipeline._identity_paths(
        adapter_type="synthetic",
        config_path=config,
        source_path=None,
        erp_path=None,
        wms_path=None,
    )

    assert events in paths
    assert run_pipeline.PROJECT_ROOT / "src/data_generation.py" in paths
    assert run_pipeline.PROJECT_ROOT / "src/config.py" in paths


def test_main_delegates_to_governed_orchestrator_and_writes_legacy_timings(
    monkeypatch, tmp_path
) -> None:
    captured: dict = {}

    def fake_orchestrate(**kwargs):
        captured.update(kwargs)
        return {
            "status": "succeeded",
            "duration_seconds": 1.25,
            "stages": [{"stage": "ingestion", "command": "src.ingestion", "seconds": 0.5}],
        }

    monkeypatch.setattr(run_pipeline, "orchestrate", fake_orchestrate)
    monkeypatch.setattr(run_pipeline, "_build_store", lambda _: object())
    monkeypatch.setattr(run_pipeline, "default_asset_specs", lambda: [])
    monkeypatch.setattr(run_pipeline, "compute_run_id", lambda _, **__: "run-test")
    monkeypatch.setattr(run_pipeline, "TIMINGS_FILE", tmp_path / "timings.json")

    run_pipeline.main()

    assert captured["run_id"] == "run-test"
    assert captured["stages"] == run_pipeline.STEPS
    assert captured["runner"] is run_pipeline._run_stage
    assert run_pipeline.TIMINGS_FILE.exists()


def test_main_translates_pipeline_failure_to_system_exit(monkeypatch) -> None:
    def fake_orchestrate(**kwargs):
        raise PipelineExecutionError("contract failure")

    monkeypatch.setattr(run_pipeline, "orchestrate", fake_orchestrate)
    monkeypatch.setattr(run_pipeline, "_build_store", lambda _: object())
    monkeypatch.setattr(run_pipeline, "default_asset_specs", lambda: [])
    monkeypatch.setattr(run_pipeline, "compute_run_id", lambda _, **__: "run-test")

    with pytest.raises(SystemExit, match="contract failure"):
        run_pipeline.main()


def test_directory_identity_rejects_empty_or_missing_export(tmp_path) -> None:
    config = tmp_path / "pipeline.json"
    config.write_text("{}", encoding="utf-8")
    empty = tmp_path / "empty"
    empty.mkdir()

    with pytest.raises(ValueError, match="contains no files"):
        run_pipeline._identity_paths(
            adapter_type="directory",
            config_path=config,
            source_path=str(empty),
            erp_path=None,
            wms_path=None,
        )
    with pytest.raises(FileNotFoundError):
        run_pipeline._export_files(Path(tmp_path / "missing"))
