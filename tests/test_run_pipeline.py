"""Unit tests for the subprocess pipeline orchestrator.

These mock ``subprocess.run`` so the orchestration logic (step ordering, env
propagation, failure handling) is verified without re-executing the heavy
end-to-end pipeline, which is covered separately by the integration test.
"""

from __future__ import annotations

import os
import subprocess

import pytest
from src import run_pipeline


def test_steps_cover_every_pipeline_stage_in_order() -> None:
    scripts = [name for name, _ in run_pipeline.STEPS]
    assert scripts == [
        "data_generation.py",
        "data_preparation.py",
        "scoring.py",
        "data_contracts.py",
        "impact_analysis.py",
        "build_charts.py",
        "build_report.py",
        "executive_dashboard.py",
        "sql_quality_gate.py",
        "pre_delivery_validation.py",
        "ci_quality_gate.py",
    ]


def test_main_runs_each_step_once_with_merged_env(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    def fake_run(cmd, check, cwd, env):
        calls.append((cmd[-1], env))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_pipeline.subprocess, "run", fake_run)

    run_pipeline.main()

    invoked = [pathlib_tail(script_path) for script_path, _ in calls]
    assert invoked == [name for name, _ in run_pipeline.STEPS]

    # The chart step receives the matplotlib backend overrides merged over the
    # process environment; other steps receive the unmodified environment.
    env_by_script = {pathlib_tail(s): e for s, e in calls}
    assert env_by_script["build_charts.py"]["MPLBACKEND"] == "Agg"
    assert (
        env_by_script["build_charts.py"]["XDG_CACHE_HOME"]
        == run_pipeline.PLOT_ENV["XDG_CACHE_HOME"]
    )
    assert env_by_script["scoring.py"].get("XDG_CACHE_HOME") == os.environ.get("XDG_CACHE_HOME")


def test_main_raises_system_exit_when_a_step_fails(monkeypatch) -> None:
    def fake_run(cmd, check, cwd, env):
        raise subprocess.CalledProcessError(returncode=2, cmd=cmd)

    monkeypatch.setattr(run_pipeline.subprocess, "run", fake_run)

    with pytest.raises(SystemExit) as excinfo:
        run_pipeline.main()
    assert "data_generation.py" in str(excinfo.value)


def pathlib_tail(path) -> str:
    return str(path).rsplit("/", 1)[-1]
