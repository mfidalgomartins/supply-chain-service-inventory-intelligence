from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


PLOT_ENV = {
    "MPLBACKEND": "Agg",
    "MPLCONFIGDIR": str(PROJECT_ROOT / ".cache" / "matplotlib"),
    "XDG_CACHE_HOME": str(PROJECT_ROOT / ".cache"),
}


STEPS: list[tuple[str, dict[str, str]]] = [
    ("data_generation.py", {}),
    ("data_preparation.py", {}),
    ("feature_engineering.py", {}),
    ("data_contracts.py", {}),
    ("scoring.py", {}),
    ("kpi_diagnostic_analysis.py", {}),
    ("impact_analysis.py", PLOT_ENV),
    ("visualization.py", PLOT_ENV),
    ("executive_dashboard.py", {}),
    ("sql_quality_gate.py", {}),
    ("pre_delivery_validation.py", {}),
    ("ci_quality_gate.py", {}),
]


def _run_step(script_name: str, extra_env: dict[str, str]) -> tuple[str, float]:
    script_path = SRC_DIR / script_name
    print(f"\n[PIPELINE] Running {script_name}")

    env = os.environ.copy()
    env.update(extra_env)

    start = time.perf_counter()
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=PROJECT_ROOT, env=env)
    duration_sec = time.perf_counter() - start

    return "PASS", duration_sec


def _write_run_log(run_log: pd.DataFrame) -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    run_log.to_csv(OUTPUT_TABLES_DIR / "pipeline_run_log.csv", index=False)


def main() -> None:
    (PROJECT_ROOT / ".cache" / "matplotlib").mkdir(parents=True, exist_ok=True)

    run_rows: list[dict] = []

    for script_name, step_env in STEPS:
        started = datetime.now(timezone.utc)
        status = "PASS"
        duration_sec = 0.0
        error_text = ""

        try:
            status, duration_sec = _run_step(script_name, step_env)
        except subprocess.CalledProcessError as exc:
            status = "FAIL"
            duration_sec = float(getattr(exc, "duration", 0.0))
            error_text = f"Command failed for {script_name} with exit code {exc.returncode}"
        finally:
            run_rows.append(
                {
                    "run_timestamp_utc": started.strftime("%Y-%m-%d %H:%M:%S"),
                    "script_name": script_name,
                    "status": status,
                    "duration_sec": round(duration_sec, 4),
                    "error_text": error_text,
                }
            )

        if status == "FAIL":
            break

    run_log = pd.DataFrame(run_rows)
    _write_run_log(run_log)

    failed = run_log[run_log["status"] == "FAIL"]
    if not failed.empty:
        msg = failed.iloc[0]["error_text"] or "Pipeline failed"
        raise SystemExit(msg)

    print("\nPipeline finished successfully.")


if __name__ == "__main__":
    main()
