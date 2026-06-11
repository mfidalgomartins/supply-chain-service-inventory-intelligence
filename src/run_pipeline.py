from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

PLOT_ENV = {
    "MPLBACKEND": "Agg",
    "MPLCONFIGDIR": str(PROJECT_ROOT / ".cache" / "matplotlib"),
    "XDG_CACHE_HOME": str(PROJECT_ROOT / ".cache"),
}

STEPS: list[tuple[str, dict[str, str]]] = [
    ("data_generation.py", {}),
    ("data_preparation.py", {}),
    ("scoring.py", {}),
    ("data_contracts.py", {}),
    ("impact_analysis.py", {}),
    ("build_charts.py", PLOT_ENV),
    ("build_report.py", {}),
    ("executive_dashboard.py", {}),
    ("sql_quality_gate.py", {}),
    ("pre_delivery_validation.py", {}),
    ("ci_quality_gate.py", {}),
]


def _run_step(script_name: str, extra_env: dict[str, str]) -> float:
    script_path = SRC_DIR / script_name
    print(f"\n[PIPELINE] Running {script_name}")

    env = os.environ.copy()
    env.update(extra_env)

    start = time.perf_counter()
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=PROJECT_ROOT, env=env)
    duration_sec = time.perf_counter() - start

    print(f"[PIPELINE] Finished {script_name} in {duration_sec:.2f}s")
    return duration_sec


def main() -> None:
    (PROJECT_ROOT / ".cache" / "matplotlib").mkdir(parents=True, exist_ok=True)

    for script_name, step_env in STEPS:
        try:
            _run_step(script_name, step_env)
        except subprocess.CalledProcessError as exc:
            raise SystemExit(
                f"Command failed for {script_name} with exit code {exc.returncode}"
            ) from exc

    print("\nPipeline finished successfully.")


if __name__ == "__main__":
    main()
