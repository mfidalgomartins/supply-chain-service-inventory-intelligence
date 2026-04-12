from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from src.config import PROJECT_ROOT
except ModuleNotFoundError:
    from config import PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_DASHBOARD = PROJECT_ROOT / "outputs" / "dashboard" / "index.html"
REQUIRED_FILES = [
    PROJECT_ROOT / "docs" / "validation_report.md",
    PROJECT_ROOT / "docs" / "methodology.md",
    PROJECT_ROOT / "docs" / "metric_dictionary.md",
    PROJECT_ROOT / "docs" / "scoring_framework.md",
    PROJECT_ROOT / "docs" / "data_model.md",
    PROJECT_ROOT / "docs" / "release_governance.md",
    PROJECT_ROOT / "outputs" / "reports" / "executive_kpi_diagnostic_analysis.md",
    PROJECT_ROOT / "outputs" / "reports" / "executive_summary.md",
    PROJECT_ROOT / "outputs" / "reports" / "data_contracts_summary.md",
    PROJECT_ROOT / "outputs" / "reports" / "release_readiness.md",
]


def _require_exists(paths: list[Path]) -> None:
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        print("Missing required release artifacts:")
        for p in missing:
            print("-", p)
        raise SystemExit(1)


def run_ci_quality_gate() -> None:
    pre_delivery = pd.read_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_checks.csv")
    sql_checks = pd.read_csv(OUTPUT_TABLES_DIR / "ci_sql_validation_checks.csv")
    release_matrix = pd.read_csv(OUTPUT_TABLES_DIR / "validation_release_state_matrix.csv")

    pre_fail = int((pre_delivery["status"] == "FAIL").sum())
    pre_warn = int((pre_delivery["status"] == "WARN").sum())
    sql_fail = int((sql_checks["status"] != "PASS").sum())
    release_classification = str(release_matrix["release_classification"].iloc[0]) if not release_matrix.empty else "publish-blocked"
    publish_blocked = release_classification == "publish-blocked"

    _require_exists(REQUIRED_FILES + [OUTPUT_DASHBOARD])

    print("CI quality gate summary:")
    print(f"- Pre-delivery checks: {len(pre_delivery)} total, {pre_fail} FAIL, {pre_warn} WARN")
    print(f"- SQL checks: {len(sql_checks)} total, {sql_fail} non-pass")
    print(f"- Release classification: {release_classification}")

    if pre_fail > 0 or sql_fail > 0 or publish_blocked:
        raise SystemExit(1)


if __name__ == "__main__":
    run_ci_quality_gate()
