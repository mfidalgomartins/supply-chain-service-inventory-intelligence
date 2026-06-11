from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from src.config import PROJECT_ROOT
except ModuleNotFoundError:
    from config import PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_DASHBOARD = PROJECT_ROOT / "index.html"
OUTPUT_REPORT = PROJECT_ROOT / "outputs" / "reports" / "service_inventory_intelligence_report.pdf"
OUTPUT_GRAPHS_DIR = PROJECT_ROOT / "outputs" / "graphs"
REQUIRED_GRAPH_COUNT = 14
REQUIRED_FILES = [
    PROJECT_ROOT / "LICENSE",
    PROJECT_ROOT / "README.md",
    PROJECT_ROOT / "docs" / "methodology.md",
    PROJECT_ROOT / "docs" / "metric_dictionary.md",
    PROJECT_ROOT / "docs" / "scoring_framework.md",
    PROJECT_ROOT / "docs" / "data_model.md",
    PROJECT_ROOT / "docs" / "release_governance.md",
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

    publication_graphs = sorted(OUTPUT_GRAPHS_DIR.glob("*.png"))
    _require_exists(REQUIRED_FILES + [OUTPUT_DASHBOARD, OUTPUT_REPORT])

    print("CI quality gate summary:")
    print(f"- Pre-delivery checks: {len(pre_delivery)} total, {pre_fail} FAIL, {pre_warn} WARN")
    print(f"- SQL checks: {len(sql_checks)} total, {sql_fail} non-pass")
    print(f"- Release classification: {release_classification}")
    print(f"- Publication graphs: {len(publication_graphs)}")

    if (
        pre_fail > 0
        or sql_fail > 0
        or publish_blocked
        or len(publication_graphs) != REQUIRED_GRAPH_COUNT
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    run_ci_quality_gate()
