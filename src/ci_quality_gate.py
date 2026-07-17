"""Final CI gate: reads the pre-delivery and SQL check outputs and fails the
build on any check failure, publish-blocked release state, wrong publication
chart count, or missing release artefact (dashboard, report, docs)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from PIL import Image
from pypdf import PdfReader
from pypdf.errors import PdfReadError

from src.config import PROJECT_ROOT

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_DASHBOARD = PROJECT_ROOT / "index.html"
OUTPUT_REPORT = PROJECT_ROOT / "outputs" / "reports" / "service_inventory_intelligence_report.pdf"
OUTPUT_GRAPHS_DIR = PROJECT_ROOT / "outputs" / "graphs"
REQUIRED_GRAPH_FILES = (
    "01_opportunity_by_category.png",
    "02_service_level_trend.png",
    "03_warehouse_service_inventory.png",
    "04_supplier_reliability.png",
    "05_top_sku_opportunity.png",
    "06_stockout_lost_sales_trend.png",
    "07_lost_sales_concentration.png",
    "08_abc_class_cohort.png",
    "09_days_of_supply_distribution.png",
    "10_supplier_ontime_ranking.png",
    "11_opportunity_bridge.png",
    "12_segment_risk_heatmap.png",
    "13_fill_rate_by_category.png",
    "14_before_after_fill_rate.png",
)
REQUIRED_GRAPH_COUNT = len(REQUIRED_GRAPH_FILES)
REPORT_PAGE_COUNT_RANGE = range(24, 34)
REPORT_ANCHOR_PHRASES = (
    "Executive summary",
    "Analytical framework",
    "Findings",
    "Recommendations and action priorities",
    "Appendix",
)
REQUIRED_FILES = [
    PROJECT_ROOT / "LICENSE",
    PROJECT_ROOT / "README.md",
    PROJECT_ROOT / "docs" / "methodology.md",
    PROJECT_ROOT / "docs" / "metric_dictionary.md",
    PROJECT_ROOT / "docs" / "scoring_framework.md",
    PROJECT_ROOT / "docs" / "data_model.md",
    PROJECT_ROOT / "docs" / "advanced_analytics.md",
    PROJECT_ROOT / "docs" / "release_governance.md",
    PROJECT_ROOT / "docs" / "adr_presentation_templates.md",
]


def _require_exists(paths: list[Path]) -> None:
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        print("Missing required release artifacts:")
        for p in missing:
            print("-", p)
        raise SystemExit(1)


def _publication_issues() -> list[str]:
    """Return structural defects in the tracked charts and analytical report."""
    issues: list[str] = []
    expected_graphs = set(REQUIRED_GRAPH_FILES)
    observed_graphs = {path.name for path in OUTPUT_GRAPHS_DIR.glob("*.png")}
    if observed_graphs != expected_graphs:
        missing = sorted(expected_graphs - observed_graphs)
        unexpected = sorted(observed_graphs - expected_graphs)
        issues.append(f"graph set mismatch: missing={missing}, unexpected={unexpected}")

    for graph_name in sorted(observed_graphs & expected_graphs):
        graph_path = OUTPUT_GRAPHS_DIR / graph_name
        try:
            with Image.open(graph_path) as image:
                image.verify()
            with Image.open(graph_path) as image:
                if image.format != "PNG" or image.width < 1_400 or image.height < 900:
                    issues.append(
                        f"invalid graph {graph_name}: format={image.format}, size={image.size}"
                    )
        except OSError as exc:
            issues.append(f"unreadable graph {graph_name}: {exc}")

    try:
        reader = PdfReader(str(OUTPUT_REPORT))
        if len(reader.pages) not in REPORT_PAGE_COUNT_RANGE:
            issues.append(f"report page count outside guardrail: {len(reader.pages)}")
        report_text = "\n".join(page.extract_text() or "" for page in reader.pages)
        missing_anchors = [phrase for phrase in REPORT_ANCHOR_PHRASES if phrase not in report_text]
        if missing_anchors:
            issues.append(f"report sections missing: {missing_anchors}")
    except (OSError, ValueError, PdfReadError) as exc:
        issues.append(f"unreadable report: {exc}")

    return issues


def run_ci_quality_gate() -> None:
    pre_delivery = pd.read_csv(OUTPUT_TABLES_DIR / "validation_pre_delivery_checks.csv")
    sql_checks = pd.read_csv(OUTPUT_TABLES_DIR / "ci_sql_validation_checks.csv")
    release_matrix = pd.read_csv(OUTPUT_TABLES_DIR / "validation_release_state_matrix.csv")

    pre_fail = int((pre_delivery["status"] == "FAIL").sum())
    pre_warn = int((pre_delivery["status"] == "WARN").sum())
    sql_fail = int((sql_checks["status"] != "PASS").sum())
    release_classification = (
        str(release_matrix["release_classification"].iloc[0])
        if not release_matrix.empty
        else "publish-blocked"
    )
    publish_blocked = release_classification == "publish-blocked"

    publication_graphs = sorted(OUTPUT_GRAPHS_DIR.glob("*.png"))
    _require_exists(
        REQUIRED_FILES
        + [OUTPUT_DASHBOARD, OUTPUT_REPORT]
        + [OUTPUT_GRAPHS_DIR / name for name in REQUIRED_GRAPH_FILES]
    )
    publication_issues = _publication_issues()

    print("CI quality gate summary:")
    print(f"- Pre-delivery checks: {len(pre_delivery)} total, {pre_fail} FAIL, {pre_warn} WARN")
    print(f"- SQL checks: {len(sql_checks)} total, {sql_fail} non-pass")
    print(f"- Release classification: {release_classification}")
    print(f"- Publication graphs: {len(publication_graphs)}")
    print(f"- Publication structure issues: {len(publication_issues)}")
    for issue in publication_issues:
        print(f"  - {issue}")

    if (
        pre_fail > 0
        or sql_fail > 0
        or publish_blocked
        or len(publication_graphs) != REQUIRED_GRAPH_COUNT
        or publication_issues
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    run_ci_quality_gate()
