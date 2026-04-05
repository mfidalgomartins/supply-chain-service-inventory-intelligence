from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from src.config import DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_RAW, PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
EXTERNAL_DATA_DIR = PROJECT_ROOT / "data" / "external"
EXTERNAL_TEMPLATE_DIR = EXTERNAL_DATA_DIR / "templates"


RAW_TABLES = [
    "products",
    "suppliers",
    "warehouses",
    "inventory_snapshots",
    "demand_history",
    "purchase_orders",
    "product_classification",
]


def _template_from_raw(raw_path: Path, template_path: Path) -> None:
    if not raw_path.exists():
        return
    cols = pd.read_csv(raw_path, nrows=1).columns.tolist()
    pd.DataFrame(columns=cols).to_csv(template_path, index=False)


def _ensure_templates() -> None:
    EXTERNAL_TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

    for table in RAW_TABLES:
        raw_path = DATA_RAW / f"{table}.csv"
        template_path = EXTERNAL_TEMPLATE_DIR / f"{table}_template.csv"
        if not template_path.exists():
            _template_from_raw(raw_path, template_path)


def _inspect_source(table: str) -> dict:
    external_path = EXTERNAL_DATA_DIR / f"{table}.csv"
    synthetic_path = DATA_RAW / f"{table}.csv"

    if external_path.exists():
        selected_path = external_path
        source_mode = "external"
    else:
        selected_path = synthetic_path
        source_mode = "synthetic"

    expected_cols = pd.read_csv(synthetic_path, nrows=1).columns.tolist() if synthetic_path.exists() else []

    if selected_path.exists():
        sample = pd.read_csv(selected_path, nrows=500)
        observed_cols = sample.columns.tolist()
        with selected_path.open("r", encoding="utf-8") as f:
            row_count = max(int(sum(1 for _ in f) - 1), 0)
        column_match_rate = (
            len(set(observed_cols).intersection(expected_cols)) / len(expected_cols)
            if expected_cols
            else 0.0
        )
        mtime = datetime.fromtimestamp(selected_path.stat().st_mtime, tz=timezone.utc)
        freshness_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600.0
    else:
        observed_cols = []
        row_count = 0
        column_match_rate = 0.0
        freshness_hours = None

    status = "READY"
    if not selected_path.exists():
        status = "MISSING"
    elif column_match_rate < 0.85:
        status = "SCHEMA_MISMATCH"
    elif row_count == 0:
        status = "EMPTY"

    return {
        "table_name": table,
        "source_mode": source_mode,
        "source_path": selected_path.as_posix(),
        "status": status,
        "row_count": row_count,
        "expected_column_count": len(expected_cols),
        "observed_column_count": len(observed_cols),
        "column_match_rate": column_match_rate,
        "freshness_hours": freshness_hours,
        "used_external_source": int(source_mode == "external" and status == "READY"),
    }


def _build_refresh_manifest() -> pd.DataFrame:
    key_paths = [
        PROJECT_ROOT / "outputs" / "tables" / "validation_pre_delivery_checks.csv",
        PROJECT_ROOT / "outputs" / "tables" / "dashboard_monthly_sku_fact.csv",
        PROJECT_ROOT / "outputs" / "dashboard" / "index.html",
        PROJECT_ROOT / "outputs" / "reports" / "executive_summary.md",
    ]

    rows: list[dict] = []
    now = datetime.now(timezone.utc)
    for p in key_paths:
        exists = p.exists()
        if exists:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            age_hours = (now - mtime).total_seconds() / 3600.0
            size_kb = p.stat().st_size / 1024.0
        else:
            mtime = None
            age_hours = None
            size_kb = None

        rows.append(
            {
                "artifact_path": p.as_posix(),
                "exists": int(exists),
                "modified_utc": mtime.strftime("%Y-%m-%d %H:%M:%S") if mtime else None,
                "age_hours": age_hours,
                "size_kb": size_kb,
            }
        )

    return pd.DataFrame(rows)


def _write_summary(readiness: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    ready = int((readiness["status"] == "READY").sum())
    external_ready = int(readiness["used_external_source"].sum())

    lines = [
        "# Source Adapter Readiness",
        "",
        "This module provides a production-style bridge from synthetic CSVs to real external extracts with schema-readiness checks.",
        "",
        "## Readiness Snapshot",
        f"- Tables assessed: **{len(readiness)}**",
        f"- Ready tables: **{ready}**",
        f"- Ready external tables currently in use: **{external_ready}**",
        "",
        "## Usage",
        "- Drop real source extracts in `data/external/<table>.csv` with matching headers.",
        "- Run pipeline as-is; adapter automatically promotes external files when schema checks pass.",
        "- Template headers are available in `data/external/templates/`.",
    ]

    (OUTPUT_REPORTS_DIR / "source_adapter_readiness.md").write_text("\n".join(lines), encoding="utf-8")


def run_source_adapter() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    EXTERNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    _ensure_templates()

    readiness_rows = [_inspect_source(t) for t in RAW_TABLES]
    readiness = pd.DataFrame(readiness_rows).sort_values("table_name")
    manifest = _build_refresh_manifest()

    readiness.to_csv(OUTPUT_TABLES_DIR / "source_adapter_readiness.csv", index=False)
    manifest.to_csv(OUTPUT_TABLES_DIR / "source_refresh_manifest.csv", index=False)

    _write_summary(readiness)

    print("Source adapter readiness complete.")
    print(f"Tables assessed: {len(readiness):,}")
    print(f"External sources ready: {int(readiness['used_external_source'].sum()):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_source_adapter()
