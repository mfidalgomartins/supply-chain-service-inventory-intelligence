"""Production entry point for the governed analytics pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from src.data_catalog import default_asset_specs
from src.object_store import LocalObjectStore, ObjectStore, S3ObjectStore
from src.orchestration import (
    PipelineExecutionError,
    PipelineStage,
    compute_run_id,
    orchestrate,
)
from src.settings import DEFAULT_CONFIG_PATH, ObjectStoreSettings, load_settings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUT_TABLES_DIR = OUTPUTS_DIR / "tables"
TIMINGS_FILE = OUTPUTS_DIR / "pipeline_timings.json"
EVENTS_FILE = OUTPUTS_DIR / "pipeline_run_events.jsonl"
SUMMARY_FILE = OUTPUTS_DIR / "pipeline_run_summary.json"

PLOT_ENV = {
    "MPLBACKEND": "Agg",
    "MPLCONFIGDIR": str(PROJECT_ROOT / ".cache" / "matplotlib"),
    "XDG_CACHE_HOME": str(PROJECT_ROOT / ".cache"),
}


def _table(name: str) -> Path:
    return OUTPUT_TABLES_DIR / f"{name}.csv"


STEPS: list[PipelineStage] = [
    PipelineStage(
        "ingestion",
        "src.ingestion",
        outputs=(_table("ingestion_manifest"), _table("source_readiness_checks")),
    ),
    PipelineStage(
        "storage_raw",
        "src.storage",
        args=("--layer", "raw"),
        dependencies=("ingestion",),
        outputs=(PROJECT_ROOT / "data/lake/raw/products.parquet",),
    ),
    PipelineStage(
        "data_preparation",
        "src.data_preparation",
        dependencies=("storage_raw",),
        outputs=(PROJECT_ROOT / "data/processed/daily_product_warehouse_metrics.csv",),
    ),
    PipelineStage(
        "scoring",
        "src.scoring",
        dependencies=("data_preparation",),
        outputs=(PROJECT_ROOT / "data/processed/governance_priority_master.csv",),
    ),
    PipelineStage(
        "impact_analysis",
        "src.impact_analysis",
        dependencies=("scoring",),
        outputs=(_table("impact_overall_summary"),),
    ),
    PipelineStage(
        "backtesting",
        "src.backtesting",
        dependencies=("data_preparation",),
        outputs=(_table("policy_backtest_recommendations"),),
    ),
    PipelineStage(
        "monte_carlo",
        "src.monte_carlo",
        dependencies=("backtesting",),
        outputs=(_table("monte_carlo_recommendations"),),
    ),
    PipelineStage(
        "action_tracking",
        "src.action_tracking",
        dependencies=("scoring",),
        outputs=(_table("action_register"),),
    ),
    PipelineStage(
        "causal_evaluation",
        "src.causal_evaluation",
        dependencies=("action_tracking",),
        outputs=(_table("causal_effect_estimates"), _table("causal_diagnostics")),
    ),
    PipelineStage(
        "network_optimization",
        "src.network_optimization",
        dependencies=("data_preparation",),
        outputs=(
            _table("network_optimization_summary"),
            _table("network_constraint_utilization"),
        ),
    ),
    PipelineStage(
        "storage_downstream",
        "src.storage",
        args=("--layer", "downstream"),
        dependencies=(
            "impact_analysis",
            "monte_carlo",
            "action_tracking",
            "causal_evaluation",
            "network_optimization",
        ),
        outputs=(_table("storage_manifest"),),
    ),
    PipelineStage(
        "data_contracts",
        "src.data_contracts",
        dependencies=("storage_downstream",),
        outputs=(_table("data_contract_check_results"),),
    ),
    PipelineStage(
        "build_charts",
        "src.build_charts",
        dependencies=("data_contracts",),
        outputs=(OUTPUTS_DIR / "graphs/01_opportunity_by_category.png",),
        env=PLOT_ENV,
    ),
    PipelineStage(
        "build_report",
        "src.build_report",
        dependencies=("build_charts",),
        outputs=(OUTPUTS_DIR / "reports/service_inventory_intelligence_report.pdf",),
    ),
    PipelineStage(
        "executive_dashboard",
        "src.executive_dashboard",
        dependencies=("build_report",),
        outputs=(PROJECT_ROOT / "index.html", _table("dashboard_official_snapshot")),
    ),
    PipelineStage(
        "sql_quality_gate",
        "src.sql_quality_gate",
        dependencies=("executive_dashboard",),
        outputs=(_table("ci_sql_validation_checks"),),
    ),
    PipelineStage(
        "pre_delivery_validation",
        "src.pre_delivery_validation",
        dependencies=("sql_quality_gate",),
        outputs=(
            _table("validation_pre_delivery_checks"),
            _table("validation_release_state_matrix"),
        ),
    ),
    PipelineStage(
        "ci_quality_gate",
        "src.ci_quality_gate",
        dependencies=("pre_delivery_validation",),
    ),
]


def _run_stage(stage: PipelineStage) -> None:
    """Run one isolated module with stage-specific environment overrides."""
    print(f"\n[PIPELINE] Running {stage.label}")
    env = os.environ.copy()
    env.update(stage.env or {})
    subprocess.run(
        [sys.executable, "-m", stage.module, *stage.args],
        check=True,
        cwd=PROJECT_ROOT,
        env=env,
    )


def _export_files(path: Path) -> list[Path]:
    if not path.exists():
        raise FileNotFoundError(f"Configured export path does not exist: {path}")
    files = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
    if not files:
        raise ValueError(f"Configured export path contains no files: {path}")
    return files


def _configured_path(value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def _identity_paths(
    *,
    adapter_type: str,
    config_path: Path,
    source_path: str | None,
    erp_path: str | None,
    wms_path: str | None,
) -> list[Path]:
    """Resolve the governed inputs whose bytes define an idempotent run."""
    paths = [config_path]
    config_payload = json.loads(config_path.read_text(encoding="utf-8"))
    events_file = config_payload.get("action_tracking", {}).get("events_file")
    if events_file:
        paths.append(_configured_path(str(events_file)))
    if adapter_type == "synthetic":
        paths.extend(
            [
                PROJECT_ROOT / "src/data_generation.py",
                PROJECT_ROOT / "src/config.py",
            ]
        )
    elif adapter_type == "directory":
        if source_path is None:
            raise ValueError("directory adapter requires source_path")
        paths.extend(_export_files(_configured_path(source_path)))
    elif adapter_type == "erp_wms":
        if erp_path is None or wms_path is None:
            raise ValueError("erp_wms adapter requires erp_path and wms_path")
        paths.extend(_export_files(_configured_path(erp_path)))
        paths.extend(_export_files(_configured_path(wms_path)))
    else:
        raise ValueError(f"Unsupported adapter type: {adapter_type}")
    return paths


def _build_store(settings: ObjectStoreSettings) -> ObjectStore:
    if settings.backend == "local":
        return LocalObjectStore(_configured_path(settings.local_root))

    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
    )
    return S3ObjectStore(
        bucket=settings.s3_bucket or "",
        prefix=settings.s3_prefix,
        client=client,
        server_side_encryption=settings.s3_server_side_encryption,
    )


def _write_legacy_timings(summary: dict) -> None:
    """Retain the compact timing artifact consumed by CI and prior releases."""
    payload = {
        "total_seconds": round(float(summary["duration_seconds"]), 3),
        "steps": [
            {"step": stage["command"], "seconds": round(float(stage["seconds"]), 3)}
            for stage in summary["stages"]
        ],
    }
    TIMINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TIMINGS_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    (PROJECT_ROOT / ".cache/matplotlib").mkdir(parents=True, exist_ok=True)
    configured = Path(os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH)).expanduser()
    config_path = configured if configured.is_absolute() else PROJECT_ROOT / configured
    settings = load_settings(config_path)
    identity_paths = _identity_paths(
        adapter_type=settings.adapter.type,
        config_path=config_path,
        source_path=settings.adapter.source_path,
        erp_path=settings.adapter.erp_path,
        wms_path=settings.adapter.wms_path,
    )
    run_id = compute_run_id(
        identity_paths,
        context={
            "adapter_type": settings.adapter.type,
            "source_as_of_date": settings.source_governance.as_of_date,
        },
    )
    try:
        summary = orchestrate(
            stages=STEPS,
            runner=_run_stage,
            settings=settings.orchestration,
            run_id=run_id,
            catalog_specs=default_asset_specs(),
            store=_build_store(settings.orchestration.object_store),
            output_dir=OUTPUT_TABLES_DIR,
            event_path=EVENTS_FILE,
            summary_path=SUMMARY_FILE,
        )
    except PipelineExecutionError as exc:
        raise SystemExit(f"Pipeline failed: {exc}") from exc

    _write_legacy_timings(summary)
    print(f"\nPipeline {run_id} finished successfully in {summary['duration_seconds']:.2f}s.")
    print(f"Run summary -> {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
