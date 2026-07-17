"""Content-level data catalog and validated asset-lineage graph."""

from __future__ import annotations

import hashlib
import json
import mimetypes
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.config import PROJECT_ROOT
from src.object_store import ObjectDescriptor, ObjectStore, content_addressed_key

CONTRACT_FILE = PROJECT_ROOT / "configs" / "table_contracts.json"
CATALOG_COLUMNS = [
    "asset_name",
    "asset_type",
    "producer_stage",
    "path",
    "row_count",
    "column_count",
    "size_bytes",
    "content_sha256",
    "schema_sha256",
    "watermark_column",
    "watermark_min",
    "watermark_max",
    "parent_count",
    "run_id",
]
LINEAGE_COLUMNS = ["parent_asset", "child_asset", "producer_stage", "run_id"]


@dataclass(frozen=True)
class AssetSpec:
    asset_name: str
    path: Path
    asset_type: str
    producer_stage: str
    parents: tuple[str, ...] = ()
    watermark_column: str | None = None


def validate_lineage(specs: list[AssetSpec]) -> None:
    """Reject ambiguous producers, missing parents, and lineage cycles."""
    names = [spec.asset_name for spec in specs]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError("Assets have multiple producers: " + ", ".join(duplicates))
    by_name = {spec.asset_name: spec for spec in specs}
    for spec in specs:
        if spec.asset_type not in {"source", "table", "artifact"}:
            raise ValueError(f"Unsupported asset_type for {spec.asset_name}: {spec.asset_type}")
        missing = sorted(set(spec.parents) - set(by_name))
        if missing:
            raise ValueError(
                f"Asset {spec.asset_name} references missing parents: {', '.join(missing)}"
            )
        if len(set(spec.parents)) != len(spec.parents):
            raise ValueError(f"Asset {spec.asset_name} contains duplicate lineage parents")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError(f"Data lineage contains a cycle through {name}")
        if name in visited:
            return
        visiting.add(name)
        for parent in by_name[name].parents:
            visit(parent)
        visiting.remove(name)
        visited.add(name)

    for name in sorted(by_name):
        visit(name)


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def _schema_hash(frame: pd.DataFrame) -> str:
    schema = {str(column): str(frame[column].dtype) for column in sorted(frame.columns)}
    payload = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _profile(spec: AssetSpec, run_id: str) -> dict:
    if not spec.path.exists():
        raise FileNotFoundError(f"Catalog asset is missing: {spec.path}")
    payload = spec.path.read_bytes()
    row_count: int | None = None
    column_count: int | None = None
    schema_sha256: str | None = None
    watermark_min: str | None = None
    watermark_max: str | None = None
    if spec.path.suffix.lower() == ".csv":
        frame = pd.read_csv(spec.path)
        row_count = int(len(frame))
        column_count = int(len(frame.columns))
        schema_sha256 = _schema_hash(frame)
        if spec.watermark_column is not None:
            if spec.watermark_column not in frame.columns:
                raise ValueError(
                    f"Catalog watermark is missing for {spec.asset_name}: {spec.watermark_column}"
                )
            watermarks = pd.to_datetime(frame[spec.watermark_column], errors="coerce")
            if watermarks.isna().all():
                raise ValueError(f"Catalog watermark has no valid values: {spec.asset_name}")
            watermark_min = watermarks.min().strftime("%Y-%m-%d")
            watermark_max = watermarks.max().strftime("%Y-%m-%d")
    return {
        "asset_name": spec.asset_name,
        "asset_type": spec.asset_type,
        "producer_stage": spec.producer_stage,
        "path": _display_path(spec.path),
        "row_count": row_count,
        "column_count": column_count,
        "size_bytes": len(payload),
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "schema_sha256": schema_sha256,
        "watermark_column": spec.watermark_column,
        "watermark_min": watermark_min,
        "watermark_max": watermark_max,
        "parent_count": len(spec.parents),
        "run_id": run_id,
    }


def build_catalog(specs: list[AssetSpec], run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Profile assets and materialize a deterministic edge list in memory."""
    if not run_id.strip():
        raise ValueError("Catalog run_id must not be empty")
    validate_lineage(specs)
    catalog = pd.DataFrame(
        [_profile(spec, run_id) for spec in sorted(specs, key=lambda item: item.asset_name)],
        columns=CATALOG_COLUMNS,
    )
    lineage_rows = [
        {
            "parent_asset": parent,
            "child_asset": spec.asset_name,
            "producer_stage": spec.producer_stage,
            "run_id": run_id,
        }
        for spec in specs
        for parent in spec.parents
    ]
    lineage = pd.DataFrame(lineage_rows, columns=LINEAGE_COLUMNS).sort_values(
        ["parent_asset", "child_asset"], ignore_index=True
    )
    return catalog, lineage


def materialize_catalog(
    specs: list[AssetSpec], *, run_id: str, output_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Write catalog and lineage outputs after profiling all required assets."""
    catalog, lineage = build_catalog(specs, run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    catalog.to_csv(output_dir / "data_catalog.csv", index=False)
    lineage.to_csv(output_dir / "data_lineage.csv", index=False)
    return catalog, lineage


def default_asset_specs(contract_path: Path = CONTRACT_FILE) -> list[AssetSpec]:
    """Build the production registry from contracted assets and explicit lineage."""
    contract_payload = json.loads(contract_path.read_text(encoding="utf-8"))
    contracts = {table["name"]: table for table in contract_payload["tables"]}
    raw_assets = {
        "products",
        "suppliers",
        "warehouses",
        "inventory_snapshots",
        "demand_history",
        "purchase_orders",
        "product_classification",
        "intervention_assignments",
        "network_nodes",
        "network_lanes",
        "product_sources",
    }
    stage_assets = {
        "data_preparation": {
            "daily_product_warehouse_metrics",
            "supplier_performance_summary",
            "product_inventory_profile",
            "warehouse_service_profile",
        },
        "scoring": {
            "sku_risk_table",
            "supplier_risk_table",
            "segment_risk_table",
            "governance_priority_master",
        },
        "impact_analysis": {"impact_overall_summary", "impact_opportunity_priority"},
        "ingestion": {
            "ingestion_manifest",
            "source_readiness_checks",
            "source_schema_registry",
            "source_schema_drift_events",
        },
        "backtesting": {
            "policy_backtest_folds",
            "policy_backtest_recommendations",
            "policy_backtest_abc_summary",
        },
        "monte_carlo": {
            "monte_carlo_policy_scenarios",
            "monte_carlo_recommendations",
            "monte_carlo_portfolio_summary",
        },
        "action_tracking": {"action_register", "action_kpi_timeseries"},
        "causal_evaluation": {
            "causal_effect_estimates",
            "causal_diagnostics",
            "causal_cohort_timeseries",
        },
        "network_optimization": {
            "network_optimization_plan",
            "network_flow_plan",
            "network_constraint_utilization",
            "network_optimization_summary",
        },
        "storage": {"storage_manifest"},
    }
    producer_by_asset = {asset: stage for stage, assets in stage_assets.items() for asset in assets}
    covered = raw_assets | set(producer_by_asset)
    unknown = sorted(set(contracts) - covered)
    missing = sorted(covered - set(contracts))
    if unknown or missing:
        raise ValueError(
            f"Catalog registry/contract mismatch; unregistered={unknown}, missing_contracts={missing}"
        )

    preparation_parents = (
        "products",
        "suppliers",
        "warehouses",
        "inventory_snapshots",
        "demand_history",
        "purchase_orders",
        "product_classification",
    )
    parents_by_asset: dict[str, tuple[str, ...]] = {}
    for asset in stage_assets["ingestion"]:
        parents_by_asset[asset] = tuple(sorted(raw_assets))
    for asset in stage_assets["data_preparation"]:
        parents_by_asset[asset] = preparation_parents
    for asset in stage_assets["scoring"]:
        parents_by_asset[asset] = (
            "daily_product_warehouse_metrics",
            "supplier_performance_summary",
        )
    for asset in stage_assets["impact_analysis"]:
        parents_by_asset[asset] = (
            "governance_priority_master",
            "product_inventory_profile",
            "warehouse_service_profile",
        )
    for asset in stage_assets["backtesting"]:
        parents_by_asset[asset] = (
            "daily_product_warehouse_metrics",
            "products",
            "suppliers",
            "purchase_orders",
        )
    for asset in stage_assets["monte_carlo"]:
        parents_by_asset[asset] = (
            "daily_product_warehouse_metrics",
            "products",
            "suppliers",
            "purchase_orders",
            "policy_backtest_recommendations",
        )
    for asset in stage_assets["action_tracking"]:
        parents_by_asset[asset] = (
            "daily_product_warehouse_metrics",
            "products",
            "suppliers",
            "purchase_orders",
        )
    for asset in stage_assets["causal_evaluation"]:
        parents_by_asset[asset] = (
            "intervention_assignments",
            "daily_product_warehouse_metrics",
            "products",
        )
    for asset in stage_assets["network_optimization"]:
        parents_by_asset[asset] = (
            "network_nodes",
            "network_lanes",
            "product_sources",
            "daily_product_warehouse_metrics",
            "inventory_snapshots",
            "products",
        )
    parents_by_asset["storage_manifest"] = tuple(sorted(set(contracts) - {"storage_manifest"}))
    watermarks = {
        "inventory_snapshots": "snapshot_date",
        "demand_history": "date",
        "purchase_orders": "order_date",
        "daily_product_warehouse_metrics": "date",
        "policy_backtest_folds": "fold_start",
        "causal_cohort_timeseries": "date",
    }
    return [
        AssetSpec(
            asset_name=name,
            path=PROJECT_ROOT / contract["path"],
            asset_type="source" if name in raw_assets else "table",
            producer_stage="ingestion" if name in raw_assets else producer_by_asset[name],
            parents=() if name in raw_assets else parents_by_asset[name],
            watermark_column=watermarks.get(name),
        )
        for name, contract in sorted(contracts.items())
    ]


def _catalogued_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _content_type(path: Path) -> str:
    if path.suffix.lower() == ".csv":
        return "text/csv"
    if path.suffix.lower() == ".json":
        return "application/json"
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def publish_catalogued_run(
    catalog: pd.DataFrame,
    lineage: pd.DataFrame,
    *,
    store: ObjectStore,
    run_id: str,
    output_dir: Path,
) -> tuple[pd.DataFrame, ObjectDescriptor]:
    """Publish catalogued bytes, then atomically promote an immutable run manifest."""
    required_catalog = {"asset_name", "path", "content_sha256", "size_bytes"}
    missing = sorted(required_catalog - set(catalog.columns))
    if missing:
        raise ValueError(f"Data catalog fields missing for publication: {missing}")
    publication_assets = [
        (
            str(row.asset_name),
            _catalogued_path(str(row.path)),
            str(row.content_sha256),
        )
        for row in catalog.itertuples(index=False)
    ]
    for name, path in (
        ("data_catalog", output_dir / "data_catalog.csv"),
        ("data_lineage", output_dir / "data_lineage.csv"),
    ):
        publication_assets.append((name, path, hashlib.sha256(path.read_bytes()).hexdigest()))

    rows: list[dict] = []
    for asset_name, path, expected_sha256 in sorted(publication_assets):
        payload = path.read_bytes()
        observed_sha256 = hashlib.sha256(payload).hexdigest()
        if observed_sha256 != expected_sha256:
            raise ValueError(f"Catalogued asset changed before publication: {asset_name}")
        descriptor = store.put_immutable(
            content_addressed_key(payload),
            payload,
            content_type=_content_type(path),
        )
        rows.append(
            {
                "asset_name": asset_name,
                "logical_path": _display_path(path),
                "object_key": descriptor.key,
                "content_sha256": descriptor.sha256,
                "size_bytes": descriptor.size_bytes,
                "content_type": descriptor.content_type,
                "run_id": run_id,
            }
        )
    manifest = pd.DataFrame(rows).sort_values("asset_name", ignore_index=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(output_dir / "object_publication_manifest.csv", index=False)
    manifest_payload = (
        json.dumps(
            {"run_id": run_id, "assets": manifest.to_dict(orient="records")},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    manifest_descriptor = store.put_immutable(
        content_addressed_key(manifest_payload),
        manifest_payload,
        content_type="application/json",
    )
    store.promote_pointer(
        "pointers/latest.json",
        {
            "run_id": run_id,
            "manifest_object_key": manifest_descriptor.key,
            "manifest_sha256": manifest_descriptor.sha256,
            "published_asset_count": len(manifest),
        },
    )
    return manifest, manifest_descriptor
