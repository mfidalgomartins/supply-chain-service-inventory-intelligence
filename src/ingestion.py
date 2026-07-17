"""Configurable ingestion for synthetic, canonical-directory, and ERP/WMS exports."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd

from src.config import DATA_RAW, PROJECT_ROOT
from src.data_contracts import evaluate_dataframe_contract, evaluate_reference_contract
from src.data_generation import generate_all_tables
from src.settings import AdapterSettings, SourceGovernanceSettings, load_settings
from src.source_readiness import publish_source_readiness
from src.warehouse import RAW_TABLE_FILES

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
CONTRACT_FILE = PROJECT_ROOT / "configs" / "table_contracts.json"

ERP_TABLES = {
    "products",
    "suppliers",
    "purchase_orders",
    "product_classification",
    "intervention_assignments",
    "network_nodes",
    "network_lanes",
    "product_sources",
}
WMS_TABLES = {"warehouses", "inventory_snapshots", "demand_history"}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve_source(path_value: str) -> Path:
    path = Path(path_value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def _read_export(path: Path, file_format: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing source export: {path}")
    if file_format == "csv":
        return pd.read_csv(path)
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute("SELECT * FROM read_parquet(?)", [str(path)]).df()
    finally:
        con.close()


def _source_file(directory: Path, table_name: str, file_format: str) -> Path:
    return directory / f"{table_name}.{file_format}"


def _source_directory(adapter: AdapterSettings, table_name: str) -> Path:
    if adapter.type == "directory":
        if adapter.source_path is None:
            raise ValueError("directory adapter requires source_path")
        return _resolve_source(adapter.source_path)
    if table_name in ERP_TABLES:
        if adapter.erp_path is None:
            raise ValueError("erp_wms adapter requires erp_path")
        return _resolve_source(adapter.erp_path)
    if adapter.wms_path is None:
        raise ValueError("erp_wms adapter requires wms_path")
    return _resolve_source(adapter.wms_path)


def _load_raw_contracts() -> tuple[list[dict], list[dict]]:
    spec = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))
    raw_names = set(RAW_TABLE_FILES)
    contracts = [contract for contract in spec["tables"] if contract["name"] in raw_names]
    relationships = [
        relationship
        for relationship in spec.get("relationships", [])
        if relationship["source_table"] in raw_names and relationship["target_table"] in raw_names
    ]
    return contracts, relationships


def _validate_tables(tables: dict[str, pd.DataFrame]) -> None:
    contracts, relationships = _load_raw_contracts()
    failures: list[str] = []
    for contract in contracts:
        table_name = contract["name"]
        if table_name not in tables:
            failures.append(f"missing table {table_name}")
            continue
        failures.extend(
            f"{table_name}.{check.check_name}: {check.details}"
            for check in evaluate_dataframe_contract(tables[table_name], contract)
            if check.status == "FAIL"
        )
    for relationship in relationships:
        if relationship["source_table"] in tables and relationship["target_table"] in tables:
            check = evaluate_reference_contract(tables, relationship)
            if check.status == "FAIL":
                failures.append(f"{check.table_name}.{check.check_name}: {check.details}")
    if failures:
        raise ValueError("Input adapter contract failure:\n- " + "\n- ".join(failures))


def ingest_external_exports(
    adapter: AdapterSettings,
    *,
    governance: SourceGovernanceSettings | None = None,
    readiness_output_dir: Path = OUTPUT_TABLES_DIR,
) -> pd.DataFrame:
    """Normalize configured exports into the canonical raw CSV contract."""
    tables: dict[str, pd.DataFrame] = {}
    source_paths: dict[str, Path] = {}
    for table_name in RAW_TABLE_FILES:
        directory = _source_directory(adapter, table_name)
        source_path = _source_file(directory, table_name, adapter.file_format)
        frame = _read_export(source_path, adapter.file_format)
        mapping = adapter.column_mapping.get(table_name, {})
        frame = frame.rename(columns=mapping)
        duplicated_columns = frame.columns[frame.columns.duplicated()].tolist()
        if duplicated_columns:
            raise ValueError(
                f"Column mapping creates duplicate columns in {table_name}: "
                + ", ".join(sorted(set(duplicated_columns)))
            )
        tables[table_name] = frame
        source_paths[table_name] = source_path

    if governance is not None:
        contracts, _ = _load_raw_contracts()
        publish_source_readiness(
            tables,
            contracts,
            governance,
            output_dir=readiness_output_dir,
        )
    _validate_tables(tables)
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    for table_name, frame in tables.items():
        output_path = RAW_TABLE_FILES[table_name]
        frame.to_csv(output_path, index=False)
        rows.append(
            {
                "adapter_type": adapter.type,
                "table_name": table_name,
                "source": source_paths[table_name].as_posix(),
                "row_count": len(frame),
                "source_sha256": _sha256(source_paths[table_name]),
                "canonical_sha256": _sha256(output_path),
            }
        )
    return pd.DataFrame(rows).sort_values("table_name")


def run_ingestion(config_path: Path | None = None) -> pd.DataFrame:
    settings = load_settings(config_path)
    if settings.adapter.type == "synthetic":
        generate_all_tables()
        synthetic_tables = {
            table_name: pd.read_csv(path) for table_name, path in RAW_TABLE_FILES.items()
        }
        contracts, _ = _load_raw_contracts()
        publish_source_readiness(
            synthetic_tables,
            contracts,
            settings.source_governance,
            output_dir=OUTPUT_TABLES_DIR,
        )
        manifest = pd.DataFrame(
            [
                {
                    "adapter_type": "synthetic",
                    "table_name": table_name,
                    "source": "deterministic_generator",
                    "row_count": len(pd.read_csv(path)),
                    "source_sha256": _sha256(path),
                    "canonical_sha256": _sha256(path),
                }
                for table_name, path in RAW_TABLE_FILES.items()
            ]
        ).sort_values("table_name")
    else:
        manifest = ingest_external_exports(
            settings.adapter,
            governance=settings.source_governance,
        )

    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(OUTPUT_TABLES_DIR / "ingestion_manifest.csv", index=False)
    print(f"Ingestion complete. Adapter: {settings.adapter.type}; tables: {len(manifest)}")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Load canonical supply-chain source extracts")
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    run_ingestion(args.config)


if __name__ == "__main__":
    main()
