"""Parquet lake synchronization with deterministic incremental upserts."""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from src.config import (
    DATA_PROCESSED,
    LAKE_ANALYTICS,
    LAKE_PROCESSED,
    LAKE_RAW,
    PROJECT_ROOT,
)
from src.settings import load_settings
from src.warehouse import RAW_TABLE_FILES, _validate_identifier

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
MANIFEST_PATH = OUTPUT_TABLES_DIR / "storage_manifest.csv"


@dataclass(frozen=True)
class TableSpec:
    layer: str
    table_name: str
    source_path: Path
    key: tuple[str, ...]
    watermark: str | None = None
    incremental: bool = False


RAW_SPECS = (
    TableSpec("raw", "products", RAW_TABLE_FILES["products"], ("product_id",)),
    TableSpec("raw", "suppliers", RAW_TABLE_FILES["suppliers"], ("supplier_id",)),
    TableSpec("raw", "warehouses", RAW_TABLE_FILES["warehouses"], ("warehouse_id",)),
    TableSpec(
        "raw",
        "inventory_snapshots",
        RAW_TABLE_FILES["inventory_snapshots"],
        ("snapshot_date", "warehouse_id", "product_id"),
        "snapshot_date",
        True,
    ),
    TableSpec(
        "raw",
        "demand_history",
        RAW_TABLE_FILES["demand_history"],
        ("date", "warehouse_id", "product_id"),
        "date",
        True,
    ),
    TableSpec(
        "raw",
        "purchase_orders",
        RAW_TABLE_FILES["purchase_orders"],
        ("po_id",),
        "order_date",
        True,
    ),
    TableSpec(
        "raw",
        "product_classification",
        RAW_TABLE_FILES["product_classification"],
        ("product_id",),
    ),
    TableSpec(
        "raw",
        "intervention_assignments",
        RAW_TABLE_FILES["intervention_assignments"],
        ("experiment_id", "unit_id"),
    ),
    TableSpec("raw", "network_nodes", RAW_TABLE_FILES["network_nodes"], ("node_id",)),
    TableSpec("raw", "network_lanes", RAW_TABLE_FILES["network_lanes"], ("lane_id",)),
    TableSpec(
        "raw",
        "product_sources",
        RAW_TABLE_FILES["product_sources"],
        ("product_id", "supplier_id"),
    ),
)

PROCESSED_SPECS = (
    TableSpec(
        "processed",
        "daily_product_warehouse_metrics",
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
        ("date", "warehouse_id", "product_id"),
        "date",
        True,
    ),
    TableSpec(
        "processed",
        "supplier_performance_summary",
        DATA_PROCESSED / "supplier_performance_summary.csv",
        ("supplier_id",),
    ),
    TableSpec(
        "processed",
        "product_inventory_profile",
        DATA_PROCESSED / "product_inventory_profile.csv",
        ("product_id",),
    ),
    TableSpec(
        "processed",
        "warehouse_service_profile",
        DATA_PROCESSED / "warehouse_service_profile.csv",
        ("warehouse_id",),
    ),
    TableSpec(
        "processed",
        "sku_risk_table",
        DATA_PROCESSED / "sku_risk_table.csv",
        ("product_id", "warehouse_id", "supplier_id"),
    ),
    TableSpec(
        "processed",
        "supplier_risk_table",
        DATA_PROCESSED / "supplier_risk_table.csv",
        ("supplier_id",),
    ),
    TableSpec(
        "processed",
        "segment_risk_table",
        DATA_PROCESSED / "segment_risk_table.csv",
        ("category", "region"),
    ),
    TableSpec(
        "processed",
        "governance_priority_master",
        DATA_PROCESSED / "governance_priority_master.csv",
        ("entity_type", "entity_id"),
    ),
)


def _analytics_specs() -> tuple[TableSpec, ...]:
    definitions = (
        ("ingestion_manifest", ("table_name",)),
        ("source_readiness_checks", ("table_name", "check_name")),
        ("source_schema_registry", ("table_name", "column_name")),
        ("source_schema_drift_events", ("table_name", "drift_type", "column_name")),
        ("impact_overall_summary", ("metric",)),
        ("impact_opportunity_priority", ("entity_type", "entity_id")),
        ("policy_backtest_folds", ("product_id", "warehouse_id", "fold_start", "policy_id")),
        ("policy_backtest_recommendations", ("product_id", "warehouse_id")),
        ("policy_backtest_abc_summary", ("abc_class", "policy_id")),
        ("monte_carlo_policy_scenarios", ("product_id", "warehouse_id", "scenario_id")),
        ("monte_carlo_recommendations", ("product_id", "warehouse_id")),
        ("monte_carlo_portfolio_summary", ("metric",)),
        ("action_register", ("action_id",)),
        ("action_kpi_timeseries", ("action_id", "month", "period")),
        ("causal_effect_estimates", ("experiment_id",)),
        ("causal_diagnostics", ("experiment_id", "diagnostic")),
        ("causal_cohort_timeseries", ("experiment_id", "date", "treatment_flag")),
        ("network_optimization_plan", ("product_id", "warehouse_id")),
        ("network_flow_plan", ("product_id", "lane_id")),
        ("network_constraint_utilization", ("constraint_type", "constraint_id")),
        ("network_optimization_summary", ("solver_status",)),
    )
    return tuple(
        TableSpec("analytics", name, OUTPUT_TABLES_DIR / f"{name}.csv", key)
        for name, key in definitions
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fetchone(
    con: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[str] | None = None,
) -> tuple:
    row = con.execute(query, parameters or []).fetchone()
    if row is None:
        raise ValueError(f"Query returned no row: {query}")
    return row


def _lake_directory(layer: str) -> Path:
    return {"raw": LAKE_RAW, "processed": LAKE_PROCESSED, "analytics": LAKE_ANALYTICS}[layer]


def _specs(layer: str) -> tuple[TableSpec, ...]:
    if layer == "raw":
        return RAW_SPECS
    if layer == "processed":
        return PROCESSED_SPECS
    if layer == "analytics":
        return _analytics_specs()
    if layer == "downstream":
        return (*PROCESSED_SPECS, *_analytics_specs())
    raise ValueError(f"Unsupported storage layer: {layer}")


def _load_csv(con: duckdb.DuckDBPyConnection, source_path: Path) -> None:
    con.execute(
        "CREATE OR REPLACE TABLE incoming_data AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)",
        [str(source_path)],
    )


def _write_parquet(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    parquet_path: Path,
    compression: str,
) -> None:
    relation_name = _validate_identifier(relation)
    con.execute(
        f"COPY {relation_name} TO ? (FORMAT PARQUET, COMPRESSION ?, OVERWRITE_OR_IGNORE TRUE)",
        [str(parquet_path), compression],
    )


def _incremental_merge(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    key: tuple[str, ...],
) -> int:
    con.execute("CREATE TABLE existing_data AS SELECT * FROM read_parquet(?)", [str(parquet_path)])
    existing_columns = [row[0] for row in con.execute("DESCRIBE existing_data").fetchall()]
    incoming_columns = [row[0] for row in con.execute("DESCRIBE incoming_data").fetchall()]
    if existing_columns != incoming_columns:
        raise ValueError(
            f"Incremental schema drift for {parquet_path.name}: "
            f"existing={existing_columns}, incoming={incoming_columns}"
        )
    partition = ", ".join(_validate_identifier(column) for column in key)
    con.execute(
        f"""
        CREATE TABLE output_data AS
        SELECT * EXCLUDE (_source_priority, _row_number)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {partition}
                ORDER BY _source_priority DESC
            ) AS _row_number
            FROM (
                SELECT *, 0 AS _source_priority FROM existing_data
                UNION ALL BY NAME
                SELECT *, 1 AS _source_priority FROM incoming_data
            )
        )
        WHERE _row_number = 1
        """
    )
    return int(_fetchone(con, "SELECT COUNT(*) FROM existing_data")[0])


def _watermark_bounds(
    con: duckdb.DuckDBPyConnection, relation: str, watermark: str | None
) -> tuple[str, str]:
    if watermark is None:
        return "", ""
    column = _validate_identifier(watermark)
    relation_name = _validate_identifier(relation)
    minimum, maximum = _fetchone(
        con,
        f"SELECT MIN({column})::VARCHAR, MAX({column})::VARCHAR FROM {relation_name}",
    )
    return str(minimum or ""), str(maximum or "")


def sync_layer(layer: str, config_path: Path | None = None) -> pd.DataFrame:
    settings = load_settings(config_path)
    existing_manifest = pd.read_csv(MANIFEST_PATH) if MANIFEST_PATH.exists() else pd.DataFrame()
    if not existing_manifest.empty:
        required_manifest_columns = {
            "layer",
            "table_name",
            "source_sha256",
            "parquet_sha256",
        }
        missing_columns = required_manifest_columns - set(existing_manifest.columns)
        if missing_columns:
            raise ValueError(
                "Storage manifest is missing columns: " + ", ".join(sorted(missing_columns))
            )
        if existing_manifest.duplicated(["layer", "table_name"]).any():
            raise ValueError("Storage manifest contains duplicate layer/table entries")
    prior_records = (
        existing_manifest.set_index(["layer", "table_name"]).to_dict(orient="index")
        if not existing_manifest.empty
        else {}
    )
    current_layers = {spec.layer for spec in _specs(layer)}
    retained = (
        existing_manifest[~existing_manifest["layer"].isin(current_layers)].copy()
        if not existing_manifest.empty
        else pd.DataFrame()
    )

    rows: list[dict] = []
    for spec in _specs(layer):
        if not spec.source_path.exists():
            raise FileNotFoundError(f"Missing {spec.layer} storage source: {spec.source_path}")
        lake_dir = _lake_directory(spec.layer)
        lake_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = lake_dir / f"{spec.table_name}.parquet"
        source_hash = _sha256(spec.source_path)
        prior_record = prior_records.get((spec.layer, spec.table_name), {})
        source_unchanged = prior_record.get("source_sha256") == source_hash
        compression_unchanged = prior_record.get("compression") == settings.storage.compression
        parquet_trusted = parquet_path.exists() and prior_record.get("parquet_sha256") == _sha256(
            parquet_path
        )

        con = duckdb.connect(database=":memory:")
        try:
            _load_csv(con, spec.source_path)
            input_rows = int(_fetchone(con, "SELECT COUNT(*) FROM incoming_data")[0])
            prior_rows = 0
            if source_unchanged and compression_unchanged and parquet_trusted:
                con.execute(
                    "CREATE TABLE output_data AS SELECT * FROM read_parquet(?)",
                    [str(parquet_path)],
                )
                refresh_mode = "skipped"
                needs_write = False
            elif (
                settings.storage.incremental
                and spec.incremental
                and parquet_trusted
                and not source_unchanged
            ):
                prior_rows = _incremental_merge(con, parquet_path, spec.key)
                refresh_mode = "incremental_upsert"
                needs_write = True
            else:
                con.execute("CREATE TABLE output_data AS SELECT * FROM incoming_data")
                refresh_mode = "full_replace"
                needs_write = True

            stored_rows = int(_fetchone(con, "SELECT COUNT(*) FROM output_data")[0])
            null_key_count = int(
                _fetchone(
                    con,
                    "SELECT COUNT(*) FROM output_data WHERE "
                    + " OR ".join(f"{_validate_identifier(column)} IS NULL" for column in spec.key),
                )[0]
            )
            if null_key_count:
                raise ValueError(
                    f"Parquet null business keys in {spec.table_name}: {null_key_count}"
                )
            duplicate_count = int(
                _fetchone(
                    con,
                    "SELECT COUNT(*) - COUNT(DISTINCT ("
                    + ", ".join(_validate_identifier(column) for column in spec.key)
                    + ")) FROM output_data",
                )[0]
            )
            if duplicate_count:
                raise ValueError(f"Parquet key duplication in {spec.table_name}: {duplicate_count}")
            watermark_min, watermark_max = _watermark_bounds(con, "output_data", spec.watermark)
            if needs_write:
                _write_parquet(
                    con,
                    "output_data",
                    parquet_path,
                    settings.storage.compression,
                )
        finally:
            con.close()

        rows.append(
            {
                "layer": spec.layer,
                "table_name": spec.table_name,
                "source_path": spec.source_path.relative_to(PROJECT_ROOT).as_posix(),
                "parquet_path": parquet_path.relative_to(PROJECT_ROOT).as_posix(),
                "compression": settings.storage.compression,
                "refresh_mode": refresh_mode,
                "input_rows": input_rows,
                "prior_rows": prior_rows,
                "stored_rows": stored_rows,
                "key_columns": "|".join(spec.key),
                "watermark_min": watermark_min,
                "watermark_max": watermark_max,
                "source_sha256": source_hash,
                "parquet_sha256": _sha256(parquet_path),
            }
        )

    manifest = pd.concat([retained, pd.DataFrame(rows)], ignore_index=True)
    manifest = manifest.sort_values(["layer", "table_name"]).reset_index(drop=True)
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(MANIFEST_PATH, index=False)
    print(
        f"Parquet synchronization complete. Layer: {layer}; "
        f"tables: {len(rows)}; rows: {sum(row['stored_rows'] for row in rows):,}"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize governed CSV layers to Parquet")
    parser.add_argument(
        "--layer",
        choices=["raw", "processed", "analytics", "downstream"],
        required=True,
    )
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    sync_layer(args.layer, args.config)


if __name__ == "__main__":
    main()
