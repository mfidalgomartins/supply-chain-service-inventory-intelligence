"""DuckDB loading helpers for governed raw and processed analytics tables."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import duckdb
import pandas as pd

from src.config import DATA_RAW, LAKE_RAW, PROJECT_ROOT, SQL_DIR

STORAGE_MANIFEST = PROJECT_ROOT / "outputs" / "tables" / "storage_manifest.csv"

RAW_TABLE_FILES: dict[str, Path] = {
    "products": DATA_RAW / "products.csv",
    "suppliers": DATA_RAW / "suppliers.csv",
    "warehouses": DATA_RAW / "warehouses.csv",
    "inventory_snapshots": DATA_RAW / "inventory_snapshots.csv",
    "demand_history": DATA_RAW / "demand_history.csv",
    "purchase_orders": DATA_RAW / "purchase_orders.csv",
    "product_classification": DATA_RAW / "product_classification.csv",
    "intervention_assignments": DATA_RAW / "intervention_assignments.csv",
    "network_nodes": DATA_RAW / "network_nodes.csv",
    "network_lanes": DATA_RAW / "network_lanes.csv",
    "product_sources": DATA_RAW / "product_sources.csv",
}

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(identifier: str) -> str:
    if not _VALID_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    return identifier


def _parquet_is_current(
    table_name: str,
    csv_path: Path,
    parquet_path: Path,
    manifest_path: Path = STORAGE_MANIFEST,
) -> bool:
    """Use Parquet only when the manifest proves it matches the current CSV."""
    if not (csv_path.exists() and parquet_path.exists() and manifest_path.exists()):
        return False
    manifest = pd.read_csv(manifest_path)
    row = manifest[(manifest["layer"] == "raw") & (manifest["table_name"] == table_name)]
    if len(row) != 1:
        return False
    source_hash = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    parquet_hash = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    return bool(
        row.iloc[0]["source_sha256"] == source_hash
        and row.iloc[0]["parquet_sha256"] == parquet_hash
    )


def load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create schema-defined raw tables and load their CSV extracts.

    Loading into the physical schema, instead of relying on ``read_csv_auto``
    alone, enforces column order, data types, primary keys, and SQL checks at
    the first transformation boundary.
    """
    for table_name in reversed(RAW_TABLE_FILES):
        table = _validate_identifier(table_name)
        con.execute(f"DROP TABLE IF EXISTS {table}")

    schema_sql = (SQL_DIR / "01_schema.sql").read_text(encoding="utf-8")
    con.execute(schema_sql)

    for table_name, csv_path in RAW_TABLE_FILES.items():
        table = _validate_identifier(table_name)
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing raw input: {csv_path}")
        parquet_path = LAKE_RAW / f"{table_name}.parquet"
        if _parquet_is_current(table_name, csv_path, parquet_path):
            con.execute(
                f"INSERT INTO {table} SELECT * FROM read_parquet(?)",
                [str(parquet_path)],
            )
        else:
            con.execute(
                f"COPY {table} FROM ? (FORMAT CSV, HEADER TRUE)",
                [str(csv_path)],
            )


def load_csv_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    csv_path: Path,
) -> None:
    """Load a governed repository CSV as a DuckDB table."""
    table = _validate_identifier(table_name)
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing table input: {csv_path}")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv_auto(?, HEADER=TRUE)
        """,
        [str(csv_path)],
    )
