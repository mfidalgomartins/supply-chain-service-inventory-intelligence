"""Schema and loader safeguards at the raw-to-analytics boundary."""

from __future__ import annotations

import hashlib

import duckdb
import pandas as pd
import pytest
from src.config import SQL_DIR
from src.warehouse import _parquet_is_current, _validate_identifier


def test_schema_rejects_inconsistent_demand_balance() -> None:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute((SQL_DIR / "01_schema.sql").read_text(encoding="utf-8"))
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                """
                INSERT INTO demand_history VALUES (
                    DATE '2026-01-01', 'WH-1', 'SKU-1', 'Region',
                    10, 8, 1, 1, 0, 1.0
                )
                """
            )
    finally:
        con.close()


@pytest.mark.parametrize("identifier", ["bad-name", "table; DROP TABLE products", "1table"])
def test_loader_rejects_unsafe_identifiers(identifier: str) -> None:
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier(identifier)


def test_parquet_requires_hash_match_with_current_csv(tmp_path) -> None:
    csv_path = tmp_path / "products.csv"
    parquet_path = tmp_path / "products.parquet"
    manifest_path = tmp_path / "storage_manifest.csv"
    csv_path.write_text("product_id\nP1\n", encoding="utf-8")
    parquet_path.write_bytes(b"parquet-content")

    pd.DataFrame(
        [
            {
                "layer": "raw",
                "table_name": "products",
                "source_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                "parquet_sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
            }
        ]
    ).to_csv(manifest_path, index=False)

    assert _parquet_is_current("products", csv_path, parquet_path, manifest_path)
    csv_path.write_text("product_id\nP2\n", encoding="utf-8")
    assert not _parquet_is_current("products", csv_path, parquet_path, manifest_path)
