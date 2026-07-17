"""Incremental Parquet merge behavior."""

from __future__ import annotations

import json

import duckdb
import pytest
from src import storage
from src.settings import DEFAULT_CONFIG_PATH
from src.storage import _incremental_merge


def test_incremental_merge_upserts_and_preserves_existing_rows(tmp_path) -> None:
    parquet_path = tmp_path / "fact.parquet"
    setup = duckdb.connect(database=":memory:")
    try:
        setup.execute("CREATE TABLE existing(id INTEGER, value VARCHAR)")
        setup.execute("INSERT INTO existing VALUES (1, 'old'), (2, 'keep')")
        setup.execute(
            "COPY existing TO ? (FORMAT PARQUET)",
            [str(parquet_path)],
        )
    finally:
        setup.close()

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("CREATE TABLE incoming_data(id INTEGER, value VARCHAR)")
        con.execute("INSERT INTO incoming_data VALUES (1, 'new'), (3, 'insert')")
        prior_rows = _incremental_merge(con, parquet_path, ("id",))
        rows = con.execute("SELECT * FROM output_data ORDER BY id").fetchall()
    finally:
        con.close()

    assert prior_rows == 2
    assert rows == [(1, "new"), (2, "keep"), (3, "insert")]


def test_sync_rebuilds_tampered_parquet_instead_of_trusting_source_hash(
    tmp_path, monkeypatch
) -> None:
    source_path = tmp_path / "source.csv"
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "outputs"
    source_path.write_text("id,value\n1,original\n", encoding="utf-8")

    monkeypatch.setattr(storage, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(storage, "LAKE_RAW", lake_path)
    monkeypatch.setattr(storage, "OUTPUT_TABLES_DIR", output_path)
    monkeypatch.setattr(storage, "MANIFEST_PATH", output_path / "storage_manifest.csv")
    monkeypatch.setattr(
        storage,
        "RAW_SPECS",
        (storage.TableSpec("raw", "sample", source_path, ("id",)),),
    )

    storage.sync_layer("raw")
    parquet_path = lake_path / "sample.parquet"
    parquet_path.write_bytes(b"tampered")

    manifest = storage.sync_layer("raw")
    restored = duckdb.sql("SELECT * FROM read_parquet(?)", params=[str(parquet_path)]).df()

    assert manifest.iloc[0]["refresh_mode"] == "full_replace"
    assert restored.to_dict(orient="records") == [{"id": 1, "value": "original"}]
    assert manifest.iloc[0]["parquet_sha256"] == storage._sha256(parquet_path)

    payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    payload["storage"]["compression"] = "snappy"
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    recompressed = storage.sync_layer("raw", config_path)

    assert recompressed.iloc[0]["refresh_mode"] == "full_replace"
    assert recompressed.iloc[0]["compression"] == "snappy"


def test_sync_rejects_null_business_keys(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source.csv"
    source_path.write_text("id,value\n,invalid\n", encoding="utf-8")
    output_path = tmp_path / "outputs"

    monkeypatch.setattr(storage, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(storage, "LAKE_RAW", tmp_path / "lake")
    monkeypatch.setattr(storage, "OUTPUT_TABLES_DIR", output_path)
    monkeypatch.setattr(storage, "MANIFEST_PATH", output_path / "storage_manifest.csv")
    monkeypatch.setattr(
        storage,
        "RAW_SPECS",
        (storage.TableSpec("raw", "sample", source_path, ("id",)),),
    )

    with pytest.raises(ValueError, match="null business keys"):
        storage.sync_layer("raw")
