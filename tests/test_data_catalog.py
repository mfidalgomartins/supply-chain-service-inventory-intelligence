"""Data-catalog profiling and lineage graph integrity."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest
from src.data_catalog import (
    AssetSpec,
    build_catalog,
    default_asset_specs,
    materialize_catalog,
    publish_catalogued_run,
    validate_lineage,
)
from src.object_store import LocalObjectStore


def test_catalog_profiles_schema_hash_watermark_and_lineage(tmp_path) -> None:
    source_path = tmp_path / "source.csv"
    derived_path = tmp_path / "derived.csv"
    pd.DataFrame(
        {"event_date": ["2025-01-01", "2025-01-02"], "id": [1, 2], "value": [3.0, 4.0]}
    ).to_csv(source_path, index=False)
    pd.DataFrame({"id": [1, 2], "score": [30.0, 40.0]}).to_csv(derived_path, index=False)
    specs = [
        AssetSpec(
            asset_name="source",
            path=source_path,
            asset_type="source",
            producer_stage="ingestion",
            watermark_column="event_date",
        ),
        AssetSpec(
            asset_name="derived",
            path=derived_path,
            asset_type="table",
            producer_stage="scoring",
            parents=("source",),
        ),
    ]

    catalog, lineage = build_catalog(specs, run_id="run-123")

    source = catalog.set_index("asset_name").loc["source"]
    assert source["row_count"] == 2
    assert source["column_count"] == 3
    assert source["content_sha256"] == hashlib.sha256(source_path.read_bytes()).hexdigest()
    assert source["schema_sha256"]
    assert source["watermark_min"] == "2025-01-01"
    assert source["watermark_max"] == "2025-01-02"
    assert lineage.to_dict("records") == [
        {
            "parent_asset": "source",
            "child_asset": "derived",
            "producer_stage": "scoring",
            "run_id": "run-123",
        }
    ]


@pytest.mark.parametrize("problem", ["duplicate", "missing_parent", "cycle"])
def test_lineage_rejects_invalid_graphs(tmp_path, problem: str) -> None:
    path = tmp_path / "asset.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    if problem == "duplicate":
        specs = [
            AssetSpec("a", path, "source", "ingestion"),
            AssetSpec("a", path, "table", "scoring"),
        ]
    elif problem == "missing_parent":
        specs = [AssetSpec("a", path, "table", "scoring", parents=("missing",))]
    else:
        specs = [
            AssetSpec("a", path, "table", "one", parents=("b",)),
            AssetSpec("b", path, "table", "two", parents=("a",)),
        ]

    with pytest.raises(ValueError):
        validate_lineage(specs)


def test_catalog_is_deterministic_and_materialized_without_self_reference(tmp_path) -> None:
    source_path = tmp_path / "source.csv"
    source_path.write_text("id\n1\n", encoding="utf-8")
    specs = [AssetSpec("source", source_path, "source", "ingestion")]

    first = build_catalog(specs, run_id="stable-run")
    second = build_catalog(specs, run_id="stable-run")
    pd.testing.assert_frame_equal(first[0], second[0])
    pd.testing.assert_frame_equal(first[1], second[1])

    catalog, lineage = materialize_catalog(specs, run_id="stable-run", output_dir=tmp_path)
    assert (tmp_path / "data_catalog.csv").exists()
    assert (tmp_path / "data_lineage.csv").exists()
    assert set(catalog["asset_name"]) == {"source"}
    assert lineage.empty


def test_catalog_fails_when_required_asset_is_missing(tmp_path) -> None:
    specs = [AssetSpec("missing", tmp_path / "missing.csv", "source", "ingestion")]

    with pytest.raises(FileNotFoundError):
        build_catalog(specs, run_id="run")


def test_default_registry_covers_every_non_meta_table_contract() -> None:
    specs = default_asset_specs()

    validate_lineage(specs)
    assert len(specs) == 41
    assert len({spec.asset_name for spec in specs}) == 41
    assert all(spec.asset_type == "source" or spec.parents for spec in specs)


def test_catalog_publication_promotes_latest_only_after_immutable_manifest(tmp_path) -> None:
    source_path = tmp_path / "source.csv"
    source_path.write_text("id\n1\n", encoding="utf-8")
    specs = [AssetSpec("source", source_path, "source", "ingestion")]
    catalog, lineage = materialize_catalog(specs, run_id="run-1", output_dir=tmp_path)
    store = LocalObjectStore(tmp_path / "objects")

    manifest, descriptor = publish_catalogued_run(
        catalog,
        lineage,
        store=store,
        run_id="run-1",
        output_dir=tmp_path,
    )

    assert set(manifest["asset_name"]) == {"source", "data_catalog", "data_lineage"}
    assert store.read_verified(descriptor.key, descriptor.sha256)
    latest = pd.read_json(tmp_path / "objects/pointers/latest.json", typ="series")
    assert latest["run_id"] == "run-1"
    assert latest["manifest_object_key"] == descriptor.key
    assert (tmp_path / "object_publication_manifest.csv").exists()
