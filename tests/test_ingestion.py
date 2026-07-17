"""ERP/WMS adapters normalize external columns into the canonical raw contract."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from src import ingestion
from src.settings import AdapterSettings, SourceGovernanceSettings, SourceTablePolicy


def test_erp_wms_adapter_applies_mapping_and_contracts(tmp_path, monkeypatch) -> None:
    erp = tmp_path / "erp"
    wms = tmp_path / "wms"
    canonical = tmp_path / "canonical"
    erp.mkdir()
    wms.mkdir()
    canonical.mkdir()

    tables = {
        "products": pd.DataFrame(
            [
                {
                    "sku_code": "P1",
                    "product_name": "Product",
                    "category": "Health",
                    "unit_cost": 2.0,
                    "unit_price": 4.0,
                    "shelf_life_days": 100,
                    "supplier_id": "S1",
                    "lead_time_days": 5,
                    "target_service_level": 0.95,
                }
            ]
        ),
        "suppliers": pd.DataFrame(
            [
                {
                    "supplier_id": "S1",
                    "supplier_name": "Supplier",
                    "supplier_region": "EU",
                    "reliability_score": 0.9,
                    "average_lead_time_days": 5,
                    "lead_time_variability": 0.1,
                    "minimum_order_qty": 10,
                }
            ]
        ),
        "warehouses": pd.DataFrame(
            [
                {
                    "warehouse_id": "W1",
                    "warehouse_name": "Warehouse",
                    "region": "EU",
                    "storage_capacity_units": 1000,
                }
            ]
        ),
        "inventory_snapshots": pd.DataFrame(
            [
                {
                    "snapshot_date": "2025-01-01",
                    "warehouse_id": "W1",
                    "product_id": "P1",
                    "on_hand_units": 10,
                    "on_order_units": 0,
                    "reserved_units": 1,
                    "available_units": 9,
                    "inventory_value": 20.0,
                    "days_of_supply": 5.0,
                }
            ]
        ),
        "demand_history": pd.DataFrame(
            [
                {
                    "date": "2025-01-01",
                    "warehouse_id": "W1",
                    "product_id": "P1",
                    "region": "EU",
                    "units_demanded": 5,
                    "units_fulfilled": 4,
                    "units_lost_sales": 1,
                    "stockout_flag": 1,
                    "promo_flag": 0,
                    "seasonality_index": 1.0,
                }
            ]
        ),
        "purchase_orders": pd.DataFrame(
            [
                {
                    "po_id": "PO1",
                    "supplier_id": "S1",
                    "product_id": "P1",
                    "warehouse_id": "W1",
                    "order_date": "2024-12-20",
                    "expected_arrival_date": "2024-12-25",
                    "actual_arrival_date": "2024-12-25",
                    "ordered_units": 10,
                    "received_units": 9,
                    "late_delivery_flag": 0,
                }
            ]
        ),
        "product_classification": pd.DataFrame(
            [{"product_id": "P1", "abc_class": "A", "criticality_level": "High"}]
        ),
    }
    for name, frame in tables.items():
        target = erp if name in ingestion.ERP_TABLES else wms
        frame.to_csv(target / f"{name}.csv", index=False)

    output_paths = {name: canonical / f"{name}.csv" for name in tables}
    monkeypatch.setattr(ingestion, "RAW_TABLE_FILES", output_paths)
    adapter = AdapterSettings(
        type="erp_wms",
        source_path=None,
        erp_path=str(erp),
        wms_path=str(wms),
        file_format="csv",
        column_mapping={"products": {"sku_code": "product_id"}},
    )

    manifest = ingestion.ingest_external_exports(adapter)

    assert len(manifest) == 7
    assert pd.read_csv(output_paths["products"])["product_id"].tolist() == ["P1"]
    assert all(Path(path).exists() for path in output_paths.values())

    tables["products"]["product_id"] = "P1"
    tables["products"].to_csv(erp / "products.csv", index=False)
    with pytest.raises(ValueError, match="duplicate columns"):
        ingestion.ingest_external_exports(adapter)


def test_readiness_failure_does_not_overwrite_canonical(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    canonical = tmp_path / "products.csv"
    canonical.write_text("product_id,unit_price\nSAFE,10\n", encoding="utf-8")
    pd.DataFrame({"product_id": ["P1"]}).to_csv(source / "products.csv", index=False)
    monkeypatch.setattr(ingestion, "RAW_TABLE_FILES", {"products": canonical})
    governance = SourceGovernanceSettings(
        enabled=True,
        as_of_date="2025-01-01",
        registry_path=str(tmp_path / "source_schema_registry.csv"),
        table_policies={
            "products": SourceTablePolicy(
                source_system="ERP",
                owner="Master Data",
                watermark_column=None,
                max_lag_hours=None,
                stale_severity="WARN",
            )
        },
    )
    adapter = AdapterSettings(
        type="directory",
        source_path=str(source),
        erp_path=None,
        wms_path=None,
        file_format="csv",
        column_mapping={},
    )

    with pytest.raises(ValueError, match="Source readiness failed"):
        ingestion.ingest_external_exports(
            adapter,
            governance=governance,
            readiness_output_dir=tmp_path,
        )

    assert canonical.read_text(encoding="utf-8") == "product_id,unit_price\nSAFE,10\n"
    checks = pd.read_csv(tmp_path / "source_readiness_checks.csv")
    assert "FAIL" in set(checks["status"])
    assert not (tmp_path / "source_schema_registry.csv").exists()
