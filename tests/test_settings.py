"""Versioned pipeline configuration validation."""

from __future__ import annotations

import json

import pytest
from src.settings import DEFAULT_CONFIG_PATH, load_settings


def _write_config(tmp_path, changes: list[tuple[tuple[str, ...], object]]):
    payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    for keys, value in changes:
        target = payload
        for key in keys[:-1]:
            target = target[key]
        target[keys[-1]] = value
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def test_default_settings_load_every_advanced_capability() -> None:
    settings = load_settings()

    assert settings.version == 2
    assert settings.adapter.type == "synthetic"
    assert settings.storage.incremental is True
    assert len(settings.backtesting.policies) == 3
    assert len(settings.monte_carlo.variants) == 4
    assert settings.monte_carlo.demand_block_days == 7
    assert settings.action_tracking.measurement_window_days == 90
    assert settings.source_governance.enabled is True
    assert settings.source_governance.as_of_date == "2025-12-31"
    assert len(settings.source_governance.table_policies) == 11
    assert settings.source_governance.table_policies["demand_history"].watermark_column == "date"
    assert settings.causal_evaluation.permutations == 999
    assert settings.causal_evaluation.min_units_per_group == 12
    assert settings.network_optimization.horizon_days == 30
    assert settings.network_optimization.mip_relative_gap == 0.001
    assert settings.orchestration.object_store.backend == "local"
    assert settings.orchestration.max_retries == 2


def test_settings_apply_deployment_overrides(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_AS_OF_DATE", "2026-01-15")
    monkeypatch.setenv("OBJECT_STORE_BACKEND", "s3")
    monkeypatch.setenv("OBJECT_STORE_ROOT", "/var/lib/supply-chain-objects")
    monkeypatch.setenv("S3_BUCKET", "analytics-prod")
    monkeypatch.setenv("S3_PREFIX", "inventory/runs")
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://objects.example.test")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")

    settings = load_settings()

    assert settings.source_governance.as_of_date == "2026-01-15"
    assert settings.orchestration.object_store.backend == "s3"
    assert settings.orchestration.object_store.local_root == "/var/lib/supply-chain-objects"
    assert settings.orchestration.object_store.s3_bucket == "analytics-prod"
    assert settings.orchestration.object_store.s3_prefix == "inventory/runs"
    assert settings.orchestration.object_store.s3_endpoint_url == "https://objects.example.test"
    assert settings.orchestration.object_store.s3_region == "eu-west-1"


def test_settings_reject_unsupported_version(tmp_path) -> None:
    payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    payload["version"] = 999
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported pipeline config version"):
        load_settings(config_path)


def test_settings_require_governance_for_every_canonical_source(tmp_path) -> None:
    payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    payload["source_governance"]["table_policies"].pop("network_lanes")
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="network_lanes"):
        load_settings(config_path)


def test_settings_reject_unknown_source_governance_policy(tmp_path) -> None:
    payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    payload["source_governance"]["table_policies"]["unknown_table"] = {
        "source_system": "ERP",
        "owner": "Nobody",
        "watermark_column": None,
        "max_lag_hours": None,
        "stale_severity": "WARN",
    }
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown_table"):
        load_settings(config_path)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ([(("adapter", "column_mapping"), {"unknown": {}})], "unknown tables"),
        (
            [
                (
                    ("adapter", "column_mapping"),
                    {"products": {"sku": "product_id", "item": "product_id"}},
                )
            ],
            "duplicate canonical targets",
        ),
        ([(("adapter", "type"), "unknown")], "adapter.type"),
        ([(("adapter", "file_format"), "xml")], "file_format"),
        ([(("adapter", "type"), "directory")], "directory adapter"),
        ([(("adapter", "type"), "erp_wms")], "erp_wms adapter"),
        ([(("storage", "compression"), "brotli")], "compression"),
        ([(("backtesting", "lookback_days"), 0)], "lookback_days"),
        ([(("backtesting", "annual_holding_cost_rate"), -1)], "economic costs"),
        ([(("backtesting", "service_tolerance"), 2)], "service_tolerance"),
        ([(("backtesting", "fold_start_dates"), [])], "fold"),
        ([(("backtesting", "fold_start_dates"), ["2025-01-01", "2025-01-01"])], "unique"),
        ([(("backtesting", "fold_start_dates"), ["01/01/2025"])], "YYYY-MM-DD"),
        ([(("backtesting", "policies"), [])], "policy"),
        (
            [
                (
                    ("backtesting", "policies"),
                    [
                        {"policy_id": "same", "safety_stock_factor": 1.0, "cycle_stock_days": 7},
                        {"policy_id": "same", "safety_stock_factor": 1.2, "cycle_stock_days": 14},
                    ],
                )
            ],
            "unique",
        ),
        ([(("monte_carlo", "simulations"), 0)], "simulations"),
        ([(("monte_carlo", "demand_block_days"), 0)], "demand_block_days"),
        ([(("monte_carlo", "target_confidence"), 0)], "target_confidence"),
        ([(("monte_carlo", "max_inventory_increase_rate"), -1)], "max_inventory"),
        ([(("monte_carlo", "ordering_cost"), -1)], "economic costs"),
        ([(("monte_carlo", "variants"), [])], "variant"),
        (
            [
                (
                    ("monte_carlo", "variants"),
                    [
                        {
                            "scenario_id": "same",
                            "safety_factor_multiplier": 1.0,
                            "cycle_days_delta": 0,
                        },
                        {
                            "scenario_id": "same",
                            "safety_factor_multiplier": 1.2,
                            "cycle_days_delta": 7,
                        },
                    ],
                )
            ],
            "unique",
        ),
        ([(("action_tracking", "measurement_window_days"), 0)], "measurement_window_days"),
        ([(("source_governance", "as_of_date"), "15/01/2026")], "as_of_date"),
        (
            [
                (
                    ("source_governance", "table_policies", "demand_history", "max_lag_hours"),
                    0,
                )
            ],
            "max_lag_hours",
        ),
        (
            [
                (
                    ("source_governance", "table_policies", "demand_history", "stale_severity"),
                    "IGNORE",
                )
            ],
            "stale_severity",
        ),
        ([(("causal_evaluation", "permutations"), 0)], "permutations"),
        ([(("causal_evaluation", "min_units_per_group"), 1)], "min_units_per_group"),
        ([(("causal_evaluation", "alpha"), 0)], "causal_evaluation.alpha"),
        ([(("network_optimization", "horizon_days"), 0)], "horizon_days"),
        ([(("network_optimization", "mip_relative_gap"), -0.1)], "mip_relative_gap"),
        ([(("network_optimization", "time_limit_seconds"), 0)], "time_limit_seconds"),
        ([(("orchestration", "max_retries"), -1)], "max_retries"),
        ([(("orchestration", "retry_base_seconds"), 0)], "retry_base_seconds"),
        ([(("orchestration", "object_store", "backend"), "ftp")], "object_store.backend"),
    ],
)
def test_settings_reject_invalid_operating_parameters(tmp_path, changes, message) -> None:
    with pytest.raises(ValueError, match=message):
        load_settings(_write_config(tmp_path, changes))


def test_settings_reject_invalid_policy_and_scenario_factors(tmp_path) -> None:
    policy_payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))["backtesting"][
        "policies"
    ]
    policy_payload[0]["safety_stock_factor"] = 0
    with pytest.raises(ValueError, match="safety_stock_factor"):
        load_settings(_write_config(tmp_path, [(("backtesting", "policies"), policy_payload)]))

    variant_payload = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))["monte_carlo"][
        "variants"
    ]
    variant_payload[0]["safety_factor_multiplier"] = 0
    with pytest.raises(ValueError, match="safety_factor_multiplier"):
        load_settings(_write_config(tmp_path, [(("monte_carlo", "variants"), variant_payload)]))


def test_s3_object_store_requires_bucket(tmp_path) -> None:
    config_path = _write_config(
        tmp_path,
        [
            (("orchestration", "object_store", "backend"), "s3"),
            (("orchestration", "object_store", "s3_bucket"), None),
        ],
    )

    with pytest.raises(ValueError, match="s3_bucket"):
        load_settings(config_path)
