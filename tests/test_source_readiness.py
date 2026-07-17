"""Source freshness, schema drift, and fail-before-publish governance."""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest
from src.settings import SourceGovernanceSettings, SourceTablePolicy
from src.source_readiness import evaluate_source_readiness, schema_fingerprint


def _governance(
    *,
    as_of_date: str = "2025-01-03",
    max_lag_hours: int = 48,
    stale_severity: str = "FAIL",
) -> SourceGovernanceSettings:
    return SourceGovernanceSettings(
        enabled=True,
        as_of_date=as_of_date,
        registry_path="outputs/tables/source_schema_registry.csv",
        table_policies={
            "orders": SourceTablePolicy(
                source_system="ERP",
                owner="Procurement",
                watermark_column="event_date",
                max_lag_hours=max_lag_hours,
                stale_severity=stale_severity,
            )
        },
    )


def _contract() -> dict:
    return {
        "name": "orders",
        "required_columns": ["order_id", "event_date", "units"],
        "unique_key": ["order_id"],
        "critical_columns": ["order_id", "event_date"],
    }


def _orders() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["O1", "O2"],
            "event_date": ["2025-01-02", "2025-01-03"],
            "units": [10, 12],
        }
    )


def test_schema_fingerprint_is_stable_to_column_order() -> None:
    orders = _orders()

    assert schema_fingerprint(orders) == schema_fingerprint(
        orders[["units", "event_date", "order_id"]]
    )


def test_first_observation_passes_schema_freshness_and_key_checks() -> None:
    checks, registry, drift = evaluate_source_readiness(
        {"orders": _orders()},
        [_contract()],
        _governance(),
    )

    assert set(checks["check_name"]) == {
        "required_schema",
        "schema_drift",
        "business_key_integrity",
        "freshness_sla",
    }
    assert (checks["status"] == "PASS").all()
    assert registry["column_name"].tolist() == ["event_date", "order_id", "units"]
    assert drift.empty


def test_additive_drift_warns_without_blocking() -> None:
    first_checks, previous, _ = evaluate_source_readiness(
        {"orders": _orders()}, [_contract()], _governance()
    )
    assert (first_checks["status"] == "PASS").all()
    changed = _orders().assign(source_note="ok")

    checks, _, drift = evaluate_source_readiness(
        {"orders": changed},
        [_contract()],
        _governance(),
        previous_registry=previous,
    )

    assert checks.loc[checks["check_name"] == "schema_drift", "status"].item() == "WARN"
    assert drift[["drift_type", "severity", "column_name"]].to_dict("records") == [
        {"drift_type": "column_added", "severity": "WARN", "column_name": "source_note"}
    ]


@pytest.mark.parametrize("change", ["removed", "type_changed"])
def test_breaking_drift_fails(change: str) -> None:
    _, previous, _ = evaluate_source_readiness({"orders": _orders()}, [_contract()], _governance())
    changed = _orders().copy()
    if change == "removed":
        changed = changed.drop(columns="units")
    else:
        changed["units"] = changed["units"].astype(str)

    checks, _, drift = evaluate_source_readiness(
        {"orders": changed},
        [_contract()],
        _governance(),
        previous_registry=previous,
    )

    assert "FAIL" in set(checks["status"])
    assert "FAIL" in set(drift["severity"])


def test_stale_watermark_uses_configured_severity() -> None:
    stale = _orders().assign(event_date="2025-01-01")
    governance = _governance(as_of_date="2025-01-05", stale_severity="WARN")

    checks, _, _ = evaluate_source_readiness({"orders": stale}, [_contract()], governance)

    freshness = checks.loc[checks["check_name"] == "freshness_sla"].iloc[0]
    assert freshness["status"] == "WARN"
    assert freshness["observed"] == "96.0 hours"
    assert freshness["expected"] == "<= 48 hours"


def test_duplicate_or_null_business_keys_fail() -> None:
    invalid = _orders()
    invalid.loc[1, "order_id"] = "O1"

    checks, _, _ = evaluate_source_readiness({"orders": invalid}, [_contract()], _governance())

    key_check = checks.loc[checks["check_name"] == "business_key_integrity"].iloc[0]
    assert key_check["status"] == "FAIL"
    assert key_check["observed"] == "duplicates=1, nulls=0"


def test_disabled_governance_returns_explicit_skipped_checks() -> None:
    governance = replace(_governance(), enabled=False)

    checks, registry, drift = evaluate_source_readiness(
        {"orders": _orders()}, [_contract()], governance
    )

    assert checks[["check_name", "status"]].to_dict("records") == [
        {"check_name": "source_governance_enabled", "status": "SKIP"}
    ]
    assert registry.empty
    assert drift.empty
