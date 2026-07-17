"""Pre-publication source freshness and schema-drift governance."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import PROJECT_ROOT
from src.settings import SourceGovernanceSettings, SourceTablePolicy

CHECK_COLUMNS = [
    "table_name",
    "check_name",
    "severity",
    "status",
    "observed",
    "expected",
    "details",
]
REGISTRY_COLUMNS = [
    "table_name",
    "source_system",
    "owner",
    "column_name",
    "data_type",
    "nullable",
    "ordinal_position",
    "schema_fingerprint",
    "observed_as_of",
]
DRIFT_COLUMNS = [
    "table_name",
    "drift_type",
    "severity",
    "column_name",
    "previous_type",
    "current_type",
    "details",
    "observed_as_of",
]


class SourceReadinessError(ValueError):
    """Raised when a blocker-level source check fails before publication."""


def _schema_types(frame: pd.DataFrame) -> dict[str, str]:
    return {str(column): str(frame[column].dtype) for column in sorted(frame.columns)}


def schema_fingerprint(frame: pd.DataFrame) -> str:
    """Hash a normalized name/type schema; physical column order is immaterial."""
    payload = json.dumps(_schema_types(frame), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _registry_rows(
    table_name: str,
    frame: pd.DataFrame,
    policy: SourceTablePolicy,
    as_of_date: str,
) -> list[dict[str, Any]]:
    fingerprint = schema_fingerprint(frame)
    return [
        {
            "table_name": table_name,
            "source_system": policy.source_system,
            "owner": policy.owner,
            "column_name": column,
            "data_type": str(frame[column].dtype),
            "nullable": bool(frame[column].isna().any()),
            "ordinal_position": position,
            "schema_fingerprint": fingerprint,
            "observed_as_of": as_of_date,
        }
        for position, column in enumerate(sorted(frame.columns), start=1)
    ]


def _prior_types(previous_registry: pd.DataFrame | None, table_name: str) -> dict[str, str]:
    if previous_registry is None or previous_registry.empty:
        return {}
    required = {"table_name", "column_name", "data_type"}
    missing = required - set(previous_registry.columns)
    if missing:
        raise ValueError(
            "Previous source schema registry is missing columns: " + ", ".join(sorted(missing))
        )
    table = previous_registry[previous_registry["table_name"] == table_name]
    if table["column_name"].duplicated().any():
        raise ValueError(f"Previous source schema registry duplicates columns for {table_name}")
    return dict(zip(table["column_name"].astype(str), table["data_type"].astype(str), strict=True))


def _drift_rows(
    table_name: str,
    frame: pd.DataFrame,
    required_columns: set[str],
    previous_registry: pd.DataFrame | None,
    as_of_date: str,
) -> list[dict[str, Any]]:
    current_types = _schema_types(frame)
    previous_types = _prior_types(previous_registry, table_name)
    rows: list[dict[str, Any]] = []

    def add(
        drift_type: str,
        severity: str,
        column: str,
        previous_type: str | None,
        current_type: str | None,
        details: str,
    ) -> None:
        rows.append(
            {
                "table_name": table_name,
                "drift_type": drift_type,
                "severity": severity,
                "column_name": column,
                "previous_type": previous_type,
                "current_type": current_type,
                "details": details,
                "observed_as_of": as_of_date,
            }
        )

    for column in sorted(required_columns - set(current_types)):
        add(
            "required_column_missing",
            "FAIL",
            column,
            previous_types.get(column),
            None,
            "A contract-required column is absent from the mapped source schema.",
        )
    if previous_types:
        for column in sorted(set(previous_types) - set(current_types) - required_columns):
            add(
                "column_removed",
                "FAIL",
                column,
                previous_types[column],
                None,
                "A column present in the last successful schema was removed.",
            )
        for column in sorted(set(current_types) - set(previous_types)):
            add(
                "column_added",
                "WARN",
                column,
                None,
                current_types[column],
                "A column not present in the last successful schema was added.",
            )
        for column in sorted(set(current_types) & set(previous_types)):
            if current_types[column] != previous_types[column]:
                add(
                    "type_changed",
                    "FAIL",
                    column,
                    previous_types[column],
                    current_types[column],
                    "The observed pandas data type changed since the last successful schema.",
                )
    return rows


def _check(
    table_name: str,
    check_name: str,
    severity: str,
    status: str,
    observed: str,
    expected: str,
    details: str,
) -> dict[str, str]:
    return {
        "table_name": table_name,
        "check_name": check_name,
        "severity": severity,
        "status": status,
        "observed": observed,
        "expected": expected,
        "details": details,
    }


def _freshness_check(
    table_name: str,
    frame: pd.DataFrame,
    policy: SourceTablePolicy,
    as_of_date: str,
) -> dict[str, str]:
    if policy.watermark_column is None or policy.max_lag_hours is None:
        return _check(
            table_name,
            "freshness_sla",
            policy.stale_severity,
            "PASS",
            "not configured",
            "not configured",
            "This table has no business-watermark freshness SLA.",
        )
    if policy.watermark_column not in frame.columns:
        return _check(
            table_name,
            "freshness_sla",
            policy.stale_severity,
            "FAIL",
            "watermark missing",
            policy.watermark_column,
            "The configured business watermark column is absent.",
        )
    watermarks = pd.to_datetime(frame[policy.watermark_column], errors="coerce")
    if watermarks.isna().all():
        return _check(
            table_name,
            "freshness_sla",
            policy.stale_severity,
            "FAIL",
            "no valid watermark",
            f"<= {policy.max_lag_hours} hours",
            "No valid timestamp is available for freshness evaluation.",
        )
    maximum = watermarks.max()
    as_of = pd.Timestamp(as_of_date)
    lag_hours = max(0.0, float((as_of - maximum).total_seconds() / 3600))
    is_fresh = lag_hours <= policy.max_lag_hours
    return _check(
        table_name,
        "freshness_sla",
        policy.stale_severity,
        "PASS" if is_fresh else policy.stale_severity,
        f"{lag_hours:.1f} hours",
        f"<= {policy.max_lag_hours} hours",
        f"Latest {policy.watermark_column}: {maximum.isoformat()}.",
    )


def evaluate_source_readiness(
    tables: dict[str, pd.DataFrame],
    contracts: list[dict[str, Any]],
    governance: SourceGovernanceSettings,
    *,
    previous_registry: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Evaluate mapped source tables without mutating the canonical layer."""
    if not governance.enabled:
        checks = pd.DataFrame(
            [
                _check(
                    "_pipeline",
                    "source_governance_enabled",
                    "INFO",
                    "SKIP",
                    "false",
                    "true",
                    "Source governance is disabled by versioned configuration.",
                )
            ],
            columns=CHECK_COLUMNS,
        )
        return (
            checks,
            pd.DataFrame(columns=REGISTRY_COLUMNS),
            pd.DataFrame(columns=DRIFT_COLUMNS),
        )

    check_rows: list[dict[str, str]] = []
    registry_rows: list[dict[str, Any]] = []
    drift_rows: list[dict[str, Any]] = []
    for contract in sorted(contracts, key=lambda item: str(item["name"])):
        table_name = str(contract["name"])
        policy = governance.table_policies.get(table_name)
        if policy is None:
            check_rows.append(
                _check(
                    table_name,
                    "source_policy_present",
                    "CRITICAL",
                    "FAIL",
                    "missing",
                    "configured",
                    "No source-governance policy exists for this canonical table.",
                )
            )
            continue
        frame = tables.get(table_name)
        if frame is None:
            check_rows.append(
                _check(
                    table_name,
                    "source_table_present",
                    "CRITICAL",
                    "FAIL",
                    "missing",
                    "present",
                    "The source adapter did not provide this canonical table.",
                )
            )
            continue

        required = set(map(str, contract.get("required_columns", [])))
        missing = sorted(required - set(frame.columns))
        check_rows.append(
            _check(
                table_name,
                "required_schema",
                "CRITICAL",
                "FAIL" if missing else "PASS",
                ", ".join(missing) if missing else "0 missing",
                "0 missing",
                "Mapped source columns must satisfy the canonical contract.",
            )
        )
        table_drift = _drift_rows(
            table_name, frame, required, previous_registry, governance.as_of_date
        )
        drift_rows.extend(table_drift)
        drift_severities = {row["severity"] for row in table_drift}
        drift_status = (
            "FAIL"
            if "FAIL" in drift_severities
            else "WARN"
            if "WARN" in drift_severities
            else "PASS"
        )
        check_rows.append(
            _check(
                table_name,
                "schema_drift",
                "CRITICAL",
                drift_status,
                str(len(table_drift)),
                "0 breaking changes",
                "Schema changes are compared with the last successful registry.",
            )
        )

        unique_key = list(map(str, contract.get("unique_key", [])))
        if unique_key and set(unique_key).issubset(frame.columns):
            duplicates = int(frame.duplicated(unique_key).sum())
            nulls = int(frame[unique_key].isna().sum().sum())
        elif unique_key:
            duplicates = 0
            nulls = len(frame)
        else:
            duplicates = 0
            nulls = 0
        check_rows.append(
            _check(
                table_name,
                "business_key_integrity",
                "CRITICAL",
                "PASS" if duplicates == 0 and nulls == 0 else "FAIL",
                f"duplicates={duplicates}, nulls={nulls}",
                "duplicates=0, nulls=0",
                "Business keys must be complete and unique before publication.",
            )
        )
        check_rows.append(_freshness_check(table_name, frame, policy, governance.as_of_date))
        registry_rows.extend(_registry_rows(table_name, frame, policy, governance.as_of_date))

    checks = pd.DataFrame(check_rows, columns=CHECK_COLUMNS).sort_values(
        ["table_name", "check_name"], ignore_index=True
    )
    registry = pd.DataFrame(registry_rows, columns=REGISTRY_COLUMNS).sort_values(
        ["table_name", "column_name"], ignore_index=True
    )
    drift = pd.DataFrame(drift_rows, columns=DRIFT_COLUMNS).sort_values(
        ["table_name", "drift_type", "column_name"], ignore_index=True
    )
    return checks, registry, drift


def _resolve_registry(path_value: str) -> Path:
    path = Path(path_value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def publish_source_readiness(
    tables: dict[str, pd.DataFrame],
    contracts: list[dict[str, Any]],
    governance: SourceGovernanceSettings,
    *,
    output_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Persist readiness evidence and promote a schema registry only on success."""
    registry_path = _resolve_registry(governance.registry_path)
    previous_registry = pd.read_csv(registry_path) if registry_path.exists() else None
    checks, registry, drift = evaluate_source_readiness(
        tables,
        contracts,
        governance,
        previous_registry=previous_registry,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    checks.to_csv(output_dir / "source_readiness_checks.csv", index=False)
    drift.to_csv(output_dir / "source_schema_drift_events.csv", index=False)
    failures = checks[checks["status"] == "FAIL"]
    if not failures.empty:
        failed_checks = ", ".join(
            f"{row.table_name}.{row.check_name}" for row in failures.itertuples(index=False)
        )
        raise SourceReadinessError(f"Source readiness failed: {failed_checks}")

    if governance.enabled:
        registry_path.parent.mkdir(parents=True, exist_ok=True)
        registry.to_csv(registry_path, index=False)
    return checks, registry, drift
