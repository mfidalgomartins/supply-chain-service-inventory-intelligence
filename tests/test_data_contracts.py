"""Contract-check primitives against small in-memory tables: missing columns,
duplicate keys, out-of-range values, invalid domains, and broken foreign-key
references must each fail cleanly and specifically."""

from __future__ import annotations

import pandas as pd
from src.data_contracts import evaluate_dataframe_contract, evaluate_reference_contract


def _contract() -> dict:
    return {
        "name": "sample_table",
        "required_columns": ["id", "value", "qty"],
        "unique_key": ["id"],
        "critical_columns": ["id", "value"],
        "non_negative": ["qty"],
        "value_ranges": {"value": {"min": 0, "max": 20}},
        "allowed_values": {"status": ["active", "inactive"]},
    }


def test_contract_passes_for_valid_dataframe() -> None:
    df = pd.DataFrame(
        {"id": [1, 2], "value": [10.0, 12.0], "qty": [1, 2], "status": ["active", "inactive"]}
    )
    out = evaluate_dataframe_contract(df, _contract())
    assert all(r.status == "PASS" for r in out)


def test_contract_fails_on_missing_columns() -> None:
    df = pd.DataFrame({"id": [1], "value": [10.0], "status": ["active"]})
    out = evaluate_dataframe_contract(df, _contract())
    assert out[0].status == "FAIL"
    assert out[0].check_name == "required_columns_present"


def test_contract_fails_cleanly_when_constraint_column_is_missing() -> None:
    contract = _contract()
    contract["required_columns"] = ["id", "value", "qty"]
    df = pd.DataFrame({"id": [1], "value": [10.0], "qty": [1]})

    out = evaluate_dataframe_contract(df, contract)

    assert len(out) == 1
    assert out[0].status == "FAIL"
    assert "status" in out[0].details


def test_contract_detects_duplicates_and_negative_values() -> None:
    df = pd.DataFrame(
        {"id": [1, 1], "value": [10.0, 9.5], "qty": [1, -1], "status": ["active", "inactive"]}
    )
    out = evaluate_dataframe_contract(df, _contract())
    status = {r.check_name: r.status for r in out}
    assert status["unique_key_duplicates"] == "FAIL"
    assert status["non_negative_fields"] == "FAIL"


def test_contract_detects_invalid_domain_values() -> None:
    df = pd.DataFrame({"id": [1], "value": [21.0], "qty": [1], "status": ["unknown"]})
    out = evaluate_dataframe_contract(df, _contract())
    status = {r.check_name: r.status for r in out}
    assert status["value_value_range"] == "FAIL"
    assert status["status_allowed_values"] == "FAIL"


def test_reference_contract_detects_missing_foreign_keys() -> None:
    tables = {
        "orders": pd.DataFrame({"supplier_id": ["SUP-001", "SUP-404"]}),
        "suppliers": pd.DataFrame({"supplier_id": ["SUP-001"]}),
    }
    relationship = {
        "source_table": "orders",
        "source_column": "supplier_id",
        "target_table": "suppliers",
        "target_column": "supplier_id",
    }
    out = evaluate_reference_contract(tables, relationship)
    assert out.status == "FAIL"
    assert out.observed == "1"
