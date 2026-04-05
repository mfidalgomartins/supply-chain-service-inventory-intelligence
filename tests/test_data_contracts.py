from __future__ import annotations

import pandas as pd

from src.data_contracts import evaluate_dataframe_contract


def _contract() -> dict:
    return {
        "name": "sample_table",
        "required_columns": ["id", "value", "qty"],
        "unique_key": ["id"],
        "critical_columns": ["id", "value"],
        "non_negative": ["qty"],
    }


def test_contract_passes_for_valid_dataframe() -> None:
    df = pd.DataFrame({"id": [1, 2], "value": [10.0, 12.0], "qty": [1, 2]})
    out = evaluate_dataframe_contract(df, _contract())
    assert all(r.status == "PASS" for r in out)


def test_contract_fails_on_missing_columns() -> None:
    df = pd.DataFrame({"id": [1], "value": [10.0]})
    out = evaluate_dataframe_contract(df, _contract())
    assert out[0].status == "FAIL"
    assert out[0].check_name == "required_columns_present"


def test_contract_detects_duplicates_and_negative_values() -> None:
    df = pd.DataFrame({"id": [1, 1], "value": [10.0, 9.5], "qty": [1, -1]})
    out = evaluate_dataframe_contract(df, _contract())
    status = {r.check_name: r.status for r in out}
    assert status["unique_key_duplicates"] == "FAIL"
    assert status["non_negative_fields"] == "FAIL"
