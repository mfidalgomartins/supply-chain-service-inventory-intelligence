"""Action-event schema validation."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from src.action_tracking import (
    _action_timeseries_rows,
    _supplier_scores_for_window,
    load_action_events,
)


def test_action_events_reject_duplicate_ids(tmp_path) -> None:
    action = {
        "action_id": "ACT-1",
        "entity_type": "supplier",
        "supplier_id": "SUP-001",
        "action_type": "recovery",
        "owner": "Procurement",
        "status": "implemented",
        "decision_date": "2025-01-01",
        "implementation_date": "2025-02-01",
        "target_metric": "fill_rate",
        "target_value": 0.95,
    }
    path = tmp_path / "actions.json"
    path.write_text(
        json.dumps({"version": 1, "actions": [action, action]}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="action_id must be unique"):
        load_action_events(path)


def _write_events(tmp_path, payload: dict):
    path = tmp_path / "actions.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _valid_action() -> dict:
    return {
        "action_id": "ACT-1",
        "entity_type": "supplier",
        "supplier_id": "SUP-001",
        "action_type": "recovery",
        "owner": "Procurement",
        "status": "implemented",
        "decision_date": "2025-01-01",
        "implementation_date": "2025-02-01",
        "target_metric": "fill_rate",
        "target_value": 0.95,
    }


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"version": 2, "actions": [_valid_action()]}, "schema version"),
        ({"version": 1, "actions": []}, "no actions"),
        (
            {"version": 1, "actions": [{k: v for k, v in _valid_action().items() if k != "owner"}]},
            "fields missing",
        ),
        (
            {"version": 1, "actions": [{**_valid_action(), "status": "unknown"}]},
            "status is invalid",
        ),
        (
            {"version": 1, "actions": [{**_valid_action(), "entity_type": "warehouse"}]},
            "entity_type is invalid",
        ),
        (
            {"version": 1, "actions": [{**_valid_action(), "target_metric": "observation_days"}]},
            "target metrics",
        ),
        (
            {"version": 1, "actions": [{**_valid_action(), "target_value": "not-a-number"}]},
            "finite number",
        ),
        (
            {"version": 1, "actions": [{**_valid_action(), "target_value": 1.1}]},
            "Rate target_value",
        ),
        (
            {
                "version": 1,
                "actions": [
                    {
                        **_valid_action(),
                        "decision_date": "2025-03-01",
                        "implementation_date": "2025-02-01",
                    }
                ],
            },
            "decision_date",
        ),
    ],
)
def test_action_events_reject_invalid_lifecycle_payloads(tmp_path, payload, message) -> None:
    with pytest.raises(ValueError, match=message):
        load_action_events(_write_events(tmp_path, payload))


def test_supplier_window_scores_exclude_future_receipts_and_use_neutral_missing_evidence() -> None:
    suppliers = pd.DataFrame(
        {
            "supplier_id": ["S1", "S2"],
            "supplier_name": ["Observed", "No receipts"],
        }
    )
    purchase_orders = pd.DataFrame(
        {
            "supplier_id": ["S1", "S1"],
            "order_date": pd.to_datetime(["2025-01-01", "2025-04-01"]),
            "expected_arrival_date": pd.to_datetime(["2025-01-05", "2025-04-05"]),
            "actual_arrival_date": pd.to_datetime(["2025-01-05", "2025-04-10"]),
            "late_delivery_flag": [0, 1],
            "ordered_units": [10, 10],
            "received_units": [9, 5],
        }
    )

    scores = _supplier_scores_for_window(
        purchase_orders,
        suppliers,
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-03-31"),
    ).set_index("supplier_id")

    assert scores.loc["S1", "on_time_delivery_rate"] == 1.0
    assert scores.loc["S1", "received_vs_ordered_fill_rate"] == 0.9
    assert scores.loc["S2", "supplier_risk_score_base"] == pytest.approx(50.0)


def test_action_timeseries_splits_midmonth_implementation_period() -> None:
    scope = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-14", "2025-01-15"]),
            "units_demanded": [10, 10],
            "units_fulfilled": [9, 10],
            "units_lost_sales": [1, 0],
            "lost_sales_revenue": [5.0, 0.0],
            "lost_margin_proxy": [2.0, 0.0],
            "inventory_value": [100.0, 90.0],
        }
    )

    rows = _action_timeseries_rows(scope, "ACT-1", pd.Timestamp("2025-01-15"))

    assert {(row["month"], row["period"]) for row in rows} == {
        ("2025-01", "pre"),
        ("2025-01", "post"),
    }
