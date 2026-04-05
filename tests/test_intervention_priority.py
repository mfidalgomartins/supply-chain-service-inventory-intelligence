from __future__ import annotations

import pandas as pd

from src.intervention_tracker import _apply_priority_order, _status_priority


def test_status_priority_mapping_order():
    assert _status_priority("Open") < _status_priority("Monitor")
    assert _status_priority("In Progress") < _status_priority("Closed")


def test_apply_priority_order_open_first_then_score():
    df = pd.DataFrame(
        {
            "intervention_status": ["Monitor", "Open", "Open", "Monitor"],
            "governance_priority_score": [99.0, 60.0, 70.0, 40.0],
            "expected_value_proxy": [1000.0, 900.0, 800.0, 700.0],
        }
    )

    ordered = _apply_priority_order(df)

    assert ordered.iloc[0]["intervention_status"] == "Open"
    assert ordered.iloc[1]["intervention_status"] == "Open"
    assert ordered.iloc[0]["governance_priority_score"] >= ordered.iloc[1]["governance_priority_score"]
