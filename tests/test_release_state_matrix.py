from __future__ import annotations

import pandas as pd

from src.pre_delivery_validation import _compute_release_state_matrix


def _row(check: str, layer: str, status: str, severity: str) -> dict[str, str]:
    return {
        "check_name": check,
        "layer": layer,
        "method": "Python",
        "status": status,
        "severity": severity,
        "observed": "0",
        "expected": "0",
        "details": "",
    }


def test_release_state_decision_support_when_all_pass() -> None:
    checks = pd.DataFrame(
        [
            _row("raw_ok", "raw", "PASS", "HIGH"),
            _row("proc_ok", "processed", "PASS", "HIGH"),
            _row("score_ok", "scoring", "PASS", "HIGH"),
            _row("impact_ok", "impact", "PASS", "MEDIUM"),
            _row("report_ok", "reporting", "PASS", "LOW"),
        ]
    )

    matrix = _compute_release_state_matrix(checks)
    assert matrix["release_classification"].iloc[0] == "decision-support only"
    publish_blocked = matrix.loc[matrix["state_name"] == "publish_blocked", "status"].iloc[0]
    assert publish_blocked == "FAIL"


def test_release_state_publish_blocked_on_high_fail() -> None:
    checks = pd.DataFrame(
        [
            _row("raw_ok", "raw", "PASS", "HIGH"),
            _row("dashboard_fail", "dashboard", "FAIL", "HIGH"),
        ]
    )

    matrix = _compute_release_state_matrix(checks)
    assert matrix["release_classification"].iloc[0] == "publish-blocked"


def test_release_state_publish_blocked_on_high_warn() -> None:
    checks = pd.DataFrame(
        [
            _row("raw_ok", "raw", "PASS", "HIGH"),
            _row("report_warn", "reporting", "WARN", "HIGH"),
        ]
    )

    matrix = _compute_release_state_matrix(checks)
    assert matrix["release_classification"].iloc[0] == "publish-blocked"
