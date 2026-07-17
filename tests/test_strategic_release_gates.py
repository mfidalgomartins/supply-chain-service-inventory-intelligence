"""Release controls for source, causal, and network expansion outputs."""

from __future__ import annotations

import pandas as pd
from src.pre_delivery_validation import _strategic_expansion_checks


def _frames() -> tuple[pd.DataFrame, ...]:
    source = pd.DataFrame(
        [
            {"status": "PASS", "severity": "CRITICAL"},
            {"status": "WARN", "severity": "WARN"},
        ]
    )
    effects = pd.DataFrame(
        [
            {
                "experiment_id": "RCT",
                "design": "randomized_controlled_trial",
                "evidence_status": "causal_supported",
                "attribution_status": "causal_estimate",
            },
            {
                "experiment_id": "DID",
                "design": "difference_in_differences",
                "evidence_status": "quasi_causal_inconclusive",
                "attribution_status": "quasi_causal_estimate",
            },
        ]
    )
    diagnostics = pd.DataFrame(
        [
            {"experiment_id": "RCT", "status": "PASS"},
            {"experiment_id": "DID", "status": "PASS"},
        ]
    )
    network_summary = pd.DataFrame(
        [{"solver_status": "optimal", "mip_gap": 0.0, "max_flow_balance_error": 0}]
    )
    network_plan = pd.DataFrame(
        [
            {
                "target_service_level": 0.95,
                "achieved_service_level": 0.96,
                "balance_error_units": 0,
            }
        ]
    )
    constraints = pd.DataFrame([{"slack_units": 0.0, "capacity_units": 10, "used_units": 10}])
    return source, effects, diagnostics, network_summary, network_plan, constraints


def test_strategic_release_checks_pass_governed_outputs() -> None:
    checks = _strategic_expansion_checks(*_frames())

    assert {check.check_name for check in checks} == {
        "source_readiness_release_gate",
        "causal_claim_discipline",
        "network_solution_feasibility",
    }
    assert all(check.status == "PASS" for check in checks)


def test_strategic_release_checks_block_invalid_claim_and_infeasible_plan() -> None:
    source, effects, diagnostics, summary, plan, constraints = _frames()
    effects.loc[effects["experiment_id"].eq("DID"), "attribution_status"] = "causal_estimate"
    constraints.loc[0, "slack_units"] = -1
    constraints.loc[0, "used_units"] = 11

    checks = {
        check.check_name: check
        for check in _strategic_expansion_checks(
            source, effects, diagnostics, summary, plan, constraints
        )
    }

    assert checks["causal_claim_discipline"].status == "FAIL"
    assert checks["network_solution_feasibility"].status == "FAIL"


def test_supported_effect_requires_clean_diagnostics() -> None:
    source, effects, diagnostics, summary, plan, constraints = _frames()
    diagnostics.loc[diagnostics["experiment_id"].eq("RCT"), "status"] = "FAIL"

    checks = _strategic_expansion_checks(source, effects, diagnostics, summary, plan, constraints)

    causal = next(check for check in checks if check.check_name == "causal_claim_discipline")
    assert causal.status == "FAIL"


def test_every_causal_experiment_requires_diagnostic_coverage() -> None:
    source, effects, diagnostics, summary, plan, constraints = _frames()
    diagnostics = diagnostics[diagnostics["experiment_id"] != "RCT"]

    checks = _strategic_expansion_checks(source, effects, diagnostics, summary, plan, constraints)

    causal = next(check for check in checks if check.check_name == "causal_claim_discipline")
    assert causal.status == "FAIL"


def test_source_governance_cannot_be_skipped_for_release() -> None:
    source, effects, diagnostics, summary, plan, constraints = _frames()
    source.loc[0, "status"] = "SKIP"

    checks = _strategic_expansion_checks(source, effects, diagnostics, summary, plan, constraints)

    source_gate = next(
        check for check in checks if check.check_name == "source_readiness_release_gate"
    )
    assert source_gate.status == "FAIL"


def test_network_gap_uses_configured_release_tolerance() -> None:
    source, effects, diagnostics, summary, plan, constraints = _frames()
    summary.loc[0, "mip_gap"] = 0.0005

    checks = _strategic_expansion_checks(
        source,
        effects,
        diagnostics,
        summary,
        plan,
        constraints,
        max_mip_gap=0.0001,
    )

    network = next(check for check in checks if check.check_name == "network_solution_feasibility")
    assert network.status == "FAIL"
