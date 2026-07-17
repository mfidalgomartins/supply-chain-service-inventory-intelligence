"""Identification, inference, and causal-claim discipline."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from src.causal_evaluation import (
    _economic_value,
    _experiment_panel,
    evaluate_experiment,
    run_causal_evaluation,
)
from src.settings import CausalEvaluationSettings


def _settings() -> CausalEvaluationSettings:
    return CausalEvaluationSettings(
        enabled=True,
        seed=812,
        permutations=199,
        min_units_per_group=12,
        pre_days=30,
        post_days=30,
        alpha=0.05,
        parallel_trend_alpha=0.05,
    )


def _assignment(design: str, units_per_group: int = 20) -> pd.DataFrame:
    rows = []
    for treatment_flag in (0, 1):
        for idx in range(units_per_group):
            rows.append(
                {
                    "experiment_id": "EXP-TEST",
                    "design": design,
                    "assignment_method": (
                        "stratified_randomization"
                        if design == "randomized_controlled_trial"
                        else "matched_supplier_comparison"
                    ),
                    "unit_id": f"U-{treatment_flag}-{idx:02d}",
                    "stratum": f"S-{idx % 4}",
                    "treatment_flag": treatment_flag,
                    "assignment_date": "2025-01-01",
                    "intervention_date": "2025-03-02",
                    "outcome_metric": "fill_rate",
                    "status": "completed",
                }
            )
    return pd.DataFrame(rows)


def _panel(
    assignments: pd.DataFrame,
    *,
    treatment_effect: float,
    treated_pretrend: float = 0.0,
    seed: int = 17,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-31", "2025-03-31", freq="D")
    rows = []
    for unit in assignments.itertuples(index=False):
        unit_offset = rng.normal(0, 0.01)
        for date_index, date in enumerate(dates):
            is_post = date >= pd.Timestamp(unit.intervention_date)
            pretrend = treated_pretrend * date_index if unit.treatment_flag else 0.0
            effect = treatment_effect if is_post and unit.treatment_flag else 0.0
            rows.append(
                {
                    "unit_id": unit.unit_id,
                    "date": date,
                    "outcome": 0.82 + unit_offset + pretrend + effect + rng.normal(0, 0.01),
                    "weight": 1.0,
                }
            )
    return pd.DataFrame(rows)


def test_rct_recovers_effect_and_passes_randomization_inference() -> None:
    assignments = _assignment("randomized_controlled_trial")

    estimate, diagnostics, cohort = evaluate_experiment(
        _panel(assignments, treatment_effect=0.08), assignments, _settings()
    )

    assert estimate["effect_estimate"] == pytest.approx(0.08, abs=0.01)
    assert estimate["randomization_p_value"] <= 0.05
    assert estimate["evidence_status"] == "causal_supported"
    assert estimate["attribution_status"] == "causal_estimate"
    assert (diagnostics["status"] != "FAIL").all()
    assert set(cohort["period"]) == {"pre", "post"}


def test_valid_rct_without_detectable_effect_is_labelled_inconclusive() -> None:
    assignments = _assignment("randomized_controlled_trial")

    estimate, diagnostics, _ = evaluate_experiment(
        _panel(assignments, treatment_effect=0.0), assignments, _settings()
    )

    assert estimate["effect_estimate"] == pytest.approx(0.0, abs=0.01)
    assert estimate["evidence_status"] == "causal_inconclusive"
    assert estimate["attribution_status"] == "causal_estimate"
    assert "FAIL" not in set(diagnostics["status"])


@pytest.mark.parametrize("invalidity", ["duplicate", "late_assignment", "small_group"])
def test_invalid_assignment_is_rejected(invalidity: str) -> None:
    assignments = _assignment("randomized_controlled_trial")
    if invalidity == "duplicate":
        assignments = pd.concat([assignments, assignments.iloc[[0]]], ignore_index=True)
    elif invalidity == "late_assignment":
        assignments["assignment_date"] = assignments["intervention_date"]
    else:
        assignments = _assignment("randomized_controlled_trial", units_per_group=5)

    with pytest.raises(ValueError):
        evaluate_experiment(
            _panel(assignments.drop_duplicates("unit_id"), treatment_effect=0.05),
            assignments,
            _settings(),
        )


@pytest.mark.parametrize(
    ("invalidity", "message"),
    [
        ("missing_field", "fields missing"),
        ("empty", "contains no units"),
        ("mixed_design", "exactly one design"),
        ("unsupported_design", "Unsupported causal design"),
        ("wrong_method", "requires assignment_method"),
        ("bad_flag", "treatment_flag"),
        ("multiple_intervention_dates", "one common intervention_date"),
        ("bad_status", "Only active or completed"),
    ],
)
def test_assignment_contract_rejects_unsupported_states(invalidity: str, message: str) -> None:
    assignments = _assignment("randomized_controlled_trial")
    if invalidity == "missing_field":
        assignments = assignments.drop(columns="assignment_method")
    elif invalidity == "empty":
        assignments = assignments.iloc[0:0]
    elif invalidity == "mixed_design":
        assignments.loc[0, "design"] = "difference_in_differences"
    elif invalidity == "unsupported_design":
        assignments["design"] = "observational"
    elif invalidity == "wrong_method":
        assignments["assignment_method"] = "manual"
    elif invalidity == "bad_flag":
        assignments.loc[0, "treatment_flag"] = 2
    elif invalidity == "multiple_intervention_dates":
        assignments.loc[0, "intervention_date"] = "2025-03-03"
    else:
        assignments.loc[0, "status"] = "planned"

    with pytest.raises(ValueError, match=message):
        evaluate_experiment(
            _panel(_assignment("randomized_controlled_trial"), treatment_effect=0.05),
            assignments,
            _settings(),
        )


@pytest.mark.parametrize(
    ("invalidity", "message"),
    [
        ("missing_field", "panel fields missing"),
        ("duplicate", "unique by unit_id and date"),
        ("infinite", "must be finite"),
        ("negative_weight", "must be non-negative"),
        ("missing_unit", "missing from the outcome panel"),
        ("missing_day", "complete daily observations"),
        ("incomplete_window", "complete pre and post"),
    ],
)
def test_outcome_panel_contract_rejects_invalid_evidence(invalidity: str, message: str) -> None:
    assignments = _assignment("randomized_controlled_trial")
    panel = _panel(assignments, treatment_effect=0.05)
    if invalidity == "missing_field":
        panel = panel.drop(columns="weight")
    elif invalidity == "duplicate":
        panel = pd.concat([panel, panel.iloc[[0]]], ignore_index=True)
    elif invalidity == "infinite":
        panel.loc[0, "outcome"] = np.inf
    elif invalidity == "negative_weight":
        panel.loc[0, "weight"] = -1
    elif invalidity == "missing_unit":
        panel = panel[panel["unit_id"] != assignments.iloc[0]["unit_id"]]
    elif invalidity == "missing_day":
        panel = panel.drop(index=0)
    else:
        first_unit = assignments.iloc[0]["unit_id"]
        panel = panel[
            ~(panel["unit_id"].eq(first_unit) & panel["date"].ge(pd.Timestamp("2025-03-02")))
        ]

    with pytest.raises(ValueError, match=message):
        evaluate_experiment(panel, assignments, _settings())


def test_did_recovers_effect_when_pretrends_are_parallel() -> None:
    assignments = _assignment("difference_in_differences")

    estimate, diagnostics, _ = evaluate_experiment(
        _panel(assignments, treatment_effect=0.06), assignments, _settings()
    )

    assert estimate["effect_estimate"] == pytest.approx(0.06, abs=0.01)
    assert estimate["evidence_status"] == "quasi_causal_supported"
    assert estimate["attribution_status"] == "quasi_causal_estimate"
    parallel = diagnostics[diagnostics["diagnostic"] == "parallel_pretrend"].iloc[0]
    assert parallel["status"] == "PASS"


def test_did_downgrades_attribution_when_parallel_trends_fail() -> None:
    assignments = _assignment("difference_in_differences")

    estimate, diagnostics, _ = evaluate_experiment(
        _panel(assignments, treatment_effect=0.06, treated_pretrend=0.002),
        assignments,
        _settings(),
    )

    parallel = diagnostics[diagnostics["diagnostic"] == "parallel_pretrend"].iloc[0]
    assert parallel["status"] == "FAIL"
    assert estimate["evidence_status"] == "insufficient_evidence"
    assert estimate["attribution_status"] == "not_causal"


def test_causal_evaluation_is_deterministic() -> None:
    assignments = _assignment("randomized_controlled_trial")
    panel = _panel(assignments, treatment_effect=0.05)

    first = evaluate_experiment(panel, assignments, _settings())
    second = evaluate_experiment(panel, assignments, _settings())

    assert first[0] == second[0]
    pd.testing.assert_frame_equal(first[1], second[1])
    pd.testing.assert_frame_equal(first[2], second[2])


@pytest.mark.parametrize(
    ("metric", "expected"),
    [
        ("fill_rate", 0.8),
        ("stockout_rate", 0.2),
        ("lost_margin_proxy", 8.0),
        ("average_inventory_value", 75.0),
    ],
)
def test_operational_outcome_mapping_and_economic_value(metric: str, expected: float) -> None:
    assignments = pd.DataFrame(
        [
            {
                "unit_id": "U-1",
                "product_id": "P-1",
                "warehouse_id": "W-1",
                "treatment_flag": 1,
            }
        ]
    )
    daily = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2025-03-02"),
                "product_id": "P-1",
                "warehouse_id": "W-1",
                "units_demanded": 10.0,
                "units_fulfilled": 8.0,
                "units_lost_sales": 2.0,
                "lost_sales_revenue": 20.0,
                "gross_margin_rate": 0.4,
                "inventory_value": 75.0,
                "unit_margin": 4.0,
            }
        ]
    )

    panel = _experiment_panel(daily, assignments, metric)
    estimate = {
        "intervention_date": "2025-03-02",
        "post_days": 1,
        "effect_estimate": 0.1,
        "outcome_metric": metric,
        "treated_units": 1,
    }

    assert panel.iloc[0]["outcome"] == pytest.approx(expected)
    assert np.isfinite(_economic_value(daily, assignments, estimate))


def test_operational_outcome_mapping_rejects_empty_scope_and_unknown_metric() -> None:
    assignments = pd.DataFrame(
        [
            {
                "unit_id": "U-1",
                "product_id": "missing",
                "warehouse_id": "W-1",
                "treatment_flag": 1,
            }
        ]
    )
    daily = pd.DataFrame(
        columns=[
            "date",
            "product_id",
            "warehouse_id",
            "units_demanded",
            "units_fulfilled",
            "units_lost_sales",
            "lost_sales_revenue",
            "gross_margin_rate",
            "inventory_value",
            "unit_margin",
        ]
    )
    with pytest.raises(ValueError, match="No operational observations"):
        _experiment_panel(daily, assignments, "fill_rate")

    daily = pd.DataFrame([{"date": "2025-01-01", "product_id": "missing", "warehouse_id": "W-1"}])
    with pytest.raises(ValueError, match="Unsupported causal outcome"):
        _experiment_panel(daily, assignments, "unsupported")


def test_runner_materializes_estimates_diagnostics_and_cohorts(tmp_path) -> None:
    assignments = _assignment("randomized_controlled_trial")
    assignments["product_id"] = assignments["unit_id"]
    assignments["warehouse_id"] = "WH-TEST"
    assignments["supplier_id"] = "SUP-TEST"
    assignments["treatment_group"] = np.where(
        assignments["treatment_flag"].eq(1), "policy_reset", "business_as_usual"
    )
    causal_panel = _panel(assignments, treatment_effect=0.08)
    daily = causal_panel.copy()
    daily["product_id"] = daily.pop("unit_id")
    daily["warehouse_id"] = "WH-TEST"
    daily["units_demanded"] = 100.0
    daily["units_fulfilled"] = daily.pop("outcome") * daily["units_demanded"]
    daily["units_lost_sales"] = daily["units_demanded"] - daily["units_fulfilled"]
    daily["inventory_value"] = 1_000.0
    daily["lost_sales_revenue"] = daily["units_lost_sales"] * 10.0
    products = pd.DataFrame(
        {
            "product_id": assignments["product_id"].unique(),
            "unit_cost": 6.0,
            "unit_price": 10.0,
        }
    )
    assignments_path = tmp_path / "assignments.csv"
    daily_path = tmp_path / "daily.csv"
    products_path = tmp_path / "products.csv"
    assignments.to_csv(assignments_path, index=False)
    daily.to_csv(daily_path, index=False)
    products.to_csv(products_path, index=False)

    estimates, diagnostics, cohort = run_causal_evaluation(
        settings=_settings(),
        assignments_path=assignments_path,
        daily_path=daily_path,
        products_path=products_path,
        output_dir=tmp_path,
    )

    assert len(estimates) == 1
    assert estimates.iloc[0]["economic_value_estimate"] > 0
    assert not diagnostics.empty
    assert not cohort.empty
    for file_name in (
        "causal_effect_estimates.csv",
        "causal_diagnostics.csv",
        "causal_cohort_timeseries.csv",
    ):
        assert (tmp_path / file_name).exists()
