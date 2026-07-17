"""Design-aware RCT and difference-in-differences intervention evaluation."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
from src.settings import CausalEvaluationSettings, load_settings

SUPPORTED_DESIGNS = {"randomized_controlled_trial", "difference_in_differences"}
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
ESTIMATE_COLUMNS = [
    "experiment_id",
    "design",
    "outcome_metric",
    "estimand",
    "intervention_date",
    "treated_units",
    "control_units",
    "pre_days",
    "post_days",
    "effect_estimate",
    "standard_error",
    "ci_lower",
    "ci_upper",
    "p_value",
    "randomization_p_value",
    "economic_value_estimate",
    "evidence_status",
    "attribution_status",
]
DIAGNOSTIC_COLUMNS = [
    "experiment_id",
    "diagnostic",
    "status",
    "observed",
    "threshold",
    "details",
]
COHORT_COLUMNS = [
    "experiment_id",
    "outcome_metric",
    "date",
    "period",
    "treatment_group",
    "treatment_flag",
    "units",
    "outcome",
    "weight",
]


def _one_value(frame: pd.DataFrame, column: str) -> Any:
    values = frame[column].dropna().unique()
    if len(values) != 1:
        raise ValueError(f"Experiment requires exactly one {column}")
    return values[0]


def _validate_assignments(
    assignments: pd.DataFrame, settings: CausalEvaluationSettings
) -> tuple[str, str, str, pd.Timestamp]:
    required = {
        "experiment_id",
        "design",
        "assignment_method",
        "unit_id",
        "stratum",
        "treatment_flag",
        "assignment_date",
        "intervention_date",
        "outcome_metric",
        "status",
    }
    missing = sorted(required - set(assignments.columns))
    if missing:
        raise ValueError(f"Intervention assignment fields missing: {missing}")
    if assignments.empty:
        raise ValueError("Intervention assignment contains no units")
    if assignments.duplicated(["experiment_id", "unit_id"]).any():
        raise ValueError("Experiment assignments must be unique by experiment_id and unit_id")
    experiment_id = str(_one_value(assignments, "experiment_id"))
    design = str(_one_value(assignments, "design"))
    outcome_metric = str(_one_value(assignments, "outcome_metric"))
    if design not in SUPPORTED_DESIGNS:
        raise ValueError(f"Unsupported causal design: {design}")
    expected_method = {
        "randomized_controlled_trial": "stratified_randomization",
        "difference_in_differences": "matched_supplier_comparison",
    }[design]
    if set(assignments["assignment_method"]) != {expected_method}:
        raise ValueError(f"{design} requires assignment_method={expected_method}")
    if not set(assignments["treatment_flag"]).issubset({0, 1}):
        raise ValueError("treatment_flag must contain only 0 and 1")
    group_counts = assignments.groupby("treatment_flag")["unit_id"].nunique()
    if set(group_counts.index) != {0, 1} or (group_counts < settings.min_units_per_group).any():
        raise ValueError(
            "Treatment and control groups must each meet causal_evaluation.min_units_per_group"
        )
    assignment_dates = pd.to_datetime(assignments["assignment_date"], errors="raise")
    intervention_dates = pd.to_datetime(assignments["intervention_date"], errors="raise")
    if intervention_dates.nunique() != 1:
        raise ValueError("Experiment requires one common intervention_date")
    if (assignment_dates >= intervention_dates).any():
        raise ValueError("Treatment assignment must precede the intervention and outcomes")
    if set(assignments["status"]) - {"active", "completed"}:
        raise ValueError("Only active or completed interventions can be evaluated")
    return experiment_id, design, outcome_metric, intervention_dates.iloc[0]


def _weighted_mean(frame: pd.DataFrame) -> float:
    weights = frame["weight"].to_numpy(dtype=float)
    outcomes = frame["outcome"].to_numpy(dtype=float)
    total_weight = float(weights.sum())
    if total_weight <= 0:
        raise ValueError("Causal panel weights must sum to a positive value")
    return float(np.dot(outcomes, weights) / total_weight)


def _welch_difference(
    treated: pd.Series, control: pd.Series
) -> tuple[float, float, float, float, float]:
    treated_values = treated.to_numpy(dtype=float)
    control_values = control.to_numpy(dtype=float)
    effect = float(treated_values.mean() - control_values.mean())
    var_t = float(treated_values.var(ddof=1))
    var_c = float(control_values.var(ddof=1))
    component_t = var_t / len(treated_values)
    component_c = var_c / len(control_values)
    standard_error = float(np.sqrt(component_t + component_c))
    if standard_error == 0:
        p_value = 1.0 if effect == 0 else 0.0
        critical = 1.96
    else:
        numerator = (component_t + component_c) ** 2
        denominator = component_t**2 / (len(treated_values) - 1) + component_c**2 / (
            len(control_values) - 1
        )
        degrees_freedom = numerator / denominator if denominator else np.inf
        statistic = effect / standard_error
        p_value = float(2 * stats.t.sf(abs(statistic), degrees_freedom))
        critical = float(stats.t.ppf(0.975, degrees_freedom))
    return (
        effect,
        standard_error,
        effect - critical * standard_error,
        effect + critical * standard_error,
        p_value,
    )


def _randomization_p_value(
    changes: pd.DataFrame,
    assignments: pd.DataFrame,
    observed_effect: float,
    settings: CausalEvaluationSettings,
    experiment_id: str,
) -> float:
    joined = changes[["unit_id", "change"]].merge(
        assignments[["unit_id", "stratum", "treatment_flag"]],
        on="unit_id",
        validate="one_to_one",
    )
    stable_seed = int.from_bytes(hashlib.sha256(experiment_id.encode("utf-8")).digest()[:4], "big")
    rng = np.random.default_rng(settings.seed + stable_seed)
    exceedances = 0
    for _ in range(settings.permutations):
        permuted_flags = joined["treatment_flag"].to_numpy(copy=True)
        for indices in joined.groupby("stratum", sort=True).groups.values():
            positions = np.asarray(list(indices), dtype=int)
            permuted_flags[positions] = rng.permutation(permuted_flags[positions])
        values = joined["change"].to_numpy(dtype=float)
        permuted_effect = float(
            values[permuted_flags == 1].mean() - values[permuted_flags == 0].mean()
        )
        exceedances += int(abs(permuted_effect) >= abs(observed_effect) - 1e-12)
    return (exceedances + 1) / (settings.permutations + 1)


def _diagnostic(
    experiment_id: str,
    diagnostic: str,
    status: str,
    observed: str,
    threshold: str,
    details: str,
) -> dict[str, str]:
    return {
        "experiment_id": experiment_id,
        "diagnostic": diagnostic,
        "status": status,
        "observed": observed,
        "threshold": threshold,
        "details": details,
    }


def _pretrend_diagnostic(
    panel: pd.DataFrame,
    assignments: pd.DataFrame,
    intervention_date: pd.Timestamp,
    experiment_id: str,
    alpha: float,
) -> tuple[dict[str, str], dict[str, str]]:
    pre = panel[panel["date"] < intervention_date].copy()
    pre_dates = sorted(pre["date"].unique())
    midpoint = pd.Timestamp(pre_dates[len(pre_dates) // 2])
    pre["pre_period"] = np.where(pre["date"] < midpoint, "early", "late")
    unit_period = (
        pre.groupby(["unit_id", "pre_period"], sort=True)
        .apply(_weighted_mean, include_groups=False)
        .rename("outcome")
        .reset_index()
    )
    pivot = unit_period.pivot(index="unit_id", columns="pre_period", values="outcome")
    if not {"early", "late"}.issubset(pivot.columns) or pivot.isna().any().any():
        raise ValueError("Every causal unit requires complete early and late pre-period data")
    placebo = (pivot["late"] - pivot["early"]).rename("change").reset_index()
    placebo = placebo.merge(
        assignments[["unit_id", "treatment_flag"]], on="unit_id", validate="one_to_one"
    )
    effect, _, _, _, p_value = _welch_difference(
        placebo.loc[placebo["treatment_flag"] == 1, "change"],
        placebo.loc[placebo["treatment_flag"] == 0, "change"],
    )
    status = "PASS" if p_value >= alpha else "FAIL"
    parallel = _diagnostic(
        experiment_id,
        "parallel_pretrend",
        status,
        f"placebo_effect={effect:.6f}, p_value={p_value:.6f}",
        f"p_value >= {alpha}",
        "Pre-period changes must not differ statistically between cohorts.",
    )
    placebo_check = _diagnostic(
        experiment_id,
        "pre_period_placebo",
        status,
        f"effect={effect:.6f}, p_value={p_value:.6f}",
        f"p_value >= {alpha}",
        "A false intervention inside the pre-period must not produce an effect.",
    )
    return parallel, placebo_check


def evaluate_experiment(
    panel: pd.DataFrame,
    assignments: pd.DataFrame,
    settings: CausalEvaluationSettings,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    """Estimate one experiment and return its estimate, diagnostics, and cohort trend."""
    experiment_id, design, outcome_metric, intervention_date = _validate_assignments(
        assignments, settings
    )
    required_panel = {"unit_id", "date", "outcome", "weight"}
    missing_panel = sorted(required_panel - set(panel.columns))
    if missing_panel:
        raise ValueError(f"Causal panel fields missing: {missing_panel}")
    working = panel.copy()
    working["date"] = pd.to_datetime(working["date"], errors="raise")
    if working.duplicated(["unit_id", "date"]).any():
        raise ValueError("Causal panel must be unique by unit_id and date")
    if not np.isfinite(working[["outcome", "weight"]].to_numpy(dtype=float)).all():
        raise ValueError("Causal outcomes and weights must be finite")
    if (working["weight"] < 0).any():
        raise ValueError("Causal panel weights must be non-negative")
    unknown_units = set(assignments["unit_id"]) - set(working["unit_id"])
    if unknown_units:
        raise ValueError("Assigned causal units are missing from the outcome panel")

    pre_start = intervention_date - pd.Timedelta(days=settings.pre_days)
    post_end = intervention_date + pd.Timedelta(days=settings.post_days - 1)
    working = working[
        (working["unit_id"].isin(assignments["unit_id"]))
        & (working["date"] >= pre_start)
        & (working["date"] <= post_end)
    ].copy()
    working["period"] = np.where(working["date"] < intervention_date, "pre", "post")
    unit_period = (
        working.groupby(["unit_id", "period"], sort=True)
        .apply(_weighted_mean, include_groups=False)
        .rename("outcome")
        .reset_index()
    )
    pivot = unit_period.pivot(index="unit_id", columns="period", values="outcome")
    if not {"pre", "post"}.issubset(pivot.columns) or pivot.isna().any().any():
        raise ValueError("Every causal unit requires complete pre and post outcome windows")
    expected_observations = settings.pre_days + settings.post_days
    observation_counts = working.groupby("unit_id")["date"].nunique()
    if (observation_counts != expected_observations).any():
        raise ValueError(
            "Every causal unit requires complete daily observations in the common window"
        )
    changes = (pivot["post"] - pivot["pre"]).rename("change").reset_index()
    changes = changes.merge(
        assignments[["unit_id", "treatment_flag"]], on="unit_id", validate="one_to_one"
    )
    treated_changes = changes.loc[changes["treatment_flag"] == 1, "change"]
    control_changes = changes.loc[changes["treatment_flag"] == 0, "change"]
    effect, standard_error, ci_lower, ci_upper, p_value = _welch_difference(
        treated_changes, control_changes
    )

    diagnostics = [
        _diagnostic(
            experiment_id,
            "design_integrity",
            "PASS",
            design,
            "supported design and pre-outcome assignment",
            "Assignment schema, timing, method, and treatment groups are valid.",
        ),
        _diagnostic(
            experiment_id,
            "sample_support",
            "PASS",
            f"treated={len(treated_changes)}, control={len(control_changes)}",
            f">= {settings.min_units_per_group} per group",
            "Inference is performed at the assigned intervention-unit level.",
        ),
    ]
    pre_values = (
        pivot["pre"]
        .rename("pre_outcome")
        .reset_index()
        .merge(assignments[["unit_id", "treatment_flag"]], on="unit_id", validate="one_to_one")
    )
    treated_pre = pre_values.loc[pre_values["treatment_flag"] == 1, "pre_outcome"]
    control_pre = pre_values.loc[pre_values["treatment_flag"] == 0, "pre_outcome"]
    pooled_sd = float(np.sqrt((treated_pre.var(ddof=1) + control_pre.var(ddof=1)) / 2))
    standardized_difference = (
        float((treated_pre.mean() - control_pre.mean()) / pooled_sd) if pooled_sd else 0.0
    )
    balance_status = "PASS" if abs(standardized_difference) <= 0.50 else "FAIL"
    diagnostics.append(
        _diagnostic(
            experiment_id,
            "pre_outcome_balance",
            balance_status,
            f"standardized_difference={standardized_difference:.6f}",
            "absolute standardized difference <= 0.50",
            "Large baseline outcome imbalance weakens common-support credibility.",
        )
    )

    randomization_p_value: float | None = None
    if design == "randomized_controlled_trial":
        randomization_p_value = _randomization_p_value(
            changes, assignments, effect, settings, experiment_id
        )
        diagnostics.append(
            _diagnostic(
                experiment_id,
                "randomization_inference",
                "PASS",
                f"p_value={randomization_p_value:.6f}",
                f"{settings.permutations} stratified permutations",
                "Permutation inference preserves treatment counts inside each stratum.",
            )
        )
    else:
        parallel, placebo = _pretrend_diagnostic(
            working, assignments, intervention_date, experiment_id, settings.parallel_trend_alpha
        )
        diagnostics.extend([parallel, placebo])

    diagnostics_frame = pd.DataFrame(diagnostics).sort_values("diagnostic", ignore_index=True)
    valid_identification = not (diagnostics_frame["status"] == "FAIL").any()
    detected = p_value <= settings.alpha and (
        randomization_p_value is None or randomization_p_value <= settings.alpha
    )
    if not valid_identification:
        evidence_status = "insufficient_evidence"
        attribution_status = "not_causal"
    elif design == "randomized_controlled_trial":
        evidence_status = "causal_supported" if detected else "causal_inconclusive"
        attribution_status = "causal_estimate"
    else:
        evidence_status = "quasi_causal_supported" if detected else "quasi_causal_inconclusive"
        attribution_status = "quasi_causal_estimate"

    merged_panel = working.merge(
        assignments[["unit_id", "treatment_flag"]], on="unit_id", validate="many_to_one"
    )
    cohort = (
        merged_panel.groupby(["date", "period", "treatment_flag"], sort=True)
        .apply(
            lambda group: pd.Series(
                {
                    "units": int(group["unit_id"].nunique()),
                    "outcome": _weighted_mean(group),
                    "weight": float(group["weight"].sum()),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    cohort.insert(0, "experiment_id", experiment_id)
    cohort.insert(1, "outcome_metric", outcome_metric)
    cohort["treatment_group"] = np.where(cohort["treatment_flag"].eq(1), "treated", "control")
    cohort = cohort[
        [
            "experiment_id",
            "outcome_metric",
            "date",
            "period",
            "treatment_group",
            "treatment_flag",
            "units",
            "outcome",
            "weight",
        ]
    ]
    estimate = {
        "experiment_id": experiment_id,
        "design": design,
        "outcome_metric": outcome_metric,
        "estimand": "average_treatment_effect_on_treated",
        "intervention_date": intervention_date.strftime("%Y-%m-%d"),
        "treated_units": int(len(treated_changes)),
        "control_units": int(len(control_changes)),
        "pre_days": settings.pre_days,
        "post_days": settings.post_days,
        "effect_estimate": round(effect, 10),
        "standard_error": round(standard_error, 10),
        "ci_lower": round(ci_lower, 10),
        "ci_upper": round(ci_upper, 10),
        "p_value": round(p_value, 10),
        "randomization_p_value": (
            round(randomization_p_value, 10) if randomization_p_value is not None else None
        ),
        "economic_value_estimate": None,
        "evidence_status": evidence_status,
        "attribution_status": attribution_status,
    }
    return estimate, diagnostics_frame, cohort


def _experiment_panel(
    daily: pd.DataFrame,
    assignments: pd.DataFrame,
    outcome_metric: str,
) -> pd.DataFrame:
    scoped = daily.merge(
        assignments[["unit_id", "product_id", "warehouse_id"]],
        on=["product_id", "warehouse_id"],
        how="inner",
        validate="many_to_one",
    )
    if scoped.empty:
        raise ValueError("No operational observations match the assigned causal units")
    if outcome_metric == "fill_rate":
        scoped["outcome"] = np.divide(
            scoped["units_fulfilled"],
            scoped["units_demanded"],
            out=np.ones(len(scoped), dtype=float),
            where=scoped["units_demanded"].to_numpy() > 0,
        )
        scoped["weight"] = scoped["units_demanded"].astype(float)
    elif outcome_metric == "stockout_rate":
        scoped["outcome"] = np.divide(
            scoped["units_lost_sales"],
            scoped["units_demanded"],
            out=np.zeros(len(scoped), dtype=float),
            where=scoped["units_demanded"].to_numpy() > 0,
        )
        scoped["weight"] = scoped["units_demanded"].astype(float)
    elif outcome_metric == "lost_margin_proxy":
        scoped["outcome"] = scoped["lost_sales_revenue"] * scoped["gross_margin_rate"]
        scoped["weight"] = 1.0
    elif outcome_metric == "average_inventory_value":
        scoped["outcome"] = scoped["inventory_value"].astype(float)
        scoped["weight"] = 1.0
    else:
        raise ValueError(f"Unsupported causal outcome metric: {outcome_metric}")
    return scoped[["unit_id", "date", "outcome", "weight"]]


def _economic_value(
    daily: pd.DataFrame,
    assignments: pd.DataFrame,
    estimate: dict[str, Any],
) -> float:
    intervention_date = pd.Timestamp(str(estimate["intervention_date"]))
    post_end = intervention_date + pd.Timedelta(days=int(estimate["post_days"]) - 1)
    treated = assignments.loc[assignments["treatment_flag"].eq(1), ["product_id", "warehouse_id"]]
    post = daily.merge(
        treated,
        on=["product_id", "warehouse_id"],
        how="inner",
        validate="many_to_one",
    )
    post = post[(post["date"] >= intervention_date) & (post["date"] <= post_end)]
    effect = float(estimate["effect_estimate"])
    outcome_metric = str(estimate["outcome_metric"])
    if outcome_metric == "fill_rate":
        value = effect * float((post["units_demanded"] * post["unit_margin"]).sum())
    elif outcome_metric == "stockout_rate":
        value = -effect * float((post["units_demanded"] * post["unit_margin"]).sum())
    elif outcome_metric == "lost_margin_proxy":
        value = -effect * int(estimate["treated_units"]) * int(estimate["post_days"])
    elif outcome_metric == "average_inventory_value":
        value = -effect * int(estimate["treated_units"])
    else:  # guarded by _experiment_panel
        raise ValueError(f"Unsupported causal outcome metric: {outcome_metric}")
    return round(value, 2)


def run_causal_evaluation(
    *,
    settings: CausalEvaluationSettings | None = None,
    config_path: Path | None = None,
    assignments_path: Path = DATA_RAW / "intervention_assignments.csv",
    daily_path: Path = DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
    products_path: Path = DATA_RAW / "products.csv",
    output_dir: Path = OUTPUT_TABLES_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Evaluate every registered experiment and materialize governed evidence."""
    if settings is not None and config_path is not None:
        raise ValueError("Provide settings or config_path, not both")
    causal_settings = settings or load_settings(config_path).causal_evaluation
    assignments = pd.read_csv(assignments_path)
    daily = pd.read_csv(daily_path, parse_dates=["date"])
    products = pd.read_csv(products_path)
    margins = products[["product_id", "unit_cost", "unit_price"]].copy()
    margins["unit_margin"] = (margins["unit_price"] - margins["unit_cost"]).clip(lower=0)
    margins["gross_margin_rate"] = np.divide(
        margins["unit_margin"],
        margins["unit_price"],
        out=np.zeros(len(margins), dtype=float),
        where=margins["unit_price"].to_numpy() > 0,
    )
    daily = daily.merge(
        margins[["product_id", "unit_margin", "gross_margin_rate"]],
        on="product_id",
        how="left",
        validate="many_to_one",
    )
    if daily[["unit_margin", "gross_margin_rate"]].isna().any().any():
        raise ValueError("Causal panel contains products without margin data")

    estimate_rows: list[dict[str, Any]] = []
    diagnostic_frames: list[pd.DataFrame] = []
    cohort_frames: list[pd.DataFrame] = []
    if causal_settings.enabled:
        for experiment_id in sorted(assignments["experiment_id"].unique()):
            experiment = assignments[assignments["experiment_id"] == experiment_id].copy()
            outcome_metric = str(_one_value(experiment, "outcome_metric"))
            panel = _experiment_panel(daily, experiment, outcome_metric)
            estimate, diagnostics, cohort = evaluate_experiment(panel, experiment, causal_settings)
            estimate["economic_value_estimate"] = _economic_value(daily, experiment, estimate)
            estimate_rows.append(estimate)
            diagnostic_frames.append(diagnostics)
            cohort_frames.append(cohort)

    estimates = pd.DataFrame(estimate_rows, columns=ESTIMATE_COLUMNS).sort_values(
        "experiment_id", ignore_index=True
    )
    diagnostics = (
        pd.concat(diagnostic_frames, ignore_index=True)
        if diagnostic_frames
        else pd.DataFrame(columns=DIAGNOSTIC_COLUMNS)
    ).sort_values(["experiment_id", "diagnostic"], ignore_index=True)
    cohorts = (
        pd.concat(cohort_frames, ignore_index=True)
        if cohort_frames
        else pd.DataFrame(columns=COHORT_COLUMNS)
    ).sort_values(["experiment_id", "date", "treatment_flag"], ignore_index=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    estimates.to_csv(output_dir / "causal_effect_estimates.csv", index=False)
    diagnostics.to_csv(output_dir / "causal_diagnostics.csv", index=False)
    cohorts.to_csv(output_dir / "causal_cohort_timeseries.csv", index=False)
    print(
        "Causal evaluation complete. "
        f"Experiments: {len(estimates)}; supported effects: "
        f"{int(estimates['evidence_status'].str.endswith('supported').sum()) if len(estimates) else 0}"
    )
    return estimates, diagnostics, cohorts


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate registered supply-chain interventions")
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    run_causal_evaluation(config_path=args.config)


if __name__ == "__main__":
    main()
