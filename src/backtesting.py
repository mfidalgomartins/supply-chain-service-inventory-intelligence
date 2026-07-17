"""Leakage-safe walk-forward backtesting for reorder and safety-stock policies."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
from src.inventory_policy import PolicyParameters, simulate_policy
from src.settings import BacktestSettings, load_settings

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


def temporal_windows(settings: BacktestSettings) -> pd.DataFrame:
    rows = []
    for fold_start_value in settings.fold_start_dates:
        fold_start = pd.Timestamp(fold_start_value)
        rows.append(
            {
                "fold_start": fold_start,
                "train_start": fold_start - pd.Timedelta(days=settings.lookback_days),
                "train_end": fold_start - pd.Timedelta(days=1),
                "evaluation_end": fold_start + pd.Timedelta(days=settings.horizon_days - 1),
            }
        )
    windows = pd.DataFrame(rows)
    if (windows["train_end"] >= windows["fold_start"]).any():
        raise ValueError("Temporal fold leakage detected")
    return windows


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = pd.read_csv(
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"]
    )
    products = pd.read_csv(DATA_RAW / "products.csv")
    suppliers = pd.read_csv(DATA_RAW / "suppliers.csv")
    master = products.merge(
        suppliers[["supplier_id", "minimum_order_qty"]],
        on="supplier_id",
        how="left",
        validate="many_to_one",
    )
    master["unit_margin"] = (master["unit_price"] - master["unit_cost"]).clip(lower=0)
    return daily.sort_values(["product_id", "warehouse_id", "date"]), master


def _starting_inventory(entity: pd.DataFrame, fold_start: pd.Timestamp) -> int:
    prior = entity.loc[entity["date"] < fold_start, "on_hand_units"]
    if not prior.empty:
        return int(prior.iloc[-1])
    first = entity.loc[entity["date"] == fold_start]
    if first.empty:
        raise ValueError(f"No starting inventory for fold {fold_start.date()}")
    return int(first.iloc[0]["on_hand_units"] + first.iloc[0]["units_fulfilled"])


def run_policy_backtest(config_path: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    settings = load_settings(config_path).backtesting
    daily, master = _load_inputs()
    windows = temporal_windows(settings)
    policy_inputs = tuple(
        PolicyParameters(
            policy_id=policy.policy_id,
            safety_stock_factor=policy.safety_stock_factor,
            cycle_stock_days=policy.cycle_stock_days,
        )
        for policy in settings.policies
    )

    rows: list[dict] = []
    grouped = daily.groupby(["product_id", "warehouse_id"], sort=True)
    master_lookup = master.set_index("product_id").to_dict(orient="index")
    for (product_id, warehouse_id), entity in grouped:
        product = master_lookup[product_id]
        target_service = float(product["target_service_level"])
        service_z = NormalDist().inv_cdf(float(np.clip(target_service, 0.5001, 0.999)))
        for fold in windows.itertuples(index=False):
            train = entity[
                (entity["date"] >= fold.train_start) & (entity["date"] <= fold.train_end)
            ]
            evaluation = entity[
                (entity["date"] >= fold.fold_start) & (entity["date"] <= fold.evaluation_end)
            ]
            if len(train) < settings.lookback_days or len(evaluation) < settings.horizon_days:
                continue
            mean_demand = float(train["units_demanded"].mean())
            demand_std = float(train["units_demanded"].std(ddof=0))
            starting_on_hand = _starting_inventory(entity, fold.train_start)
            actual_demand = int(evaluation["units_demanded"].sum())
            actual_fill = (
                float(evaluation["units_fulfilled"].sum() / actual_demand) if actual_demand else 1.0
            )
            actual_inventory = float(evaluation["inventory_value"].mean())

            for policy in policy_inputs:
                result = simulate_policy(
                    demand=pd.concat(
                        [train["units_demanded"], evaluation["units_demanded"]]
                    ).to_numpy(dtype=int),
                    starting_on_hand=starting_on_hand,
                    mean_daily_demand=mean_demand,
                    demand_std=demand_std,
                    lead_time_days=int(product["lead_time_days"]),
                    service_z=service_z,
                    minimum_order_qty=int(product["minimum_order_qty"]),
                    unit_cost=float(product["unit_cost"]),
                    unit_margin=float(product["unit_margin"]),
                    annual_holding_cost_rate=settings.annual_holding_cost_rate,
                    ordering_cost=settings.ordering_cost,
                    policy=policy,
                    warmup_days=len(train),
                )
                rows.append(
                    {
                        "product_id": product_id,
                        "warehouse_id": warehouse_id,
                        "supplier_id": product["supplier_id"],
                        "category": product["category"],
                        "abc_class": evaluation["abc_class"].iloc[0],
                        "fold_start": fold.fold_start.strftime("%Y-%m-%d"),
                        "train_start": fold.train_start.strftime("%Y-%m-%d"),
                        "train_end": fold.train_end.strftime("%Y-%m-%d"),
                        "evaluation_end": fold.evaluation_end.strftime("%Y-%m-%d"),
                        "warmup_days": len(train),
                        "target_service_level": target_service,
                        "actual_fill_rate": actual_fill,
                        "actual_average_inventory_value": actual_inventory,
                        "mean_daily_demand_training": mean_demand,
                        "demand_std_training": demand_std,
                        "safety_stock_factor": policy.safety_stock_factor,
                        "cycle_stock_days": policy.cycle_stock_days,
                        "target_met": result.fill_rate
                        >= target_service - settings.service_tolerance,
                        **asdict(result),
                    }
                )

    folds = pd.DataFrame(rows)
    if folds.empty:
        raise ValueError("Backtest produced no eligible temporal folds")
    if (folds["balance_error_units"] != 0).any():
        raise ValueError("Inventory conservation failed in policy backtest")
    if (pd.to_datetime(folds["train_end"]) >= pd.to_datetime(folds["fold_start"])).any():
        raise ValueError("Backtest train/evaluation overlap detected")

    folds["fold_winner"] = False
    for _, group in folds.groupby(["product_id", "warehouse_id", "fold_start"]):
        eligible = group[group["target_met"]]
        if eligible.empty:
            winner = group.sort_values(
                ["fill_rate", "economic_cost_proxy"], ascending=[False, True]
            ).index[0]
        else:
            winner = eligible.sort_values("economic_cost_proxy").index[0]
        folds.loc[winner, "fold_winner"] = True

    summary = folds.groupby(
        [
            "product_id",
            "warehouse_id",
            "supplier_id",
            "category",
            "abc_class",
            "policy_id",
            "safety_stock_factor",
            "cycle_stock_days",
        ],
        as_index=False,
    ).agg(
        folds_evaluated=("fold_start", "nunique"),
        folds_won=("fold_winner", "sum"),
        target_success_rate=("target_met", "mean"),
        mean_fill_rate=("fill_rate", "mean"),
        mean_actual_fill_rate=("actual_fill_rate", "mean"),
        mean_inventory_value=("average_inventory_value", "mean"),
        mean_actual_inventory_value=("actual_average_inventory_value", "mean"),
        mean_economic_cost_proxy=("economic_cost_proxy", "mean"),
        mean_lost_margin_proxy=("lost_margin_proxy", "mean"),
        mean_order_count=("order_count", "mean"),
    )
    summary = summary.sort_values(
        [
            "product_id",
            "warehouse_id",
            "target_success_rate",
            "folds_won",
            "mean_economic_cost_proxy",
            "mean_inventory_value",
        ],
        ascending=[True, True, False, False, True, True],
    )
    recommendations = summary.drop_duplicates(["product_id", "warehouse_id"]).copy()
    recommendations["fill_rate_delta_vs_observed"] = (
        recommendations["mean_fill_rate"] - recommendations["mean_actual_fill_rate"]
    )
    recommendations["inventory_value_delta_vs_observed"] = (
        recommendations["mean_inventory_value"] - recommendations["mean_actual_inventory_value"]
    )
    recommendations["evidence_status"] = np.where(
        recommendations["folds_evaluated"] >= len(settings.fold_start_dates),
        "walk_forward_validated",
        "limited_history",
    )

    abc_summary = (
        summary.groupby(["abc_class", "policy_id"], as_index=False)
        .agg(
            sku_locations=("product_id", "size"),
            average_target_success_rate=("target_success_rate", "mean"),
            average_fill_rate=("mean_fill_rate", "mean"),
            average_inventory_value=("mean_inventory_value", "mean"),
            average_economic_cost_proxy=("mean_economic_cost_proxy", "mean"),
        )
        .sort_values(["abc_class", "average_economic_cost_proxy"])
    )

    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    folds.to_csv(OUTPUT_TABLES_DIR / "policy_backtest_folds.csv", index=False)
    recommendations.to_csv(OUTPUT_TABLES_DIR / "policy_backtest_recommendations.csv", index=False)
    abc_summary.to_csv(OUTPUT_TABLES_DIR / "policy_backtest_abc_summary.csv", index=False)
    print(
        f"Policy backtest complete. Folds: {len(folds):,}; "
        f"recommendations: {len(recommendations):,}"
    )
    return folds, recommendations


def main() -> None:
    run_policy_backtest()


if __name__ == "__main__":
    main()
