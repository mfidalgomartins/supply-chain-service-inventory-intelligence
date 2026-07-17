"""Monte Carlo optimization of service and inventory-capital policy trade-offs."""

from __future__ import annotations

import hashlib
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
from src.inventory_policy import PolicyParameters, policy_levels
from src.settings import load_settings

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


def _stable_seed(base_seed: int, *parts: str) -> int:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).digest()
    return (base_seed + int.from_bytes(digest[:4], "big")) % (2**32)


def _moving_block_bootstrap(
    rng: np.random.Generator,
    history: np.ndarray,
    simulations: int,
    total_days: int,
    block_days: int,
) -> np.ndarray:
    """Sample contiguous circular blocks to retain short-run demand dependence."""
    if block_days < 1:
        raise ValueError("block_days must be positive")
    block_count = int(np.ceil(total_days / block_days))
    starts = rng.integers(0, len(history), size=(simulations, block_count))
    offsets = np.arange(block_days)
    indices = (starts[:, :, None] + offsets) % len(history)
    return history[indices].reshape(simulations, -1)[:, :total_days]


def simulate_monte_carlo_policy(
    demand_history: np.ndarray,
    lead_time_history: np.ndarray,
    starting_on_hand: int,
    mean_daily_demand: float,
    demand_std: float,
    service_z: float,
    minimum_order_qty: int,
    unit_cost: float,
    unit_margin: float,
    annual_holding_cost_rate: float,
    ordering_cost: float,
    policy: PolicyParameters,
    simulations: int,
    warmup_days: int,
    horizon_days: int,
    seed: int,
    demand_block_days: int = 1,
) -> pd.DataFrame:
    """Vectorized stochastic simulation across runs, with daily state progression."""
    demand_history = np.asarray(demand_history, dtype=int)
    lead_time_history = np.asarray(lead_time_history, dtype=int)
    if len(demand_history) == 0 or len(lead_time_history) == 0:
        raise ValueError("Monte Carlo histories must be non-empty")
    if (demand_history < 0).any() or (lead_time_history < 1).any():
        raise ValueError("Monte Carlo histories contain invalid values")
    if simulations < 1 or warmup_days < 1 or horizon_days < 1:
        raise ValueError("simulations, warmup_days, and horizon_days must be positive")
    if demand_block_days < 1:
        raise ValueError("demand_block_days must be positive")

    rng = np.random.default_rng(seed)
    total_days = warmup_days + horizon_days
    demands = _moving_block_bootstrap(
        rng,
        demand_history,
        simulations,
        total_days,
        demand_block_days,
    )
    planning_lead_time = max(1, int(round(float(np.mean(lead_time_history)))))
    reorder_point, order_up_to = policy_levels(
        mean_daily_demand,
        demand_std,
        planning_lead_time,
        service_z,
        policy,
    )

    max_lead_time = int(lead_time_history.max())
    receipts = np.zeros((total_days + max_lead_time + 1, simulations), dtype=int)
    on_hand = np.full(simulations, starting_on_hand, dtype=int)
    on_order = np.zeros(simulations, dtype=int)
    fulfilled_total = np.zeros(simulations, dtype=int)
    fulfilled_balance_total = np.zeros(simulations, dtype=int)
    received_total = np.zeros(simulations, dtype=int)
    order_count = np.zeros(simulations, dtype=int)
    inventory_sum = np.zeros(simulations, dtype=np.int64)

    for day in range(total_days):
        received = receipts[day]
        on_hand += received
        on_order -= received
        received_total += received

        demanded = demands[:, day]
        fulfilled = np.minimum(on_hand, demanded)
        on_hand -= fulfilled
        fulfilled_balance_total += fulfilled
        if day >= warmup_days:
            fulfilled_total += fulfilled

        inventory_position = on_hand + on_order
        order_mask = inventory_position <= reorder_point
        order_indices = np.flatnonzero(order_mask)
        if len(order_indices):
            quantities = np.maximum(
                order_up_to - inventory_position[order_indices], minimum_order_qty
            ).astype(int)
            sampled_leads = rng.choice(lead_time_history, size=len(order_indices), replace=True)
            arrival_days = day + sampled_leads
            np.add.at(receipts, (arrival_days, order_indices), quantities)
            on_order[order_indices] += quantities
            if day >= warmup_days:
                order_count[order_indices] += 1

        if day >= warmup_days:
            inventory_sum += on_hand

    demanded_total = demands[:, warmup_days:].sum(axis=1)
    lost_total = demanded_total - fulfilled_total
    fill_rate = np.divide(
        fulfilled_total,
        demanded_total,
        out=np.ones(simulations, dtype=float),
        where=demanded_total > 0,
    )
    average_inventory_units = inventory_sum / horizon_days
    average_inventory_value = average_inventory_units * unit_cost
    lost_margin = lost_total * unit_margin
    holding_cost = average_inventory_value * annual_holding_cost_rate * horizon_days / 365.0
    ordering_cost_values = order_count * ordering_cost
    balance_error = starting_on_hand + received_total - fulfilled_balance_total - on_hand

    return pd.DataFrame(
        {
            "simulation": np.arange(1, simulations + 1),
            "units_demanded": demanded_total,
            "units_fulfilled": fulfilled_total,
            "units_lost": lost_total,
            "fill_rate": fill_rate,
            "average_inventory_value": average_inventory_value,
            "lost_margin_proxy": lost_margin,
            "holding_cost_proxy": holding_cost,
            "ordering_cost_proxy": ordering_cost_values,
            "economic_cost_proxy": lost_margin + holding_cost + ordering_cost_values,
            "order_count": order_count,
            "balance_error_units": balance_error,
            "reorder_point": reorder_point,
            "order_up_to": order_up_to,
        }
    )


def _is_frontier(group: pd.DataFrame) -> pd.Series:
    frontier = pd.Series(True, index=group.index)
    for idx, row in group.iterrows():
        dominated = (
            (group["expected_fill_rate"] >= row["expected_fill_rate"])
            & (group["expected_inventory_value"] <= row["expected_inventory_value"])
            & (
                (group["expected_fill_rate"] > row["expected_fill_rate"])
                | (group["expected_inventory_value"] < row["expected_inventory_value"])
            )
        ).any()
        frontier.loc[idx] = not bool(dominated)
    return frontier


def _lead_time_history(
    purchase_orders: pd.DataFrame, supplier_id: str, fallback: int
) -> np.ndarray:
    supplier_orders = purchase_orders[purchase_orders["supplier_id"] == supplier_id]
    if supplier_orders.empty:
        return np.array([fallback], dtype=int)
    lead_days = (supplier_orders["actual_arrival_date"] - supplier_orders["order_date"]).dt.days
    return lead_days.clip(lower=1, upper=60).to_numpy(dtype=int)


def run_monte_carlo_optimization(
    config_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    settings = load_settings(config_path).monte_carlo
    daily = pd.read_csv(
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"]
    )
    products = pd.read_csv(DATA_RAW / "products.csv")
    suppliers = pd.read_csv(DATA_RAW / "suppliers.csv")
    purchase_orders = pd.read_csv(
        DATA_RAW / "purchase_orders.csv",
        parse_dates=["order_date", "actual_arrival_date"],
    )
    risk = pd.read_csv(DATA_PROCESSED / "sku_risk_table.csv")
    backtest = pd.read_csv(OUTPUT_TABLES_DIR / "policy_backtest_recommendations.csv")

    entities = risk.nlargest(settings.target_entities, "governance_priority_score")[
        ["product_id", "warehouse_id", "supplier_id", "governance_priority_score"]
    ]
    entities = entities.merge(
        backtest[
            [
                "product_id",
                "warehouse_id",
                "policy_id",
                "safety_stock_factor",
                "cycle_stock_days",
            ]
        ],
        on=["product_id", "warehouse_id"],
        how="left",
        validate="one_to_one",
    )
    entities = entities.merge(
        products[
            [
                "product_id",
                "category",
                "unit_cost",
                "unit_price",
                "lead_time_days",
                "target_service_level",
            ]
        ],
        on="product_id",
        how="left",
        validate="many_to_one",
    ).merge(
        suppliers[["supplier_id", "minimum_order_qty"]],
        on="supplier_id",
        how="left",
        validate="many_to_one",
    )

    scenario_rows: list[dict] = []
    for entity in entities.sort_values(["product_id", "warehouse_id"]).itertuples(index=False):
        history = daily[
            (daily["product_id"] == entity.product_id)
            & (daily["warehouse_id"] == entity.warehouse_id)
        ].sort_values("date")
        demand_history = history["units_demanded"].tail(365).to_numpy(dtype=int)
        mean_demand = float(demand_history.mean())
        demand_std = float(demand_history.std(ddof=0))
        starting_on_hand = int(history["on_hand_units"].iloc[-1])
        baseline_inventory = float(history["inventory_value"].tail(90).mean())
        target_service = float(entity.target_service_level)
        service_z = NormalDist().inv_cdf(float(np.clip(target_service, 0.5001, 0.999)))
        lead_history = _lead_time_history(
            purchase_orders, entity.supplier_id, int(entity.lead_time_days)
        )
        unit_margin = max(0.0, float(entity.unit_price - entity.unit_cost))

        for variant in settings.variants:
            policy = PolicyParameters(
                policy_id=variant.scenario_id,
                safety_stock_factor=float(
                    entity.safety_stock_factor * variant.safety_factor_multiplier
                ),
                cycle_stock_days=max(1, int(entity.cycle_stock_days + variant.cycle_days_delta)),
            )
            simulations = simulate_monte_carlo_policy(
                demand_history=demand_history,
                lead_time_history=lead_history,
                starting_on_hand=starting_on_hand,
                mean_daily_demand=mean_demand,
                demand_std=demand_std,
                service_z=service_z,
                minimum_order_qty=int(entity.minimum_order_qty),
                unit_cost=float(entity.unit_cost),
                unit_margin=unit_margin,
                annual_holding_cost_rate=settings.annual_holding_cost_rate,
                ordering_cost=settings.ordering_cost,
                policy=policy,
                simulations=settings.simulations,
                warmup_days=settings.warmup_days,
                horizon_days=settings.horizon_days,
                seed=_stable_seed(
                    settings.seed,
                    entity.product_id,
                    entity.warehouse_id,
                    variant.scenario_id,
                ),
                demand_block_days=settings.demand_block_days,
            )
            balance_error_max = int(simulations["balance_error_units"].abs().max())
            if balance_error_max:
                raise ValueError(
                    f"Monte Carlo inventory balance failed for "
                    f"{entity.product_id}/{entity.warehouse_id}/{variant.scenario_id}"
                )
            scenario_rows.append(
                {
                    "product_id": entity.product_id,
                    "warehouse_id": entity.warehouse_id,
                    "supplier_id": entity.supplier_id,
                    "category": entity.category,
                    "governance_priority_score": entity.governance_priority_score,
                    "source_policy_id": entity.policy_id,
                    "scenario_id": variant.scenario_id,
                    "safety_stock_factor": policy.safety_stock_factor,
                    "cycle_stock_days": policy.cycle_stock_days,
                    "target_service_level": target_service,
                    "baseline_inventory_value": baseline_inventory,
                    "expected_units_demanded": simulations["units_demanded"].mean(),
                    "expected_units_fulfilled": simulations["units_fulfilled"].mean(),
                    "expected_fill_rate": simulations["fill_rate"].mean(),
                    "fill_rate_p10": simulations["fill_rate"].quantile(0.10),
                    "fill_rate_p50": simulations["fill_rate"].quantile(0.50),
                    "fill_rate_p90": simulations["fill_rate"].quantile(0.90),
                    "probability_target_met": (simulations["fill_rate"] >= target_service).mean(),
                    "expected_inventory_value": simulations["average_inventory_value"].mean(),
                    "inventory_value_p90": simulations["average_inventory_value"].quantile(0.90),
                    "expected_lost_margin_proxy": simulations["lost_margin_proxy"].mean(),
                    "lost_margin_p95": simulations["lost_margin_proxy"].quantile(0.95),
                    "expected_total_cost_proxy": simulations["economic_cost_proxy"].mean(),
                    "expected_order_count": simulations["order_count"].mean(),
                    "balance_error_max": balance_error_max,
                    "simulation_count": settings.simulations,
                    "warmup_days": settings.warmup_days,
                    "horizon_days": settings.horizon_days,
                    "demand_block_days": settings.demand_block_days,
                }
            )

    scenarios = pd.DataFrame(scenario_rows)
    scenarios["is_frontier"] = scenarios.groupby(
        ["product_id", "warehouse_id"], group_keys=False
    ).apply(_is_frontier, include_groups=False)
    scenarios["capital_constraint_met"] = scenarios["expected_inventory_value"] <= (
        scenarios["baseline_inventory_value"] * (1.0 + settings.max_inventory_increase_rate)
    )
    scenarios["inventory_increase_rate"] = (
        np.divide(
            scenarios["expected_inventory_value"],
            scenarios["baseline_inventory_value"],
            out=np.full(len(scenarios), np.inf),
            where=scenarios["baseline_inventory_value"] > 0,
        )
        - 1.0
    )
    scenarios["capital_excess_rate"] = (
        scenarios["inventory_increase_rate"] - settings.max_inventory_increase_rate
    ).clip(lower=0)
    scenarios["service_constraint_met"] = (
        scenarios["probability_target_met"] >= settings.target_confidence
    )
    scenarios["selection_eligible"] = (
        scenarios["is_frontier"]
        & scenarios["capital_constraint_met"]
        & scenarios["service_constraint_met"]
    )

    recommendations: list[pd.Series] = []
    for _, group in scenarios.groupby(["product_id", "warehouse_id"]):
        eligible = group[group["selection_eligible"]]
        if not eligible.empty:
            selected = eligible.sort_values("expected_total_cost_proxy").iloc[0].copy()
            selected["selection_reason"] = "frontier_constraints_met"
        else:
            service_feasible = group[group["is_frontier"] & group["service_constraint_met"]]
            capital_feasible = group[group["is_frontier"] & group["capital_constraint_met"]]
            if not service_feasible.empty:
                selected = (
                    service_feasible.sort_values(
                        ["capital_excess_rate", "expected_total_cost_proxy"]
                    )
                    .iloc[0]
                    .copy()
                )
                selected["selection_reason"] = "service_met_capital_relaxed"
            elif not capital_feasible.empty:
                selected = (
                    capital_feasible.sort_values(
                        ["probability_target_met", "expected_total_cost_proxy"],
                        ascending=[False, True],
                    )
                    .iloc[0]
                    .copy()
                )
                selected["selection_reason"] = "capital_met_service_relaxed"
            else:
                selected = (
                    group.sort_values(
                        ["probability_target_met", "expected_total_cost_proxy"],
                        ascending=[False, True],
                    )
                    .iloc[0]
                    .copy()
                )
                selected["selection_reason"] = "best_available_under_constraints"
        recommendations.append(selected)
    recommendation_df = pd.DataFrame(recommendations).reset_index(drop=True)

    expected_demand = float(recommendation_df["expected_units_demanded"].sum())
    expected_fulfilled = float(recommendation_df["expected_units_fulfilled"].sum())
    portfolio_metrics = [
        ("selected_entity_count", len(recommendation_df), "SKU-locations optimized"),
        (
            "demand_weighted_expected_fill_rate",
            expected_fulfilled / expected_demand if expected_demand else 1.0,
            "Expected fulfilled units divided by expected demand",
        ),
        (
            "expected_inventory_value",
            recommendation_df["expected_inventory_value"].sum(),
            "Sum of expected average inventory value across selected entities",
        ),
        (
            "expected_lost_margin_proxy",
            recommendation_df["expected_lost_margin_proxy"].sum(),
            "Expected lost margin over the simulation horizon",
        ),
        (
            "expected_total_cost_proxy",
            recommendation_df["expected_total_cost_proxy"].sum(),
            "Lost margin plus holding and ordering cost proxies",
        ),
        (
            "service_constraint_success_rate",
            recommendation_df["service_constraint_met"].mean(),
            "Share of selected policies meeting the service confidence constraint",
        ),
        (
            "capital_constraint_success_rate",
            recommendation_df["capital_constraint_met"].mean(),
            "Share of selected policies within the configured inventory-capital guardrail",
        ),
        (
            "joint_constraint_success_rate",
            (
                recommendation_df["service_constraint_met"]
                & recommendation_df["capital_constraint_met"]
            ).mean(),
            "Share of selected policies meeting service and capital constraints together",
        ),
        (
            "constraint_relaxation_rate",
            (recommendation_df["selection_reason"] != "frontier_constraints_met").mean(),
            "Share of selections requiring an explicitly labelled relaxed constraint",
        ),
        (
            "expected_incremental_inventory_value",
            (
                recommendation_df["expected_inventory_value"]
                - recommendation_df["baseline_inventory_value"]
            ).sum(),
            "Expected inventory increase versus the observed baseline for selected entities",
        ),
        (
            "frontier_selection_rate",
            recommendation_df["is_frontier"].mean(),
            "Share of selected policies on the service-capital frontier",
        ),
    ]
    portfolio = pd.DataFrame(portfolio_metrics, columns=["metric", "value", "definition"])

    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    scenarios.to_csv(OUTPUT_TABLES_DIR / "monte_carlo_policy_scenarios.csv", index=False)
    recommendation_df.to_csv(OUTPUT_TABLES_DIR / "monte_carlo_recommendations.csv", index=False)
    portfolio.to_csv(OUTPUT_TABLES_DIR / "monte_carlo_portfolio_summary.csv", index=False)
    print(
        f"Monte Carlo optimization complete. Entities: {len(recommendation_df)}; "
        f"scenario runs: {len(scenarios) * settings.simulations:,}"
    )
    return scenarios, recommendation_df


def main() -> None:
    run_monte_carlo_optimization()


if __name__ == "__main__":
    main()
