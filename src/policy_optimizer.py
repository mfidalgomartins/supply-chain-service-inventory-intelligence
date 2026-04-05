from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    from src.config import PROJECT_ROOT
except ModuleNotFoundError:
    from config import PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


def _load() -> pd.DataFrame:
    df = pd.read_csv(OUTPUT_TABLES_DIR / "policy_simulation_sku_scenarios.csv")

    keep_cols = [
        "scenario_name",
        "product_id",
        "warehouse_id",
        "supplier_id",
        "category",
        "abc_class",
        "expected_inventory_value",
        "expected_lost_sales_value_annual",
        "estimated_service_level",
    ]
    return df[keep_cols].copy()


def _build_lane_options(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    key_cols = ["product_id", "warehouse_id", "supplier_id", "category", "abc_class"]

    base = (
        df[df["scenario_name"] == "Balanced Baseline"]
        .rename(
            columns={
                "expected_inventory_value": "base_inventory",
                "expected_lost_sales_value_annual": "base_lost_sales",
                "estimated_service_level": "base_service",
            }
        )
        .copy()
    )

    if base.empty:
        raise RuntimeError("Balanced Baseline scenario not found in policy simulation outputs")

    options = (
        df.merge(base[key_cols + ["base_inventory", "base_lost_sales", "base_service"]], on=key_cols, how="inner")
        .assign(
            delta_inventory=lambda x: x["expected_inventory_value"] - x["base_inventory"],
            delta_lost_sales=lambda x: x["expected_lost_sales_value_annual"] - x["base_lost_sales"],
            lost_sales_improvement=lambda x: (x["base_lost_sales"] - x["expected_lost_sales_value_annual"]).clip(lower=0),
            service_improvement=lambda x: (x["estimated_service_level"] - x["base_service"]).clip(lower=0),
        )
    )

    options["priority_weight"] = np.select(
        [options["abc_class"] == "A", options["abc_class"] == "B"],
        [1.30, 1.00],
        default=0.75,
    )

    options["benefit_proxy"] = (
        0.70 * options["lost_sales_improvement"]
        + 0.30 * (options["service_improvement"] * 1_000_000)
    ) * options["priority_weight"]

    options["cost_proxy"] = options["delta_inventory"].clip(lower=0.0)
    options["benefit_cost_ratio"] = np.where(
        options["cost_proxy"] > 0,
        options["benefit_proxy"] / options["cost_proxy"],
        0.0,
    )

    return base, options


def _solve_budget_level(
    base: pd.DataFrame,
    options: pd.DataFrame,
    budget_uplift: float,
) -> pd.DataFrame:
    key_cols = ["product_id", "warehouse_id", "supplier_id", "category", "abc_class"]
    base_inventory_total = float(base["base_inventory"].sum())
    budget = base_inventory_total * budget_uplift

    candidates = options[options["scenario_name"].isin(["Service Protect", "Peak Guard"])].copy()
    candidates = candidates[candidates["cost_proxy"] > 0].sort_values(
        ["benefit_cost_ratio", "benefit_proxy"], ascending=[False, False]
    )

    selected: set[tuple] = set()
    selected_rows: list[pd.Series] = []
    used_budget = 0.0

    for row in candidates.itertuples(index=False):
        lane_key = (row.product_id, row.warehouse_id, row.supplier_id, row.category, row.abc_class)
        if lane_key in selected:
            continue

        new_budget = used_budget + float(row.cost_proxy)
        if new_budget <= budget:
            used_budget = new_budget
            selected.add(lane_key)
            selected_rows.append(pd.Series(row._asdict()))

    if selected_rows:
        picks = pd.DataFrame(selected_rows)
        picks = picks[key_cols + [
            "scenario_name",
            "expected_inventory_value",
            "expected_lost_sales_value_annual",
            "estimated_service_level",
            "delta_inventory",
            "lost_sales_improvement",
            "service_improvement",
            "benefit_proxy",
            "cost_proxy",
        ]]
    else:
        picks = pd.DataFrame(columns=key_cols + [
            "scenario_name",
            "expected_inventory_value",
            "expected_lost_sales_value_annual",
            "estimated_service_level",
            "delta_inventory",
            "lost_sales_improvement",
            "service_improvement",
            "benefit_proxy",
            "cost_proxy",
        ])

    lane_result = base[key_cols + ["base_inventory", "base_lost_sales", "base_service"]].copy()
    lane_result["selected_scenario"] = "Balanced Baseline"
    lane_result["selected_inventory"] = lane_result["base_inventory"]
    lane_result["selected_lost_sales"] = lane_result["base_lost_sales"]
    lane_result["selected_service"] = lane_result["base_service"]

    if not picks.empty:
        pick_map = picks.set_index(key_cols)
        lane_idx = lane_result.set_index(key_cols)
        intersect = lane_idx.index.intersection(pick_map.index)

        lane_idx.loc[intersect, "selected_scenario"] = pick_map.loc[intersect, "scenario_name"]
        lane_idx.loc[intersect, "selected_inventory"] = pick_map.loc[intersect, "expected_inventory_value"]
        lane_idx.loc[intersect, "selected_lost_sales"] = pick_map.loc[intersect, "expected_lost_sales_value_annual"]
        lane_idx.loc[intersect, "selected_service"] = pick_map.loc[intersect, "estimated_service_level"]
        lane_result = lane_idx.reset_index()

    lane_result["budget_uplift"] = budget_uplift
    lane_result["selected_delta_inventory"] = lane_result["selected_inventory"] - lane_result["base_inventory"]
    lane_result["selected_lost_sales_improvement"] = lane_result["base_lost_sales"] - lane_result["selected_lost_sales"]
    lane_result["selected_service_improvement"] = lane_result["selected_service"] - lane_result["base_service"]

    return lane_result


def _run_optimizer(base: pd.DataFrame, options: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    budget_levels = [0.00, 0.03, 0.06, 0.10, 0.15]

    all_lanes = []
    summaries = []

    for budget in budget_levels:
        lane_result = _solve_budget_level(base, options, budget)
        all_lanes.append(lane_result)

        total_base_inventory = float(lane_result["base_inventory"].sum())
        total_selected_inventory = float(lane_result["selected_inventory"].sum())
        total_base_lost = float(lane_result["base_lost_sales"].sum())
        total_selected_lost = float(lane_result["selected_lost_sales"].sum())

        weighted_base_service = np.average(lane_result["base_service"], weights=lane_result["base_inventory"].clip(lower=1.0))
        weighted_selected_service = np.average(lane_result["selected_service"], weights=lane_result["selected_inventory"].clip(lower=1.0))

        summaries.append(
            {
                "budget_uplift": budget,
                "total_base_inventory": total_base_inventory,
                "total_selected_inventory": total_selected_inventory,
                "inventory_uplift_value": total_selected_inventory - total_base_inventory,
                "total_base_lost_sales": total_base_lost,
                "total_selected_lost_sales": total_selected_lost,
                "lost_sales_improvement": total_base_lost - total_selected_lost,
                "weighted_base_service": weighted_base_service,
                "weighted_selected_service": weighted_selected_service,
                "service_uplift": weighted_selected_service - weighted_base_service,
                "lanes_changed": int((lane_result["selected_scenario"] != "Balanced Baseline").sum()),
            }
        )

    lanes_df = pd.concat(all_lanes, ignore_index=True)
    summary_df = pd.DataFrame(summaries).sort_values("budget_uplift")

    return lanes_df, summary_df


def _plot(summary: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    plot = summary.copy()
    plot["budget_label"] = (plot["budget_uplift"] * 100).round(0).astype(int).astype(str) + "%"

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()

    sns.lineplot(data=plot, x="budget_label", y="service_uplift", marker="o", color="#1F4E79", ax=ax1)
    sns.lineplot(data=plot, x="budget_label", y="lost_sales_improvement", marker="o", color="#B83232", ax=ax2)

    ax1.set_title("Policy Optimizer: Service Uplift and Lost-Sales Recovery by Capital Budget")
    ax1.set_xlabel("Capital Budget Uplift vs Baseline")
    ax1.set_ylabel("Service Uplift")
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2%}"))
    ax2.set_ylabel("Lost-Sales Improvement")
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"EUR {x:,.0f}"))

    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "policy_optimizer_budget_tradeoff.png", dpi=180)
    plt.close()


def _write_summary(summary: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    best = summary.sort_values(["lost_sales_improvement", "service_uplift"], ascending=[False, False]).iloc[0]

    lines = [
        "# Policy Optimizer Summary",
        "",
        "Optimizer selects lane-level policy upgrades under explicit working-capital budget constraints.",
        "",
        "## Best Budget Scenario",
        f"- Budget uplift: **{best['budget_uplift']:.0%}** vs baseline inventory value.",
        f"- Service uplift: **{best['service_uplift']:.2%}**.",
        f"- Estimated lost-sales improvement: **EUR {best['lost_sales_improvement']:,.0f}**.",
        f"- Lanes changed from baseline policy: **{int(best['lanes_changed'])}**.",
        "",
        "## Governance Use",
        "- Use this output to decide how much capital to allocate for service recovery.",
        "- Prioritize budgets where marginal lost-sales recovery per EUR of inventory is strongest.",
    ]

    (OUTPUT_REPORTS_DIR / "policy_optimizer_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_policy_optimizer() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    sim = _load()
    base, options = _build_lane_options(sim)
    lanes, summary = _run_optimizer(base, options)

    lanes.to_csv(OUTPUT_TABLES_DIR / "policy_optimizer_lane_selection.csv", index=False)
    summary.to_csv(OUTPUT_TABLES_DIR / "policy_optimizer_budget_summary.csv", index=False)

    _plot(summary)
    _write_summary(summary)

    print("Policy optimizer complete.")
    print(f"Lane rows: {len(lanes):,}")
    print(f"Budget rows: {len(summary):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_policy_optimizer()
