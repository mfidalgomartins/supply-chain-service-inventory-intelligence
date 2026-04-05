from __future__ import annotations

from datetime import datetime, timezone

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


OWNER_MAP = {
    "Service Recovery": "Inventory Planning",
    "Capital Release": "Supply Chain Finance",
    "Supplier Stabilization": "Procurement",
    "Working Capital Optimization": "Inventory Planning",
    "Monitor": "Operations Intelligence",
}

SLA_DAYS_MAP = {
    "Critical": 14,
    "High": 21,
    "Medium": 35,
    "Low": 60,
}


def _status_priority(status: str) -> int:
    order = {"Open": 0, "In Progress": 1, "Monitor": 2, "Closed": 3}
    return order.get(status, 9)


def _apply_priority_order(register: pd.DataFrame) -> pd.DataFrame:
    out = register.copy()
    out["status_priority"] = out["intervention_status"].map(_status_priority).fillna(9).astype(int)
    out = out.sort_values(
        ["status_priority", "governance_priority_score", "expected_value_proxy"],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    out.drop(columns=["status_priority"], inplace=True)
    return out


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    sku = pd.read_csv(DATA_PROCESSED / "sku_risk_table.csv")
    products = pd.read_csv(DATA_RAW / "products.csv")[["product_id", "product_name"]]
    return sku, products


def _derive_intervention_type(df: pd.DataFrame) -> pd.Series:
    return np.select(
        [
            (df["stockout_risk_score"] >= 75) & (df["fill_rate"] < 0.90),
            (df["supplier_risk_score"] >= 70),
            (df["excess_inventory_score"] >= 70) & (df["stockout_risk_score"] < 60),
            (df["working_capital_risk_score"] >= 70),
        ],
        [
            "Service Recovery",
            "Supplier Stabilization",
            "Capital Release",
            "Working Capital Optimization",
        ],
        default="Monitor",
    )


def _build_register(sku: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    as_of = datetime.now(timezone.utc).date()

    register = sku.merge(products, on="product_id", how="left")
    register["product_name"] = register["product_name"].fillna(register["product_id"])

    register["intervention_type"] = _derive_intervention_type(register)
    register["owner_function"] = register["intervention_type"].map(OWNER_MAP).fillna("Operations Intelligence")
    register["sla_days"] = register["risk_tier"].map(SLA_DAYS_MAP).fillna(45).astype(int)
    register["created_date"] = pd.to_datetime(as_of)
    register["due_date"] = register["created_date"] + pd.to_timedelta(register["sla_days"], unit="D")

    register["expected_margin_recovery_proxy"] = register["lost_sales_revenue"] * 0.35 * (register["service_risk_score"] / 100.0)
    register["expected_wc_release_proxy"] = (
        register["inventory_value_sum"] * 0.25 * (register["working_capital_risk_score"] / 100.0)
    )
    register["expected_value_proxy"] = register["expected_margin_recovery_proxy"] + register["expected_wc_release_proxy"]

    register["intervention_status"] = np.where(register["risk_tier"].isin(["Critical", "High"]), "Open", "Monitor")
    register["target_governance_score"] = np.where(
        register["risk_tier"] == "Critical",
        60.0,
        np.where(register["risk_tier"] == "High", 45.0, np.where(register["risk_tier"] == "Medium", 30.0, 20.0)),
    )
    register["required_score_reduction"] = (register["governance_priority_score"] - register["target_governance_score"]).clip(lower=0.0)

    register["evidence_required"] = np.select(
        [
            register["intervention_type"] == "Service Recovery",
            register["intervention_type"] == "Supplier Stabilization",
            register["intervention_type"] == "Capital Release",
            register["intervention_type"] == "Working Capital Optimization",
        ],
        [
            "ROP/safety-stock update plus fill-rate improvement evidence",
            "Supplier corrective action plan and OTD recovery evidence",
            "DOS reduction plan and inventory release evidence",
            "Policy rebalance with tracked service-capital outcome",
        ],
        default="Periodic risk monitoring evidence",
    )

    optimizer_path = OUTPUT_TABLES_DIR / "policy_optimizer_lane_selection.csv"
    if optimizer_path.exists():
        opt = pd.read_csv(optimizer_path)
        if "budget_uplift" in opt.columns:
            opt = opt.sort_values("budget_uplift", ascending=False).drop_duplicates(["product_id", "warehouse_id", "supplier_id"])
            register = register.merge(
                opt[["product_id", "warehouse_id", "supplier_id", "selected_scenario"]],
                on=["product_id", "warehouse_id", "supplier_id"],
                how="left",
            )
        else:
            register["selected_scenario"] = "Balanced Baseline"
    else:
        register["selected_scenario"] = "Balanced Baseline"

    register["selected_scenario"] = register["selected_scenario"].fillna("Balanced Baseline")

    register = _apply_priority_order(register)
    register["intervention_rank"] = np.arange(1, len(register) + 1)
    register["intervention_id"] = "INT-" + register["intervention_rank"].astype(str).str.zfill(5)

    ordered_cols = [
        "intervention_id",
        "intervention_rank",
        "created_date",
        "due_date",
        "intervention_status",
        "owner_function",
        "intervention_type",
        "product_id",
        "product_name",
        "warehouse_id",
        "supplier_id",
        "category",
        "region",
        "risk_tier",
        "main_risk_driver",
        "recommended_action",
        "selected_scenario",
        "governance_priority_score",
        "target_governance_score",
        "required_score_reduction",
        "service_risk_score",
        "stockout_risk_score",
        "excess_inventory_score",
        "supplier_risk_score",
        "working_capital_risk_score",
        "fill_rate",
        "stockout_rate",
        "lost_sales_revenue",
        "inventory_value_sum",
        "expected_margin_recovery_proxy",
        "expected_wc_release_proxy",
        "expected_value_proxy",
        "sla_days",
        "evidence_required",
    ]

    return register[ordered_cols].copy()


def _build_summaries(register: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_owner = (
        register.groupby(["owner_function", "intervention_status"], as_index=False)
        .agg(
            intervention_count=("intervention_id", "count"),
            avg_governance_score=("governance_priority_score", "mean"),
            total_expected_value_proxy=("expected_value_proxy", "sum"),
        )
        .sort_values(["total_expected_value_proxy", "intervention_count"], ascending=[False, False])
    )

    by_driver = (
        register.groupby(["main_risk_driver", "intervention_type"], as_index=False)
        .agg(
            intervention_count=("intervention_id", "count"),
            avg_governance_score=("governance_priority_score", "mean"),
            total_lost_sales=("lost_sales_revenue", "sum"),
            total_inventory_value=("inventory_value_sum", "sum"),
            total_expected_value_proxy=("expected_value_proxy", "sum"),
        )
        .sort_values(["total_expected_value_proxy", "intervention_count"], ascending=[False, False])
    )

    return by_owner, by_driver


def _plot(by_owner: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    plot = by_owner.copy()
    plt.figure(figsize=(11, 6))
    sns.barplot(data=plot, x="owner_function", y="total_expected_value_proxy", hue="intervention_status", palette="Set2")
    plt.title("Intervention Tracker: Expected Value Backlog by Owner")
    plt.xlabel("Owner Function")
    plt.ylabel("Expected Value Proxy")
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"EUR {x:,.0f}"))
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "intervention_backlog_by_owner.png", dpi=180)
    plt.close()


def _write_summary(register: pd.DataFrame, by_owner: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    active = register[register["intervention_status"] == "Open"].copy()
    top = active.iloc[0] if not active.empty else register.iloc[0]
    top_owner = by_owner.sort_values("total_expected_value_proxy", ascending=False).iloc[0]

    lines = [
        "# Intervention Tracker Summary",
        "",
        "This layer operationalizes governance scores into an action register with owners, due dates, and expected value impact proxies.",
        "",
        "## Backlog Snapshot",
        f"- Total interventions: **{len(register):,}**",
        f"- Open interventions: **{int((register['intervention_status'] == 'Open').sum()):,}**",
        f"- Total expected value proxy: **EUR {register['expected_value_proxy'].sum():,.0f}**",
        "",
        "## Highest Priority Item",
        f"- {top['intervention_id']} | {top['product_id']} | {top['warehouse_id']} | {top['supplier_id']}",
        f"- Risk tier: **{top['risk_tier']}**, governance score: **{top['governance_priority_score']:.2f}**",
        f"- Owner: **{top['owner_function']}**, due by: **{pd.to_datetime(top['due_date']).strftime('%Y-%m-%d')}**",
        f"- Expected value proxy: **EUR {top['expected_value_proxy']:,.0f}**",
        "",
        "## Highest Backlog Owner",
        f"- **{top_owner['owner_function']}** with expected backlog value **EUR {top_owner['total_expected_value_proxy']:,.0f}**.",
        "",
        "## Governance Use",
        "- Use this register in weekly S&OP governance to track closure and score migration.",
        "- Escalate items with due-date breach and persistent score deterioration.",
    ]

    (OUTPUT_REPORTS_DIR / "intervention_tracker_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_intervention_tracker() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    sku, products = _load_inputs()
    register = _build_register(sku, products)
    by_owner, by_driver = _build_summaries(register)

    register.to_csv(OUTPUT_TABLES_DIR / "intervention_register.csv", index=False)
    by_owner.to_csv(OUTPUT_TABLES_DIR / "intervention_summary_by_owner.csv", index=False)
    by_driver.to_csv(OUTPUT_TABLES_DIR / "intervention_summary_by_driver.csv", index=False)

    _plot(by_owner)
    _write_summary(register, by_owner)

    print("Intervention tracker complete.")
    print(f"Register rows: {len(register):,}")
    print(f"Owner summary rows: {len(by_owner):,}")
    print(f"Driver summary rows: {len(by_driver):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_intervention_tracker()
