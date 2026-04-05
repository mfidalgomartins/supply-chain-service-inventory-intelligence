from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    from src.config import DATA_PROCESSED, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, PROJECT_ROOT


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


@dataclass(frozen=True)
class Thresholds:
    fill_gap_good: float = 0.01
    fill_gap_bad: float = 0.15
    service_gap_good: float = 0.00
    service_gap_bad: float = 0.20
    stockout_rate_good: float = 0.01
    stockout_rate_bad: float = 0.18
    stockout_persistence_good: float = 0.10
    stockout_persistence_bad: float = 0.65
    dos_stretch_good: float = 1.00
    dos_stretch_bad: float = 2.25
    excess_day_rate_good: float = 0.05
    excess_day_rate_bad: float = 0.40
    slow_moving_good: float = 0.01
    slow_moving_bad: float = 0.12
    criticality_good: float = 0.45
    criticality_bad: float = 0.85
    supplier_otd_gap_good: float = 0.05
    supplier_otd_gap_bad: float = 0.45
    supplier_delay_good: float = 0.50
    supplier_delay_bad: float = 5.00
    supplier_lt_var_good: float = 1.50
    supplier_lt_var_bad: float = 10.00
    supplier_underfill_good: float = 0.00
    supplier_underfill_bad: float = 0.08
    lost_share_low: float = 0.0005
    lost_share_high: float = 0.08
    inventory_share_low: float = 0.0010
    inventory_share_high: float = 0.12


THRESHOLDS = Thresholds()


def linear_score(values: pd.Series, good: float, bad: float) -> pd.Series:
    if bad <= good:
        raise ValueError("'bad' must be greater than 'good' for scoring")
    score = (values - good) / (bad - good)
    return (score * 100.0).clip(0, 100)


def log_share_score(values: pd.Series, low: float, high: float) -> pd.Series:
    if low <= 0 or high <= low:
        raise ValueError("Share score bounds must satisfy 0 < low < high")

    eps = 1e-12
    safe_values = values.clip(lower=0) + eps
    low_log = np.log(low + eps)
    high_log = np.log(high + eps)
    scaled = (np.log(safe_values) - low_log) / (high_log - low_log)
    return (scaled * 100.0).clip(0, 100)


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    den = denominator.replace(0, np.nan)
    return (numerator / den).fillna(0.0)


def assign_risk_tier(score: pd.Series) -> pd.Series:
    return pd.cut(
        score,
        bins=[-0.001, 35, 55, 75, 1000],
        labels=["Low", "Medium", "High", "Critical"],
    ).astype(str)


def score_supplier_base(supplier_performance: pd.DataFrame) -> pd.DataFrame:
    scored = supplier_performance.copy()

    otd_gap = (1.0 - scored["on_time_delivery_rate"]).clip(0, 1)
    delay = scored["average_delay_days"].clip(lower=0)
    lt_var = scored["lead_time_variability"].clip(lower=0)
    underfill = (1.0 - scored["received_vs_ordered_fill_rate"]).clip(0, 1)

    otd_score = linear_score(otd_gap, THRESHOLDS.supplier_otd_gap_good, THRESHOLDS.supplier_otd_gap_bad)
    delay_score = linear_score(delay, THRESHOLDS.supplier_delay_good, THRESHOLDS.supplier_delay_bad)
    lt_var_score = linear_score(lt_var, THRESHOLDS.supplier_lt_var_good, THRESHOLDS.supplier_lt_var_bad)
    underfill_score = linear_score(
        underfill, THRESHOLDS.supplier_underfill_good, THRESHOLDS.supplier_underfill_bad
    )

    scored["supplier_risk_score_base"] = (
        0.45 * otd_score + 0.20 * delay_score + 0.20 * lt_var_score + 0.15 * underfill_score
    ).clip(0, 100)

    return scored[
        [
            "supplier_id",
            "supplier_name",
            "on_time_delivery_rate",
            "average_delay_days",
            "lead_time_variability",
            "received_vs_ordered_fill_rate",
            "supplier_risk_score_base",
        ]
    ]


def prepare_daily_input(daily: pd.DataFrame, supplier_scores: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()

    out["date"] = pd.to_datetime(out["date"])
    out["month"] = out["date"].dt.to_period("M").dt.to_timestamp()

    out["abc_dos_cap"] = np.select(
        [out["abc_class"] == "A", out["abc_class"] == "B"],
        [20.0, 30.0],
        default=45.0,
    )
    out["excess_day"] = (out["days_of_supply"] > out["abc_dos_cap"]).astype(int)
    out["slow_moving_day"] = ((out["available_units"] > 0) & (out["units_fulfilled"] == 0)).astype(int)

    criticality_map = {"High": 1.0, "Medium": 0.6, "Low": 0.3}
    out["criticality_weight"] = out["criticality_level"].map(criticality_map).fillna(0.6)

    demand_weight = out["units_demanded"].clip(lower=1)
    out["demand_weight"] = demand_weight
    out["weighted_criticality"] = out["criticality_weight"] * demand_weight
    out["weighted_dos_cap"] = out["abc_dos_cap"] * demand_weight

    supplier_score_map = supplier_scores[["supplier_id", "supplier_risk_score_base"]]
    out = out.merge(supplier_score_map, on="supplier_id", how="left")
    out["supplier_risk_score_base"] = out["supplier_risk_score_base"].fillna(50.0)
    out["weighted_supplier_risk"] = out["supplier_risk_score_base"] * demand_weight

    return out


def aggregate_entity(daily: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    monthly_cols = group_cols + ["month"]

    monthly = (
        daily.groupby(monthly_cols, as_index=False)
        .agg(
            month_units_demanded=("units_demanded", "sum"),
            month_units_lost_sales=("units_lost_sales", "sum"),
        )
        .assign(stockout_month_flag=lambda df: (df["month_units_lost_sales"] > 0).astype(int))
    )

    stockout_persistence = (
        monthly.groupby(group_cols, as_index=False)["stockout_month_flag"]
        .mean()
        .rename(columns={"stockout_month_flag": "stockout_active_month_ratio"})
    )

    agg = (
        daily.groupby(group_cols, as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_fulfilled=("units_fulfilled", "sum"),
            units_lost_sales=("units_lost_sales", "sum"),
            service_gap_units=("service_gap_units", "sum"),
            stockout_frequency=("stockout_flag", "mean"),
            excess_day_rate=("excess_day", "mean"),
            slow_moving_rate=("slow_moving_day", "mean"),
            average_days_of_supply=("days_of_supply", "mean"),
            average_inventory_value=("inventory_value", "mean"),
            inventory_value_sum=("inventory_value", "sum"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
            weighted_criticality_sum=("weighted_criticality", "sum"),
            demand_weight_sum=("demand_weight", "sum"),
            weighted_dos_cap_sum=("weighted_dos_cap", "sum"),
            weighted_supplier_risk_sum=("weighted_supplier_risk", "sum"),
            active_days=("date", "nunique"),
        )
        .merge(stockout_persistence, on=group_cols, how="left")
    )

    agg["fill_rate"] = safe_divide(agg["units_fulfilled"], agg["units_demanded"])
    agg["stockout_rate"] = safe_divide(agg["units_lost_sales"], agg["units_demanded"])
    agg["service_gap_rate"] = safe_divide(agg["service_gap_units"], agg["units_demanded"])

    agg["criticality_index"] = safe_divide(agg["weighted_criticality_sum"], agg["demand_weight_sum"])
    agg["avg_dos_cap"] = safe_divide(agg["weighted_dos_cap_sum"], agg["demand_weight_sum"]).replace(0, np.nan)
    agg["dos_stretch"] = (agg["average_days_of_supply"] / agg["avg_dos_cap"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    agg["supplier_risk_score"] = safe_divide(
        agg["weighted_supplier_risk_sum"], agg["demand_weight_sum"]
    ).clip(0, 100)

    agg["stockout_active_month_ratio"] = agg["stockout_active_month_ratio"].fillna(0.0)

    return agg


def compute_component_scores(entity_df: pd.DataFrame, base_daily: pd.DataFrame) -> pd.DataFrame:
    out = entity_df.copy()

    total_company_lost_sales = float(base_daily["lost_sales_revenue"].sum())
    total_company_inventory = float(base_daily["inventory_value"].sum())

    out["lost_sales_share"] = out["lost_sales_revenue"] / max(total_company_lost_sales, 1.0)
    out["inventory_value_share"] = out["inventory_value_sum"] / max(total_company_inventory, 1.0)

    fill_gap_score = linear_score(
        (1.0 - out["fill_rate"]).clip(0, 1), THRESHOLDS.fill_gap_good, THRESHOLDS.fill_gap_bad
    )
    service_gap_score = linear_score(
        out["service_gap_rate"].clip(0, 1), THRESHOLDS.service_gap_good, THRESHOLDS.service_gap_bad
    )
    criticality_score = linear_score(
        out["criticality_index"].clip(0, 1), THRESHOLDS.criticality_good, THRESHOLDS.criticality_bad
    )
    lost_share_score = log_share_score(
        out["lost_sales_share"], THRESHOLDS.lost_share_low, THRESHOLDS.lost_share_high
    )

    stockout_rate_score = linear_score(
        out["stockout_rate"].clip(0, 1), THRESHOLDS.stockout_rate_good, THRESHOLDS.stockout_rate_bad
    )
    stockout_persistence_score = linear_score(
        out["stockout_active_month_ratio"].clip(0, 1),
        THRESHOLDS.stockout_persistence_good,
        THRESHOLDS.stockout_persistence_bad,
    )

    dos_stretch_score = linear_score(
        out["dos_stretch"].clip(lower=0), THRESHOLDS.dos_stretch_good, THRESHOLDS.dos_stretch_bad
    )
    excess_day_score = linear_score(
        out["excess_day_rate"].clip(0, 1), THRESHOLDS.excess_day_rate_good, THRESHOLDS.excess_day_rate_bad
    )
    slow_moving_score = linear_score(
        out["slow_moving_rate"].clip(0, 1), THRESHOLDS.slow_moving_good, THRESHOLDS.slow_moving_bad
    )
    inventory_share_score = log_share_score(
        out["inventory_value_share"], THRESHOLDS.inventory_share_low, THRESHOLDS.inventory_share_high
    )

    out["service_risk_score"] = (
        0.35 * fill_gap_score + 0.30 * service_gap_score + 0.20 * criticality_score + 0.15 * lost_share_score
    ).clip(0, 100)

    out["stockout_risk_score"] = (
        0.55 * stockout_rate_score + 0.30 * stockout_persistence_score + 0.15 * lost_share_score
    ).clip(0, 100)

    out["excess_inventory_score"] = (
        0.45 * dos_stretch_score + 0.35 * excess_day_score + 0.20 * inventory_share_score
    ).clip(0, 100)

    out["working_capital_risk_score"] = (
        0.45 * dos_stretch_score + 0.30 * slow_moving_score + 0.25 * inventory_share_score
    ).clip(0, 100)

    out["supplier_risk_score"] = out["supplier_risk_score"].clip(0, 100)

    dual_imbalance_score = np.minimum(out["service_risk_score"], out["excess_inventory_score"])

    out["governance_priority_score"] = (
        0.24 * out["service_risk_score"]
        + 0.22 * out["stockout_risk_score"]
        + 0.18 * out["excess_inventory_score"]
        + 0.16 * out["supplier_risk_score"]
        + 0.14 * out["working_capital_risk_score"]
        + 0.06 * dual_imbalance_score
    ).clip(0, 100)

    score_cols = [
        "service_risk_score",
        "stockout_risk_score",
        "excess_inventory_score",
        "supplier_risk_score",
        "working_capital_risk_score",
    ]
    score_name_map = {
        "service_risk_score": "Service Risk",
        "stockout_risk_score": "Stockout Risk",
        "excess_inventory_score": "Excess Inventory",
        "supplier_risk_score": "Supplier Risk",
        "working_capital_risk_score": "Working Capital",
    }

    out["risk_tier"] = assign_risk_tier(out["governance_priority_score"])
    out["main_risk_driver"] = out[score_cols].idxmax(axis=1).map(score_name_map)

    return out


def recommended_action(main_driver: str, risk_tier: str, entity_type: str) -> str:
    if risk_tier == "Low":
        return "monitor only"

    action_library = {
        "Service Risk": {
            "sku_warehouse": "review reorder point and raise safety stock",
            "supplier": "review service target by SKU class and stabilize replenishment cadence",
            "segment": "review planning assumptions and rebalance stock across warehouses",
        },
        "Stockout Risk": {
            "sku_warehouse": "expedite replenishment and rebalance stock across warehouses",
            "supplier": "expedite replenishment on constrained supplier lanes",
            "segment": "raise safety stock for critical SKUs and protect promotion allocation",
        },
        "Excess Inventory": {
            "sku_warehouse": "reduce safety stock and review assortment strategy",
            "supplier": "review MOQ and order cadence to reduce overstock propagation",
            "segment": "rebalance stock across warehouses and reduce order-up-to levels",
        },
        "Supplier Risk": {
            "sku_warehouse": "investigate supplier reliability and qualify backup source",
            "supplier": "investigate supplier reliability and execute corrective action plan",
            "segment": "review sourcing mix and supplier dependency for this segment",
        },
        "Working Capital": {
            "sku_warehouse": "review planning assumptions and tighten order-up-to policy",
            "supplier": "review planning assumptions and align procurement with demand volatility",
            "segment": "review assortment strategy and release cash from slow-moving inventory",
        },
    }

    return action_library.get(main_driver, {}).get(entity_type, "review planning assumptions")


def build_sku_scoring(daily: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["product_id", "warehouse_id", "supplier_id", "category", "region"]
    sku = aggregate_entity(daily, group_cols)
    sku = compute_component_scores(sku, daily)

    sku["recommended_action"] = [
        recommended_action(driver, tier, "sku_warehouse")
        for driver, tier in zip(sku["main_risk_driver"], sku["risk_tier"])
    ]

    return sku[
        [
            "product_id",
            "warehouse_id",
            "supplier_id",
            "category",
            "region",
            "service_risk_score",
            "stockout_risk_score",
            "excess_inventory_score",
            "supplier_risk_score",
            "working_capital_risk_score",
            "governance_priority_score",
            "risk_tier",
            "main_risk_driver",
            "recommended_action",
            "fill_rate",
            "stockout_rate",
            "service_gap_rate",
            "excess_day_rate",
            "slow_moving_rate",
            "dos_stretch",
            "lost_sales_share",
            "inventory_value_share",
            "lost_sales_revenue",
            "inventory_value_sum",
        ]
    ].sort_values(["governance_priority_score", "lost_sales_revenue"], ascending=[False, False])


def build_supplier_scoring(daily: pd.DataFrame, supplier_base_scores: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["supplier_id"]
    supplier = aggregate_entity(daily, group_cols)

    supplier = supplier.merge(
        supplier_base_scores[
            [
                "supplier_id",
                "supplier_name",
                "on_time_delivery_rate",
                "average_delay_days",
                "lead_time_variability",
                "received_vs_ordered_fill_rate",
                "supplier_risk_score_base",
            ]
        ],
        on="supplier_id",
        how="left",
    )

    supplier["supplier_risk_score"] = supplier["supplier_risk_score_base"].fillna(supplier["supplier_risk_score"])
    supplier = compute_component_scores(supplier, daily)

    supplier["recommended_action"] = [
        recommended_action(driver, tier, "supplier")
        for driver, tier in zip(supplier["main_risk_driver"], supplier["risk_tier"])
    ]

    return supplier[
        [
            "supplier_id",
            "supplier_name",
            "service_risk_score",
            "stockout_risk_score",
            "excess_inventory_score",
            "supplier_risk_score",
            "working_capital_risk_score",
            "governance_priority_score",
            "risk_tier",
            "main_risk_driver",
            "recommended_action",
            "on_time_delivery_rate",
            "average_delay_days",
            "lead_time_variability",
            "received_vs_ordered_fill_rate",
            "fill_rate",
            "stockout_rate",
            "lost_sales_share",
            "inventory_value_share",
            "lost_sales_revenue",
            "inventory_value_sum",
        ]
    ].sort_values(["governance_priority_score", "lost_sales_revenue"], ascending=[False, False])


def build_segment_scoring(daily: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["category", "region"]
    segment = aggregate_entity(daily, group_cols)
    segment = compute_component_scores(segment, daily)

    segment["segment_id"] = segment["category"] + " | " + segment["region"]
    segment["recommended_action"] = [
        recommended_action(driver, tier, "segment")
        for driver, tier in zip(segment["main_risk_driver"], segment["risk_tier"])
    ]

    return segment[
        [
            "segment_id",
            "category",
            "region",
            "service_risk_score",
            "stockout_risk_score",
            "excess_inventory_score",
            "supplier_risk_score",
            "working_capital_risk_score",
            "governance_priority_score",
            "risk_tier",
            "main_risk_driver",
            "recommended_action",
            "fill_rate",
            "stockout_rate",
            "lost_sales_share",
            "inventory_value_share",
            "lost_sales_revenue",
            "inventory_value_sum",
        ]
    ].sort_values(["governance_priority_score", "lost_sales_revenue"], ascending=[False, False])


def build_master_priority_table(
    sku_scores: pd.DataFrame,
    supplier_scores: pd.DataFrame,
    segment_scores: pd.DataFrame,
) -> pd.DataFrame:
    sku_master = sku_scores.assign(
        entity_type="sku_warehouse",
        entity_id=sku_scores["product_id"] + "|" + sku_scores["warehouse_id"],
        entity_name=sku_scores["product_id"] + " @ " + sku_scores["warehouse_id"],
    )

    supplier_master = supplier_scores.assign(
        entity_type="supplier",
        entity_id=supplier_scores["supplier_id"],
        entity_name=supplier_scores["supplier_name"],
    )

    segment_master = segment_scores.assign(
        entity_type="segment",
        entity_id=segment_scores["segment_id"],
        entity_name=segment_scores["segment_id"],
    )

    keep_cols = [
        "entity_type",
        "entity_id",
        "entity_name",
        "service_risk_score",
        "stockout_risk_score",
        "excess_inventory_score",
        "supplier_risk_score",
        "working_capital_risk_score",
        "governance_priority_score",
        "risk_tier",
        "main_risk_driver",
        "recommended_action",
        "lost_sales_revenue",
        "inventory_value_sum",
    ]

    return pd.concat(
        [sku_master[keep_cols], supplier_master[keep_cols], segment_master[keep_cols]],
        ignore_index=True,
    ).sort_values(["governance_priority_score", "lost_sales_revenue"], ascending=[False, False])


def save_scoring_outputs(
    sku_scores: pd.DataFrame,
    supplier_scores: pd.DataFrame,
    segment_scores: pd.DataFrame,
    master_scores: pd.DataFrame,
) -> None:
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    processed_files = {
        "sku_risk_table.csv": sku_scores,
        "supplier_risk_table.csv": supplier_scores,
        "segment_risk_table.csv": segment_scores,
        "governance_priority_master.csv": master_scores,
    }

    table_files = {
        "scoring_sku_risk_table.csv": sku_scores,
        "scoring_supplier_risk_table.csv": supplier_scores,
        "scoring_segment_risk_table.csv": segment_scores,
        "scoring_governance_priority_master.csv": master_scores,
    }

    for file_name, df in processed_files.items():
        df.to_csv(DATA_PROCESSED / file_name, index=False)

    for file_name, df in table_files.items():
        df.to_csv(OUTPUT_TABLES_DIR / file_name, index=False)


def write_management_brief(
    sku_scores: pd.DataFrame,
    supplier_scores: pd.DataFrame,
    segment_scores: pd.DataFrame,
) -> None:
    high_critical_sku = int((sku_scores["risk_tier"].isin(["High", "Critical"])).sum())
    high_critical_supplier = int((supplier_scores["risk_tier"].isin(["High", "Critical"])).sum())
    high_critical_segment = int((segment_scores["risk_tier"].isin(["High", "Critical"])).sum())

    top_sku = sku_scores.iloc[0]
    top_supplier = supplier_scores.iloc[0]
    top_segment = segment_scores.iloc[0]

    lines = [
        "# Scoring Management Brief",
        "",
        "Use the governance scores as a weekly intervention queue, not as a static KPI.",
        "",
        "## How Leadership Should Use the Scores",
        "1. Start with High/Critical entities by governance_priority_score; these are highest expected operational intervention value.",
        "2. Use main_risk_driver to route ownership: service/stockout to planning and DC ops, supplier risk to procurement, excess and working-capital risks to inventory governance and finance.",
        "3. Use recommended_action as the first operational playbook and confirm with planner/supplier context before execution.",
        "4. Track score movement weekly; sustained score reduction is required to close actions.",
        "",
        "## Current Snapshot",
        f"- High/Critical SKU-warehouse combinations: **{high_critical_sku}**.",
        f"- High/Critical suppliers: **{high_critical_supplier}**.",
        f"- High/Critical segments: **{high_critical_segment}**.",
        f"- Top SKU-warehouse priority: **{top_sku['product_id']} @ {top_sku['warehouse_id']}** (score {top_sku['governance_priority_score']:.2f}, driver {top_sku['main_risk_driver']}).",
        f"- Top supplier priority: **{top_supplier['supplier_id']} ({top_supplier['supplier_name']})** (score {top_supplier['governance_priority_score']:.2f}, driver {top_supplier['main_risk_driver']}).",
        f"- Top segment priority: **{top_segment['segment_id']}** (score {top_segment['governance_priority_score']:.2f}, driver {top_segment['main_risk_driver']}).",
    ]

    (OUTPUT_REPORTS_DIR / "scoring_management_brief.md").write_text("\n".join(lines), encoding="utf-8")


def run_scoring() -> None:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    supplier_performance = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")

    supplier_base_scores = score_supplier_base(supplier_performance)
    daily_enriched = prepare_daily_input(daily, supplier_base_scores)

    sku_scores = build_sku_scoring(daily_enriched)
    supplier_scores = build_supplier_scoring(daily_enriched, supplier_base_scores)
    segment_scores = build_segment_scoring(daily_enriched)
    master_scores = build_master_priority_table(sku_scores, supplier_scores, segment_scores)

    save_scoring_outputs(sku_scores, supplier_scores, segment_scores, master_scores)
    write_management_brief(sku_scores, supplier_scores, segment_scores)

    print("Scoring framework execution complete.")
    print(f"SKU rows scored: {len(sku_scores):,}")
    print(f"Supplier rows scored: {len(supplier_scores):,}")
    print(f"Segment rows scored: {len(segment_scores):,}")
    print(f"Master priority rows: {len(master_scores):,}")
    print(f"Processed outputs written to: {DATA_PROCESSED}")
    print(f"Scoring tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_scoring()
