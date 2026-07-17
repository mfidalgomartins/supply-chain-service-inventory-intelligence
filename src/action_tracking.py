"""Action register, score migration, and observational benefit measurement."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
from src.scoring import (
    THRESHOLDS,
    build_sku_scoring,
    build_supplier_scoring,
    prepare_daily_input,
    score_supplier_base,
)
from src.settings import load_settings

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
VALID_STATUSES = {"planned", "in_progress", "implemented", "closed", "cancelled"}
VALID_ENTITY_TYPES = {"sku_warehouse", "supplier"}
MEASURABLE_STATUSES = {"implemented", "closed"}
TARGET_DIRECTIONS = {
    "fill_rate": "higher",
    "stockout_rate": "lower",
    "lost_sales_revenue": "lower",
    "lost_margin_proxy": "lower",
    "average_inventory_value": "lower",
}
TIMESERIES_COLUMNS = [
    "action_id",
    "month",
    "period",
    "observation_days",
    "fill_rate",
    "stockout_rate",
    "lost_sales_revenue",
    "lost_margin_proxy",
    "average_inventory_value",
]


def _format_optional_date(value: pd.Timestamp | None) -> str | None:
    return value.strftime("%Y-%m-%d") if value is not None else None


def _unmeasured_action(
    event: pd.Series,
    measurement_status: str,
    *,
    pre_start: pd.Timestamp | None = None,
    pre_end: pd.Timestamp | None = None,
    post_start: pd.Timestamp | None = None,
    post_end: pd.Timestamp | None = None,
) -> dict:
    return {
        **event.to_dict(),
        "decision_date": event["decision_date"].strftime("%Y-%m-%d"),
        "implementation_date": event["implementation_date"].strftime("%Y-%m-%d"),
        "pre_window_start": _format_optional_date(pre_start),
        "pre_window_end": _format_optional_date(pre_end),
        "post_window_start": _format_optional_date(post_start),
        "post_window_end": _format_optional_date(post_end),
        "pre_priority_score": None,
        "post_priority_score": None,
        "priority_score_improvement": None,
        "pre_risk_tier": None,
        "post_risk_tier": None,
        "pre_fill_rate": None,
        "post_fill_rate": None,
        "pre_stockout_rate": None,
        "post_stockout_rate": None,
        "pre_average_inventory_value": None,
        "post_average_inventory_value": None,
        "observed_lost_margin_recovery_proxy": None,
        "observed_inventory_release_proxy": None,
        "observed_total_benefit_proxy": None,
        "target_achieved": None,
        "measurement_status": measurement_status,
        "attribution_status": "not_measured",
    }


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_action_events(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != 1:
        raise ValueError("Unsupported action event schema version")
    events = pd.DataFrame(payload.get("actions", []))
    if events.empty:
        raise ValueError("Action event file contains no actions")
    required = {
        "action_id",
        "entity_type",
        "supplier_id",
        "action_type",
        "owner",
        "status",
        "decision_date",
        "implementation_date",
        "target_metric",
        "target_value",
    }
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"Action event fields missing: {missing}")
    if events["action_id"].duplicated().any():
        raise ValueError("action_id must be unique")
    if not set(events["status"]).issubset(VALID_STATUSES):
        raise ValueError("Action event status is invalid")
    if not set(events["entity_type"]).issubset(VALID_ENTITY_TYPES):
        raise ValueError("Action event entity_type is invalid")
    events["target_metric"] = events["target_metric"].astype(str)
    unsupported_metrics = set(events["target_metric"]) - set(TARGET_DIRECTIONS)
    if unsupported_metrics:
        raise ValueError(
            "Unsupported action target metrics: " + ", ".join(sorted(unsupported_metrics))
        )
    target_values = pd.to_numeric(events["target_value"], errors="coerce")
    if target_values.isna().any() or not np.isfinite(target_values).all():
        raise ValueError("target_value must be a finite number")
    if (target_values < 0).any():
        raise ValueError("target_value must be non-negative")
    rate_targets = events["target_metric"].isin({"fill_rate", "stockout_rate"})
    if (target_values[rate_targets] > 1).any():
        raise ValueError("Rate target_value must be in [0, 1]")
    events["target_value"] = target_values
    events["decision_date"] = pd.to_datetime(events["decision_date"], errors="raise")
    events["implementation_date"] = pd.to_datetime(events["implementation_date"], errors="raise")
    if events[["decision_date", "implementation_date"]].isna().any().any():
        raise ValueError("Action dates must not be null")
    if (events["decision_date"] > events["implementation_date"]).any():
        raise ValueError("decision_date must not follow implementation_date")
    return events.sort_values("action_id").reset_index(drop=True)


def _filter_entity(daily: pd.DataFrame, event: pd.Series) -> pd.DataFrame:
    if event["entity_type"] == "supplier":
        return daily[daily["supplier_id"] == event["supplier_id"]]
    if pd.isna(event.get("product_id")) or pd.isna(event.get("warehouse_id")):
        raise ValueError(f"{event['action_id']} requires product_id and warehouse_id")
    return daily[
        (daily["product_id"] == event["product_id"])
        & (daily["warehouse_id"] == event["warehouse_id"])
    ]


def _operating_metrics(entity: pd.DataFrame) -> dict[str, float]:
    demanded = float(entity["units_demanded"].sum())
    fulfilled = float(entity["units_fulfilled"].sum())
    dates = max(1, int(entity["date"].nunique()))
    daily_inventory = entity.groupby("date")["inventory_value"].sum()
    return {
        "observation_days": float(dates),
        "fill_rate": fulfilled / demanded if demanded else 1.0,
        "stockout_rate": float(entity["units_lost_sales"].sum()) / demanded if demanded else 0.0,
        "lost_sales_revenue": float(entity["lost_sales_revenue"].sum()),
        "lost_margin_proxy": float(entity["lost_margin_proxy"].sum()),
        "average_inventory_value": float(daily_inventory.mean()),
    }


def _action_timeseries_rows(
    action_scope: pd.DataFrame,
    action_id: str,
    implementation_date: pd.Timestamp,
) -> list[dict]:
    scoped = action_scope.copy()
    scoped["month"] = scoped["date"].dt.to_period("M").astype(str)
    scoped["period"] = np.where(scoped["date"] < implementation_date, "pre", "post")
    rows: list[dict] = []
    for (month, period), month_data in scoped.groupby(["month", "period"]):
        rows.append(
            {
                "action_id": action_id,
                "month": month,
                "period": period,
                **_operating_metrics(month_data),
            }
        )
    return rows


def _window_scores(
    daily_window: pd.DataFrame,
    supplier_base_scores: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    enriched = prepare_daily_input(daily_window, supplier_base_scores)
    sku_scores = build_sku_scoring(enriched)
    supplier_scores = build_supplier_scoring(enriched, supplier_base_scores)
    return sku_scores, supplier_scores


def _supplier_scores_for_window(
    purchase_orders: pd.DataFrame,
    suppliers: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Build supplier evidence only from receipts observed inside the window."""
    observed = purchase_orders[
        (purchase_orders["actual_arrival_date"] >= start)
        & (purchase_orders["actual_arrival_date"] <= end)
    ].copy()
    observed["delay_days"] = (
        observed["actual_arrival_date"] - observed["expected_arrival_date"]
    ).dt.days.clip(lower=0)
    observed["actual_lead_time_days"] = (
        observed["actual_arrival_date"] - observed["order_date"]
    ).dt.days
    observed["on_time"] = (observed["late_delivery_flag"] == 0).astype(float)

    performance = observed.groupby("supplier_id", as_index=False).agg(
        on_time_delivery_rate=("on_time", "mean"),
        average_delay_days=("delay_days", "mean"),
        lead_time_variability=("actual_lead_time_days", "std"),
        ordered_units=("ordered_units", "sum"),
        received_units=("received_units", "sum"),
    )
    performance["received_vs_ordered_fill_rate"] = np.divide(
        performance["received_units"],
        performance["ordered_units"],
        out=np.ones(len(performance), dtype=float),
        where=performance["ordered_units"] > 0,
    )
    performance = suppliers[["supplier_id", "supplier_name"]].merge(
        performance,
        on="supplier_id",
        how="left",
        validate="one_to_one",
    )
    neutral_otd = 1.0 - (THRESHOLDS.supplier_otd_gap_good + THRESHOLDS.supplier_otd_gap_bad) / 2.0
    neutral_delay = (THRESHOLDS.supplier_delay_good + THRESHOLDS.supplier_delay_bad) / 2.0
    neutral_variability = (THRESHOLDS.supplier_lt_var_good + THRESHOLDS.supplier_lt_var_bad) / 2.0
    neutral_fill = (
        1.0 - (THRESHOLDS.supplier_underfill_good + THRESHOLDS.supplier_underfill_bad) / 2.0
    )
    performance["on_time_delivery_rate"] = performance["on_time_delivery_rate"].fillna(neutral_otd)
    performance["average_delay_days"] = performance["average_delay_days"].fillna(neutral_delay)
    performance["lead_time_variability"] = performance["lead_time_variability"].fillna(
        neutral_variability
    )
    performance["received_vs_ordered_fill_rate"] = performance[
        "received_vs_ordered_fill_rate"
    ].fillna(neutral_fill)
    return score_supplier_base(performance)


def _score_for_event(
    event: pd.Series,
    sku_scores: pd.DataFrame,
    supplier_scores: pd.DataFrame,
) -> tuple[float, str]:
    if event["entity_type"] == "supplier":
        match = supplier_scores[supplier_scores["supplier_id"] == event["supplier_id"]]
    else:
        match = sku_scores[
            (sku_scores["product_id"] == event["product_id"])
            & (sku_scores["warehouse_id"] == event["warehouse_id"])
        ]
    if len(match) != 1:
        raise ValueError(f"Action score entity not found or duplicated: {event['action_id']}")
    return float(match.iloc[0]["governance_priority_score"]), str(match.iloc[0]["risk_tier"])


def run_action_tracking(config_path: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    settings = load_settings(config_path).action_tracking
    events = load_action_events(_resolve_path(settings.events_file))
    daily = pd.read_csv(
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"]
    )
    products = pd.read_csv(DATA_RAW / "products.csv")
    suppliers = pd.read_csv(DATA_RAW / "suppliers.csv")
    purchase_orders = pd.read_csv(
        DATA_RAW / "purchase_orders.csv",
        parse_dates=["order_date", "expected_arrival_date", "actual_arrival_date"],
    )
    margins = products[["product_id", "unit_cost", "unit_price"]].copy()
    margins["gross_margin_rate"] = np.where(
        margins["unit_price"] > 0,
        ((margins["unit_price"] - margins["unit_cost"]) / margins["unit_price"]).clip(lower=0),
        0.0,
    )
    daily = daily.merge(
        margins[["product_id", "gross_margin_rate"]],
        on="product_id",
        how="left",
        validate="many_to_one",
    )
    daily["lost_margin_proxy"] = daily["lost_sales_revenue"] * daily["gross_margin_rate"]

    known_products = set(products["product_id"])
    known_warehouses = set(daily["warehouse_id"])
    known_suppliers = set(suppliers["supplier_id"])
    product_suppliers = products.set_index("product_id")["supplier_id"].to_dict()
    result_rows: list[dict] = []
    timeseries_rows: list[dict] = []

    for _, event in events.iterrows():
        if event["supplier_id"] not in known_suppliers:
            raise ValueError(f"Unknown supplier in action {event['action_id']}")
        if event["entity_type"] == "sku_warehouse" and (
            event["product_id"] not in known_products
            or event["warehouse_id"] not in known_warehouses
        ):
            raise ValueError(f"Unknown SKU-location in action {event['action_id']}")
        if (
            event["entity_type"] == "sku_warehouse"
            and product_suppliers[event["product_id"]] != event["supplier_id"]
        ):
            raise ValueError(f"Supplier does not match product in action {event['action_id']}")

        if event["status"] not in MEASURABLE_STATUSES:
            result_rows.append(_unmeasured_action(event, "not_eligible_status"))
            continue

        implementation_date = event["implementation_date"]
        window = settings.measurement_window_days
        pre_start = implementation_date - pd.Timedelta(days=window)
        pre_end = implementation_date - pd.Timedelta(days=1)
        post_start = implementation_date
        post_end = implementation_date + pd.Timedelta(days=window - 1)
        pre_company = daily[(daily["date"] >= pre_start) & (daily["date"] <= pre_end)]
        post_company = daily[(daily["date"] >= post_start) & (daily["date"] <= post_end)]
        if pre_company["date"].nunique() != window or post_company["date"].nunique() != window:
            result_rows.append(
                _unmeasured_action(
                    event,
                    "measurement_pending",
                    pre_start=pre_start,
                    pre_end=pre_end,
                    post_start=post_start,
                    post_end=post_end,
                )
            )
            continue

        pre_entity = _filter_entity(pre_company, event)
        post_entity = _filter_entity(post_company, event)
        if pre_entity.empty or post_entity.empty:
            raise ValueError(f"Action entity has no measurement data: {event['action_id']}")
        pre_metrics = _operating_metrics(pre_entity)
        post_metrics = _operating_metrics(post_entity)
        pre_supplier_base = _supplier_scores_for_window(
            purchase_orders, suppliers, pre_start, pre_end
        )
        post_supplier_base = _supplier_scores_for_window(
            purchase_orders, suppliers, post_start, post_end
        )
        pre_sku_scores, pre_supplier_scores = _window_scores(pre_company, pre_supplier_base)
        post_sku_scores, post_supplier_scores = _window_scores(post_company, post_supplier_base)
        pre_score, pre_tier = _score_for_event(event, pre_sku_scores, pre_supplier_scores)
        post_score, post_tier = _score_for_event(event, post_sku_scores, post_supplier_scores)

        expected_post_lost_margin = (
            pre_metrics["lost_margin_proxy"]
            / pre_metrics["observation_days"]
            * post_metrics["observation_days"]
        )
        lost_margin_recovery = expected_post_lost_margin - post_metrics["lost_margin_proxy"]
        inventory_release = (
            pre_metrics["average_inventory_value"] - post_metrics["average_inventory_value"]
        )
        realized_benefit = lost_margin_recovery + inventory_release
        target_value = float(event["target_value"])
        target_metric = str(event["target_metric"])
        target_achieved = (
            post_metrics[target_metric] >= target_value
            if TARGET_DIRECTIONS[target_metric] == "higher"
            else post_metrics[target_metric] <= target_value
        )

        result_rows.append(
            {
                **event.to_dict(),
                "decision_date": event["decision_date"].strftime("%Y-%m-%d"),
                "implementation_date": implementation_date.strftime("%Y-%m-%d"),
                "pre_window_start": pre_start.strftime("%Y-%m-%d"),
                "pre_window_end": pre_end.strftime("%Y-%m-%d"),
                "post_window_start": post_start.strftime("%Y-%m-%d"),
                "post_window_end": post_end.strftime("%Y-%m-%d"),
                "pre_priority_score": pre_score,
                "post_priority_score": post_score,
                "priority_score_improvement": pre_score - post_score,
                "pre_risk_tier": pre_tier,
                "post_risk_tier": post_tier,
                "pre_fill_rate": pre_metrics["fill_rate"],
                "post_fill_rate": post_metrics["fill_rate"],
                "pre_stockout_rate": pre_metrics["stockout_rate"],
                "post_stockout_rate": post_metrics["stockout_rate"],
                "pre_average_inventory_value": pre_metrics["average_inventory_value"],
                "post_average_inventory_value": post_metrics["average_inventory_value"],
                "observed_lost_margin_recovery_proxy": lost_margin_recovery,
                "observed_inventory_release_proxy": inventory_release,
                "observed_total_benefit_proxy": realized_benefit,
                "target_achieved": bool(target_achieved),
                "measurement_status": "observed_pre_post_complete",
                "attribution_status": "observational_not_causal",
            }
        )

        action_scope = _filter_entity(
            daily[(daily["date"] >= pre_start) & (daily["date"] <= post_end)],
            event,
        ).copy()
        timeseries_rows.extend(
            _action_timeseries_rows(action_scope, event["action_id"], implementation_date)
        )

    action_register = pd.DataFrame(result_rows).sort_values("action_id")
    timeseries = pd.DataFrame(timeseries_rows, columns=TIMESERIES_COLUMNS).sort_values(
        ["action_id", "month", "period"]
    )
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    action_register.to_csv(OUTPUT_TABLES_DIR / "action_register.csv", index=False)
    timeseries.to_csv(OUTPUT_TABLES_DIR / "action_kpi_timeseries.csv", index=False)
    achieved_count = int(action_register["target_achieved"].dropna().astype(bool).sum())
    print(
        f"Action tracking complete. Actions: {len(action_register)}; "
        f"targets achieved: {achieved_count}"
    )
    return action_register, timeseries


def main() -> None:
    run_action_tracking()


if __name__ == "__main__":
    main()
