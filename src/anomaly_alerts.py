from __future__ import annotations

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


def _warehouse_alerts(daily: pd.DataFrame) -> pd.DataFrame:
    warehouse_daily = (
        daily.groupby(["date", "warehouse_id", "region"], as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_lost_sales=("units_lost_sales", "sum"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
        )
        .sort_values(["warehouse_id", "date"])
    )
    warehouse_daily["stockout_rate"] = warehouse_daily["units_lost_sales"] / warehouse_daily["units_demanded"].clip(lower=1)

    alerts: list[pd.DataFrame] = []

    for metric in ["stockout_rate", "lost_sales_revenue"]:
        frame = warehouse_daily.copy()
        grp = frame.groupby("warehouse_id")[metric]

        frame["baseline_mean"] = grp.transform(lambda s: s.shift(1).rolling(28, min_periods=14).mean())
        frame["baseline_std"] = grp.transform(lambda s: s.shift(1).rolling(28, min_periods=14).std())
        frame["baseline_std"] = frame["baseline_std"].fillna(0.0).clip(lower=1e-6)
        frame["z_score"] = (frame[metric] - frame["baseline_mean"]) / frame["baseline_std"]
        frame["z_score"] = frame["z_score"].replace([np.inf, -np.inf], np.nan)

        metric_alert = frame[(frame["z_score"] >= 2.5) & frame["baseline_mean"].notna()].copy()
        metric_alert["entity_type"] = "warehouse"
        metric_alert["entity_id"] = metric_alert["warehouse_id"]
        metric_alert["metric_name"] = metric
        alerts.append(metric_alert)

    if not alerts:
        return pd.DataFrame()

    out = pd.concat(alerts, ignore_index=True)
    out["severity"] = np.select(
        [out["z_score"] >= 4.0, out["z_score"] >= 3.0, out["z_score"] >= 2.5],
        ["Critical", "High", "Medium"],
        default="Low",
    )
    out["diagnosis"] = np.where(
        out["metric_name"] == "stockout_rate",
        "Warehouse stockout spike vs 4-week baseline",
        "Warehouse lost-sales spike vs 4-week baseline",
    )
    out["recommended_action"] = np.where(
        out["metric_name"] == "stockout_rate",
        "expedite replenishment and rebalance stock across warehouses",
        "review demand shock and recover priority SKUs",
    )

    out = out.rename(columns={"date": "alert_date", "warehouse_id": "source_warehouse_id"})
    out["metric_value"] = out.apply(
        lambda r: r["stockout_rate"] if r["metric_name"] == "stockout_rate" else r["lost_sales_revenue"],
        axis=1,
    )
    keep = [
        "alert_date",
        "entity_type",
        "entity_id",
        "region",
        "metric_name",
        "metric_value",
        "baseline_mean",
        "baseline_std",
        "z_score",
        "severity",
        "diagnosis",
        "recommended_action",
    ]
    return out[keep]


def _supplier_alerts(po: pd.DataFrame) -> pd.DataFrame:
    po = po.copy()
    po["order_date"] = pd.to_datetime(po["order_date"])
    po["expected_arrival_date"] = pd.to_datetime(po["expected_arrival_date"])
    po["actual_arrival_date"] = pd.to_datetime(po["actual_arrival_date"])
    po["delay_days"] = (po["actual_arrival_date"] - po["expected_arrival_date"]).dt.days.clip(lower=0)

    weekly = (
        po.assign(week=po["order_date"].dt.to_period("W-MON").dt.start_time)
        .groupby(["week", "supplier_id"], as_index=False)
        .agg(
            po_count=("po_id", "count"),
            late_rate=("late_delivery_flag", "mean"),
            avg_delay_days=("delay_days", "mean"),
        )
        .sort_values(["supplier_id", "week"])
    )

    alerts: list[pd.DataFrame] = []
    for metric in ["late_rate", "avg_delay_days"]:
        frame = weekly.copy()
        grp = frame.groupby("supplier_id")[metric]

        frame["baseline_mean"] = grp.transform(lambda s: s.shift(1).rolling(8, min_periods=4).mean())
        frame["baseline_std"] = grp.transform(lambda s: s.shift(1).rolling(8, min_periods=4).std())
        frame["baseline_std"] = frame["baseline_std"].fillna(0.0).clip(lower=1e-6)
        frame["z_score"] = (frame[metric] - frame["baseline_mean"]) / frame["baseline_std"]

        metric_alert = frame[(frame["z_score"] >= 2.0) & frame["baseline_mean"].notna()].copy()
        metric_alert["entity_type"] = "supplier"
        metric_alert["entity_id"] = metric_alert["supplier_id"]
        metric_alert["metric_name"] = metric
        alerts.append(metric_alert)

    if not alerts:
        return pd.DataFrame()

    out = pd.concat(alerts, ignore_index=True)
    out["severity"] = np.select(
        [out["z_score"] >= 4.0, out["z_score"] >= 3.0, out["z_score"] >= 2.0],
        ["Critical", "High", "Medium"],
        default="Low",
    )
    out["diagnosis"] = np.where(
        out["metric_name"] == "late_rate",
        "Supplier late-delivery spike vs 8-week baseline",
        "Supplier delay-duration spike vs 8-week baseline",
    )
    out["recommended_action"] = "investigate supplier reliability and trigger corrective plan"

    out = out.rename(columns={"week": "alert_date"})
    out["region"] = "Global"
    out["metric_value"] = np.where(out["metric_name"] == "late_rate", out["late_rate"], out["avg_delay_days"])

    keep = [
        "alert_date",
        "entity_type",
        "entity_id",
        "region",
        "metric_name",
        "metric_value",
        "baseline_mean",
        "baseline_std",
        "z_score",
        "severity",
        "diagnosis",
        "recommended_action",
    ]
    return out[keep]


def _build_alerts() -> pd.DataFrame:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    po = pd.read_csv(DATA_RAW / "purchase_orders.csv")

    wh_alerts = _warehouse_alerts(daily)
    sup_alerts = _supplier_alerts(po)

    alerts = pd.concat([wh_alerts, sup_alerts], ignore_index=True)
    if alerts.empty:
        return alerts

    alerts = alerts.sort_values(["alert_date", "z_score"], ascending=[False, False]).reset_index(drop=True)
    alerts["alert_id"] = "ALT-" + pd.Series(np.arange(1, len(alerts) + 1), index=alerts.index).astype(str).str.zfill(5)

    cols = [
        "alert_id",
        "alert_date",
        "entity_type",
        "entity_id",
        "region",
        "metric_name",
        "metric_value",
        "baseline_mean",
        "baseline_std",
        "z_score",
        "severity",
        "diagnosis",
        "recommended_action",
    ]
    return alerts[cols]


def _plot(alerts: pd.DataFrame) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    plot = alerts.copy()
    plot["alert_week"] = pd.to_datetime(plot["alert_date"]).dt.to_period("W-MON").dt.start_time
    weekly = (
        plot.groupby(["alert_week", "severity"], as_index=False)
        .agg(alert_count=("alert_id", "count"))
        .sort_values("alert_week")
    )

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=weekly, x="alert_week", y="alert_count", hue="severity", marker="o")
    plt.title("Operational Alert Timeline: Spikes in Service and Supplier Stability")
    plt.xlabel("Week")
    plt.ylabel("Alert Count")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "anomaly_alert_timeline.png", dpi=180)
    plt.close()


def _write_summary(alerts: pd.DataFrame, summary: pd.DataFrame) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    if alerts.empty:
        text = "\n".join(
            [
                "# Anomaly Alerts Summary",
                "",
                "No significant anomalies were detected under the configured thresholds.",
            ]
        )
    else:
        top = alerts.iloc[0]
        sev = summary.groupby("severity", as_index=False).agg(alert_count=("alert_count", "sum")).sort_values("alert_count", ascending=False)
        top_sev = sev.iloc[0]

        text = "\n".join(
            [
                "# Anomaly Alerts Summary",
                "",
                "This layer detects sudden operational spikes in warehouse service failure and supplier execution instability.",
                "",
                "## Current Alert Book",
                f"- Total alerts: **{len(alerts):,}**",
                f"- Most frequent severity: **{top_sev['severity']}** ({int(top_sev['alert_count'])} alerts)",
                "",
                "## Highest-Z Alert",
                f"- {top['entity_type']} `{top['entity_id']}` | metric `{top['metric_name']}`",
                f"- z-score: **{top['z_score']:.2f}**, severity: **{top['severity']}**",
                f"- diagnosis: {top['diagnosis']}",
                "",
                "## Governance Use",
                "- Route Critical/High alerts to daily operations stand-up.",
                "- Link repeated alerts to intervention ownership and closure evidence.",
            ]
        )

    (OUTPUT_REPORTS_DIR / "anomaly_alerts_summary.md").write_text(text, encoding="utf-8")


def run_anomaly_alerts() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    alerts = _build_alerts()
    if alerts.empty:
        summary = pd.DataFrame(columns=["entity_type", "entity_id", "severity", "alert_count", "max_z_score"])
    else:
        summary = (
            alerts.groupby(["entity_type", "entity_id", "severity"], as_index=False)
            .agg(alert_count=("alert_id", "count"), max_z_score=("z_score", "max"))
            .sort_values(["alert_count", "max_z_score"], ascending=[False, False])
        )

    alerts.to_csv(OUTPUT_TABLES_DIR / "anomaly_alerts.csv", index=False)
    summary.to_csv(OUTPUT_TABLES_DIR / "anomaly_alerts_summary.csv", index=False)

    if not alerts.empty:
        _plot(alerts)

    _write_summary(alerts, summary)

    print("Anomaly alerts complete.")
    print(f"Alert rows: {len(alerts):,}")
    print(f"Summary rows: {len(summary):,}")
    print(f"Tables written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_anomaly_alerts()
