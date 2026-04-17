from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from plotly.offline.offline import get_plotlyjs

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


OUTPUT_DASHBOARD_FILE = PROJECT_ROOT / "index.html"
DOCS_DASHBOARD_ENTRY = PROJECT_ROOT / "docs" / "index.html"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


def _sha256_for_file(path: Path) -> str:
    payload = path.read_bytes()
    return hashlib.sha256(payload).hexdigest()


def _prepare_dashboard_data() -> dict:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    products = pd.read_csv(DATA_RAW / "products.csv")[["product_id", "product_name", "unit_cost", "unit_price"]]
    suppliers = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")
    warehouses = pd.read_csv(DATA_RAW / "warehouses.csv")[["warehouse_id", "warehouse_name", "region"]]
    sku_risk = pd.read_csv(DATA_PROCESSED / "sku_risk_table.csv")

    daily = daily.copy()
    pricing = products[["product_id", "unit_cost", "unit_price"]].copy()
    pricing["gross_margin_rate"] = np.where(
        pricing["unit_price"] > 0,
        (pricing["unit_price"] - pricing["unit_cost"]) / pricing["unit_price"],
        0.0,
    )
    pricing["gross_margin_rate"] = pricing["gross_margin_rate"].clip(0, 0.90)

    daily = daily.merge(pricing[["product_id", "gross_margin_rate"]], on="product_id", how="left")
    daily["gross_margin_rate"] = daily["gross_margin_rate"].fillna(0.30)
    daily["month"] = daily["date"].dt.to_period("M").dt.to_timestamp()
    daily["dos_cap"] = np.select(
        [daily["abc_class"] == "A", daily["abc_class"] == "B"],
        [20.0, 30.0],
        default=45.0,
    )
    daily["excess_inventory_proxy"] = (
        daily["inventory_value"] * ((daily["days_of_supply"] - daily["dos_cap"]).clip(lower=0) / daily["days_of_supply"].clip(lower=1e-9))
    )
    daily["slow_moving_proxy"] = np.where(
        (daily["available_units"] > 0) & (daily["units_fulfilled"] == 0),
        daily["inventory_value"],
        0.0,
    )
    daily["trapped_wc_proxy"] = daily["excess_inventory_proxy"] + 0.5 * (daily["slow_moving_proxy"] - daily["excess_inventory_proxy"]).clip(
        lower=0
    )
    daily["lost_sales_margin_proxy"] = daily["lost_sales_revenue"] * daily["gross_margin_rate"]

    monthly_sku = (
        daily.groupby(
            ["month", "region", "warehouse_id", "product_id", "category", "supplier_id", "abc_class"],
            as_index=False,
        )
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_fulfilled=("units_fulfilled", "sum"),
            units_lost_sales=("units_lost_sales", "sum"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
            inventory_value=("inventory_value", "sum"),
            avg_days_of_supply=("days_of_supply", "mean"),
            excess_inventory_proxy=("excess_inventory_proxy", "sum"),
            slow_moving_proxy=("slow_moving_proxy", "sum"),
            trapped_wc_proxy=("trapped_wc_proxy", "sum"),
            lost_sales_margin_proxy=("lost_sales_margin_proxy", "sum"),
            observation_days=("date", "nunique"),
        )
    )

    monthly_sku["stockout_month_flag"] = (monthly_sku["units_lost_sales"] > 0).astype(int)
    monthly_sku["month"] = monthly_sku["month"].dt.strftime("%Y-%m-01")

    # Compact float precision to keep HTML size practical.
    float_cols = [
        "lost_sales_revenue",
        "inventory_value",
        "avg_days_of_supply",
        "excess_inventory_proxy",
        "slow_moving_proxy",
        "trapped_wc_proxy",
        "lost_sales_margin_proxy",
    ]
    monthly_sku[float_cols] = monthly_sku[float_cols].round(2)

    sku_baseline = (
        sku_risk[
            [
                "product_id",
                "warehouse_id",
                "supplier_id",
                "service_risk_score",
                "stockout_risk_score",
                "excess_inventory_score",
                "supplier_risk_score",
                "working_capital_risk_score",
                "governance_priority_score",
                "risk_tier",
                "main_risk_driver",
                "recommended_action",
            ]
        ]
        .copy()
        .round(4)
    )

    overall_kpi_path = PROJECT_ROOT / "outputs" / "reports" / "kpi_overall_service_health.csv"
    impact_overall_path = OUTPUT_TABLES_DIR / "impact_overall_summary.csv"
    if overall_kpi_path.exists():
        overall_kpi = pd.read_csv(overall_kpi_path).iloc[0].to_dict()
    else:
        total_demand = float(daily["units_demanded"].sum())
        total_fulfilled = float(daily["units_fulfilled"].sum())
        total_lost = float(daily["units_lost_sales"].sum())
        overall_kpi = {
            "overall_fill_rate": total_fulfilled / total_demand if total_demand > 0 else 1.0,
            "overall_stockout_rate": total_lost / total_demand if total_demand > 0 else 0.0,
            "total_lost_sales_revenue": float(daily["lost_sales_revenue"].sum()),
        }

    if impact_overall_path.exists():
        impact_map = dict(zip(pd.read_csv(impact_overall_path)["metric"], pd.read_csv(impact_overall_path)["value"]))
    else:
        impact_map = {}

    snapshot = {
        "overall_fill_rate": float(overall_kpi.get("overall_fill_rate", 0.0)),
        "overall_stockout_rate": float(overall_kpi.get("overall_stockout_rate", 0.0)),
        "total_lost_sales_revenue": float(overall_kpi.get("total_lost_sales_revenue", 0.0)),
        "trapped_working_capital_proxy_observed": float(impact_map.get("trapped_working_capital_proxy_observed", 0.0)),
        "opportunity_total_12m_proxy": float(impact_map.get("opportunity_total_12m_proxy", 0.0)),
    }

    product_dim = products[["product_id", "product_name"]].copy()
    product_dim["product_name"] = product_dim["product_name"].fillna(product_dim["product_id"])

    hash_seed = (
        monthly_sku.sort_values(["month", "warehouse_id", "product_id"])[["month", "warehouse_id", "product_id", "units_demanded", "units_fulfilled", "lost_sales_revenue"]]
        .to_csv(index=False)
        .encode("utf-8")
        + sku_baseline.sort_values(["product_id", "warehouse_id", "supplier_id"])[["product_id", "warehouse_id", "supplier_id", "governance_priority_score"]]
        .to_csv(index=False)
        .encode("utf-8")
    )
    dataset_hash = hashlib.sha256(hash_seed).hexdigest()
    dashboard_version = f"v{datetime.now(timezone.utc):%Y.%m.%d}.{dataset_hash[:8]}"

    monthly_compact_columns = [
        "month",
        "region",
        "warehouse_id",
        "product_id",
        "category",
        "supplier_id",
        "abc_class",
        "units_demanded",
        "units_fulfilled",
        "units_lost_sales",
        "lost_sales_revenue",
        "inventory_value",
        "avg_days_of_supply",
        "excess_inventory_proxy",
        "slow_moving_proxy",
        "trapped_wc_proxy",
        "lost_sales_margin_proxy",
        "observation_days",
        "stockout_month_flag",
    ]
    dim_values = {
        "month": sorted(monthly_sku["month"].unique().tolist()),
        "region": sorted(monthly_sku["region"].unique().tolist()),
        "warehouse_id": sorted(monthly_sku["warehouse_id"].unique().tolist()),
        "product_id": sorted(monthly_sku["product_id"].unique().tolist()),
        "category": sorted(monthly_sku["category"].unique().tolist()),
        "supplier_id": sorted(monthly_sku["supplier_id"].unique().tolist()),
        "abc_class": sorted(monthly_sku["abc_class"].unique().tolist()),
    }
    dim_lookup = {k: {v: i for i, v in enumerate(vals)} for k, vals in dim_values.items()}
    monthly_rows_compact: list[list] = []
    for r in monthly_sku[monthly_compact_columns].itertuples(index=False):
        monthly_rows_compact.append(
            [
                dim_lookup["month"][r.month],
                dim_lookup["region"][r.region],
                dim_lookup["warehouse_id"][r.warehouse_id],
                dim_lookup["product_id"][r.product_id],
                dim_lookup["category"][r.category],
                dim_lookup["supplier_id"][r.supplier_id],
                dim_lookup["abc_class"][r.abc_class],
                int(r.units_demanded),
                int(r.units_fulfilled),
                int(r.units_lost_sales),
                float(r.lost_sales_revenue),
                float(r.inventory_value),
                float(r.avg_days_of_supply),
                float(r.excess_inventory_proxy),
                float(r.slow_moving_proxy),
                float(r.trapped_wc_proxy),
                float(r.lost_sales_margin_proxy),
                int(r.observation_days),
                int(r.stockout_month_flag),
            ]
        )

    product_name_map = dict(product_dim[["product_id", "product_name"]].itertuples(index=False, name=None))

    data_payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "dashboard_version": dashboard_version,
        "monthly_sku_compact": {
            "columns": monthly_compact_columns,
            "rows": monthly_rows_compact,
            "dim": dim_values,
        },
        "product_name_map": product_name_map,
        "suppliers": suppliers.round(4).to_dict(orient="records"),
        "warehouses": warehouses.to_dict(orient="records"),
        "sku_risk_baseline": sku_baseline.to_dict(orient="records"),
        "meta": {
            "date_min": monthly_sku["month"].min(),
            "date_max": monthly_sku["month"].max(),
            "row_count_monthly_sku": int(len(monthly_sku)),
            "dataset_hash": dataset_hash,
            "official_snapshot": snapshot,
            "assumptions_default": {
                "recoverable_margin_rate": 0.35,
                "releasable_wc_rate": 0.25,
                "slow_moving_incremental_weight": 0.50,
            },
        },
    }

    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    monthly_sku.to_csv(OUTPUT_TABLES_DIR / "dashboard_monthly_sku_fact.csv", index=False)
    suppliers.to_csv(OUTPUT_TABLES_DIR / "dashboard_supplier_dim.csv", index=False)
    warehouses.to_csv(OUTPUT_TABLES_DIR / "dashboard_warehouse_dim.csv", index=False)
    sku_baseline.to_csv(OUTPUT_TABLES_DIR / "dashboard_sku_risk_baseline.csv", index=False)
    pd.DataFrame([snapshot]).to_csv(OUTPUT_TABLES_DIR / "dashboard_official_snapshot.csv", index=False)

    dashboard_manifest = pd.DataFrame(
        [
            {
                "dashboard_version": dashboard_version,
                "dataset_hash": dataset_hash,
                "generated_at_utc": data_payload["generated_at"],
                "monthly_rows": len(monthly_sku),
                "sku_baseline_rows": len(sku_baseline),
            }
        ]
    )
    dashboard_manifest.to_csv(OUTPUT_TABLES_DIR / "dashboard_build_manifest.csv", index=False)

    return data_payload


def _build_html(data_payload: dict) -> str:
    plotly_js = get_plotlyjs()
    data_json = json.dumps(data_payload, ensure_ascii=False, separators=(",", ":"))

    template = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System</title>
  <style>
    :root {
      --bg: #edf1f4;
      --bg-grad-a: #d7e3ec;
      --bg-grad-b: #ece0d1;
      --bg-grad-c: #e8edf1;
      --panel: rgba(255,255,255,0.84);
      --panel-alt: rgba(255,255,255,0.9);
      --panel-soft: rgba(246,249,251,0.88);
      --narrative-bg: rgba(248,250,252,0.96);
      --ink: #122531;
      --muted: #617381;
      --accent: #0d617d;
      --accent-soft: #e4edf3;
      --title-ink: #10222d;
      --section-title-ink: #143142;
      --kpi-value-ink: #163649;
      --callout-ink: #203948;
      --border-strong-soft: #d2e0ea;
      --danger: #b53a33;
      --warn: #9b6a12;
      --ok: #1f6b44;
      --border: rgba(196,211,221,0.82);
      --border-soft: rgba(219,229,235,0.9);
      --input-bg: rgba(255,255,255,0.88);
      --table-head-bg: #eef3f7;
      --table-row-hover: #f5f8fb;
      --alert-bg: #fff5f5;
      --alert-border: #f0c5c5;
      --alert-ink: #7e1f1f;
      --kpi-inset-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
      --risk-low-bg: #d8f0df;
      --risk-low-ink: #1d633d;
      --risk-medium-bg: #f7eac8;
      --risk-medium-ink: #8c5f00;
      --risk-high-bg: #ffd8b8;
      --risk-high-ink: #9b4a00;
      --risk-critical-bg: #ffd5d5;
      --risk-critical-ink: #8b1717;
      --shadow-sm: 0 14px 38px rgba(17, 37, 49, 0.07);
      --shadow-md: 0 24px 64px rgba(17, 37, 49, 0.11);
      --radius-lg: 22px;
      --radius-md: 18px;
      --radius-sm: 14px;
    }

    [data-theme="dark"] {
      --bg: #0d1720;
      --bg-grad-a: #22384a;
      --bg-grad-b: #47382f;
      --bg-grad-c: #14222d;
      --panel: rgba(19,35,49,0.84);
      --panel-alt: rgba(23,41,56,0.9);
      --panel-soft: rgba(28,47,62,0.9);
      --narrative-bg: rgba(24,42,56,0.96);
      --ink: #dde8f1;
      --muted: #99afbf;
      --accent: #4aa4c7;
      --accent-soft: #26455b;
      --title-ink: #f2f7fb;
      --section-title-ink: #dceaf6;
      --kpi-value-ink: #f3f8fc;
      --callout-ink: #dbe7f1;
      --border-strong-soft: #36556c;
      --danger: #e87979;
      --warn: #f1c07d;
      --ok: #5db68e;
      --border: rgba(49,75,94,0.88);
      --border-soft: rgba(55,84,105,0.92);
      --input-bg: rgba(14,27,38,0.92);
      --table-head-bg: #21384a;
      --table-row-hover: #1b3142;
      --alert-bg: #3c1e26;
      --alert-border: #73414e;
      --alert-ink: #ffd8df;
      --kpi-inset-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
      --risk-low-bg: #1f4a35;
      --risk-low-ink: #b8efd2;
      --risk-medium-bg: #4c3d1f;
      --risk-medium-ink: #f6db9d;
      --risk-high-bg: #563520;
      --risk-high-ink: #ffd2ae;
      --risk-critical-bg: #552a33;
      --risk-critical-ink: #ffc6d1;
      --shadow-sm: 0 18px 44px rgba(0, 0, 0, 0.34);
      --shadow-md: 0 28px 76px rgba(0, 0, 0, 0.44);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Avenir Next", "Source Sans 3", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 420px at 8% -10%, var(--bg-grad-a) 0%, transparent 70%),
        radial-gradient(980px 320px at 92% -18%, var(--bg-grad-b) 0%, transparent 68%),
        linear-gradient(180deg, var(--bg-grad-c) 0%, var(--bg) 100%);
      transition: background 180ms ease, color 180ms ease;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }

    .container {
      max-width: 1720px;
      margin: 0 auto;
      padding: 18px 22px 28px;
    }

    .header {
      background: var(--panel);
      border: 1px solid var(--border);
      box-shadow: var(--shadow-md);
      border-radius: var(--radius-lg);
      padding: 18px 18px 16px;
      margin-bottom: 16px;
      position: relative;
      overflow: hidden;
      backdrop-filter: blur(14px);
    }

    .header::before,
    .section::before {
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      height: 1px;
      background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.95) 24%, transparent 100%);
      pointer-events: none;
    }

    .header-top {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(300px, 0.92fr);
      gap: 14px;
      align-items: start;
    }

    .title {
      margin: 0;
      font-size: clamp(1.28rem, 1.9vw, 2.05rem);
      letter-spacing: 0.08px;
      line-height: 1.1;
      color: var(--title-ink);
      font-weight: 780;
    }

    .subtitle {
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.91rem;
      line-height: 1.48;
      max-width: 760px;
    }

    .header-copy {
      min-width: 0;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .header-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
      margin-bottom: 10px;
    }

    .meta-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--border-soft);
      background: var(--panel-soft);
      color: var(--muted);
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: 0.38px;
      text-transform: uppercase;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.64);
    }

    .hero-panel {
      border: 1px solid var(--border-soft);
      border-radius: 20px;
      background:
        radial-gradient(120% 140% at 100% 0%, rgba(13,97,125,0.12) 0%, transparent 56%),
        linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(246,249,252,0.9) 100%);
      padding: 15px 15px 14px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.78), 0 18px 40px rgba(20, 44, 58, 0.08);
      display: grid;
      gap: 10px;
      position: relative;
      overflow: hidden;
    }

    [data-theme="dark"] .hero-panel {
      background:
        radial-gradient(120% 140% at 100% 0%, rgba(74,164,199,0.13) 0%, transparent 54%),
        linear-gradient(180deg, rgba(24,42,56,0.98) 0%, rgba(17,33,46,0.98) 100%);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 18px 42px rgba(0,0,0,0.28);
    }

    .hero-eyebrow {
      color: var(--muted);
      font-size: 0.68rem;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      font-weight: 700;
    }

    .hero-headline {
      font-size: 1.02rem;
      line-height: 1.28;
      font-weight: 760;
      color: var(--title-ink);
      margin-top: -2px;
      max-width: 30ch;
    }

    .hero-summary {
      color: var(--muted);
      font-size: 0.8rem;
      line-height: 1.42;
    }

    .hero-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }

    .hero-card {
      border: 1px solid var(--border-soft);
      border-radius: 14px;
      padding: 10px 11px 11px;
      background: linear-gradient(180deg, rgba(255,255,255,0.9) 0%, rgba(250,252,253,0.75) 100%);
      display: grid;
      gap: 6px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.72);
    }

    [data-theme="dark"] .hero-card {
      background: linear-gradient(180deg, rgba(18,33,46,0.82) 0%, rgba(14,27,38,0.72) 100%);
    }

    .hero-label {
      color: var(--muted);
      font-size: 0.64rem;
      text-transform: uppercase;
      letter-spacing: 0.48px;
      font-weight: 700;
    }

    .hero-value {
      color: var(--kpi-value-ink);
      font-size: 0.92rem;
      line-height: 1.2;
      font-weight: 760;
    }

    .hero-detail {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.34;
    }

    .header-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-top: 12px;
      margin-bottom: 10px;
    }

    .toolbar-button {
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 8px 12px;
      cursor: pointer;
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.22px;
      color: var(--ink);
      background: rgba(255,255,255,0.58);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.72);
      transition: transform 140ms ease, box-shadow 180ms ease, border-color 180ms ease, background 180ms ease, color 180ms ease;
    }

    .toolbar-button:hover {
      transform: translateY(-1px);
      border-color: var(--accent);
    }

    .toolbar-button-accent {
      color: #fff;
      border-color: transparent;
      background: linear-gradient(180deg, color-mix(in srgb, var(--accent) 88%, white 12%) 0%, var(--accent) 100%);
      box-shadow: 0 10px 20px rgba(13, 97, 125, 0.14), inset 0 1px 0 rgba(255,255,255,0.2);
    }

    .toolbar-button-ok {
      color: #fff;
      border-color: transparent;
      background: linear-gradient(180deg, color-mix(in srgb, var(--ok) 86%, white 14%) 0%, var(--ok) 100%);
      box-shadow: 0 10px 20px rgba(31, 107, 68, 0.13), inset 0 1px 0 rgba(255,255,255,0.18);
    }

    .toolbar-button-ghost {
      background: var(--panel-soft);
    }

    .toolbar-note {
      margin-left: auto;
      color: var(--muted);
      font-size: 0.74rem;
      line-height: 1.35;
    }

    .consistency-alert {
      display: none;
      margin-top: 10px;
      padding: 9px 11px;
      border-radius: 9px;
      border: 1px solid var(--alert-border);
      background: var(--alert-bg);
      color: var(--alert-ink);
      font-size: 0.79rem;
      line-height: 1.4;
    }

    .no-data-alert {
      display: none;
      margin-top: 10px;
      padding: 9px 11px;
      border-radius: 9px;
      border: 1px solid #e8d7a8;
      background: #fff9e9;
      color: #6d4f00;
      font-size: 0.79rem;
      line-height: 1.4;
    }

    [data-theme="dark"] .no-data-alert {
      border-color: #665327;
      background: #3b341f;
      color: #f5dda0;
    }

    .filters {
      margin-top: 0;
      display: grid;
      grid-template-columns: repeat(7, minmax(128px, 1fr));
      gap: 10px;
      align-items: end;
      border: 1px solid var(--border-soft);
      border-radius: 18px;
      background: var(--panel-soft);
      padding: 12px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.72);
    }

    .filter-box label {
      display: block;
      margin-bottom: 5px;
      color: var(--muted);
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.55px;
      font-weight: 600;
    }

    .filter-box select,
    .filter-box input {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 11px;
      padding: 10px 12px;
      background: var(--input-bg);
      color: var(--ink);
      font-size: 0.85rem;
      min-height: 42px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.65);
    }

    .filter-box select:focus-visible,
    .filter-box input:focus-visible,
    .table-controls input:focus-visible,
    button:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
      border-color: var(--accent);
    }

    .toolbar-button,
    .kpi,
    .callout,
    .chart-card {
      transition: transform 140ms ease, box-shadow 180ms ease, border-color 180ms ease, background 180ms ease;
    }

    .kpi:hover,
    .callout:hover,
    .chart-card:hover {
      transform: translateY(-1px);
    }

    .methodology-panel {
      margin-top: 10px;
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-sm);
      background: var(--narrative-bg);
      padding: 11px 12px;
      display: none;
      color: var(--ink);
      font-size: 0.83rem;
      line-height: 1.48;
    }

    .assumption-panel {
      margin-top: 10px;
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-sm);
      background: var(--panel-soft);
      padding: 12px 14px 12px;
      display: none;
    }

    .assumption-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(180px, 1fr));
      gap: 12px;
    }

    .assumption-box label {
      display: block;
      margin-bottom: 4px;
      color: var(--muted);
      font-size: 0.73rem;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      font-weight: 600;
    }

    .assumption-box input[type="range"] {
      width: 100%;
      accent-color: var(--accent);
    }

    .assumption-value {
      margin-top: 4px;
      color: var(--kpi-value-ink);
      font-weight: 700;
      font-size: 0.79rem;
    }

    .section {
      margin-bottom: 16px;
      background: var(--panel);
      border: 1px solid var(--border);
      box-shadow: var(--shadow-sm);
      border-radius: var(--radius-md);
      padding: 18px 18px 16px;
      position: relative;
      overflow: hidden;
      backdrop-filter: blur(18px);
    }

    .section-head {
      margin-bottom: 12px;
      display: grid;
      gap: 5px;
    }

    .section-kicker {
      color: var(--muted);
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.62px;
      font-weight: 760;
    }

    .section h2 {
      margin: 0 0 2px;
      font-size: 1.1rem;
      color: var(--section-title-ink);
      letter-spacing: 0.16px;
      font-weight: 760;
    }

    .section-sub {
      color: var(--muted);
      font-size: 0.82rem;
      line-height: 1.42;
      max-width: 980px;
    }

    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(185px, 1fr));
      gap: 14px;
    }

    .kpi {
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-sm);
      background: linear-gradient(180deg, rgba(255,255,255,0.9) 0%, rgba(249,251,252,0.78) 100%);
      padding: 14px 15px;
      min-height: 120px;
      box-shadow: var(--kpi-inset-shadow);
      border-top: 4px solid var(--border-strong-soft);
      display: grid;
      gap: 6px;
      position: relative;
      overflow: hidden;
    }

    [data-theme="dark"] .kpi {
      background: linear-gradient(180deg, rgba(24,41,55,0.9) 0%, rgba(18,33,46,0.82) 100%);
    }

    .kpi .label {
      font-size: 0.69rem;
      text-transform: uppercase;
      color: var(--muted);
      letter-spacing: 0.48px;
      font-weight: 700;
    }

    .kpi .value {
      font-size: 1.44rem;
      font-weight: 760;
      color: var(--kpi-value-ink);
      line-height: 1.08;
    }

    .kpi .context {
      color: var(--muted);
      font-size: 0.78rem;
      line-height: 1.38;
    }

    .kpi-positive { border-top-color: rgba(31, 107, 68, 0.46); }
    .kpi-watch { border-top-color: rgba(155, 106, 18, 0.52); }
    .kpi-critical { border-top-color: rgba(181, 58, 51, 0.54); }
    .kpi-neutral { border-top-color: var(--border-strong-soft); }

    .kpi-positive .value { color: var(--ok); }
    .kpi-watch .value { color: var(--warn); }
    .kpi-critical .value { color: var(--danger); }

    [data-theme="dark"] .kpi-positive .value,
    [data-theme="dark"] .kpi-watch .value,
    [data-theme="dark"] .kpi-critical .value {
      filter: brightness(1.1);
    }

    .callout-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }

    .callout {
      border: 1px solid var(--border-soft);
      border-left: 4px solid var(--accent);
      border-radius: 16px;
      padding: 14px 15px;
      background: linear-gradient(180deg, rgba(248,250,252,0.94) 0%, rgba(244,248,250,0.84) 100%);
      min-height: 132px;
      font-size: 0.86rem;
      line-height: 1.5;
      color: var(--callout-ink);
      display: grid;
      gap: 7px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.68);
    }

    [data-theme="dark"] .callout {
      background: linear-gradient(180deg, rgba(28,47,62,0.92) 0%, rgba(22,39,53,0.86) 100%);
    }

    .callout-eyebrow {
      color: var(--muted);
      font-size: 0.68rem;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      font-weight: 700;
    }

    .callout-title {
      color: var(--section-title-ink);
      font-size: 0.97rem;
      line-height: 1.34;
      font-weight: 760;
    }

    .callout-body {
      color: var(--callout-ink);
      font-size: 0.83rem;
      line-height: 1.48;
    }

    .callout-critical { border-left-color: var(--danger); }
    .callout-watch { border-left-color: var(--warn); }
    .callout-positive { border-left-color: var(--ok); }
    .callout-neutral { border-left-color: var(--accent); }

    .callout-critical .callout-title { color: var(--danger); }
    .callout-watch .callout-title { color: var(--warn); }
    .callout-positive .callout-title { color: var(--ok); }

    [data-theme="dark"] .callout-critical .callout-title,
    [data-theme="dark"] .callout-watch .callout-title,
    [data-theme="dark"] .callout-positive .callout-title {
      filter: brightness(1.1);
    }

    .chart-grid-2 {
      display: grid;
      grid-template-columns: repeat(2, minmax(320px, 1fr));
      gap: 12px;
    }

    .chart-grid-3 {
      display: grid;
      grid-template-columns: repeat(3, minmax(280px, 1fr));
      gap: 12px;
    }

    .chart-card {
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-sm);
      background:
        radial-gradient(115% 120% at 100% 0%, rgba(13,97,125,0.08) 0%, transparent 56%),
        linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(249,251,252,0.82) 100%);
      padding: 12px 13px 10px;
      min-height: 324px;
      overflow: hidden;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.6), 0 16px 36px rgba(19, 39, 52, 0.05);
    }

    [data-theme="dark"] .chart-card {
      background:
        radial-gradient(115% 120% at 100% 0%, rgba(74,164,199,0.1) 0%, transparent 56%),
        linear-gradient(180deg, rgba(24,41,55,0.92) 0%, rgba(19,35,49,0.84) 100%);
    }

    .chart-card.tall { min-height: 388px; }
    .chart-card.short { min-height: 296px; }
    .chart-card > .js-plotly-plot,
    .chart-card > .plot-container {
      width: 100% !important;
      height: 100% !important;
      min-height: inherit;
    }

    .section.tradeoff .chart-card.tall {
      min-height: 430px;
    }

    .narrative {
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-sm);
      padding: 16px 16px;
      background: var(--narrative-bg);
      font-size: 0.88rem;
      line-height: 1.58;
      color: var(--ink);
    }

    .brief-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }

    .brief-item {
      border: 1px solid var(--border-soft);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(248,250,252,0.86) 100%);
      padding: 13px 14px;
      display: grid;
      gap: 6px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
    }

    [data-theme="dark"] .brief-item {
      background: linear-gradient(180deg, rgba(24,41,55,0.9) 0%, rgba(18,33,46,0.84) 100%);
    }

    .brief-label {
      color: var(--muted);
      font-size: 0.68rem;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      font-weight: 700;
    }

    .brief-copy {
      color: var(--ink);
      font-size: 0.84rem;
      line-height: 1.48;
    }

    .table-controls {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
      gap: 10px;
      flex-wrap: wrap;
    }

    .table-controls input {
      border: 1px solid var(--border-soft);
      border-radius: 11px;
      padding: 10px 12px;
      font-size: 0.85rem;
      min-width: 260px;
      background: var(--input-bg);
      color: var(--ink);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.66);
    }

    .table-wrap {
      overflow: auto;
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-sm);
      max-height: 420px;
      background: linear-gradient(180deg, rgba(255,255,255,0.95) 0%, rgba(250,252,253,0.9) 100%);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
    }

    [data-theme="dark"] .table-wrap {
      background: linear-gradient(180deg, rgba(24,41,55,0.9) 0%, rgba(18,33,46,0.84) 100%);
    }

    table {
      border-collapse: collapse;
      width: 100%;
      font-size: 0.82rem;
    }

    thead th {
      position: sticky;
      top: 0;
      background: var(--table-head-bg);
      color: var(--title-ink);
      text-align: left;
      border-bottom: 1px solid var(--border);
      padding: 8px;
      cursor: pointer;
      white-space: nowrap;
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.36px;
    }

    tbody td {
      border-bottom: 1px solid var(--border-soft);
      padding: 9px 8px;
      white-space: nowrap;
      vertical-align: top;
    }

    tbody tr:hover { background: var(--table-row-hover); }
    tbody tr:nth-child(even) { background: var(--panel-soft); }

    .risk-badge {
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.3px;
      display: inline-block;
    }

    .risk-low { background: var(--risk-low-bg); color: var(--risk-low-ink); }
    .risk-medium { background: var(--risk-medium-bg); color: var(--risk-medium-ink); }
    .risk-high { background: var(--risk-high-bg); color: var(--risk-high-ink); }
    .risk-critical { background: var(--risk-critical-bg); color: var(--risk-critical-ink); }

    .footer-note {
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.77rem;
      line-height: 1.4;
    }

    .priority-cell {
      font-weight: 760;
      color: var(--section-title-ink);
    }

    .action-cell {
      white-space: normal;
      min-width: 240px;
      line-height: 1.42;
    }

    .entity-cell {
      font-weight: 650;
      color: var(--title-ink);
    }

    @media (max-width: 1280px) {
      .container { padding: 18px; }
      .header-top { grid-template-columns: 1fr; }
      .hero-grid { grid-template-columns: 1fr 1fr 1fr; }
      .filters { grid-template-columns: repeat(4, minmax(150px, 1fr)); }
      .kpi-grid { grid-template-columns: repeat(4, minmax(170px, 1fr)); }
      .callout-grid { grid-template-columns: 1fr 1fr; }
      .chart-grid-3 { grid-template-columns: 1fr 1fr; }
      .assumption-grid { grid-template-columns: repeat(2, minmax(170px, 1fr)); }
      .chart-card.short { min-height: 330px; }
      .chart-card.tall { min-height: 420px; }
    }

    @media (max-width: 820px) {
      .container { padding: 14px; }
      .header-top { grid-template-columns: 1fr; }
      .hero-grid { grid-template-columns: 1fr; }
      .header-toolbar { align-items: stretch; }
      .toolbar-note { margin-left: 0; width: 100%; }
      .filters { grid-template-columns: repeat(2, minmax(130px, 1fr)); }
      .kpi-grid { grid-template-columns: repeat(2, minmax(150px, 1fr)); }
      .callout-grid { grid-template-columns: 1fr; }
      .chart-grid-2, .chart-grid-3 { grid-template-columns: 1fr; }
      .assumption-grid { grid-template-columns: 1fr; }
      .brief-grid { grid-template-columns: 1fr; }
      .table-controls input { min-width: 100%; }
      .chart-card,
      .chart-card.short,
      .chart-card.tall {
        min-height: 345px;
      }
    }

    @media print {
      :root,
      [data-theme="dark"] {
        --bg: #ffffff;
        --bg-grad-a: #ffffff;
        --bg-grad-b: #ffffff;
        --bg-grad-c: #ffffff;
        --panel: #ffffff;
        --panel-alt: #ffffff;
        --panel-soft: #ffffff;
        --narrative-bg: #ffffff;
        --ink: #111111;
        --muted: #444444;
        --title-ink: #111111;
        --section-title-ink: #111111;
        --kpi-value-ink: #111111;
        --border: #d8d8d8;
        --border-soft: #e2e2e2;
        --shadow-sm: none;
        --shadow-md: none;
      }

      body {
        background: #ffffff !important;
        color: #111111 !important;
      }

      .container {
        max-width: none;
        padding: 0;
      }

      .header,
      .section {
        box-shadow: none !important;
        break-inside: avoid;
        page-break-inside: avoid;
      }

      .filters,
      .assumption-panel,
      .methodology-panel,
      .consistency-alert,
      .table-controls {
        display: none !important;
      }

      .section {
        margin-bottom: 12px;
      }

      .chart-grid-2,
      .chart-grid-3 {
        grid-template-columns: 1fr;
        gap: 10px;
      }

      .chart-card,
      .chart-card.short,
      .chart-card.tall {
        min-height: 255px !important;
        border-color: #d8d8d8;
      }

      .table-wrap {
        max-height: none;
        overflow: visible;
      }

      thead th {
        position: static;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="header-top">
        <div class="header-copy">
          <div class="header-meta">
            <span class="meta-pill">Executive command center</span>
            <span class="meta-pill" id="header-scope">Filtered operating scope</span>
            <span class="meta-pill" id="header-updated">Updated from governed snapshot</span>
          </div>
          <div>
          <h1 class="title">Supply Chain Service Level, Inventory Risk & Working Capital Intelligence System</h1>
          <div class="subtitle">Executive Operations & Finance Review Dashboard: service reliability, stockout leakage, inventory efficiency, and intervention priorities.</div>
        </div>
        </div>
        <div class="hero-panel">
          <div class="hero-eyebrow">Current decision frame</div>
          <div class="hero-headline" id="hero-headline"></div>
          <div class="hero-summary" id="hero-summary"></div>
          <div class="hero-grid">
            <div class="hero-card">
              <div class="hero-label">Immediate action</div>
              <div class="hero-value" id="hero-primary"></div>
              <div class="hero-detail" id="hero-primary-detail"></div>
            </div>
            <div class="hero-card">
              <div class="hero-label">Largest exposure</div>
              <div class="hero-value" id="hero-exposure"></div>
              <div class="hero-detail" id="hero-exposure-detail"></div>
            </div>
            <div class="hero-card">
              <div class="hero-label">Value at stake</div>
              <div class="hero-value" id="hero-opportunity"></div>
              <div class="hero-detail" id="hero-opportunity-detail"></div>
            </div>
          </div>
        </div>
      </div>
      <div id="consistency-alert" class="consistency-alert"></div>
      <div id="no-data-alert" class="no-data-alert"></div>

      <div class="header-toolbar">
        <button class="toolbar-button toolbar-button-ghost" id="toggle-assumptions">Scenario Controls</button>
        <button class="toolbar-button toolbar-button-ghost" id="toggle-methodology">Method Notes</button>
        <button class="toolbar-button toolbar-button-accent" id="reset-filters">Reset Filters</button>
        <button class="toolbar-button" id="toggle-theme">Dark Mode</button>
        <button class="toolbar-button toolbar-button-ok" id="print-dashboard">Print / Export PDF</button>
        <div class="toolbar-note">Primary controls stay visible. Scenario and method panels stay collapsed until needed.</div>
      </div>

      <div class="filters">
        <div class="filter-box"><label>Region</label><select id="filter-region"></select></div>
        <div class="filter-box"><label>Warehouse</label><select id="filter-warehouse"></select></div>
        <div class="filter-box"><label>Category</label><select id="filter-category"></select></div>
        <div class="filter-box"><label>Supplier</label><select id="filter-supplier"></select></div>
        <div class="filter-box"><label>ABC Class</label><select id="filter-abc"></select></div>
        <div class="filter-box"><label>Date Start</label><input id="filter-start" type="month" /></div>
        <div class="filter-box"><label>Date End</label><input id="filter-end" type="month" /></div>
      </div>

      <div class="methodology-panel" id="methodology-panel">
        KPI and risk metrics use filtered monthly operational aggregates. Governance scores are interpretable weighted composites of service risk, stockout risk, excess inventory risk, supplier risk, and working-capital risk. Scenario controls recalculate the 12M opportunity proxy in real time; these values are directional planning estimates, not statutory accounting values.
      </div>

      <div class="assumption-panel">
        <div class="assumption-grid">
          <div class="assumption-box">
            <label>Recoverable Lost Margin Rate</label>
            <input id="assump-margin-rate" type="range" min="10" max="60" step="1" />
            <div class="assumption-value" id="assump-margin-rate-value"></div>
          </div>
          <div class="assumption-box">
            <label>Releasable Working Capital Rate</label>
            <input id="assump-wc-rate" type="range" min="5" max="60" step="1" />
            <div class="assumption-value" id="assump-wc-rate-value"></div>
          </div>
          <div class="assumption-box">
            <label>Slow-Moving Incremental Weight</label>
            <input id="assump-slow-weight" type="range" min="0" max="100" step="1" />
            <div class="assumption-value" id="assump-slow-weight-value"></div>
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Portfolio scorecard</div>
        <h2>Operating Scorecard</h2>
        <div class="section-sub">Portfolio-level service, stockout, working-capital, supplier execution, and intervention-pressure KPIs.</div>
      </div>
      <div class="kpi-grid" id="kpi-grid"></div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Priority signals</div>
        <h2>Executive Decision Priorities</h2>
        <div class="section-sub">Concise operating signals on where leadership attention should go first.</div>
      </div>
      <div class="callout-grid" id="callout-grid"></div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Trend view</div>
        <h2>Performance Over Time</h2>
        <div class="section-sub">Trend diagnostics to separate temporary volatility from persistent operating drift.</div>
      </div>
      <div class="chart-grid-2">
        <div class="chart-card" id="chart-service-trend"></div>
        <div class="chart-card" id="chart-stockout-trend"></div>
        <div class="chart-card" id="chart-lost-sales-trend"></div>
        <div class="chart-card" id="chart-inventory-trend"></div>
      </div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Cross-section comparison</div>
        <h2>Exposure by Node</h2>
        <div class="section-sub">Cross-sectional comparison of warehouses, categories, regions, and suppliers to locate operational imbalance.</div>
      </div>
      <div class="chart-grid-3">
        <div class="chart-card short" id="chart-fill-warehouse"></div>
        <div class="chart-card short" id="chart-fill-category"></div>
        <div class="chart-card short" id="chart-lostsales-region"></div>
        <div class="chart-card short" id="chart-supplier-otd"></div>
        <div class="chart-card short" id="chart-lead-var"></div>
        <div class="chart-card short" id="chart-excess-category"></div>
      </div>
    </div>

    <div class="section tradeoff">
      <div class="section-head">
        <div class="section-kicker">Portfolio balance</div>
        <h2>Service vs Inventory Trade-off</h2>
        <div class="section-sub">Quadrant and scatter analysis for identifying understock, overstock, and dual-failure segments.</div>
      </div>
      <div class="chart-grid-3">
        <div class="chart-card tall" id="chart-service-vs-inventory"></div>
        <div class="chart-card tall" id="chart-service-vs-dos"></div>
        <div class="chart-card tall" id="chart-quadrant"></div>
      </div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Execution queue</div>
        <h2>Intervention Prioritization</h2>
        <div class="section-sub">Action queue diagnostics by SKU, supplier, warehouse, and supplier-risk intensity.</div>
      </div>
      <div class="chart-grid-3">
        <div class="chart-card tall" id="chart-top-governance"></div>
        <div class="chart-card tall" id="chart-top-suppliers"></div>
        <div class="chart-card tall" id="chart-top-warehouses"></div>
      </div>
      <div style="margin-top:10px" class="chart-card tall" id="chart-supplier-heatmap"></div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Execution detail</div>
        <h2>Priority Action Table</h2>
        <div class="section-sub">Sortable and filterable SKU-warehouse intervention table for tactical execution ownership.</div>
      </div>
      <div class="table-controls">
        <input id="table-search" type="text" placeholder="Search product, warehouse, supplier, risk driver, action..." />
        <div id="table-meta" class="footer-note"></div>
      </div>
      <div class="table-wrap">
        <table id="detail-table">
          <thead>
            <tr>
              <th data-key="product_id">SKU</th>
              <th data-key="product_name">Product</th>
              <th data-key="warehouse_id">Warehouse</th>
              <th data-key="supplier_id">Supplier</th>
              <th data-key="fill_rate">Fill Rate</th>
              <th data-key="stockout_risk_score">Stockout Risk</th>
              <th data-key="excess_inventory_score">Excess Risk</th>
              <th data-key="working_capital_risk_score">WC Risk</th>
              <th data-key="governance_priority_score">Priority Score</th>
              <th data-key="risk_tier">Tier</th>
              <th data-key="main_risk_driver">Primary Driver</th>
              <th data-key="recommended_action">Recommended Action</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <div class="section">
      <div class="section-head">
        <div class="section-kicker">Executive interpretation</div>
        <h2>Executive Brief</h2>
        <div class="section-sub">Decision-oriented interpretation summarizing what is wrong, where exposure sits, and what should happen next.</div>
      </div>
      <div class="narrative" id="narrative-panel"></div>
      <div class="footer-note">Synthetic data demonstration. Replace source tables with company data extracts to operationalize.</div>
    </div>
  </div>

  <script>__PLOTLY_JS__</script>
  <script>
    const dashboardData = __DATA_JSON__;

    function decodeMonthlyFact(compact) {
      const dim = compact.dim;
      const rows = compact.rows || [];
      return rows.map(r => ({
        month: dim.month[r[0]],
        region: dim.region[r[1]],
        warehouse_id: dim.warehouse_id[r[2]],
        product_id: dim.product_id[r[3]],
        category: dim.category[r[4]],
        supplier_id: dim.supplier_id[r[5]],
        abc_class: dim.abc_class[r[6]],
        units_demanded: Number(r[7]),
        units_fulfilled: Number(r[8]),
        units_lost_sales: Number(r[9]),
        lost_sales_revenue: Number(r[10]),
        inventory_value: Number(r[11]),
        avg_days_of_supply: Number(r[12]),
        excess_inventory_proxy: Number(r[13]),
        slow_moving_proxy: Number(r[14]),
        trapped_wc_proxy: Number(r[15]),
        lost_sales_margin_proxy: Number(r[16]),
        observation_days: Number(r[17]),
        stockout_month_flag: Number(r[18]),
      }));
    }

    const monthlyFact = decodeMonthlyFact(dashboardData.monthly_sku_compact);
    const supplierMeta = Object.fromEntries(dashboardData.suppliers.map(s => [s.supplier_id, s]));
    const warehouseMeta = Object.fromEntries(dashboardData.warehouses.map(w => [w.warehouse_id, w]));
    const productMeta = dashboardData.product_name_map || {};
    const skuRiskBaselineMap = Object.fromEntries(
      dashboardData.sku_risk_baseline.map(s => [`${s.product_id}|${s.warehouse_id}|${s.supplier_id}`, s])
    );
    const PLOT_CONFIG = {
      displayModeBar: false,
      responsive: true,
      scrollZoom: false
    };
    const printButton = document.getElementById('print-dashboard');
    const resetButton = document.getElementById('reset-filters');
    const themeToggle = document.getElementById('toggle-theme');
    const methodologyToggle = document.getElementById('toggle-methodology');
    const assumptionToggle = document.getElementById('toggle-assumptions');

    const filters = {
      region: document.getElementById('filter-region'),
      warehouse: document.getElementById('filter-warehouse'),
      category: document.getElementById('filter-category'),
      supplier: document.getElementById('filter-supplier'),
      abc: document.getElementById('filter-abc'),
      start: document.getElementById('filter-start'),
      end: document.getElementById('filter-end')
    };

    const assumptions = {
      marginRate: document.getElementById('assump-margin-rate'),
      wcRate: document.getElementById('assump-wc-rate'),
      slowWeight: document.getElementById('assump-slow-weight'),
      marginRateValue: document.getElementById('assump-margin-rate-value'),
      wcRateValue: document.getElementById('assump-wc-rate-value'),
      slowWeightValue: document.getElementById('assump-slow-weight-value'),
    };

    const tableSearch = document.getElementById('table-search');
    const tableBody = document.querySelector('#detail-table tbody');
    const tableMeta = document.getElementById('table-meta');

    let tableSort = { key: 'governance_priority_score', dir: 'desc' };
    let lastAgg = null;
    let currentTheme = 'light';

    function fmtPct(v) { return `${(v * 100).toFixed(1)}%`; }
    function fmtNum(v) { return Number(v).toLocaleString(); }
    function fmtEur(v) { return `EUR ${Number(v).toLocaleString(undefined, {maximumFractionDigits: 0})}`; }
    function fmtEurM(v) { return `EUR ${(Number(v) / 1_000_000).toFixed(2)}M`; }
    function fmtCompactEur(v) {
      const value = Math.abs(Number(v) || 0);
      if (value >= 1_000_000_000) return `EUR ${(value / 1_000_000_000).toFixed(2)}B`;
      if (value >= 1_000_000) return `EUR ${(value / 1_000_000).toFixed(2)}M`;
      if (value >= 1_000) return `EUR ${(value / 1_000).toFixed(1)}K`;
      return fmtEur(value);
    }
    function ellipsize(text, maxLen = 28) {
      const value = String(text || '');
      return value.length > maxLen ? `${value.slice(0, Math.max(0, maxLen - 1))}…` : value;
    }
    function dynamicLeftMargin(labels, floor = 120, ceil = 235, unit = 6.2) {
      const maxLen = labels.reduce((acc, label) => Math.max(acc, String(label || '').length), 0);
      return Math.max(floor, Math.min(ceil, Math.round(maxLen * unit)));
    }

    function clamp01(x) { return Math.max(0, Math.min(1, x)); }
    function norm(v, low, high) {
      if (high <= low) return 0;
      return clamp01((v - low) / (high - low));
    }

    function computeBalancedShare(agg) {
      return agg.skuRows.filter(r => r.fill_rate >= 0.97 && r.avg_dos >= 8 && r.avg_dos <= 35).length / Math.max(agg.skuRows.length, 1);
    }

    function classifyPosture(agg, balancedShare) {
      if (agg.totals.fillRate < 0.95 && balancedShare < 0.35) return 'critical';
      if (agg.totals.stockoutRate > 0.05 || agg.totals.totalExcess > agg.totals.totalInventory * 0.18) return 'watch';
      return 'stable';
    }

    function getPreferredTheme() {
      const saved = window.localStorage.getItem('dashboard_theme');
      if (saved === 'dark' || saved === 'light') return saved;
      return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }

    function getThemePalette() {
      if (currentTheme === 'dark') {
        return {
          title: '#eaf4ff',
          font: '#dbe8f5',
          grid: '#2a4458',
          zero: '#36556d',
          paper: 'rgba(0,0,0,0)',
          plot: 'rgba(0,0,0,0)',
          hoverBg: '#0f1d29',
          service: '#7db8e8',
          stockout: '#df857c',
          lostSales: '#e48f7c',
          inventory: '#7da0c8',
          warehouse: '#6d97c7',
          category: '#64baac',
          region: '#d68a72',
          supplierOtd: '#9387d8',
          leadVar: '#d89884',
          excess: '#d3a563',
          quadrant: '#7ca7d1',
          governance: '#53a9c5',
          supplierRisk: '#c88863',
          warehouseRisk: '#d86f67',
          lineRef: '#90a4b8',
          annGood: '#6dc2af',
          annBad: '#df857c',
        };
      }
      return {
        title: '#133a4d',
        font: '#1c2f3b',
        grid: '#eef2f5',
        zero: '#dfe6eb',
        paper: 'rgba(0,0,0,0)',
        plot: 'rgba(0,0,0,0)',
        hoverBg: '#0f2f40',
        service: '#1b6784',
        stockout: '#b14f48',
        lostSales: '#c9654d',
        inventory: '#5a83aa',
        warehouse: '#4c7198',
        category: '#2d8473',
        region: '#b85d4d',
        supplierOtd: '#7468b6',
        leadVar: '#b77160',
        excess: '#bf8a38',
        quadrant: '#3f6f9a',
        governance: '#0d607d',
        supplierRisk: '#a86142',
        warehouseRisk: '#b94f49',
        lineRef: '#6b7280',
        annGood: '#2d8473',
        annBad: '#b14f48',
      };
    }

    function applyTheme(mode, rerender = true) {
      currentTheme = mode === 'dark' ? 'dark' : 'light';
      document.documentElement.setAttribute('data-theme', currentTheme);
      window.localStorage.setItem('dashboard_theme', currentTheme);
      if (themeToggle) {
        themeToggle.textContent = currentTheme === 'dark' ? 'Light Mode' : 'Dark Mode';
      }
      if (rerender && lastAgg) {
        renderCharts(lastAgg);
      }
    }

    function togglePanel(panelId, triggerButton, closedLabel, openLabel) {
      const panel = document.getElementById(panelId);
      if (!panel || !triggerButton) return;
      const willOpen = panel.style.display !== 'block';
      panel.style.display = willOpen ? 'block' : 'none';
      triggerButton.textContent = willOpen ? openLabel : closedLabel;
      triggerButton.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
    }

    function getUniqueValues(rows, key) {
      return Array.from(new Set(rows.map(r => r[key]).filter(v => v !== null && v !== undefined && v !== ''))).sort();
    }

    function initializeAssumptions() {
      const defaults = dashboardData.meta.assumptions_default || {
        recoverable_margin_rate: 0.35,
        releasable_wc_rate: 0.25,
        slow_moving_incremental_weight: 0.50,
      };

      assumptions.marginRate.value = String(Math.round(defaults.recoverable_margin_rate * 100));
      assumptions.wcRate.value = String(Math.round(defaults.releasable_wc_rate * 100));
      assumptions.slowWeight.value = String(Math.round(defaults.slow_moving_incremental_weight * 100));
      updateAssumptionLabels();
    }

    function readAssumptions() {
      return {
        recoverableMarginRate: Number(assumptions.marginRate.value) / 100.0,
        releasableWcRate: Number(assumptions.wcRate.value) / 100.0,
        slowMovingIncrementalWeight: Number(assumptions.slowWeight.value) / 100.0,
      };
    }

    function updateAssumptionLabels() {
      const a = readAssumptions();
      assumptions.marginRateValue.textContent = `${fmtPct(a.recoverableMarginRate)} of annualized lost-sales margin`;
      assumptions.wcRateValue.textContent = `${fmtPct(a.releasableWcRate)} of trapped working capital`;
      assumptions.slowWeightValue.textContent = `${fmtPct(a.slowMovingIncrementalWeight)} weight on non-excess slow-moving value`;
    }

    function populateFilters() {
      const rows = monthlyFact;
      const cfg = [
        ['region', 'region'],
        ['warehouse', 'warehouse_id'],
        ['category', 'category'],
        ['supplier', 'supplier_id'],
        ['abc', 'abc_class']
      ];

      cfg.forEach(([id, key]) => {
        const select = filters[id];
        const values = getUniqueValues(rows, key);
        select.innerHTML = '<option value="ALL">All</option>' + values.map(v => `<option value="${v}">${v}</option>`).join('');
      });

      filters.start.value = dashboardData.meta.date_min.slice(0, 7);
      filters.end.value = dashboardData.meta.date_max.slice(0, 7);
    }

    function resetFilters() {
      filters.region.value = 'ALL';
      filters.warehouse.value = 'ALL';
      filters.category.value = 'ALL';
      filters.supplier.value = 'ALL';
      filters.abc.value = 'ALL';
      filters.start.value = dashboardData.meta.date_min.slice(0, 7);
      filters.end.value = dashboardData.meta.date_max.slice(0, 7);
      if (tableSearch) {
        tableSearch.value = '';
      }
      tableSort = { key: 'governance_priority_score', dir: 'desc' };
      updateDashboard();
    }

    function getNormalizedDateRange() {
      let start = (filters.start.value || dashboardData.meta.date_min.slice(0, 7)) + '-01';
      let end = (filters.end.value || dashboardData.meta.date_max.slice(0, 7)) + '-01';
      if (start > end) {
        const temp = start;
        start = end;
        end = temp;
      }
      return { start, end };
    }

    function passesFilter(row, dateRange) {
      return (
        (filters.region.value === 'ALL' || row.region === filters.region.value) &&
        (filters.warehouse.value === 'ALL' || row.warehouse_id === filters.warehouse.value) &&
        (filters.category.value === 'ALL' || row.category === filters.category.value) &&
        (filters.supplier.value === 'ALL' || row.supplier_id === filters.supplier.value) &&
        (filters.abc.value === 'ALL' || row.abc_class === filters.abc.value) &&
        row.month >= dateRange.start && row.month <= dateRange.end
      );
    }

    function aggregate(rows, assumptionSet) {
      const monthMap = new Map();
      const whMap = new Map();
      const catMap = new Map();
      const regionMap = new Map();
      const supplierMap = new Map();
      const segmentMap = new Map();
      const skuMap = new Map();

      let totalDemand = 0;
      let totalFulfilled = 0;
      let totalLost = 0;
      let totalLostSales = 0;
      let totalInventory = 0;
      let totalExcess = 0;
      let totalTrappedWCObserved = 0;
      let totalTrappedWCScenario = 0;
      let totalSlow = 0;
      let totalLostSalesMargin = 0;

      for (const r of rows) {
        const demand = Number(r.units_demanded);
        const fulfilled = Number(r.units_fulfilled);
        const lost = Number(r.units_lost_sales);
        const lostSales = Number(r.lost_sales_revenue);
        const inv = Number(r.inventory_value);
        const excess = Number(r.excess_inventory_proxy);
        const trapped = Number(r.trapped_wc_proxy);
        const slow = Number(r.slow_moving_proxy);
        const slowNonExcess = Math.max(slow - excess, 0);
        const trappedScenario = excess + assumptionSet.slowMovingIncrementalWeight * slowNonExcess;
        const lostSalesMargin = Number(r.lost_sales_margin_proxy || 0);
        const dos = Number(r.avg_days_of_supply);
        const obsDays = Number(r.observation_days);
        const stockoutMonthFlag = Number(r.stockout_month_flag);

        totalDemand += demand;
        totalFulfilled += fulfilled;
        totalLost += lost;
        totalLostSales += lostSales;
        totalInventory += inv;
        totalExcess += excess;
        totalTrappedWCObserved += trapped;
        totalTrappedWCScenario += trappedScenario;
        totalSlow += slow;
        totalLostSalesMargin += lostSalesMargin;

        const monthKey = r.month;
        if (!monthMap.has(monthKey)) monthMap.set(monthKey, { month: monthKey, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0 });
        const m = monthMap.get(monthKey);
        m.demand += demand; m.fulfilled += fulfilled; m.lost += lost; m.lostSales += lostSales; m.inventory += inv;

        const whKey = r.warehouse_id;
        if (!whMap.has(whKey)) whMap.set(whKey, { warehouse_id: whKey, warehouse_name: (warehouseMeta[whKey] || {}).warehouse_name || whKey, region: r.region, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0, dosWeighted: 0, obsDays: 0 });
        const w = whMap.get(whKey);
        w.demand += demand; w.fulfilled += fulfilled; w.lost += lost; w.lostSales += lostSales; w.inventory += inv; w.dosWeighted += dos * obsDays; w.obsDays += obsDays;

        const catKey = r.category;
        if (!catMap.has(catKey)) catMap.set(catKey, { category: catKey, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0, excess: 0, trapped: 0, dosWeighted: 0, obsDays: 0 });
        const c = catMap.get(catKey);
        c.demand += demand; c.fulfilled += fulfilled; c.lost += lost; c.lostSales += lostSales; c.inventory += inv; c.excess += excess; c.trapped += trapped; c.dosWeighted += dos * obsDays; c.obsDays += obsDays;

        const regKey = r.region;
        if (!regionMap.has(regKey)) regionMap.set(regKey, { region: regKey, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0 });
        const rg = regionMap.get(regKey);
        rg.demand += demand; rg.fulfilled += fulfilled; rg.lost += lost; rg.lostSales += lostSales; rg.inventory += inv;

        const supKey = r.supplier_id;
        if (!supplierMap.has(supKey)) {
          const meta = supplierMeta[supKey] || {};
          supplierMap.set(supKey, {
            supplier_id: supKey,
            supplier_name: meta.supplier_name || supKey,
            on_time_delivery_rate: Number(meta.on_time_delivery_rate || 0),
            average_delay_days: Number(meta.average_delay_days || 0),
            lead_time_variability: Number(meta.lead_time_variability || 0),
            supplier_service_risk_proxy: Number(meta.supplier_service_risk_proxy || 0),
            demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0
          });
        }
        const s = supplierMap.get(supKey);
        s.demand += demand; s.fulfilled += fulfilled; s.lost += lost; s.lostSales += lostSales; s.inventory += inv;

        const segKey = `${r.category}|${r.region}`;
        if (!segmentMap.has(segKey)) segmentMap.set(segKey, { segment: segKey, category: r.category, region: r.region, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0, dosWeighted: 0, obsDays: 0 });
        const sg = segmentMap.get(segKey);
        sg.demand += demand; sg.fulfilled += fulfilled; sg.lost += lost; sg.lostSales += lostSales; sg.inventory += inv; sg.dosWeighted += dos * obsDays; sg.obsDays += obsDays;

        const skuKey = `${r.product_id}|${r.warehouse_id}|${r.supplier_id}`;
        if (!skuMap.has(skuKey)) skuMap.set(skuKey, {
          product_id: r.product_id,
          product_name: productMeta[r.product_id] || r.product_id,
          warehouse_id: r.warehouse_id,
          supplier_id: r.supplier_id,
          category: r.category,
          region: r.region,
          abc_class: r.abc_class,
          demand: 0,
          fulfilled: 0,
          lost: 0,
          lostSales: 0,
          inventory: 0,
          excess: 0,
          trapped: 0,
          slow: 0,
          dosWeighted: 0,
          obsDays: 0,
          monthCount: 0,
          stockoutMonthCount: 0,
        });

        const k = skuMap.get(skuKey);
        k.demand += demand; k.fulfilled += fulfilled; k.lost += lost; k.lostSales += lostSales;
        k.inventory += inv; k.excess += excess; k.trapped += trapped; k.slow += slow;
        k.dosWeighted += dos * obsDays; k.obsDays += obsDays;
        k.monthCount += 1;
        k.stockoutMonthCount += stockoutMonthFlag;
      }

      const monthSeries = Array.from(monthMap.values()).sort((a,b) => a.month.localeCompare(b.month)).map(m => ({
        ...m,
        fill_rate: m.demand > 0 ? m.fulfilled / m.demand : 1,
        stockout_rate: m.demand > 0 ? m.lost / m.demand : 0
      }));

      const warehouses = Array.from(whMap.values()).map(w => ({
        ...w,
        fill_rate: w.demand > 0 ? w.fulfilled / w.demand : 1,
        stockout_rate: w.demand > 0 ? w.lost / w.demand : 0,
        avg_dos: w.obsDays > 0 ? w.dosWeighted / w.obsDays : 0
      })).sort((a,b)=>b.lostSales-a.lostSales);

      const categories = Array.from(catMap.values()).map(c => ({
        ...c,
        fill_rate: c.demand > 0 ? c.fulfilled / c.demand : 1,
        stockout_rate: c.demand > 0 ? c.lost / c.demand : 0,
        avg_dos: c.obsDays > 0 ? c.dosWeighted / c.obsDays : 0
      })).sort((a,b)=>b.lostSales-a.lostSales);

      const regions = Array.from(regionMap.values()).map(r => ({
        ...r,
        fill_rate: r.demand > 0 ? r.fulfilled / r.demand : 1,
        stockout_rate: r.demand > 0 ? r.lost / r.demand : 0
      })).sort((a,b)=>b.lostSales-a.lostSales);

      const suppliers = Array.from(supplierMap.values()).map(s => ({
        ...s,
        fill_rate: s.demand > 0 ? s.fulfilled / s.demand : 1,
        stockout_rate: s.demand > 0 ? s.lost / s.demand : 0
      })).sort((a,b)=>b.lostSales-a.lostSales);

      const segments = Array.from(segmentMap.values()).map(s => ({
        ...s,
        fill_rate: s.demand > 0 ? s.fulfilled / s.demand : 1,
        stockout_rate: s.demand > 0 ? s.lost / s.demand : 0,
        avg_dos: s.obsDays > 0 ? s.dosWeighted / s.obsDays : 0
      }));

      const skuRows = Array.from(skuMap.values()).map(k => {
        const fillRate = k.demand > 0 ? k.fulfilled / k.demand : 1;
        const stockoutRate = k.demand > 0 ? k.lost / k.demand : 0;
        const avgDos = k.obsDays > 0 ? k.dosWeighted / k.obsDays : 0;
        const stockoutPersistence = k.monthCount > 0 ? k.stockoutMonthCount / k.monthCount : 0;
        const baseline = skuRiskBaselineMap[`${k.product_id}|${k.warehouse_id}|${k.supplier_id}`] || {};

        const serviceRiskScore = Number(baseline.service_risk_score || 0);
        const stockoutRiskScore = Number(baseline.stockout_risk_score || 0);
        const excessInventoryScore = Number(baseline.excess_inventory_score || 0);
        const supplierRiskScore = Number(baseline.supplier_risk_score || 0);
        const workingCapitalRiskScore = Number(baseline.working_capital_risk_score || 0);
        const governancePriorityScore = Number(baseline.governance_priority_score || 0);

        return {
          product_id: k.product_id,
          product_name: productMeta[k.product_id] || k.product_id,
          warehouse_id: k.warehouse_id,
          supplier_id: k.supplier_id,
          category: k.category,
          region: k.region,
          abc_class: k.abc_class,
          fill_rate: fillRate,
          stockout_rate: stockoutRate,
          avg_dos: avgDos,
          lost_sales_revenue: k.lostSales,
          inventory_value: k.inventory,
          excess_inventory_proxy: k.excess,
          trapped_wc_proxy: k.trapped,
          service_risk_score: serviceRiskScore,
          stockout_risk_score: stockoutRiskScore,
          excess_inventory_score: excessInventoryScore,
          supplier_risk_score: supplierRiskScore,
          working_capital_risk_score: workingCapitalRiskScore,
          governance_priority_score: governancePriorityScore,
          risk_tier: baseline.risk_tier || "Low",
          main_risk_driver: baseline.main_risk_driver || "Service Risk",
          recommended_action: baseline.recommended_action || "review planning assumptions",
          stockout_persistence: stockoutPersistence,
        };
      }).sort((a,b)=>b.governance_priority_score-a.governance_priority_score);

      const weightedSupplierOTD = suppliers.reduce((acc, s) => acc + s.on_time_delivery_rate * s.demand, 0) / Math.max(totalDemand, 1);
      const scenarioOpportunity12m =
        (totalLostSalesMargin * assumptionSet.recoverableMarginRate) +
        (totalTrappedWCScenario * assumptionSet.releasableWcRate);

      return {
        totals: {
          totalDemand, totalFulfilled, totalLost,
          totalLostSales, totalInventory, totalExcess, totalTrappedWCObserved, totalTrappedWCScenario, totalSlow, totalLostSalesMargin,
          scenarioOpportunity12m,
          fillRate: totalDemand > 0 ? totalFulfilled / totalDemand : 1,
          stockoutRate: totalDemand > 0 ? totalLost / totalDemand : 0,
          weightedSupplierOTD: Number.isFinite(weightedSupplierOTD) ? weightedSupplierOTD : 0,
          recoverableMarginRate: assumptionSet.recoverableMarginRate,
          releasableWcRate: assumptionSet.releasableWcRate,
          slowMovingIncrementalWeight: assumptionSet.slowMovingIncrementalWeight,
        },
        monthSeries,
        warehouses,
        categories,
        regions,
        suppliers,
        segments,
        skuRows,
      };
    }

    function renderHeaderSummary(agg, dateRange) {
      const balancedShare = computeBalancedShare(agg);
      const posture = classifyPosture(agg, balancedShare);
      const topSku = agg.skuRows[0];
      const topSupplier = [...agg.suppliers].sort((a,b) => (b.stockout_rate * b.lostSales) - (a.stockout_rate * a.lostSales))[0];
      const topWarehouse = [...agg.warehouses].sort((a,b) => b.lostSales - a.lostSales)[0];
      const headerScope = document.getElementById('header-scope');
      const headerUpdated = document.getElementById('header-updated');
      const headline = document.getElementById('hero-headline');
      const summary = document.getElementById('hero-summary');

      if (headerScope) {
        headerScope.textContent = `${fmtNum(agg.skuRows.length)} SKU-warehouse positions | ${fmtNum(agg.warehouses.length)} warehouses | ${fmtNum(agg.suppliers.length)} suppliers`;
      }
      if (headerUpdated) {
        headerUpdated.textContent = `${dateRange.start.slice(0, 7)} to ${dateRange.end.slice(0, 7)} | ${dashboardData.generated_at}`;
      }

      if (headline) {
        if (posture === 'critical') {
          headline.textContent = 'Service is under target while capital remains trapped in the network.';
        } else if (posture === 'watch') {
          headline.textContent = 'Targeted intervention is required to recover service without adding broad inventory.';
        } else {
          headline.textContent = 'Portfolio is broadly controlled, but concentration pockets still require active management.';
        }
      }

      if (summary) {
        summary.textContent = `This view isolates the filtered operating scope and surfaces the trade-off between customer service, supplier execution, and working-capital drag. Balanced positions currently represent ${fmtPct(balancedShare)} of the active portfolio.`;
      }

      const primary = document.getElementById('hero-primary');
      const primaryDetail = document.getElementById('hero-primary-detail');
      if (primary) {
        primary.textContent = topSku ? `${topSku.product_id} @ ${topSku.warehouse_id}` : 'No priority item';
      }
      if (primaryDetail) {
        primaryDetail.textContent = topSku
          ? `Priority score ${topSku.governance_priority_score.toFixed(1)}. Main driver: ${topSku.main_risk_driver}.`
          : 'No governed SKU priority is available in the current slice.';
      }

      const exposure = document.getElementById('hero-exposure');
      const exposureDetail = document.getElementById('hero-exposure-detail');
      if (exposure) {
        exposure.textContent = topSupplier ? topSupplier.supplier_name : (topWarehouse ? topWarehouse.warehouse_name : 'No exposure pocket');
      }
      if (exposureDetail) {
        exposureDetail.textContent = topSupplier
          ? `${fmtEurM(topSupplier.lostSales)} linked lost sales, OTD ${fmtPct(topSupplier.on_time_delivery_rate)}.`
          : (topWarehouse ? `${fmtEurM(topWarehouse.lostSales)} lost sales exposure in the most pressured warehouse.` : 'No material exposure pocket in the current slice.');
      }

      const opportunity = document.getElementById('hero-opportunity');
      const opportunityDetail = document.getElementById('hero-opportunity-detail');
      if (opportunity) {
        opportunity.textContent = fmtCompactEur(agg.totals.scenarioOpportunity12m);
      }
      if (opportunityDetail) {
        opportunityDetail.textContent = `${fmtPct(agg.totals.recoverableMarginRate)} lost-margin recovery + ${fmtPct(agg.totals.releasableWcRate)} releasable working capital assumption.`;
      }
    }

    function buildCallouts(agg) {
      const topSku = agg.skuRows[0];
      const whWorst = [...agg.warehouses].sort((a,b) => a.fill_rate - b.fill_rate)[0];
      const supWorst = [...agg.suppliers].sort((a,b) => (b.stockout_rate * b.lostSales) - (a.stockout_rate * a.lostSales))[0];
      const catExcess = [...agg.categories].sort((a,b)=>b.excess-a.excess)[0];
      const segmentImbalance = [...agg.segments].sort((a,b)=>{
        const aScore = (1-a.fill_rate)*0.65 + norm(a.avg_dos, 20, 70)*0.35;
        const bScore = (1-b.fill_rate)*0.65 + norm(b.avg_dos, 20, 70)*0.35;
        return bScore - aScore;
      })[0];

      const callouts = [];

      if (topSku) {
        callouts.push({
          tone: 'critical',
          eyebrow: 'Priority 1',
          title: `${topSku.product_id} @ ${topSku.warehouse_id}`,
          body: `Start with the highest-governance item. Score ${topSku.governance_priority_score.toFixed(1)} with ${topSku.main_risk_driver} as the lead driver.`
        });
      }

      if (supWorst) {
        callouts.push({
          tone: 'watch',
          eyebrow: 'Supplier escalation',
          title: supWorst.supplier_name,
          body: `${fmtEurM(supWorst.lostSales)} lost sales exposure sits behind this supplier. Weighted OTD is ${fmtPct(supWorst.on_time_delivery_rate)}.`
        });
      }

      if (whWorst) {
        callouts.push({
          tone: 'watch',
          eyebrow: 'Warehouse pressure',
          title: whWorst.warehouse_name,
          body: `Fill rate is ${fmtPct(whWorst.fill_rate)} with ${fmtEurM(whWorst.lostSales)} in lost sales. Replenishment settings need review before broad network changes.`
        });
      }

      if (catExcess) {
        callouts.push({
          tone: 'neutral',
          eyebrow: 'Capital concentration',
          title: catExcess.category,
          body: `${fmtEurM(catExcess.excess)} of excess-value proxy is concentrated here, making it the cleanest release candidate.`
        });
      }

      if (segmentImbalance) {
        callouts.push({
          tone: 'positive',
          eyebrow: 'Trade-off lens',
          title: `${segmentImbalance.category} | ${segmentImbalance.region}`,
          body: `${fmtPct(segmentImbalance.fill_rate)} fill rate with ${segmentImbalance.avg_dos.toFixed(1)} days of supply. This is the clearest service-versus-capital imbalance in the current slice.`
        });
      }

      return callouts.slice(0, 4);
    }

    function renderKPIs(agg) {
      const highRiskSkuCount = agg.skuRows.filter(r => r.risk_tier === 'High' || r.risk_tier === 'Critical').length;
      const kpis = [
        {
          label: 'Fill Rate',
          value: fmtPct(agg.totals.fillRate),
          context: 'Target operating threshold: 97%+.',
          tone: agg.totals.fillRate >= 0.97 ? 'positive' : (agg.totals.fillRate >= 0.95 ? 'watch' : 'critical'),
        },
        {
          label: 'Stockout Rate',
          value: fmtPct(agg.totals.stockoutRate),
          context: 'Direct indicator of demand leakage.',
          tone: agg.totals.stockoutRate <= 0.02 ? 'positive' : (agg.totals.stockoutRate <= 0.05 ? 'watch' : 'critical'),
        },
        {
          label: 'Lost Sales Value',
          value: fmtCompactEur(agg.totals.totalLostSales),
          context: 'Current filtered revenue exposure.',
          tone: agg.totals.totalLostSales <= 500000 ? 'neutral' : 'critical',
        },
        {
          label: '12M Opportunity',
          value: fmtCompactEur(agg.totals.scenarioOpportunity12m),
          context: 'Scenario-based upside from service recovery + capital release.',
          tone: 'positive',
        },
        {
          label: 'Working Capital at Risk',
          value: fmtCompactEur(agg.totals.totalTrappedWCObserved),
          context: 'Observed value currently trapped in inventory.',
          tone: agg.totals.totalTrappedWCObserved <= agg.totals.totalInventory * 0.12 ? 'neutral' : 'watch',
        },
        {
          label: 'Excess Inventory',
          value: fmtCompactEur(agg.totals.totalExcess),
          context: 'Value above days-of-supply caps.',
          tone: agg.totals.totalExcess <= agg.totals.totalInventory * 0.08 ? 'positive' : 'watch',
        },
        {
          label: 'Supplier OTD',
          value: fmtPct(agg.totals.weightedSupplierOTD),
          context: 'Demand-weighted supplier execution quality.',
          tone: agg.totals.weightedSupplierOTD >= 0.92 ? 'positive' : (agg.totals.weightedSupplierOTD >= 0.88 ? 'watch' : 'critical'),
        },
        {
          label: 'High-Risk SKU Count',
          value: fmtNum(highRiskSkuCount),
          context: 'High + critical governed intervention items.',
          tone: highRiskSkuCount <= 20 ? 'positive' : (highRiskSkuCount <= 50 ? 'watch' : 'critical'),
        },
      ];

      const grid = document.getElementById('kpi-grid');
      grid.innerHTML = kpis.map(k => `
        <div class="kpi kpi-${k.tone}">
          <div class="label">${k.label}</div>
          <div class="value">${k.value}</div>
          <div class="context">${k.context}</div>
        </div>
      `).join('');
    }

    function renderCallouts(callouts) {
      document.getElementById('callout-grid').innerHTML = callouts.map(c => `
        <div class="callout callout-${c.tone}">
          <div class="callout-eyebrow">${c.eyebrow}</div>
          <div class="callout-title">${c.title}</div>
          <div class="callout-body">${c.body}</div>
        </div>
      `).join('');
    }

    function chartLayout(title) {
      const c = getThemePalette();
      return {
        title: { text: title, x: 0.01, xanchor: 'left', font: { size: 15, color: c.title } },
        margin: { l: 68, r: 20, t: 58, b: 58 },
        paper_bgcolor: c.paper,
        plot_bgcolor: c.plot,
        font: { family: 'IBM Plex Sans, Avenir Next, Source Sans 3, Segoe UI, sans-serif', size: 12, color: c.font },
        xaxis: { gridcolor: c.grid, zerolinecolor: c.zero, automargin: true, tickangle: 0, tickfont: { size: 11 } },
        yaxis: { gridcolor: c.grid, zerolinecolor: c.zero, automargin: true, tickfont: { size: 11 } },
        showlegend: false,
        hoverlabel: { bgcolor: c.hoverBg, font: { color: '#ffffff' } },
        dragmode: false
      };
    }

    function renderCharts(agg) {
      const monthSeries = agg.monthSeries;
      const c = getThemePalette();

      Plotly.react('chart-service-trend', [{
        x: monthSeries.map(d => d.month),
        y: monthSeries.map(d => d.fill_rate),
        mode: 'lines+markers',
        line: { color: c.service, width: 2.5 },
        marker: { size: 6 },
        hovertemplate: 'Month %{x}<br>Fill Rate %{y:.1%}<extra></extra>'
      }], { ...chartLayout('Service Level Trend'), hovermode: 'x unified', xaxis: { gridcolor: c.grid, nticks: 7 }, yaxis: { tickformat: '.0%', gridcolor: c.grid, nticks: 6 } }, PLOT_CONFIG);

      Plotly.react('chart-stockout-trend', [{
        x: monthSeries.map(d => d.month),
        y: monthSeries.map(d => d.stockout_rate),
        mode: 'lines+markers',
        line: { color: c.stockout, width: 2.5 },
        marker: { size: 6 },
        hovertemplate: 'Month %{x}<br>Stockout %{y:.1%}<extra></extra>'
      }], { ...chartLayout('Stockout Rate Trend'), hovermode: 'x unified', xaxis: { gridcolor: c.grid, nticks: 7 }, yaxis: { tickformat: '.0%', gridcolor: c.grid, nticks: 6 } }, PLOT_CONFIG);

      Plotly.react('chart-lost-sales-trend', [{
        x: monthSeries.map(d => d.month),
        y: monthSeries.map(d => d.lostSales),
        type: 'bar',
        marker: { color: c.lostSales },
        hovertemplate: 'Month %{x}<br>Lost Sales %{y:$,.0f}<extra></extra>'
      }], { ...chartLayout('Lost Sales Exposure Trend'), hovermode: 'x unified', xaxis: { gridcolor: c.grid, nticks: 7 }, yaxis: { tickprefix: 'EUR ', separatethousands: true, gridcolor: c.grid, nticks: 6 } }, PLOT_CONFIG);

      Plotly.react('chart-inventory-trend', [{
        x: monthSeries.map(d => d.month),
        y: monthSeries.map(d => d.inventory),
        type: 'bar',
        marker: { color: c.inventory },
        hovertemplate: 'Month %{x}<br>Inventory %{y:$,.0f}<extra></extra>'
      }], { ...chartLayout('Inventory Value Trend'), hovermode: 'x unified', xaxis: { gridcolor: c.grid, nticks: 7 }, yaxis: { tickprefix: 'EUR ', separatethousands: true, gridcolor: c.grid, nticks: 6 } }, PLOT_CONFIG);

      const wh = [...agg.warehouses].sort((a,b)=>a.fill_rate-b.fill_rate);
      const whLabels = wh.map(d=>ellipsize(d.warehouse_name, 26));
      const whMargin = dynamicLeftMargin(whLabels, 128, 230, 6.8);
      Plotly.react('chart-fill-warehouse', [{
        y: whLabels,
        x: wh.map(d=>d.fill_rate),
        type:'bar',
        orientation:'h',
        marker:{color:c.warehouse},
        hovertemplate: '%{customdata}<br>Fill Rate %{x:.1%}<extra></extra>',
        customdata: wh.map(d=>d.warehouse_name)
      }], {
        ...chartLayout('Fill Rate by Warehouse'),
        xaxis:{tickformat:'.0%', gridcolor:c.grid, nticks: 5},
        yaxis:{gridcolor:c.grid, automargin:true, tickfont:{size:11}},
        margin:{l:whMargin, r:20, t:52, b:58}
      }, PLOT_CONFIG);

      const cat = [...agg.categories].sort((a,b)=>a.fill_rate-b.fill_rate);
      const catLabels = cat.map(d=>ellipsize(d.category, 24));
      Plotly.react('chart-fill-category', [{
        y: catLabels,
        x: cat.map(d=>d.fill_rate),
        type:'bar',
        orientation:'h',
        marker:{color:c.category},
        hovertemplate: '%{customdata}<br>Fill Rate %{x:.1%}<extra></extra>',
        customdata: cat.map(d=>d.category)
      }], { ...chartLayout('Fill Rate by Category'), xaxis:{tickformat:'.0%', gridcolor:c.grid, nticks: 5}, margin:{l: dynamicLeftMargin(catLabels, 124, 220, 6.5), r:20, t:52, b:58} }, PLOT_CONFIG);

      const reg = [...agg.regions].sort((a,b)=>b.lostSales-a.lostSales);
      Plotly.react('chart-lostsales-region', [{
        y: reg.map(d=>ellipsize(d.region, 26)),
        x: reg.map(d=>d.lostSales),
        type:'bar',
        orientation:'h',
        marker:{color:c.region},
        hovertemplate: '%{customdata}<br>Lost Sales %{x:$,.0f}<extra></extra>',
        customdata: reg.map(d=>d.region)
      }], { ...chartLayout('Lost Sales Exposure by Region'), xaxis:{tickprefix:'EUR ', separatethousands:true, gridcolor:c.grid, nticks: 5} }, PLOT_CONFIG);

      const sup = [...agg.suppliers].sort((a,b)=>a.on_time_delivery_rate-b.on_time_delivery_rate);
      const supLabels = sup.map(d=>ellipsize(d.supplier_name, 24));
      Plotly.react('chart-supplier-otd', [{
        y: supLabels,
        x: sup.map(d=>d.on_time_delivery_rate),
        type:'bar',
        orientation:'h',
        marker:{color:c.supplierOtd},
        hovertemplate: '%{customdata}<br>OTD %{x:.1%}<extra></extra>',
        customdata: sup.map(d=>d.supplier_name)
      }], { ...chartLayout('Supplier On-Time Delivery Comparison'), xaxis:{tickformat:'.0%', gridcolor:c.grid, nticks: 5}, margin:{l: dynamicLeftMargin(supLabels, 126, 225, 6.5), r:20, t:52, b:58} }, PLOT_CONFIG);

      const supVar = [...agg.suppliers].sort((a,b)=>b.lead_time_variability-a.lead_time_variability);
      Plotly.react('chart-lead-var', [{
        y: supVar.map(d=>ellipsize(d.supplier_name, 24)),
        x: supVar.map(d=>d.lead_time_variability),
        type:'bar',
        orientation:'h',
        marker:{color:c.leadVar},
        hovertemplate: '%{customdata}<br>Lead Time Variability %{x:.2f}<extra></extra>',
        customdata: supVar.map(d=>d.supplier_name)
      }], { ...chartLayout('Lead Time Variability by Supplier'), xaxis:{gridcolor:c.grid, nticks: 5}, margin:{l: dynamicLeftMargin(supVar.map(d=>ellipsize(d.supplier_name, 24)), 126, 225, 6.5), r:20, t:52, b:58} }, PLOT_CONFIG);

      const catEx = [...agg.categories].sort((a,b)=>b.excess-a.excess);
      Plotly.react('chart-excess-category', [{
        y: catEx.map(d=>ellipsize(d.category, 24)),
        x: catEx.map(d=>d.excess),
        type:'bar',
        orientation:'h',
        marker:{color:c.excess},
        hovertemplate: '%{customdata}<br>Excess Proxy %{x:$,.0f}<extra></extra>',
        customdata: catEx.map(d=>d.category)
      }], { ...chartLayout('Excess Inventory Exposure by Category'), xaxis:{tickprefix:'EUR ', separatethousands:true, gridcolor:c.grid, nticks: 5}, margin:{l: dynamicLeftMargin(catEx.map(d=>ellipsize(d.category, 24)), 124, 220, 6.5), r:20, t:52, b:58} }, PLOT_CONFIG);

      const segments = agg.segments;
      const segmentColorScale = currentTheme === 'dark'
        ? [[0, '#24465a'], [0.55, '#4f8ca7'], [1, '#96e0df']]
        : [[0, '#cbe8de'], [0.55, '#5ea8a0'], [1, '#1f6f78']];
      Plotly.react('chart-service-vs-inventory', [{
        x: segments.map(s=>s.inventory),
        y: segments.map(s=>s.fill_rate),
        mode:'markers',
        marker:{
          size: segments.map(s=>Math.max(10, Math.min(30, s.lostSales / 220000))),
          color: segments.map(s=>s.lostSales),
          colorscale:segmentColorScale,
          showscale:true,
          colorbar:{
            title:{text:'Lost Sales', font:{color:c.font}},
            thickness: 11,
            tickfont:{color:c.font}
          }
        },
        customdata: segments.map(s=>`${s.category} | ${s.region}`),
        hovertemplate: '%{customdata}<br>Inventory %{x:$,.0f}<br>Fill %{y:.1%}<br>Lost Sales %{marker.color:$,.0f}<extra></extra>'
      }], {
        ...chartLayout('Service Level vs Inventory Value (By Category-Region)'),
        yaxis:{tickformat:'.0%', gridcolor:c.grid, automargin:true, nticks: 6},
        xaxis:{tickprefix:'EUR ', separatethousands:true, gridcolor:c.grid, automargin:true, nticks: 6}
      }, PLOT_CONFIG);

      Plotly.react('chart-service-vs-dos', [{
        x: segments.map(s=>s.avg_dos),
        y: segments.map(s=>s.fill_rate),
        mode:'markers',
        marker:{ size: 9, color:c.category, opacity: 0.82 },
        customdata: segments.map(s=>`${s.category} | ${s.region}`),
        hovertemplate: '%{customdata}<br>DOS %{x:.1f}<br>Fill %{y:.1%}<extra></extra>'
      }], {
        ...chartLayout('Service Level vs Days of Supply'),
        yaxis:{tickformat:'.0%', gridcolor:c.grid, automargin:true, nticks: 6},
        xaxis:{automargin:true, nticks: 6}
      }, PLOT_CONFIG);

      const dosMedian = wh.reduce((acc,d)=>acc+d.avg_dos,0)/Math.max(wh.length,1);
      const fillMedian = wh.reduce((acc,d)=>acc+d.fill_rate,0)/Math.max(wh.length,1);
      const rankedWh = [...wh].sort((a,b)=>b.stockout_rate-a.stockout_rate);
      const annotatedWh = rankedWh.slice(0, Math.min(4, rankedWh.length)).map(d => d.warehouse_id);
      Plotly.react('chart-quadrant', [{
        x: wh.map(d=>d.avg_dos),
        y: wh.map(d=>d.fill_rate),
        mode:'markers+text',
        text: wh.map(d=>annotatedWh.includes(d.warehouse_id) ? d.warehouse_id : ''),
        customdata: wh.map(d=>d.warehouse_id),
        textposition:'top center',
        textfont:{size:10, color:c.font},
        marker:{ size: wh.map(d=>Math.max(14, Math.min(48, d.lostSales / 350000))), color:c.quadrant, opacity:0.8 },
        hovertemplate: '%{customdata}<br>DOS %{x:.1f}<br>Fill %{y:.1%}<extra></extra>'
      }], {
        ...chartLayout('Warehouse Service vs Working-Capital Quadrant'),
        yaxis:{tickformat:'.0%', gridcolor:c.grid, nticks: 6},
        xaxis:{gridcolor:c.grid, nticks: 6},
        shapes:[
          {type:'line', x0:dosMedian, x1:dosMedian, y0:0, y1:1, yref:'paper', line:{dash:'dash', color:c.lineRef}},
          {type:'line', x0:0, x1:1, xref:'paper', y0:fillMedian, y1:fillMedian, line:{dash:'dash', color:c.lineRef}}
        ],
        annotations:[
          {x:0.22, y:0.94, xref:'paper', yref:'paper', text:'Service-Healthy / Lean', showarrow:false, font:{size:10, color:c.annGood}},
          {x:0.78, y:0.94, xref:'paper', yref:'paper', text:'Service-Healthy / Capital-Heavy', showarrow:false, font:{size:10, color:c.inventory}},
          {x:0.22, y:0.06, xref:'paper', yref:'paper', text:'Understocked Risk', showarrow:false, font:{size:10, color:c.annBad}},
          {x:0.78, y:0.06, xref:'paper', yref:'paper', text:'Dual Failure', showarrow:false, font:{size:10, color:c.annBad}}
        ]
      }, PLOT_CONFIG);

      const topGov = agg.skuRows.slice(0, 15).reverse();
      Plotly.react('chart-top-governance', [{
        y: topGov.map(d=>`${d.product_id} | ${d.warehouse_id}`),
        x: topGov.map(d=>d.governance_priority_score),
        type:'bar',
        orientation:'h',
        marker:{color:c.governance},
        hovertemplate: '%{y}<br>Governance Score %{x:.1f}<extra></extra>'
      }], { ...chartLayout('Top Governance Priority SKUs'), xaxis:{gridcolor:c.grid, nticks: 5}, margin:{l: 162, r:20, t:52, b:58} }, PLOT_CONFIG);

      const topSup = [...agg.suppliers].sort((a,b)=>b.stockout_rate-a.stockout_rate).slice(0,12).reverse();
      Plotly.react('chart-top-suppliers', [{
        y: topSup.map(d=>d.supplier_name),
        x: topSup.map(d=>d.stockout_rate),
        type:'bar',
        orientation:'h',
        marker:{color:c.supplierRisk},
        hovertemplate: '%{y}<br>Downstream Stockout %{x:.1%}<extra></extra>'
      }], { ...chartLayout('Highest-Risk Suppliers (Downstream Stockout)'), xaxis:{tickformat:'.0%', gridcolor:c.grid, nticks: 5}, margin:{l: 150, r:20, t:52, b:58} }, PLOT_CONFIG);

      const topWh = [...agg.warehouses].sort((a,b)=>b.stockout_rate-a.stockout_rate).slice(0, 12).reverse();
      Plotly.react('chart-top-warehouses', [{
        y: topWh.map(d=>d.warehouse_name),
        x: topWh.map(d=>d.stockout_rate),
        type:'bar',
        orientation:'h',
        marker:{color:c.warehouseRisk},
        hovertemplate: '%{y}<br>Stockout Rate %{x:.1%}<extra></extra>'
      }], { ...chartLayout('Highest-Risk Warehouses (Stockout Rate)'), xaxis:{tickformat:'.0%', gridcolor:c.grid, nticks: 5}, margin:{l: 160, r:20, t:52, b:58} }, PLOT_CONFIG);

      const heatSup = [...agg.suppliers].sort((a,b)=>b.supplier_service_risk_proxy-a.supplier_service_risk_proxy).slice(0,10);
      const heatmapScale = currentTheme === 'dark'
        ? [[0, '#173042'], [0.45, '#5a7e95'], [1, '#d07f68']]
        : [[0, '#e6eef3'], [0.45, '#8caab8'], [1, '#ba604b']];
      const z = [
        heatSup.map(s=>1 - s.on_time_delivery_rate),
        heatSup.map(s=>s.average_delay_days / Math.max(...heatSup.map(x=>x.average_delay_days), 1)),
        heatSup.map(s=>s.lead_time_variability / Math.max(...heatSup.map(x=>x.lead_time_variability), 1)),
        heatSup.map(s=>s.supplier_service_risk_proxy / Math.max(...heatSup.map(x=>x.supplier_service_risk_proxy), 1)),
      ];
      Plotly.react('chart-supplier-heatmap', [{
        z,
        x: heatSup.map(s=>s.supplier_name),
        y: ['OTD Gap', 'Delay Severity', 'Lead-Time Volatility', 'Composite Risk'],
        type: 'heatmap',
        colorscale: heatmapScale,
        colorbar: {
          tickfont: { color: c.font },
        },
        hovertemplate: '%{y}<br>%{x}<br>Intensity %{z:.2f}<extra></extra>'
      }], {
        ...chartLayout('Supplier-Risk Heatmap (Relative Intensity)'),
        margin:{l:132, r:20, t:52, b:132},
        xaxis:{tickangle:-28, automargin:true, tickfont:{size:10}},
        yaxis:{automargin:true, tickfont:{size:11}}
      }, PLOT_CONFIG);
    }

    function renderTable(rows) {
      const q = (tableSearch.value || '').toLowerCase().trim();
      let data = rows.filter(r => {
        if (!q) return true;
        const blob = `${r.product_id} ${r.product_name} ${r.warehouse_id} ${r.supplier_id} ${r.main_risk_driver} ${r.recommended_action}`.toLowerCase();
        return blob.includes(q);
      });

      data.sort((a,b) => {
        const key = tableSort.key;
        const dir = tableSort.dir === 'asc' ? 1 : -1;
        const av = a[key];
        const bv = b[key];
        if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
        return String(av).localeCompare(String(bv)) * dir;
      });

      const maxRows = 250;
      const shown = data.slice(0, maxRows);

      tableBody.innerHTML = shown.map(r => {
        const tierClass = `risk-${r.risk_tier.toLowerCase()}`;
        return `<tr>
          <td class="entity-cell">${r.product_id}</td>
          <td class="action-cell">${r.product_name}</td>
          <td>${r.warehouse_id}</td>
          <td>${r.supplier_id}</td>
          <td>${fmtPct(r.fill_rate)}</td>
          <td>${r.stockout_risk_score.toFixed(1)}</td>
          <td>${r.excess_inventory_score.toFixed(1)}</td>
          <td>${r.working_capital_risk_score.toFixed(1)}</td>
          <td class="priority-cell">${r.governance_priority_score.toFixed(1)}</td>
          <td><span class="risk-badge ${tierClass}">${r.risk_tier}</span></td>
          <td>${r.main_risk_driver}</td>
          <td class="action-cell">${r.recommended_action}</td>
        </tr>`;
      }).join('');

      tableMeta.textContent = `Showing ${shown.length.toLocaleString()} of ${data.length.toLocaleString()} filtered rows (cap ${maxRows}).`;
    }

    function renderNarrative(agg) {
      const topWarehouse = [...agg.warehouses].sort((a,b)=>a.fill_rate-b.fill_rate)[0];
      const topSupplier = [...agg.suppliers].sort((a,b)=>b.stockout_rate-a.stockout_rate)[0];
      const topCategoryExcess = [...agg.categories].sort((a,b)=>b.excess-a.excess)[0];
      const topSku = agg.skuRows[0];
      const balancedShare = computeBalancedShare(agg);
      const html = `
        <div class="brief-grid">
          <div class="brief-item">
            <div class="brief-label">What is off plan</div>
            <div class="brief-copy">Fill rate is <strong>${fmtPct(agg.totals.fillRate)}</strong> while stockout rate is <strong>${fmtPct(agg.totals.stockoutRate)}</strong>. Service is still leaking demand and should not be treated as a transient fluctuation.</div>
          </div>
          <div class="brief-item">
            <div class="brief-label">Where exposure sits</div>
            <div class="brief-copy">Pressure is concentrated in <strong>${topWarehouse ? topWarehouse.warehouse_name : 'n/a'}</strong>, supplier instability is led by <strong>${topSupplier ? topSupplier.supplier_name : 'n/a'}</strong>, and excess value is most visible in <strong>${topCategoryExcess ? topCategoryExcess.category : 'n/a'}</strong>.</div>
          </div>
          <div class="brief-item">
            <div class="brief-label">What leadership should do first</div>
            <div class="brief-copy">Start with <strong>${topSku ? `${topSku.product_id} @ ${topSku.warehouse_id}` : 'the top filtered SKU'}</strong>, then address the supplier and warehouse policies driving the highest combined service and capital penalty.</div>
          </div>
          <div class="brief-item">
            <div class="brief-label">Trade-off to manage</div>
            <div class="brief-copy">Only <strong>${fmtPct(balancedShare)}</strong> of filtered SKU-warehouse positions sit in a balanced zone. The objective is targeted service recovery without broad inventory expansion.</div>
          </div>
        </div>
      `;

      document.getElementById('narrative-panel').innerHTML = html;
    }

    function renderConsistencyAlert(agg, dateRange) {
      const alert = document.getElementById('consistency-alert');
      const fullRange = (
        dateRange.start === `${dashboardData.meta.date_min.slice(0, 7)}-01` &&
        dateRange.end === `${dashboardData.meta.date_max.slice(0, 7)}-01`
      );
      const allFilters = (
        filters.region.value === 'ALL' &&
        filters.warehouse.value === 'ALL' &&
        filters.category.value === 'ALL' &&
        filters.supplier.value === 'ALL' &&
        filters.abc.value === 'ALL'
      );
      if (!allFilters || !fullRange) {
        alert.style.display = 'none';
        alert.textContent = '';
        return;
      }

      const snap = dashboardData.meta.official_snapshot || {};
      const fillDiff = Math.abs((snap.overall_fill_rate || 0) - agg.totals.fillRate);
      const stockoutDiff = Math.abs((snap.overall_stockout_rate || 0) - agg.totals.stockoutRate);
      const lostDiff = Math.abs((snap.total_lost_sales_revenue || 0) - agg.totals.totalLostSales);
      const mismatch = fillDiff > 0.0005 || stockoutDiff > 0.0005 || lostDiff > 1;

      if (mismatch) {
        alert.style.display = 'block';
        alert.textContent = 'Dashboard QA warning: default filter KPIs do not reconcile to official governed snapshot. Review dashboard build inputs before distribution.';
      } else {
        alert.style.display = 'none';
        alert.textContent = '';
      }
    }

    function renderNoDataAlert(hasRows) {
      const alert = document.getElementById('no-data-alert');
      if (!alert) return;
      if (hasRows) {
        alert.style.display = 'none';
        alert.textContent = '';
        return;
      }
      alert.style.display = 'block';
      alert.textContent = 'No records match the current filter combination. Reset filters or widen the date range.';
    }

    function updateDashboard() {
      const dateRange = getNormalizedDateRange();
      const filteredRows = monthlyFact.filter(r => passesFilter(r, dateRange));
      renderNoDataAlert(filteredRows.length > 0);
      const agg = aggregate(filteredRows, readAssumptions());
      lastAgg = agg;
      renderHeaderSummary(agg, dateRange);
      renderKPIs(agg);
      renderCallouts(buildCallouts(agg));
      renderCharts(agg);
      renderTable(agg.skuRows);
      renderNarrative(agg);
      renderConsistencyAlert(agg, dateRange);
    }

    function initTableSorting() {
      const headers = document.querySelectorAll('#detail-table thead th');
      headers.forEach(h => {
        h.addEventListener('click', () => {
          const key = h.dataset.key;
          if (!key) return;
          if (tableSort.key === key) {
            tableSort.dir = tableSort.dir === 'asc' ? 'desc' : 'asc';
          } else {
            tableSort.key = key;
            tableSort.dir = 'desc';
          }
          if (lastAgg) {
            renderTable(lastAgg.skuRows);
          }
        });
      });
    }

    function initEvents() {
      Object.values(filters).forEach(el => el.addEventListener('change', updateDashboard));
      tableSearch.addEventListener('input', () => {
        if (lastAgg) {
          renderTable(lastAgg.skuRows);
        }
      });
      [assumptions.marginRate, assumptions.wcRate, assumptions.slowWeight].forEach(el => {
        el.addEventListener('input', () => {
          updateAssumptionLabels();
          updateDashboard();
        });
      });

      if (methodologyToggle) {
        methodologyToggle.setAttribute('aria-expanded', 'false');
        methodologyToggle.addEventListener('click', () => {
          togglePanel('methodology-panel', methodologyToggle, 'Method Notes', 'Hide Method Notes');
        });
      }
      if (assumptionToggle) {
        assumptionToggle.setAttribute('aria-expanded', 'false');
        assumptionToggle.addEventListener('click', () => {
          togglePanel('assumption-panel', assumptionToggle, 'Scenario Controls', 'Hide Scenario Controls');
        });
      }
      if (themeToggle) {
        themeToggle.addEventListener('click', () => {
          const nextTheme = currentTheme === 'dark' ? 'light' : 'dark';
          applyTheme(nextTheme);
        });
      }
      if (printButton) {
        printButton.addEventListener('click', () => {
          window.print();
        });
      }
      if (resetButton) {
        resetButton.addEventListener('click', resetFilters);
      }

      window.addEventListener('resize', () => {
        if (!lastAgg) return;
        [
          'chart-service-trend', 'chart-stockout-trend', 'chart-lost-sales-trend', 'chart-inventory-trend',
          'chart-fill-warehouse', 'chart-fill-category', 'chart-lostsales-region', 'chart-supplier-otd',
          'chart-lead-var', 'chart-excess-category', 'chart-service-vs-inventory', 'chart-service-vs-dos',
          'chart-quadrant', 'chart-top-governance', 'chart-top-suppliers', 'chart-top-warehouses',
          'chart-supplier-heatmap'
        ].forEach((id) => {
          const el = document.getElementById(id);
          if (el) {
            Plotly.Plots.resize(el);
          }
        });
      });
    }

    populateFilters();
    initializeAssumptions();
    applyTheme(getPreferredTheme(), false);
    initTableSorting();
    initEvents();
    updateDashboard();
  </script>
</body>
</html>
"""

    return (
        template.replace("__PLOTLY_JS__", plotly_js)
        .replace("__DATA_JSON__", data_json)
    )


def build_executive_dashboard() -> Path:
    data_payload = _prepare_dashboard_data()
    html = _build_html(data_payload)
    OUTPUT_DASHBOARD_FILE.write_text(html, encoding="utf-8")
    DOCS_DASHBOARD_ENTRY.parent.mkdir(parents=True, exist_ok=True)
    DOCS_DASHBOARD_ENTRY.write_text(
        """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=../index.html" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Supply Chain Dashboard Redirect</title>
  </head>
  <body>
    <p>Redirecting to dashboard...</p>
    <p>If redirect does not work, open <a href="../index.html">../index.html</a>.</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    build_info = pd.DataFrame(
        [
            {
                "dashboard_version": data_payload["dashboard_version"],
                "generated_at_utc": data_payload["generated_at"],
                "html_path": str(OUTPUT_DASHBOARD_FILE),
                "html_size_bytes": OUTPUT_DASHBOARD_FILE.stat().st_size,
                "html_sha256": _sha256_for_file(OUTPUT_DASHBOARD_FILE),
                "dataset_hash": data_payload["meta"]["dataset_hash"],
            }
        ]
    )
    build_info.to_csv(OUTPUT_TABLES_DIR / "dashboard_release_manifest.csv", index=False)

    return OUTPUT_DASHBOARD_FILE


def main() -> None:
    output_path = build_executive_dashboard()
    print(f"Executive dashboard generated: {output_path}")


if __name__ == "__main__":
    main()
