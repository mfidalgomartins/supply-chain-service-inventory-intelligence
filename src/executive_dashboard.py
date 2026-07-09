from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT  # type: ignore[no-redef]


OUTPUT_DASHBOARD_FILE = PROJECT_ROOT / "index.html"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
PLOTLY_CDN_URL = "https://cdn.plot.ly/plotly-3.5.0.min.js"
# Subresource Integrity hash for the pinned bundle. The browser refuses to run
# the script if the fetched bytes do not match, protecting the published
# dashboard against a tampered or substituted CDN payload. Regenerate with:
#   curl -sSL <PLOTLY_CDN_URL> | openssl dgst -sha384 -binary | openssl base64 -A
PLOTLY_SRI = "sha384-DPvk2KODrsA0CfBr4HTwAwhdDROPqqK2PvSSswJMQMpnUkwSTg4gLBxXc3wv2e5L"


def _prepare_dashboard_data() -> dict:
    daily = pd.read_csv(
        DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"]
    )
    products = pd.read_csv(DATA_RAW / "products.csv")[
        ["product_id", "product_name", "unit_cost", "unit_price"]
    ]
    suppliers = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")
    warehouses = pd.read_csv(DATA_RAW / "warehouses.csv")[
        ["warehouse_id", "warehouse_name", "region"]
    ]
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
    daily["excess_inventory_proxy"] = daily["inventory_value"] * (
        (daily["days_of_supply"] - daily["dos_cap"]).clip(lower=0)
        / daily["days_of_supply"].clip(lower=1e-9)
    )
    daily["slow_moving_proxy"] = np.where(
        (daily["available_units"] > 0) & (daily["units_fulfilled"] == 0),
        daily["inventory_value"],
        0.0,
    )
    daily["slow_moving_non_excess_proxy"] = (
        daily["slow_moving_proxy"] - daily["excess_inventory_proxy"]
    ).clip(lower=0)
    daily["trapped_wc_proxy"] = (
        daily["excess_inventory_proxy"] + 0.5 * daily["slow_moving_non_excess_proxy"]
    )
    daily["lost_sales_margin_proxy"] = daily["lost_sales_revenue"] * daily["gross_margin_rate"]

    monthly_sku = daily.groupby(
        ["month", "region", "warehouse_id", "product_id", "category", "supplier_id", "abc_class"],
        as_index=False,
    ).agg(
        units_demanded=("units_demanded", "sum"),
        units_fulfilled=("units_fulfilled", "sum"),
        units_lost_sales=("units_lost_sales", "sum"),
        lost_sales_revenue=("lost_sales_revenue", "sum"),
        inventory_value=("inventory_value", "mean"),
        avg_days_of_supply=("days_of_supply", "mean"),
        excess_inventory_proxy=("excess_inventory_proxy", "mean"),
        slow_moving_proxy=("slow_moving_proxy", "mean"),
        slow_moving_non_excess_proxy=("slow_moving_non_excess_proxy", "mean"),
        trapped_wc_proxy=("trapped_wc_proxy", "mean"),
        lost_sales_margin_proxy=("lost_sales_margin_proxy", "sum"),
        observation_days=("date", "nunique"),
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
        "slow_moving_non_excess_proxy",
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

    impact_overall_path = OUTPUT_TABLES_DIR / "impact_overall_summary.csv"
    total_demand = float(daily["units_demanded"].sum())
    total_fulfilled = float(daily["units_fulfilled"].sum())
    total_lost = float(daily["units_lost_sales"].sum())
    overall_kpi = {
        "overall_fill_rate": total_fulfilled / total_demand if total_demand > 0 else 1.0,
        "overall_stockout_rate": total_lost / total_demand if total_demand > 0 else 0.0,
        "total_lost_sales_revenue": float(daily["lost_sales_revenue"].sum()),
    }

    if impact_overall_path.exists():
        impact_overall = pd.read_csv(impact_overall_path)
        impact_map = dict(zip(impact_overall["metric"], impact_overall["value"], strict=False))
    else:
        impact_map = {}

    snapshot = {
        "overall_fill_rate": float(overall_kpi.get("overall_fill_rate", 0.0)),
        "overall_stockout_rate": float(overall_kpi.get("overall_stockout_rate", 0.0)),
        "total_lost_sales_revenue": float(overall_kpi.get("total_lost_sales_revenue", 0.0)),
        "trapped_working_capital_proxy_average": float(
            impact_map.get("trapped_working_capital_proxy_average", 0.0)
        ),
        "opportunity_total_12m_proxy": float(impact_map.get("opportunity_total_12m_proxy", 0.0)),
    }

    product_dim = products[["product_id", "product_name"]].copy()
    product_dim["product_name"] = product_dim["product_name"].fillna(product_dim["product_id"])

    hash_seed = monthly_sku.sort_values(["month", "warehouse_id", "product_id"])[
        [
            "month",
            "warehouse_id",
            "product_id",
            "units_demanded",
            "units_fulfilled",
            "lost_sales_revenue",
        ]
    ].to_csv(index=False).encode("utf-8") + sku_baseline.sort_values(
        ["product_id", "warehouse_id", "supplier_id"]
    )[["product_id", "warehouse_id", "supplier_id", "governance_priority_score"]].to_csv(
        index=False
    ).encode("utf-8")
    dataset_hash = hashlib.sha256(hash_seed).hexdigest()
    dashboard_version = f"v{dataset_hash[:12]}"
    data_through = daily["date"].max().strftime("%Y-%m-%d")

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
        "slow_moving_non_excess_proxy",
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
                float(r.slow_moving_non_excess_proxy),
                float(r.trapped_wc_proxy),
                float(r.lost_sales_margin_proxy),
                int(r.observation_days),
                int(r.stockout_month_flag),
            ]
        )

    product_name_map = dict(
        product_dim[["product_id", "product_name"]].itertuples(index=False, name=None)
    )

    data_payload = {
        "generated_at": f"Data through {data_through}",
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
    pd.DataFrame([snapshot]).to_csv(
        OUTPUT_TABLES_DIR / "dashboard_official_snapshot.csv", index=False
    )

    return data_payload


def _stabilize_floats(obj: object, ndigits: int = 6) -> object:
    """Round every float in the payload to a fixed precision.

    Floating-point summation order differs across Python and NumPy versions,
    which would otherwise leak non-deterministic trailing digits (e.g.
    ``20281573.240000002`` vs ``20281573.24``) into the published dashboard and
    break the CI freshness check. Rounding before serialisation makes the output
    byte-identical across environments without affecting the dataset hash, which
    is computed upstream from the raw processed data.
    """
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, dict):
        return {key: _stabilize_floats(value, ndigits) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_stabilize_floats(value, ndigits) for value in obj]
    return obj


def _build_html(data_payload: dict) -> str:
    data_json = json.dumps(
        _stabilize_floats(data_payload), ensure_ascii=False, separators=(",", ":")
    )

    template = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="color-scheme" content="light dark" />
  <meta name="description" content="Reproducible decision-support review of service level, stockout, supplier reliability, and inventory risk across a multi-warehouse network." />
  <meta name="theme-color" media="(prefers-color-scheme: light)" content="#f5f5f7" />
  <meta name="theme-color" media="(prefers-color-scheme: dark)" content="#000000" />
  <meta property="og:type" content="website" />
  <meta property="og:title" content="Service &amp; Inventory Intelligence — Operating Review" />
  <meta property="og:description" content="Where the network loses service, where capital sits idle, and the SKU-location actions that recover the most value over the next 12 months." />
  <meta property="og:url" content="https://mfidalgomartins.github.io/supply-chain-service-inventory-intelligence/" />
  <meta property="og:image" content="https://mfidalgomartins.github.io/supply-chain-service-inventory-intelligence/outputs/graphs/01_opportunity_by_category.png" />
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="Service &amp; Inventory Intelligence — Operating Review" />
  <meta name="twitter:description" content="Where the network loses service, where capital sits idle, and the SKU-location actions that recover the most value over the next 12 months." />
  <meta name="twitter:image" content="https://mfidalgomartins.github.io/supply-chain-service-inventory-intelligence/outputs/graphs/01_opportunity_by_category.png" />
  <link rel="icon" href="data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%2032%2032'%3E%3Crect%20width='32'%20height='32'%20rx='8'%20fill='%230071e3'/%3E%3Ctext%20x='16'%20y='22'%20font-size='15'%20font-weight='600'%20fill='white'%20font-family='-apple-system,Helvetica,Arial,sans-serif'%20text-anchor='middle'%3ESI%3C/text%3E%3C/svg%3E" />
  <title>Service &amp; Inventory Intelligence — Operating Review</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Geist:wght@400;450;500;600;700&family=Geist+Mono:wght@400;500;600&display=swap" />
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f5f7;
      --surface: #ffffff;
      --surface-soft: #fbfbfd;
      --inset: #f0f0f3;
      --ink: #1d1d1f;
      --ink-soft: #424245;
      --muted: #6d6d72;
      --faint: #707073;
      --border: #e6e6eb;
      --border-strong: #d2d2d7;
      --accent: #0071e3;
      --accent-ink: #ffffff;
      --accent-soft: #e8f1fd;
      --good: #107d41;
      --warn: #976200;
      --bad: #d70015;
      --good-soft: #e6f4ec;
      --warn-soft: #fbf0db;
      --bad-soft: #fdeceb;
      --shadow: 0 1px 2px rgba(0, 0, 0, 0.04), 0 4px 14px -8px rgba(0, 0, 0, 0.10);
      --shadow-pop: 0 18px 50px -22px rgba(0, 0, 0, 0.30);
      --radius: 16px;
      --radius-sm: 10px;
      --radius-lg: 22px;
      --ease: cubic-bezier(0.4, 0, 0.2, 1);
      --spring: cubic-bezier(0.34, 1.4, 0.5, 1);
      --font: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Geist", "Inter", "Segoe UI", system-ui, sans-serif;
      --mono: ui-monospace, "SF Mono", "SFMono-Regular", "Geist Mono", "Roboto Mono", Menlo, monospace;
    }

    [data-theme="dark"] {
      color-scheme: dark;
      --bg: #000000;
      --surface: #1c1c1e;
      --surface-soft: #232325;
      --inset: #2c2c2e;
      --ink: #f5f5f7;
      --ink-soft: #d6d6da;
      --muted: #98989d;
      --faint: #838387;
      --border: #303032;
      --border-strong: #404043;
      --accent: #0a84ff;
      --accent-ink: #ffffff;
      --accent-soft: #11243d;
      --good: #30d158;
      --warn: #ff9f0a;
      --bad: #ff453a;
      --good-soft: #122b1c;
      --warn-soft: #2e2410;
      --bad-soft: #331613;
      --shadow: 0 1px 2px rgba(0, 0, 0, 0.5), 0 8px 28px -14px rgba(0, 0, 0, 0.7);
      --shadow-pop: 0 22px 56px -22px rgba(0, 0, 0, 0.85);
    }

    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--font);
      font-size: 15px;
      line-height: 1.5;
      letter-spacing: -0.005em;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }

    a { color: inherit; }
    button, input, select { font: inherit; color: inherit; }
    ::selection { background: color-mix(in srgb, var(--accent) 26%, transparent); }
    :focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
      border-radius: 4px;
    }
    .num { font-family: var(--mono); font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; letter-spacing: -0.01em; }

    .skip-link {
      position: absolute;
      top: -48px;
      left: 16px;
      z-index: 60;
      padding: 9px 13px;
      background: var(--surface);
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      box-shadow: var(--shadow);
    }
    .skip-link:focus { top: 12px; }
    .sr-only {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }

    /* ---------- Masthead ---------- */
    .masthead {
      position: sticky;
      top: 0;
      z-index: 50;
      background: color-mix(in srgb, var(--surface) 72%, transparent);
      -webkit-backdrop-filter: saturate(180%) blur(20px);
      backdrop-filter: saturate(180%) blur(20px);
      border-bottom: 1px solid color-mix(in srgb, var(--border) 70%, transparent);
    }
    .masthead-inner {
      width: min(1320px, calc(100% - 44px));
      margin: 0 auto;
      height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
    }
    .brand { display: flex; align-items: center; gap: 11px; min-width: 0; }
    .brand-mark {
      width: 28px;
      height: 28px;
      flex: none;
      border-radius: 9px;
      background: linear-gradient(160deg, color-mix(in srgb, var(--accent) 88%, white), var(--accent));
      color: var(--accent-ink);
      display: grid;
      place-items: center;
      font-weight: 600;
      font-size: 12px;
      letter-spacing: -0.03em;
      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
    }
    .brand-text { display: flex; flex-direction: column; line-height: 1.15; min-width: 0; }
    .brand-name { font-size: 0.84rem; font-weight: 600; letter-spacing: -0.01em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .brand-sub { font-size: 0.7rem; color: var(--faint); font-weight: 450; }
    .masthead-actions { display: flex; align-items: center; gap: 8px; }

    /* ---------- Page ---------- */
    .page {
      width: min(1320px, calc(100% - 44px));
      margin: 0 auto;
      padding: 26px 0 56px;
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }

    /* ---------- Hero ---------- */
    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(330px, 0.85fr);
      gap: 14px;
      margin-bottom: 26px;
    }
    .hero-head, .verdict { padding: 24px 26px; }
    .hero-head { display: flex; flex-direction: column; }
    .hero-head .meta-row { margin-top: auto; padding-top: 22px; }
    .eyebrow {
      margin: 0 0 11px;
      color: var(--faint);
      font-size: 0.69rem;
      font-weight: 600;
      letter-spacing: 0.13em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      max-width: 18ch;
      font-size: clamp(1.7rem, 2.4vw, 2.5rem);
      font-weight: 600;
      line-height: 1.06;
      letter-spacing: -0.022em;
    }
    .lead {
      max-width: 52ch;
      margin: 14px 0 0;
      color: var(--muted);
      font-size: 0.95rem;
    }

    .meta-row, .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
      align-items: center;
    }
    .meta-row { margin-top: 18px; }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      height: 28px;
      padding: 0 12px;
      border: 1px solid var(--border);
      border-radius: 980px;
      background: var(--surface-soft);
      color: var(--muted);
      font-size: 0.74rem;
      font-weight: 500;
      white-space: nowrap;
    }
    .chip-quiet { background: transparent; }
    .chip-dot::before {
      content: "";
      width: 5px; height: 5px; border-radius: 50%;
      background: var(--good);
    }

    /* ---------- Buttons ---------- */
    .btn {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      height: 34px;
      border: 1px solid var(--border-strong);
      border-radius: 980px;
      background: var(--surface);
      color: var(--ink-soft);
      padding: 0 15px;
      cursor: pointer;
      font-size: 0.82rem;
      font-weight: 500;
      letter-spacing: -0.006em;
      transition: background 0.18s var(--ease), border-color 0.18s var(--ease),
        color 0.18s var(--ease), transform 0.18s var(--spring);
    }
    .btn:hover { background: var(--inset); border-color: var(--border-strong); color: var(--ink); }
    .btn:active { transform: scale(0.97); }
    .btn[aria-expanded="true"] { background: var(--accent-soft); border-color: transparent; color: var(--accent); }
    .btn-ghost { border-color: transparent; background: transparent; }
    .btn-ghost:hover { background: var(--inset); }

    /* ---------- Verdict panel ---------- */
    .verdict {
      display: flex;
      flex-direction: column;
      background: var(--surface);
    }
    .verdict-line {
      margin: 0;
      font-size: 1.18rem;
      font-weight: 600;
      line-height: 1.28;
      letter-spacing: -0.015em;
    }
    .verdict-copy {
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 0.86rem;
      line-height: 1.5;
    }
    .verdict-foot {
      margin-top: auto;
      padding-top: 18px;
      display: grid;
      gap: 14px;
    }
    .verdict-stat { display: grid; gap: 3px; }
    .verdict-stat + .verdict-stat { border-top: 1px solid var(--border); padding-top: 14px; }
    .verdict-k {
      color: var(--faint);
      font-size: 0.66rem;
      font-weight: 600;
      letter-spacing: 0.1em;
      text-transform: uppercase;
    }
    .verdict-v { font-size: 0.96rem; font-weight: 600; letter-spacing: -0.01em; }
    .verdict-d { color: var(--muted); font-size: 0.8rem; line-height: 1.45; }

    /* ---------- Controls ---------- */
    .controls {
      display: flex;
      align-items: flex-end;
      gap: 16px;
      padding: 16px 18px;
      margin-bottom: 26px;
    }
    .filters {
      flex: 1 1 auto;
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr)) repeat(2, minmax(0, 1.18fr));
      gap: 10px 12px;
    }
    .controls-bar { display: flex; gap: 7px; flex: none; }
    .field { min-width: 0; }
    .field label {
      display: block;
      margin-bottom: 6px;
      color: var(--faint);
      font-size: 0.67rem;
      font-weight: 600;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .field select, .field input {
      width: 100%;
      height: 38px;
      padding: 0 12px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: var(--inset);
      color: var(--ink);
      font-size: 0.84rem;
      letter-spacing: -0.006em;
      transition: border-color 0.16s var(--ease), box-shadow 0.16s var(--ease), background 0.16s var(--ease);
    }
    .field select {
      appearance: none;
      -webkit-appearance: none;
      padding-right: 32px;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12' fill='none'%3E%3Cpath d='M2.5 4.5L6 8l3.5-3.5' stroke='%2386868b' stroke-width='1.4' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 11px center;
    }
    .field select:hover, .field input:hover { border-color: var(--border-strong); background: var(--surface-soft); }
    .field select:focus, .field input:focus {
      outline: none;
      border-color: var(--accent);
      background: var(--surface);
      box-shadow: 0 0 0 4px var(--accent-soft);
    }

    /* ---------- Sections ---------- */
    .section { margin-bottom: 26px; }
    .section-head { margin-bottom: 18px; }
    .section h2 {
      margin: 2px 0 0;
      font-size: 1.18rem;
      font-weight: 600;
      letter-spacing: -0.015em;
    }
    .section-sub {
      margin: 6px 0 0;
      max-width: 70ch;
      color: var(--muted);
      font-size: 0.86rem;
    }

    /* ---------- KPI band ---------- */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 1px;
      background: var(--border);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
    }
    .kpi {
      padding: 18px 18px 16px;
      background: var(--surface);
      display: flex;
      flex-direction: column;
      min-height: 158px;
      transition: background 0.16s var(--ease);
    }
    .kpi:hover { background: var(--surface-soft); }
    .kpi-top { display: flex; align-items: center; gap: 7px; }
    .kpi-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--faint); flex: none; }
    .kpi.good .kpi-dot { background: var(--good); }
    .kpi.warn .kpi-dot { background: var(--warn); }
    .kpi.bad .kpi-dot { background: var(--bad); }
    .kpi.accent .kpi-dot { background: var(--accent); }
    .kpi-label {
      color: var(--muted);
      font-size: 0.73rem;
      font-weight: 500;
      letter-spacing: 0.01em;
    }
    .kpi-value {
      margin-top: 11px;
      font-family: var(--mono);
      font-size: clamp(1.45rem, 1.75vw, 1.9rem);
      font-weight: 500;
      line-height: 1;
      letter-spacing: -0.04em;
      font-variant-numeric: tabular-nums;
    }
    .kpi-spark { margin-top: 12px; height: 26px; }
    .kpi-spark svg { display: block; width: 100%; height: 26px; overflow: visible; }
    .kpi-spark .spark-line {
      fill: none;
      stroke: var(--accent);
      stroke-width: 1.5;
      opacity: 0.75;
    }
    .kpi-note {
      margin-top: auto;
      padding-top: 11px;
      color: var(--faint);
      font-size: 0.74rem;
      line-height: 1.45;
    }
    .kpi-note b { color: var(--ink-soft); font-weight: 500; }
    .delta-good { color: var(--good); font-weight: 600; }
    .delta-bad { color: var(--bad); font-weight: 600; }
    .delta-flat { color: var(--faint); font-weight: 500; }

    /* ---------- Triage cards ---------- */
    .triage-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .triage {
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface);
      padding: 18px;
      display: flex;
      flex-direction: column;
      min-height: 138px;
      box-shadow: var(--shadow);
      transition: transform 0.24s var(--ease), box-shadow 0.24s var(--ease), border-color 0.24s var(--ease);
    }
    .triage:hover { transform: translateY(-2px); box-shadow: var(--shadow-pop); border-color: var(--border-strong); }
    .triage-head { display: flex; align-items: center; gap: 7px; }
    .triage-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--accent); flex: none; }
    .triage.bad .triage-dot { background: var(--bad); }
    .triage.warn .triage-dot { background: var(--warn); }
    .triage.good .triage-dot { background: var(--good); }
    .triage-label {
      color: var(--faint);
      font-size: 0.66rem;
      font-weight: 600;
      letter-spacing: 0.1em;
      text-transform: uppercase;
    }
    .triage-title {
      margin-top: 11px;
      font-size: 1rem;
      font-weight: 600;
      line-height: 1.25;
      letter-spacing: -0.01em;
    }
    .triage-body {
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.83rem;
      line-height: 1.5;
    }

    /* ---------- Charts ---------- */
    .chart-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .chart-grid.three { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .chart-card {
      display: flex;
      flex-direction: column;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface);
      padding: 18px 16px 10px;
      box-shadow: var(--shadow);
      transition: box-shadow 0.24s var(--ease), border-color 0.24s var(--ease);
    }
    .chart-card:hover { box-shadow: var(--shadow-pop); border-color: var(--border-strong); }
    .chart-head { padding: 0 6px 2px; }
    .chart-title {
      margin: 0;
      font-size: 0.95rem;
      font-weight: 600;
      line-height: 1.3;
      letter-spacing: -0.01em;
    }
    .chart-kicker {
      margin: 3px 0 0;
      color: var(--muted);
      font-size: 0.8rem;
    }
    .chart-kicker b { color: var(--ink); font-weight: 600; }
    .chart-body { flex: 1; min-height: 296px; }
    .chart-card.tall .chart-body { min-height: 352px; }
    .chart-body > .js-plotly-plot,
    .chart-body > .plot-container {
      width: 100% !important;
      min-height: inherit;
    }

    /* ---------- Scenario / method panels ---------- */
    .scenario-panel, .method-panel {
      display: none;
      padding: 18px;
      margin-bottom: 26px;
    }
    .method-panel {
      color: var(--muted);
      font-size: 0.86rem;
      line-height: 1.6;
      max-width: 90ch;
    }
    .scenario-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(180px, 1fr));
      gap: 18px;
    }
    .scenario-grid input[type="range"] {
      width: 100%;
      accent-color: var(--accent);
      margin-top: 4px;
    }
    .scenario-value {
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.78rem;
      font-weight: 500;
    }
    .scenario-value b { font-family: var(--mono); color: var(--ink); font-weight: 500; }

    /* ---------- Table ---------- */
    .table-tools {
      display: flex;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
    }
    .table-tools input {
      width: min(380px, 100%);
      height: 36px;
      padding: 0 12px;
      border: 1px solid var(--border-strong);
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
      font-size: 0.84rem;
    }
    .table-tools input:focus {
      outline: none;
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-soft);
    }
    .table-wrap {
      max-height: 460px;
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: var(--radius);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.82rem;
    }
    th, td {
      padding: 11px 14px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }
    tbody tr:last-child td { border-bottom: 0; }
    tbody tr { transition: background 0.1s ease; }
    tbody tr:hover { background: var(--surface-soft); }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: var(--surface-soft);
      color: var(--muted);
      cursor: pointer;
      font-size: 0.68rem;
      font-weight: 600;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      border-bottom: 1px solid var(--border-strong);
      user-select: none;
    }
    th:hover { color: var(--ink); }
    th[aria-sort="ascending"], th[aria-sort="descending"] { color: var(--accent); }
    td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
    .wrap { white-space: normal; min-width: 230px; color: var(--muted); }
    .score { font-family: var(--mono); font-weight: 600; }
    .badge {
      display: inline-flex;
      align-items: center;
      height: 21px;
      padding: 0 8px;
      border-radius: 6px;
      font-size: 0.68rem;
      font-weight: 600;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }
    .tier-low { color: var(--good); background: var(--good-soft); }
    .tier-medium { color: var(--warn); background: var(--warn-soft); }
    .tier-high, .tier-critical { color: var(--bad); background: var(--bad-soft); }

    /* ---------- Footer ---------- */
    .foot {
      margin-top: 34px;
      padding-top: 16px;
      border-top: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      flex-wrap: wrap;
      color: var(--faint);
      font-size: 0.74rem;
    }
    .foot .num { letter-spacing: 0; font-size: 0.72rem; }

    /* ---------- Status alert ---------- */
    .status-alert {
      display: none;
      margin: 0 0 22px;
      padding: 12px 14px;
      border: 1px solid var(--border-strong);
      border-left: 3px solid var(--warn);
      border-radius: 8px;
      background: var(--warn-soft);
      color: var(--ink);
      font-size: 0.85rem;
    }

    /* ---------- Brief ---------- */
    .brief-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .brief {
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface);
      padding: 18px 20px;
      box-shadow: var(--shadow);
      transition: transform 0.24s var(--ease), box-shadow 0.24s var(--ease), border-color 0.24s var(--ease);
    }
    .brief:hover { transform: translateY(-2px); box-shadow: var(--shadow-pop); border-color: var(--border-strong); }
    .brief-title {
      color: var(--faint);
      font-size: 0.66rem;
      font-weight: 600;
      letter-spacing: 0.1em;
      text-transform: uppercase;
    }
    .brief-copy {
      margin-top: 9px;
      color: var(--ink-soft);
      font-size: 0.88rem;
      line-height: 1.55;
    }
    .brief-copy b { color: var(--ink); font-weight: 600; }

    @media (max-width: 1080px) {
      .hero { grid-template-columns: 1fr; }
      .controls { flex-direction: column; align-items: stretch; }
      .filters { grid-template-columns: repeat(4, minmax(0, 1fr)); }
      .controls-bar { justify-content: flex-end; }
      .kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .triage-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .chart-grid, .chart-grid.three { grid-template-columns: 1fr; }
    }

    @media (max-width: 640px) {
      .page { width: calc(100% - 28px); padding-top: 18px; }
      .masthead-inner { width: calc(100% - 28px); }
      .brand-sub { display: none; }
      .masthead .chip-quiet { display: none; }
      h1 { font-size: clamp(1.55rem, 7vw, 1.95rem); }
      .hero-head, .verdict, .section { padding: 18px; }
      .filters, .triage-grid, .scenario-grid, .brief-grid { grid-template-columns: 1fr; }
      .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px; }
      .chart-card, .chart-card.tall { min-height: 330px; }
      th, td { padding: 10px 12px; font-size: 0.78rem; }
    }

    @media (prefers-reduced-motion: reduce) {
      html { scroll-behavior: auto; }
      *, *::before, *::after { transition: none !important; animation: none !important; }
    }

    @media print {
      body { background: #fff; color: #111; }
      .masthead { position: static; backdrop-filter: none; }
      .page { width: 100%; padding: 0; }
      .panel, .section { box-shadow: none; break-inside: avoid; }
      .controls, .scenario-panel, .method-panel, .table-tools, .masthead-actions { display: none !important; }
      .chart-grid, .chart-grid.three { grid-template-columns: 1fr; }
      .chart-card, .chart-card.tall { min-height: 280px; }
      .table-wrap { max-height: none; overflow: visible; }
      th { position: static; }
    }
  </style>
</head>
<body>
  <a class="skip-link" href="#main-content">Skip to dashboard content</a>

  <header class="masthead">
    <div class="masthead-inner">
      <div class="brand">
        <span class="brand-mark" aria-hidden="true">SI</span>
        <span class="brand-text">
          <span class="brand-name">Service &amp; Inventory Intelligence</span>
          <span class="brand-sub">Operating review</span>
        </span>
      </div>
      <div class="masthead-actions">
        <span class="chip chip-quiet chip-dot" id="meta-refresh">Snapshot loading</span>
        <button class="btn btn-ghost" id="toggle-theme" type="button">Dark</button>
        <button class="btn btn-ghost" id="print-dashboard" type="button">Print</button>
      </div>
    </div>
  </header>

  <div class="page">
    <section class="hero" aria-labelledby="page-title">
      <div class="panel hero-head">
        <p class="eyebrow">Supply chain operating review</p>
        <h1 id="page-title">Service, inventory &amp; working-capital review</h1>
        <p class="lead">Where the network loses service, where capital sits idle, and the SKU-location actions that recover the most value over the next 12 months.</p>
        <div class="meta-row" aria-label="Dashboard metadata">
          <span class="chip" id="meta-scope">All operating records</span>
          <span class="chip" id="meta-period">Period loading</span>
        </div>
      </div>

      <aside class="panel verdict" aria-label="Current decision frame">
        <p class="eyebrow">The call</p>
        <p class="verdict-line" id="decision-title">Loading decision frame</p>
        <p class="verdict-copy" id="decision-copy"></p>
        <div class="verdict-foot">
          <div class="verdict-stat">
            <span class="verdict-k">Act first</span>
            <span class="verdict-v" id="hero-action"></span>
            <span class="verdict-d" id="hero-action-detail"></span>
          </div>
          <div class="verdict-stat">
            <span class="verdict-k">12-month value at stake</span>
            <span class="verdict-v num" id="hero-value"></span>
            <span class="verdict-d" id="hero-value-detail"></span>
          </div>
        </div>
      </aside>
    </section>

    <section class="panel controls" aria-label="Dashboard filters">
      <div class="filters">
        <div class="field"><label for="filter-region">Region</label><select id="filter-region"></select></div>
        <div class="field"><label for="filter-warehouse">Warehouse</label><select id="filter-warehouse"></select></div>
        <div class="field"><label for="filter-category">Category</label><select id="filter-category"></select></div>
        <div class="field"><label for="filter-supplier">Supplier</label><select id="filter-supplier"></select></div>
        <div class="field"><label for="filter-abc">ABC class</label><select id="filter-abc"></select></div>
        <div class="field"><label for="filter-start">From</label><input id="filter-start" type="month" /></div>
        <div class="field"><label for="filter-end">To</label><input id="filter-end" type="month" /></div>
      </div>
      <div class="controls-bar">
        <button class="btn" id="reset-filters" type="button">Reset</button>
        <button class="btn" id="toggle-scenario" type="button" aria-controls="scenario-panel" aria-expanded="false">Assumptions</button>
        <button class="btn" id="toggle-method" type="button" aria-controls="method-panel" aria-expanded="false">Method</button>
      </div>
    </section>

    <div id="status-alert" class="status-alert" role="status"></div>

    <section class="panel scenario-panel" id="scenario-panel" aria-label="Scenario assumptions">
      <div class="scenario-grid">
        <div class="field">
          <label for="assump-margin-rate">Recoverable lost margin rate</label>
          <input id="assump-margin-rate" type="range" min="10" max="60" step="1" />
          <div class="scenario-value" id="assump-margin-rate-value"></div>
        </div>
        <div class="field">
          <label for="assump-wc-rate">Releasable working capital rate</label>
          <input id="assump-wc-rate" type="range" min="5" max="60" step="1" />
          <div class="scenario-value" id="assump-wc-rate-value"></div>
        </div>
        <div class="field">
          <label for="assump-slow-weight">Slow-moving incremental weight</label>
          <input id="assump-slow-weight" type="range" min="0" max="100" step="1" />
          <div class="scenario-value" id="assump-slow-weight-value"></div>
        </div>
      </div>
    </section>

    <section class="panel method-panel" id="method-panel">
      Metrics are demand-weighted where required. Fill rate is fulfilled units divided by demanded units. Stockout rate is lost units divided by demanded units. The 12-month opportunity proxy combines annualized recoverable lost-sales margin and releasable trapped working capital under the visible scenario assumptions. Financial values are directional operating proxies, not accounting entries.
    </section>

    <main id="main-content">
      <section class="section" aria-labelledby="scorecard-title">
        <div class="section-head">
          <div>
            <p class="eyebrow">Portfolio signals</p>
            <h2 id="scorecard-title">Current performance</h2>
            <p class="section-sub">Demand-weighted service metrics, financial exposure proxies, and the active priority queue.</p>
          </div>
        </div>
        <div class="kpi-grid" id="kpi-grid"></div>
      </section>

      <section class="section" aria-labelledby="priorities-title">
        <div class="section-head">
          <div>
            <p class="eyebrow">Priorities</p>
            <h2 id="priorities-title">Priority actions</h2>
            <p class="section-sub">The worst service site, largest supplier exposure, biggest capital pocket, and sharpest policy conflict in the current scope.</p>
          </div>
        </div>
        <div class="triage-grid" id="priority-grid"></div>
      </section>

      <section class="section" aria-labelledby="charts-title">
        <div class="section-head">
          <div>
            <p class="eyebrow">Operational drivers</p>
            <h2 id="charts-title">Performance and exposure</h2>
            <p class="section-sub">Monthly trends and the warehouses and categories contributing most to current exposure.</p>
          </div>
        </div>
        <div class="chart-grid">
          <div class="chart-card">
            <div class="chart-head"><h3 class="chart-title">Is service holding the 97% bar?</h3></div>
            <div class="chart-body" id="chart-trend" role="img" aria-label="Monthly fill rate trend against the 97 percent service target"></div>
          </div>
          <div class="chart-card">
            <div class="chart-head"><h3 class="chart-title">Is value leakage revenue-led or capital-led?</h3></div>
            <div class="chart-body" id="chart-value-trend" role="img" aria-label="Monthly lost sales and trapped working capital trend"></div>
          </div>
          <div class="chart-card">
            <div class="chart-head"><h3 class="chart-title">Which warehouse loses the most revenue?</h3></div>
            <div class="chart-body" id="chart-bottlenecks" role="img" aria-label="Warehouse lost sales comparison"></div>
          </div>
          <div class="chart-card">
            <div class="chart-head"><h3 class="chart-title">Where is working capital trapped?</h3></div>
            <div class="chart-body" id="chart-category-capital" role="img" aria-label="Category working capital concentration"></div>
          </div>
        </div>
      </section>

      <section class="section" aria-labelledby="tradeoff-title">
        <div class="section-head">
          <div>
            <p class="eyebrow">Policy trade-off</p>
            <h2 id="tradeoff-title">Service and inventory conflict</h2>
            <p class="section-sub">Identify understocked revenue exposure, capital-heavy pockets, and cases where service misses coexist with excess inventory.</p>
          </div>
        </div>
        <div class="chart-grid three">
          <div class="chart-card tall">
            <div class="chart-head"><h3 class="chart-title">Which segments under-serve despite inventory?</h3></div>
            <div class="chart-body" id="chart-tradeoff" role="img" aria-label="Service level versus days of supply by category and region"></div>
          </div>
          <div class="chart-card tall">
            <div class="chart-head"><h3 class="chart-title">Which suppliers carry the largest exposure?</h3></div>
            <div class="chart-body" id="chart-supplier" role="img" aria-label="Supplier execution and downstream lost sales"></div>
          </div>
          <div class="chart-card tall">
            <div class="chart-head">
              <h3 class="chart-title">How concentrated is the lost-sales problem?</h3>
              <p class="chart-kicker" id="pareto-kicker"></p>
            </div>
            <div class="chart-body" id="chart-governance" role="img" aria-label="Concentration of lost sales across the highest exposure SKU locations"></div>
          </div>
        </div>
      </section>

      <section class="section" aria-labelledby="detail-title">
        <div class="section-head">
          <div>
            <p class="eyebrow">Execution detail</p>
            <h2 id="detail-title">Priority Action Queue</h2>
            <p class="section-sub">Scores use the full-period governed baseline; operating metrics reflect the active filters.</p>
          </div>
        </div>
        <div class="table-tools">
          <input id="table-search" type="search" placeholder="Search SKU, product, warehouse, supplier, driver, action" aria-label="Search priority action queue" />
          <div class="chip" id="table-meta">Rows loading</div>
        </div>
        <div class="table-wrap">
          <table id="detail-table">
            <caption class="sr-only">Full-period priority scores with operating metrics from the active filters</caption>
            <thead>
              <tr>
                <th tabindex="0" class="num" data-key="governance_priority_score" aria-sort="descending">Priority</th>
                <th tabindex="0" data-key="product_id" aria-sort="none">SKU</th>
                <th tabindex="0" data-key="product_name" aria-sort="none">Product</th>
                <th tabindex="0" data-key="warehouse_id" aria-sort="none">Warehouse</th>
                <th tabindex="0" data-key="supplier_id" aria-sort="none">Supplier</th>
                <th tabindex="0" class="num" data-key="fill_rate" aria-sort="none">Fill</th>
                <th tabindex="0" class="num" data-key="stockout_rate" aria-sort="none">Stockout</th>
                <th tabindex="0" class="num" data-key="lost_sales_revenue" aria-sort="none">Lost sales</th>
                <th tabindex="0" data-key="risk_tier" aria-sort="none">Tier</th>
                <th tabindex="0" data-key="main_risk_driver" aria-sort="none">Driver</th>
                <th tabindex="0" data-key="recommended_action" aria-sort="none">Action</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </section>

      <section class="section" aria-labelledby="brief-title">
        <div class="section-head">
          <div>
            <p class="eyebrow">Interpretation</p>
            <h2 id="brief-title">Review summary</h2>
            <p class="section-sub">Concise interpretation of the active filters and full-period priority baseline.</p>
          </div>
        </div>
        <div class="brief-grid" id="brief-grid"></div>
      </section>

      <footer class="foot" aria-label="Dataset provenance">
        <span class="num" id="foot-provenance"></span>
        <span>Demand-weighted metrics · financial values are directional operating proxies, not accounting entries</span>
      </footer>
    </main>
  </div>

  <script src="__PLOTLY_CDN_URL__" integrity="__PLOTLY_SRI__" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
  <script>
    const dashboardData = __DATA_JSON__;

    function decodeMonthlyFact(compact) {
      const dim = compact.dim || {};
      return (compact.rows || []).map(r => ({
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
        slow_moving_non_excess_proxy: Number(r[15]),
        trapped_wc_proxy: Number(r[16]),
        lost_sales_margin_proxy: Number(r[17]),
        observation_days: Number(r[18]),
        stockout_month_flag: Number(r[19]),
      }));
    }

    const monthlyFact = decodeMonthlyFact(dashboardData.monthly_sku_compact);
    const productMeta = dashboardData.product_name_map || {};
    const supplierMeta = Object.fromEntries((dashboardData.suppliers || []).map(s => [s.supplier_id, s]));
    const warehouseMeta = Object.fromEntries((dashboardData.warehouses || []).map(w => [w.warehouse_id, w]));
    const skuRiskBaselineMap = Object.fromEntries(
      (dashboardData.sku_risk_baseline || []).map(s => [`${s.product_id}|${s.warehouse_id}|${s.supplier_id}`, s])
    );

    const filters = {
      region: document.getElementById('filter-region'),
      warehouse: document.getElementById('filter-warehouse'),
      category: document.getElementById('filter-category'),
      supplier: document.getElementById('filter-supplier'),
      abc: document.getElementById('filter-abc'),
      start: document.getElementById('filter-start'),
      end: document.getElementById('filter-end'),
    };
    const scenario = {
      marginRate: document.getElementById('assump-margin-rate'),
      wcRate: document.getElementById('assump-wc-rate'),
      slowWeight: document.getElementById('assump-slow-weight'),
      marginRateValue: document.getElementById('assump-margin-rate-value'),
      wcRateValue: document.getElementById('assump-wc-rate-value'),
      slowWeightValue: document.getElementById('assump-slow-weight-value'),
    };
    const tableBody = document.querySelector('#detail-table tbody');
    const tableSearch = document.getElementById('table-search');
    const tableMeta = document.getElementById('table-meta');
    const plotConfig = { displayModeBar: false, responsive: true, scrollZoom: false };
    let tableSort = { key: 'governance_priority_score', dir: 'desc' };
    let currentAgg = null;
    let currentTheme = 'light';

    const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const fmtPct = v => `${((Number(v) || 0) * 100).toFixed(1)}%`;
    const fmtNum = v => Number(v || 0).toLocaleString();
    const fmtEur = v => `€${Number(v || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
    const fmtEurM = v => `€${(Number(v || 0) / 1_000_000).toFixed(2)}M`;
    const fmtCompactEur = v => {
      const value = Math.abs(Number(v) || 0);
      const sign = (Number(v) || 0) < 0 ? '-' : '';
      if (value >= 1_000_000_000) return `${sign}€${(value / 1_000_000_000).toFixed(2)}B`;
      if (value >= 1_000_000) return `${sign}€${(value / 1_000_000).toFixed(1)}M`;
      if (value >= 1_000) return `${sign}€${(value / 1_000).toFixed(0)}K`;
      return `${sign}€${value.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
    };
    const short = (value, limit = 30) => {
      const text = String(value || '');
      return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
    };
    const clamp = (x, min, max) => Math.max(min, Math.min(max, x));
    const norm = (x, min, max) => max <= min ? 0 : clamp((x - min) / (max - min), 0, 1);
    const cap = s => { const t = String(s || '').trim(); return t ? t.charAt(0).toUpperCase() + t.slice(1) : t; };

    function palette() {
      if (currentTheme === 'dark') {
        return {
          paper: 'rgba(0,0,0,0)', plot: 'rgba(0,0,0,0)',
          text: '#f5f5f7', muted: '#98989d', faint: '#6e6e73', grid: '#2c2c2e',
          accent: '#0a84ff', accentDim: 'rgba(10,132,255,0.30)',
          good: '#30d158', warn: '#ff9f0a', bad: '#ff453a', slate: '#8e8e93',
        };
      }
      return {
        paper: 'rgba(0,0,0,0)', plot: 'rgba(0,0,0,0)',
        text: '#1d1d1f', muted: '#6e6e73', faint: '#86868b', grid: '#e6e6eb',
        accent: '#0071e3', accentDim: 'rgba(0,113,227,0.14)',
        good: '#1d8a4e', warn: '#9a6500', bad: '#d70015', slate: '#aeaeb2',
      };
    }

    function baseLayout() {
      const c = palette();
      return {
        paper_bgcolor: c.paper,
        plot_bgcolor: c.plot,
        font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", Geist, system-ui, sans-serif', size: 11.5, color: c.muted },
        margin: { l: 64, r: 18, t: 14, b: 42 },
        xaxis: { gridcolor: c.grid, zeroline: false, automargin: true, tickfont: { family: 'Geist Mono, monospace', size: 10.5, color: c.faint }, linecolor: c.grid },
        yaxis: { gridcolor: c.grid, zeroline: false, automargin: true, tickfont: { family: 'Geist Mono, monospace', size: 10.5, color: c.faint } },
        hoverlabel: { bgcolor: currentTheme === 'dark' ? '#1a232d' : '#0b1a24', bordercolor: 'rgba(0,0,0,0)', font: { color: '#ffffff', family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", Geist, system-ui, sans-serif', size: 12 } },
        showlegend: false,
        dragmode: false,
      };
    }

    function getUnique(rows, key) {
      return [...new Set(rows.map(r => r[key]).filter(Boolean))].sort();
    }

    function populateSelect(select, values) {
      select.innerHTML = '<option value="ALL">All</option>' + values.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
    }

    function initializeControls() {
      populateSelect(filters.region, getUnique(monthlyFact, 'region'));
      populateSelect(filters.warehouse, getUnique(monthlyFact, 'warehouse_id'));
      populateSelect(filters.category, getUnique(monthlyFact, 'category'));
      populateSelect(filters.supplier, getUnique(monthlyFact, 'supplier_id'));
      populateSelect(filters.abc, getUnique(monthlyFact, 'abc_class'));
      filters.start.value = dashboardData.meta.date_min.slice(0, 7);
      filters.end.value = dashboardData.meta.date_max.slice(0, 7);

      const defaults = dashboardData.meta.assumptions_default || {};
      scenario.marginRate.value = String(Math.round((defaults.recoverable_margin_rate ?? 0.35) * 100));
      scenario.wcRate.value = String(Math.round((defaults.releasable_wc_rate ?? 0.25) * 100));
      scenario.slowWeight.value = String(Math.round((defaults.slow_moving_incremental_weight ?? 0.50) * 100));
      updateScenarioLabels();
    }

    function readScenario() {
      return {
        recoverableMarginRate: Number(scenario.marginRate.value) / 100,
        releasableWcRate: Number(scenario.wcRate.value) / 100,
        slowMovingIncrementalWeight: Number(scenario.slowWeight.value) / 100,
      };
    }

    function updateScenarioLabels() {
      const s = readScenario();
      scenario.marginRateValue.textContent = `${fmtPct(s.recoverableMarginRate)} of annualized lost-sales margin`;
      scenario.wcRateValue.textContent = `${fmtPct(s.releasableWcRate)} of trapped working capital`;
      scenario.slowWeightValue.textContent = `${fmtPct(s.slowMovingIncrementalWeight)} incremental slow-moving value`;
    }

    function getDateRange() {
      let start = `${filters.start.value || dashboardData.meta.date_min.slice(0, 7)}-01`;
      let end = `${filters.end.value || dashboardData.meta.date_max.slice(0, 7)}-01`;
      if (start > end) [start, end] = [end, start];
      return { start, end };
    }

    function rowPasses(row, range) {
      return (
        (filters.region.value === 'ALL' || row.region === filters.region.value) &&
        (filters.warehouse.value === 'ALL' || row.warehouse_id === filters.warehouse.value) &&
        (filters.category.value === 'ALL' || row.category === filters.category.value) &&
        (filters.supplier.value === 'ALL' || row.supplier_id === filters.supplier.value) &&
        (filters.abc.value === 'ALL' || row.abc_class === filters.abc.value) &&
        row.month >= range.start && row.month <= range.end
      );
    }

    function ensure(map, key, seed) {
      if (!map.has(key)) map.set(key, seed());
      return map.get(key);
    }

    function aggregate(rows, s) {
      const monthMap = new Map();
      const warehouseMap = new Map();
      const categoryMap = new Map();
      const supplierMap = new Map();
      const segmentMap = new Map();
      const skuMap = new Map();
      let demand = 0, fulfilled = 0, lost = 0, lostSales = 0, lostMargin = 0;

      const addMonthlyBalance = (entity, month, obsDays, inventory, excess, trapped) => {
        const balance = ensure(entity.balanceByMonth, month, () => ({ obsDays, inventory: 0, excess: 0, trapped: 0 }));
        balance.inventory += inventory;
        balance.excess += excess;
        balance.trapped += trapped;
      };
      const finalizeBalance = entity => {
        const balances = [...entity.balanceByMonth.values()];
        const days = balances.reduce((sum, x) => sum + x.obsDays, 0);
        const weighted = key => balances.reduce((sum, x) => sum + x[key] * x.obsDays, 0) / Math.max(days, 1);
        const { balanceByMonth, ...clean } = entity;
        return { ...clean, inventory: weighted('inventory'), excess: weighted('excess'), trapped: weighted('trapped') };
      };

      for (const r of rows) {
        const d = r.units_demanded;
        const f = r.units_fulfilled;
        const l = r.units_lost_sales;
        const ls = r.lost_sales_revenue;
        const inv = r.inventory_value;
        const ex = r.excess_inventory_proxy;
        const trappedScenario = ex + s.slowMovingIncrementalWeight * r.slow_moving_non_excess_proxy;
        const obsDays = r.observation_days;
        demand += d; fulfilled += f; lost += l; lostSales += ls; lostMargin += r.lost_sales_margin_proxy;

        const m = ensure(monthMap, r.month, () => ({ month: r.month, obsDays, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, inventory: 0, excess: 0, trapped: 0 }));
        m.demand += d; m.fulfilled += f; m.lost += l; m.lostSales += ls; m.inventory += inv; m.trapped += trappedScenario;
        m.excess += ex;

        const wMeta = warehouseMeta[r.warehouse_id] || {};
        const w = ensure(warehouseMap, r.warehouse_id, () => ({ warehouse_id: r.warehouse_id, warehouse_name: wMeta.warehouse_name || r.warehouse_id, region: r.region, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, balanceByMonth: new Map(), dosWeighted: 0, obsDays: 0 }));
        w.demand += d; w.fulfilled += f; w.lost += l; w.lostSales += ls; w.dosWeighted += r.avg_days_of_supply * obsDays; w.obsDays += obsDays;
        addMonthlyBalance(w, r.month, obsDays, inv, ex, trappedScenario);

        const c = ensure(categoryMap, r.category, () => ({ category: r.category, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, balanceByMonth: new Map(), dosWeighted: 0, obsDays: 0 }));
        c.demand += d; c.fulfilled += f; c.lost += l; c.lostSales += ls; c.dosWeighted += r.avg_days_of_supply * obsDays; c.obsDays += obsDays;
        addMonthlyBalance(c, r.month, obsDays, inv, ex, trappedScenario);

        const sMeta = supplierMeta[r.supplier_id] || {};
        const sup = ensure(supplierMap, r.supplier_id, () => ({ supplier_id: r.supplier_id, supplier_name: sMeta.supplier_name || r.supplier_id, on_time_delivery_rate: Number(sMeta.on_time_delivery_rate || 0), average_delay_days: Number(sMeta.average_delay_days || 0), lead_time_variability: Number(sMeta.lead_time_variability || 0), demand: 0, fulfilled: 0, lost: 0, lostSales: 0 }));
        sup.demand += d; sup.fulfilled += f; sup.lost += l; sup.lostSales += ls;

        const segKey = `${r.category}|${r.region}`;
        const seg = ensure(segmentMap, segKey, () => ({ segment: segKey, category: r.category, region: r.region, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, balanceByMonth: new Map(), dosWeighted: 0, obsDays: 0 }));
        seg.demand += d; seg.fulfilled += f; seg.lost += l; seg.lostSales += ls; seg.dosWeighted += r.avg_days_of_supply * obsDays; seg.obsDays += obsDays;
        addMonthlyBalance(seg, r.month, obsDays, inv, ex, trappedScenario);

        const skuKey = `${r.product_id}|${r.warehouse_id}|${r.supplier_id}`;
        const sku = ensure(skuMap, skuKey, () => ({ product_id: r.product_id, product_name: productMeta[r.product_id] || r.product_id, warehouse_id: r.warehouse_id, supplier_id: r.supplier_id, category: r.category, region: r.region, abc_class: r.abc_class, demand: 0, fulfilled: 0, lost: 0, lostSales: 0, balanceByMonth: new Map(), dosWeighted: 0, obsDays: 0 }));
        sku.demand += d; sku.fulfilled += f; sku.lost += l; sku.lostSales += ls; sku.dosWeighted += r.avg_days_of_supply * obsDays; sku.obsDays += obsDays;
        addMonthlyBalance(sku, r.month, obsDays, inv, ex, trappedScenario);
      }

      const finalizeService = x => ({ ...x, fill_rate: x.demand > 0 ? x.fulfilled / x.demand : 1, stockout_rate: x.demand > 0 ? x.lost / x.demand : 0 });
      const monthSeries = [...monthMap.values()].sort((a,b) => a.month.localeCompare(b.month)).map(finalizeService);
      const warehouses = [...warehouseMap.values()].map(finalizeBalance).map(x => ({ ...finalizeService(x), avg_dos: x.obsDays > 0 ? x.dosWeighted / x.obsDays : 0 })).sort((a,b) => b.lostSales - a.lostSales);
      const categories = [...categoryMap.values()].map(finalizeBalance).map(x => ({ ...finalizeService(x), avg_dos: x.obsDays > 0 ? x.dosWeighted / x.obsDays : 0 })).sort((a,b) => b.lostSales - a.lostSales);
      const suppliers = [...supplierMap.values()].map(finalizeService).sort((a,b) => b.lostSales - a.lostSales);
      const segments = [...segmentMap.values()].map(finalizeBalance).map(x => ({ ...finalizeService(x), avg_dos: x.obsDays > 0 ? x.dosWeighted / x.obsDays : 0 })).sort((a,b) => b.lostSales - a.lostSales);
      const skuRows = [...skuMap.values()].map(finalizeBalance).map(x => {
        const baseline = skuRiskBaselineMap[`${x.product_id}|${x.warehouse_id}|${x.supplier_id}`] || {};
        return {
          ...finalizeService(x),
          avg_dos: x.obsDays > 0 ? x.dosWeighted / x.obsDays : 0,
          service_risk_score: Number(baseline.service_risk_score || 0),
          stockout_risk_score: Number(baseline.stockout_risk_score || 0),
          excess_inventory_score: Number(baseline.excess_inventory_score || 0),
          supplier_risk_score: Number(baseline.supplier_risk_score || 0),
          working_capital_risk_score: Number(baseline.working_capital_risk_score || 0),
          governance_priority_score: Number(baseline.governance_priority_score || 0),
          risk_tier: baseline.risk_tier || 'Low',
          main_risk_driver: baseline.main_risk_driver || 'No dominant driver',
          recommended_action: baseline.recommended_action || 'monitor within normal replenishment cadence',
        };
      }).sort((a,b) => b.governance_priority_score - a.governance_priority_score);

      const weightedSupplierOTD = suppliers.reduce((acc, x) => acc + x.on_time_delivery_rate * x.demand, 0) / Math.max(demand, 1);
      const balanceDays = monthSeries.reduce((sum, x) => sum + x.obsDays, 0);
      const annualizationFactor = 365 / Math.max(balanceDays, 1);
      const averageBalance = key => monthSeries.reduce((sum, x) => sum + x[key] * x.obsDays, 0) / Math.max(balanceDays, 1);
      const inventory = averageBalance('inventory');
      const excess = averageBalance('excess');
      const trapped = averageBalance('trapped');
      const opportunity12m = lostMargin * annualizationFactor * s.recoverableMarginRate + trapped * s.releasableWcRate;
      return {
        monthSeries, warehouses, categories, suppliers, segments, skuRows,
        totals: {
          demand, fulfilled, lost, lostSales, inventory, excess, trapped, lostMargin,
          fillRate: demand > 0 ? fulfilled / demand : 1,
          stockoutRate: demand > 0 ? lost / demand : 0,
          weightedSupplierOTD,
          annualizationFactor,
          opportunity12m,
          recoverableMarginRate: s.recoverableMarginRate,
          releasableWcRate: s.releasableWcRate,
        }
      };
    }

    function riskTone(value, good, warn, lowerIsBetter = false) {
      if (lowerIsBetter) return value <= good ? 'good' : (value <= warn ? 'warn' : 'bad');
      return value >= good ? 'good' : (value >= warn ? 'warn' : 'bad');
    }

    function sparklineSvg(values) {
      const pts = values.filter(v => Number.isFinite(v));
      if (pts.length < 2) return '';
      const min = Math.min(...pts);
      const max = Math.max(...pts);
      const span = max - min || 1;
      const w = 100, h = 26, pad = 2.5;
      const step = w / (pts.length - 1);
      const coords = pts.map((v, i) => `${(i * step).toFixed(2)},${(h - pad - ((v - min) / span) * (h - pad * 2)).toFixed(2)}`);
      return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true"><polyline class="spark-line" points="${coords.join(' ')}" vector-effect="non-scaling-stroke"></polyline></svg>`;
    }

    function splitHalves(monthSeries) {
      if (monthSeries.length < 4) return null;
      const mid = Math.ceil(monthSeries.length / 2);
      const summarize = part => {
        const demand = part.reduce((sum, x) => sum + x.demand, 0);
        const fulfilled = part.reduce((sum, x) => sum + x.fulfilled, 0);
        const lost = part.reduce((sum, x) => sum + x.lost, 0);
        const days = part.reduce((sum, x) => sum + x.obsDays, 0);
        return {
          fill: demand > 0 ? fulfilled / demand : 1,
          stockout: demand > 0 ? lost / demand : 0,
          lostSalesMonthly: part.reduce((sum, x) => sum + x.lostSales, 0) / part.length,
          trappedAvg: part.reduce((sum, x) => sum + x.trapped * x.obsDays, 0) / Math.max(days, 1),
        };
      };
      return { first: summarize(monthSeries.slice(0, mid)), second: summarize(monthSeries.slice(mid)) };
    }

    function deltaSpan(diff, threshold, goodWhenDown, fmt) {
      const up = diff > threshold;
      const down = diff < -threshold;
      if (!up && !down) return '<span class="delta-flat">— stable</span> vs first half';
      const improving = goodWhenDown ? down : up;
      const cls = improving ? 'delta-good' : 'delta-bad';
      const arrow = up ? '▲' : '▼';
      return `<span class="${cls}">${arrow} ${fmt(Math.abs(diff))}</span> vs first half`;
    }

    function renderHero(agg, range) {
      const topSku = agg.skuRows[0];
      const worstWarehouse = [...agg.warehouses].sort((a,b) => a.fill_rate - b.fill_rate)[0];
      const topSupplier = [...agg.suppliers].sort((a,b) => (b.stockout_rate * b.lostSales) - (a.stockout_rate * a.lostSales))[0];
      const topCategory = agg.categories[0];
      const criticalCount = agg.skuRows.filter(x => x.risk_tier === 'High' || x.risk_tier === 'Critical').length;
      const posture = agg.totals.fillRate < 0.95 || agg.totals.stockoutRate > 0.05 ? 'Service recovery is the immediate management agenda.' : 'Service is controlled; capital release is the next agenda.';

      document.getElementById('decision-title').textContent = posture;
      document.getElementById('decision-copy').textContent = `${fmtCompactEur(agg.totals.lostSales)} lost sales and ${fmtCompactEur(agg.totals.trapped)} trapped working-capital proxy are concentrated enough to manage through targeted exceptions, not broad inventory expansion.`;
      document.getElementById('hero-action').textContent = topSku ? `${topSku.product_id} · ${topSku.warehouse_id}` : 'No priority item';
      document.getElementById('hero-action-detail').textContent = topSku ? `Priority ${topSku.governance_priority_score.toFixed(1)}, led by ${topSku.main_risk_driver.toLowerCase()} — ${topSku.recommended_action}.` : 'No rows in current filter.';
      document.getElementById('hero-value').textContent = fmtCompactEur(agg.totals.opportunity12m);
      document.getElementById('hero-value-detail').textContent = topCategory ? `Largest lost-sales pocket: ${topCategory.category} at ${fmtCompactEur(topCategory.lostSales)}.` : 'No category exposure.';
      document.getElementById('meta-scope').textContent = `${fmtNum(agg.skuRows.length)} SKU-location rows`;
      document.getElementById('meta-period').textContent = `${range.start.slice(0, 7)} → ${range.end.slice(0, 7)}`;
      document.getElementById('meta-refresh').textContent = `${dashboardData.generated_at}`;
      return { topSku, worstWarehouse, topSupplier, topCategory, criticalCount };
    }

    function renderKPIs(agg, context) {
      const series = agg.monthSeries;
      const halves = splitHalves(series);
      const fmtPp = v => `${(v * 100).toFixed(1)} pp`;
      const fmtEurMo = v => `${fmtCompactEur(v)}/mo`;
      const kpis = [
        {
          label: 'Fill rate', value: fmtPct(agg.totals.fillRate), tone: riskTone(agg.totals.fillRate, 0.97, 0.95),
          spark: sparklineSvg(series.map(x => x.fill_rate)),
          note: halves ? `${deltaSpan(halves.second.fill - halves.first.fill, 0.001, false, fmtPp)} · target <b>97%+</b>` : 'Target <b>97%+</b> · below 95% critical',
        },
        {
          label: 'Stockout rate', value: fmtPct(agg.totals.stockoutRate), tone: riskTone(agg.totals.stockoutRate, 0.02, 0.05, true),
          spark: sparklineSvg(series.map(x => x.stockout_rate)),
          note: halves ? `${deltaSpan(halves.second.stockout - halves.first.stockout, 0.001, true, fmtPp)} · healthy <b>≤2%</b>` : 'Healthy <b>≤2%</b> · above 5% critical',
        },
        {
          label: 'Lost-sales exposure', value: fmtCompactEur(agg.totals.lostSales), tone: agg.totals.lostSales > 1_000_000 ? 'bad' : 'warn',
          spark: sparklineSvg(series.map(x => x.lostSales)),
          note: halves ? `${deltaSpan(halves.second.lostSalesMonthly - halves.first.lostSalesMonthly, agg.totals.lostSales * 0.002, true, fmtEurMo)} · unmet demand at price` : 'Unmet demand at selling price',
        },
        {
          label: 'Trapped working capital', value: fmtCompactEur(agg.totals.trapped), tone: agg.totals.trapped > agg.totals.inventory * 0.12 ? 'warn' : 'good',
          spark: sparklineSvg(series.map(x => x.trapped)),
          note: halves ? `${deltaSpan(halves.second.trappedAvg - halves.first.trappedAvg, agg.totals.trapped * 0.01, true, fmtCompactEur)} · excess + slow-moving` : 'Excess DOS + slow-moving proxy',
        },
        {
          label: 'Supplier OTD', value: fmtPct(agg.totals.weightedSupplierOTD), tone: riskTone(agg.totals.weightedSupplierOTD, 0.92, 0.88),
          spark: '',
          note: 'Demand-weighted execution across active suppliers',
        },
        {
          label: '12-month opportunity', value: fmtCompactEur(agg.totals.opportunity12m), tone: 'accent',
          spark: '',
          note: 'Recoverable margin + releasable capital at current assumptions',
        },
      ];
      document.getElementById('kpi-grid').innerHTML = kpis.map(k => `
        <article class="kpi ${k.tone}">
          <div class="kpi-top"><span class="kpi-dot" aria-hidden="true"></span><span class="kpi-label">${escapeHtml(k.label)}</span></div>
          <div class="kpi-value">${escapeHtml(k.value)}</div>
          ${k.spark ? `<div class="kpi-spark">${k.spark}</div>` : ''}
          <div class="kpi-note">${k.note}</div>
        </article>
      `).join('');
    }

    function renderPriorities(agg, context) {
      const capitalCategory = [...agg.categories].sort((a,b) => b.excess - a.excess)[0];
      const imbalance = [...agg.segments].sort((a,b) => {
        const as = (1 - a.fill_rate) * 0.65 + norm(a.avg_dos, 20, 70) * 0.35;
        const bs = (1 - b.fill_rate) * 0.65 + norm(b.avg_dos, 20, 70) * 0.35;
        return bs - as;
      })[0];
      const cards = [
        { tone: 'bad', label: 'Service recovery', title: context.worstWarehouse ? context.worstWarehouse.warehouse_name : 'No warehouse in scope', body: context.worstWarehouse ? `Lowest service in scope: ${fmtPct(context.worstWarehouse.fill_rate)} fill with ${fmtCompactEur(context.worstWarehouse.lostSales)} lost sales. Run replenishment exceptions here first.` : 'Current filter has no warehouse rows.' },
        { tone: 'warn', label: 'Supplier exposure', title: context.topSupplier ? context.topSupplier.supplier_name : 'No supplier exposure', body: context.topSupplier ? `${fmtCompactEur(context.topSupplier.lostSales)} downstream lost sales at ${fmtPct(context.topSupplier.on_time_delivery_rate)} OTD. Review SLA recovery and backup sourcing.` : 'No supplier signal in current scope.' },
        { tone: 'warn', label: 'Capital to release', title: capitalCategory ? capitalCategory.category : 'No excess pocket', body: capitalCategory ? `${fmtCompactEur(capitalCategory.excess)} excess-value proxy. Freeze avoidable replenishment and force DOS-cap exceptions.` : 'No capital concentration in current scope.' },
        { tone: 'good', label: 'Policy conflict', title: imbalance ? `${imbalance.category} · ${imbalance.region}` : 'No segment', body: imbalance ? `${fmtPct(imbalance.fill_rate)} fill on ${imbalance.avg_dos.toFixed(0)} days of supply. Fix planning rules before adding network-wide inventory.` : 'No segment imbalance found.' },
      ];
      document.getElementById('priority-grid').innerHTML = cards.map(c => `
        <article class="triage ${c.tone}">
          <div class="triage-head"><span class="triage-dot" aria-hidden="true"></span><span class="triage-label">${escapeHtml(c.label)}</span></div>
          <div class="triage-title">${escapeHtml(c.title)}</div>
          <div class="triage-body">${escapeHtml(c.body)}</div>
        </article>
      `).join('');
    }

    function renderCharts(agg) {
      const c = palette();
      const months = agg.monthSeries.map(x => x.month.slice(0, 7));
      const legendStyle = { orientation: 'h', y: -0.18, x: 0, xanchor: 'left', font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", Geist, system-ui, sans-serif', size: 11, color: c.muted } };
      const monoTicks = { family: 'Geist Mono, monospace', size: 10.5, color: c.faint };

      const fillValues = agg.monthSeries.map(x => x.fill_rate);
      const fillFloor = Math.max(0, Math.min(...fillValues, 0.95) - 0.015);
      Plotly.react('chart-trend', [
        { x: months, y: fillValues, name: 'Fill rate', type: 'scatter', mode: 'lines', line: { color: c.accent, width: 2.5, shape: 'spline', smoothing: 0.6 }, hovertemplate: '%{x}<br>Fill rate  %{y:.1%}<extra></extra>' },
      ], {
        ...baseLayout(),
        yaxis: { tickformat: '.0%', range: [fillFloor, 1.004], gridcolor: c.grid, zeroline: false, tickfont: monoTicks },
        xaxis: { gridcolor: 'rgba(0,0,0,0)', nticks: 7, tickfont: monoTicks },
        shapes: [
          { type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 0.97, y1: 0.97, line: { color: c.good, width: 1, dash: 'dot' }, layer: 'below' },
          { type: 'rect', xref: 'paper', x0: 0, x1: 1, y0: fillFloor, y1: 0.95, fillcolor: c.bad, opacity: 0.05, line: { width: 0 }, layer: 'below' },
        ],
        annotations: [
          { xref: 'paper', x: 1, y: 0.97, xanchor: 'right', yanchor: 'bottom', text: 'target 97%', showarrow: false, font: { family: 'Geist Mono, monospace', size: 10, color: c.good } },
          { xref: 'paper', x: 0.005, y: 0.95, xanchor: 'left', yanchor: 'top', text: 'critical below 95%', showarrow: false, font: { family: 'Geist Mono, monospace', size: 10, color: c.bad } },
        ],
      }, plotConfig);

      Plotly.react('chart-value-trend', [
        { x: months, y: agg.monthSeries.map(x => x.lostSales), name: 'Lost sales', type: 'bar', marker: { color: c.accentDim, line: { color: c.accent, width: 1 } }, hovertemplate: 'Lost sales  €%{y:,.0f}<extra></extra>' },
        { x: months, y: agg.monthSeries.map(x => x.trapped), name: 'Trapped WC', type: 'scatter', mode: 'lines', yaxis: 'y2', line: { color: c.slate, width: 2, shape: 'spline', smoothing: 0.6 }, hovertemplate: 'Trapped WC  €%{y:,.0f}<extra></extra>' },
      ], {
        ...baseLayout(),
        showlegend: true,
        legend: legendStyle,
        bargap: 0.45,
        margin: { l: 58, r: 54, t: 14, b: 46 },
        yaxis: { tickprefix: '€', tickformat: '~s', gridcolor: c.grid, zeroline: false, tickfont: monoTicks },
        yaxis2: { tickprefix: '€', tickformat: '~s', overlaying: 'y', side: 'right', showgrid: false, zeroline: false, tickfont: monoTicks },
        xaxis: { gridcolor: 'rgba(0,0,0,0)', nticks: 7, tickfont: monoTicks },
      }, plotConfig);

      const wh = [...agg.warehouses].sort((a,b) => b.lostSales - a.lostSales);
      Plotly.react('chart-bottlenecks', [{
        y: wh.map(x => short(x.warehouse_name, 26)).reverse(),
        x: wh.map(x => x.lostSales).reverse(),
        type: 'bar',
        orientation: 'h',
        marker: { color: wh.map(x => x.fill_rate < 0.95 ? c.bad : c.accent).reverse() },
        text: wh.map(x => `${fmtCompactEur(x.lostSales)} · fill ${fmtPct(x.fill_rate)}`).reverse(),
        textposition: 'outside',
        cliponaxis: false,
        textfont: { family: 'Geist Mono, monospace', size: 10.5, color: c.muted },
        customdata: wh.map(x => `${x.warehouse_id} · fill ${fmtPct(x.fill_rate)}`).reverse(),
        hovertemplate: '%{customdata}<br>Lost sales  €%{x:,.0f}<extra></extra>',
      }], { ...baseLayout(), bargap: 0.42, xaxis: { tickprefix: '€', tickformat: '~s', gridcolor: c.grid, zeroline: false, tickfont: monoTicks, range: [0, Math.max(...wh.map(x => x.lostSales), 1) * 1.38] }, yaxis: { ticklabelstandoff: 10, tickfont: monoTicks }, margin: { l: 184, r: 16, t: 14, b: 40 } }, plotConfig);

      const cat = [...agg.categories].sort((a,b) => b.trapped - a.trapped);
      Plotly.react('chart-category-capital', [{
        y: cat.map(x => x.category).reverse(),
        x: cat.map(x => x.trapped).reverse(),
        type: 'bar',
        orientation: 'h',
        marker: { color: c.accent },
        text: cat.map(x => fmtCompactEur(x.trapped)).reverse(),
        textposition: 'outside',
        cliponaxis: false,
        textfont: { family: 'Geist Mono, monospace', size: 10.5, color: c.muted },
        hovertemplate: '%{y}<br>Trapped WC  €%{x:,.0f}<extra></extra>',
      }], { ...baseLayout(), bargap: 0.42, xaxis: { tickprefix: '€', tickformat: '~s', gridcolor: c.grid, zeroline: false, tickfont: monoTicks, range: [0, Math.max(...cat.map(x => x.trapped), 1) * 1.22] }, yaxis: { ticklabelstandoff: 10, tickfont: monoTicks }, margin: { l: 124, r: 16, t: 14, b: 40 } }, plotConfig);

      const seg = agg.segments;
      const isConflict = x => x.fill_rate < 0.97 && x.avg_dos > 30;
      Plotly.react('chart-tradeoff', [{
        x: seg.map(x => x.avg_dos),
        y: seg.map(x => x.fill_rate),
        mode: 'markers',
        marker: {
          size: seg.map(x => clamp(x.lostSales / 180000, 9, 32)),
          color: seg.map(x => isConflict(x) ? c.bad : c.accent),
          line: { color: c.paper, width: 1 },
          opacity: 0.85,
        },
        customdata: seg.map(x => `${x.category} · ${x.region} · excess ${fmtCompactEur(x.excess)}`),
        hovertemplate: '%{customdata}<br>DOS %{x:.0f}  ·  Fill %{y:.1%}<extra></extra>',
      }], {
        ...baseLayout(),
        xaxis: { title: { text: 'Average days of supply', font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", Geist, system-ui, sans-serif', size: 11, color: c.muted } }, gridcolor: c.grid, zeroline: false, tickfont: monoTicks },
        yaxis: { title: { text: 'Fill rate', font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", Geist, system-ui, sans-serif', size: 11, color: c.muted } }, tickformat: '.0%', gridcolor: c.grid, zeroline: false, tickfont: monoTicks },
        shapes: [
          { type: 'line', x0: 30, x1: 30, y0: 0, y1: 1, yref: 'paper', line: { color: c.faint, dash: 'dot', width: 1 } },
          { type: 'line', x0: 0, x1: 1, xref: 'paper', y0: 0.97, y1: 0.97, line: { color: c.faint, dash: 'dot', width: 1 } },
        ],
        annotations: [
          { x: 30, y: 1, yref: 'paper', xanchor: 'left', yanchor: 'top', text: ' 30d', showarrow: false, font: { family: 'Geist Mono, monospace', size: 10, color: c.faint } },
          { xref: 'paper', x: 1, y: 0.97, xanchor: 'right', yanchor: 'bottom', text: '97% ', showarrow: false, font: { family: 'Geist Mono, monospace', size: 10, color: c.faint } },
        ],
      }, plotConfig);

      const sup = [...agg.suppliers].sort((a,b) => b.lostSales - a.lostSales).slice(0, 8).reverse();
      Plotly.react('chart-supplier', [{
        y: sup.map(x => short(x.supplier_name, 22)),
        x: sup.map(x => x.lostSales),
        type: 'bar',
        orientation: 'h',
        marker: { color: sup.map(x => x.on_time_delivery_rate < 0.60 ? c.bad : c.accent) },
        text: sup.map(x => `OTD ${fmtPct(x.on_time_delivery_rate)}`),
        textposition: 'outside',
        cliponaxis: false,
        textfont: { family: 'Geist Mono, monospace', size: 10, color: c.muted },
        customdata: sup.map(x => `OTD ${fmtPct(x.on_time_delivery_rate)} · delay ${x.average_delay_days.toFixed(1)}d`),
        hovertemplate: '%{y}<br>%{customdata}<br>Lost sales  €%{x:,.0f}<extra></extra>',
      }], { ...baseLayout(), bargap: 0.4, xaxis: { tickprefix: '€', tickformat: '~s', gridcolor: c.grid, zeroline: false, tickfont: monoTicks, range: [0, Math.max(...sup.map(x => x.lostSales), 1) * 1.5] }, yaxis: { ticklabelstandoff: 10, tickfont: monoTicks }, margin: { l: 104, r: 16, t: 14, b: 40 } }, plotConfig);

      const ranked = [...agg.skuRows].sort((a,b) => b.lostSales - a.lostSales);
      const totalLost = ranked.reduce((sum, x) => sum + x.lostSales, 0) || 1;
      let running = 0;
      const cumShare = ranked.map(x => (running += x.lostSales) / totalLost);
      const topRanked = ranked.slice(0, 12);
      const topShareIdx = Math.min(9, ranked.length - 1);
      const topShare = ranked.length ? cumShare[topShareIdx] : 0;
      Plotly.react('chart-governance', [
        {
          x: topRanked.map((_, i) => i + 1),
          y: topRanked.map(x => x.lostSales),
          type: 'bar',
          name: 'Lost sales',
          marker: { color: topRanked.map(x => x.risk_tier === 'Critical' || x.risk_tier === 'High' ? c.bad : c.accent) },
          customdata: topRanked.map(x => `${x.product_id} · ${x.warehouse_id} · priority ${x.governance_priority_score.toFixed(1)}`),
          hovertemplate: '%{customdata}<br>Lost sales  €%{y:,.0f}<extra></extra>',
        },
        {
          x: topRanked.map((_, i) => i + 1),
          y: cumShare.slice(0, topRanked.length),
          yaxis: 'y2',
          type: 'scatter',
          mode: 'lines+markers',
          name: 'Cumulative share',
          line: { color: c.slate, width: 2 },
          marker: { size: 5, color: c.slate },
          hovertemplate: 'Top %{x} rows  %{y:.0%} of lost sales<extra></extra>',
        },
      ], {
        ...baseLayout(),
        bargap: 0.4,
        margin: { l: 58, r: 50, t: 14, b: 50 },
        xaxis: { title: { text: 'SKU-location rank by lost sales', font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", Geist, system-ui, sans-serif', size: 11, color: c.muted } }, dtick: 1, gridcolor: 'rgba(0,0,0,0)', tickfont: monoTicks },
        yaxis: { tickprefix: '€', tickformat: '~s', range: [0, Math.max(...topRanked.map(x => x.lostSales), 1) * 1.08], gridcolor: c.grid, zeroline: false, tickfont: monoTicks },
        yaxis2: { tickformat: '.0%', overlaying: 'y', side: 'right', range: [0, 1.04], showgrid: false, zeroline: false, tickfont: monoTicks },
      }, plotConfig);
      document.getElementById('pareto-kicker').innerHTML = ranked.length
        ? `Top ${Math.min(10, ranked.length)} of ${fmtNum(ranked.length)} SKU-locations carry <b>${fmtPct(topShare)}</b> of lost sales`
        : 'No rows in current scope';
    }

    function renderTable(rows) {
      const query = (tableSearch.value || '').toLowerCase().trim();
      let data = rows.filter(r => {
        if (!query) return true;
        return `${r.product_id} ${r.product_name} ${r.warehouse_id} ${r.supplier_id} ${r.main_risk_driver} ${r.recommended_action}`.toLowerCase().includes(query);
      });
      data.sort((a,b) => {
        const dir = tableSort.dir === 'asc' ? 1 : -1;
        const av = a[tableSort.key], bv = b[tableSort.key];
        if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
        return String(av).localeCompare(String(bv)) * dir;
      });
      const shown = data.slice(0, 250);
      tableBody.innerHTML = shown.map(r => `
        <tr>
          <td class="score num">${r.governance_priority_score.toFixed(1)}</td>
          <td><b style="font-weight:600">${escapeHtml(r.product_id)}</b></td>
          <td class="wrap">${escapeHtml(r.product_name)}</td>
          <td>${escapeHtml(r.warehouse_id)}</td>
          <td>${escapeHtml(r.supplier_id)}</td>
          <td class="num">${fmtPct(r.fill_rate)}</td>
          <td class="num">${fmtPct(r.stockout_rate)}</td>
          <td class="num">${fmtCompactEur(r.lostSales)}</td>
          <td><span class="badge tier-${escapeHtml(String(r.risk_tier).toLowerCase())}">${escapeHtml(r.risk_tier)}</span></td>
          <td>${escapeHtml(r.main_risk_driver)}</td>
          <td class="wrap">${escapeHtml(r.recommended_action)}</td>
        </tr>
      `).join('');
      tableMeta.textContent = `Showing ${fmtNum(shown.length)} of ${fmtNum(data.length)} rows`;
    }

    function renderBrief(agg, context) {
      const balancedShare = agg.skuRows.filter(x => x.fill_rate >= 0.97 && x.avg_dos >= 8 && x.avg_dos <= 35).length / Math.max(agg.skuRows.length, 1);
      const cards = [
        ['Service position', `Fill is ${fmtPct(agg.totals.fillRate)} and stockout is ${fmtPct(agg.totals.stockoutRate)}. Use the priority queue to manage the highest-score SKU locations.`],
        ['Operational impact', `${fmtCompactEur(agg.totals.lostSales)} observed lost sales sits alongside ${fmtCompactEur(agg.totals.trapped)} trapped working-capital proxy. Both sides of the service-capital trade-off need active control.`],
        ['Supplier signal to investigate', context.topSupplier ? `${context.topSupplier.supplier_name} has ${fmtPct(context.topSupplier.on_time_delivery_rate)} OTD and ${fmtCompactEur(context.topSupplier.lostSales)} downstream lost sales. Validate supplier recovery before changing network-wide safety stock.` : 'No supplier signal in this filtered view.'],
        ['Decision rule', `Only ${fmtPct(balancedShare)} of rows are balanced efficient. Raise service on the top queue first, then release capital from excess pockets after confirming demand risk.`],
      ];
      document.getElementById('brief-grid').innerHTML = cards.map(([title, body]) => `
        <article class="brief">
          <div class="brief-title">${escapeHtml(title)}</div>
          <div class="brief-copy">${escapeHtml(body)}</div>
        </article>
      `).join('');
    }

    function renderStatus(agg, range, rows) {
      const alert = document.getElementById('status-alert');
      if (!rows.length) {
        alert.style.display = 'block';
        alert.textContent = 'No records match the current filters. Reset filters or widen the date range.';
        return;
      }
      const fullRange = range.start === `${dashboardData.meta.date_min.slice(0, 7)}-01` && range.end === `${dashboardData.meta.date_max.slice(0, 7)}-01`;
      const allFilters = Object.entries(filters).filter(([k]) => !['start', 'end'].includes(k)).every(([,el]) => el.value === 'ALL');
      const snap = dashboardData.meta.official_snapshot || {};
      const reconciles = Math.abs((snap.overall_fill_rate || 0) - agg.totals.fillRate) <= 0.0005 &&
        Math.abs((snap.overall_stockout_rate || 0) - agg.totals.stockoutRate) <= 0.0005 &&
        Math.abs((snap.total_lost_sales_revenue || 0) - agg.totals.lostSales) <= 1 &&
        Math.abs((snap.trapped_working_capital_proxy_average || 0) - agg.totals.trapped) <= 1;
      if (fullRange && allFilters && !reconciles) {
        alert.style.display = 'block';
        alert.textContent = 'QA warning: default dashboard KPIs do not reconcile to the official governed snapshot.';
        return;
      }
      alert.style.display = 'none';
      alert.textContent = '';
    }

    function updateDashboard() {
      const range = getDateRange();
      const rows = monthlyFact.filter(r => rowPasses(r, range));
      const agg = aggregate(rows, readScenario());
      currentAgg = agg;
      const context = renderHero(agg, range);
      renderKPIs(agg, context);
      renderPriorities(agg, context);
      renderCharts(agg);
      renderTable(agg.skuRows);
      renderBrief(agg, context);
      renderStatus(agg, range, rows);
    }

    function resetFilters() {
      filters.region.value = 'ALL';
      filters.warehouse.value = 'ALL';
      filters.category.value = 'ALL';
      filters.supplier.value = 'ALL';
      filters.abc.value = 'ALL';
      filters.start.value = dashboardData.meta.date_min.slice(0, 7);
      filters.end.value = dashboardData.meta.date_max.slice(0, 7);
      tableSearch.value = '';
      tableSort = { key: 'governance_priority_score', dir: 'desc' };
      document.querySelectorAll('#detail-table th').forEach(header => header.setAttribute('aria-sort', 'none'));
      document.querySelector('#detail-table th[data-key="governance_priority_score"]').setAttribute('aria-sort', 'descending');
      updateDashboard();
    }

    function togglePanel(id, button, openLabel, closedLabel) {
      const panel = document.getElementById(id);
      const open = panel.style.display !== 'block';
      panel.style.display = open ? 'block' : 'none';
      button.setAttribute('aria-expanded', open ? 'true' : 'false');
      button.textContent = open ? openLabel : closedLabel;
    }

    function applyTheme(mode, redraw = true) {
      currentTheme = mode === 'dark' ? 'dark' : 'light';
      document.documentElement.setAttribute('data-theme', currentTheme);
      localStorage.setItem('supply_chain_dashboard_theme', currentTheme);
      document.getElementById('toggle-theme').textContent = currentTheme === 'dark' ? 'Light' : 'Dark';
      if (redraw && currentAgg) renderCharts(currentAgg);
    }

    function initEvents() {
      Object.values(filters).forEach(el => el.addEventListener('change', updateDashboard));
      [scenario.marginRate, scenario.wcRate, scenario.slowWeight].forEach(el => {
        el.addEventListener('input', () => {
          updateScenarioLabels();
          updateDashboard();
        });
      });
      tableSearch.addEventListener('input', () => currentAgg && renderTable(currentAgg.skuRows));
      document.getElementById('reset-filters').addEventListener('click', resetFilters);
      document.getElementById('print-dashboard').addEventListener('click', () => window.print());
      document.getElementById('toggle-scenario').addEventListener('click', e => togglePanel('scenario-panel', e.currentTarget, 'Hide scenario controls', 'Scenario controls'));
      document.getElementById('toggle-method').addEventListener('click', e => togglePanel('method-panel', e.currentTarget, 'Hide method notes', 'Method notes'));
      document.getElementById('toggle-theme').addEventListener('click', () => applyTheme(currentTheme === 'dark' ? 'light' : 'dark'));
      document.querySelectorAll('#detail-table th').forEach(th => {
        const sort = () => {
          const key = th.dataset.key;
          tableSort = tableSort.key === key ? { key, dir: tableSort.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'desc' };
          document.querySelectorAll('#detail-table th').forEach(header => header.setAttribute('aria-sort', 'none'));
          th.setAttribute('aria-sort', tableSort.dir === 'asc' ? 'ascending' : 'descending');
          if (currentAgg) renderTable(currentAgg.skuRows);
        };
        th.addEventListener('click', sort);
        th.addEventListener('keydown', e => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            sort();
          }
        });
      });
      window.addEventListener('resize', () => {
        if (!currentAgg) return;
        ['chart-trend', 'chart-value-trend', 'chart-bottlenecks', 'chart-category-capital', 'chart-tradeoff', 'chart-supplier', 'chart-governance'].forEach(id => {
          const el = document.getElementById(id);
          if (el) Plotly.Plots.resize(el);
        });
      });
    }

    initializeControls();
    document.getElementById('foot-provenance').textContent =
      `Dataset ${dashboardData.dashboard_version} · ${dashboardData.generated_at} · ${fmtNum(dashboardData.meta.row_count_monthly_sku)} monthly records`;
    const savedTheme = localStorage.getItem('supply_chain_dashboard_theme');
    const preferredTheme = savedTheme || (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    applyTheme(preferredTheme, false);
    initEvents();
    updateDashboard();
  </script>
</body>
</html>
"""

    rendered = (
        template.replace("__PLOTLY_CDN_URL__", PLOTLY_CDN_URL)
        .replace("__PLOTLY_SRI__", PLOTLY_SRI)
        .replace("__DATA_JSON__", data_json)
    )
    return "\n".join(line.rstrip() for line in rendered.splitlines()) + "\n"


def build_executive_dashboard() -> Path:
    data_payload = _prepare_dashboard_data()
    html = _build_html(data_payload)
    OUTPUT_DASHBOARD_FILE.write_text(html, encoding="utf-8")

    return OUTPUT_DASHBOARD_FILE


def main() -> None:
    output_path = build_executive_dashboard()
    print(f"Executive dashboard generated: {output_path}")


if __name__ == "__main__":
    main()
