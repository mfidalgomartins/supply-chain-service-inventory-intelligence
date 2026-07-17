"""Builds the static executive dashboard (index.html).

Prepares a compact monthly payload from the processed layers, stabilises
float serialisation so the published file is byte-identical across Python
and NumPy versions, and renders the single-file HTML template with an
SRI-pinned Plotly bundle. All risk scores shown in the dashboard come from
the governed baseline tables — nothing is recomputed in the browser."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import ABC_DOS_CAPS, DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
from src.impact_analysis import ASSUMPTIONS

OUTPUT_DASHBOARD_FILE = PROJECT_ROOT / "index.html"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
DASHBOARD_TEMPLATE_FILE = PROJECT_ROOT / "templates" / "executive_dashboard.html"
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
    daily["dos_cap"] = daily["abc_class"].map(ABC_DOS_CAPS)
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
        daily["excess_inventory_proxy"]
        + ASSUMPTIONS.slow_moving_incremental_weight * daily["slow_moving_non_excess_proxy"]
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
                "recoverable_margin_rate": ASSUMPTIONS.recoverable_lost_margin_rate_12m,
                "releasable_wc_rate": ASSUMPTIONS.releasable_trapped_wc_rate_12m,
                "slow_moving_incremental_weight": ASSUMPTIONS.slow_moving_incremental_weight,
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
    data_json = (
        data_json.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )

    template = "\n" + DASHBOARD_TEMPLATE_FILE.read_text(encoding="utf-8")

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
