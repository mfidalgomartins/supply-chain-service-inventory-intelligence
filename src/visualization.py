from __future__ import annotations

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


matplotlib.use("Agg")

OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


def _pct(x: float, _: int) -> str:
    return f"{x:.0%}"


def _eur_mn(x: float, _: int) -> str:
    return f"EUR {x/1_000_000:.1f}M"


def _prepare_data() -> dict[str, pd.DataFrame]:
    daily = pd.read_csv(DATA_PROCESSED / "daily_product_warehouse_metrics.csv", parse_dates=["date"])
    supplier = pd.read_csv(DATA_PROCESSED / "supplier_performance_summary.csv")
    warehouse = pd.read_csv(DATA_PROCESSED / "warehouse_service_profile.csv")
    sku = pd.read_csv(DATA_PROCESSED / "sku_risk_table.csv")
    products = pd.read_csv(DATA_RAW / "products.csv")[["product_id", "product_name"]]

    daily = daily.copy()
    daily["month"] = daily["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        daily.groupby("month", as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_fulfilled=("units_fulfilled", "sum"),
            units_lost_sales=("units_lost_sales", "sum"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
        )
        .assign(
            fill_rate=lambda d: np.where(d["units_demanded"] > 0, d["units_fulfilled"] / d["units_demanded"], 1.0),
            stockout_rate=lambda d: np.where(d["units_demanded"] > 0, d["units_lost_sales"] / d["units_demanded"], 0.0),
        )
    )

    category_service = (
        daily.groupby("category", as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_fulfilled=("units_fulfilled", "sum"),
        )
        .assign(fill_rate=lambda d: np.where(d["units_demanded"] > 0, d["units_fulfilled"] / d["units_demanded"], 1.0))
        .sort_values("fill_rate", ascending=True)
    )

    region_lost_sales = daily.groupby("region", as_index=False)["lost_sales_revenue"].sum().sort_values(
        "lost_sales_revenue", ascending=False
    )

    category_inventory = (
        daily.groupby("category", as_index=False)
        .agg(avg_inventory_value=("inventory_value", "mean"), total_inventory_value=("inventory_value", "sum"))
        .sort_values("total_inventory_value", ascending=False)
    )
    category_inventory["inventory_share"] = category_inventory["total_inventory_value"] / category_inventory[
        "total_inventory_value"
    ].sum()
    category_inventory["cumulative_share"] = category_inventory["inventory_share"].cumsum()

    dos_distribution = daily[["days_of_supply"]].copy()

    category_region_scatter = (
        daily.groupby(["category", "region"], as_index=False)
        .agg(
            units_demanded=("units_demanded", "sum"),
            units_fulfilled=("units_fulfilled", "sum"),
            avg_inventory_value=("inventory_value", "mean"),
            lost_sales_revenue=("lost_sales_revenue", "sum"),
        )
        .assign(fill_rate=lambda d: np.where(d["units_demanded"] > 0, d["units_fulfilled"] / d["units_demanded"], 1.0))
    )

    sku_rank = (
        sku.merge(products, on="product_id", how="left")
        .assign(product_name=lambda d: d["product_name"].fillna(d["product_id"]))
        .assign(sku_label=lambda d: d["product_id"] + " | " + d["warehouse_id"])
        .sort_values("governance_priority_score", ascending=False)
    )

    excess_ranking = (
        sku.groupby("category", as_index=False)
        .agg(
            excess_inventory_value_proxy=("inventory_value_sum", lambda x: float(np.sum(x))),
            avg_excess_rate=("excess_day_rate", "mean"),
        )
        .assign(excess_exposure_proxy=lambda d: d["excess_inventory_value_proxy"] * d["avg_excess_rate"])
        .sort_values("excess_exposure_proxy", ascending=False)
    )

    slow_moving_ranking = (
        sku.assign(slow_moving_value_proxy=lambda d: d["inventory_value_sum"] * d["slow_moving_rate"])
        .groupby(["product_id", "warehouse_id"], as_index=False)
        .agg(slow_moving_value_proxy=("slow_moving_value_proxy", "sum"), slow_moving_rate=("slow_moving_rate", "mean"))
        .sort_values("slow_moving_value_proxy", ascending=False)
        .head(15)
    )
    slow_moving_ranking["sku_label"] = slow_moving_ranking["product_id"] + " | " + slow_moving_ranking["warehouse_id"]

    warehouse_quadrant = warehouse.copy()

    supplier_heatmap = supplier.copy()
    supplier_heatmap["otd_gap"] = 1 - supplier_heatmap["on_time_delivery_rate"]
    supplier_heatmap["avg_delay_norm"] = supplier_heatmap["average_delay_days"] / max(
        supplier_heatmap["average_delay_days"].max(), 1e-9
    )
    supplier_heatmap["lt_variability_norm"] = supplier_heatmap["lead_time_variability"] / max(
        supplier_heatmap["lead_time_variability"].max(), 1e-9
    )
    supplier_heatmap["risk_proxy_norm"] = supplier_heatmap["supplier_service_risk_proxy"] / max(
        supplier_heatmap["supplier_service_risk_proxy"].max(), 1e-9
    )
    supplier_heatmap = supplier_heatmap.sort_values("supplier_service_risk_proxy", ascending=False)

    return {
        "monthly": monthly,
        "warehouse": warehouse,
        "category_service": category_service,
        "region_lost_sales": region_lost_sales,
        "category_inventory": category_inventory,
        "dos_distribution": dos_distribution,
        "supplier": supplier,
        "category_region_scatter": category_region_scatter,
        "sku_rank": sku_rank,
        "excess_ranking": excess_ranking,
        "slow_moving_ranking": slow_moving_ranking,
        "warehouse_quadrant": warehouse_quadrant,
        "supplier_heatmap": supplier_heatmap,
    }


def _save_chart_data(data: dict[str, pd.DataFrame]) -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in data.items():
        if isinstance(df, pd.DataFrame):
            df.to_csv(OUTPUT_TABLES_DIR / f"viz_data_{name}.csv", index=False)


def _plot_service_level_trend(monthly: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.5))
    sns.lineplot(data=monthly, x="month", y="fill_rate", marker="o", linewidth=2.5, color="#1F4E79", ax=ax)
    worst = monthly.loc[monthly["fill_rate"].idxmin()]
    ax.scatter([worst["month"]], [worst["fill_rate"]], color="#C53030", s=80, zorder=5)
    ax.annotate(
        f"Lowest service: {worst['fill_rate']:.1%}",
        (worst["month"], worst["fill_rate"]),
        textcoords="offset points",
        xytext=(8, -20),
    )
    ax.set_title("Service Level Erosion Is Seasonal, With Deep Year-End Troughs")
    ax.set_ylabel("Fill Rate")
    ax.set_xlabel("Month")
    ax.yaxis.set_major_formatter(FuncFormatter(_pct))
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_01_service_level_trend.png", dpi=220)
    plt.close(fig)


def _plot_stockout_trend(monthly: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.5))
    sns.lineplot(data=monthly, x="month", y="stockout_rate", marker="o", linewidth=2.5, color="#9B2C2C", ax=ax)
    peak = monthly.loc[monthly["stockout_rate"].idxmax()]
    ax.scatter([peak["month"]], [peak["stockout_rate"]], color="#742A2A", s=80, zorder=5)
    ax.annotate(
        f"Peak stockout: {peak['stockout_rate']:.1%}",
        (peak["month"], peak["stockout_rate"]),
        textcoords="offset points",
        xytext=(8, 10),
    )
    ax.set_title("Stockout Pressure Spikes in Peak Demand Windows")
    ax.set_ylabel("Stockout Rate")
    ax.set_xlabel("Month")
    ax.yaxis.set_major_formatter(FuncFormatter(_pct))
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_02_stockout_rate_trend.png", dpi=220)
    plt.close(fig)


def _plot_fill_rate_by_warehouse(warehouse: pd.DataFrame) -> None:
    df = warehouse.sort_values("fill_rate", ascending=True).copy()
    df["warehouse_label"] = df["warehouse_name"].apply(lambda x: x if len(x) <= 22 else f"{x[:19]}...")
    fig, ax = plt.subplots(figsize=(10.2, 5.4))
    sns.barplot(data=df, y="warehouse_label", x="fill_rate", color="#2C5282", ax=ax)
    ax.axvline(0.95, linestyle="--", color="#4A5568", linewidth=1.2, label="95% target reference")
    ax.set_title("Service Imbalance by Warehouse Highlights Execution Hotspots")
    ax.set_xlabel("Fill Rate")
    ax.set_ylabel("Warehouse")
    ax.xaxis.set_major_formatter(FuncFormatter(_pct))
    ax.grid(axis="x", alpha=0.25)
    ax.legend(loc="lower right", frameon=False)
    ax.tick_params(axis="y", labelsize=9)
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_03_fill_rate_by_warehouse.png", dpi=220)
    plt.close(fig)


def _plot_fill_rate_by_category(category_service: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10.2, 5.6))
    sns.barplot(data=category_service, y="category", x="fill_rate", color="#2B6CB0", ax=ax)
    ax.axvline(0.95, linestyle="--", color="#4A5568", linewidth=1.2)
    ax.set_title("Health and Pet Care Categories Drive Disproportionate Service Risk")
    ax.set_xlabel("Fill Rate")
    ax.set_ylabel("Category")
    ax.xaxis.set_major_formatter(FuncFormatter(_pct))
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_04_fill_rate_by_category.png", dpi=220)
    plt.close(fig)


def _plot_lost_sales_by_region(region_lost_sales: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9.8, 5.6))
    sns.barplot(data=region_lost_sales, y="region", x="lost_sales_revenue", color="#C53030", ax=ax)
    top = region_lost_sales.iloc[0]
    ax.annotate(
        f"Largest exposure: EUR {top['lost_sales_revenue']/1_000_000:.1f}M",
        (top["lost_sales_revenue"], 0),
        xytext=(8, -12),
        textcoords="offset points",
    )
    ax.set_title("Lost Sales Exposure Concentrates in Spain Central and France South-East")
    ax.set_xlabel("Lost Sales Revenue")
    ax.set_ylabel("Region")
    ax.xaxis.set_major_formatter(FuncFormatter(_eur_mn))
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_05_lost_sales_by_region.png", dpi=220)
    plt.close(fig)


def _plot_inventory_concentration_by_category(category_inventory: pd.DataFrame) -> None:
    fig, ax1 = plt.subplots(figsize=(10.5, 5.8))
    sns.barplot(data=category_inventory, x="category", y="inventory_share", color="#2C5282", ax=ax1)
    ax1.set_ylabel("Inventory Share")
    ax1.yaxis.set_major_formatter(FuncFormatter(_pct))
    ax1.tick_params(axis="x", rotation=20)

    ax2 = ax1.twinx()
    sns.lineplot(
        data=category_inventory,
        x="category",
        y="cumulative_share",
        marker="o",
        color="#C53030",
        linewidth=2.2,
        ax=ax2,
    )
    ax2.set_ylabel("Cumulative Share")
    ax2.yaxis.set_major_formatter(FuncFormatter(_pct))
    ax2.set_ylim(0, 1.05)

    ax1.set_title("Inventory Value Is Highly Concentrated in a Small Category Set")
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_06_inventory_value_concentration_by_category.png", dpi=220)
    plt.close(fig)


def _plot_days_of_supply_distribution(dos_distribution: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10.5, 5.8))
    sns.histplot(dos_distribution, x="days_of_supply", bins=60, color="#2F855A", kde=True, ax=ax)
    p90 = float(dos_distribution["days_of_supply"].quantile(0.90))
    ax.axvline(p90, color="#C53030", linestyle="--", linewidth=1.5)
    ax.annotate(f"P90 = {p90:.1f} days", (p90, ax.get_ylim()[1] * 0.75), xytext=(8, 0), textcoords="offset points")
    ax.set_xlim(0, float(dos_distribution["days_of_supply"].quantile(0.99)))
    ax.set_title("Days of Supply Distribution Shows a Long Overstock Tail")
    ax.set_xlabel("Days of Supply")
    ax.set_ylabel("Count")
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_07_days_of_supply_distribution.png", dpi=220)
    plt.close(fig)


def _plot_supplier_otd(supplier: pd.DataFrame) -> None:
    df = supplier.sort_values("on_time_delivery_rate", ascending=True)
    fig, ax = plt.subplots(figsize=(10.0, 6.0))
    sns.barplot(data=df, y="supplier_name", x="on_time_delivery_rate", color="#805AD5", ax=ax)
    ax.axvline(0.80, color="#4A5568", linestyle="--", linewidth=1.2)
    ax.set_title("Supplier On-Time Delivery Gaps Create Systemic Service Vulnerability")
    ax.set_xlabel("On-Time Delivery Rate")
    ax.set_ylabel("Supplier")
    ax.xaxis.set_major_formatter(FuncFormatter(_pct))
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_08_supplier_otd_comparison.png", dpi=220)
    plt.close(fig)


def _plot_lead_time_variability(supplier: pd.DataFrame) -> None:
    df = supplier.sort_values("lead_time_variability", ascending=False)
    fig, ax = plt.subplots(figsize=(10.0, 6.0))
    sns.barplot(data=df, y="supplier_name", x="lead_time_variability", color="#9B2C2C", ax=ax)
    ax.set_title("Lead Time Variability Is Elevated for Critical Suppliers")
    ax.set_xlabel("Lead Time Variability (std dev days)")
    ax.set_ylabel("Supplier")
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_09_lead_time_variability_comparison.png", dpi=220)
    plt.close(fig)


def _plot_service_vs_inventory_scatter(category_region_scatter: pd.DataFrame) -> None:
    df = category_region_scatter.copy()
    fig, ax = plt.subplots(figsize=(10.8, 6.0))
    sns.scatterplot(
        data=df,
        x="avg_inventory_value",
        y="fill_rate",
        size="lost_sales_revenue",
        hue="region",
        sizes=(40, 500),
        alpha=0.75,
        ax=ax,
    )
    worst = df.sort_values("fill_rate", ascending=True).head(3)
    for row in worst.itertuples(index=False):
        ax.annotate(
            f"{row.category} | {row.region}",
            (row.avg_inventory_value, row.fill_rate),
            textcoords="offset points",
            xytext=(6, 6),
            fontsize=8,
        )
    ax.axhline(0.95, linestyle="--", color="#4A5568", linewidth=1.2)
    ax.set_title("Higher Inventory Value Does Not Guarantee Service Performance")
    ax.set_xlabel("Average Inventory Value")
    ax.set_ylabel("Fill Rate")
    ax.yaxis.set_major_formatter(FuncFormatter(_pct))
    ax.xaxis.set_major_formatter(FuncFormatter(_eur_mn))
    ax.legend(loc="lower left", bbox_to_anchor=(1.02, 0.0), borderaxespad=0)
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_10_service_vs_inventory_scatter.png", dpi=220)
    plt.close(fig)


def _plot_top_governance_priority_skus(sku_rank: pd.DataFrame) -> None:
    top = sku_rank.head(15).copy().sort_values("governance_priority_score", ascending=True)
    fig, ax = plt.subplots(figsize=(11.0, 7.2))
    sns.barplot(data=top, y="sku_label", x="governance_priority_score", hue="main_risk_driver", dodge=False, ax=ax)
    ax.set_title("Priority Queue: Governance Score Identifies Immediate SKU-Warehouse Interventions")
    ax.set_xlabel("Governance Priority Score")
    ax.set_ylabel("SKU | Warehouse")
    ax.legend(title="Main Driver", loc="lower right")
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_11_top_governance_priority_skus.png", dpi=220)
    plt.close(fig)


def _plot_excess_inventory_ranking(excess_ranking: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10.2, 5.8))
    sns.barplot(data=excess_ranking, y="category", x="excess_exposure_proxy", color="#B7791F", ax=ax)
    ax.set_title("Excess Inventory Exposure Is Concentrated in Few Categories")
    ax.set_xlabel("Excess Inventory Exposure Proxy")
    ax.set_ylabel("Category")
    ax.xaxis.set_major_formatter(FuncFormatter(_eur_mn))
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_12_excess_inventory_exposure_ranking.png", dpi=220)
    plt.close(fig)


def _plot_slow_moving_ranking(slow_moving_ranking: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(11.0, 7.0))
    sns.barplot(data=slow_moving_ranking, y="sku_label", x="slow_moving_value_proxy", color="#805AD5", ax=ax)
    ax.set_title("Slow-Moving Inventory Capital Is Locked in a Narrow SKU Set")
    ax.set_xlabel("Slow-Moving Inventory Value Proxy")
    ax.set_ylabel("SKU | Warehouse")
    ax.xaxis.set_major_formatter(FuncFormatter(_eur_mn))
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_13_slow_moving_inventory_ranking.png", dpi=220)
    plt.close(fig)


def _plot_warehouse_quadrant(warehouse_quadrant: pd.DataFrame) -> None:
    df = warehouse_quadrant.copy()
    median_dos = float(df["average_days_of_supply"].median())
    median_fill = float(df["fill_rate"].median())

    fig, ax = plt.subplots(figsize=(9.8, 6.0))
    sns.scatterplot(
        data=df,
        x="average_days_of_supply",
        y="fill_rate",
        size="inventory_value",
        hue="region",
        sizes=(200, 900),
        alpha=0.8,
        ax=ax,
    )
    for row in df.itertuples(index=False):
        ax.annotate(row.warehouse_id, (row.average_days_of_supply, row.fill_rate), textcoords="offset points", xytext=(6, 4))

    ax.axvline(median_dos, linestyle="--", color="#4A5568", linewidth=1.2)
    ax.axhline(median_fill, linestyle="--", color="#4A5568", linewidth=1.2)
    ax.set_title("Warehouse Quadrant: Service Performance vs Working-Capital Intensity")
    ax.set_xlabel("Average Days of Supply")
    ax.set_ylabel("Fill Rate")
    ax.yaxis.set_major_formatter(FuncFormatter(_pct))
    ax.legend(loc="lower left", bbox_to_anchor=(1.02, 0.0), borderaxespad=0)
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_14_warehouse_service_vs_working_capital_quadrant.png", dpi=220)
    plt.close(fig)


def _plot_supplier_risk_heatmap(supplier_heatmap: pd.DataFrame) -> None:
    heat = supplier_heatmap[
        ["supplier_name", "otd_gap", "avg_delay_norm", "lt_variability_norm", "risk_proxy_norm"]
    ].set_index("supplier_name")

    fig, ax = plt.subplots(figsize=(9.5, 6.5))
    sns.heatmap(heat, cmap="Reds", linewidths=0.4, linecolor="white", cbar_kws={"label": "Relative Risk Intensity"}, ax=ax)
    ax.set_title("Supplier Risk Heatmap: Reliability, Delay, and Variability Signals")
    ax.set_xlabel("Risk Dimensions")
    ax.set_ylabel("Supplier")
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / "viz_15_supplier_risk_heatmap.png", dpi=220)
    plt.close(fig)


def run_visualization_suite() -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "axes.titlesize": 13,
            "axes.labelsize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 8,
            "figure.dpi": 140,
        }
    )

    data = _prepare_data()
    _save_chart_data(data)

    _plot_service_level_trend(data["monthly"])
    _plot_stockout_trend(data["monthly"])
    _plot_fill_rate_by_warehouse(data["warehouse"])
    _plot_fill_rate_by_category(data["category_service"])
    _plot_lost_sales_by_region(data["region_lost_sales"])
    _plot_inventory_concentration_by_category(data["category_inventory"])
    _plot_days_of_supply_distribution(data["dos_distribution"])
    _plot_supplier_otd(data["supplier"])
    _plot_lead_time_variability(data["supplier"])
    _plot_service_vs_inventory_scatter(data["category_region_scatter"])
    _plot_top_governance_priority_skus(data["sku_rank"])
    _plot_excess_inventory_ranking(data["excess_ranking"])
    _plot_slow_moving_ranking(data["slow_moving_ranking"])
    _plot_warehouse_quadrant(data["warehouse_quadrant"])
    _plot_supplier_risk_heatmap(data["supplier_heatmap"])

    print("Visualization suite complete.")
    print(f"Charts written to: {OUTPUT_CHARTS_DIR}")
    print(f"Chart data written to: {OUTPUT_TABLES_DIR}")


if __name__ == "__main__":
    run_visualization_suite()
