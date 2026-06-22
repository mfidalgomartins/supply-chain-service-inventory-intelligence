"""Generate publication-ready charts from processed data.

Each chart answers one decision question and uses real processed/output data.
Consistent typography, palette, grid, and spacing across all figures.
Output: outputs/graphs/*.png
"""

from __future__ import annotations

import os
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".cache" / "matplotlib"))

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

PROC = ROOT / "data" / "processed"
TBL = ROOT / "outputs" / "tables"
OUT = ROOT / "outputs" / "graphs"
OUT.mkdir(parents=True, exist_ok=True)

# ---- Shared style -----------------------------------------------------------
INK = "#1d1d1f"  # primary text / strong elements (Apple ink)
MUTED = "#aeaeb2"  # neutral bars and secondary marks (Apple system gray)
GRID = "#e6e6eb"  # gridlines
ACCENT = "#0071e3"  # single accent for the element that carries the point (Apple blue)
ACCENT_SOFT = "#a8ccf5"  # accent tint

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.edgecolor": INK,
        "axes.linewidth": 0.8,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.titlecolor": INK,
        "axes.labelcolor": INK,
        "axes.labelsize": 11,
        "text.color": INK,
        "xtick.color": INK,
        "ytick.color": INK,
        "figure.dpi": 150,
        "savefig.dpi": 200,
        "savefig.bbox": "tight",
    }
)


def _titles(ax, title, subtitle):
    """Title above, subtitle below it, both clear of the plot area."""
    ax.set_title(title, loc="left", pad=40)
    ax.text(0, 1.045, subtitle, transform=ax.transAxes, fontsize=10.5, color="#6e6e73", va="bottom")


def _clean(ax, grid_axis="y"):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.9, zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)


def _eur(x):
    if abs(x) >= 1e6:
        return f"€{x / 1e6:.1f}M"
    if abs(x) >= 1e3:
        return f"€{x / 1e3:.0f}k"
    return f"€{x:.0f}"


def _save(fig, name, caption):
    fig.text(
        0.0,
        -0.02,
        caption,
        ha="left",
        va="top",
        fontsize=8.5,
        color=MUTED,
        transform=fig.transFigure,
    )
    fig.savefig(OUT / name)
    plt.close(fig)
    print("wrote", name)


# ---- Chart 1: where the value pool sits (category) --------------------------
def chart_opportunity_by_category():
    p = pd.read_csv(TBL / "impact_opportunity_priority.csv")
    d = p[p.entity_type == "Category"].sort_values("opportunity_total_12m_proxy")
    total = d["opportunity_total_12m_proxy"].sum()
    colors = [
        ACCENT if v == d["opportunity_total_12m_proxy"].max() else MUTED
        for v in d["opportunity_total_12m_proxy"]
    ]

    fig, ax = plt.subplots(figsize=(8.6, 5.0))
    bars = ax.barh(
        d["entity_name"], d["opportunity_total_12m_proxy"], color=colors, zorder=3, height=0.66
    )
    _clean(ax, grid_axis="x")
    ax.set_xlim(0, d["opportunity_total_12m_proxy"].max() * 1.16)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _eur(x)))
    for bar, val in zip(bars, d["opportunity_total_12m_proxy"], strict=False):
        ax.text(
            val + total * 0.006,
            bar.get_y() + bar.get_height() / 2,
            f"{_eur(val)}  ({val / total * 100:.0f}%)",
            va="center",
            ha="left",
            fontsize=10,
            color=INK if val == d["opportunity_total_12m_proxy"].max() else "#6e6e73",
            fontweight="bold" if val == d["opportunity_total_12m_proxy"].max() else "normal",
        )
    _titles(
        ax,
        "Health holds nearly half of the recoverable value pool",
        "Estimated 12-month opportunity by category (margin recovery + working-capital release)",
    )
    fig.subplots_adjust(top=0.84)
    _save(
        fig,
        "01_opportunity_by_category.png",
        "Source: outputs/tables/impact_opportunity_priority.csv. Proxy 12-month value pool under the scenario recovery rate.",
    )


# ---- Chart 2: service decline over time -------------------------------------
def chart_service_trend():
    df = pd.read_csv(
        PROC / "daily_product_warehouse_metrics.csv",
        usecols=["date", "units_demanded", "units_fulfilled", "lost_sales_revenue"],
        parse_dates=["date"],
    )
    df["m"] = df["date"].dt.to_period("M").dt.to_timestamp()
    g = (
        df.groupby("m")
        .agg(
            dem=("units_demanded", "sum"),
            ful=("units_fulfilled", "sum"),
            lost=("lost_sales_revenue", "sum"),
        )
        .reset_index()
    )
    g["fill"] = g["ful"] / g["dem"]

    fig, ax = plt.subplots(figsize=(8.6, 5.0))
    ax.plot(g["m"], g["fill"], color=ACCENT, linewidth=2.4, zorder=3)
    ax.scatter(
        [g["m"].iloc[0], g["m"].iloc[-1]],
        [g["fill"].iloc[0], g["fill"].iloc[-1]],
        color=ACCENT,
        s=42,
        zorder=4,
    )
    _clean(ax, grid_axis="y")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
    ax.set_ylim(0.90, 1.0)
    # annotate endpoints
    ax.annotate(
        f"{g['fill'].iloc[0] * 100:.1f}%",
        (g["m"].iloc[0], g["fill"].iloc[0]),
        textcoords="offset points",
        xytext=(6, 8),
        fontsize=10,
        fontweight="bold",
        color=ACCENT,
    )
    ax.annotate(
        f"{g['fill'].iloc[-1] * 100:.1f}%",
        (g["m"].iloc[-1], g["fill"].iloc[-1]),
        textcoords="offset points",
        xytext=(-6, -16),
        fontsize=10,
        fontweight="bold",
        color=ACCENT,
        ha="right",
    )
    drop = (g["fill"].iloc[0] - g["fill"].iloc[-1]) * 100
    _titles(
        ax,
        f"Network fill rate fell {drop:.1f} points over two years",
        "Monthly network fill rate, Jan 2024 to Dec 2025.",
    )
    fig.subplots_adjust(top=0.84)
    _save(
        fig,
        "02_service_level_trend.png",
        "Source: data/processed/daily_product_warehouse_metrics.csv. Fill rate = units fulfilled / units demanded.",
    )


# ---- Chart 3: warehouse service vs inventory tradeoff -----------------------
def chart_warehouse_quadrant():
    w = pd.read_csv(PROC / "warehouse_service_profile.csv")
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    x = w["average_days_of_supply"]
    y = w["fill_rate"] * 100
    fmean, dmean = y.mean(), x.mean()
    ax.axhline(fmean, color=GRID, linewidth=1.2, zorder=1)
    ax.axvline(dmean, color=GRID, linewidth=1.2, zorder=1)
    for _, r in w.iterrows():
        under = r["fill_rate"] * 100 < fmean
        col = ACCENT if under else MUTED
        ax.scatter(
            r["average_days_of_supply"],
            r["fill_rate"] * 100,
            s=r["inventory_value"] / w["inventory_value"].max() * 900 + 140,
            color=col,
            alpha=0.85,
            zorder=3,
            edgecolor="white",
            linewidth=1.5,
        )
        ax.annotate(
            f"{r['warehouse_name']}\n{r['fill_rate'] * 100:.1f}% fill  "
            f"{_eur(r['inventory_value'])} inv",
            (r["average_days_of_supply"], r["fill_rate"] * 100),
            textcoords="offset points",
            xytext=(11, 6),
            fontsize=9,
            color=INK,
        )
    _clean(ax, grid_axis="both")
    ax.set_xlabel("Average days of supply")
    ax.set_ylabel("Fill rate")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    pad = (x.max() - x.min()) * 0.35
    ax.set_xlim(x.min() - pad, x.max() + pad * 1.5)
    ax.set_ylim(y.min() - 0.8, y.max() + 0.9)
    _titles(
        ax,
        "Madrid and Lyon run leaner yet serve worse",
        "Warehouse fill rate vs days of supply. Bubble area = inventory value.",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "03_warehouse_service_inventory.png",
        "Source: data/processed/warehouse_service_profile.csv. Gridlines mark network means.",
    )


# ---- Chart 4: supplier reliability -----------------------------------------
def chart_supplier_reliability():
    s = pd.read_csv(PROC / "supplier_risk_table.csv")
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    for _, r in s.iterrows():
        high = r["risk_tier"] == "High"
        col = ACCENT if high else MUTED
        ax.scatter(
            r["on_time_delivery_rate"] * 100,
            r["lead_time_variability"],
            s=r["lost_sales_revenue"] / s["lost_sales_revenue"].max() * 1100 + 90,
            color=col,
            alpha=0.82,
            zorder=3,
            edgecolor="white",
            linewidth=1.4,
        )
        if high:
            ax.annotate(
                f"{r['supplier_name']}\n{r['on_time_delivery_rate'] * 100:.0f}% on-time  "
                f"{_eur(r['lost_sales_revenue'])} lost",
                (r["on_time_delivery_rate"] * 100, r["lead_time_variability"]),
                textcoords="offset points",
                xytext=(12, 4),
                fontsize=9.2,
                color=INK,
                fontweight="bold",
            )
    _clean(ax, grid_axis="both")
    ax.set_xlabel("On-time delivery rate")
    ax.set_ylabel("Lead-time variability (days, std.)")
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax.margins(x=0.10, y=0.16)
    _titles(
        ax,
        "Two suppliers concentrate the delivery risk",
        "Lead-time variability vs on-time delivery. Bubble area = lost-sales value.",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "04_supplier_reliability.png",
        "Source: data/processed/supplier_risk_table.csv. Bubble area scaled to annualized lost-sales value associated with each supplier.",
    )


# ---- Chart 5: concentration of SKU-location opportunity ---------------------
def chart_top_sku():
    p = pd.read_csv(TBL / "impact_opportunity_priority.csv")
    d = (
        p[p.entity_type == "SKU"]
        .sort_values("opportunity_total_12m_proxy", ascending=False)
        .head(10)
        .iloc[::-1]
    )
    is016 = d["entity_id"].str.startswith("SKU-0016")
    colors = [ACCENT if f else MUTED for f in is016]
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    bars = ax.barh(
        d["entity_name"], d["opportunity_total_12m_proxy"], color=colors, zorder=3, height=0.68
    )
    _clean(ax, grid_axis="x")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _eur(x)))
    ax.set_xlim(0, d["opportunity_total_12m_proxy"].max() * 1.18)
    for bar, val in zip(bars, d["opportunity_total_12m_proxy"], strict=False):
        ax.text(
            val + d["opportunity_total_12m_proxy"].max() * 0.012,
            bar.get_y() + bar.get_height() / 2,
            _eur(val),
            va="center",
            ha="left",
            fontsize=9.5,
            color="#6e6e73",
        )
    _titles(
        ax,
        "One product across four warehouses leads the list",
        "Top 10 SKU-warehouse pairs by 12-month opportunity. Accent = Health Product 16.",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "05_top_sku_opportunity.png",
        "Source: outputs/tables/impact_opportunity_priority.csv. SKU-warehouse grain, proxy 12-month value.",
    )


# ---- Chart 6: stockout rate and lost-sales value over time ------------------
def chart_stockout_trend():
    df = pd.read_csv(
        PROC / "daily_product_warehouse_metrics.csv",
        usecols=["date", "stockout_flag", "lost_sales_revenue"],
        parse_dates=["date"],
    )
    df["m"] = df["date"].dt.to_period("M").dt.to_timestamp()
    g = (
        df.groupby("m")
        .agg(so=("stockout_flag", "mean"), lost=("lost_sales_revenue", "sum"))
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(8.6, 5.0))
    ax2 = ax.twinx()
    ax2.bar(g["m"], g["lost"], width=20, color=MUTED, alpha=0.55, zorder=1)
    ax.plot(g["m"], g["so"] * 100, color=ACCENT, linewidth=2.4, zorder=3)
    _clean(ax, grid_axis="y")
    for s in ("top",):
        ax2.spines[s].set_visible(False)
    ax2.tick_params(length=0)
    ax.set_zorder(ax2.get_zorder() + 1)
    ax.patch.set_visible(False)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _eur(x)))
    ax.set_ylabel("Stockout rate", color=ACCENT)
    ax2.set_ylabel("Monthly lost-sales value", color=MUTED)
    ax.annotate(
        f"{g['so'].iloc[-1] * 100:.1f}%",
        (g["m"].iloc[-1], g["so"].iloc[-1] * 100),
        textcoords="offset points",
        xytext=(-6, 8),
        fontsize=10,
        fontweight="bold",
        color=ACCENT,
        ha="right",
    )
    rise = (g["so"].iloc[-1] - g["so"].iloc[0]) * 100
    _titles(
        ax,
        "Stockouts climbed steadily as lost sales rose",
        f"Monthly stockout rate (line) and lost-sales value (bars). "
        f"Stockout rate up {rise:.1f} points over the window.",
    )
    fig.subplots_adjust(top=0.84)
    _save(
        fig,
        "06_stockout_lost_sales_trend.png",
        "Source: data/processed/daily_product_warehouse_metrics.csv. Stockout rate = share of product-warehouse-days with unmet demand.",
    )


# ---- Chart 7: concentration of lost sales (Lorenz curve) --------------------
def chart_lost_sales_concentration():
    sk = pd.read_csv(PROC / "sku_risk_table.csv")
    v = sk["lost_sales_revenue"].sort_values(ascending=False).values
    n = len(v)
    cum = v.cumsum() / v.sum()
    xp = (pd.Series(range(1, n + 1)) / n).values

    fig, ax = plt.subplots(figsize=(8.6, 5.2))
    ax.plot(xp * 100, cum * 100, color=ACCENT, linewidth=2.6, zorder=3)
    ax.plot([0, 100], [0, 100], color=MUTED, linewidth=1.1, linestyle=(0, (4, 4)), zorder=2)
    _clean(ax, grid_axis="both")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax.set_xlabel("Share of product-warehouse pairs (ranked by lost sales)")
    ax.set_ylabel("Cumulative share of lost-sales value")
    k = int(n * 0.10)
    yv = cum[k - 1] * 100
    ax.scatter([10], [yv], color=ACCENT, s=55, zorder=4)
    ax.annotate(
        f"Top 10% of pairs\ncarry {yv:.0f}% of lost sales",
        (10, yv),
        textcoords="offset points",
        xytext=(14, -6),
        fontsize=10,
        fontweight="bold",
        color=INK,
    )
    ax.vlines(10, 0, yv, color=ACCENT, linewidth=0.9, linestyle=":", zorder=2)
    _titles(
        ax,
        "Lost sales are heavily concentrated",
        "Cumulative lost-sales value across 480 ranked SKU-warehouse pairs. "
        "Diagonal = perfectly even spread.",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "07_lost_sales_concentration.png",
        "Source: data/processed/sku_risk_table.csv. Lorenz-style cumulative concentration curve.",
    )


# ---- Chart 8: ABC class cohort - inventory vs lost sales --------------------
def chart_abc_cohort():
    pi = pd.read_csv(PROC / "product_inventory_profile.csv")
    g = (
        pi.groupby("abc_class")
        .agg(
            inv=("average_inventory_value", "sum"),
            lost=("lost_sales_exposure", "sum"),
            n=("product_id", "count"),
        )
        .reindex(["A", "B", "C"])
    )
    inv_sh = g["inv"] / g["inv"].sum() * 100
    lost_sh = g["lost"] / g["lost"].sum() * 100

    fig, ax = plt.subplots(figsize=(8.6, 5.2))
    y = range(len(g))
    h = 0.36
    ax.barh(
        [i + h / 2 for i in y], inv_sh, height=h, color=MUTED, zorder=3, label="Inventory value"
    )
    ax.barh(
        [i - h / 2 for i in y],
        lost_sh,
        height=h,
        color=ACCENT,
        zorder=3,
        label="Lost-sales exposure",
    )
    _clean(ax, grid_axis="x")
    ax.set_yticks(list(y))
    ax.set_yticklabels([f"Class {c}\n({int(g.loc[c, 'n'])} SKUs)" for c in g.index])
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax.set_xlim(0, max(inv_sh.max(), lost_sh.max()) * 1.18)
    for i, c in enumerate(g.index):
        ax.text(
            inv_sh[c] + 1,
            i + h / 2,
            f"{inv_sh[c]:.0f}%",
            va="center",
            fontsize=9.5,
            color="#6e6e73",
        )
        ax.text(
            lost_sh[c] + 1,
            i - h / 2,
            f"{lost_sh[c]:.0f}%",
            va="center",
            fontsize=9.5,
            color=ACCENT,
            fontweight="bold",
        )
    ax.legend(loc="lower right", frameon=False, fontsize=9.5)
    _titles(
        ax,
        "Class A drives lost sales out of proportion to its count",
        "Share of inventory value and lost-sales exposure by ABC class.",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "08_abc_class_cohort.png",
        "Source: data/processed/product_inventory_profile.csv. ABC class by average inventory value.",
    )


# ---- Chart 9: days-of-supply distribution -----------------------------------
def chart_dos_distribution():
    pi = pd.read_csv(PROC / "product_inventory_profile.csv")
    d = pi["average_days_of_supply"].clip(upper=60)
    fig, ax = plt.subplots(figsize=(8.6, 5.0))
    n, bins, patches = ax.hist(d, bins=24, color=MUTED, zorder=3, edgecolor="white", linewidth=0.7)
    med = pi["average_days_of_supply"].median()
    for patch, left in zip(patches, bins[:-1], strict=False):  # type: ignore[arg-type]
        if left >= 30:
            patch.set_facecolor(ACCENT)
    ax.axvline(med, color=INK, linewidth=1.4, zorder=4)
    ax.annotate(
        f"Median {med:.0f} days",
        (med, max(n) * 0.92),  # type: ignore[arg-type]
        textcoords="offset points",
        xytext=(8, 0),
        fontsize=10,
        fontweight="bold",
        color=INK,
    )
    over = (pi["average_days_of_supply"] >= 30).mean() * 100
    _clean(ax, grid_axis="y")
    ax.set_xlabel("Average days of supply (capped at 60 for display)")
    ax.set_ylabel("Number of SKUs")
    _titles(
        ax,
        "A long tail of overstocked SKUs sits past 30 days",
        f"Distribution of average days of supply across 120 SKUs. "
        f"{over:.0f}% hold 30+ days (accent).",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "09_days_of_supply_distribution.png",
        "Source: data/processed/product_inventory_profile.csv. Values capped at 60 days for display; a thin tail extends to 231 days.",
    )


# ---- Chart 10: supplier on-time delivery ranking ----------------------------
def chart_supplier_ranking():
    s = pd.read_csv(PROC / "supplier_performance_summary.csv").sort_values("on_time_delivery_rate")
    colors = [ACCENT if v < 0.80 else MUTED for v in s["on_time_delivery_rate"]]
    fig, ax = plt.subplots(figsize=(8.6, 5.6))
    bars = ax.barh(
        s["supplier_name"], s["on_time_delivery_rate"] * 100, color=colors, zorder=3, height=0.66
    )
    ax.axvline(80, color=INK, linewidth=1.0, linestyle=(0, (4, 3)), zorder=4)
    _clean(ax, grid_axis="x")
    ax.set_xlim(0, 100)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    for bar, v in zip(bars, s["on_time_delivery_rate"], strict=False):
        ax.text(
            v * 100 - 1.5,
            bar.get_y() + bar.get_height() / 2,
            f"{v * 100:.0f}%",
            va="center",
            ha="right",
            fontsize=9,
            color="white",
            fontweight="bold",
        )
    ax.annotate(
        "80% service line",
        (80, len(s) - 0.4),
        textcoords="offset points",
        xytext=(6, 0),
        fontsize=9,
        color=INK,
    )
    _titles(
        ax,
        "Three suppliers fall below the 80% on-time line",
        "On-time delivery rate by supplier. Accent marks below-threshold suppliers.",
    )
    fig.subplots_adjust(top=0.88)
    _save(
        fig,
        "10_supplier_ontime_ranking.png",
        "Source: data/processed/supplier_performance_summary.csv. On-time rate = orders received on or before due date.",
    )


# ---- Chart 11: opportunity bridge -------------------------------------------
def chart_opportunity_bridge():
    s = pd.read_csv(TBL / "impact_overall_summary.csv").set_index("metric")["value"]
    margin = s["opportunity_margin_recovery_12m_proxy"]
    wc = s["opportunity_wc_release_12m_proxy"]
    total = s["opportunity_total_12m_proxy"]

    fig, ax = plt.subplots(figsize=(8.6, 5.0))
    labels = ["Margin\nrecovery", "Working-capital\nrelease", "Total\nvalue pool"]
    ax.bar(0, margin, color=ACCENT, zorder=3, width=0.62)
    ax.bar(1, wc, bottom=margin, color=MUTED, zorder=3, width=0.62)
    ax.bar(2, total, color=INK, zorder=3, width=0.62)
    # waterfall connectors at the running-total level
    ax.plot(
        [0.31, 0.69],
        [margin, margin],
        color="#aeaeb2",
        linewidth=0.9,
        linestyle=(0, (2, 2)),
        zorder=2,
    )
    ax.plot(
        [1.31, 1.69],
        [total, total],
        color="#aeaeb2",
        linewidth=0.9,
        linestyle=(0, (2, 2)),
        zorder=2,
    )
    _clean(ax, grid_axis="y")
    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(labels)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _eur(x)))
    ax.set_ylim(0, total * 1.18)
    ax.text(
        0,
        margin + total * 0.02,
        _eur(margin),
        ha="center",
        fontsize=10.5,
        fontweight="bold",
        color=INK,
    )
    ax.text(
        1,
        margin + wc + total * 0.02,
        f"+{_eur(wc)}",
        ha="center",
        fontsize=10.5,
        fontweight="bold",
        color="#6e6e73",
    )
    ax.text(
        2,
        total + total * 0.02,
        _eur(total),
        ha="center",
        fontsize=10.5,
        fontweight="bold",
        color=INK,
    )
    _titles(
        ax,
        "Margin recovery dominates the value pool",
        f"Composition of the estimated 12-month opportunity ({_eur(total)} proxy).",
    )
    fig.subplots_adjust(top=0.85)
    _save(
        fig,
        "11_opportunity_bridge.png",
        "Source: outputs/tables/impact_overall_summary.csv. Proxy estimates: recoverable lost-sales margin plus releasable working capital.",
    )


# ---- Chart 12: segment risk heatmap (category x region) ---------------------
def chart_segment_heatmap():
    seg = pd.read_csv(PROC / "segment_risk_table.csv")
    piv = seg.pivot_table(index="category", columns="region", values="governance_priority_score")
    piv = piv.loc[piv.mean(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(8.6, 6.0))
    import numpy as np

    data = piv.values
    cmap = plt.colormaps["Oranges"]
    im = ax.imshow(data, cmap=cmap, aspect="auto", vmin=np.nanmin(data), vmax=np.nanmax(data))
    ax.set_xticks(range(len(piv.columns)))
    ax.set_xticklabels(piv.columns, rotation=18, ha="right", fontsize=9.5)
    ax.set_yticks(range(len(piv.index)))
    ax.set_yticklabels(piv.index, fontsize=9.5)
    ax.tick_params(length=0)
    for s in ("top", "right", "left", "bottom"):
        ax.spines[s].set_visible(False)
    mx = np.nanmax(data)
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            if np.isnan(val):
                continue
            ax.text(
                j,
                i,
                f"{val:.0f}",
                ha="center",
                va="center",
                fontsize=9,
                color="white" if val > mx * 0.6 else INK,
                fontweight="bold" if val > mx * 0.85 else "normal",
            )
    cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.03)
    cb.outline.set_visible(False)  # type: ignore[operator]
    cb.ax.tick_params(length=0, labelsize=8)
    cb.set_label("Governance priority score", fontsize=9)
    _titles(
        ax,
        "Health and Pet Care segments carry the highest risk",
        "Governance priority score by category and region (0-100). Darker = higher priority.",
    )
    fig.subplots_adjust(top=0.88)
    _save(
        fig,
        "12_segment_risk_heatmap.png",
        "Source: data/processed/segment_risk_table.csv. Composite governance priority score by category-region segment.",
    )


# ---- Chart 13: fill rate by category (service ranking) ----------------------
def chart_category_fill():
    df = pd.read_csv(
        PROC / "daily_product_warehouse_metrics.csv",
        usecols=["category", "units_demanded", "units_fulfilled", "lost_sales_revenue"],
    )
    g = (
        df.groupby("category")
        .agg(
            dem=("units_demanded", "sum"),
            ful=("units_fulfilled", "sum"),
            lost=("lost_sales_revenue", "sum"),
        )
        .reset_index()
    )
    g["fill"] = g["ful"] / g["dem"] * 100
    g = g.sort_values("fill")
    colors = [ACCENT if v == g["fill"].min() else MUTED for v in g["fill"]]
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    bars = ax.barh(g["category"], g["fill"], color=colors, zorder=3, height=0.66)
    _clean(ax, grid_axis="x")
    ax.set_xlim(85, 100)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    net = g["ful"].sum() / g["dem"].sum() * 100
    ax.axvline(net, color=INK, linewidth=1.0, linestyle=(0, (4, 3)), zorder=4)
    ax.annotate(
        f"Network {net:.1f}%",
        (net, len(g) - 0.4),
        textcoords="offset points",
        xytext=(6, 0),
        fontsize=9,
        color=INK,
    )
    for bar, v in zip(bars, g["fill"], strict=False):
        ax.text(
            v - 0.3,
            bar.get_y() + bar.get_height() / 2,
            f"{v:.1f}%",
            va="center",
            ha="right",
            fontsize=9,
            color="white",
            fontweight="bold",
        )
    _titles(
        ax,
        "Health serves worst despite holding the most value",
        "Fill rate by product category over the full window.",
    )
    fig.subplots_adjust(top=0.86)
    _save(
        fig,
        "13_fill_rate_by_category.png",
        "Source: data/processed/daily_product_warehouse_metrics.csv. Fill rate aggregated over 24 months.",
    )


# ---- Chart 14: before vs after fill rate by warehouse -----------------------
def chart_before_after():
    df = pd.read_csv(
        PROC / "daily_product_warehouse_metrics.csv",
        usecols=["date", "warehouse_id", "units_demanded", "units_fulfilled"],
        parse_dates=["date"],
    )
    wh = pd.read_csv(PROC / "warehouse_service_profile.csv")[["warehouse_id", "warehouse_name"]]
    dmin, dmax = df["date"].min(), df["date"].max()
    early = df[df["date"] < dmin + pd.Timedelta(days=90)]
    late = df[df["date"] > dmax - pd.Timedelta(days=90)]

    def fr(d):
        g = d.groupby("warehouse_id").agg(
            dem=("units_demanded", "sum"), ful=("units_fulfilled", "sum")
        )
        return g["ful"] / g["dem"] * 100

    early_fr, late_fr = fr(early), fr(late)
    m = (
        pd.DataFrame({"early": early_fr, "late": late_fr})
        .join(wh.set_index("warehouse_id"))
        .sort_values("early")
    )

    fig, ax = plt.subplots(figsize=(8.6, 5.2))
    y = range(len(m))
    for i, (_, r) in enumerate(m.iterrows()):
        ax.plot([r["early"], r["late"]], [i, i], color="#d2d2d7", linewidth=2.4, zorder=2)
        ax.scatter(r["early"], i, color=MUTED, s=90, zorder=3)
        ax.scatter(r["late"], i, color=ACCENT, s=90, zorder=4)
        ax.annotate(
            f"{r['late'] - r['early']:+.1f} pts",
            (min(r["early"], r["late"]) - 0.15, i),
            ha="right",
            va="center",
            fontsize=9,
            color=INK,
        )
    _clean(ax, grid_axis="x")
    ax.set_yticks(list(y))
    ax.set_yticklabels(m["warehouse_name"])
    ax.set_xlim(88, 101)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    ax.scatter([], [], color=MUTED, s=90, label="First 90 days")
    ax.scatter([], [], color=ACCENT, s=90, label="Last 90 days")
    ax.legend(loc="lower right", frameon=False, fontsize=9.5)
    _titles(
        ax,
        "Every warehouse serves worse than it did at the start",
        "Fill rate, first 90 days vs last 90 days of the window.",
    )
    fig.subplots_adjust(top=0.86)
    _save(
        fig,
        "14_before_after_fill_rate.png",
        "Source: data/processed/daily_product_warehouse_metrics.csv. First vs last 90-day windows.",
    )


def main() -> None:
    """Render every publication chart to ``outputs/graphs/``."""
    chart_opportunity_by_category()
    chart_service_trend()
    chart_warehouse_quadrant()
    chart_supplier_reliability()
    chart_top_sku()
    chart_stockout_trend()
    chart_lost_sales_concentration()
    chart_abc_cohort()
    chart_dos_distribution()
    chart_supplier_ranking()
    chart_opportunity_bridge()
    chart_segment_heatmap()
    chart_category_fill()
    chart_before_after()
    print("done ->", OUT)


if __name__ == "__main__":
    main()
