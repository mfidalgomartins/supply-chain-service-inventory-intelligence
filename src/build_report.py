"""Build the analytical report (PDF) for the service-level and inventory study.

Reads processed data and impact tables, embeds the publication charts, and
writes a paginated, multi-page report with a table of contents to
outputs/reports/.
"""

from __future__ import annotations

import pathlib

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    BaseDocTemplate,
    Flowable,
    Frame,
    Image,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"
TBL = ROOT / "outputs" / "tables"
GRAPHS = ROOT / "outputs" / "graphs"
OUT = ROOT / "outputs" / "reports"
OUT.mkdir(parents=True, exist_ok=True)

# Palette (matches charts)
INK = colors.HexColor("#1d1d1f")
SLATE = colors.HexColor("#6e6e73")
MUTED = colors.HexColor("#86868b")
ACCENT = colors.HexColor("#0071e3")
LINE = colors.HexColor("#d2d2d7")
BG = colors.HexColor("#f5f5f7")
WHITE = colors.white

PAGE_W, PAGE_H = A4
MARGIN = 2.0 * cm
CW = PAGE_W - 2 * MARGIN


# ----------------------------------------------------------------------------
# Data
# ----------------------------------------------------------------------------
imp = pd.read_csv(TBL / "impact_overall_summary.csv").set_index("metric")["value"]
sup = pd.read_csv(PROC / "supplier_risk_table.csv")
wh = pd.read_csv(PROC / "warehouse_service_profile.csv")
sku = pd.read_csv(PROC / "sku_risk_table.csv")
seg = pd.read_csv(PROC / "segment_risk_table.csv")
pinv = pd.read_csv(PROC / "product_inventory_profile.csv")
prio = pd.read_csv(TBL / "impact_opportunity_priority.csv")

high = sup[sup.risk_tier == "High"]
non_high = sup[sup.risk_tier != "High"]
total_lost = imp["lost_sales_revenue_observed"]
margin_obs = imp["lost_sales_margin_proxy_observed"]
two_high_lost = high.lost_sales_revenue.sum()
two_high_share = two_high_lost / total_lost
high_on_time = high.on_time_delivery_rate.mean() * 100
non_high_on_time = non_high.on_time_delivery_rate.mean() * 100
high_lt_var = high.lead_time_variability.mean()
non_high_lt_var = non_high.lead_time_variability.mean()
high_stockout = high.stockout_rate.mean() * 100
non_high_stockout = non_high.stockout_rate.mean() * 100
opp_total = imp["opportunity_total_12m_proxy"]
opp_margin = imp["opportunity_margin_recovery_12m_proxy"]
opp_wc = imp["opportunity_wc_release_12m_proxy"]
trapped_wc = imp["trapped_working_capital_proxy_average"]
excess_inv = imp["excess_inventory_value_proxy_average"]

cat = prio[prio.entity_type == "Category"].sort_values(
    "opportunity_total_12m_proxy", ascending=False
)
cat_pool = cat.opportunity_total_12m_proxy.sum()
health_opp = cat.iloc[0].opportunity_total_12m_proxy
petcare_opp = cat.iloc[1].opportunity_total_12m_proxy
top2_cat_share = (health_opp + petcare_opp) / cat_pool

sku_prio = prio[prio.entity_type == "SKU"]
sku_pool = sku_prio.opportunity_total_12m_proxy.sum()
h16 = sku_prio[sku_prio.entity_id.str.startswith("SKU-0016")]
h16_opp = h16.opportunity_total_12m_proxy.sum()
h16_pool_share = h16_opp / sku_pool

tier_counts = sku.risk_tier.value_counts()
n_high_pairs = int(tier_counts.get("High", 0))
n_med_pairs = int(tier_counts.get("Medium", 0))

fill_spread = (wh.fill_rate.max() - wh.fill_rate.min()) * 100
madrid_lyon_lost_share = (
    wh[wh.warehouse_name.str.contains("Madrid|Lyon", regex=True)].lost_sales_value.sum()
    / wh.lost_sales_value.sum()
)
lisbon_porto_inventory_share = (
    wh[wh.warehouse_name.str.contains("Lisbon|Porto", regex=True)].inventory_value.sum()
    / wh.inventory_value.sum()
)

# category service
_d = pd.read_csv(
    PROC / "daily_product_warehouse_metrics.csv",
    usecols=["category", "units_demanded", "units_fulfilled", "lost_sales_revenue"],
)
catfill = _d.groupby("category").agg(
    dem=("units_demanded", "sum"),
    ful=("units_fulfilled", "sum"),
    lost=("lost_sales_revenue", "sum"),
)
catfill["fill"] = catfill.ful / catfill.dem * 100
catfill["lost_share"] = catfill.lost / catfill.lost.sum() * 100
catfill = catfill.sort_values("fill")
health_fill = catfill.loc["Health", "fill"]
health_lost_share = catfill.loc["Health", "lost_share"]
petcare_lost_share = catfill.loc["Pet Care", "lost_share"]

# ABC cohort
abc = (
    pinv.groupby("abc_class")
    .agg(
        inv=("average_inventory_value", "sum"),
        lost=("lost_sales_exposure", "sum"),
        n=("product_id", "count"),
    )
    .reindex(["A", "B", "C"])
)
abc["inv_sh"] = abc.inv / abc.inv.sum() * 100
abc["lost_sh"] = abc.lost / abc.lost.sum() * 100

# concentration
ls = sku.lost_sales_revenue.sort_values(ascending=False).values
ls_cum = ls.cumsum() / ls.sum()
top10_share = ls_cum[int(len(ls) * 0.10) - 1] * 100
top5_share = ls_cum[int(len(ls) * 0.05) - 1] * 100

dos_median = pinv.average_days_of_supply.median()
dos_over30 = (pinv.average_days_of_supply >= 30).mean() * 100
dos_max = pinv.average_days_of_supply.max()

# monthly KPI series for the appendix
_md = pd.read_csv(
    PROC / "daily_product_warehouse_metrics.csv",
    usecols=["date", "units_demanded", "units_fulfilled", "lost_sales_revenue", "stockout_flag"],
    parse_dates=["date"],
)
_md["m"] = _md["date"].dt.to_period("M")
monthly = _md.groupby("m").agg(
    dem=("units_demanded", "sum"),
    ful=("units_fulfilled", "sum"),
    so=("stockout_flag", "mean"),
    lost=("lost_sales_revenue", "sum"),
)
monthly["fill"] = monthly.ful / monthly.dem * 100
last3_lost_multiple = monthly.tail(3).lost.mean() / monthly.head(3).lost.mean()


def eur(x, dp=1):
    if abs(x) >= 1e6:
        return f"€{x / 1e6:.{dp}f}M"
    if abs(x) >= 1e3:
        return f"€{x / 1e3:.0f}k"
    return f"€{x:.0f}"


# ----------------------------------------------------------------------------
# Styles
# ----------------------------------------------------------------------------
ss = getSampleStyleSheet()
body = ParagraphStyle(
    "body",
    parent=ss["BodyText"],
    fontName="Helvetica",
    fontSize=10,
    leading=15.5,
    textColor=INK,
    alignment=TA_JUSTIFY,
    spaceAfter=8,
)
lead = ParagraphStyle("lead", parent=body, fontSize=11, leading=17, textColor=INK, spaceAfter=10)
h1 = ParagraphStyle(
    "h1",
    parent=ss["Heading1"],
    fontName="Helvetica-Bold",
    fontSize=16,
    leading=20,
    textColor=INK,
    spaceBefore=4,
    spaceAfter=4,
)
kicker = ParagraphStyle(
    "kicker", fontName="Helvetica-Bold", fontSize=8.5, textColor=ACCENT, leading=11, spaceAfter=2
)
h2 = ParagraphStyle(
    "h2",
    parent=ss["Heading2"],
    fontName="Helvetica-Bold",
    fontSize=12.5,
    leading=16,
    textColor=INK,
    spaceBefore=14,
    spaceAfter=5,
)
h3 = ParagraphStyle(
    "h3",
    parent=ss["Heading3"],
    fontName="Helvetica-Bold",
    fontSize=10.5,
    leading=14,
    textColor=ACCENT,
    spaceBefore=10,
    spaceAfter=3,
)
caption = ParagraphStyle(
    "caption",
    fontName="Helvetica-Oblique",
    fontSize=8,
    textColor=MUTED,
    leading=11,
    spaceBefore=3,
    spaceAfter=12,
)
small = ParagraphStyle("small", fontName="Helvetica", fontSize=8.5, textColor=SLATE, leading=12)
takeaway = ParagraphStyle(
    "takeaway",
    parent=body,
    fontName="Helvetica-Bold",
    fontSize=10,
    leading=14,
    textColor=INK,
    alignment=TA_LEFT,
    spaceAfter=2,
)

# TOC heading styles carry a marker name used by afterFlowable
TOC1 = ParagraphStyle("TOC1", parent=h1)
TOC1.tocLevel = 0
TOC2 = ParagraphStyle("TOC2", parent=h2)
TOC2.tocLevel = 1


# ----------------------------------------------------------------------------
# Custom flowables
# ----------------------------------------------------------------------------
class HRule(Flowable):
    def __init__(self, width, color=LINE, thickness=0.8, space=4):
        super().__init__()
        self.width = width
        self.color = color
        self.thickness = thickness
        self.space = space
        self.height = thickness + space

    def draw(self):
        self.canv.setStrokeColor(self.color)
        self.canv.setLineWidth(self.thickness)
        self.canv.line(0, self.space, self.width, self.space)


class KpiStrip(Flowable):
    """Headline metrics in a row of cards."""

    def __init__(self, items, width):
        super().__init__()
        self.items = items
        self.width = width
        self.height = 2.5 * cm

    def draw(self):
        c = self.canv
        n = len(self.items)
        gap = 0.35 * cm
        cw = (self.width - gap * (n - 1)) / n
        for i, (val, lab) in enumerate(self.items):
            x = i * (cw + gap)
            c.setFillColor(BG)
            c.roundRect(x, 0, cw, self.height, 5, stroke=0, fill=1)
            c.setFillColor(ACCENT)
            c.rect(x, 0, 3, self.height, stroke=0, fill=1)
            c.setFillColor(INK)
            c.setFont("Helvetica-Bold", 16)
            c.drawString(x + 0.42 * cm, self.height - 0.95 * cm, val)
            c.setFillColor(SLATE)
            c.setFont("Helvetica", 7.6)
            for j, line in enumerate(_wrap(lab, 26)):
                c.drawString(x + 0.42 * cm, self.height - 1.5 * cm - j * 0.36 * cm, line)


def _wrap(text, width):
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 <= width:
            cur = (cur + " " + w).strip()
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines[:2]


def takeaway_box(text, width=CW):
    p = Paragraph(f'<font color="#0071e3"><b>Takeaway&nbsp;&nbsp;</b></font>{text}', takeaway)
    t = Table([[p]], colWidths=[width])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), BG),
                ("LINEBEFORE", (0, 0), (0, -1), 3, ACCENT),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
            ]
        )
    )
    return t


_IMG_CACHE: dict = {}


def _img_size(path):
    from PIL import Image as PILImage

    if path not in _IMG_CACHE:
        with PILImage.open(path) as im:
            _IMG_CACHE[path] = im.size
    return _IMG_CACHE[path]


def chart(name, cap=None, w=None):
    """Image plus an italic figure caption, kept together where possible."""
    path = GRAPHS / name
    iw, ih = _img_size(path)
    target_w = w or CW
    scale = target_w / iw
    img = Image(str(path), width=target_w, height=ih * scale)
    items = [img]
    if cap:
        items.append(Paragraph(cap, caption))
    # Keep an image and its caption on the same page.
    return [KeepTogether(items)]


def data_table(rows, col_widths, header=True, align_right=None, fs=8.5):
    align_right = align_right or []
    style = [
        ("FONT", (0, 0), (-1, -1), "Helvetica", fs),
        ("TEXTCOLOR", (0, 0), (-1, -1), INK),
        ("LINEBELOW", (0, 0), (-1, -2), 0.4, LINE),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, BG]),
    ]
    if header:
        style += [
            ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", fs - 0.3),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("BACKGROUND", (0, 0), (-1, 0), INK),
            ("TOPPADDING", (0, 0), (-1, 0), 6),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ]
    for c in align_right:
        style.append(("ALIGN", (c, 0), (c, -1), "RIGHT"))
    t = Table(rows, colWidths=col_widths, repeatRows=1 if header else 0)
    t.setStyle(TableStyle(style))
    return t


# helpers to register section headings for the TOC
def H1(text):
    return Paragraph(text, TOC1)


def H2(text):
    return Paragraph(text, TOC2)


# ----------------------------------------------------------------------------
# Page furniture
# ----------------------------------------------------------------------------
DATA_THROUGH_DATE = _md["date"].max().strftime("%d %B %Y")
TITLE = "Service Level and Inventory Intelligence"


def cover(canvas, doc):
    canvas.saveState()
    canvas.setFillColor(INK)
    canvas.rect(0, PAGE_H - 5.4 * cm, PAGE_W, 5.4 * cm, stroke=0, fill=1)
    canvas.setFillColor(ACCENT)
    canvas.rect(0, PAGE_H - 5.55 * cm, PAGE_W, 0.15 * cm, stroke=0, fill=1)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawString(MARGIN, PAGE_H - 1.7 * cm, "ANALYTICAL REPORT")
    canvas.setFont("Helvetica", 9)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 1.7 * cm, "Supply Chain Analytics")
    canvas.setFont("Helvetica-Bold", 25)
    canvas.drawString(MARGIN, PAGE_H - 3.5 * cm, "Service Level and Inventory")
    canvas.drawString(MARGIN, PAGE_H - 4.5 * cm, "Intelligence")

    canvas.setFillColor(BG)
    canvas.rect(0, 0, PAGE_W, 2.4 * cm, stroke=0, fill=1)
    canvas.setFillColor(SLATE)
    canvas.setFont("Helvetica", 8.5)
    canvas.drawString(
        MARGIN,
        1.45 * cm,
        "Multi-warehouse distribution network  |  24 months observed (Jan 2024 - Dec 2025)",
    )
    canvas.drawString(MARGIN, 0.95 * cm, f"Data through {DATA_THROUGH_DATE}")
    canvas.setFont("Helvetica-Oblique", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawRightString(
        PAGE_W - MARGIN, 0.95 * cm, "Synthetic dataset. Financial values are proxy estimates."
    )
    canvas.restoreState()


def standard(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(LINE)
    canvas.setLineWidth(0.6)
    canvas.line(MARGIN, PAGE_H - 1.35 * cm, PAGE_W - MARGIN, PAGE_H - 1.35 * cm)
    canvas.setFont("Helvetica", 7.6)
    canvas.setFillColor(MUTED)
    canvas.drawString(MARGIN, PAGE_H - 1.15 * cm, TITLE)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 1.15 * cm, "Analytical Report")
    canvas.line(MARGIN, 1.25 * cm, PAGE_W - MARGIN, 1.25 * cm)
    canvas.setFillColor(MUTED)
    canvas.drawString(MARGIN, 0.85 * cm, "Synthetic data | proxy financials")
    canvas.drawRightString(PAGE_W - MARGIN, 0.85 * cm, f"Page {doc.page - 1}")
    canvas.restoreState()


# ----------------------------------------------------------------------------
# Document with TOC support
# ----------------------------------------------------------------------------
class Report(BaseDocTemplate):
    def afterFlowable(self, flowable):
        if not isinstance(flowable, Paragraph):
            return
        level = getattr(flowable.style, "tocLevel", None)
        if level is None:
            return
        text = flowable.getPlainText()
        key = f"toc-{id(flowable)}"
        self.canv.bookmarkPage(key)
        self.notify("TOCEntry", (level, text, self.page - 1, key))


def bullets(items, style=body):
    return [Paragraph(f"•&nbsp;&nbsp;{t}", style) for t in items]


def build():
    doc = Report(
        str(OUT / "service_inventory_intelligence_report.pdf"),
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=1.9 * cm,
        bottomMargin=1.7 * cm,
        title="Service Level and Inventory Intelligence",
        author="Supply Chain Analytics",
        invariant=1,
    )
    cover_frame = Frame(MARGIN, MARGIN, CW, PAGE_H - 2 * MARGIN, id="cover")
    body_frame = Frame(MARGIN, 1.5 * cm, CW, PAGE_H - 3.5 * cm, id="body")
    doc.addPageTemplates(
        [
            PageTemplate(id="Cover", frames=[cover_frame], onPage=cover),
            PageTemplate(id="Body", frames=[body_frame], onPage=standard),
        ]
    )

    s: list = []
    A = s.append

    # ============================= COVER ================================
    A(NextPageTemplate("Body"))
    A(Spacer(1, 7.2 * cm))
    A(
        Paragraph(
            "What the data says",
            ParagraphStyle("ct", fontName="Helvetica-Bold", fontSize=12, textColor=ACCENT),
        )
    )
    A(Spacer(1, 4))
    A(
        Paragraph(
            "Service quality across the four-warehouse network has eroded steadily "
            "over two years while inventory sits unevenly against demand. This report "
            "quantifies where service is failing, which suppliers and locations carry "
            "the exposure, how concentrated the losses are, and the size of the value "
            "pool that disciplined intervention can recover.",
            ParagraphStyle("cl", parent=body, fontSize=11, leading=16.5),
        )
    )
    A(Spacer(1, 0.5 * cm))
    A(
        KpiStrip(
            [
                (f"{two_high_share * 100:.0f}%", "of lost-sales value tied to two suppliers"),
                ("7.6 pts", "network fill-rate decline over 24 months"),
                (eur(opp_total), "estimated 12-month value pool"),
                (f"{top10_share:.0f}%", "of lost sales in the worst 10% of SKU-pairs"),
            ],
            CW,
        )
    )
    A(PageBreak())

    # ============================= TOC ==================================
    A(Paragraph("CONTENTS", kicker))
    A(Paragraph("Table of contents", h1))
    A(HRule(CW))
    A(Spacer(1, 10))
    toc = TableOfContents()
    toc.levelStyles = [
        ParagraphStyle("toc0", fontName="Helvetica-Bold", fontSize=11, leading=20, textColor=INK),
        ParagraphStyle(
            "toc1", fontName="Helvetica", fontSize=9.5, leading=16, textColor=SLATE, leftIndent=14
        ),
    ]
    A(toc)
    A(PageBreak())

    # ========================= EXECUTIVE SUMMARY ========================
    A(Paragraph("SECTION 1", kicker))
    A(H1("Executive summary"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            f"<b>The operating issue is now decision-grade, not exploratory.</b> "
            f"Across 120 products, four warehouses, and 12 suppliers over 24 months, "
            f"the network lost {eur(total_lost)} of demand to stockouts, worth "
            f"{eur(margin_obs)} in gross margin at category-level margin assumptions. "
            f"The monthly fill rate fell from 99.6% in January 2024 to 92.0% in "
            f"December 2025, a decline of 7.6 percentage points. Over the same window "
            f"the stockout rate rose from 0.4% to 7.9%. The pattern is a structural "
            f"drift in replenishment performance, not a set of isolated shocks: the "
            f"final quarter lost sales ran {last3_lost_multiple:.1f}x the first-quarter "
            f"baseline and the final month is the worst in the series.",
            lead,
        )
    )
    A(
        Paragraph(
            f"<b>The loss is concentrated enough to manage surgically.</b> Two "
            f"suppliers, Supplier 2 and Supplier 3, account for {eur(two_high_lost)} "
            f"of lost-sales value, {two_high_share * 100:.0f}% of the total, and are the "
            f"only two suppliers in the High risk tier. Their combined on-time rate is "
            f"{high_on_time:.0f}% versus {non_high_on_time:.0f}% for the rest of the base, "
            f"and their lead-time variability is {high_lt_var:.1f} days versus "
            f"{non_high_lt_var:.1f}. At SKU-location grain the concentration is sharper "
            f"still: the worst 10% of the 480 scored pairs carry {top10_share:.0f}% of "
            f"all lost sales, and the worst 5% carry {top5_share:.0f}%.",
            body,
        )
    )
    A(
        Paragraph(
            f"<b>One product is the cleanest root-cause candidate.</b> Of the 480 "
            f"product-warehouse pairs scored, only {n_high_pairs} reach the High tier, "
            f"and all {n_high_pairs} are Health Product 16, one in each warehouse. "
            f"Combined they represent {eur(h16_opp)} of recoverable annual value, "
            f"{h16_pool_share * 100:.0f}% of the ranked SKU-location pool. The same "
            f"product fails wherever it is stocked, which makes the working hypothesis "
            f"a product-level supply, allocation, or planning fault rather than a local "
            f"warehouse execution issue.",
            body,
        )
    )
    A(
        Paragraph(
            f"<b>The inventory problem is allocation quality, not aggregate stock.</b> "
            f"Madrid and Lyon generate {madrid_lyon_lost_share * 100:.0f}% of warehouse "
            f"lost-sales value while Lisbon and Porto hold "
            f"{lisbon_porto_inventory_share * 100:.0f}% of inventory value. The "
            f"fill-rate spread across warehouses is {fill_spread:.1f} points and tracks "
            f"placement, not total volume. At the same time {dos_over30:.0f}% of SKUs "
            f"hold 30 or more days of supply against a {dos_median:.0f}-day median, so "
            f"capital is trapped in slow lines while fast lines run thin.",
            body,
        )
    )
    A(
        Paragraph(
            f"<b>The value case supports a sequenced intervention, not a broad "
            f"inventory build.</b> Disciplined action on the ranked priorities supports "
            f"an estimated "
            f"{eur(opp_total)} twelve-month value pool: {eur(opp_margin)} from "
            f"recovered lost-sales margin and {eur(opp_wc)} from released working "
            f"capital. Nearly half of that pool sits in the Health category and "
            f"{top2_cat_share * 100:.0f}% sits in Health and Pet Care together. The six "
            f"recommendations in Section 8 sequence the work from supplier remediation "
            f"to inventory rebalancing, ordered by return and dependency.",
            body,
        )
    )
    A(Spacer(1, 8))
    A(
        takeaway_box(
            f"Fix two suppliers and one product family first. They carry "
            f"{two_high_share * 100:.0f}% of lost-sales value and the largest single SKU "
            f"opportunity, and neither fix needs new inventory investment."
        )
    )
    A(PageBreak())

    # ========================= CONTEXT ==================================
    A(Paragraph("SECTION 2", kicker))
    A(H1("Context and objectives"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The study covers a multi-warehouse fast-moving consumer goods network "
            "spanning Iberia and southern France. Four distribution centres serve "
            "demand across eight product categories sourced from 12 suppliers. The "
            "observation window runs 731 days, from January 2024 to December 2025. "
            "Daily records capture demand, fulfilment, lost sales, on-hand and "
            "on-order inventory, days of supply, and supplier delivery performance at "
            "the product-warehouse grain, producing roughly 351,000 daily rows.",
            body,
        )
    )
    A(
        Paragraph(
            "Service performance and inventory exposure pull against each other. "
            "Holding more stock lifts the fill rate but ties up working capital and "
            "raises obsolescence risk; holding less frees capital but exposes the "
            "network to stockouts and lost margin. A network can post an acceptable "
            "average and still be failing badly in the places that matter, because "
            "good locations mask weak ones and slow lines mask fast ones. The "
            "objective is to find the specific points where that balance is failing "
            "and to size the value of fixing them.",
            body,
        )
    )
    A(Paragraph("The analysis answers six questions:", body))
    for p in bullets(
        [
            "Where is service failing, and is the trend improving or deteriorating?",
            "Which suppliers carry the largest downstream lost-sales exposure?",
            "Where is inventory misaligned with demand rather than simply high?",
            "How concentrated are the losses across products, locations, and segments?",
            "What is the recoverable value, and how should the work be sequenced?",
            "Which assumptions must be validated before the value case is treated as budget-ready?",
        ]
    ):
        A(p)
    A(Spacer(1, 8))
    A(H2("Network at a glance"))
    A(
        data_table(
            [
                ["Dimension", "Coverage"],
                ["Warehouses", "4 (Madrid, Lyon, Porto, Lisbon)"],
                ["Regions", "Spain Central, France South-East, Portugal North and South"],
                ["Products", "120 across 8 categories"],
                ["Suppliers", "12"],
                ["Product-warehouse pairs scored", "480"],
                ["Observation window", "731 days (Jan 2024 - Dec 2025)"],
                ["Daily fact rows", "~351,000"],
                ["Observed lost-sales value", eur(total_lost)],
                ["Observed lost-sales margin (proxy)", eur(margin_obs)],
                ["Average inventory value (daily)", eur(wh.inventory_value.sum())],
            ],
            [7.2 * cm, CW - 7.2 * cm],
        )
    )
    A(Spacer(1, 10))
    A(Spacer(1, 8))
    A(H2("Companion dashboard"))
    A(
        Paragraph(
            "This report has an interactive companion. The published dashboard exposes "
            "the same service, supplier, warehouse, and SKU-risk views with filtering "
            "down to the product-warehouse pair, so a reader can move from a finding "
            "here to the underlying records without rerunning the pipeline. The static "
            "charts in this report are the fixed, citable version of those views. Where "
            "a figure caption names a processed table, the dashboard reads the same "
            "table, and a reconciliation gate enforces that the two never disagree.",
            body,
        )
    )
    A(PageBreak())

    # ========================= DATA & METHODOLOGY =======================
    A(Paragraph("SECTION 3", kicker))
    A(H1("Data and methodology"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The pipeline moves from reproducible synthetic data to SQL analytical "
            "views, policy-based risk scoring, directional impact estimates, and a "
            "published dashboard. Every stage is covered by data contracts and "
            "reconciliation gates that block release on any failure or high-severity "
            "warning. The sections below describe each metric so that the findings can "
            "be read against a precise definition rather than an intuition.",
            body,
        )
    )

    A(H2("Service and inventory metrics"))
    A(
        Paragraph(
            "Fill rate is units fulfilled divided by units demanded. Stockout rate is "
            "units of unmet demand divided by units demanded. Days of supply is "
            "available inventory divided by average daily demand. Lost-sales value "
            "prices unmet demand at unit selling price, and the lost-sales margin "
            "proxy applies category gross-margin assumptions to that value. Excess "
            "inventory is value held above the ABC-class days-of-supply policy cap, "
            "and the slow-moving proxy isolates value sitting on days with no off-take.",
            body,
        )
    )

    A(H2("Risk scoring"))
    A(
        Paragraph(
            "Each product-warehouse pair, supplier, and category-region segment "
            "receives a composite governance priority score from 0 to 100, built from "
            "five weighted components: service risk, stockout risk, excess inventory, "
            "supplier risk, and working-capital risk. Each component is normalized "
            "against fixed policy thresholds before weighting, so the score expresses "
            "priority against declared operating tolerances. "
            "Pairs are tiered Critical, High, Medium, or Low. The score records which of the "
            "five components is the main driver for each entity, which is what lets the "
            "findings separate a stockout problem from an overstock problem.",
            body,
        )
    )
    A(Spacer(1, 4))
    A(
        data_table(
            [
                ["Score component", "What it captures"],
                ["Service risk", "Fill-rate gap against demand at the entity grain."],
                ["Stockout risk", "Frequency and depth of unmet-demand days."],
                ["Excess inventory", "Value held above the ABC days-of-supply policy cap."],
                ["Supplier risk", "Lead-time variability and on-time delivery shortfall."],
                ["Working-capital risk", "Capital tied in slow-moving and overstocked lines."],
            ],
            [4.7 * cm, CW - 4.7 * cm],
            fs=9,
        )
    )
    A(Spacer(1, 8))
    A(
        takeaway_box(
            "Scores prioritise; they do not prove causation. The score points to where "
            "to look first, and the underlying component then says why."
        )
    )

    A(H2("Impact and opportunity estimates"))
    A(
        Paragraph(
            "Financial figures are directional proxy estimates, not audited profit and "
            "loss. Observed values are annualised using a 365-over-731-day factor. The "
            "twelve-month value pool combines recoverable lost-sales margin under a "
            "scenario recovery rate with releasable trapped working capital. The margin "
            "component applies category gross-margin assumptions to the units a "
            "scenario recovery rate would convert; the working-capital component "
            "applies the same recovery logic to the average trapped capital proxy of "
            f"{eur(trapped_wc)} per day. Because recovery is partial by design, the "
            f"pool of {eur(opp_total)} is a fraction of the {eur(total_lost)} gross "
            f"loss, not a claim that all of it is reachable.",
            body,
        )
    )

    A(H2("Quality controls"))
    A(
        Paragraph(
            "Data contracts cover required columns, grain, nulls, ranges, domains, and "
            "key references. SQL and Python checks reconcile service, inventory, "
            "impact, scoring, and dashboard metrics against each other so that a number "
            "shown on the dashboard matches the number in the underlying table. "
            "Continuous integration runs the full pipeline, unit tests, release gates, "
            "and a dashboard freshness check on every change. The release gate blocks "
            "publication on any failure or high-severity warning.",
            body,
        )
    )
    A(PageBreak())

    # ========================= ANALYTICAL FRAMEWORK =====================
    A(Paragraph("SECTION 4", kicker))
    A(H1("Analytical framework"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The analysis works through four lenses, each answering a different "
            "question about the same loss. Reading them in order moves from symptom to "
            "cause to size, which is also the order in which the recommendations act.",
            body,
        )
    )

    A(H2("1. Trend: is the problem getting worse?"))
    A(
        Paragraph(
            "A point-in-time fill rate cannot tell remediation from drift. The trend "
            "lens tracks fill rate, stockout rate, and lost-sales value month by month "
            "and compares the first and last 90-day windows by location. A declining "
            "baseline changes the economics of any fix, because inventory added to a "
            "sliding system is absorbed rather than converted to service.",
            body,
        )
    )

    A(H2("2. Concentration: where is the loss, exactly?"))
    A(
        Paragraph(
            "Averages hide the structure that matters for action. The concentration "
            "lens ranks SKU-location pairs, categories, ABC classes, and "
            "category-region segments by their share of loss, then measures how much "
            "of the total sits in the worst few. Sharp concentration means a short "
            "list of targeted fixes will outperform a uniform service target applied "
            "across the catalogue.",
            body,
        )
    )

    A(H2("3. Cause: supply reliability versus inventory placement"))
    A(
        Paragraph(
            "Lost sales come from two distinct failures: stock that never arrives "
            "reliably, and stock that sits in the wrong place. The cause lens "
            "separates them by reading supplier on-time performance and lead-time "
            "variability against warehouse fill rate and days of supply. The two "
            "failures need different fixes, so labelling each loss correctly is what "
            "keeps the plan from over-investing in inventory.",
            body,
        )
    )

    A(H2("4. Size: what is recoverable, and in what order?"))
    A(
        Paragraph(
            "The final lens converts the diagnosis into a ranked value pool. It "
            "estimates recoverable margin and releasable working capital under a "
            "scenario recovery rate, attributes the pool to categories, suppliers, "
            "warehouses, and SKUs, and orders the work by return and dependency. The "
            "output is a sequence, not a wish list, because the first fixes are "
            "preconditions for the later ones to pay back.",
            body,
        )
    )
    A(Spacer(1, 8))
    A(
        takeaway_box(
            "Symptom, structure, cause, size. Each finding section below maps to one "
            "of these lenses, and the recommendations follow the same order."
        )
    )
    A(PageBreak())

    # ========================= DECISION CASE ============================
    A(Paragraph("SECTION 5", kicker))
    A(H1("Decision case"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The evidence supports three management decisions. First, the near-term "
            "service plan should be funded as a supplier and SKU recovery effort, not "
            "as a network-wide safety-stock increase. Second, the first inventory move "
            "should be a reallocation from slow lines and stronger sites toward the "
            "underserved Madrid and Lyon lanes. Third, the value case should be "
            "managed against recoverable margin first and working-capital release "
            "second, because the financial loss is caused primarily by missed demand.",
            body,
        )
    )
    A(
        Paragraph(
            "The decision logic below translates the analytical readout into action. It "
            "separates the operating hypothesis, the evidence already strong enough to "
            "act on, and the validation required before committing material capital. "
            "This is the difference between a diagnostic report and an execution case: "
            "the next step is not more broad analysis, but controlled remediation with "
            "specific proof points.",
            body,
        )
    )
    decision_points = [
        (
            "Supplier remediation",
            (
                f"Supplier 2 and Supplier 3 carry {two_high_share * 100:.0f}% of "
                f"lost-sales value; their on-time rate is {high_on_time:.0f}% versus "
                f"{non_high_on_time:.0f}% for the rest of the base. Prioritise "
                "lead-time stability, constrained-lane expediting, and "
                "supplier-specific recovery governance before adding stock."
            ),
        ),
        (
            "SKU recovery",
            (
                f"All {n_high_pairs} High-tier SKU-location pairs are Health Product "
                f"16, worth {eur(h16_opp)} of ranked SKU-location value. Run one "
                "cross-warehouse recovery plan covering supply source, allocation "
                "rules, reorder point, and substitution risk."
            ),
        ),
        (
            "Inventory placement",
            (
                f"Madrid and Lyon generate {madrid_lyon_lost_share * 100:.0f}% of "
                f"warehouse lost sales while Lisbon and Porto hold "
                f"{lisbon_porto_inventory_share * 100:.0f}% of inventory value. Move "
                "stock selectively after supplier inputs stabilise; avoid a blanket "
                "stock build that would raise capital without fixing drift."
            ),
        ),
        (
            "Category focus",
            (
                f"Health and Pet Care hold {top2_cat_share * 100:.0f}% of the "
                f"recoverable value pool; Health alone is {health_lost_share:.0f}% of "
                f"lost sales at {health_fill:.1f}% fill. Create a Health and Pet Care "
                "service cell with weekly category-level decisions rather than "
                "diffuse catalogue-wide reviews."
            ),
        ),
    ]
    for title, text in decision_points:
        A(Paragraph(f"<b>{title}.</b> {text}", body))
    A(Spacer(1, 10))
    A(H2("What would change the decision"))
    A(
        Paragraph(
            "The current evidence is sufficient to start the no-regret actions: supplier "
            "stabilisation, Health Product 16 recovery, and SKU-level policy review. "
            "Three findings would change the investment case before material inventory "
            "is moved. The first is a commercial substitution effect showing that a "
            "large share of stockout demand was captured by another SKU rather than "
            "lost. The second is a supplier contract or promotion calendar proving that "
            "Health Product 16 demand was abnormal and non-repeatable. The third is a "
            "warehouse capacity constraint that prevents Madrid or Lyon from absorbing "
            "rebalanced stock without higher handling cost. None invalidates the "
            "diagnosis; each changes the size, sequencing, or net economics of the fix.",
            body,
        )
    )
    A(
        takeaway_box(
            "Approve the first wave as a controlled recovery programme. Hold back broad "
            "inventory investment until substitution, promotion, and capacity checks "
            "confirm the net value case."
        )
    )
    A(PageBreak())

    # ============================= FINDINGS =============================
    A(Paragraph("SECTION 6", kicker))
    A(H1("Findings"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The findings are grouped into six themes. The first establishes that "
            "service is deteriorating, the next three locate the loss across suppliers, "
            "warehouses, and the catalogue, the fifth measures how concentrated it is, "
            "and the last sizes the recoverable pool.",
            body,
        )
    )
    A(Spacer(1, 6))

    # ---- 5.1 service decline ----
    A(H2("6.1  Service quality is on a two-year decline"))
    A(
        Paragraph(
            "The monthly network fill rate fell from 99.6% in January 2024 to 92.0% in "
            "December 2025. The path is not a one-off shock. The rate drifts down "
            "through 2024, recovers partially in early 2025, then slides again to its "
            "lowest point in the final month of the window. December 2025 is the worst "
            "month in the series on both fill rate and lost-sales value, which means "
            "the network entered 2026 at its weakest observed state rather than "
            "recovering toward the mean.",
            body,
        )
    )
    for it in chart(
        "02_service_level_trend.png", "Figure 1. Monthly network fill rate, Jan 2024 to Dec 2025."
    ):
        A(it)
    A(
        Paragraph(
            "Reading the stockout rate alongside lost-sales value confirms the same "
            "story from the cost side. The stockout rate climbed from 0.4% to 7.9% over "
            "the window, and monthly lost-sales value rose with it, peaking in the "
            f"final months. The last three months averaged {last3_lost_multiple:.1f}x "
            "the lost-sales value of the first three months, so the end-state is not "
            "just lower service; it is a materially more expensive operating baseline. "
            "A declining baseline matters for planning: inventory added without fixing "
            "replenishment discipline will be absorbed by the drift rather than "
            "converted into service, so stabilising the trend is a precondition for "
            "any inventory investment to pay back.",
            body,
        )
    )
    for it in chart(
        "06_stockout_lost_sales_trend.png",
        "Figure 2. Monthly stockout rate (line) against lost-sales value (bars).",
    ):
        A(it)
    A(
        Paragraph(
            "The decline is general, not local. Comparing the first and last 90 days "
            "of the window, every warehouse serves worse at the end than at the start. "
            "Madrid falls 5.4 points and Lyon 4.2, the two that were already weakest, "
            "while even the strongest sites, Lisbon and Porto, give up 2.3 and 3.7 "
            "points. A decline that touches every location points to a system-level "
            "cause in replenishment rather than a problem isolated to one hub.",
            body,
        )
    )
    # Bind the closing figure and its takeaway so the takeaway never orphans
    # onto a near-empty page.
    A(
        KeepTogether(
            chart(
                "14_before_after_fill_rate.png",
                "Figure 3. Fill rate, first 90 days versus last 90 days, by warehouse.",
            )
            + [
                Spacer(1, 8),
                takeaway_box(
                    "Treat the decline as structural and network-wide. Stabilising "
                    "replenishment is the first move, before any inventory is added."
                ),
            ]
        )
    )
    A(PageBreak())

    # ---- 5.2 supplier exposure ----
    A(H2("6.2  Two suppliers carry four fifths of the exposure"))
    A(
        Paragraph(
            f"Supplier 2 and Supplier 3 are the only two suppliers in the High risk "
            f"tier. Supplier 2 delivers on time on just 66% of orders with lead-time "
            f"variability of 4.0 days; Supplier 3 sits at 78% on-time and 3.8 days. "
            f"Together they are associated with {eur(two_high_lost)} of lost-sales "
            f"value, {two_high_share * 100:.0f}% of the network total. The remaining ten "
            f"suppliers cluster above 83% on-time with variability below 1.7 days. The "
            f"portfolio-level contrast is material: high-risk suppliers average "
            f"{high_stockout:.1f}% stockout exposure versus {non_high_stockout:.1f}% for "
            f"the rest. The relationship is clear enough for action: unreliable lead "
            f"times, not low received quantity, drive the lost sales, since both "
            f"high-risk suppliers still ship more than 92% of ordered units once they "
            f"arrive.",
            body,
        )
    )
    for it in chart(
        "04_supplier_reliability.png",
        "Figure 4. Lead-time variability against on-time delivery. Bubble area is lost-sales value.",
    ):
        A(it)
    A(
        Paragraph(
            "Ranked on on-time delivery alone, three suppliers fall below an 80% "
            "service line: Supplier 2 at 66%, Supplier 1 and Supplier 3 in the high "
            "70s. Supplier 1 sits in the Medium tier because the volume behind it is "
            "smaller, so it carries less lost-sales value despite similar reliability. "
            "This is why the priority order follows exposure, not the reliability "
            "percentage on its own: the same delay rate matters more behind a "
            "high-volume lane than a thin one.",
            body,
        )
    )
    for it in chart(
        "10_supplier_ontime_ranking.png",
        "Figure 5. On-time delivery rate by supplier against the 80% service line.",
    ):
        A(it)
    A(
        Paragraph(
            "The practical test is simple. If lead-time variability falls on the two "
            "high-risk suppliers and the affected SKUs recover before additional "
            "inventory is deployed, the supplier hypothesis is confirmed. If service "
            "does not recover, the next diagnostic branch is allocation policy, order "
            "sizing, and demand volatility. That validation path keeps the response "
            "disciplined: fix the highest-probability cause first, but instrument the "
            "result so the organisation does not mistake correlation for proof.",
            body,
        )
    )
    A(
        takeaway_box(
            "Supplier 2 and Supplier 3 are the single highest-return intervention. "
            "Expedite constrained lanes and renegotiate lead-time variability before "
            "any broad safety-stock increase."
        )
    )
    A(PageBreak())

    # ---- 5.3 warehouse misalignment ----
    A(H2("6.3  Inventory is misaligned, not uniformly excessive"))
    A(
        Paragraph(
            f"Madrid and Lyon post the network's worst fill rates, 94.6% and 94.1%, "
            f"while holding the least inventory value, around {eur(927400)} and "
            f"{eur(764000)}. Lisbon and Porto carry the most inventory, {eur(1370000)} "
            f"and {eur(1020000)}, and deliver the best service at 97.8% and 96.9%. The "
            f"fill-rate spread of {fill_spread:.1f} points across warehouses tracks "
            f"inventory placement, not total volume. Stock is sitting where service is "
            f"already strong and is thin where it is failing.",
            body,
        )
    )
    for it in chart(
        "03_warehouse_service_inventory.png",
        "Figure 6. Warehouse fill rate against days of supply. Bubble area is inventory value.",
    ):
        A(it)
    A(
        Paragraph(
            f"The same misalignment shows at SKU level. The median SKU holds "
            f"{dos_median:.0f} days of supply, but {dos_over30:.0f}% of SKUs carry 30 "
            f"days or more, and a thin tail stretches past {dos_max:.0f} days. Capital "
            f"is trapped in slow lines, an average of {eur(trapped_wc)} per day in the "
            f"trapped working-capital proxy and {eur(excess_inv)} per day above the ABC "
            f"days-of-supply caps, while the fast lines that drive lost sales run "
            f"short. The problem is distribution and policy, not a uniform shortage or "
            f"a uniform glut.",
            body,
        )
    )
    for it in chart(
        "09_days_of_supply_distribution.png",
        "Figure 7. Distribution of average days of supply across 120 SKUs (capped at 60 for display).",
    ):
        A(it)
    A(
        takeaway_box(
            "Rebalance toward Madrid and Lyon and trim the 30-plus-day tail rather "
            "than adding network-wide stock. The service gap is a placement gap."
        )
    )
    A(PageBreak())

    # ---- 5.4 category concentration ----
    A(H2("6.4  Half the value pool sits in one category"))
    A(
        Paragraph(
            f"The estimated twelve-month opportunity totals {eur(opp_total)}. The "
            f"Health category alone holds {eur(health_opp)} of it, "
            f"{health_opp / cat_pool * 100:.0f}% of the pool, with Pet Care second at "
            f"{eur(petcare_opp)} ({petcare_opp / cat_pool * 100:.0f}%). The remaining six "
            f"categories split the final third. Concentration this sharp means a "
            f"category-led operating rhythm will outperform a uniform service target "
            f"applied across the catalogue.",
            body,
        )
    )
    for it in chart(
        "01_opportunity_by_category.png", "Figure 8. Estimated 12-month opportunity by category."
    ):
        A(it)
    A(
        Paragraph(
            "Health does not lead the pool because it is large; it leads because it "
            f"serves worst. Health posts the lowest fill rate of any category at "
            f"{health_fill:.1f}% and the highest lost-sales value at €9.60M, despite "
            f"holding among the most inventory value. It represents "
            f"{health_lost_share:.0f}% of lost sales, while Pet Care adds another "
            f"{petcare_lost_share:.0f}%. Pet Care is second worst on both. The categories that "
            "serve well, Beverages at 98.1% and Snacks at 97.6%, contribute almost "
            "nothing to the pool. Service quality, not category size, sets the "
            "priority order.",
            body,
        )
    )
    for it in chart(
        "13_fill_rate_by_category.png",
        "Figure 9. Fill rate by product category over the full window.",
    ):
        A(it)
    A(
        Paragraph(
            "The category signal repeats at the category-region grain. Every Health "
            "segment scores in the high 40s to low 50s on governance priority, the "
            "highest band in the network, and each one is driven by stockout risk "
            "rather than overstock. Health in France South-East tops the segment "
            "ranking at 52. Pet Care segments form the next band. No region escapes "
            "the Health problem, which reinforces that the fix belongs at category and "
            "product level rather than at a single site.",
            body,
        )
    )
    for it in chart(
        "12_segment_risk_heatmap.png",
        "Figure 10. Governance priority score by category and region (0-100).",
    ):
        A(it)
    A(
        takeaway_box(
            f"Stand up a Health-category service cell first. Health and Pet Care "
            f"together hold {top2_cat_share * 100:.0f}% of the recoverable value."
        )
    )
    A(PageBreak())

    # ---- 5.5 SKU and ABC concentration ----
    A(H2("6.5  The loss is concentrated in a few SKU-location pairs"))
    A(
        Paragraph(
            f"Of the 480 product-warehouse pairs scored, only {n_high_pairs} reach the "
            f"High tier and {n_med_pairs} reach Medium; the other {480 - n_high_pairs - n_med_pairs} "
            f"are Low. The lost-sales value behind them is far more skewed than the "
            f"tier counts suggest. The worst 10% of pairs carry {top10_share:.0f}% of "
            f"all lost sales and the worst 5% carry {top5_share:.0f}%. A short, "
            f"well-chosen list of interventions therefore reaches most of the loss "
            f"without touching the long tail of pairs that lose almost nothing.",
            body,
        )
    )
    for it in chart(
        "07_lost_sales_concentration.png",
        "Figure 11. Cumulative share of lost-sales value across 480 ranked SKU-warehouse pairs.",
    ):
        A(it)
    A(
        Paragraph(
            f"All four High-tier pairs are the same product, Health Product 16, one in "
            f"each warehouse. Combined they represent {eur(h16_opp)} of twelve-month "
            f"opportunity and {h16_pool_share * 100:.0f}% of the ranked SKU-location "
            f"pool. The single largest pair is Health Product 16 in Madrid at roughly "
            f"€158k. A product that fails in all four warehouses at once is failing for "
            f"a product-level reason, supply or planning, not a local one, so it should "
            f"be run as one cross-warehouse recovery rather than four separate fixes.",
            body,
        )
    )
    for it in chart(
        "05_top_sku_opportunity.png",
        "Figure 12. Top 10 SKU-warehouse pairs by 12-month opportunity.",
    ):
        A(it)
    A(
        Paragraph(
            "The ABC view explains why so few lines carry so much. Class A holds 24 of "
            "the 120 SKUs and a third of inventory value, but 58% of lost-sales "
            "exposure. Class C, 60 SKUs and a third of inventory value, carries only "
            "8% of the loss while absorbing capital that the fast lines need. The "
            "network is overstocked on slow C lines and underserved on fast A lines. "
            "That is a policy and placement gap, not evidence that the network needs "
            "a larger inventory budget.",
            body,
        )
    )
    for it in chart(
        "08_abc_class_cohort.png",
        "Figure 13. Share of inventory value and lost-sales exposure by ABC class.",
    ):
        A(it)
    A(
        takeaway_box(
            "Run a dedicated recovery plan for Health Product 16 across all four "
            "warehouses and reweight the ABC policy from C toward A. The loss lives in "
            "a small, addressable set of fast lines."
        )
    )
    A(PageBreak())

    # ---- 5.6 value pool ----
    A(H2("6.6  The recoverable pool is real but partial"))
    A(
        Paragraph(
            f"The twelve-month value pool of {eur(opp_total)} splits into "
            f"{eur(opp_margin)} of recoverable lost-sales margin and {eur(opp_wc)} of "
            f"releasable working capital. Margin recovery dominates because the "
            f"network's core problem is service, not capital efficiency: the gross loss "
            f"to stockouts of {eur(total_lost)} is large, and even a partial recovery "
            f"of its margin outweighs the capital that can be safely released from "
            f"overstocked lines. The pool is deliberately a fraction of the gross loss, "
            f"because the scenario recovery rate assumes only part of unmet demand can "
            f"be converted once service is restored.",
            body,
        )
    )
    for it in chart(
        "11_opportunity_bridge.png",
        "Figure 14. Composition of the estimated 12-month opportunity (proxy).",
    ):
        A(it)
    A(
        Paragraph(
            f"The working-capital component is modest by design. The trapped-capital "
            f"proxy averages {eur(trapped_wc)} per day, but only a portion is "
            f"releasable without re-exposing service, so the pool credits {eur(opp_wc)} "
            f"rather than the full balance. This is the right asymmetry for a network "
            f"whose losses come from missing the sale, not from carrying too much: the "
            f"prize is protecting and recovering demand, with capital release as a "
            f"secondary benefit from trimming the slow-moving tail.",
            body,
        )
    )
    A(
        takeaway_box(
            "Manage to the margin pool first. Working-capital release is a by-product "
            "of fixing service and trimming slow lines, not a separate programme."
        )
    )
    A(PageBreak())

    # ========================= RISKS & LIMITATIONS ======================
    A(Paragraph("SECTION 7", kicker))
    A(H1("Risks, limitations, and caveats"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The findings are only as strong as the assumptions behind them. This "
            "section states the main limits plainly so that the recommendations are "
            "read with the right level of confidence.",
            body,
        )
    )

    A(H2("Data scope"))
    A(
        Paragraph(
            "The dataset is synthetic and does not represent a specific company. It is "
            "constructed to exhibit the structural patterns common in multi-warehouse "
            "FMCG operations: a service decline, supplier concentration, and ABC "
            "inventory skew. The findings describe the modelled network; the methods "
            "apply directly to operational data.",
            body,
        )
    )

    A(H2("Proxy financials, not audited profit and loss"))
    A(
        Paragraph(
            "Every euro figure is a directional proxy. Lost-sales value prices unmet "
            "demand at list price and assumes the demand was real and would have "
            "converted. The margin proxy applies category-level gross-margin "
            "assumptions rather than SKU-level actuals. The opportunity pool applies a "
            "scenario recovery rate. None of these are audited profit and loss, and "
            "the absolute numbers would move with different assumptions. The relative "
            "structure, which suppliers, products, and locations dominate, is far more "
            "stable than the absolute totals.",
            body,
        )
    )

    A(H2("Correlation, not proven causation"))
    A(
        Paragraph(
            "The governance priority score ranks where to look; it does not prove why "
            "a line fails. The supplier finding rests on a strong association between "
            "lead-time variability and lost sales, but the data cannot rule out a "
            "confound such as those suppliers serving inherently harder demand. The "
            "Health Product 16 finding identifies a product that fails everywhere, "
            "which is suggestive of a product-level cause, but confirming it requires "
            "operational review of the supply and planning records behind that SKU.",
            body,
        )
    )

    A(H2("Scope boundaries"))
    A(
        Paragraph(
            "The analysis covers service and inventory outcomes. It does not model "
            "price elasticity, promotional cannibalisation, substitution between SKUs "
            "when one stocks out, or the cost of the interventions themselves. A "
            "stockout on one product may shift demand to another rather than lose it "
            "outright, which would lower the true lost-sales figure. The recommended "
            "actions carry implementation cost that is not netted against the value "
            "pool. Both effects argue for treating the pool as an upper-bound on "
            "service-side value rather than a net benefit.",
            body,
        )
    )
    A(Spacer(1, 6))
    A(
        takeaway_box(
            "Trust the structure more than the totals. The ranking of suppliers, "
            "products, and locations is more stable; the absolute euro figures are scenario "
            "estimates that move with the assumptions."
        )
    )
    A(PageBreak())

    # ========================= RECOMMENDATIONS ==========================
    A(Paragraph("SECTION 8", kicker))
    A(H1("Recommendations and action priorities"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            f"The network's service problem is concentrated enough to act on "
            f"precisely. Two suppliers, one product family, and two warehouses form a "
            f"concentrated intervention set within the {eur(total_lost)} lost-sales "
            f"exposure. The declining fill-rate "
            f"trend means the window to act is closing, but it also means targeted "
            f"fixes will show up clearly against the baseline. The six recommendations "
            f"below are ordered by return and by dependency: the first two carry the "
            f"bulk of the value and require no inventory investment.",
            body,
        )
    )
    A(Spacer(1, 4))

    recs = [
        (
            "1. Remediate Supplier 2 and Supplier 3",
            f"Expedite replenishment on their constrained lanes and renegotiate "
            f"lead-time variability toward the sub-1.7-day network norm. Addresses "
            f"{two_high_share * 100:.0f}% of lost-sales value with no added stock.",
            "0-3 months",
            "Highest",
        ),
        (
            "2. Launch a Health Product 16 recovery plan",
            f"Treat the four High-tier pairs as one cross-warehouse effort: review the "
            f"supply source, reset safety stock, and protect allocation. Largest "
            f"single SKU opportunity at {eur(h16_opp)}.",
            "0-3 months",
            "High",
        ),
        (
            "3. Stand up a Health and Pet Care service cell",
            f"Own the two categories that hold {top2_cat_share * 100:.0f}% of the "
            f"{eur(opp_total)} pool, with weekly review against the fill-rate "
            f"baseline and stockout-driven safety-stock rules.",
            "0-6 months",
            "High",
        ),
        (
            "4. Rebalance inventory to Madrid and Lyon",
            f"Shift placement from Lisbon and Porto toward the two underserved hubs "
            f"rather than adding network-wide stock. Closes the {fill_spread:.1f}-point "
            f"fill spread without raising total inventory.",
            "3-6 months",
            "Medium-High",
        ),
        (
            "5. Reweight the ABC days-of-supply policy",
            f"Trim the {dos_over30:.0f}% of SKUs holding 30-plus days, concentrated in "
            f"Class C, and redeploy the freed capital to Class A lines that carry 58% "
            f"of lost-sales exposure.",
            "3-9 months",
            "Medium",
        ),
        (
            "6. Instrument the trend and hold the line",
            "Track monthly fill and stockout against the December 2025 baseline on the "
            "dashboard, with an alert when any warehouse drops below its 90-day "
            "average. Prevents the structural drift from resuming after the fixes.",
            "Ongoing",
            "Medium",
        ),
    ]
    rows = [["Recommendation", "Horizon", "Priority"]]
    for title, desc, hor, pr in recs:
        cell = Paragraph(
            f"<b>{title}</b><br/><font size=8.5 color='#6e6e73'>{desc}</font>",
            ParagraphStyle("rc", parent=body, spaceAfter=0, leading=12.5),
        )
        rows.append([cell, Paragraph(hor, small), Paragraph(pr, small)])
    rt = Table(rows, colWidths=[CW - 5.6 * cm, 2.8 * cm, 2.8 * cm])
    rt.setStyle(
        TableStyle(
            [
                ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 8.5),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                ("BACKGROUND", (0, 0), (-1, 0), INK),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("LINEBELOW", (0, 0), (-1, -1), 0.4, LINE),
                ("ALIGN", (1, 0), (2, -1), "CENTER"),
            ]
        )
    )
    A(rt)
    A(Spacer(1, 12))
    A(H2("Sequencing logic"))
    A(
        Paragraph(
            f"The order is deliberate. Supplier remediation and the Health Product 16 "
            f"plan come first because they carry the largest return, need no capital, "
            f"and stabilise the inputs that every later fix depends on. The category "
            f"service cell institutionalises that focus. Inventory rebalancing and the "
            f"ABC policy change come next, once supply is reliable enough that moving "
            f"stock does not simply relocate the stockout. The monitoring step runs "
            f"throughout. Executed in sequence, the plan targets the {eur(opp_total)} "
            f"pool, {eur(opp_margin)} of recoverable margin and {eur(opp_wc)} of "
            f"released capital, with the first two actions carrying most of it.",
            body,
        )
    )
    A(H2("Execution governance"))
    A(
        Paragraph(
            "The programme should be governed as a 12-week recovery sprint followed by "
            "a six-month operating cadence. The first four weeks should validate the "
            "supplier and SKU hypotheses, lock the baseline, and agree exception rules "
            "for constrained allocation. Weeks five to twelve should execute supplier "
            "stabilisation, Health Product 16 protection, and the first Madrid/Lyon "
            "rebalancing moves. The six-month cadence should then institutionalise the "
            "category service cell, ABC policy changes, and monitoring rules.",
            body,
        )
    )
    control_points = [
        (
            "Supplier 2 and 3 recovery",
            (
                "Owner: Procurement / Supply Planning. Target signal: on-time rate "
                f"moves from {high_on_time:.0f}% toward the {non_high_on_time:.0f}% "
                "non-high supplier benchmark, with lead-time variability trending "
                "below 2.0 days."
            ),
        ),
        (
            "Health Product 16 protection",
            (
                "Owner: Category / Inventory Planning. Target signal: all four "
                "warehouse pairs exit High tier and SKU stockout rate falls before "
                "broad category stock is added."
            ),
        ),
        (
            "Madrid and Lyon rebalancing",
            (
                "Owner: Network Planning. Target signal: the warehouse fill spread "
                f"narrows from {fill_spread:.1f} points without raising total network "
                "inventory value."
            ),
        ),
        (
            "ABC policy reset",
            (
                "Owner: Inventory Governance. Target signal: the 30-plus-day SKU "
                f"share falls from {dos_over30:.0f}% while Class A lost-sales exposure "
                "declines."
            ),
        ),
        (
            "Value tracking",
            (
                "Owner: Finance / Analytics. Target signal: recovered margin run-rate "
                f"tracks toward the {eur(opp_margin)} 12-month proxy, with capital "
                "release measured separately from service gain."
            ),
        ),
    ]
    for title, text in control_points:
        A(Paragraph(f"<b>{title}.</b> {text}", body))
    A(Spacer(1, 10))
    A(H2("Further questions before scale-up"))
    further_questions = [
        (
            "Substitution",
            "What share of stockout demand is truly lost versus substituted to adjacent SKUs, especially inside Health and Pet Care?",
        ),
        (
            "Demand abnormality",
            "Did promotions, commercial commitments, or one-off allocation rules distort Health Product 16 demand during the final quarter of 2025?",
        ),
        (
            "Warehouse capacity",
            "Do Madrid and Lyon have the physical capacity, labour coverage, and inbound appointment availability to absorb rebalanced stock without creating a new bottleneck?",
        ),
        (
            "Supplier constraints",
            "Which supplier terms, minimum-order constraints, or production calendars prevent Supplier 2 and Supplier 3 from reducing lead-time variability?",
        ),
        (
            "Net economics",
            "What implementation cost should be netted against the value pool before the programme is converted from recovery sprint to recurring operating model?",
        ),
    ]
    for title, text in further_questions:
        A(Paragraph(f"<b>{title}.</b> {text}", body))
    A(Spacer(1, 6))
    A(
        takeaway_box(
            "Do the two no-capital fixes first, then prove the inventory move. Supplier "
            "discipline and one product recovery plan reach most of the value before "
            "any broad stock decision is needed."
        )
    )
    A(PageBreak())

    # ============================= APPENDIX =============================
    A(Paragraph("SECTION 9", kicker))
    A(H1("Appendix"))
    A(HRule(CW))
    A(Spacer(1, 6))
    A(
        Paragraph(
            "The tables below give the supporting detail behind the findings. All "
            "figures are reproducible from the source pipeline.",
            body,
        )
    )

    A(H2("A. Supplier risk ranking"))
    srank = sup.sort_values("governance_priority_score", ascending=False)
    rows = [["Supplier", "Tier", "On-time", "LT var (d)", "Lost sales", "Priority"]]
    for _, r in srank.iterrows():
        rows.append(
            [
                r.supplier_name,
                r.risk_tier,
                f"{r.on_time_delivery_rate * 100:.0f}%",
                f"{r.lead_time_variability:.1f}",
                eur(r.lost_sales_revenue),
                f"{r.governance_priority_score:.0f}",
            ]
        )
    A(
        data_table(
            rows,
            [3.4 * cm, 1.9 * cm, 1.9 * cm, 2.1 * cm, 2.6 * cm, 1.8 * cm],
            align_right=[2, 3, 4, 5],
        )
    )
    A(Spacer(1, 14))

    A(H2("B. Warehouse service and inventory profile"))
    rows = [["Warehouse", "Region", "Fill", "Stockout", "DOS", "Inventory"]]
    for _, r in wh.sort_values("fill_rate").iterrows():
        rows.append(
            [
                r.warehouse_name,
                r.region,
                f"{r.fill_rate * 100:.1f}%",
                f"{r.stockout_rate * 100:.1f}%",
                f"{r.average_days_of_supply:.1f}",
                eur(r.inventory_value),
            ]
        )
    A(
        data_table(
            rows,
            [4.0 * cm, 3.6 * cm, 1.7 * cm, 1.9 * cm, 1.6 * cm, 2.3 * cm],
            align_right=[2, 3, 4, 5],
        )
    )
    A(Spacer(1, 14))

    A(H2("C. Twelve-month opportunity by category"))
    rows = [["Category", "Opportunity (12m proxy)", "Share", "Fill rate"]]
    for _, r in cat.iterrows():
        cf = catfill.loc[r.entity_name, "fill"] if r.entity_name in catfill.index else float("nan")
        rows.append(
            [
                r.entity_name,
                eur(r.opportunity_total_12m_proxy),
                f"{r.opportunity_total_12m_proxy / cat_pool * 100:.0f}%",
                f"{cf:.1f}%",
            ]
        )
    A(data_table(rows, [4.6 * cm, 4.6 * cm, 2.2 * cm, CW - 11.4 * cm], align_right=[1, 2, 3]))
    A(Spacer(1, 14))

    A(H2("D. ABC class concentration"))
    rows = [["Class", "SKUs", "Inventory value share", "Lost-sales share"]]
    for c in ["A", "B", "C"]:
        rows.append(
            [
                f"Class {c}",
                f"{int(abc.loc[c, 'n'])}",
                f"{abc.loc[c, 'inv_sh']:.0f}%",
                f"{abc.loc[c, 'lost_sh']:.0f}%",
            ]
        )
    A(data_table(rows, [3.5 * cm, 2.5 * cm, 5.0 * cm, CW - 11.0 * cm], align_right=[1, 2, 3]))
    A(Spacer(1, 14))

    A(H2("E. Top SKU-location priorities"))
    skp = sku_prio.sort_values("opportunity_total_12m_proxy", ascending=False).head(8)
    rows = [["SKU-location", "Opportunity (12m proxy)", "Share of SKU pool"]]
    for _, r in skp.iterrows():
        rows.append(
            [
                r.entity_name,
                eur(r.opportunity_total_12m_proxy),
                f"{r.opportunity_total_12m_proxy / sku_pool * 100:.0f}%",
            ]
        )
    A(data_table(rows, [8.5 * cm, 4.5 * cm, CW - 13.0 * cm], align_right=[1, 2]))
    A(Spacer(1, 14))

    A(H2("F. Metric definitions"))
    defs = [
        ("Fill rate", "Units fulfilled / units demanded."),
        ("Stockout rate", "Units of unmet demand / units demanded."),
        ("Days of supply", "Available inventory / average daily demand."),
        ("Lost-sales value", "Unmet demand priced at unit selling price."),
        ("Lost-sales margin proxy", "Lost-sales value at category gross-margin assumptions."),
        ("Excess inventory", "Inventory value above the ABC days-of-supply policy cap."),
        ("Trapped working capital", "Average daily inefficient inventory capital proxy."),
        (
            "Governance priority score",
            "0-100 composite of service, stockout, excess, supplier, and working-capital risk.",
        ),
        (
            "12-month value pool",
            "Recoverable lost-sales margin plus releasable working capital, proxy estimates.",
        ),
    ]
    rows = [["Term", "Definition"]]
    for t, d in defs:
        rows.append([Paragraph(f"<b>{t}</b>", small), Paragraph(d, small)])
    A(data_table(rows, [4.7 * cm, CW - 4.7 * cm]))
    A(PageBreak())

    A(H2(f"G. Category-region segment risk (all {len(seg)} segments)"))
    A(
        Paragraph(
            "Composite governance priority score by category-region segment, with the "
            "main risk driver the score identifies for each. Ranked highest priority "
            "first.",
            small,
        )
    )
    A(Spacer(1, 4))
    segr = seg.sort_values("governance_priority_score", ascending=False)
    rows = [["Segment", "Score", "Tier", "Main driver", "Fill"]]
    for _, r in segr.iterrows():
        rows.append(
            [
                r.segment_id,
                f"{r.governance_priority_score:.0f}",
                r.risk_tier,
                r.main_risk_driver,
                f"{r.fill_rate * 100:.1f}%",
            ]
        )
    A(
        data_table(
            rows,
            [6.0 * cm, 1.6 * cm, 1.9 * cm, 4.0 * cm, CW - 13.5 * cm],
            align_right=[1, 4],
            fs=7.6,
        )
    )
    A(PageBreak())

    A(H2("H. Monthly service KPIs (24 months)"))
    A(
        Paragraph(
            "Network fill rate, stockout rate, and lost-sales value by month over the "
            "full observation window. This is the series behind Figures 1 and 2.",
            small,
        )
    )
    A(Spacer(1, 4))
    rows = [["Month", "Fill rate", "Stockout rate", "Lost-sales value"]]
    for m, r in monthly.iterrows():
        rows.append([str(m), f"{r.fill:.1f}%", f"{r.so * 100:.1f}%", eur(r.lost)])
    A(data_table(rows, [3.5 * cm, 3.5 * cm, 4.0 * cm, CW - 11.0 * cm], align_right=[1, 2, 3], fs=8))
    A(Spacer(1, 14))

    A(H2("I. Most overstocked SKUs (releasable-capital candidates)"))
    A(
        Paragraph(
            "Products carrying the most days of supply, the working-capital side of the "
            "opportunity. These are the lines to trim first when redeploying capital to "
            "fast-moving Class A items.",
            small,
        )
    )
    A(Spacer(1, 4))
    over = pinv.sort_values("average_days_of_supply", ascending=False).head(10)
    rows = [["Product", "Category", "ABC", "Days of supply", "Avg inventory value"]]
    for _, r in over.iterrows():
        rows.append(
            [
                r.product_name,
                r.category,
                r.abc_class,
                f"{r.average_days_of_supply:.0f}",
                eur(r.average_inventory_value),
            ]
        )
    A(
        data_table(
            rows, [5.4 * cm, 3.2 * cm, 1.6 * cm, 3.2 * cm, CW - 13.4 * cm], align_right=[3, 4], fs=8
        )
    )
    A(Spacer(1, 12))

    doc.multiBuild(s)
    print("wrote", OUT / "service_inventory_intelligence_report.pdf")


if __name__ == "__main__":
    build()
