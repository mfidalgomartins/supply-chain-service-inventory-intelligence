from __future__ import annotations

from src.executive_dashboard import (
    PLOTLY_CDN_URL,
    PLOTLY_SRI,
    _build_html,
    _stabilize_floats,
)


def _minimal_payload() -> dict:
    return {
        "generated_at": "2026-04-02 00:00 UTC",
        "dashboard_version": "v2026.04.02.abcdef12",
        "monthly_sku_compact": {
            "columns": [
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
            ],
            "rows": [],
            "dim": {
                "month": [],
                "region": [],
                "warehouse_id": [],
                "product_id": [],
                "category": [],
                "supplier_id": [],
                "abc_class": [],
            },
        },
        "product_name_map": {},
        "suppliers": [],
        "warehouses": [],
        "sku_risk_baseline": [],
        "meta": {
            "date_min": "2024-01-01",
            "date_max": "2025-12-01",
            "row_count_monthly_sku": 0,
            "dataset_hash": "abcdef1234567890",
            "official_snapshot": {
                "overall_fill_rate": 0.92,
                "overall_stockout_rate": 0.08,
                "total_lost_sales_revenue": 1000000.0,
                "trapped_working_capital_proxy_average": 2000000.0,
                "opportunity_total_12m_proxy": 500000.0,
            },
            "assumptions_default": {
                "recoverable_margin_rate": 0.35,
                "releasable_wc_rate": 0.25,
                "slow_moving_incremental_weight": 0.50,
            },
        },
    }


def test_dashboard_template_has_theme_toggle_without_executive_noise() -> None:
    html = _build_html(_minimal_payload())
    assert 'id="toggle-theme"' in html
    assert '[data-theme="dark"]' in html
    assert "Dashboard Version:" not in html
    assert "Dataset Fingerprint:" not in html
    assert "Data Refresh:" not in html


def test_dashboard_template_avoids_frontend_scoring_formulas() -> None:
    html = _build_html(_minimal_payload())
    assert "function riskTier(" not in html
    assert "function recommendedAction(" not in html
    assert "0.24 * serviceRiskScore" not in html
    assert "skuRiskBaselineMap" in html


def test_dashboard_loads_pinned_plotly_bundle_from_cdn() -> None:
    html = _build_html(_minimal_payload())
    assert f'src="{PLOTLY_CDN_URL}"' in html
    assert "__PLOTLY_CDN_URL__" not in html


def test_dashboard_pins_plotly_with_subresource_integrity() -> None:
    html = _build_html(_minimal_payload())
    assert PLOTLY_SRI.startswith("sha384-")
    assert f'integrity="{PLOTLY_SRI}"' in html
    assert 'crossorigin="anonymous"' in html
    assert "__PLOTLY_SRI__" not in html


def test_dashboard_head_has_branded_favicon_and_share_metadata() -> None:
    html = _build_html(_minimal_payload())
    # Branded SVG favicon (not the empty data: placeholder).
    assert 'rel="icon" href="data:image/svg+xml,' in html
    assert 'href="data:," ' not in html
    # Mobile browser chrome tint for both schemes.
    assert '<meta name="theme-color" media="(prefers-color-scheme: light)"' in html
    assert '<meta name="theme-color" media="(prefers-color-scheme: dark)"' in html
    # Document + social share metadata for rich link previews.
    assert '<meta name="description"' in html
    assert '<meta property="og:title"' in html
    assert '<meta property="og:image"' in html
    assert '<meta name="twitter:card" content="summary_large_image"' in html


def test_payload_floats_are_stabilised_for_reproducible_output() -> None:
    payload = {"a": 20281573.240000002, "b": [1.123456789, {"c": 2.0}], "d": "x", "e": 3}
    assert _stabilize_floats(payload) == {
        "a": 20281573.24,
        "b": [1.123457, {"c": 2.0}],
        "d": "x",
        "e": 3,
    }


def test_dashboard_sortable_table_announces_sort_state() -> None:
    html = _build_html(_minimal_payload())
    assert 'aria-sort="descending">Priority</th>' in html
    assert "<caption" in html
