from __future__ import annotations

from src import analyze, build_dashboard, generate_data, run_sql_analysis, validate


def test_generate_data_wrapper_routes_to_authoritative_module(monkeypatch) -> None:
    called = {"ok": False}

    def _fake_generate() -> None:
        called["ok"] = True

    monkeypatch.setattr(generate_data, "generate_all_tables", _fake_generate)
    generate_data.main()
    assert called["ok"]


def test_validate_wrapper_routes_to_authoritative_module(monkeypatch) -> None:
    called = {"ok": False}

    def _fake_validate() -> None:
        called["ok"] = True

    monkeypatch.setattr(validate, "run_pre_delivery_validation", _fake_validate)
    validate.main()
    assert called["ok"]


def test_build_dashboard_wrapper_routes_to_authoritative_module(monkeypatch) -> None:
    called = {"ok": False}

    def _fake_dashboard() -> None:
        called["ok"] = True

    monkeypatch.setattr(build_dashboard, "build_executive_dashboard", _fake_dashboard)
    build_dashboard.main()
    assert called["ok"]


def test_run_sql_analysis_wrapper_routes_to_authoritative_modules(monkeypatch) -> None:
    called = {"prep": False, "gate": False}

    def _fake_prep() -> None:
        called["prep"] = True

    def _fake_gate() -> None:
        called["gate"] = True

    monkeypatch.setattr(run_sql_analysis, "run_data_preparation", _fake_prep)
    monkeypatch.setattr(run_sql_analysis, "run_sql_quality_gate", _fake_gate)
    run_sql_analysis.main()

    assert called["prep"]
    assert called["gate"]


def test_analyze_wrapper_routes_to_authoritative_modules(monkeypatch) -> None:
    called = {"kpi": False, "impact": False, "viz": False}

    def _fake_kpi() -> None:
        called["kpi"] = True

    def _fake_impact() -> None:
        called["impact"] = True

    def _fake_viz() -> None:
        called["viz"] = True

    monkeypatch.setattr(analyze, "run_analysis", _fake_kpi)
    monkeypatch.setattr(analyze, "run_impact_analysis", _fake_impact)
    monkeypatch.setattr(analyze, "run_visualization_suite", _fake_viz)
    analyze.main()

    assert called["kpi"]
    assert called["impact"]
    assert called["viz"]
