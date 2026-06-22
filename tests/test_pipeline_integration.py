"""End-to-end, in-process execution of the full analytics pipeline.

Runs every stage in dependency order inside a single interpreter (rather than
the subprocess orchestration of ``run_pipeline.py``) so that:

* a single regression test proves the whole pipeline executes cleanly and
  produces the published artifacts, and
* coverage reflects the rendering and gate stages that are exercised only by a
  full run.

The stages write to the tracked ``data/``, ``outputs/`` and ``index.html``
locations. Because float serialisation is stabilised and the dataset is seeded,
regenerating those artifacts is deterministic.
"""

from __future__ import annotations

import pandas as pd
from src import (
    build_charts,
    build_report,
    ci_quality_gate,
    data_contracts,
    data_generation,
    data_preparation,
    executive_dashboard,
    impact_analysis,
    pre_delivery_validation,
    scoring,
    sql_quality_gate,
)
from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT


def test_full_pipeline_runs_and_publishes_artifacts() -> None:
    # Stage 1-5: generate, transform, score, validate contracts, estimate impact.
    data_generation.generate_all_tables()
    assert (DATA_RAW / "products.csv").exists()

    data_preparation.run_data_preparation()
    assert (DATA_PROCESSED / "daily_product_warehouse_metrics.csv").exists()

    scoring.run_scoring()
    data_contracts.run_data_contracts()
    impact_analysis.run_impact_analysis()

    # Stage 6-8: publication artifacts.
    build_charts.main()
    graphs = sorted((PROJECT_ROOT / "outputs" / "graphs").glob("*.png"))
    assert len(graphs) == ci_quality_gate.REQUIRED_GRAPH_COUNT

    build_report.build()
    assert (
        PROJECT_ROOT / "outputs" / "reports" / "service_inventory_intelligence_report.pdf"
    ).exists()

    dashboard_path = executive_dashboard.build_executive_dashboard()
    assert dashboard_path.exists()
    assert "integrity=" in dashboard_path.read_text(encoding="utf-8")

    # Stage 9-11: release gates. Each raises on failure, so reaching the end
    # without exception means every gate passed.
    sql_result = sql_quality_gate.run_sql_quality_gate()
    assert isinstance(sql_result, pd.DataFrame)
    assert (sql_result["status"] == "PASS").all()

    pre_delivery_validation.run_pre_delivery_validation()
    ci_quality_gate.run_ci_quality_gate()
