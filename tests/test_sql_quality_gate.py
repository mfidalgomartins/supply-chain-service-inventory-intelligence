from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.sql_quality_gate import _split_sql_statements


def test_split_sql_statements_removes_empty_segments() -> None:
    sql = "SELECT 1; SELECT 2;"
    assert _split_sql_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_inventory_kpis_average_daily_network_balances() -> None:
    sql_path = Path(__file__).resolve().parents[1] / "sql" / "03_kpi_queries.sql"
    statements = _split_sql_statements(sql_path.read_text(encoding="utf-8"))
    category_query = next(statement for statement in statements if "-- KPI 04:" in statement)
    concentration_query = next(statement for statement in statements if "-- KPI 10:" in statement)

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(
            """
            CREATE TABLE products AS
            SELECT * FROM (VALUES
                ('P1', 'A'), ('P2', 'A'), ('P3', 'A'), ('P4', 'A'), ('P5', 'A')
            ) AS t(product_id, category);

            CREATE TABLE product_classification AS
            SELECT * FROM (VALUES
                ('P1', 'A'), ('P2', 'B'), ('P3', 'B'), ('P4', 'C'), ('P5', 'C')
            ) AS t(product_id, abc_class);

            CREATE TABLE inventory_snapshots AS
            SELECT * FROM (VALUES
                ('2026-01-01'::DATE, 'P1', 'W1', 10.0),
                ('2026-01-01'::DATE, 'P1', 'W2', 20.0),
                ('2026-01-02'::DATE, 'P1', 'W1', 30.0),
                ('2026-01-02'::DATE, 'P1', 'W2', 40.0),
                ('2026-01-01'::DATE, 'P2', 'W1', 20.0),
                ('2026-01-02'::DATE, 'P2', 'W1', 20.0),
                ('2026-01-01'::DATE, 'P3', 'W1', 15.0),
                ('2026-01-02'::DATE, 'P3', 'W1', 15.0),
                ('2026-01-01'::DATE, 'P4', 'W1', 10.0),
                ('2026-01-02'::DATE, 'P4', 'W1', 10.0),
                ('2026-01-01'::DATE, 'P5', 'W1', 5.0),
                ('2026-01-02'::DATE, 'P5', 'W1', 5.0)
            ) AS t(snapshot_date, product_id, warehouse_id, inventory_value);
            """
        )

        category = con.execute(category_query).fetchdf().iloc[0]
        concentration = con.execute(concentration_query).fetchdf().iloc[0]
    finally:
        con.close()

    assert category["avg_daily_inventory_value"] == pytest.approx(100.0)
    assert category["peak_daily_inventory_value"] == pytest.approx(120.0)
    assert concentration["product_id"] == "P1"
    assert concentration["avg_inventory_value"] == pytest.approx(50.0)
    assert concentration["total_portfolio_value"] == pytest.approx(100.0)
