"""SQL reconciliation gate: reloads the raw and processed CSVs into DuckDB,
smoke-tests every KPI query in 03_kpi_queries.sql, and runs the validation
checks in 04_validation_queries.sql, writing pass/fail results for CI."""

from __future__ import annotations

import duckdb
import pandas as pd

from src.config import DATA_PROCESSED, PROJECT_ROOT, SQL_DIR
from src.warehouse import load_csv_table, load_raw_tables

OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


PROCESSED_TABLES = {
    "daily_product_warehouse_metrics": DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
    "supplier_performance_summary": DATA_PROCESSED / "supplier_performance_summary.csv",
    "product_inventory_profile": DATA_PROCESSED / "product_inventory_profile.csv",
    "warehouse_service_profile": DATA_PROCESSED / "warehouse_service_profile.csv",
    "sku_risk_table": DATA_PROCESSED / "sku_risk_table.csv",
}


def _split_sql_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def _run_kpi_query_smoke_test() -> int:
    con = duckdb.connect(database=":memory:")
    try:
        load_raw_tables(con)

        intermediate_sql = (SQL_DIR / "02_intermediate_views.sql").read_text(encoding="utf-8")
        con.execute(intermediate_sql)

        load_csv_table(con, "sku_risk_table", PROCESSED_TABLES["sku_risk_table"])

        kpi_sql = (SQL_DIR / "03_kpi_queries.sql").read_text(encoding="utf-8")
        statements = _split_sql_statements(kpi_sql)
        for statement in statements:
            con.execute(statement).fetchall()
    finally:
        con.close()

    return len(statements)


def run_sql_quality_gate() -> pd.DataFrame:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        load_raw_tables(con)
        for table_name, path in PROCESSED_TABLES.items():
            load_csv_table(con, table_name, path)

        sql = (SQL_DIR / "04_validation_queries.sql").read_text(encoding="utf-8")
        checks = con.execute(sql).df()
    finally:
        con.close()

    checks.to_csv(OUTPUT_TABLES_DIR / "ci_sql_validation_checks.csv", index=False)

    failed = int((checks["status"] != "PASS").sum())
    kpi_query_count = _run_kpi_query_smoke_test()
    print("SQL quality gate complete.")
    print(f"Checks: {len(checks)} | Non-pass: {failed}")
    print(f"KPI query smoke tests: {kpi_query_count}")

    if failed > 0:
        failed_checks = checks[checks["status"] != "PASS"]
        print("Failing SQL checks:")
        print(failed_checks[["check_name", "issue_count", "status"]].to_string(index=False))
        raise SystemExit(1)

    return checks


if __name__ == "__main__":
    run_sql_quality_gate()
