from __future__ import annotations

import duckdb
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT, SQL_DIR
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, PROJECT_ROOT, SQL_DIR


OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


RAW_TABLES = {
    "products": DATA_RAW / "products.csv",
    "suppliers": DATA_RAW / "suppliers.csv",
    "warehouses": DATA_RAW / "warehouses.csv",
    "inventory_snapshots": DATA_RAW / "inventory_snapshots.csv",
    "demand_history": DATA_RAW / "demand_history.csv",
    "purchase_orders": DATA_RAW / "purchase_orders.csv",
    "product_classification": DATA_RAW / "product_classification.csv",
}

PROCESSED_TABLES = {
    "daily_product_warehouse_metrics": DATA_PROCESSED / "daily_product_warehouse_metrics.csv",
    "supplier_performance_summary": DATA_PROCESSED / "supplier_performance_summary.csv",
    "product_inventory_profile": DATA_PROCESSED / "product_inventory_profile.csv",
    "warehouse_service_profile": DATA_PROCESSED / "warehouse_service_profile.csv",
    "sku_risk_table": DATA_PROCESSED / "sku_risk_table.csv",
}


def run_sql_quality_gate() -> pd.DataFrame:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        for table_name, path in {**RAW_TABLES, **PROCESSED_TABLES}.items():
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE);
                """
            )

        sql = (SQL_DIR / "04_validation_queries.sql").read_text(encoding="utf-8")
        checks = con.execute(sql).df()
    finally:
        con.close()

    checks.to_csv(OUTPUT_TABLES_DIR / "ci_sql_validation_checks.csv", index=False)

    failed = int((checks["status"] != "PASS").sum())
    print("SQL quality gate complete.")
    print(f"Checks: {len(checks)} | Non-pass: {failed}")

    if failed > 0:
        failed_checks = checks[checks["status"] != "PASS"]
        print("Failing SQL checks:")
        print(failed_checks[["check_name", "issue_count", "status"]].to_string(index=False))
        raise SystemExit(1)

    return checks


if __name__ == "__main__":
    run_sql_quality_gate()
