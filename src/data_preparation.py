from __future__ import annotations

import duckdb
import pandas as pd

try:
    from src.config import DATA_PROCESSED, DATA_RAW, SQL_DIR
except ModuleNotFoundError:
    from config import DATA_PROCESSED, DATA_RAW, SQL_DIR


RAW_TABLE_FILES = {
    "products": "products.csv",
    "suppliers": "suppliers.csv",
    "warehouses": "warehouses.csv",
    "inventory_snapshots": "inventory_snapshots.csv",
    "demand_history": "demand_history.csv",
    "purchase_orders": "purchase_orders.csv",
    "product_classification": "product_classification.csv",
}

INTERMEDIATE_VIEWS = {
    "daily_product_warehouse_metrics": [
        "date",
        "warehouse_id",
        "region",
        "product_id",
        "category",
        "supplier_id",
        "abc_class",
        "criticality_level",
        "units_demanded",
        "units_fulfilled",
        "units_lost_sales",
        "stockout_flag",
        "fill_rate",
        "promo_flag",
        "on_hand_units",
        "on_order_units",
        "available_units",
        "days_of_supply",
        "inventory_value",
        "lost_sales_revenue",
        "service_gap_units",
    ],
    "supplier_performance_summary": [
        "supplier_id",
        "supplier_name",
        "on_time_delivery_rate",
        "average_delay_days",
        "lead_time_variability",
        "received_vs_ordered_fill_rate",
        "supplier_service_risk_proxy",
    ],
    "product_inventory_profile": [
        "product_id",
        "product_name",
        "category",
        "abc_class",
        "average_inventory_units",
        "average_inventory_value",
        "average_days_of_supply",
        "stockout_frequency",
        "fill_rate_average",
        "lost_sales_exposure",
        "slow_moving_inventory_proxy",
        "excess_inventory_proxy",
        "working_capital_risk_proxy",
    ],
    "warehouse_service_profile": [
        "warehouse_id",
        "warehouse_name",
        "region",
        "fill_rate",
        "stockout_rate",
        "lost_sales_value",
        "average_days_of_supply",
        "inventory_value",
        "capacity_pressure_proxy",
        "warehouse_service_risk_proxy",
    ],
}



def _load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    for table_name, file_name in RAW_TABLE_FILES.items():
        csv_path = DATA_RAW / file_name
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE);
            """
        )



def _execute_intermediate_sql(con: duckdb.DuckDBPyConnection) -> None:
    sql_text = (SQL_DIR / "02_intermediate_views.sql").read_text(encoding="utf-8")
    con.execute(sql_text)



def _validate_columns(df: pd.DataFrame, expected_columns: list[str], object_name: str) -> None:
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{object_name} is missing expected columns: {missing}")



def _materialize_views(con: duckdb.DuckDBPyConnection) -> None:
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    for view_name, expected_cols in INTERMEDIATE_VIEWS.items():
        df = con.execute(f"SELECT * FROM {view_name}").df()
        _validate_columns(df, expected_cols, view_name)

        output_path = DATA_PROCESSED / f"{view_name}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved {view_name}: {len(df):,} rows")



def run_data_preparation() -> None:
    con = duckdb.connect(database=":memory:")
    try:
        _load_raw_tables(con)
        _execute_intermediate_sql(con)
        _materialize_views(con)
    finally:
        con.close()


if __name__ == "__main__":
    run_data_preparation()
