from __future__ import annotations

"""Legacy compatibility wrapper for SQL execution checks.

Use the authoritative modules directly:
- `src/data_preparation.py`
- `src/sql_quality_gate.py`
"""

try:
    from src.data_preparation import run_data_preparation
    from src.sql_quality_gate import run_sql_quality_gate
except ModuleNotFoundError:
    from data_preparation import run_data_preparation
    from sql_quality_gate import run_sql_quality_gate


def main() -> None:
    print("[DEPRECATED] src/run_sql_analysis.py is a compatibility wrapper.")
    print("[DEPRECATED] Running authoritative SQL modules.")
    run_data_preparation()
    run_sql_quality_gate()


if __name__ == "__main__":
    main()
