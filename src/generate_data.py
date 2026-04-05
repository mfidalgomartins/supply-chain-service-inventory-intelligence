from __future__ import annotations

"""Legacy compatibility wrapper for synthetic data generation.

Use `src/data_generation.py` as the authoritative implementation.
"""

try:
    from src.data_generation import generate_all_tables
except ModuleNotFoundError:
    from data_generation import generate_all_tables


def main() -> None:
    print("[DEPRECATED] src/generate_data.py is a compatibility wrapper.")
    print("[DEPRECATED] Running authoritative module: src/data_generation.py")
    generate_all_tables()


if __name__ == "__main__":
    main()
