"""Shared filesystem paths plus the simulation's seed and date window."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_LAKE = PROJECT_ROOT / "data" / "lake"
LAKE_RAW = DATA_LAKE / "raw"
LAKE_PROCESSED = DATA_LAKE / "processed"
LAKE_ANALYTICS = DATA_LAKE / "analytics"
SQL_DIR = PROJECT_ROOT / "sql"

RANDOM_SEED = 42
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"

ABC_DOS_CAPS: dict[str, float] = {"A": 20.0, "B": 30.0, "C": 45.0}
