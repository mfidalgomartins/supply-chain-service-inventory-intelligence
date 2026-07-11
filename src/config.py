"""Shared filesystem paths plus the simulation's seed and date window."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
SQL_DIR = PROJECT_ROOT / "sql"

RANDOM_SEED = 42
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"
