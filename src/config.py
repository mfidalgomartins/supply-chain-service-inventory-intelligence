from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
OUTPUT_TABLES = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_CHARTS = PROJECT_ROOT / "outputs" / "charts"
SQL_DIR = PROJECT_ROOT / "sql"
DASHBOARD_DIR = PROJECT_ROOT / "outputs" / "dashboard"
DOCS_DIR = PROJECT_ROOT / "docs"

RANDOM_SEED = 42
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"

for path in [DATA_RAW, DATA_PROCESSED, OUTPUT_TABLES, OUTPUT_CHARTS, DASHBOARD_DIR, DOCS_DIR]:
    path.mkdir(parents=True, exist_ok=True)
