# Source Adapter Readiness

This module provides a production-style bridge from synthetic CSVs to real external extracts with schema-readiness checks.

## Readiness Snapshot
- Tables assessed: **7**
- Ready tables: **7**
- Ready external tables currently in use: **0**

## Usage
- Drop real source extracts in `data/external/<table>.csv` with matching headers.
- Run pipeline as-is; adapter automatically promotes external files when schema checks pass.
- Template headers are available in `data/external/templates/`.