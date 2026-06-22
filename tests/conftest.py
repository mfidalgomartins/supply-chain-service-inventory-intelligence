"""Shared test configuration.

Force a headless, deterministic matplotlib backend and a repo-local cache
directory before any module imports ``matplotlib.pyplot``. This mirrors the
environment the production pipeline sets up in ``run_pipeline.py`` so chart
rendering behaves identically under tests.
"""

from __future__ import annotations

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("MPLCONFIGDIR", str(_ROOT / ".cache" / "matplotlib"))
