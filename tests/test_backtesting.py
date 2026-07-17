"""Walk-forward windows must keep training strictly before evaluation."""

from __future__ import annotations

import pandas as pd
from src.backtesting import temporal_windows
from src.settings import load_settings


def test_temporal_windows_are_leakage_safe() -> None:
    windows = temporal_windows(load_settings().backtesting)

    assert len(windows) == 4
    assert (windows["train_end"] < windows["fold_start"]).all()
    assert (
        (windows["train_end"] - windows["train_start"]).dt.days + 1
        == load_settings().backtesting.lookback_days
    ).all()
    assert pd.Timestamp("2025-01-01") == windows.iloc[0]["fold_start"]
