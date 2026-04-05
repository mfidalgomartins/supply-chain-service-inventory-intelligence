from __future__ import annotations

"""Legacy compatibility wrapper for analysis outputs.

Use the authoritative modules directly:
- `src/kpi_diagnostic_analysis.py`
- `src/impact_analysis.py`
- `src/visualization.py`
"""

try:
    from src.impact_analysis import run_impact_analysis
    from src.kpi_diagnostic_analysis import run_analysis
    from src.visualization import run_visualization_suite
except ModuleNotFoundError:
    from impact_analysis import run_impact_analysis
    from kpi_diagnostic_analysis import run_analysis
    from visualization import run_visualization_suite


def main() -> None:
    print("[DEPRECATED] src/analyze.py is a compatibility wrapper.")
    print("[DEPRECATED] Running authoritative analysis modules.")
    run_analysis()
    run_impact_analysis()
    run_visualization_suite()


if __name__ == "__main__":
    main()
