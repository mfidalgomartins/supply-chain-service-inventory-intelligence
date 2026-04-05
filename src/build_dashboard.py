from __future__ import annotations

"""Legacy compatibility wrapper for dashboard build.

Use `src/executive_dashboard.py` as the authoritative implementation.
"""

try:
    from src.executive_dashboard import main as build_executive_dashboard
except ModuleNotFoundError:
    from executive_dashboard import main as build_executive_dashboard


def main() -> None:
    print("[DEPRECATED] src/build_dashboard.py is a compatibility wrapper.")
    print("[DEPRECATED] Running authoritative module: src/executive_dashboard.py")
    build_executive_dashboard()


if __name__ == "__main__":
    main()
