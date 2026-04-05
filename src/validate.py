from __future__ import annotations

"""Legacy compatibility wrapper for validation.

Use `src/pre_delivery_validation.py` as the authoritative implementation.
"""

try:
    from src.pre_delivery_validation import run_pre_delivery_validation
except ModuleNotFoundError:
    from pre_delivery_validation import run_pre_delivery_validation


def main() -> None:
    print("[DEPRECATED] src/validate.py is a compatibility wrapper.")
    print("[DEPRECATED] Running authoritative module: src/pre_delivery_validation.py")
    run_pre_delivery_validation()


if __name__ == "__main__":
    main()
