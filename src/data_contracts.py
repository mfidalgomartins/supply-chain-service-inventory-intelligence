from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from src.config import PROJECT_ROOT
except ModuleNotFoundError:
    from config import PROJECT_ROOT


CONTRACT_FILE = PROJECT_ROOT / "contracts" / "table_contracts.json"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


@dataclass
class ContractCheck:
    table_name: str
    check_name: str
    severity: str
    status: str
    observed: str
    expected: str
    details: str


def _hash_file(path: Path) -> str:
    payload = path.read_bytes()
    return hashlib.sha256(payload).hexdigest()


def evaluate_dataframe_contract(df: pd.DataFrame, contract: dict) -> list[ContractCheck]:
    checks: list[ContractCheck] = []
    name = contract["name"]

    required_cols = contract.get("required_columns", [])
    missing = sorted([c for c in required_cols if c not in df.columns])
    checks.append(
        ContractCheck(
            table_name=name,
            check_name="required_columns_present",
            severity="CRITICAL",
            status="PASS" if len(missing) == 0 else "FAIL",
            observed=str(len(missing)),
            expected="0",
            details="All required columns must exist." if not missing else f"Missing columns: {', '.join(missing)}",
        )
    )

    if missing:
        return checks

    unique_key = contract.get("unique_key", [])
    if unique_key:
        dup_count = int(df.duplicated(unique_key).sum())
        checks.append(
            ContractCheck(
                table_name=name,
                check_name="unique_key_duplicates",
                severity="HIGH",
                status="PASS" if dup_count == 0 else "FAIL",
                observed=str(dup_count),
                expected="0",
                details=f"Grain uniqueness on {', '.join(unique_key)}.",
            )
        )

    critical_cols = contract.get("critical_columns", [])
    if critical_cols:
        null_count = int(df[critical_cols].isna().sum().sum())
        checks.append(
            ContractCheck(
                table_name=name,
                check_name="critical_columns_nulls",
                severity="HIGH",
                status="PASS" if null_count == 0 else "FAIL",
                observed=str(null_count),
                expected="0",
                details=f"No nulls allowed in critical fields: {', '.join(critical_cols)}.",
            )
        )

    non_negative = contract.get("non_negative", [])
    if non_negative:
        negative_count = int((df[non_negative] < 0).sum().sum())
        checks.append(
            ContractCheck(
                table_name=name,
                check_name="non_negative_fields",
                severity="HIGH",
                status="PASS" if negative_count == 0 else "FAIL",
                observed=str(negative_count),
                expected="0",
                details=f"No negatives allowed in fields: {', '.join(non_negative)}.",
            )
        )

    return checks


def run_data_contracts() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    contracts = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))["tables"]

    check_rows: list[ContractCheck] = []
    profile_rows: list[dict] = []

    for contract in contracts:
        table_name = contract["name"]
        path = PROJECT_ROOT / contract["path"]

        if not path.exists():
            check_rows.append(
                ContractCheck(
                    table_name=table_name,
                    check_name="table_file_exists",
                    severity="CRITICAL",
                    status="FAIL",
                    observed="0",
                    expected="1",
                    details=f"Missing file: {path}",
                )
            )
            continue

        check_rows.append(
            ContractCheck(
                table_name=table_name,
                check_name="table_file_exists",
                severity="CRITICAL",
                status="PASS",
                observed="1",
                expected="1",
                details=f"File found: {path}",
            )
        )

        df = pd.read_csv(path)
        profile_rows.append(
            {
                "table_name": table_name,
                "path": str(path),
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "file_size_bytes": int(path.stat().st_size),
                "sha256": _hash_file(path),
            }
        )

        check_rows.extend(evaluate_dataframe_contract(df, contract))

    checks_df = pd.DataFrame([asdict(r) for r in check_rows])
    profile_df = pd.DataFrame(profile_rows)

    checks_df.to_csv(OUTPUT_TABLES_DIR / "data_contract_check_results.csv", index=False)
    profile_df.to_csv(OUTPUT_TABLES_DIR / "data_contract_table_profile.csv", index=False)

    fail_count = int((checks_df["status"] == "FAIL").sum())
    warn_count = int((checks_df["status"] == "WARN").sum())

    lines = [
        "# Data Contracts Summary",
        "",
        f"- Generated at: **{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}**",
        f"- Contract file: `{CONTRACT_FILE}`",
        f"- Tables covered: **{len(contracts)}**",
        f"- Checks: **{len(checks_df)}**",
        f"- Fails: **{fail_count}**",
        f"- Warnings: **{warn_count}**",
        "",
        "## Check Results",
        "| Table | Check | Severity | Status | Observed | Expected |",
        "|---|---|---|---|---:|---:|",
    ]

    for r in checks_df.itertuples(index=False):
        lines.append(
            f"| {r.table_name} | {r.check_name} | {r.severity} | {r.status} | {r.observed} | {r.expected} |"
        )

    (OUTPUT_REPORTS_DIR / "data_contracts_summary.md").write_text("\n".join(lines), encoding="utf-8")

    print("Data contracts validation complete.")
    print(f"Tables covered: {len(contracts)}")
    print(f"Checks: {len(checks_df)} | FAIL: {fail_count} | WARN: {warn_count}")

    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    run_data_contracts()
