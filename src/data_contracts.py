from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

try:
    from src.config import PROJECT_ROOT
except ModuleNotFoundError:
    from config import PROJECT_ROOT  # type: ignore[no-redef]


CONTRACT_FILE = PROJECT_ROOT / "configs" / "table_contracts.json"
OUTPUT_TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"


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

    required_cols = set(contract.get("required_columns", []))
    contract_columns = (
        required_cols
        | set(contract.get("unique_key", []))
        | set(contract.get("critical_columns", []))
        | set(contract.get("non_negative", []))
        | set(contract.get("value_ranges", {}))
        | set(contract.get("allowed_values", {}))
    )
    missing = sorted(contract_columns - set(df.columns))
    checks.append(
        ContractCheck(
            table_name=name,
            check_name="required_columns_present",
            severity="CRITICAL",
            status="PASS" if len(missing) == 0 else "FAIL",
            observed=str(len(missing)),
            expected="0",
            details=(
                "All contract-referenced columns must exist."
                if not missing
                else f"Missing columns: {', '.join(missing)}"
            ),
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

    value_ranges = contract.get("value_ranges", {})
    for column, bounds in value_ranges.items():
        minimum = bounds.get("min")
        maximum = bounds.get("max")
        out_of_range = pd.Series(False, index=df.index)
        if minimum is not None:
            out_of_range |= df[column] < minimum
        if maximum is not None:
            out_of_range |= df[column] > maximum
        invalid_count = int(out_of_range.sum())
        checks.append(
            ContractCheck(
                table_name=name,
                check_name=f"{column}_value_range",
                severity="HIGH",
                status="PASS" if invalid_count == 0 else "FAIL",
                observed=str(invalid_count),
                expected=f"[{minimum}, {maximum}]",
                details=f"{column} values must stay inside the configured domain.",
            )
        )

    allowed_values = contract.get("allowed_values", {})
    for column, allowed in allowed_values.items():
        invalid_values = sorted(
            df.loc[~df[column].isin(allowed), column].dropna().astype(str).unique()
        )
        checks.append(
            ContractCheck(
                table_name=name,
                check_name=f"{column}_allowed_values",
                severity="HIGH",
                status="PASS" if not invalid_values else "FAIL",
                observed=", ".join(invalid_values) if invalid_values else "0",
                expected=", ".join(map(str, allowed)),
                details=f"{column} must use the configured categorical domain.",
            )
        )

    return checks


def evaluate_reference_contract(
    tables: dict[str, pd.DataFrame],
    relationship: dict,
) -> ContractCheck:
    source_table = relationship["source_table"]
    source_column = relationship["source_column"]
    target_table = relationship["target_table"]
    target_column = relationship["target_column"]

    source = tables[source_table][source_column].dropna()
    target = set(tables[target_table][target_column].dropna())
    missing_values = sorted(set(source) - target)

    return ContractCheck(
        table_name=source_table,
        check_name=f"{source_column}_references_{target_table}.{target_column}",
        severity="HIGH",
        status="PASS" if not missing_values else "FAIL",
        observed=str(len(missing_values)),
        expected="0",
        details=(
            f"All {source_table}.{source_column} values must exist in "
            f"{target_table}.{target_column}."
        ),
    )


def run_data_contracts() -> None:
    OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    contract_spec = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))
    contracts = contract_spec["tables"]

    check_rows: list[ContractCheck] = []
    profile_rows: list[dict] = []
    tables: dict[str, pd.DataFrame] = {}

    for contract in contracts:
        table_name = contract["name"]
        path = PROJECT_ROOT / contract["path"]
        display_path = path.relative_to(PROJECT_ROOT).as_posix()

        if not path.exists():
            check_rows.append(
                ContractCheck(
                    table_name=table_name,
                    check_name="table_file_exists",
                    severity="CRITICAL",
                    status="FAIL",
                    observed="0",
                    expected="1",
                    details=f"Missing file: {display_path}",
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
                details=f"File found: {display_path}",
            )
        )

        df = pd.read_csv(path)
        tables[table_name] = df
        profile_rows.append(
            {
                "table_name": table_name,
                "path": display_path,
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "file_size_bytes": int(path.stat().st_size),
                "sha256": _hash_file(path),
            }
        )

        check_rows.extend(evaluate_dataframe_contract(df, contract))

    for relationship in contract_spec.get("relationships", []):
        if relationship["source_table"] in tables and relationship["target_table"] in tables:
            check_rows.append(evaluate_reference_contract(tables, relationship))

    checks_df = pd.DataFrame([asdict(r) for r in check_rows])
    profile_df = pd.DataFrame(profile_rows)

    checks_df.to_csv(OUTPUT_TABLES_DIR / "data_contract_check_results.csv", index=False)
    profile_df.to_csv(OUTPUT_TABLES_DIR / "data_contract_table_profile.csv", index=False)

    fail_count = int((checks_df["status"] == "FAIL").sum())
    warn_count = int((checks_df["status"] == "WARN").sum())

    print("Data contracts validation complete.")
    print(f"Tables covered: {len(contracts)}")
    print(f"Checks: {len(checks_df)} | FAIL: {fail_count} | WARN: {warn_count}")

    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    run_data_contracts()
