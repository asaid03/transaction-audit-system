from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

from transaction_audit.config import ValidationConfig
from transaction_audit.ingestion import load_transactions
from transaction_audit.parsing import prepare_transactions
from transaction_audit.rules import (
    check_amounts,
    check_currencies,
    check_dates,
    check_duplicate_transaction_ids,
    check_missing_values,
    check_possible_duplicate_payments,
    check_required_columns,
    sort_issues,
)
from transaction_audit.schema import apply_schema
from transaction_audit.types import ValidationResult

if TYPE_CHECKING:
    import pandas as pd


def validate_transactions(
    transactions: pd.DataFrame,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    config = config or ValidationConfig()
    missing_column_issues = check_required_columns(transactions)

    if missing_column_issues:
        return ValidationResult(transactions=transactions, issues=missing_column_issues)

    prepared = prepare_transactions(transactions, config)
    issues = []
    issues.extend(check_missing_values(prepared.original, prepared.checked))
    issues.extend(check_amounts(prepared.original, prepared.checked, config))
    issues.extend(check_dates(prepared.original, prepared.checked))
    issues.extend(check_currencies(prepared.original, prepared.checked, config))
    issues.extend(check_duplicate_transaction_ids(prepared.checked))
    issues.extend(check_possible_duplicate_payments(prepared.checked, config))

    return ValidationResult(transactions=prepared.checked, issues=sort_issues(issues))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a transaction file.")
    parser.add_argument("path", type=Path)
    args = parser.parse_args()

    transactions = load_transactions(args.path)
    schema_result = apply_schema(transactions)
    result = validate_transactions(schema_result.transactions)
    print(f"Rows checked: {len(result.transactions)}")
    print(f"Issues found: {len(result.issues)}")
    if result.issues:
        print(result.issues_dataframe().to_string(index=False))


if __name__ == "__main__":
    main()
