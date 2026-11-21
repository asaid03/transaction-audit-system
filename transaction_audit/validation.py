from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

from transaction_audit.config import ReviewThreshold, ValidationConfig
from transaction_audit.ingestion import load_transactions
from transaction_audit.parsing import prepare_transactions
from transaction_audit.population import (
    check_running_balance_integrity,
    check_vendor_concentration,
)
from transaction_audit.rules import (
    check_amounts,
    check_currencies,
    check_dates,
    check_duplicate_transaction_ids,
    check_missing_values,
    check_missing_invoice_references,
    check_possible_duplicate_payments,
    check_possible_split_payments,
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
        return ValidationResult(transactions=transactions, issues=missing_column_issues, config=config)

    prepared = prepare_transactions(transactions, config)
    issues = []
    issues.extend(check_missing_values(prepared.original, prepared.checked))
    issues.extend(check_amounts(prepared.original, prepared.checked, config))
    issues.extend(check_dates(prepared.original, prepared.checked))
    issues.extend(check_currencies(prepared.original, prepared.checked, config))
    issues.extend(check_missing_invoice_references(prepared.original, prepared.checked))
    issues.extend(check_duplicate_transaction_ids(prepared.checked))
    issues.extend(check_possible_duplicate_payments(prepared.checked, config))
    issues.extend(check_possible_split_payments(prepared.checked, config))
    issues.extend(check_running_balance_integrity(prepared.checked, config))
    issues.extend(check_vendor_concentration(prepared.checked, config))

    return ValidationResult(transactions=prepared.checked, issues=sort_issues(issues), config=config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a transaction file.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--review-threshold", action="append", metavar="CCY=AMOUNT", help="Select individual transactions at or above this absolute amount in one currency.")
    args = parser.parse_args()

    transactions = load_transactions(args.path)
    schema_result = apply_schema(transactions)
    try:
        threshold = None
        if args.review_threshold:
            if len(args.review_threshold) != 1 or args.review_threshold[0].count("=") != 1:
                raise ValueError("Supply one --review-threshold in the form CCY=AMOUNT.")
            currency, amount = args.review_threshold[0].split("=")
            threshold = ReviewThreshold(currency.upper(), float(amount))
        config = ValidationConfig(review_threshold=threshold)
    except ValueError as error:
        parser.error(str(error))
    result = validate_transactions(schema_result.transactions, config)
    print("Threshold basis: reviewer-selected absolute magnitude; approval not assessed.")
    if "currency" in result.transactions:
        for currency, amount in config.threshold_coverage(result.transactions["currency"]).items():
            print(f"{currency}: {amount or 'No absolute threshold supplied'}")
    print(f"Rows checked: {len(result.transactions)}")
    print(f"Issues found: {len(result.issues)}")
    if result.issues:
        print(result.issues_dataframe().to_string(index=False))


if __name__ == "__main__":
    main()
