from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from transaction_audit.config import REQUIRED_COLUMNS, ValidationConfig
from transaction_audit.ingestion import load_transactions
from transaction_audit.schema import apply_schema


@dataclass(frozen=True)
class ValidationIssue:
    row_number: int | None
    transaction_id: str | None
    field: str
    issue_type: str
    severity: str
    message: str


@dataclass(frozen=True)
class ValidationResult:
    transactions: pd.DataFrame
    issues: list[ValidationIssue]

    @property
    def is_valid(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    def issues_dataframe(self) -> pd.DataFrame:
        if not self.issues:
            return pd.DataFrame(
                columns=["row_number", "transaction_id", "field", "issue_type", "severity", "message"]
            )
        return pd.DataFrame([asdict(issue) for issue in self.issues])


def validate_transactions(
    transactions: pd.DataFrame,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    config = config or ValidationConfig()
    issues: list[ValidationIssue] = []

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in transactions.columns]
    for column in missing_columns:
        issues.append(
            ValidationIssue(
                row_number=None,
                transaction_id=None,
                field=column,
                issue_type="missing_column",
                severity="error",
                message=f"Required column '{column}' is missing.",
            )
        )

    if missing_columns:
        return ValidationResult(transactions=transactions, issues=issues)

    original = transactions.copy()
    checked = transactions.copy()
    checked["transaction_id"] = checked["transaction_id"].astype("string").str.strip()
    checked["currency"] = checked["currency"].astype("string").str.upper().str.strip()
    checked["counterparty"] = checked["counterparty"].astype("string").str.strip()
    checked["account_id"] = checked["account_id"].astype("string").str.strip()
    checked["amount"] = pd.to_numeric(checked["amount"], errors="coerce")
    checked["transaction_date"] = pd.to_datetime(
        checked["transaction_date"],
        errors="coerce",
        dayfirst=config.date_dayfirst,
    )

    for position, (index, row) in enumerate(checked.iterrows(), start=2):
        row_number = position
        transaction_id = _clean_optional(row["transaction_id"])

        original_row = original.loc[index]

        missing_fields: set[str] = set()

        for column in REQUIRED_COLUMNS:
            if pd.isna(original_row[column]) or str(original_row[column]).strip() == "":
                missing_fields.add(column)
                issues.append(
                    ValidationIssue(
                        row_number=row_number,
                        transaction_id=transaction_id,
                        field=column,
                        issue_type="missing_value",
                        severity="error",
                        message=f"'{column}' is required.",
                    )
                )

        if "amount" not in missing_fields and pd.isna(row["amount"]):
            issues.append(
                ValidationIssue(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    field="amount",
                    issue_type="invalid_amount",
                    severity="error",
                    message="Amount must be numeric.",
                )
            )
        elif row["amount"] == 0:
            issues.append(
                ValidationIssue(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    field="amount",
                    issue_type="zero_amount",
                    severity="warning",
                    message="Amount is zero.",
                )
            )
        elif abs(float(row["amount"])) >= config.large_amount_threshold:
            issues.append(
                ValidationIssue(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    field="amount",
                    issue_type="large_amount",
                    severity="warning",
                    message=f"Amount is at or above {config.large_amount_threshold:,.2f}.",
                )
            )

        if "transaction_date" not in missing_fields and pd.isna(row["transaction_date"]):
            issues.append(
                ValidationIssue(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    field="transaction_date",
                    issue_type="invalid_date",
                    severity="error",
                    message="Transaction date could not be parsed.",
                )
            )

        if "currency" not in missing_fields and row["currency"] not in config.allowed_currencies:
            issues.append(
                ValidationIssue(
                    row_number=row_number,
                    transaction_id=transaction_id,
                    field="currency",
                    issue_type="unsupported_currency",
                    severity="warning",
                    message=f"Currency '{row['currency']}' is outside the allowed list.",
                )
            )

    duplicate_mask = (
        checked["transaction_id"].notna()
        & checked["transaction_id"].ne("")
        & checked["transaction_id"].duplicated(keep=False)
    )
    for position, (_, row) in enumerate(checked.iterrows(), start=2):
        if not bool(duplicate_mask.iloc[position - 2]):
            continue

        issues.append(
            ValidationIssue(
                row_number=position,
                transaction_id=_clean_optional(row["transaction_id"]),
                field="transaction_id",
                issue_type="duplicate_transaction_id",
                severity="error",
                message="Transaction ID appears more than once in this file.",
            )
        )

    return ValidationResult(transactions=checked, issues=issues)


def _clean_optional(value: object) -> str | None:
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None


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
