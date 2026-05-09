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

    return ValidationResult(transactions=prepared.checked, issues=sort_issues(issues))


@dataclass(frozen=True)
class PreparedTransactions:
    original: pd.DataFrame
    checked: pd.DataFrame


def check_required_columns(transactions: pd.DataFrame) -> list[ValidationIssue]:
    issues = []
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

    return issues


def prepare_transactions(transactions: pd.DataFrame, config: ValidationConfig) -> PreparedTransactions:
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
        format="mixed",
    )
    return PreparedTransactions(original=original, checked=checked)


def check_missing_values(original: pd.DataFrame, checked: pd.DataFrame) -> list[ValidationIssue]:
    issues = []
    for column in REQUIRED_COLUMNS:
        missing_mask = original[column].isna() | original[column].astype(str).str.strip().eq("")
        for position, row in rows_matching(checked, missing_mask):
            issues.append(
                ValidationIssue(
                    row_number=position,
                    transaction_id=_clean_optional(row["transaction_id"]),
                    field=column,
                    issue_type="missing_value",
                    severity="error",
                    message=f"'{column}' is required.",
                )
            )
    return issues


def check_amounts(
    original: pd.DataFrame,
    checked: pd.DataFrame,
    config: ValidationConfig,
) -> list[ValidationIssue]:
    issues = []
    amount_missing = _missing_mask(original["amount"])
    invalid_amount = checked["amount"].isna() & ~amount_missing
    zero_amount = checked["amount"].eq(0)
    large_amount = checked["amount"].abs().ge(config.large_amount_threshold)

    for position, row in rows_matching(checked, invalid_amount):
        issues.append(
            ValidationIssue(
                row_number=position,
                transaction_id=_clean_optional(row["transaction_id"]),
                field="amount",
                issue_type="invalid_amount",
                severity="error",
                message="Amount must be numeric.",
            )
        )

    for position, row in rows_matching(checked, zero_amount):
        issues.append(
            ValidationIssue(
                row_number=position,
                transaction_id=_clean_optional(row["transaction_id"]),
                field="amount",
                issue_type="zero_amount",
                severity="warning",
                message="Amount is zero.",
            )
        )

    for position, row in rows_matching(checked, large_amount):
        issues.append(
            ValidationIssue(
                row_number=position,
                transaction_id=_clean_optional(row["transaction_id"]),
                field="amount",
                issue_type="large_amount",
                severity="warning",
                message=f"Amount is at or above {config.large_amount_threshold:,.2f}.",
            )
        )

    return issues


def check_dates(original: pd.DataFrame, checked: pd.DataFrame) -> list[ValidationIssue]:
    issues = []
    date_missing = _missing_mask(original["transaction_date"])
    invalid_date = checked["transaction_date"].isna() & ~date_missing

    for position, row in rows_matching(checked, invalid_date):
        issues.append(
            ValidationIssue(
                row_number=position,
                transaction_id=_clean_optional(row["transaction_id"]),
                field="transaction_date",
                issue_type="invalid_date",
                severity="error",
                message="Transaction date could not be parsed.",
            )
        )

    return issues


def check_currencies(
    original: pd.DataFrame,
    checked: pd.DataFrame,
    config: ValidationConfig,
) -> list[ValidationIssue]:
    issues = []
    currency_missing = _missing_mask(original["currency"])
    unsupported_currency = ~checked["currency"].isin(config.allowed_currencies) & ~currency_missing

    for position, row in rows_matching(checked, unsupported_currency):
        issues.append(
            ValidationIssue(
                row_number=position,
                transaction_id=_clean_optional(row["transaction_id"]),
                field="currency",
                issue_type="unsupported_currency",
                severity="warning",
                message=f"Currency '{row['currency']}' is outside the allowed list.",
            )
        )

    return issues


def check_duplicate_transaction_ids(checked: pd.DataFrame) -> list[ValidationIssue]:
    issues = []
    duplicate_mask = (
        checked["transaction_id"].notna()
        & checked["transaction_id"].ne("")
        & checked["transaction_id"].duplicated(keep=False)
    )

    for position, row in rows_matching(checked, duplicate_mask):
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

    return issues


def rows_matching(dataframe: pd.DataFrame, mask: pd.Series):
    matched_mask = mask.fillna(False).astype(bool).to_numpy()
    positions = [
        position
        for offset, is_match in enumerate(matched_mask)
        if is_match
        for position in [offset + 2]
    ]
    records = dataframe.loc[matched_mask].to_dict("records")

    for position, row in zip(positions, records):
        yield position, row


def sort_issues(issues: list[ValidationIssue]) -> list[ValidationIssue]:
    return sorted(
        issues,
        key=lambda issue: (
            -1 if issue.row_number is None else issue.row_number,
            issue.field,
            issue.issue_type,
        ),
    )


def _missing_mask(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().eq("")


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
