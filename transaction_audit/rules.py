from __future__ import annotations

from typing import Any, Hashable

import pandas as pd

from transaction_audit.config import REQUIRED_COLUMNS, ValidationConfig
from transaction_audit.types import ValidationIssue


def check_required_columns(transactions: pd.DataFrame) -> list[ValidationIssue]:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in transactions.columns]
    return [
        make_issue(
            row_number=None,
            transaction_id=None,
            field=column,
            issue_type="missing_column",
            severity="error",
            message=f"Required column '{column}' is missing.",
        )
        for column in missing_columns
    ]


def check_missing_values(original: pd.DataFrame, checked: pd.DataFrame) -> list[ValidationIssue]:
    issues = []
    for column in REQUIRED_COLUMNS:
        missing_mask = _missing_mask(original[column])
        for position, row in rows_matching(checked, missing_mask):
            issues.append(
                make_issue(
                    row_number=position,
                    row=row,
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
            make_issue(
                row_number=position,
                row=row,
                field="amount",
                issue_type="invalid_amount",
                severity="error",
                message="Amount must be numeric.",
            )
        )

    for position, row in rows_matching(checked, zero_amount):
        issues.append(
            make_issue(
                row_number=position,
                row=row,
                field="amount",
                issue_type="zero_amount",
                severity="warning",
                message="Amount is zero.",
            )
        )

    for position, row in rows_matching(checked, large_amount):
        issues.append(
            make_issue(
                row_number=position,
                row=row,
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
            make_issue(
                row_number=position,
                row=row,
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
            make_issue(
                row_number=position,
                row=row,
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
            make_issue(
                row_number=position,
                row=row,
                field="transaction_id",
                issue_type="duplicate_transaction_id",
                severity="error",
                message="Transaction ID appears more than once in this file.",
            )
        )

    return issues


def check_possible_duplicate_payments(
    checked: pd.DataFrame,
    config: ValidationConfig,
) -> list[ValidationIssue]:
    group_columns = ["account_id", "counterparty", "amount", "currency"]
    required_text_columns = ["account_id", "counterparty", "currency", "transaction_id"]
    usable = checked[
        checked[group_columns].notna().all(axis=1)
        & checked[required_text_columns].ne("").all(axis=1)
        & checked["transaction_date"].notna()
    ].copy()

    if usable.empty:
        return []

    flagged_indexes: set[Hashable] = set()
    for _, group in usable.groupby(group_columns, dropna=False):
        if group["transaction_id"].nunique() < 2:
            continue

        ordered = group.sort_values("transaction_date")
        ordered_indexes = list(ordered.index)
        ordered_dates = ordered["transaction_date"].tolist()
        ordered_ids = ordered["transaction_id"].tolist()

        for offset in range(1, len(ordered)):
            days_between = (ordered_dates[offset] - ordered_dates[offset - 1]).days
            if (
                ordered_ids[offset] != ordered_ids[offset - 1]
                and days_between <= config.duplicate_payment_window_days
            ):
                flagged_indexes.add(ordered_indexes[offset - 1])
                flagged_indexes.add(ordered_indexes[offset])

    if not flagged_indexes:
        return []

    duplicate_payment_mask = checked.index.isin(flagged_indexes)
    return [
        make_issue(
            row_number=position,
            row=row,
            field="transaction_id",
            issue_type="possible_duplicate_payment",
            severity="warning",
            message=(
                "Possible duplicate payment: same account, counterparty, amount, "
                f"and currency within {config.duplicate_payment_window_days} days."
            ),
        )
        for position, row in rows_matching(checked, pd.Series(duplicate_payment_mask, index=checked.index))
    ]


def rows_matching(dataframe: pd.DataFrame, mask: pd.Series):
    matched_mask = mask.fillna(False).astype(bool).to_numpy()
    positions = [
        offset + 2
        for offset, is_match in enumerate(matched_mask)
        if is_match
    ]
    records = dataframe.loc[matched_mask].to_dict("records")

    for position, row in zip(positions, records):
        yield position, row


def make_issue(
    row_number: int | None,
    field: str,
    issue_type: str,
    severity: str,
    message: str,
    row: dict[str, Any] | None = None,
    transaction_id: str | None = None,
) -> ValidationIssue:
    if row is not None:
        transaction_id = _clean_optional(row["transaction_id"])

    return ValidationIssue(
        row_number=row_number,
        transaction_id=transaction_id,
        field=field,
        issue_type=issue_type,
        severity=severity,
        message=message,
    )


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
