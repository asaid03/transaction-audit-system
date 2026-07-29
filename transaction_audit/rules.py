from __future__ import annotations

from typing import Any, Hashable

import pandas as pd

from transaction_audit.config import REQUIRED_COLUMNS, ValidationConfig
from transaction_audit.provenance import source_row_number_at_position, source_row_numbers
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
                message=f"Payment is above the review limit of {config.large_amount_threshold:,.2f}.",
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
                message=f"Currency '{row['currency']}' is not in the expected currency list.",
            )
        )

    return issues


def check_missing_invoice_references(original: pd.DataFrame, checked: pd.DataFrame) -> list[ValidationIssue]:
    if "invoice_reference" not in original:
        return []

    return [
        make_issue(
            row_number=position,
            row=row,
            field="invoice_reference",
            issue_type="missing_invoice_reference",
            severity="warning",
            message="Invoice/reference field is blank.",
        )
        for position, row in rows_matching(checked, _missing_mask(original["invoice_reference"]))
    ]


def check_duplicate_transaction_ids(checked: pd.DataFrame) -> list[ValidationIssue]:
    issues = []
    duplicate_mask = (
        checked["transaction_id"].notna()
        & checked["transaction_id"].ne("")
        & checked["transaction_id"].duplicated(keep=False)
    )

    duplicate_rows: dict[str, list[int]] = {}
    for position, row in rows_matching(checked, duplicate_mask):
        transaction_id = _clean_optional(row["transaction_id"])
        if transaction_id is not None:
            duplicate_rows.setdefault(transaction_id, []).append(position)

    for position, row in rows_matching(checked, duplicate_mask):
        transaction_id = _clean_optional(row["transaction_id"])
        matching_rows = [
            row_number
            for row_number in duplicate_rows.get(transaction_id or "", [])
            if row_number != position
        ]
        row_text = ", ".join(str(row_number) for row_number in matching_rows)
        issues.append(
            make_issue(
                row_number=position,
                row=row,
                field="transaction_id",
                issue_type="duplicate_transaction_id",
                severity="error",
                message=f"This transaction ID is also used on source row(s) {row_text}.",
                related_record_id=transaction_id,
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

    matches: dict[Hashable, set[Hashable]] = {}
    match_reasons: dict[Hashable, set[str]] = {}

    def add_match(left: Hashable, right: Hashable, reason: str) -> None:
        matches.setdefault(left, set()).add(right)
        matches.setdefault(right, set()).add(left)
        match_reasons.setdefault(left, set()).add(reason)
        match_reasons.setdefault(right, set()).add(reason)

    for _, group in usable.groupby(group_columns, dropna=False):
        if group["transaction_id"].nunique() < 2:
            continue

        if "invoice_reference" in group:
            invoice_group_candidates = group.assign(
                _invoice_reference=group["invoice_reference"].astype("string").str.strip()
            )
            invoice_group_candidates = invoice_group_candidates[
                invoice_group_candidates["_invoice_reference"].notna()
                & invoice_group_candidates["_invoice_reference"].ne("")
            ]
            for _, invoice_group in invoice_group_candidates.groupby("_invoice_reference", dropna=False):
                if invoice_group["transaction_id"].nunique() >= 2 and len(invoice_group) > 1:
                    invoice_indexes = list(invoice_group.index)
                    invoice_reference = str(invoice_group["_invoice_reference"].iloc[0])
                    for offset, left_index in enumerate(invoice_indexes):
                        for right_index in invoice_indexes[offset + 1 :]:
                            add_match(
                                left_index,
                                right_index,
                                f"the same invoice/reference ({invoice_reference})",
                            )

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
                add_match(
                    ordered_indexes[offset - 1],
                    ordered_indexes[offset],
                    (
                        "the same account, payee, amount and currency within "
                        f"{config.duplicate_payment_window_days} days"
                    ),
                )

    if not matches:
        return []

    issues = []
    for index in checked.index:
        if index not in matches:
            continue

        row = checked.loc[index].to_dict()
        peer_ids = [
            _clean_optional(checked.loc[peer_index, "transaction_id"])
            for peer_index in matches[index]
        ]
        peer_ids = sorted({peer_id for peer_id in peer_ids if peer_id is not None})
        reasons = sorted(match_reasons.get(index, set()))
        related_ids = ", ".join(peer_ids)
        reason_text = " and ".join(reasons)
        position_index = checked.index.get_loc(index)
        position = source_row_number_at_position(checked, int(position_index))
        issues.append(
            make_issue(
                row_number=int(position),
                row=row,
                field="transaction_id",
                issue_type="possible_duplicate_payment",
                severity="warning",
                message=f"Resembles transaction(s) {related_ids} because it has {reason_text}.",
                related_record_id=related_ids,
            )
        )

    return issues


def check_possible_split_payments(
    checked: pd.DataFrame,
    config: ValidationConfig,
) -> list[ValidationIssue]:
    required_columns = {
        "transaction_id",
        "amount",
        "currency",
        "counterparty",
        "invoice_reference",
        "invoice_amount",
    }
    if not required_columns.issubset(checked.columns):
        return []

    tolerance = float(config.invoice_amount_tolerance)
    if tolerance < 0:
        return []

    working = checked.copy()
    for source, target in (
        ("counterparty", "_counterparty_key"),
        ("currency", "_currency_key"),
        ("invoice_reference", "_invoice_key"),
    ):
        working[target] = (
            working[source]
            .astype("string")
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
        )

    usable = working[
        working["transaction_id"].notna()
        & working["transaction_id"].astype("string").str.strip().ne("")
        & working["_counterparty_key"].notna()
        & working["_counterparty_key"].ne("")
        & working["_currency_key"].notna()
        & working["_currency_key"].ne("")
        & working["_invoice_key"].notna()
        & working["_invoice_key"].ne("")
    ]
    if len(usable) < 2:
        return []

    issues = []
    group_columns = ["_counterparty_key", "_currency_key", "_invoice_key"]
    for _, group in usable.groupby(group_columns, dropna=False, sort=False):
        transaction_ids = sorted(
            {
                transaction_id
                for transaction_id in group["transaction_id"].map(_clean_optional)
                if transaction_id is not None
            }
        )
        if len(transaction_ids) < 2:
            continue

        payment_amounts = pd.to_numeric(group["amount"], errors="coerce")
        invoice_amounts = pd.to_numeric(group["invoice_amount"], errors="coerce")
        if (
            payment_amounts.isna().any()
            or invoice_amounts.isna().any()
            or payment_amounts.le(0).any()
            or invoice_amounts.le(0).any()
        ):
            continue

        invoice_total = float(invoice_amounts.iloc[0])
        if invoice_amounts.sub(invoice_total).abs().gt(tolerance).any():
            continue
        if payment_amounts.ge(invoice_total - tolerance).any():
            continue

        combined_amount = float(payment_amounts.sum())
        if abs(combined_amount - invoice_total) > tolerance:
            continue

        currency = str(group["currency"].iloc[0]).strip()
        invoice_reference = str(group["invoice_reference"].iloc[0]).strip()
        for index, row in group.to_dict("index").items():
            transaction_id = _clean_optional(row["transaction_id"])
            related_ids = ", ".join(
                peer_id for peer_id in transaction_ids if peer_id != transaction_id
            )
            position = source_row_number_at_position(
                checked,
                int(checked.index.get_loc(index)),
            )
            issues.append(
                make_issue(
                    row_number=position,
                    row=row,
                    field="amount",
                    issue_type="possible_split_payment",
                    severity="warning",
                    message=(
                        f"This payment and transaction(s) {related_ids} share invoice/reference "
                        f"{invoice_reference}, payee and currency. Their combined payments of "
                        f"{currency} {combined_amount:,.2f} match the invoice total of "
                        f"{currency} {invoice_total:,.2f}, while no individual payment settles "
                        "the full invoice."
                    ),
                    related_record_id=related_ids,
                )
            )

    return issues


def rows_matching(dataframe: pd.DataFrame, mask: pd.Series):
    matched_mask = mask.fillna(False).astype(bool).to_numpy()
    positions = source_row_numbers(dataframe).loc[matched_mask].tolist()
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
    dataset: str = "transactions",
    related_dataset: str | None = None,
    related_record_id: str | None = None,
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
        dataset=dataset,
        related_dataset=related_dataset,
        related_record_id=related_record_id,
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
