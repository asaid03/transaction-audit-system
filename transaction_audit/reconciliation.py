from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from transaction_audit.datasets import AuditBundle, AuditDataset
from transaction_audit.joins import AUDIT_ROW_NUMBER, safe_left_join, unmatched_left_rows
from transaction_audit.types import ISSUE_COLUMNS, ValidationIssue


SKIPPED_CHECK_COLUMNS = ["check_name", "dataset", "reason"]


@dataclass(frozen=True)
class SkippedCheck:
    check_name: str
    dataset: str
    reason: str


@dataclass(frozen=True)
class ReconciliationResult:
    issues: list[ValidationIssue]
    skipped_checks: list[SkippedCheck]

    def issues_dataframe(self) -> pd.DataFrame:
        if not self.issues:
            return pd.DataFrame(columns=ISSUE_COLUMNS)
        return pd.DataFrame([asdict(issue) for issue in self.issues], columns=ISSUE_COLUMNS)

    def skipped_checks_dataframe(self) -> pd.DataFrame:
        if not self.skipped_checks:
            return pd.DataFrame(columns=SKIPPED_CHECK_COLUMNS)
        return pd.DataFrame([asdict(check) for check in self.skipped_checks], columns=SKIPPED_CHECK_COLUMNS)


def reconcile_bundle(bundle: AuditBundle) -> ReconciliationResult:
    issues: list[ValidationIssue] = []
    skipped_checks: list[SkippedCheck] = []

    if bundle.vendors is None:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_vendor_reference",
                dataset="vendors",
                reason="Vendors file was not uploaded.",
            )
        )
    else:
        vendor_result = check_transaction_vendor_references(bundle.transactions, bundle.vendors)
        issues.extend(vendor_result.issues)
        skipped_checks.extend(vendor_result.skipped_checks)

    if bundle.beneficiaries is None:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_beneficiary_reference",
                dataset="beneficiaries",
                reason="Beneficiaries file was not uploaded.",
            )
        )
    else:
        beneficiary_result = check_transaction_beneficiary_references(
            bundle.transactions,
            bundle.beneficiaries,
        )
        issues.extend(beneficiary_result.issues)
        skipped_checks.extend(beneficiary_result.skipped_checks)

    return ReconciliationResult(issues=issues, skipped_checks=skipped_checks)


def check_transaction_vendor_references(
    transactions: AuditDataset,
    vendors: AuditDataset,
) -> ReconciliationResult:
    match = _first_available_match(
        transactions.dataframe,
        vendors.dataframe,
        preferred_pairs=(("vendor_id", "vendor_id"), ("counterparty", "vendor_name")),
    )
    if match is None:
        return ReconciliationResult(
            issues=[],
            skipped_checks=[
                SkippedCheck(
                    check_name="transaction_vendor_reference",
                    dataset="vendors",
                    reason="No usable vendor reference columns were found.",
                )
            ],
        )

    transaction_field, vendor_field = match
    joined = safe_left_join(
        transactions.dataframe,
        vendors.dataframe,
        left_on=transaction_field,
        right_on=vendor_field,
        right_prefix="vendor",
    )
    issues = [
        _reference_issue(
            row=row,
            field=transaction_field,
            issue_type="unknown_vendor_reference",
            related_dataset="vendors",
            related_record_id=row[transaction_field],
            message=f"Vendor reference '{row[transaction_field]}' was not found in the vendor file.",
        )
        for _, row in unmatched_left_rows(joined).iterrows()
        if _has_value(row[transaction_field])
    ]
    return ReconciliationResult(issues=issues, skipped_checks=[])


def check_transaction_beneficiary_references(
    transactions: AuditDataset,
    beneficiaries: AuditDataset,
) -> ReconciliationResult:
    if "beneficiary_id" not in transactions.dataframe.columns or "beneficiary_id" not in beneficiaries.dataframe.columns:
        return ReconciliationResult(
            issues=[],
            skipped_checks=[
                SkippedCheck(
                    check_name="transaction_beneficiary_reference",
                    dataset="beneficiaries",
                    reason="beneficiary_id is required in both transactions and beneficiaries.",
                )
            ],
        )

    joined = safe_left_join(
        transactions.dataframe,
        beneficiaries.dataframe,
        left_on="beneficiary_id",
        right_on="beneficiary_id",
        right_prefix="beneficiary",
    )
    issues = [
        _reference_issue(
            row=row,
            field="beneficiary_id",
            issue_type="unknown_beneficiary_reference",
            related_dataset="beneficiaries",
            related_record_id=row["beneficiary_id"],
            message=f"Beneficiary reference '{row['beneficiary_id']}' was not found in the beneficiary file.",
        )
        for _, row in unmatched_left_rows(joined).iterrows()
        if _has_value(row["beneficiary_id"])
    ]
    return ReconciliationResult(issues=issues, skipped_checks=[])


def _reference_issue(
    row: pd.Series,
    field: str,
    issue_type: str,
    related_dataset: str,
    related_record_id: object,
    message: str,
) -> ValidationIssue:
    return ValidationIssue(
        dataset="transactions",
        row_number=int(row[AUDIT_ROW_NUMBER]),
        transaction_id=_clean_optional(row.get("transaction_id")),
        field=field,
        issue_type=issue_type,
        severity="error",
        related_dataset=related_dataset,
        related_record_id=_clean_optional(related_record_id),
        message=message,
    )


def _first_available_match(
    left: pd.DataFrame,
    right: pd.DataFrame,
    preferred_pairs: tuple[tuple[str, str], ...],
) -> tuple[str, str] | None:
    for left_column, right_column in preferred_pairs:
        if left_column in left.columns and right_column in right.columns:
            return left_column, right_column
    return None


def _has_value(value: object) -> bool:
    return _clean_optional(value) is not None


def _clean_optional(value: object) -> str | None:
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None
