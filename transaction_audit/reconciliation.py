from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from transaction_audit.datasets import AuditBundle, AuditDataset
from transaction_audit.joins import AUDIT_ROW_NUMBER, normalize_join_key, safe_left_join, unmatched_left_rows, with_audit_row_numbers
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

    if bundle.budget is None:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_budget_reference",
                dataset="budget",
                reason="Budget file was not uploaded.",
            )
        )
    else:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_budget_reference",
                dataset="budget",
                reason="Budget reconciliation is planned but not implemented yet.",
            )
        )

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
    duplicate_issues = duplicate_reference_key_issues(
        vendors,
        key_field=vendor_field,
        issue_type="duplicate_vendor_reference",
        message_template="Vendor reference '{value}' appears more than once in the vendor file.",
    )
    vendors_for_join = deduplicate_reference_rows(vendors.dataframe, vendor_field)
    joined = safe_left_join(
        transactions.dataframe,
        vendors_for_join,
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
        for row in unmatched_left_rows(joined).to_dict("records")
        if _has_value(row[transaction_field])
    ]
    return ReconciliationResult(issues=duplicate_issues + issues, skipped_checks=[])


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

    duplicate_issues = duplicate_reference_key_issues(
        beneficiaries,
        key_field="beneficiary_id",
        issue_type="duplicate_beneficiary_reference",
        message_template="Beneficiary reference '{value}' appears more than once in the beneficiary file.",
    )
    beneficiaries_for_join = deduplicate_reference_rows(beneficiaries.dataframe, "beneficiary_id")
    joined = safe_left_join(
        transactions.dataframe,
        beneficiaries_for_join,
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
        for row in unmatched_left_rows(joined).to_dict("records")
        if _has_value(row["beneficiary_id"])
    ]
    return ReconciliationResult(issues=duplicate_issues + issues, skipped_checks=[])


def duplicate_reference_key_issues(
    dataset: AuditDataset,
    key_field: str,
    issue_type: str,
    message_template: str,
) -> list[ValidationIssue]:
    keyed = with_audit_row_numbers(dataset.dataframe)
    normalized_key = normalize_join_key(keyed[key_field])
    duplicate_mask = normalized_key.notna() & normalized_key.duplicated(keep=False)

    return [
        ValidationIssue(
            dataset=dataset.dataset_type.value,
            row_number=int(row[AUDIT_ROW_NUMBER]),
            transaction_id=None,
            field=key_field,
            issue_type=issue_type,
            severity="error",
            related_dataset=None,
            related_record_id=_clean_optional(row[key_field]),
            message=message_template.format(value=row[key_field]),
        )
        for row in keyed.loc[duplicate_mask].to_dict("records")
    ]


def deduplicate_reference_rows(dataframe: pd.DataFrame, key_field: str) -> pd.DataFrame:
    normalized_key = normalize_join_key(dataframe[key_field])
    usable_mask = normalized_key.notna()
    keep_mask = usable_mask & ~normalized_key.duplicated(keep="first")
    return dataframe.loc[keep_mask].copy()


def _reference_issue(
    row: dict[str, object],
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
