from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.cleaning import CleaningResult, clean_transaction_export
from transaction_audit.datasets import AuditBundle, DatasetType
from transaction_audit.reconciliation import ReconciliationResult, reconcile_bundle
from transaction_audit.types import ISSUE_COLUMNS, ValidationResult
from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import build_mapping_plan


@dataclass(frozen=True)
class BundleAuditResult:
    bundle: AuditBundle
    transaction_validation: ValidationResult
    reference_reconciliation: ReconciliationResult

    def transaction_issues_dataframe(self) -> pd.DataFrame:
        return self.transaction_validation.issues_dataframe()

    def reference_issues_dataframe(self) -> pd.DataFrame:
        return self.reference_reconciliation.issues_dataframe()

    def skipped_checks_dataframe(self) -> pd.DataFrame:
        return self.reference_reconciliation.skipped_checks_dataframe()

    def all_issues_dataframe(self) -> pd.DataFrame:
        frames = [self.transaction_issues_dataframe(), self.reference_issues_dataframe()]
        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return pd.DataFrame(columns=ISSUE_COLUMNS)
        return pd.concat(non_empty, ignore_index=True)


def run_bundle_audit(bundle: AuditBundle) -> BundleAuditResult:
    return BundleAuditResult(
        bundle=bundle,
        transaction_validation=validate_transactions(bundle.transactions.dataframe),
        reference_reconciliation=reconcile_bundle(bundle),
    )


def maybe_clean_transactions(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, CleaningResult | None]:
    raw_schema_is_ready = build_mapping_plan(
        dataframe,
        dataset_type=DatasetType.TRANSACTIONS,
    ).auto_schema.has_required_schema

    cleaning_result = clean_transaction_export(dataframe)
    if not cleaning_result.applied:
        return dataframe, None if raw_schema_is_ready else cleaning_result

    cleaned_schema_is_ready = build_mapping_plan(
        cleaning_result.dataframe,
        dataset_type=DatasetType.TRANSACTIONS,
    ).auto_schema.has_required_schema
    if not cleaned_schema_is_ready:
        return dataframe, None if raw_schema_is_ready else cleaning_result

    if not _cleaning_changed_structure(cleaning_result):
        return dataframe, None

    if not raw_schema_is_ready or _cleaning_removed_rows(cleaning_result):
        return cleaning_result.dataframe, cleaning_result

    return dataframe, None


def _cleaning_removed_rows(cleaning_result: CleaningResult) -> bool:
    return bool(cleaning_result.dropped_rows)


def _cleaning_changed_structure(cleaning_result: CleaningResult) -> bool:
    if cleaning_result.source_header_row and cleaning_result.source_header_row > 1:
        return True
    if cleaning_result.dropped_rows:
        return True

    header_message = f"Detected transaction header on source row {cleaning_result.source_header_row}."
    return any(transformation != header_message for transformation in cleaning_result.transformations)
