from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.datasets import AuditBundle
from transaction_audit.reconciliation import ReconciliationResult, reconcile_bundle
from transaction_audit.types import ISSUE_COLUMNS, ValidationResult
from transaction_audit.validation import validate_transactions


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
