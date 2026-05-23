import unittest
from pathlib import Path

from transaction_audit.bundle_cli import build_bundle_from_paths
from transaction_audit.reconciliation import reconcile_bundle
from transaction_audit.validation import validate_transactions


class BundleCliTests(unittest.TestCase):
    def test_builds_sample_bundle_and_reconciles_references(self):
        bundle = build_bundle_from_paths(
            transactions_path=Path("sample_data/phase2_transactions.csv"),
            vendors_path=Path("sample_data/phase2_vendors.csv"),
            beneficiaries_path=Path("sample_data/phase2_beneficiaries.csv"),
        )

        result = reconcile_bundle(bundle)
        issue_types = result.issues_dataframe()["issue_type"].tolist()
        transaction_validation = validate_transactions(bundle.transactions.dataframe)
        transaction_issue_types = transaction_validation.issues_dataframe()["issue_type"].tolist()

        self.assertEqual(5, bundle.transactions.row_count)
        self.assertIn("zero_amount", transaction_issue_types)
        self.assertIn("unknown_vendor_reference", issue_types)
        self.assertIn("unknown_beneficiary_reference", issue_types)
