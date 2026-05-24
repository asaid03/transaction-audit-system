import unittest

import pandas as pd

from transaction_audit.datasets import AuditBundle, AuditDataset, DatasetType
from transaction_audit.reconciliation import reconcile_bundle


class ReconciliationTests(unittest.TestCase):
    def test_flags_transaction_vendor_missing_from_vendor_file(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "vendor_id": ["VEN-001", "VEN-404"],
                "amount": ["10", "20"],
            },
        )
        vendors = _dataset(DatasetType.VENDORS, {"vendor_id": ["VEN-001"], "vendor_name": ["Vendor One"]})

        result = reconcile_bundle(AuditBundle(transactions=transactions, vendors=vendors))
        issues = result.issues_dataframe()

        self.assertEqual(["unknown_vendor_reference"], issues["issue_type"].tolist())
        self.assertEqual(["vendors"], issues["related_dataset"].tolist())
        self.assertEqual(["VEN-404"], issues["related_record_id"].tolist())
        self.assertEqual([3], issues["row_number"].tolist())

    def test_vendor_reference_can_match_counterparty_to_vendor_name(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "counterparty": ["Known Vendor", "Missing Vendor"],
            },
        )
        vendors = _dataset(DatasetType.VENDORS, {"vendor_name": ["known vendor"]})

        result = reconcile_bundle(AuditBundle(transactions=transactions, vendors=vendors))
        issues = result.issues_dataframe()

        self.assertEqual(["unknown_vendor_reference"], issues["issue_type"].tolist())
        self.assertEqual(["Missing Vendor"], issues["related_record_id"].tolist())

    def test_vendor_reference_fuzzy_match_is_review_warning(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "vendor_id": ["VEN-404"],
                "counterparty": ["Hormuud Telecom"],
            },
        )
        vendors = _dataset(
            DatasetType.VENDORS,
            {
                "vendor_id": ["VEN-001"],
                "vendor_name": ["HORMUUD TELECOM LTD"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, vendors=vendors))
        issues = result.issues_dataframe()

        self.assertEqual(["possible_vendor_match"], issues["issue_type"].tolist())
        self.assertEqual(["warning"], issues["severity"].tolist())
        self.assertEqual(["VEN-001"], issues["related_record_id"].tolist())
        self.assertIn("Possible vendor match", issues.loc[0, "message"])

    def test_vendor_reference_without_fuzzy_match_remains_unknown(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "vendor_id": ["VEN-404"],
                "counterparty": ["Unrelated Supplier"],
            },
        )
        vendors = _dataset(
            DatasetType.VENDORS,
            {
                "vendor_id": ["VEN-001"],
                "vendor_name": ["HORMUUD TELECOM LTD"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, vendors=vendors))
        issues = result.issues_dataframe()

        self.assertEqual(["unknown_vendor_reference"], issues["issue_type"].tolist())
        self.assertEqual(["error"], issues["severity"].tolist())

    def test_duplicate_vendor_references_are_reported_as_audit_issues(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "vendor_id": ["VEN-001"],
            },
        )
        vendors = _dataset(
            DatasetType.VENDORS,
            {
                "vendor_id": ["VEN-001", "ven-001"],
                "vendor_name": ["Vendor One", "Vendor One Duplicate"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, vendors=vendors))
        issues = result.issues_dataframe()

        self.assertEqual(
            ["duplicate_vendor_reference", "duplicate_vendor_reference"],
            issues["issue_type"].tolist(),
        )
        self.assertEqual(["vendors", "vendors"], issues["dataset"].tolist())
        self.assertEqual([2, 3], issues["row_number"].tolist())

    def test_flags_transaction_beneficiary_missing_from_register(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "beneficiary_id": ["BEN-001", "BEN-404"],
            },
        )
        beneficiaries = _dataset(
            DatasetType.BENEFICIARIES,
            {"beneficiary_id": ["BEN-001"], "beneficiary_name": ["Beneficiary One"]},
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, beneficiaries=beneficiaries))
        issues = result.issues_dataframe()

        self.assertEqual(["unknown_beneficiary_reference"], issues["issue_type"].tolist())
        self.assertEqual(["beneficiaries"], issues["related_dataset"].tolist())
        self.assertEqual(["BEN-404"], issues["related_record_id"].tolist())

    def test_duplicate_beneficiary_references_are_reported_as_audit_issues(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "beneficiary_id": ["BEN-001"],
            },
        )
        beneficiaries = _dataset(
            DatasetType.BENEFICIARIES,
            {
                "beneficiary_id": ["BEN-001", "ben-001"],
                "beneficiary_name": ["Beneficiary One", "Beneficiary One Duplicate"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, beneficiaries=beneficiaries))
        issues = result.issues_dataframe()

        self.assertEqual(
            ["duplicate_beneficiary_reference", "duplicate_beneficiary_reference"],
            issues["issue_type"].tolist(),
        )
        self.assertEqual(["beneficiaries", "beneficiaries"], issues["dataset"].tolist())

    def test_reports_skipped_checks_for_missing_optional_files(self):
        transactions = _dataset(DatasetType.TRANSACTIONS, {"transaction_id": ["TXN-001"]})

        result = reconcile_bundle(AuditBundle(transactions=transactions))
        skipped = result.skipped_checks_dataframe()

        self.assertEqual(
            [
                "transaction_vendor_reference",
                "transaction_beneficiary_reference",
                "transaction_budget_reference",
            ],
            skipped["check_name"].tolist(),
        )


def _dataset(dataset_type: DatasetType, data: dict[str, list[str]]) -> AuditDataset:
    return AuditDataset(
        dataset_type=dataset_type,
        dataframe=pd.DataFrame(data),
        source_name=f"{dataset_type.value}.csv",
    )
