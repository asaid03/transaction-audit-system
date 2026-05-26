import unittest

import pandas as pd

from transaction_audit.cleaning import clean_transaction_export
from transaction_audit.datasets import AuditBundle, AuditDataset, DatasetType
from transaction_audit.ingestion import load_dataset
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

    def test_realistic_bank_narration_supports_vendor_fuzzy_matching(self):
        raw = load_dataset("sample_data/realistic_bank_export_29.csv")
        cleaned = clean_transaction_export(raw).dataframe
        transactions = AuditDataset(DatasetType.TRANSACTIONS, cleaned, "realistic_bank_export_29.csv")
        vendors = _dataset(
            DatasetType.VENDORS,
            {
                "vendor_id": ["VEN-001", "VEN-002", "VEN-003"],
                "vendor_name": ["Somali Medical Supplies", "HORMUUD TELECOM LTD", "Logistics Partner"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, vendors=vendors))
        issues = result.issues_dataframe()
        possible_matches = issues[issues["issue_type"].eq("possible_vendor_match")]

        self.assertIn("VEN-001", possible_matches["related_record_id"].tolist())
        self.assertIn("Somali Medical Supplies", " ".join(possible_matches["message"].tolist()))
        self.assertIn("unknown_vendor_reference", issues["issue_type"].tolist())

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

    def test_beneficiary_overpayment_is_reported_against_expected_amount(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "beneficiary_id": ["BEN-001", "BEN-001"],
                "amount": ["150", "100"],
            },
        )
        beneficiaries = _dataset(
            DatasetType.BENEFICIARIES,
            {
                "beneficiary_id": ["BEN-001"],
                "beneficiary_name": ["Beneficiary One"],
                "expected_amount": ["200"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, beneficiaries=beneficiaries))
        issues = result.issues_dataframe()

        self.assertIn("beneficiary_overpaid", issues["issue_type"].tolist())
        overpaid = issues[issues["issue_type"].eq("beneficiary_overpaid")].iloc[0]
        self.assertEqual("BEN-001", overpaid["related_record_id"])
        self.assertIn("250.00", overpaid["message"])
        self.assertIn("200.00", overpaid["message"])

    def test_beneficiary_entitlement_matches_normalized_ids(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "beneficiary_id": [" ben-001 "],
                "amount": ["150"],
            },
        )
        beneficiaries = _dataset(
            DatasetType.BENEFICIARIES,
            {
                "beneficiary_id": ["BEN-001"],
                "beneficiary_name": ["Beneficiary One"],
                "expected_amount": ["100"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, beneficiaries=beneficiaries))
        issues = result.issues_dataframe()

        self.assertIn("beneficiary_overpaid", issues["issue_type"].tolist())

    def test_beneficiary_entitlement_is_skipped_without_expected_amount(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "beneficiary_id": ["BEN-001"],
                "amount": ["150"],
            },
        )
        beneficiaries = _dataset(
            DatasetType.BENEFICIARIES,
            {
                "beneficiary_id": ["BEN-001"],
                "beneficiary_name": ["Beneficiary One"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, beneficiaries=beneficiaries))
        skipped = result.skipped_checks_dataframe()

        self.assertIn("beneficiary_entitlement", skipped["check_name"].tolist())

    def test_beneficiary_entitlement_tolerates_duplicate_reference_rows(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "beneficiary_id": ["BEN-001"],
                "amount": ["150"],
            },
        )
        beneficiaries = _dataset(
            DatasetType.BENEFICIARIES,
            {
                "beneficiary_id": ["BEN-001", "ben-001"],
                "beneficiary_name": ["Beneficiary One", "Beneficiary One Duplicate"],
                "expected_amount": ["100", "100"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, beneficiaries=beneficiaries))
        issues = result.issues_dataframe()

        self.assertIn("duplicate_beneficiary_reference", issues["issue_type"].tolist())
        self.assertIn("beneficiary_overpaid", issues["issue_type"].tolist())

    def test_budget_reconciliation_flags_overspend_and_unknown_budget_line(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
                "budget_line_id": ["BL-FOOD", "BL-FOOD", "BL-UNKNOWN"],
                "amount": ["600", "500", "50"],
            },
        )
        budget = _dataset(
            DatasetType.BUDGET,
            {
                "budget_line_id": ["BL-FOOD"],
                "budget_name": ["Food assistance"],
                "approved_amount": ["1,000"],
                "currency": ["USD"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, budget=budget))
        issues = result.issues_dataframe()

        self.assertIn("budget_line_overspent", issues["issue_type"].tolist())
        self.assertIn("unknown_budget_line", issues["issue_type"].tolist())
        unknown = issues[issues["issue_type"].eq("unknown_budget_line")].iloc[0]
        self.assertEqual("TXN-003", unknown["transaction_id"])
        self.assertEqual(4, unknown["row_number"])

    def test_budget_reconciliation_matches_normalized_budget_keys(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "budget_line_id": [" bl-cash "],
                "amount": ["250"],
            },
        )
        budget = _dataset(
            DatasetType.BUDGET,
            {
                "budget_line_id": ["BL-CASH"],
                "budget_name": ["Cash assistance"],
                "approved_amount": ["200"],
                "currency": ["USD"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, budget=budget))
        issues = result.issues_dataframe()

        self.assertIn("budget_line_overspent", issues["issue_type"].tolist())
        self.assertNotIn("unknown_budget_line", issues["issue_type"].tolist())

    def test_budget_reconciliation_skips_without_budget_line(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "amount": ["100"],
            },
        )
        budget = _dataset(
            DatasetType.BUDGET,
            {
                "budget_line_id": ["BL-FOOD"],
                "budget_name": ["Food assistance"],
                "approved_amount": ["1,000"],
                "currency": ["USD"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, budget=budget))
        skipped = result.skipped_checks_dataframe()

        self.assertIn("transaction_budget_reference", skipped["check_name"].tolist())

    def test_budget_reconciliation_tolerates_duplicate_reference_rows(self):
        transactions = _dataset(
            DatasetType.TRANSACTIONS,
            {
                "transaction_id": ["TXN-001"],
                "budget_line_id": ["BL-FOOD"],
                "amount": ["1,100"],
            },
        )
        budget = _dataset(
            DatasetType.BUDGET,
            {
                "budget_line_id": ["BL-FOOD", "bl-food"],
                "budget_name": ["Food assistance", "Food duplicate"],
                "approved_amount": ["1,000", "1,000"],
                "currency": ["USD", "USD"],
            },
        )

        result = reconcile_bundle(AuditBundle(transactions=transactions, budget=budget))
        issues = result.issues_dataframe()

        self.assertIn("duplicate_budget_reference", issues["issue_type"].tolist())
        self.assertIn("budget_line_overspent", issues["issue_type"].tolist())

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
