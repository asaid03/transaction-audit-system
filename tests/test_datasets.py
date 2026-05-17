import unittest

import pandas as pd

from transaction_audit.datasets import (
    AuditBundle,
    AuditDataset,
    DatasetType,
    required_columns_for_dataset,
)


class DatasetTests(unittest.TestCase):
    def test_bundle_requires_transactions_but_references_are_optional(self):
        transactions = AuditDataset(
            dataset_type=DatasetType.TRANSACTIONS,
            dataframe=pd.DataFrame({"transaction_id": ["TXN-001"]}),
            source_name="transactions.csv",
        )

        bundle = AuditBundle(transactions=transactions)

        self.assertFalse(bundle.has_reference_data())
        self.assertEqual([], bundle.reference_datasets())

    def test_bundle_reports_uploaded_reference_datasets(self):
        transactions = AuditDataset(
            dataset_type=DatasetType.TRANSACTIONS,
            dataframe=pd.DataFrame({"transaction_id": ["TXN-001"]}),
            source_name="transactions.csv",
        )
        vendors = AuditDataset(
            dataset_type=DatasetType.VENDORS,
            dataframe=pd.DataFrame({"vendor_id": ["VEN-001"]}),
            source_name="vendors.csv",
        )

        bundle = AuditBundle(transactions=transactions, vendors=vendors)
        summary = bundle.dataset_summary()

        self.assertTrue(bundle.has_reference_data())
        self.assertEqual([vendors], bundle.reference_datasets())
        self.assertEqual(["transactions", "vendors", "beneficiaries", "budget"], summary["dataset"].tolist())
        self.assertEqual([True, True, False, False], summary["uploaded"].tolist())

    def test_dataset_definitions_expose_required_columns(self):
        self.assertEqual(
            ("transaction_id", "transaction_date", "amount", "currency"),
            required_columns_for_dataset(DatasetType.TRANSACTIONS),
        )
        self.assertEqual(
            ("beneficiary_id", "beneficiary_name"),
            required_columns_for_dataset(DatasetType.BENEFICIARIES),
        )
