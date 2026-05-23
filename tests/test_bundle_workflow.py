import unittest

import pandas as pd

from transaction_audit.bundle_workflow import run_bundle_audit
from transaction_audit.datasets import AuditBundle, AuditDataset, DatasetType


class BundleWorkflowTests(unittest.TestCase):
    def test_run_bundle_audit_combines_transaction_and_reference_issues(self):
        transactions = AuditDataset(
            dataset_type=DatasetType.TRANSACTIONS,
            dataframe=pd.DataFrame(
                {
                    "transaction_id": ["TXN-001"],
                    "transaction_date": ["2026-05-01"],
                    "account_id": ["OPS"],
                    "amount": ["0"],
                    "currency": ["USD"],
                    "counterparty": ["Unknown Vendor"],
                    "vendor_id": ["VEN-404"],
                }
            ),
            source_name="transactions.csv",
        )
        vendors = AuditDataset(
            dataset_type=DatasetType.VENDORS,
            dataframe=pd.DataFrame({"vendor_id": ["VEN-001"], "vendor_name": ["Known Vendor"]}),
            source_name="vendors.csv",
        )

        result = run_bundle_audit(AuditBundle(transactions=transactions, vendors=vendors))
        issue_types = result.all_issues_dataframe()["issue_type"].tolist()

        self.assertIn("zero_amount", issue_types)
        self.assertIn("unknown_vendor_reference", issue_types)
