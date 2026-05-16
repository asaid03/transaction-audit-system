import io
import unittest

import openpyxl
import pandas as pd

from transaction_audit.audit import build_audit_summary, build_audit_workbook, issue_counts_dataframe
from transaction_audit.schema import apply_schema
from transaction_audit.validation import validate_transactions


class AuditTests(unittest.TestCase):
    def test_issue_counts_group_by_type_and_severity(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-002"],
                "transaction_date": ["2026-04-01", "not-a-date", "2026-04-02"],
                "account_id": ["ACC-1", "ACC-1", "ACC-1"],
                "amount": ["0", "10", "10"],
                "currency": ["GBP", "GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor B"],
            }
        )

        result = validate_transactions(dataframe)
        issue_counts = issue_counts_dataframe(result)

        self.assertIn("issue_type", issue_counts.columns)
        self.assertIn("severity", issue_counts.columns)
        self.assertIn("count", issue_counts.columns)
        self.assertIn("duplicate_transaction_id", issue_counts["issue_type"].tolist())

    def test_build_audit_workbook_contains_review_sheets(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["2026-04-01"],
                "account_id": ["ACC-1"],
                "amount": ["10"],
                "currency": ["GBP"],
                "counterparty": ["Vendor A"],
            }
        )
        schema_result = apply_schema(dataframe)
        result = validate_transactions(schema_result.transactions)
        summary = build_audit_summary(result, "sample.csv")

        workbook_bytes = build_audit_workbook(result, schema_result, summary)
        workbook = openpyxl.load_workbook(io.BytesIO(workbook_bytes), read_only=True)

        self.assertEqual(
            {"summary", "issues", "issue_counts", "schema_mapping", "transactions"},
            set(workbook.sheetnames),
        )
