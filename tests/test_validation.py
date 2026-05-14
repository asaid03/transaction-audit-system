import unittest

import pandas as pd

from transaction_audit.validation import validate_transactions


class ValidationTests(unittest.TestCase):
    def test_detects_expected_sample_issues(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-002"],
                "transaction_date": ["2024-03-15", "not-a-date", "2024-03-16"],
                "account_id": ["ACC-1", "ACC-1", "ACC-1"],
                "amount": ["0", "£1,500", "15000"],
                "currency": ["GBP", "AED", "GBP"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor B"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertIn("zero_amount", issue_types)
        self.assertIn("invalid_amount", issue_types)
        self.assertIn("invalid_date", issue_types)
        self.assertIn("unsupported_currency", issue_types)
        self.assertEqual(2, issue_types.count("duplicate_transaction_id"))
        self.assertIn("large_amount", issue_types)

    def test_missing_amount_is_not_also_invalid_amount(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["2024-03-15"],
                "account_id": ["ACC-1"],
                "amount": [""],
                "currency": ["GBP"],
                "counterparty": ["Vendor A"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertEqual(["missing_value"], issue_types)

    def test_row_numbers_are_position_based_not_index_based(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["DUP", "DUP"],
                "transaction_date": ["2024-03-15", "2024-03-16"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["10", "20"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor B"],
            },
            index=[10, 20],
        )

        result = validate_transactions(dataframe)
        row_numbers = result.issues_dataframe()["row_number"].tolist()

        self.assertEqual([2, 3], row_numbers)
