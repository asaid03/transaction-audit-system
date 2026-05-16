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
                "amount": ["0", "not-a-number", "15000"],
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

    def test_accepts_common_finance_amount_formats(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
                "transaction_date": ["2024-03-15", "2024-03-16", "2024-03-17"],
                "account_id": ["ACC-1", "ACC-2", "ACC-3"],
                "amount": ["£1,500.25", "(1,200.50)", "€250"],
                "currency": ["GBP", "GBP", "EUR"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor C"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("invalid_amount", issue_types)
        self.assertEqual([1500.25, -1200.50, 250.00], result.transactions["amount"].tolist())

    def test_parses_supported_date_formats_without_dayfirst_ambiguity(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003", "TXN-004"],
                "transaction_date": ["2026-04-08", "08/04/2026", "08-04-2026", "08 Apr 2026"],
                "account_id": ["ACC-1", "ACC-2", "ACC-3", "ACC-4"],
                "amount": ["10", "20", "30", "40"],
                "currency": ["GBP", "GBP", "GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor C", "Vendor D"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()
        parsed_dates = result.transactions["transaction_date"].dt.strftime("%Y-%m-%d").tolist()

        self.assertNotIn("invalid_date", issue_types)
        self.assertEqual(["2026-04-08", "2026-04-08", "2026-04-08", "2026-04-08"], parsed_dates)

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

    def test_flags_possible_duplicate_payments_with_different_ids(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2024-03-15", "2024-03-16"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["250.00", "250.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor A"],
            }
        )

        result = validate_transactions(dataframe)
        issues = result.issues_dataframe()

        self.assertEqual(
            ["possible_duplicate_payment", "possible_duplicate_payment"],
            issues["issue_type"].tolist(),
        )
        self.assertEqual(["warning", "warning"], issues["severity"].tolist())

    def test_possible_duplicate_payments_require_different_transaction_ids(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-001"],
                "transaction_date": ["2024-03-15", "2024-03-16"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["250.00", "250.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor A"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("possible_duplicate_payment", issue_types)

    def test_possible_duplicate_payments_respect_date_window(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2024-03-15", "2024-03-20"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["250.00", "250.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor A"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("possible_duplicate_payment", issue_types)

    def test_possible_duplicate_payments_ignore_incomplete_key_fields(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2024-03-15", "2024-03-16"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["250.00", "250.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["", ""],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("possible_duplicate_payment", issue_types)
