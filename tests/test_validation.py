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

    def test_phase_one_issues_default_to_transactions_dataset(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["2024-03-15"],
                "account_id": ["ACC-1"],
                "amount": ["not-a-number"],
                "currency": ["GBP"],
                "counterparty": ["Vendor A"],
            }
        )

        result = validate_transactions(dataframe)
        issues = result.issues_dataframe()

        self.assertEqual(["transactions"], issues["dataset"].tolist())
        self.assertEqual([None], issues["related_dataset"].tolist())
        self.assertEqual([None], issues["related_record_id"].tolist())

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

    def test_accepts_inline_currency_and_european_amount_formats(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
                "transaction_date": ["2024-03-15", "2024-03-16", "2024-03-17"],
                "account_id": ["ACC-1", "ACC-2", "ACC-3"],
                "amount": ["GBP 1,500.25", "USD 450", "1.250,00"],
                "currency": ["GBP", "USD", "EUR"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor C"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("invalid_amount", issue_types)
        self.assertEqual([1500.25, 450.00, 1250.00], result.transactions["amount"].tolist())

    def test_parses_common_bank_statement_date_formats(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
                "transaction_date": ["01-May-26", "1-May-2026", "31-May-26"],
                "account_id": ["ACC-1", "ACC-2", "ACC-3"],
                "amount": ["10", "20", "30"],
                "currency": ["GBP", "GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor C"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()
        parsed_dates = result.transactions["transaction_date"].dt.strftime("%Y-%m-%d").tolist()

        self.assertNotIn("invalid_date", issue_types)
        self.assertEqual(["2026-05-01", "2026-05-01", "2026-05-31"], parsed_dates)

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

    def test_uses_preserved_source_row_number_when_available(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["not-a-date"],
                "account_id": ["ACC-1"],
                "amount": ["10"],
                "currency": ["GBP"],
                "counterparty": ["Vendor A"],
                "source_row_number": [12],
            }
        )

        issues = validate_transactions(dataframe).issues_dataframe()

        self.assertEqual([12], issues["row_number"].tolist())

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
        self.assertEqual(["TXN-002", "TXN-001"], issues["related_record_id"].tolist())
        self.assertIn("same account, payee, amount and currency", issues.loc[0, "message"])

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

    def test_flags_missing_invoice_reference_when_invoice_column_exists(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2024-03-15", "2024-03-16"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["250.00", "300.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor B"],
                "invoice_reference": ["INV-001", ""],
            }
        )

        result = validate_transactions(dataframe)
        issues = result.issues_dataframe()

        self.assertIn("missing_invoice_reference", issues["issue_type"].tolist())
        missing_invoice = issues[issues["issue_type"].eq("missing_invoice_reference")]
        self.assertEqual([3], missing_invoice["row_number"].tolist())
        self.assertEqual(["warning"], missing_invoice["severity"].tolist())

    def test_does_not_require_invoice_reference_column(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["2024-03-15"],
                "account_id": ["ACC-1"],
                "amount": ["250.00"],
                "currency": ["GBP"],
                "counterparty": ["Vendor A"],
            }
        )

        result = validate_transactions(dataframe)
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("missing_invoice_reference", issue_types)

    def test_possible_duplicate_payments_use_matching_invoice_reference(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2024-03-15", "2024-03-25"],
                "account_id": ["ACC-1", "ACC-1"],
                "amount": ["250.00", "250.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor A"],
                "invoice_reference": ["INV-001", "INV-001"],
            }
        )

        result = validate_transactions(dataframe)
        issues = result.issues_dataframe()

        self.assertEqual(
            ["possible_duplicate_payment", "possible_duplicate_payment"],
            issues["issue_type"].tolist(),
        )
        self.assertEqual(["TXN-002", "TXN-001"], issues["related_record_id"].tolist())
        self.assertTrue(issues["message"].str.contains("INV-001", regex=False).all())

    def test_population_relative_large_amount_is_not_a_validation_issue(self):
        rows = [
            {
                "transaction_id": f"TXN-{index:03d}",
                "transaction_date": "2024-03-15",
                "account_id": "ACC-1",
                "amount": "1000.00",
                "currency": "GBP",
                "counterparty": "Vendor A",
            }
            for index in range(55)
        ]
        rows.extend(
            [
                {
                    "transaction_id": "TXN-055",
                    "transaction_date": "2024-03-15",
                    "account_id": "ACC-1",
                    "amount": "11886.32",
                    "currency": "GBP",
                    "counterparty": "Vendor A",
                },
                {
                    "transaction_id": "TXN-056",
                    "transaction_date": "2024-03-15",
                    "account_id": "ACC-1",
                    "amount": "80000.00",
                    "currency": "GBP",
                    "counterparty": "Vendor A",
                },
                {
                    "transaction_id": "TXN-057",
                    "transaction_date": "2024-03-15",
                    "account_id": "ACC-1",
                    "amount": "90000.00",
                    "currency": "GBP",
                    "counterparty": "Vendor A",
                },
                {
                    "transaction_id": "TXN-058",
                    "transaction_date": "2024-03-15",
                    "account_id": "ACC-1",
                    "amount": "100000.00",
                    "currency": "GBP",
                    "counterparty": "Vendor A",
                },
                {
                    "transaction_id": "TXN-059",
                    "transaction_date": "2024-03-15",
                    "account_id": "ACC-1",
                    "amount": "110000.00",
                    "currency": "GBP",
                    "counterparty": "Vendor A",
                },
                {
                    "transaction_id": "TXN-060",
                    "transaction_date": "2024-03-15",
                    "account_id": "ACC-1",
                    "amount": "120000.00",
                    "currency": "GBP",
                    "counterparty": "Vendor A",
                },
            ]
        )

        result = validate_transactions(pd.DataFrame(rows))
        issue_types = result.issues_dataframe()["issue_type"].tolist()

        self.assertNotIn("population_large_amount", issue_types)
        self.assertIn("large_amount", issue_types)

    def test_runs_running_balance_integrity_when_balance_is_available(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 8)],
                "transaction_date": ["2026-05-01"] * 7,
                "account_id": ["ACC-1"] * 7,
                "amount": ["100", "50", "25", "40", "10", "20", "30"],
                "currency": ["USD"] * 7,
                "counterparty": [f"Vendor {index}" for index in range(1, 8)],
                "balance": ["900", "850", "825", "700", "690", "670", "640"],
            }
        )

        issues = validate_transactions(dataframe).issues_dataframe()

        balance_issues = issues[issues["issue_type"].eq("balance_movement_mismatch")]
        self.assertEqual([5], balance_issues["row_number"].tolist())

    def test_runs_vendor_concentration_by_currency(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 21)],
                "transaction_date": pd.date_range("2026-05-01", periods=20, freq="D").astype(str),
                "account_id": ["ACC-1"] * 20,
                "amount": ["100"] * 10 + ["40"] * 10,
                "currency": ["USD"] * 20,
                "counterparty": ["Vendor A"] * 10 + ["Vendor B"] * 5 + ["Vendor C"] * 5,
            }
        )

        issues = validate_transactions(dataframe).issues_dataframe()

        concentration = issues[issues["issue_type"].eq("vendor_concentration")]
        self.assertEqual(["Vendor A"], concentration["related_record_id"].tolist())
