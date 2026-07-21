import unittest

import pandas as pd

from transaction_audit.risk import score_issues, score_transactions
from transaction_audit.validation import validate_transactions


class RiskTests(unittest.TestCase):
    def test_duplicate_transaction_id_produces_high_priority(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-001"],
                "transaction_date": ["2026-05-01", "2026-05-02"],
                "account_id": ["OPS", "OPS"],
                "amount": ["100", "200"],
                "currency": ["USD", "USD"],
                "counterparty": ["Vendor A", "Vendor A"],
            }
        )
        result = validate_transactions(dataframe)

        review = score_transactions(result.transactions, result.issues_dataframe())

        self.assertEqual([90, 90], review["review_priority_score"].tolist())
        self.assertEqual(["high", "high"], review["review_priority_band"].tolist())
        self.assertIn("Duplicate transaction ID", review.loc[0, "review_priority_drivers"])

    def test_invalid_date_and_missing_field_accumulate_to_capped_score(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["not-a-date"],
                "account_id": ["OPS"],
                "amount": [""],
                "currency": ["USD"],
                "counterparty": ["Vendor A"],
            }
        )
        result = validate_transactions(dataframe)

        review = score_transactions(result.transactions, result.issues_dataframe())

        self.assertEqual(100, review.loc[0, "review_priority_score"])
        self.assertEqual("high", review.loc[0, "review_priority_band"])
        self.assertIn("Invalid transaction date", review.loc[0, "review_priority_drivers"])
        self.assertIn("Required transaction field is missing", review.loc[0, "review_priority_drivers"])

    def test_unsupported_currency_and_large_amount_increase_priority(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["2026-05-01"],
                "account_id": ["OPS"],
                "amount": ["15000"],
                "currency": ["AED"],
                "counterparty": ["Vendor A"],
            }
        )
        result = validate_transactions(dataframe)

        review = score_transactions(result.transactions, result.issues_dataframe())

        self.assertEqual(100, review.loc[0, "review_priority_score"])
        self.assertEqual("medium", review.loc[0, "review_priority_band"])
        self.assertIn("Currency is not in the expected list", review.loc[0, "review_priority_drivers"])
        self.assertIn("Payment is above the review limit", review.loc[0, "review_priority_drivers"])

    def test_normal_transaction_remains_low_priority(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2026-05-01", "2026-05-02"],
                "account_id": ["OPS", "OPS"],
                "amount": ["100", "200"],
                "currency": ["USD", "USD"],
                "counterparty": ["Recurring Vendor", "Recurring Vendor"],
            }
        )
        result = validate_transactions(dataframe)

        review = score_transactions(result.transactions, result.issues_dataframe())

        self.assertEqual([0, 0], review["review_priority_score"].tolist())
        self.assertEqual(["low", "low"], review["review_priority_band"].tolist())

    def test_possible_split_payment_is_explainable_high_priority(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2025-11-25", "2025-11-25"],
                "account_id": ["OPS", "OPS"],
                "amount": ["4950.00", "4925.00"],
                "currency": ["GBP", "GBP"],
                "counterparty": ["Vendor A", "Vendor A"],
                "invoice_reference": ["SP-001-INV", "SP-001-INV"],
            }
        )
        result = validate_transactions(dataframe)

        review = score_transactions(result.transactions, result.issues_dataframe())

        self.assertEqual([75, 75], review["review_priority_score"].tolist())
        self.assertEqual(["high", "high"], review["review_priority_band"].tolist())
        self.assertEqual(
            [
                "Possible split payment with TXN-002",
                "Possible split payment with TXN-001",
            ],
            review["review_priority_drivers"].tolist(),
        )

    def test_transaction_review_preserves_source_file_row_numbers(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001"],
                "transaction_date": ["2026-05-01"],
                "account_id": ["OPS"],
                "amount": ["100"],
                "currency": ["USD"],
                "counterparty": ["Vendor A"],
                "source_row_number": [17],
            }
        )
        result = validate_transactions(dataframe)

        review = score_transactions(result.transactions, result.issues_dataframe())

        self.assertEqual([17], review["row_number"].tolist())

    def test_rare_counterparty_signal_requires_enough_rows(self):
        small_file = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "transaction_date": ["2026-05-01", "2026-05-02"],
                "account_id": ["OPS", "OPS"],
                "amount": ["100", "200"],
                "currency": ["USD", "USD"],
                "counterparty": ["Vendor A", "Vendor B"],
            }
        )
        large_file = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{number:03d}" for number in range(1, 11)],
                "transaction_date": ["2026-05-01"] * 10,
                "account_id": ["OPS"] * 10,
                "amount": ["100"] * 10,
                "currency": ["USD"] * 10,
                "counterparty": ["Recurring Vendor"] * 9 + ["One-Off Vendor"],
            }
        )

        small_result = validate_transactions(small_file)
        large_result = validate_transactions(large_file)

        small_review = score_transactions(small_result.transactions, small_result.issues_dataframe())
        large_review = score_transactions(large_result.transactions, large_result.issues_dataframe())

        self.assertEqual([0, 0], small_review["review_priority_score"].tolist())
        self.assertEqual(10, large_review.loc[9, "review_priority_score"])
        self.assertIn("Payee appears only once in this file", large_review.loc[9, "review_priority_drivers"])

    def test_possible_vendor_match_is_explainable_medium_priority(self):
        issues = pd.DataFrame(
            [
                {
                    "dataset": "transactions",
                    "row_number": 2,
                    "transaction_id": "TXN-001",
                    "field": "counterparty",
                    "issue_type": "possible_vendor_match",
                    "severity": "warning",
                    "related_dataset": "vendors",
                    "related_record_id": "VEN-001",
                    "message": "Possible vendor match: Hormuud Telecom.",
                }
            ]
        )

        scored = score_issues(issues)

        self.assertEqual(45, scored.loc[0, "review_priority_score"])
        self.assertEqual("medium", scored.loc[0, "review_priority_band"])
        self.assertEqual(
            "Payee name may match an approved vendor, but needs confirmation",
            scored.loc[0, "review_priority_drivers"],
        )

    def test_scores_are_deterministic(self):
        dataframe = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-001", "TXN-003"],
                "transaction_date": ["2026-05-01", "bad-date", "2026-05-03"],
                "account_id": ["OPS", "OPS", "OPS"],
                "amount": ["15000", "200", "0"],
                "currency": ["USD", "AED", "USD"],
                "counterparty": ["Vendor A", "Vendor B", "Vendor C"],
            }
        )
        result = validate_transactions(dataframe)
        issues = result.issues_dataframe()

        first = score_transactions(result.transactions, issues)
        second = score_transactions(result.transactions, issues)

        pd.testing.assert_frame_equal(first, second)

    def test_driver_order_does_not_depend_on_issue_order(self):
        issues = pd.DataFrame(
            [
                {
                    "dataset": "transactions",
                    "row_number": 2,
                    "transaction_id": "TXN-001",
                    "field": "currency",
                    "issue_type": "unsupported_currency",
                    "severity": "warning",
                    "related_dataset": None,
                    "related_record_id": None,
                    "message": "Unsupported currency.",
                },
                {
                    "dataset": "transactions",
                    "row_number": 2,
                    "transaction_id": "TXN-001",
                    "field": "transaction_date",
                    "issue_type": "invalid_date",
                    "severity": "error",
                    "related_dataset": None,
                    "related_record_id": None,
                    "message": "Invalid date.",
                },
            ]
        )

        first = score_issues(issues)
        second = score_issues(issues.iloc[::-1].reset_index(drop=True))

        self.assertEqual(
            "Invalid transaction date; Currency is not in the expected list",
            first.loc[0, "review_priority_drivers"],
        )
        self.assertEqual(
            first.loc[0, "review_priority_drivers"],
            second.loc[0, "review_priority_drivers"],
        )


if __name__ == "__main__":
    unittest.main()
