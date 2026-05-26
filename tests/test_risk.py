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
        self.assertIn("Unsupported currency", review.loc[0, "review_priority_drivers"])
        self.assertIn("Amount exceeds configured threshold", review.loc[0, "review_priority_drivers"])

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
            "Counterparty only matched vendor file fuzzily",
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


if __name__ == "__main__":
    unittest.main()
