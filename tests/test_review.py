import unittest

import pandas as pd

from transaction_audit.review import (
    filter_by_issue_types,
    filter_by_values,
    sort_by_review_priority,
    unique_issue_types,
    unique_values,
)


class ReviewTests(unittest.TestCase):
    def test_sort_by_review_priority_uses_band_then_score(self):
        dataframe = pd.DataFrame(
            [
                {"review_priority_band": "Low", "review_priority_score": 90, "row_number": 4},
                {"review_priority_band": "High", "review_priority_score": 50, "row_number": 3},
                {"review_priority_band": "High", "review_priority_score": 80, "row_number": 2},
                {"review_priority_band": "Medium", "review_priority_score": 100, "row_number": 5},
            ]
        )

        sorted_frame = sort_by_review_priority(dataframe)

        self.assertEqual([2, 3, 5, 4], sorted_frame["row_number"].tolist())

    def test_filter_by_values_leaves_dataframe_when_no_filter_selected(self):
        dataframe = pd.DataFrame({"currency": ["USD", "GBP"]})

        filtered = filter_by_values(dataframe, "currency", [])

        pd.testing.assert_frame_equal(dataframe, filtered)

    def test_filter_by_values_filters_exact_values(self):
        dataframe = pd.DataFrame({"currency": ["USD", "GBP", "EUR"]})

        filtered = filter_by_values(dataframe, "currency", ["USD", "EUR"])

        self.assertEqual(["USD", "EUR"], filtered["currency"].tolist())

    def test_filter_by_issue_types_uses_technical_issue_type_column(self):
        dataframe = pd.DataFrame(
            {
                "technical_issue_type": ["invalid_date", "large_amount"],
                "finding": ["Date", "Amount"],
            }
        )

        filtered = filter_by_issue_types(dataframe, ["large_amount"])

        self.assertEqual(["Amount"], filtered["finding"].tolist())

    def test_filter_by_issue_types_handles_comma_separated_transaction_issue_types(self):
        dataframe = pd.DataFrame(
            {
                "issue_types": ["invalid_date, unsupported_currency", "", "large_amount"],
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
            }
        )

        filtered = filter_by_issue_types(dataframe, ["unsupported_currency"])

        self.assertEqual(["TXN-001"], filtered["transaction_id"].tolist())

    def test_unique_helpers_drop_blanks_and_split_issue_types(self):
        dataframe = pd.DataFrame(
            {
                "currency": ["USD", "", "GBP", "USD"],
                "issue_types": ["invalid_date, large_amount", "", "invalid_date", ""],
            }
        )

        self.assertEqual(["GBP", "USD"], unique_values(dataframe, "currency"))
        self.assertEqual(["invalid_date", "large_amount"], unique_issue_types(dataframe))


if __name__ == "__main__":
    unittest.main()
