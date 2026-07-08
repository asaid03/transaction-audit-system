import unittest

import pandas as pd

from transaction_audit.ui.display import (
    clean_display_frame,
    display_dataset_summary,
    display_ml_anomalies,
    display_transaction_review,
)


class AppDisplayTest(unittest.TestCase):
    def test_clean_display_frame_blanks_none_like_values(self):
        frame = pd.DataFrame(
            {
                "actual_none": [None],
                "string_none": ["None"],
                "string_missing": ["<NA>"],
            }
        )

        display = clean_display_frame(frame)

        self.assertEqual(display.iloc[0]["actual_none"], "")
        self.assertEqual(display.iloc[0]["string_none"], "")
        self.assertEqual(display.iloc[0]["string_missing"], "")

    def test_display_dataset_summary_uses_text_status(self):
        frame = pd.DataFrame(
            {
                "dataset": ["transactions", "budget"],
                "uploaded": [True, False],
                "source_name": ["transactions.csv", None],
                "rows": [12, 0],
            }
        )

        display = display_dataset_summary(frame)

        self.assertEqual(display["uploaded"].tolist(), ["Uploaded", "Not uploaded"])
        self.assertEqual(display.iloc[1]["source_name"], "")

    def test_display_transaction_review_formats_band_for_readability(self):
        frame = pd.DataFrame(
            {
                "review_priority_score": [80],
                "review_priority_band": ["high"],
                "review_priority_drivers": ["Invalid transaction date"],
                "row_number": [2],
                "issue_types": ["invalid_date"],
                "transaction_id": [None],
            }
        )

        display = display_transaction_review(frame)

        self.assertEqual(display.iloc[0]["review_priority_band"], "High")

    def test_display_ml_anomalies_hides_zero_score_rows(self):
        frame = pd.DataFrame(
            [
                {
                    "ml_anomaly_score": 0,
                    "ml_anomaly_band": "low",
                    "ml_anomaly_drivers": "No anomaly drivers detected.",
                    "row_number": 2,
                    "transaction_id": "TXN-001",
                },
                {
                    "ml_anomaly_score": 85,
                    "ml_anomaly_band": "high",
                    "ml_anomaly_drivers": "Invoice/reference repeats",
                    "row_number": 3,
                    "transaction_id": "TXN-002",
                },
            ]
        )

        display = display_ml_anomalies(frame)

        self.assertEqual([85], display["ml_anomaly_score"].tolist())
        self.assertEqual(["High"], display["ml_anomaly_band"].tolist())
        self.assertEqual(display.iloc[0]["transaction_id"], "TXN-002")


if __name__ == "__main__":
    unittest.main()
