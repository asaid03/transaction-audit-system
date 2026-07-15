import unittest
from pathlib import Path

import pandas as pd

from transaction_audit.anomaly import (
    evaluate_anomaly_detection,
    fit_anomaly_model,
    score_transaction_anomalies,
)
from transaction_audit.schema import apply_schema


BENCHMARK_PATH = Path("sample_data/reduced_transactions_200_with_anomalies.csv")


def load_benchmark_transactions():
    raw = pd.read_csv(BENCHMARK_PATH, dtype=str, keep_default_na=False)
    schema_result = apply_schema(
        raw,
        source_to_canonical={
            "transaction_id": "transaction_id",
            "date": "transaction_date",
            "account_code": "account_id",
            "amount": "amount",
            "currency": "currency",
            "description": "counterparty",
            "invoice_reference": "invoice_reference",
            "vendor_id": "vendor_id",
        },
        required_columns=[
            "transaction_id",
            "transaction_date",
            "account_id",
            "amount",
            "currency",
            "counterparty",
        ],
        allowed_columns=[
            "transaction_id",
            "transaction_date",
            "account_id",
            "amount",
            "currency",
            "counterparty",
            "invoice_reference",
            "vendor_id",
        ],
    )
    return raw, schema_result.transactions


class AnomalyTests(unittest.TestCase):
    def test_output_preserves_source_file_row_numbers(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(60)],
                "amount": [100.0] * 60,
                "counterparty": ["Vendor A"] * 60,
                "invoice_reference": [f"INV-{index:03d}" for index in range(60)],
                "source_row_number": range(9, 69),
            }
        )

        scored = score_transaction_anomalies(transactions)

        self.assertEqual(9, scored.loc[0, "row_number"])
        self.assertEqual(68, scored.loc[59, "row_number"])

    def test_benchmark_model_detects_labelled_anomaly_rows(self):
        raw, transactions = load_benchmark_transactions()

        model = fit_anomaly_model(transactions)
        scored = score_transaction_anomalies(transactions, model)
        metrics = evaluate_anomaly_detection(scored, raw["is_anomaly"])

        self.assertGreater(metrics["true_positive"], 0)
        self.assertGreaterEqual(metrics["precision"], 0.80)
        self.assertGreaterEqual(metrics["recall"], 0.60)

    def test_benchmark_fits_scikit_learn_isolation_forest(self):
        _, transactions = load_benchmark_transactions()

        model = fit_anomaly_model(transactions)

        self.assertIsNotNone(model.isolation_forest)

    def test_benchmark_large_payment_rows_have_amount_driver(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        large_rows = scored[scored["row_number"].isin(range(187, 192))]

        flagged = large_rows[large_rows["ml_model_flag"]]

        self.assertFalse(flagged.empty)
        self.assertTrue(
            flagged["ml_anomaly_drivers"]
            .astype(str)
            .str.contains("highest 5%", regex=False)
            .all()
        )

    def test_benchmark_duplicate_rows_have_repeated_invoice_driver(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        duplicate_rows = scored[scored["row_number"].isin(range(192, 197))]

        flagged = duplicate_rows[duplicate_rows["ml_model_flag"]]

        self.assertFalse(flagged.empty)
        self.assertTrue(
            flagged["ml_anomaly_drivers"]
            .astype(str)
            .str.contains("invoice/reference and amount", regex=False)
            .all()
        )

    def test_benchmark_missing_invoice_rows_have_missing_invoice_driver(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        missing_rows = scored[scored["row_number"].isin(range(197, 202))]

        flagged = missing_rows[missing_rows["ml_model_flag"]]

        self.assertFalse(flagged.empty)
        self.assertTrue(
            flagged["ml_anomaly_drivers"]
            .astype(str)
            .str.contains("invoice/reference is missing", regex=False)
            .all()
        )

    def test_deterministic_invoice_conditions_do_not_create_ml_scores_without_a_model(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002"],
                "amount": ["100", "100"],
                "counterparty": ["Vendor One", "Vendor One"],
                "invoice_reference": ["INV-001", "INV-001"],
            }
        )

        scored = score_transaction_anomalies(transactions)

        self.assertEqual([0, 0], scored["ml_anomaly_score"].tolist())
        self.assertEqual([False, False], scored["ml_model_flag"].tolist())

    def test_display_threshold_matches_isolation_forest_flag(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)

        self.assertEqual(
            scored["ml_model_flag"].tolist(),
            scored["ml_anomaly_score"].ge(50).tolist(),
        )
        self.assertTrue(scored.loc[scored["ml_model_flag"], "ml_model_decision"].lt(0).all())

    def test_model_explanation_only_names_features_used_by_the_model(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        explanations = " ".join(scored.loc[scored["ml_model_flag"], "ml_anomaly_drivers"].tolist())

        self.assertNotIn("date", explanations.lower())

    def test_anomaly_scoring_is_deterministic(self):
        _, transactions = load_benchmark_transactions()

        first = score_transaction_anomalies(transactions)
        second = score_transaction_anomalies(transactions)

        pd.testing.assert_frame_equal(
            first[["ml_anomaly_score", "ml_anomaly_band", "ml_anomaly_drivers"]],
            second[["ml_anomaly_score", "ml_anomaly_band", "ml_anomaly_drivers"]],
        )


if __name__ == "__main__":
    unittest.main()
