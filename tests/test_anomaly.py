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
    def test_benchmark_model_detects_labelled_anomaly_rows(self):
        raw, transactions = load_benchmark_transactions()

        model = fit_anomaly_model(transactions)
        scored = score_transaction_anomalies(transactions, model)
        metrics = evaluate_anomaly_detection(scored, raw["is_anomaly"])

        self.assertEqual(15, metrics["true_positive"])
        self.assertEqual(0, metrics["false_negative"])
        self.assertGreaterEqual(metrics["precision"], 0.70)
        self.assertEqual(1.0, metrics["recall"])

    def test_benchmark_fits_scikit_learn_isolation_forest(self):
        _, transactions = load_benchmark_transactions()

        model = fit_anomaly_model(transactions)

        self.assertIsNotNone(model.isolation_forest)

    def test_benchmark_large_payment_rows_have_amount_driver(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        large_rows = scored[scored["row_number"].isin(range(187, 192))]

        self.assertTrue(large_rows["ml_anomaly_score"].ge(70).all())
        self.assertTrue(
            large_rows["ml_anomaly_drivers"]
            .astype(str)
            .str.contains("trained high-amount threshold", regex=False)
            .all()
        )

    def test_benchmark_duplicate_rows_have_repeated_invoice_driver(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        duplicate_rows = scored[scored["row_number"].isin(range(192, 197))]

        self.assertTrue(duplicate_rows["ml_anomaly_score"].ge(85).all())
        self.assertTrue(
            duplicate_rows["ml_anomaly_drivers"]
            .astype(str)
            .str.contains("Invoice/reference repeats", regex=False)
            .all()
        )

    def test_benchmark_missing_invoice_rows_have_missing_invoice_driver(self):
        _, transactions = load_benchmark_transactions()

        scored = score_transaction_anomalies(transactions)
        missing_rows = scored[scored["row_number"].isin(range(197, 202))]

        self.assertTrue(missing_rows["ml_anomaly_score"].ge(65).all())
        self.assertTrue(
            missing_rows["ml_anomaly_drivers"]
            .astype(str)
            .str.contains("Invoice/reference is missing", regex=False)
            .all()
        )

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
