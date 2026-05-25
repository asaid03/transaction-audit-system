import unittest

import pandas as pd

from transaction_audit.cleaning import clean_transaction_export
from transaction_audit.ingestion import load_dataset
from transaction_audit.validation import validate_transactions


class CleaningTests(unittest.TestCase):
    def test_cleans_realistic_bank_export(self):
        raw = load_dataset("sample_data/realistic_bank_export.csv")

        result = clean_transaction_export(raw)
        cleaned = result.dataframe

        self.assertEqual(4, len(cleaned))
        self.assertEqual("TRF/2026/001", cleaned.loc[0, "transaction_id"])
        self.assertEqual("generated-000004", cleaned.loc[3, "transaction_id"])
        self.assertEqual(["450.00", "120.00", "-25.00", "75.00"], cleaned["amount"].tolist())
        self.assertEqual(["USD", "USD", "USD", "USD"], cleaned["currency"].tolist())
        self.assertEqual(["1234567890"] * 4, cleaned["account_id"].tolist())
        self.assertEqual([6, 7, 8, 9], cleaned["source_row_number"].tolist())
        self.assertIn("Detected transaction header on source row 5.", result.transformations)
        self.assertIn("Derived amount from debit/credit columns.", result.transformations)
        self.assertTrue(validate_transactions(cleaned).is_valid)

    def test_cleans_realistic_mobile_money_export(self):
        raw = load_dataset("sample_data/realistic_mobile_money_export.csv")

        result = clean_transaction_export(raw)
        cleaned = result.dataframe

        self.assertEqual(3, len(cleaned))
        self.assertEqual(["EVC3X7K9Q", "EVC3X7KAA", "generated-000003"], cleaned["transaction_id"].tolist())
        self.assertEqual(["120.00", "-50.00", "15.50"], cleaned["amount"].tolist())
        self.assertEqual(["USD", "USD", "USD"], cleaned["currency"].tolist())
        self.assertEqual(["+252610000000"] * 3, cleaned["account_id"].tolist())
        self.assertEqual([6, 7, 8], cleaned["source_row_number"].tolist())
        self.assertTrue(validate_transactions(cleaned).is_valid)

    def test_cleans_realistic_ledger_export(self):
        raw = load_dataset("sample_data/realistic_ledger_export.csv")

        result = clean_transaction_export(raw)
        cleaned = result.dataframe

        self.assertEqual(3, len(cleaned))
        self.assertEqual(
            ["generated-000001", "generated-000002", "generated-000003"],
            cleaned["transaction_id"].tolist(),
        )
        self.assertEqual(["1250.00", "-75.00", "300.00"], cleaned["amount"].tolist())
        self.assertEqual(["FIELD-OPS", "FIELD-OPS", "OPERATIONS"], cleaned["account_id"].tolist())
        self.assertEqual([6, 7, 8], cleaned["source_row_number"].tolist())
        self.assertTrue(validate_transactions(cleaned).is_valid)

    def test_detects_headers_with_currency_suffixes_and_compound_names(self):
        raw = pd.DataFrame(
            [
                ["Account: 1234567890 - USD CURRENT ACCOUNT", "", "", "", "", ""],
                ["Ref No", "Value Date", "Debit (USD)", "Credit (USD)", "Balance", "Description / Narration"],
                ["TRF/001", "01-May-26", "100.00", "", "900.00", "PAYMENT TO SUPPLIER"],
            ],
            columns=["SALAAM BANK", "", "", "", "", ""],
        )

        result = clean_transaction_export(raw)
        cleaned = result.dataframe

        self.assertTrue(result.applied)
        self.assertEqual("TRF/001", cleaned.loc[0, "transaction_id"])
        self.assertEqual("100.00", cleaned.loc[0, "amount"])
        self.assertEqual("PAYMENT TO SUPPLIER", cleaned.loc[0, "counterparty"])

    def test_returns_warning_when_no_transaction_header_is_found(self):
        raw = load_dataset("sample_data/phase2_vendors.csv")

        result = clean_transaction_export(raw)

        self.assertFalse(result.applied)
        self.assertIn("No transaction-like header row was found.", result.warnings)


if __name__ == "__main__":
    unittest.main()
