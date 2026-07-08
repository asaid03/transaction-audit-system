import unittest

import pandas as pd

from transaction_audit.narration import extract_counterparties_from_narration, extract_counterparty_from_narration


class NarrationTests(unittest.TestCase):
    def test_extracts_vendor_from_payment_narration(self):
        self.assertEqual(
            "SOMALI MED SUPPLIES",
            extract_counterparty_from_narration("PAYMENT TO SOMALI MED SUPPLIES INV 2026-04"),
        )

    def test_extracts_vendor_from_paybill_narration(self):
        self.assertEqual(
            "HORMUUD TELECOM",
            extract_counterparty_from_narration("MPESA PAYBILL 123456 HORMUUD TELECOM REF 789"),
        )

    def test_extracts_beneficiary_name_before_reference(self):
        self.assertEqual(
            "ASHA MOHAMED",
            extract_counterparty_from_narration("CASH TRANSFER ASHA MOHAMED BEN001"),
        )

    def test_preserves_unmatched_narration(self):
        self.assertEqual(
            "BANK CHARGE",
            extract_counterparty_from_narration("BANK CHARGE MAY 2026"),
        )

    def test_vectorized_extraction(self):
        result = extract_counterparties_from_narration(
            pd.Series(["SUPPLIER PMT LOGISTICS PARTNER REF INV-900", ""])
        )

        self.assertEqual(["LOGISTICS PARTNER", ""], result.tolist())


if __name__ == "__main__":
    unittest.main()
