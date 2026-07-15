import unittest

import pandas as pd

from transaction_audit.config import ValidationConfig
from transaction_audit.population import (
    check_running_balance_integrity,
    check_vendor_concentration,
)


class RunningBalanceIntegrityTests(unittest.TestCase):
    def test_flags_balance_movement_that_does_not_match_transaction_amount(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 8)],
                "amount": [100.0, 50.0, 25.0, 40.0, 10.0, 20.0, 30.0],
                "balance": [900.0, 850.0, 825.0, 700.0, 690.0, 670.0, 640.0],
            }
        )

        issues = check_running_balance_integrity(transactions, ValidationConfig())

        self.assertEqual(1, len(issues))
        self.assertEqual("balance_movement_mismatch", issues[0].issue_type)
        self.assertEqual(5, issues[0].row_number)
        self.assertIn("expected to be 785.00", issues[0].message)
        self.assertIn("reports 700.00", issues[0].message)

    def test_supports_statements_where_positive_amounts_increase_balance(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 7)],
                "amount": [100.0, 50.0, 25.0, 40.0, 10.0, 20.0],
                "balance": [1100.0, 1150.0, 1175.0, 1215.0, 1225.0, 1245.0],
            }
        )

        self.assertEqual([], check_running_balance_integrity(transactions, ValidationConfig()))

    def test_does_not_guess_when_balance_direction_is_ambiguous(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 7)],
                "amount": [100.0, 50.0, 25.0, 40.0, 10.0, 20.0],
                "balance": [900.0, 950.0, 925.0, 965.0, 955.0, 975.0],
            }
        )

        self.assertEqual([], check_running_balance_integrity(transactions, ValidationConfig()))

    def test_skips_check_without_enough_valid_balance_rows(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": ["TXN-001", "TXN-002", "TXN-003"],
                "amount": [100.0, 50.0, 25.0],
                "balance": [900.0, 850.0, 825.0],
            }
        )

        self.assertEqual([], check_running_balance_integrity(transactions, ValidationConfig()))


class VendorConcentrationTests(unittest.TestCase):
    def test_flags_vendor_above_concentration_threshold(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 21)],
                "counterparty": ["Vendor A"] * 10 + ["Vendor B"] * 5 + ["Vendor C"] * 5,
                "amount": [100.0] * 10 + [40.0] * 10,
                "currency": ["USD"] * 20,
            }
        )

        issues = check_vendor_concentration(transactions, ValidationConfig())

        self.assertEqual(1, len(issues))
        self.assertEqual("vendor_concentration", issues[0].issue_type)
        self.assertEqual("Vendor A", issues[0].related_record_id)
        self.assertIn("71.4%", issues[0].message)
        self.assertIn("USD", issues[0].message)

    def test_calculates_concentration_separately_by_currency(self):
        transactions = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 41)],
                "counterparty": (
                    ["Vendor A"] * 10
                    + ["Vendor B"] * 5
                    + ["Vendor C"] * 5
                    + ["Vendor D"] * 10
                    + ["Vendor E"] * 5
                    + ["Vendor F"] * 5
                ),
                "amount": [30.0] * 40,
                "currency": ["USD"] * 20 + ["EUR"] * 20,
            }
        )

        issues = check_vendor_concentration(transactions, ValidationConfig())

        self.assertEqual({"Vendor A", "Vendor D"}, {issue.related_record_id for issue in issues})
        self.assertTrue(any("USD" in issue.message for issue in issues))
        self.assertTrue(any("EUR" in issue.message for issue in issues))

    def test_skips_small_or_low_variety_populations(self):
        small = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 10)],
                "counterparty": ["Vendor A"] * 5 + ["Vendor B"] * 2 + ["Vendor C"] * 2,
                "amount": [100.0] * 9,
                "currency": ["USD"] * 9,
            }
        )
        low_variety = pd.DataFrame(
            {
                "transaction_id": [f"TXN-{index:03d}" for index in range(1, 21)],
                "counterparty": ["Vendor A"] * 15 + ["Vendor B"] * 5,
                "amount": [100.0] * 20,
                "currency": ["USD"] * 20,
            }
        )

        self.assertEqual([], check_vendor_concentration(small, ValidationConfig()))
        self.assertEqual([], check_vendor_concentration(low_variety, ValidationConfig()))


if __name__ == "__main__":
    unittest.main()
