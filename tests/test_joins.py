import unittest

import pandas as pd

from transaction_audit.joins import AUDIT_ROW_NUMBER, normalize_join_key, safe_left_join, unmatched_left_rows


class JoinTests(unittest.TestCase):
    def test_normalize_join_key_strips_case_and_extra_spaces(self):
        series = pd.Series(["  Vendor  A  ", "VENDOR a", "", None])

        normalized = normalize_join_key(series)

        self.assertEqual(["vendor a", "vendor a", pd.NA, pd.NA], normalized.tolist())

    def test_safe_left_join_preserves_source_row_numbers_and_merge_status(self):
        transactions = pd.DataFrame({"vendor_id": [" VEN-1 ", "VEN-404"]})
        vendors = pd.DataFrame({"vendor_id": ["ven-1"], "vendor_name": ["Vendor One"]})

        joined = safe_left_join(
            transactions,
            vendors,
            left_on="vendor_id",
            right_on="vendor_id",
            right_prefix="vendor",
        )

        self.assertEqual([2, 3], joined[AUDIT_ROW_NUMBER].tolist())
        self.assertEqual(["both", "left_only"], joined["_merge"].astype(str).tolist())

    def test_safe_left_join_uses_source_file_row_numbers_when_available(self):
        transactions = pd.DataFrame(
            {"vendor_id": ["VEN-1", "VEN-404"], "source_row_number": [8, 11]}
        )
        vendors = pd.DataFrame({"vendor_id": ["VEN-1"]})

        joined = safe_left_join(
            transactions,
            vendors,
            left_on="vendor_id",
            right_on="vendor_id",
            right_prefix="vendor",
        )

        self.assertEqual([8, 11], joined[AUDIT_ROW_NUMBER].tolist())

    def test_unmatched_left_rows_returns_missing_references(self):
        transactions = pd.DataFrame({"vendor_id": ["VEN-1", "VEN-404"]})
        vendors = pd.DataFrame({"vendor_id": ["VEN-1"]})
        joined = safe_left_join(
            transactions,
            vendors,
            left_on="vendor_id",
            right_on="vendor_id",
            right_prefix="vendor",
        )

        unmatched = unmatched_left_rows(joined)

        self.assertEqual(["VEN-404"], unmatched["vendor_id"].tolist())

    def test_safe_left_join_raises_on_duplicate_reference_keys(self):
        transactions = pd.DataFrame({"vendor_id": ["VEN-1"]})
        vendors = pd.DataFrame({"vendor_id": ["VEN-1", "VEN-1"]})

        with self.assertRaises(pd.errors.MergeError):
            safe_left_join(
                transactions,
                vendors,
                left_on="vendor_id",
                right_on="vendor_id",
                right_prefix="vendor",
            )

    def test_safe_left_join_rejects_reserved_internal_columns(self):
        transactions = pd.DataFrame({"vendor_id": ["VEN-1"], "_merge": ["both"]})
        vendors = pd.DataFrame({"vendor_id": ["VEN-1"]})

        with self.assertRaisesRegex(ValueError, "reserved audit join columns"):
            safe_left_join(
                transactions,
                vendors,
                left_on="vendor_id",
                right_on="vendor_id",
                right_prefix="vendor",
            )
