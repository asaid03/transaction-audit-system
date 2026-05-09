import tempfile
import unittest
from pathlib import Path

from transaction_audit.profiles import (
    DEFAULT_PROFILES_DIR,
    list_import_profiles,
    save_import_profile,
    validate_profile_mapping,
)


VALID_MAPPING = {
    "txn_ref": "transaction_id",
    "date": "transaction_date",
    "acc_id": "account_id",
    "amt": "amount",
    "ccy": "currency",
    "counterparty_name": "counterparty",
}


class ProfileTests(unittest.TestCase):
    def test_default_profiles_dir_is_project_root_relative(self):
        self.assertEqual("import_profiles", DEFAULT_PROFILES_DIR.name)
        self.assertEqual("transaction-audit-system", DEFAULT_PROFILES_DIR.parent.name)

    def test_save_and_load_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            profile = save_import_profile(
                "ERP Export",
                VALID_MAPPING,
                profiles_dir=Path(directory),
            )
            loaded = list_import_profiles(Path(directory))

        self.assertEqual("erp-export", profile.profile_id)
        self.assertEqual(1, len(loaded))
        self.assertTrue(loaded[0].complete_required_fields)

    def test_rejects_duplicate_canonical_targets(self):
        bad_mapping = dict(VALID_MAPPING)
        bad_mapping["duplicate_amount"] = "amount"

        with self.assertRaisesRegex(ValueError, "same field"):
            validate_profile_mapping(bad_mapping)

    def test_rejects_missing_required_fields(self):
        bad_mapping = dict(VALID_MAPPING)
        del bad_mapping["amt"]

        with self.assertRaisesRegex(ValueError, "missing required"):
            validate_profile_mapping(bad_mapping)
