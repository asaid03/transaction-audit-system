import tempfile
import unittest
from pathlib import Path

from transaction_audit.profiles import (
    DEFAULT_PROFILES_DIR,
    delete_import_profile,
    list_import_profiles,
    profiles_dataframe,
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

    def test_profiles_dataframe_contains_review_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            save_import_profile("ERP Export", VALID_MAPPING, profiles_dir=Path(directory))
            profiles = list_import_profiles(Path(directory))

        dataframe = profiles_dataframe(profiles)

        self.assertEqual(["profile_id", "display_name", "description"], list(dataframe.columns[:3]))
        self.assertEqual("erp-export", dataframe.loc[0, "profile_id"])

    def test_delete_import_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            profile = save_import_profile("ERP Export", VALID_MAPPING, profiles_dir=Path(directory))
            deleted = delete_import_profile(profile.profile_id, profiles_dir=Path(directory))
            profiles = list_import_profiles(Path(directory))

        self.assertTrue(deleted)
        self.assertEqual([], profiles)

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
