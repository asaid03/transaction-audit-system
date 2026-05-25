import unittest

import pandas as pd

from transaction_audit.datasets import DatasetType
from transaction_audit.schema import apply_schema, normalize_column_key, source_mapping_from_canonical
from transaction_audit.workflow import aliases_for_dataset, allowed_mapping_columns, build_mapping_plan, required_mapping_columns


class SchemaTests(unittest.TestCase):
    def test_infers_common_aliases(self):
        dataframe = pd.DataFrame(
            columns=["txn_ref", "date", "acc_id", "amt", "ccy", "counterparty_name"]
        )

        result = apply_schema(dataframe)

        self.assertEqual(
            {
                "txn_ref": "transaction_id",
                "date": "transaction_date",
                "acc_id": "account_id",
                "amt": "amount",
                "ccy": "currency",
                "counterparty_name": "counterparty",
            },
            result.source_to_canonical,
        )

    def test_normalizes_real_bank_export_column_names(self):
        self.assertEqual("debit", normalize_column_key("Debit (USD)"))
        self.assertEqual("credit", normalize_column_key("Credit (USD)"))
        self.assertEqual("description", normalize_column_key("Description / Narration"))

    def test_explicit_empty_mapping_does_not_fall_back_to_auto_detect(self):
        dataframe = pd.DataFrame(columns=["txn_ref", "amt"])

        result = apply_schema(dataframe, source_to_canonical={})

        self.assertEqual({}, result.source_to_canonical)
        self.assertIn("transaction_id", result.missing_required)

    def test_rejects_source_column_mapped_to_multiple_fields(self):
        dataframe = pd.DataFrame(columns=["txn_ref"])

        with self.assertRaisesRegex(ValueError, "Duplicate mapping"):
            source_mapping_from_canonical(
                dataframe,
                {
                    "transaction_id": "txn_ref",
                    "amount": "txn_ref",
                },
            )

    def test_rejects_mapping_that_creates_duplicate_canonical_columns(self):
        dataframe = pd.DataFrame(columns=["transaction_id", "date", "acc_id", "amount", "amt", "ccy", "vendor"])

        with self.assertRaisesRegex(ValueError, "duplicate canonical columns: amount"):
            apply_schema(dataframe, source_to_canonical={"amt": "amount"})

    def test_mapping_plan_reports_missing_profile_sources(self):
        dataframe = pd.DataFrame(columns=["txn_ref", "date", "acc_id", "amt", "ccy"])

        class Profile:
            source_to_canonical = {
                "txn_ref": "transaction_id",
                "missing_counterparty": "counterparty",
            }

        plan = build_mapping_plan(dataframe, Profile())

        self.assertEqual(["missing_counterparty"], plan.missing_profile_sources)
        self.assertEqual("txn_ref", plan.canonical_to_source["transaction_id"])

    def test_vendor_schema_uses_vendor_specific_aliases(self):
        dataframe = pd.DataFrame(columns=["Supplier Code", "Supplier"])

        result = apply_schema(
            dataframe,
            aliases=aliases_for_dataset(DatasetType.VENDORS),
            required_columns=required_mapping_columns(DatasetType.VENDORS),
            allowed_columns=allowed_mapping_columns(DatasetType.VENDORS),
        )

        self.assertTrue(result.has_required_schema)
        self.assertEqual(
            {
                "Supplier Code": "vendor_id",
                "Supplier": "vendor_name",
            },
            result.source_to_canonical,
        )

    def test_beneficiary_mapping_plan_uses_beneficiary_required_fields(self):
        dataframe = pd.DataFrame(columns=["Recipient Code", "Recipient"])

        plan = build_mapping_plan(dataframe, dataset_type=DatasetType.BENEFICIARIES)

        self.assertTrue(plan.auto_schema.has_required_schema)
        self.assertEqual("Recipient Code", plan.canonical_to_source["beneficiary_id"])
        self.assertEqual("Recipient", plan.canonical_to_source["beneficiary_name"])

    def test_transaction_mapping_can_include_reference_columns(self):
        dataframe = pd.DataFrame(
            columns=[
                "txn_ref",
                "date",
                "acc_id",
                "amt",
                "ccy",
                "counterparty_name",
                "supplier_code",
                "recipient_code",
            ]
        )

        plan = build_mapping_plan(dataframe, dataset_type=DatasetType.TRANSACTIONS)

        self.assertEqual("supplier_code", plan.canonical_to_source["vendor_id"])
        self.assertEqual("recipient_code", plan.canonical_to_source["beneficiary_id"])
