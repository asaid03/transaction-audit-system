import io
import unittest

from transaction_audit.ingestion import load_dataset, load_transactions


class IngestionTests(unittest.TestCase):
    def test_csv_preserves_raw_strings_and_utf8_bom_header(self):
        content = "\ufefftransaction_id,amount\nTXN-001,\"GBP 1,500\"\n"
        source = io.BytesIO(content.encode("utf-8-sig"))

        dataframe = load_dataset(source, filename="transactions.csv")

        self.assertEqual(["transaction_id", "amount"], list(dataframe.columns))
        self.assertEqual("GBP 1,500", dataframe.loc[0, "amount"])

    def test_csv_blank_cells_are_preserved_as_empty_strings(self):
        source = io.StringIO("transaction_id,amount\nTXN-001,\n")

        dataframe = load_dataset(source, filename="transactions.csv")

        self.assertEqual("", dataframe.loc[0, "amount"])

    def test_load_transactions_remains_compatible_alias(self):
        source = io.StringIO("transaction_id,amount\nTXN-001,10\n")

        dataframe = load_transactions(source, filename="transactions.csv")

        self.assertEqual("TXN-001", dataframe.loc[0, "transaction_id"])

    def test_ragged_csv_falls_back_to_raw_rows(self):
        source = io.StringIO(
            "SALAAM BANK EXPORT\n"
            "Ref No,Value Date,Debit,Credit,Balance,Narration\n"
            "TXN-001,01-May-26,10,,990,Payment\n"
        )

        dataframe = load_dataset(source, filename="transactions.csv")

        self.assertIn("Ref No", dataframe.iloc[0].tolist())
