import io
import unittest

from transaction_audit.ingestion import load_transactions


class IngestionTests(unittest.TestCase):
    def test_csv_preserves_raw_strings_and_utf8_bom_header(self):
        content = "\ufefftransaction_id,amount\nTXN-001,\"£1,500\"\n"
        source = io.BytesIO(content.encode("utf-8-sig"))

        dataframe = load_transactions(source, filename="transactions.csv")

        self.assertEqual(["transaction_id", "amount"], list(dataframe.columns))
        self.assertEqual("£1,500", dataframe.loc[0, "amount"])

    def test_csv_blank_cells_are_preserved_as_empty_strings(self):
        source = io.StringIO("transaction_id,amount\nTXN-001,\n")

        dataframe = load_transactions(source, filename="transactions.csv")

        self.assertEqual("", dataframe.loc[0, "amount"])
