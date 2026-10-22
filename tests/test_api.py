import io
import json
import unittest
from importlib.util import find_spec

import pandas as pd

if not all(find_spec(package) for package in ("fastapi", "httpx", "multipart")):
    raise unittest.SkipTest("Install requirements.txt to test the React API.")

from fastapi.testclient import TestClient

from transaction_audit.api import FRONTEND_DIST, MAX_UPLOAD_BYTES, app, analyse_data, demo
from transaction_audit.validation import validate_transactions


class ReviewApiTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.dataframe = pd.DataFrame({
            "transaction_id": ["A-1", "A-2"],
            "transaction_date": ["2025-01-01", "2025-01-08"],
            "account_id": ["OPS", "OPS"],
            "counterparty": ["Supplier A", "Supplier A"],
            "currency": ["GBP", "GBP"],
            "amount": ["2500", "2480"],
            "invoice_reference": ["INV-004582", "inv 4582"],
            "invoice_amount": ["2500", "2500"],
        })
        self.mapping = {column: column for column in self.dataframe.columns}

    def test_adapter_preserves_findings_and_keeps_model_separate(self):
        payload = analyse_data(self.dataframe, "payments.csv", self.mapping)
        expected = validate_transactions(self.dataframe).issues_dataframe()

        self.assertEqual(len(expected), payload["finding_count"])
        self.assertEqual(2, len(payload["cases"]))
        self.assertFalse(payload["model_available"])
        for case in payload["cases"]:
            self.assertEqual("high", case["audit_band"])
            self.assertFalse(case["model"]["flagged"])
            self.assertIsNone(case["model"]["decision"])
            self.assertTrue(case["findings"][0]["related_record_id"])

    def test_upload_inspection_and_manual_mapping(self):
        source = self.dataframe.rename(columns={"counterparty": "Paid to"})
        contents = source.to_csv(index=False).encode()
        inspected = self.client.post("/api/inspect", files={"file": ("payments.csv", contents, "text/csv")})
        self.assertEqual(200, inspected.status_code)
        self.assertIn("counterparty", inspected.json()["missing_required"])
        self.assertNotIn(
            "transaction_date",
            inspected.json()["suggested_columns"]["currency"],
        )

        mapping = {**self.mapping, "counterparty": "Paid to"}
        response = self.client.post("/api/analyse", files={"file": ("payments.csv", contents, "text/csv")}, data={"mapping": json.dumps(mapping)})
        self.assertEqual(200, response.status_code)
        self.assertEqual(2, len(response.json()["cases"]))
        self.assertEqual("Supplier A", response.json()["cases"][0]["transaction"]["counterparty"])

    def test_invalid_mapping_and_unsupported_file_are_actionable(self):
        contents = self.dataframe.to_csv(index=False).encode()
        response = self.client.post("/api/analyse", files={"file": ("payments.csv", contents)}, data={"mapping": "[]"})
        self.assertEqual(422, response.status_code)
        unsupported = self.client.post("/api/inspect", files={"file": ("payments.xls", contents)})
        self.assertEqual(422, unsupported.status_code)

    def test_upload_larger_than_host_limit_is_rejected(self):
        response = self.client.post(
            "/api/inspect",
            files={"file": ("payments.csv", b"x" * (MAX_UPLOAD_BYTES + 1), "text/csv")},
        )
        self.assertEqual(413, response.status_code)
        self.assertIn("4 MB", response.json()["detail"])

    def test_xlsx_upload_and_json_null_values(self):
        workbook = io.BytesIO()
        self.dataframe.assign(invoice_reference=[None, "INV-2"]).to_excel(workbook, index=False)
        response = self.client.post("/api/analyse", files={"file": ("payments.xlsx", workbook.getvalue())}, data={"mapping": json.dumps(self.mapping)})
        self.assertEqual(200, response.status_code)
        self.assertEqual(2, response.json()["row_count"])

    def test_demo_uses_real_engine_outputs_without_ground_truth(self):
        payload = demo()
        self.assertTrue(payload["demo"])
        self.assertEqual(200, payload["row_count"])
        self.assertTrue(payload["model_available"])
        self.assertNotIn("is_anomaly", payload["columns"])
        self.assertNotIn("anomaly_type", payload["columns"])
        self.assertTrue(payload["cases"])
        for case in payload["cases"]:
            self.assertTrue(case["findings"] or case["model"]["flagged"])
            self.assertNotIn("is_anomaly", case["transaction"])

    @unittest.skipUnless((FRONTEND_DIST / "index.html").is_file(), "Build frontend assets to test combined serving.")
    def test_combined_service_serves_ui_assets_and_preserves_api_routes(self):
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn('<div id="root">', response.text)
        asset = next((FRONTEND_DIST / "assets").glob("*.js"))
        self.assertEqual(200, self.client.get(f"/assets/{asset.name}").status_code)
        self.assertEqual({"status": "ok"}, self.client.get("/api/health").json())
        self.assertEqual(404, self.client.get("/api/not-an-endpoint").status_code)
        self.assertEqual(404, self.client.get("/requirements.txt").status_code)
