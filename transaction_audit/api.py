"""HTTP adapter for the review UI. Detection remains in the existing audit engine."""

from __future__ import annotations

import io
import json
from pathlib import Path
from zipfile import BadZipFile

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from transaction_audit.anomaly import score_transaction_anomalies
from transaction_audit.audit import review_findings_dataframe
from transaction_audit.bundle_workflow import maybe_clean_transactions
from transaction_audit.config import ValidationConfig
from transaction_audit.ingestion import load_transactions
from transaction_audit.risk import score_transactions
from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import (
    allowed_mapping_columns,
    apply_mapping_plan,
    build_mapping_plan,
    suggested_mapping_columns,
)
from transaction_audit.datasets import DatasetType


app = FastAPI(title="Transaction Audit Review", version="0.1.0")
MAX_UPLOAD_BYTES = 4_000_000
DEMO_PATH = Path(__file__).resolve().parent.parent / "sample_data" / "reduced_transactions_200_with_anomalies.csv"
FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"


def _records(dataframe: pd.DataFrame) -> list[dict]:
    return json.loads(dataframe.to_json(orient="records", date_format="iso"))


def inspect_data(dataframe: pd.DataFrame, filename: str) -> dict:
    cleaned, cleaning = maybe_clean_transactions(dataframe)
    plan = build_mapping_plan(cleaned)
    return {
        "filename": filename,
        "row_count": len(cleaned),
        "columns": list(cleaned.columns),
        "fields": list(allowed_mapping_columns(DatasetType.TRANSACTIONS)),
        "suggested_columns": suggested_mapping_columns(cleaned),
        "mapping": plan.canonical_to_source,
        "missing_required": plan.auto_schema.missing_required,
        "sample": _records(cleaned.head(3)),
        "cleaning": cleaning.transformations if cleaning else [],
    }


def analyse_data(dataframe: pd.DataFrame, filename: str, mapping: dict[str, str | None], *, demo: bool = False) -> dict:
    cleaned, cleaning = maybe_clean_transactions(dataframe)
    schema = apply_mapping_plan(cleaned, mapping)
    if not schema.has_required_schema:
        raise ValueError("Choose columns for: " + ", ".join(schema.missing_required))

    result = validate_transactions(schema.transactions)
    issues = result.issues_dataframe()
    findings = _records(review_findings_dataframe(issues))
    review = _records(score_transactions(result.transactions, issues))
    model = _records(score_transaction_anomalies(result.transactions))
    findings_by_row: dict[int, list[dict]] = {}
    for finding in findings:
        if finding["row_number"] not in (None, ""):
            findings_by_row.setdefault(int(finding["row_number"]), []).append(finding)

    transactions = []
    cases = []
    for transaction, signals in zip(review, model, strict=True):
        row_number = int(transaction["row_number"])
        row_findings = findings_by_row.get(row_number, [])
        record = {
            "id": f"row-{row_number}",
            "row_number": row_number,
            "transaction": {key: value for key, value in transaction.items() if key in result.transactions.columns},
            "audit_score": transaction["review_priority_score"],
            "audit_band": transaction["review_priority_band"],
            "audit_drivers": transaction["review_priority_drivers"],
            "findings": row_findings,
            "model": {
                "score": signals["ml_anomaly_score"],
                "band": signals["ml_anomaly_band"],
                "flagged": signals["ml_model_flag"],
                "decision": signals["ml_model_decision"],
                "context": signals["ml_anomaly_drivers"],
            },
        }
        transactions.append(record)
        if row_findings or signals["ml_model_flag"]:
            cases.append(record)

    dates = result.transactions["transaction_date"].dropna()
    config = ValidationConfig()
    return {
        "filename": filename,
        "demo": demo,
        "row_count": len(result.transactions),
        "date_start": dates.min().isoformat() if not dates.empty else None,
        "date_end": dates.max().isoformat() if not dates.empty else None,
        "mapping": schema.source_to_canonical,
        "columns": list(result.transactions.columns),
        "cleaning": cleaning.transformations if cleaning else [],
        "large_payment_limit": config.large_amount_threshold,
        "cases": cases,
        "transactions": transactions,
        "finding_count": len(findings),
        "unlinked_findings": [finding for finding in findings if finding["row_number"] in (None, "")],
        "model_available": any(record["model"]["decision"] is not None for record in transactions),
    }


async def _load_upload(file: UploadFile) -> pd.DataFrame:
    filename = file.filename or "transactions.csv"
    if Path(filename).suffix.lower() not in {".csv", ".xlsx"}:
        raise HTTPException(422, "Choose a CSV or .xlsx file.")
    content = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "This demonstration accepts files up to 4 MB.")
    if not content:
        raise HTTPException(422, "The file is empty.")
    try:
        return await run_in_threadpool(load_transactions, io.BytesIO(content), filename=filename)
    except (ValueError, OSError, BadZipFile, pd.errors.ParserError) as error:
        raise HTTPException(422, "The file could not be read. Choose a CSV or .xlsx transaction export.") from error


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/demo")
def demo() -> dict:
    # Labels are evaluation answers, never inputs to detection or fields in the UI.
    source = load_transactions(DEMO_PATH).drop(columns=["is_anomaly", "anomaly_type"], errors="ignore")
    plan = build_mapping_plan(source)
    mapping = {
        **{field: source for field, source in plan.canonical_to_source.items() if source != "description"},
        "counterparty": "description",
    }
    return analyse_data(source, DEMO_PATH.name, mapping, demo=True)


@app.post("/api/inspect")
async def inspect(file: UploadFile = File(...)) -> dict:
    dataframe = await _load_upload(file)
    return await run_in_threadpool(inspect_data, dataframe, file.filename or "transactions.csv")


@app.post("/api/analyse")
async def analyse(file: UploadFile = File(...), mapping: str = Form(...)) -> dict:
    dataframe = await _load_upload(file)
    try:
        selected = json.loads(mapping)
        if not isinstance(selected, dict) or any(
            not isinstance(key, str) or value is not None and not isinstance(value, str)
            for key, value in selected.items()
        ):
            raise ValueError("Choose valid source columns for each field.")
        return await run_in_threadpool(analyse_data, dataframe, file.filename or "transactions.csv", selected)
    except (ValueError, TypeError) as error:
        raise HTTPException(422, str(error)) from error


# API routes take precedence; only compiled browser assets are publicly served.
if (FRONTEND_DIST / "index.html").is_file():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")
