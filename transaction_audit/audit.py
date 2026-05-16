from __future__ import annotations

import io
from datetime import UTC, datetime

import pandas as pd

from transaction_audit.schema import SchemaResult
from transaction_audit.types import ValidationResult


def build_audit_summary(result: ValidationResult, source_name: str) -> dict[str, object]:
    issue_frame = result.issues_dataframe()
    severity_counts = (
        issue_frame["severity"].value_counts().to_dict()
        if not issue_frame.empty
        else {}
    )

    return {
        "source_name": source_name,
        "run_timestamp_utc": datetime.now(UTC).isoformat(),
        "rows_checked": int(len(result.transactions)),
        "issues_found": int(len(result.issues)),
        "error_count": int(severity_counts.get("error", 0)),
        "warning_count": int(severity_counts.get("warning", 0)),
        "passed": result.is_valid,
    }


def summary_dataframe(summary: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame([summary])


def issue_counts_dataframe(result: ValidationResult) -> pd.DataFrame:
    issues = result.issues_dataframe()
    if issues.empty:
        return pd.DataFrame(columns=["issue_type", "severity", "count"])

    return (
        issues.groupby(["issue_type", "severity"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["severity", "issue_type"])
    )


def build_audit_workbook(
    result: ValidationResult,
    schema_result: SchemaResult,
    summary: dict[str, object],
) -> bytes:
    output = io.BytesIO()
    sheets = {
        "summary": summary_dataframe(summary),
        "issues": result.issues_dataframe(),
        "issue_counts": issue_counts_dataframe(result),
        "schema_mapping": schema_result.mapping_dataframe(),
        "transactions": result.transactions,
    }

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, dataframe in sheets.items():
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)

    return output.getvalue()
