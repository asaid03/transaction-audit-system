from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

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
