from __future__ import annotations

import io
from datetime import UTC, datetime

import pandas as pd

from transaction_audit.datasets import DatasetType
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
    return issue_counts_from_dataframe(result.issues_dataframe())


def issue_counts_from_dataframe(issues: pd.DataFrame) -> pd.DataFrame:
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


def build_bundle_audit_summary(bundle_result) -> dict[str, object]:
    all_issues = bundle_result.all_issues_dataframe()
    severity_counts = (
        all_issues["severity"].value_counts().to_dict()
        if not all_issues.empty
        else {}
    )

    return {
        "run_timestamp_utc": datetime.now(UTC).isoformat(),
        "transactions": bundle_result.bundle.transactions.row_count,
        "reference_datasets_uploaded": len(bundle_result.bundle.reference_datasets()),
        "issues_found": int(len(all_issues)),
        "transaction_issues": int(len(bundle_result.transaction_validation.issues)),
        "reference_issues": int(len(bundle_result.reference_reconciliation.issues)),
        "skipped_checks": int(len(bundle_result.reference_reconciliation.skipped_checks)),
        "error_count": int(severity_counts.get("error", 0)),
        "warning_count": int(severity_counts.get("warning", 0)),
        "passed": int(severity_counts.get("error", 0)) == 0,
    }


def schema_mappings_dataframe(schema_results: dict[DatasetType, SchemaResult]) -> pd.DataFrame:
    frames = []
    for dataset_type, schema_result in schema_results.items():
        mapping = schema_result.mapping_dataframe()
        mapping.insert(0, "dataset", dataset_type.value)
        mapping.insert(1, "missing_required", ", ".join(schema_result.missing_required))
        frames.append(mapping)

    if not frames:
        return pd.DataFrame(columns=["dataset", "missing_required", "source_column", "canonical_field"])
    return pd.concat(frames, ignore_index=True)


def build_bundle_audit_workbook(
    bundle_result,
    schema_results: dict[DatasetType, SchemaResult],
) -> bytes:
    output = io.BytesIO()
    all_issues = bundle_result.all_issues_dataframe()
    sheets = {
        "summary": summary_dataframe(build_bundle_audit_summary(bundle_result)),
        "dataset_summary": bundle_result.bundle.dataset_summary(),
        "all_issues": all_issues,
        "issue_counts": issue_counts_from_dataframe(all_issues),
        "transaction_issues": bundle_result.transaction_issues_dataframe(),
        "reference_issues": bundle_result.reference_issues_dataframe(),
        "skipped_checks": bundle_result.skipped_checks_dataframe(),
        "schema_mappings": schema_mappings_dataframe(schema_results),
        "transactions": bundle_result.bundle.transactions.dataframe,
    }

    if bundle_result.bundle.vendors is not None:
        sheets["vendors"] = bundle_result.bundle.vendors.dataframe
    if bundle_result.bundle.beneficiaries is not None:
        sheets["beneficiaries"] = bundle_result.bundle.beneficiaries.dataframe
    if bundle_result.bundle.budget is not None:
        sheets["budget"] = bundle_result.bundle.budget.dataframe

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, dataframe in sheets.items():
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)

    return output.getvalue()
