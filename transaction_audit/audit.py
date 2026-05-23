from __future__ import annotations

import io
from datetime import UTC, datetime

import pandas as pd
from openpyxl.utils import get_column_letter

from transaction_audit.datasets import DatasetType
from transaction_audit.schema import SchemaResult
from transaction_audit.types import ValidationResult


FINDING_COLUMNS = [
    "finding_id",
    "priority",
    "review_status",
    "finding",
    "why_it_matters",
    "recommended_action",
    "dataset",
    "row_number",
    "transaction_id",
    "field",
    "related_dataset",
    "related_record_id",
    "evidence",
    "technical_issue_type",
    "review_notes",
]

README_COLUMNS = ["section", "details"]

ISSUE_GUIDANCE = {
    "unknown_vendor_reference": {
        "finding": "Payment references an unapproved or missing vendor",
        "why_it_matters": "The transaction points to a vendor that was not found in the uploaded vendor file.",
        "recommended_action": "Confirm whether the vendor should be added to the approved vendor list or investigate the payment.",
    },
    "unknown_beneficiary_reference": {
        "finding": "Payment references an unknown beneficiary",
        "why_it_matters": "The transaction points to a beneficiary that was not found in the uploaded beneficiary register.",
        "recommended_action": "Confirm the beneficiary identity against the programme register before approving the payment.",
    },
    "duplicate_vendor_reference": {
        "finding": "Duplicate vendor reference in master data",
        "why_it_matters": "Duplicate vendor keys can cause payments to match the wrong supplier record.",
        "recommended_action": "Review the vendor master file and merge, correct, or retire the duplicate record.",
    },
    "duplicate_beneficiary_reference": {
        "finding": "Duplicate beneficiary reference in register",
        "why_it_matters": "Duplicate beneficiary keys can hide overpayments or misdirected cash transfers.",
        "recommended_action": "Review the beneficiary register and confirm which record is valid.",
    },
    "duplicate_transaction_id": {
        "finding": "Duplicate transaction ID",
        "why_it_matters": "The same transaction identifier appears more than once.",
        "recommended_action": "Check whether the records are duplicate exports, corrections, or separate payments incorrectly sharing an ID.",
    },
    "possible_duplicate_payment": {
        "finding": "Possible duplicate payment",
        "why_it_matters": "The payment resembles another transaction by date, amount, and counterparty.",
        "recommended_action": "Compare the matching payments and confirm whether both should exist.",
    },
    "zero_amount": {
        "finding": "Zero amount transaction",
        "why_it_matters": "A zero amount entry may be a reversal, placeholder, or export error.",
        "recommended_action": "Confirm why this transaction is present and whether it should be excluded from audit totals.",
    },
    "missing_required_value": {
        "finding": "Required transaction field is missing",
        "why_it_matters": "The transaction cannot be reviewed reliably without this field.",
        "recommended_action": "Request a corrected export or fill the missing value from source records.",
    },
    "missing_column": {
        "finding": "Required transaction column is missing",
        "why_it_matters": "The uploaded file does not contain a column required by the audit engine.",
        "recommended_action": "Map the correct source column or request a corrected export before relying on the audit result.",
    },
    "missing_value": {
        "finding": "Required transaction field is missing",
        "why_it_matters": "The transaction cannot be reviewed reliably without this field.",
        "recommended_action": "Request a corrected export or fill the missing value from source records.",
    },
    "invalid_amount": {
        "finding": "Amount could not be parsed",
        "why_it_matters": "The amount is not in a format the audit engine can treat as money.",
        "recommended_action": "Correct the source amount format and rerun the audit.",
    },
    "invalid_transaction_date": {
        "finding": "Transaction date could not be parsed",
        "why_it_matters": "The transaction cannot be placed in the correct audit period.",
        "recommended_action": "Correct the date format and rerun the audit.",
    },
    "invalid_date": {
        "finding": "Transaction date could not be parsed",
        "why_it_matters": "The transaction cannot be placed in the correct audit period.",
        "recommended_action": "Correct the date format and rerun the audit.",
    },
    "unsupported_currency": {
        "finding": "Currency is outside the configured review list",
        "why_it_matters": "The transaction uses a currency that may be outside the expected audit scope.",
        "recommended_action": "Confirm whether this currency is valid for the audit period or update the allowed currency configuration.",
    },
    "large_amount": {
        "finding": "Large amount transaction",
        "why_it_matters": "The transaction meets or exceeds the configured large-amount review threshold.",
        "recommended_action": "Confirm approval evidence, supporting documentation, and whether enhanced review is required.",
    },
}


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


def review_findings_dataframe(issues: pd.DataFrame) -> pd.DataFrame:
    if issues.empty:
        return pd.DataFrame(columns=FINDING_COLUMNS)

    rows = []
    sorted_issues = issues.copy()
    sorted_issues["_priority_order"] = sorted_issues["severity"].map({"error": 0, "warning": 1}).fillna(2)
    sorted_issues = sorted_issues.sort_values(["_priority_order", "dataset", "row_number"], kind="stable")

    for position, issue in enumerate(sorted_issues.drop(columns=["_priority_order"]).to_dict("records"), start=1):
        guidance = ISSUE_GUIDANCE.get(
            issue["issue_type"],
            {
                "finding": str(issue["issue_type"]).replace("_", " ").title(),
                "why_it_matters": "This issue may affect audit reliability and should be reviewed.",
                "recommended_action": "Review the source record and document the resolution.",
            },
        )
        rows.append(
            {
                "finding_id": f"F-{position:04d}",
                "priority": priority_for_severity(issue["severity"]),
                "review_status": "Needs review",
                "finding": guidance["finding"],
                "why_it_matters": guidance["why_it_matters"],
                "recommended_action": guidance["recommended_action"],
                "dataset": issue["dataset"],
                "row_number": issue["row_number"],
                "transaction_id": issue["transaction_id"],
                "field": issue["field"],
                "related_dataset": issue["related_dataset"],
                "related_record_id": issue["related_record_id"],
                "evidence": issue["message"],
                "technical_issue_type": issue["issue_type"],
                "review_notes": "",
            }
        )

    return pd.DataFrame(rows, columns=FINDING_COLUMNS)


def finding_summary_dataframe(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return pd.DataFrame(columns=["priority", "finding", "count"])

    priority_order = {"High": 0, "Medium": 1, "Low": 2}
    summary = (
        findings.groupby(["priority", "finding"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    summary["_priority_order"] = summary["priority"].map(priority_order).fillna(99)
    return summary.sort_values(["_priority_order", "finding"]).drop(columns=["_priority_order"])


def priority_for_severity(severity: object) -> str:
    if str(severity).lower() == "error":
        return "High"
    if str(severity).lower() == "warning":
        return "Medium"
    return "Low"


def audit_readme_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "section": "How to use this workbook",
                "details": "Start with review_findings. Each row explains the issue, why it matters, and the recommended review action.",
            },
            {
                "section": "Review status",
                "details": "Use the review_status column to track whether each finding is open, resolved, accepted, or needs follow-up.",
            },
            {
                "section": "Raw evidence",
                "details": "Use raw_issues and the input dataset sheets only when you need the underlying technical detail.",
            },
        ],
        columns=README_COLUMNS,
    )


def build_audit_workbook(
    result: ValidationResult,
    schema_result: SchemaResult,
    summary: dict[str, object],
) -> bytes:
    output = io.BytesIO()
    issues = result.issues_dataframe()
    findings = review_findings_dataframe(issues)
    sheets = {
        "summary": summary_dataframe(summary),
        "review_findings": findings,
        "finding_summary": finding_summary_dataframe(findings),
        "raw_issues": issues,
        "issue_counts": issue_counts_dataframe(result),
        "schema_mapping": schema_result.mapping_dataframe(),
        "transactions": result.transactions,
    }

    write_workbook(output, sheets)
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
    findings = review_findings_dataframe(all_issues)
    sheets = {
        "read_me": audit_readme_dataframe(),
        "summary": summary_dataframe(build_bundle_audit_summary(bundle_result)),
        "review_findings": findings,
        "finding_summary": finding_summary_dataframe(findings),
        "dataset_summary": bundle_result.bundle.dataset_summary(),
        "issue_counts": issue_counts_from_dataframe(all_issues),
        "transaction_issues": bundle_result.transaction_issues_dataframe(),
        "reference_issues": bundle_result.reference_issues_dataframe(),
        "raw_issues": all_issues,
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

    write_workbook(output, sheets)
    return output.getvalue()


def write_workbook(output: io.BytesIO, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, dataframe in sheets.items():
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
            format_worksheet(writer.sheets[sheet_name], dataframe)


def format_worksheet(worksheet, dataframe: pd.DataFrame) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for index, column in enumerate(dataframe.columns, start=1):
        values = dataframe[column].dropna().astype(str).head(100).tolist() if column in dataframe else []
        longest_value = max([len(str(column)), *(len(value) for value in values)], default=len(str(column)))
        width = min(max(longest_value + 2, 12), 60)
        worksheet.column_dimensions[get_column_letter(index)].width = width
