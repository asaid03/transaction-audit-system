from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.audit import (
    build_audit_workbook,
    build_bundle_audit_workbook,
    issue_counts_dataframe,
    review_findings_dataframe,
    summary_dataframe,
)
from transaction_audit.bundle_workflow import BundleAuditResult
from transaction_audit.review import sort_by_review_priority
from transaction_audit.risk import score_transactions
from transaction_audit.ui.components import render_metric_row, render_priority_overview, render_run_status
from transaction_audit.ui.display import (
    clean_display_frame,
    display_dataset_summary,
    display_findings,
    display_mapping_summary,
)
from transaction_audit.ui.review_queue import render_review_queue
from transaction_audit.types import ValidationResult


def render_single_file_results(result: ValidationResult, schema_result, summary: dict[str, object]) -> None:
    summary_frame = summary_dataframe(summary)
    issues_frame = result.issues_dataframe()
    findings_frame = review_findings_dataframe(issues_frame)
    transaction_review = score_transactions(result.transactions, issues_frame)
    issue_counts_frame = issue_counts_dataframe(result)
    audit_workbook = build_audit_workbook(result, schema_result, summary)

    render_run_status(
        title="Ready for review" if result.is_valid else "Review required",
        message=(
            "No blocking validation errors were found."
            if result.is_valid
            else "The file loaded successfully, but some records need human review before relying on the data."
        ),
        state="ok" if result.is_valid else "warn",
    )
    render_metric_row(
        [
            ("Rows checked", summary["rows_checked"]),
            ("Issues found", summary["issues_found"]),
            ("Errors", summary["error_count"]),
            ("Warnings", summary["warning_count"]),
        ]
    )
    render_priority_overview(findings_frame, transaction_review)

    st.download_button(
        "Download audit workbook",
        data=audit_workbook,
        file_name="audit_review_pack.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    tab_review, tab_audit, tab_evidence = st.tabs(["Review queue", "Audit pack", "Evidence"])

    with tab_review:
        render_review_queue(findings_frame, transaction_review, key_prefix="single")

    with tab_audit:
        st.write("Run summary")
        st.dataframe(clean_display_frame(summary_frame), use_container_width=True, hide_index=True)
        st.write("Schema mapping")
        st.dataframe(clean_display_frame(schema_result.mapping_dataframe()), use_container_width=True, hide_index=True)
        if not issue_counts_frame.empty:
            st.write("Issue counts by rule")
            st.dataframe(clean_display_frame(issue_counts_frame), use_container_width=True, hide_index=True)
        st.download_button(
            "Download audit workbook",
            data=audit_workbook,
            file_name="audit_review_pack.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="single_audit_pack_download",
        )

    with tab_evidence:
        with st.expander("Detailed findings", expanded=True):
            st.dataframe(
                display_findings(sort_by_review_priority(findings_frame)),
                use_container_width=True,
                hide_index=True,
            )
        issue_csv = issues_frame.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download issues CSV",
            data=issue_csv,
            file_name="validation_issues.csv",
            mime="text/csv",
        )
        with st.expander("Canonical transaction data"):
            st.dataframe(clean_display_frame(result.transactions), use_container_width=True, hide_index=True)
            st.caption("This is the cleaned and mapped dataframe passed to the validation engine.")


def render_bundle_results(
    audit_result: BundleAuditResult,
    schema_results: dict,
    mapping_summary: pd.DataFrame,
) -> None:
    bundle = audit_result.bundle
    transaction_issues = audit_result.transaction_issues_dataframe()
    reference_issues = audit_result.reference_issues_dataframe()
    skipped = audit_result.skipped_checks_dataframe()
    combined_issues = audit_result.all_issues_dataframe()
    review_findings = review_findings_dataframe(combined_issues)
    transaction_review = score_transactions(audit_result.transaction_validation.transactions, combined_issues)
    audit_workbook = build_bundle_audit_workbook(audit_result, schema_results)

    render_run_status(
        title="Ready for review" if combined_issues.empty else "Review required",
        message=(
            "The uploaded files reconciled with no audit issues found."
            if combined_issues.empty
            else (
                "The uploaded files were processed successfully. Review the "
                "exceptions below before relying on the results."
            )
        ),
        state="ok" if combined_issues.empty else "warn",
    )
    render_metric_row(
        [
            ("Transactions", bundle.transactions.row_count),
            ("Transaction issues", len(transaction_issues)),
            ("Reference issues", len(reference_issues)),
            ("Skipped checks", len(skipped)),
        ]
    )
    render_priority_overview(review_findings, transaction_review, skipped_count=len(skipped))

    st.download_button(
        "Download audit workbook",
        data=audit_workbook,
        file_name="multi_file_audit_workbook.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    render_review_queue(review_findings, transaction_review, key_prefix="multi")

    with st.expander("Dataset and mapping details"):
        st.dataframe(display_dataset_summary(bundle.dataset_summary()), use_container_width=True, hide_index=True)
        st.write("Mapping summary")
        st.dataframe(display_mapping_summary(mapping_summary), use_container_width=True, hide_index=True)

    with st.expander("CSV downloads and detailed issue tables"):
        st.write("Transaction issues")
        st.dataframe(clean_display_frame(transaction_issues), use_container_width=True, hide_index=True)
        st.download_button(
            "Download transaction issues CSV",
            data=transaction_issues.to_csv(index=False).encode("utf-8"),
            file_name="transaction_validation_issues.csv",
            mime="text/csv",
        )

        st.write("Reference issues")
        st.dataframe(clean_display_frame(reference_issues), use_container_width=True, hide_index=True)
        st.download_button(
            "Download reference issues CSV",
            data=reference_issues.to_csv(index=False).encode("utf-8"),
            file_name="reference_issues.csv",
            mime="text/csv",
        )

        st.write("Skipped checks")
        st.dataframe(clean_display_frame(skipped), use_container_width=True, hide_index=True)

        st.download_button(
            "Download all issues CSV",
            data=combined_issues.to_csv(index=False).encode("utf-8"),
            file_name="multi_file_audit_issues.csv",
            mime="text/csv",
        )

    with st.expander("Input data previews"):
        st.write("Transactions")
        st.dataframe(clean_display_frame(bundle.transactions.dataframe), use_container_width=True, hide_index=True)
        if bundle.vendors:
            st.write("Vendors / suppliers")
            st.dataframe(clean_display_frame(bundle.vendors.dataframe), use_container_width=True, hide_index=True)
        if bundle.beneficiaries:
            st.write("Beneficiaries")
            st.dataframe(clean_display_frame(bundle.beneficiaries.dataframe), use_container_width=True, hide_index=True)
        if bundle.budget:
            st.write("Budget")
            st.dataframe(clean_display_frame(bundle.budget.dataframe), use_container_width=True, hide_index=True)
