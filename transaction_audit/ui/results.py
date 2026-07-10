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
from transaction_audit.anomaly import score_transaction_anomalies
from transaction_audit.bundle_workflow import BundleAuditResult
from transaction_audit.review import sort_by_review_priority
from transaction_audit.risk import score_transactions
from transaction_audit.ui.components import render_metric_cards, render_summary_banner
from transaction_audit.ui.display import (
    clean_display_frame,
    display_dataset_summary,
    display_findings,
    display_ml_anomalies,
    display_mapping_summary,
    priority_count,
)
from transaction_audit.ui.profiles import render_profile_manager
from transaction_audit.ui.review_queue import render_review_queue
from transaction_audit.types import ValidationResult


def render_single_file_results(
    result: ValidationResult,
    schema_result,
    summary: dict[str, object],
    profiles=None,
) -> None:
    summary_frame = summary_dataframe(summary)
    issues_frame = result.issues_dataframe()
    findings_frame = review_findings_dataframe(issues_frame)
    transaction_review = score_transactions(result.transactions, issues_frame)
    anomaly_frame = score_transaction_anomalies(result.transactions)
    ml_anomaly_count = anomaly_count(anomaly_frame)
    issue_counts_frame = issue_counts_dataframe(result)
    audit_workbook = build_audit_workbook(result, schema_result, summary)

    render_summary_banner(
        title=(
            f"File ready for review - {summary['rows_checked']} transactions loaded"
            if result.is_valid
            else f"{summary['issues_found']} findings need attention"
        ),
        message=(
            "No blocking errors were found."
            if result.is_valid
            else "The file loaded successfully. Review the findings before exporting the audit pack."
        ),
        state="ok" if result.is_valid else "warn",
    )
    render_metric_cards(
        [
            {
                "label": "Transactions",
                "value": summary["rows_checked"],
                "subtitle": "loaded",
                "tone": "neutral",
            },
            {
                "label": "Findings",
                "value": summary["issues_found"],
                "subtitle": "needs review",
                "tone": "neutral",
            },
            {
                "label": "High priority",
                "value": priority_count(transaction_review, "high"),
                "subtitle": "review first",
                "tone": "amber" if priority_count(transaction_review, "high") else "neutral",
            },
            {
                "label": "ML anomalies",
                "value": ml_anomaly_count,
                "subtitle": "separate model view",
                "tone": "amber" if ml_anomaly_count else "neutral",
            },
            {
                "label": "Errors",
                "value": summary["error_count"] if summary["error_count"] else "None",
                "subtitle": "blocking issues" if summary["error_count"] else "no blocking issues",
                "tone": "red" if summary["error_count"] else "green",
            },
        ]
    )

    tab_review, tab_audit, tab_settings = st.tabs(["Priority Review", "Audit Pack", "Settings"])

    with tab_review:
        review_mode = st.radio(
            "Review view",
            options=["Findings review", "ML anomaly review"],
            horizontal=True,
            key="single_review_mode",
        )
        if review_mode == "ML anomaly review":
            st.caption("Model-based anomaly review. This is separate from audit findings and is not a fraud conclusion.")
            render_ml_anomaly_table(anomaly_frame)
        else:
            render_review_queue(findings_frame, transaction_review, key_prefix="single")

    with tab_audit:
        render_audit_pack_download(
            audit_workbook,
            file_name="audit_review_pack.xlsx",
            button_key="single_audit_pack_download",
            bullet_points=[
                "Review findings and recommended actions",
                "Transaction-level review queue",
                "ML anomaly review",
                "Source transactions and column mapping",
            ],
        )

    with tab_settings:
        with st.expander("Run summary"):
            st.dataframe(clean_display_frame(summary_frame), width="stretch", hide_index=True)
        with st.expander("Column mapping"):
            st.dataframe(clean_display_frame(schema_result.mapping_dataframe()), width="stretch", hide_index=True)
        if not issue_counts_frame.empty:
            with st.expander("Finding counts"):
                st.dataframe(clean_display_frame(issue_counts_frame), width="stretch", hide_index=True)
        with st.expander("Detailed findings"):
            st.dataframe(
                display_findings(sort_by_review_priority(findings_frame)),
                width="stretch",
                hide_index=True,
            )
        issue_csv = issues_frame.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download findings CSV",
            data=issue_csv,
            file_name="validation_issues.csv",
            mime="text/csv",
        )
        with st.expander("Uploaded transaction data"):
            st.dataframe(clean_display_frame(result.transactions), width="stretch", hide_index=True)
        if profiles is not None:
            render_profile_manager(profiles)
        with st.expander("About"):
            st.write("Transaction Audit helps reviewers inspect transaction files and supporting reference files.")
            st.caption("This tool supports audit review. It does not replace professional judgement.")


def render_bundle_results(
    audit_result: BundleAuditResult,
    schema_results: dict,
    mapping_summary: pd.DataFrame,
    profiles=None,
) -> None:
    bundle = audit_result.bundle
    transaction_issues = audit_result.transaction_issues_dataframe()
    reference_issues = audit_result.reference_issues_dataframe()
    skipped = audit_result.skipped_checks_dataframe()
    combined_issues = audit_result.all_issues_dataframe()
    review_findings = review_findings_dataframe(combined_issues)
    transaction_review = score_transactions(audit_result.transaction_validation.transactions, combined_issues)
    anomaly_frame = score_transaction_anomalies(audit_result.transaction_validation.transactions)
    ml_anomaly_count = anomaly_count(anomaly_frame)
    audit_workbook = build_bundle_audit_workbook(audit_result, schema_results)

    render_summary_banner(
        title=(
            f"Files ready for review - {bundle.transactions.row_count} transactions loaded"
            if combined_issues.empty
            else f"{len(combined_issues)} findings need attention"
        ),
        message=(
            "No audit findings were found across the uploaded files."
            if combined_issues.empty
            else "The uploaded files were processed successfully. Review the findings before exporting the audit pack."
        ),
        state="ok" if combined_issues.empty else "warn",
    )
    render_metric_cards(
        [
            {
                "label": "Transactions",
                "value": bundle.transactions.row_count,
                "subtitle": "loaded",
                "tone": "neutral",
            },
            {
                "label": "Findings",
                "value": len(combined_issues),
                "subtitle": "needs review",
                "tone": "neutral",
            },
            {
                "label": "High priority",
                "value": priority_count(transaction_review, "high"),
                "subtitle": "review first",
                "tone": "amber" if priority_count(transaction_review, "high") else "neutral",
            },
            {
                "label": "ML anomalies",
                "value": ml_anomaly_count,
                "subtitle": "separate model view",
                "tone": "amber" if ml_anomaly_count else "neutral",
            },
            {
                "label": "Errors",
                "value": len(combined_issues[combined_issues["severity"].eq("error")]) if not combined_issues.empty else "None",
                "subtitle": "blocking issues" if not combined_issues.empty else "no blocking issues",
                "tone": "red" if not combined_issues.empty and combined_issues["severity"].eq("error").any() else "green",
            },
            {
                "label": "Reference findings",
                "value": len(reference_issues),
                "subtitle": "cross-file review",
                "tone": "neutral",
            },
        ]
    )

    tab_review, tab_audit, tab_settings = st.tabs(["Priority Review", "Audit Pack", "Settings"])

    with tab_review:
        review_mode = st.radio(
            "Review view",
            options=["Findings review", "ML anomaly review"],
            horizontal=True,
            key="multi_review_mode",
        )
        if review_mode == "ML anomaly review":
            st.caption("Model-based anomaly review. This is separate from audit findings and is not a fraud conclusion.")
            render_ml_anomaly_table(anomaly_frame)
        else:
            render_review_queue(review_findings, transaction_review, key_prefix="multi")
        if len(skipped) and review_mode == "Findings review":
            st.caption(
                f"{len(skipped)} optional check(s) were skipped because a reference file or field was not available."
            )

    with tab_audit:
        render_audit_pack_download(
            audit_workbook,
            file_name="multi_file_audit_workbook.xlsx",
            button_key="multi_audit_pack_download",
            bullet_points=[
                "Review findings and recommended actions",
                "Transaction and reference-file checks",
                "Skipped optional checks",
                "Source files and column mappings",
            ],
        )

    with tab_settings:
        with st.expander("File and mapping details"):
            st.dataframe(display_dataset_summary(bundle.dataset_summary()), width="stretch", hide_index=True)
            st.write("Column mapping summary")
            st.dataframe(display_mapping_summary(mapping_summary), width="stretch", hide_index=True)

        with st.expander("CSV downloads and detailed finding tables"):
            st.write("Transaction findings")
            st.dataframe(clean_display_frame(transaction_issues), width="stretch", hide_index=True)
            st.write("Reference-file findings")
            st.dataframe(clean_display_frame(reference_issues), width="stretch", hide_index=True)
            st.write("Skipped checks")
            st.dataframe(clean_display_frame(skipped), width="stretch", hide_index=True)

            st.download_button(
                "Download all findings CSV",
                data=combined_issues.to_csv(index=False).encode("utf-8"),
                file_name="multi_file_audit_issues.csv",
                mime="text/csv",
            )
        st.download_button(
            "Download transaction findings CSV",
            data=transaction_issues.to_csv(index=False).encode("utf-8"),
            file_name="transaction_validation_issues.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download reference findings CSV",
            data=reference_issues.to_csv(index=False).encode("utf-8"),
            file_name="reference_issues.csv",
            mime="text/csv",
        )
        with st.expander("Uploaded data previews"):
            st.write("Transactions")
            st.dataframe(clean_display_frame(bundle.transactions.dataframe), width="stretch", hide_index=True)
            if bundle.vendors:
                st.write("Approved vendor list")
                st.dataframe(clean_display_frame(bundle.vendors.dataframe), width="stretch", hide_index=True)
            if bundle.beneficiaries:
                st.write("Beneficiary register")
                st.dataframe(clean_display_frame(bundle.beneficiaries.dataframe), width="stretch", hide_index=True)
            if bundle.budget:
                st.write("Budget lines")
                st.dataframe(clean_display_frame(bundle.budget.dataframe), width="stretch", hide_index=True)
        if profiles is not None:
            render_profile_manager(profiles)
        with st.expander("About"):
            st.write("Transaction Audit helps reviewers inspect transaction files and supporting reference files.")
            st.caption("This tool supports audit review. It does not replace professional judgement.")


def anomaly_count(anomaly_frame: pd.DataFrame) -> int:
    if anomaly_frame.empty or "ml_anomaly_score" not in anomaly_frame:
        return 0
    return int(pd.to_numeric(anomaly_frame["ml_anomaly_score"], errors="coerce").fillna(0).ge(50).sum())


def render_ml_anomaly_table(anomaly_frame: pd.DataFrame) -> None:
    display = display_ml_anomalies(anomaly_frame)
    if display.empty:
        st.info("No model-scored anomaly rows above the review threshold.")
        return

    st.dataframe(
        display,
        width="stretch",
        hide_index=True,
        column_config={
            "ml_anomaly_score": st.column_config.NumberColumn("ML score", width="small"),
            "ml_anomaly_band": st.column_config.TextColumn("ML band", width="small"),
            "ml_anomaly_drivers": st.column_config.TextColumn("Why flagged by ML", width="large"),
            "row_number": st.column_config.TextColumn("Row", width="small"),
            "transaction_id": st.column_config.TextColumn("Transaction ID", width="small"),
            "transaction_date": st.column_config.TextColumn("Date", width="small"),
            "counterparty": st.column_config.TextColumn("Payee or vendor", width="medium"),
        },
    )


def render_audit_pack_download(
    audit_workbook: bytes,
    *,
    file_name: str,
    button_key: str,
    bullet_points: list[str],
) -> None:
    left, middle, right = st.columns([1, 2, 1])
    with middle:
        with st.container(border=True):
            st.subheader("Download audit pack")
            st.caption("Excel workbook for human review and audit evidence.")
            st.download_button(
                "Download Audit Workbook",
                data=audit_workbook,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=button_key,
                width="stretch",
            )
            for point in bullet_points:
                st.markdown(f"- {point}")
