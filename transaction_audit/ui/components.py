from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.cleaning import CleaningResult
from transaction_audit.ui.display import priority_count


def render_page_header() -> None:
    st.title("Transaction Audit")
    st.caption(
        "Audit support for transaction files: validate the data, reconcile reference files, "
        "prioritise exceptions, and export a review pack."
    )


def render_empty_start(title: str, guidance: list[str]) -> None:
    st.info(title)
    for item in guidance:
        st.markdown(f"- {item}")


def render_run_status(title: str, message: str, state: str) -> None:
    if state == "ok":
        st.success(f"{title}. {message}")
    elif state == "error":
        st.error(f"{title}. {message}")
    else:
        st.warning(f"{title}. {message}")


def render_metric_row(metrics: list[tuple[str, object]]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics):
        column.metric(label, value)


def render_priority_overview(
    findings: pd.DataFrame,
    transaction_review: pd.DataFrame,
    skipped_count: int = 0,
) -> None:
    st.write("Review summary")
    render_metric_row(
        [
            ("High priority", priority_count(transaction_review, "high")),
            ("Medium priority", priority_count(transaction_review, "medium")),
            ("Low priority", priority_count(transaction_review, "low")),
            ("Findings", len(findings)),
        ]
    )
    if skipped_count:
        st.caption(
            f"{skipped_count} check(s) were skipped because the needed reference "
            "file or fields were not available."
        )


def render_cleaning_notice(cleaning_result: CleaningResult | None) -> None:
    if cleaning_result is None:
        return

    if cleaning_result.applied:
        with st.expander("Data preparation applied"):
            st.write("Bank-style export cleanup was applied before schema mapping.")
            if cleaning_result.transformations:
                st.write("Transformations")
                st.dataframe(
                    pd.DataFrame({"transformation": cleaning_result.transformations}),
                    use_container_width=True,
                    hide_index=True,
                )
            if cleaning_result.dropped_rows:
                st.write("Dropped source rows")
                st.dataframe(
                    pd.DataFrame({"source_row_number": cleaning_result.dropped_rows}),
                    use_container_width=True,
                    hide_index=True,
                )
            if cleaning_result.warnings:
                st.write("Warnings")
                st.dataframe(
                    pd.DataFrame({"warning": cleaning_result.warnings}),
                    use_container_width=True,
                    hide_index=True,
                )
        return

    if cleaning_result.warnings:
        st.caption("Cleaning not applied: " + "; ".join(cleaning_result.warnings))
