from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.cleaning import CleaningResult
from transaction_audit.ui.display import priority_count


def render_global_styles() -> None:
    """Apply small, central Streamlit CSS overrides."""
    st.html(
        """
        <style>
        :root {
            --audit-teal: #00897b;
            --audit-amber: #ffb300;
            --audit-red: #ef5350;
            --audit-green: #2e7d32;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 4rem;
            max-width: 1180px;
        }
        #MainMenu,
        header,
        footer,
        div[data-testid="stToolbar"],
        div[data-testid="stDecoration"],
        div[data-testid="stStatusWidget"],
        a[href*="github.com/streamlit"],
        a[href*="github.com"] {
            visibility: hidden;
            display: none;
        }
        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.035);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-top: 3px solid var(--audit-teal);
            border-radius: 8px;
            padding: 0.9rem 1rem;
        }
        div[data-testid="stMetricLabel"] {
            color: rgba(255, 255, 255, 0.68);
        }
        div[data-testid="stMetricValue"] {
            font-weight: 700;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 6px;
            overflow: hidden;
        }
        div[data-testid="stAlert"] {
            border-radius: 6px;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 1.5rem;
        }
        .stTabs [data-baseweb="tab"] {
            padding-left: 0;
            padding-right: 0;
        }
        div[data-testid="stFileUploader"] section {
            border-color: rgba(0, 137, 123, 0.45);
            background: rgba(0, 137, 123, 0.04);
        }
        div[data-testid="stFileUploader"] section:hover {
            border-color: var(--audit-teal);
            background: rgba(0, 137, 123, 0.08);
        }
        </style>
        """
    )


def render_page_header() -> None:
    st.title("Transaction Review")
    st.caption("Find data errors, possible duplicate payments, and unusual activity that needs a closer look.")


def render_stepper(active_step: str) -> None:
    steps = [("upload", "1", "Upload"), ("review", "2", "Review"), ("export", "3", "Download")]
    labels = []
    for step_id, number, label in steps:
        if step_id == active_step:
            labels.append(f"**{number}. {label}**")
        else:
            labels.append(f"{number}. {label}")
    st.markdown(" &nbsp;&nbsp; / &nbsp;&nbsp; ".join(labels))


def render_summary_banner(title: str, message: str, state: str = "ok") -> None:
    text = f"{title}. {message}"
    if state == "ok":
        st.success(text)
    elif state == "error":
        st.error(text)
    else:
        st.warning(text)


def render_empty_start(title: str, guidance: list[str]) -> None:
    with st.container(border=True):
        st.subheader(title)
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


def render_metric_cards(metrics: list[dict[str, object]]) -> None:
    columns = st.columns(len(metrics))
    for column, metric in zip(columns, metrics):
        column.metric(
            label=str(metric["label"]),
            value=metric["value"],
            help=str(metric.get("subtitle", "")) or None,
        )


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
        with st.expander("Data preparation applied", expanded=False):
            st.write("The uploaded file was cleaned before review.")
            if cleaning_result.transformations:
                st.write("Transformations")
                st.dataframe(
                    pd.DataFrame({"transformation": cleaning_result.transformations}),
                    width="stretch",
                    hide_index=True,
                )
            if cleaning_result.dropped_rows:
                st.write("Dropped source rows")
                st.dataframe(
                    pd.DataFrame({"source_row_number": cleaning_result.dropped_rows}),
                    width="stretch",
                    hide_index=True,
                )
            if cleaning_result.warnings:
                st.write("Warnings")
                st.dataframe(
                    pd.DataFrame({"warning": cleaning_result.warnings}),
                    width="stretch",
                    hide_index=True,
                )
        return

    if cleaning_result.warnings:
        st.caption("Data preparation note: " + "; ".join(cleaning_result.warnings))
