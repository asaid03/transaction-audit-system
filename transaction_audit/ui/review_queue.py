from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.review import (
    filter_by_issue_types,
    filter_by_values,
    sort_by_review_priority,
    unique_issue_types,
    unique_values,
)
from transaction_audit.ui.display import display_findings, display_transaction_review


def render_review_queue(
    findings: pd.DataFrame,
    transaction_review: pd.DataFrame,
    key_prefix: str,
) -> None:
    st.subheader("Exceptions needing review")
    if findings.empty:
        st.success("No validation or reconciliation findings need review.")
    else:
        with st.expander("Filter findings"):
            filtered_findings = review_findings_with_filters(findings, key_prefix=key_prefix)
        st.dataframe(display_findings(filtered_findings), use_container_width=True, hide_index=True)

    st.subheader("Transactions by review priority")
    if transaction_review.empty:
        st.info("No transaction review rows are available.")
        return

    with st.expander("Filter transactions"):
        filtered_transactions = transaction_review_with_filters(transaction_review, key_prefix=key_prefix)
    st.dataframe(display_transaction_review(filtered_transactions), use_container_width=True, hide_index=True)


def review_findings_with_filters(findings: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    if findings.empty:
        return findings

    band_column, issue_column, dataset_column = st.columns(3)
    bands = band_column.multiselect(
        "Band",
        options=unique_values(findings, "review_priority_band"),
        key=f"{key_prefix}_finding_bands",
    )
    issue_types = issue_column.multiselect(
        "Issue type",
        options=unique_issue_types(findings),
        key=f"{key_prefix}_finding_issue_types",
    )
    datasets = dataset_column.multiselect(
        "Dataset",
        options=unique_values(findings, "dataset"),
        key=f"{key_prefix}_finding_datasets",
    )

    filtered = sort_by_review_priority(findings)
    filtered = filter_by_values(filtered, "review_priority_band", bands)
    filtered = filter_by_issue_types(filtered, issue_types)
    return filter_by_values(filtered, "dataset", datasets)


def transaction_review_with_filters(transaction_review: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    if transaction_review.empty:
        return transaction_review

    first_row = st.columns(3)
    bands = first_row[0].multiselect(
        "Band",
        options=unique_values(transaction_review, "review_priority_band"),
        key=f"{key_prefix}_transaction_bands",
    )
    issue_types = first_row[1].multiselect(
        "Issue type",
        options=unique_issue_types(transaction_review),
        key=f"{key_prefix}_transaction_issue_types",
    )
    currencies = first_row[2].multiselect(
        "Currency",
        options=unique_values(transaction_review, "currency"),
        key=f"{key_prefix}_transaction_currencies",
    )

    second_row = st.columns(2)
    accounts = second_row[0].multiselect(
        "Account",
        options=unique_values(transaction_review, "account_id"),
        key=f"{key_prefix}_transaction_accounts",
    )
    counterparties = second_row[1].multiselect(
        "Counterparty",
        options=unique_values(transaction_review, "counterparty"),
        key=f"{key_prefix}_transaction_counterparties",
    )

    filtered = sort_by_review_priority(transaction_review)
    filtered = filter_by_values(filtered, "review_priority_band", bands)
    filtered = filter_by_issue_types(filtered, issue_types)
    filtered = filter_by_values(filtered, "currency", currencies)
    filtered = filter_by_values(filtered, "account_id", accounts)
    return filter_by_values(filtered, "counterparty", counterparties)
