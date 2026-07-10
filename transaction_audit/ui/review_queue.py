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
from transaction_audit.ui.display import display_findings, display_transaction_review, reviewable_transaction_rows
from transaction_audit.ui.display import display_review_queue


def render_review_queue(
    findings: pd.DataFrame,
    transaction_review: pd.DataFrame,
    key_prefix: str,
) -> None:
    st.subheader("Priority review")
    reviewable_transactions = reviewable_transaction_rows(transaction_review)
    if reviewable_transactions.empty:
        if findings.empty:
            st.success("No findings need review.")
        else:
            st.info("Findings were created, but no transaction rows have priority drivers.")
            with st.expander("View finding-level detail", expanded=True):
                filtered_findings = review_findings_with_filters(findings, key_prefix=key_prefix)
                st.dataframe(display_findings(filtered_findings), width="stretch", hide_index=True)
        return

    filtered_transactions = priority_review_filters(reviewable_transactions, key_prefix=key_prefix)
    queue = display_review_queue(filtered_transactions)
    if queue.empty:
        st.info("No transactions match the selected filters.")
        return

    st.dataframe(
        queue[["priority", "finding", "amount", "payee_or_vendor", "transaction_id"]],
        width="stretch",
        hide_index=True,
        column_config={
            "priority": st.column_config.TextColumn("Priority", width="small"),
            "finding": st.column_config.TextColumn("Finding", width="large"),
            "amount": st.column_config.NumberColumn("Amount", format="%.2f", width="small"),
            "payee_or_vendor": st.column_config.TextColumn("Payee / vendor", width="medium"),
            "transaction_id": st.column_config.TextColumn("Transaction ID", width="small"),
        },
    )

    render_selected_transaction_detail(queue, key_prefix=key_prefix)

    if not findings.empty:
        with st.expander("View finding-level detail"):
            filtered_findings = review_findings_with_filters(findings, key_prefix=key_prefix)
            st.dataframe(display_findings(filtered_findings), width="stretch", hide_index=True)

    with st.expander("View uploaded transaction data"):
        st.dataframe(display_transaction_review(filtered_transactions), width="stretch", hide_index=True)


def priority_review_filters(transaction_review: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    filter_columns = st.columns([1.4, 2])
    band = filter_columns[0].radio(
        "Priority",
        options=["All", "High", "Medium", "Low"],
        horizontal=True,
        key=f"{key_prefix}_priority_band_filter",
    )
    search = filter_columns[1].text_input(
        "Search",
        placeholder="Search by transaction ID or payee/vendor",
        key=f"{key_prefix}_priority_search",
    )

    filtered = sort_by_review_priority(transaction_review)
    if band != "All":
        filtered = filtered[
            filtered["review_priority_band"].astype("string").str.lower().eq(band.lower())
        ].copy()

    search_text = search.strip().lower()
    if search_text:
        haystack = (
            filtered.get("transaction_id", "").astype("string").str.lower()
            + " "
            + filtered.get("counterparty", "").astype("string").str.lower()
        )
        filtered = filtered[haystack.str.contains(search_text, regex=False, na=False)].copy()
    return filtered


def render_selected_transaction_detail(queue: pd.DataFrame, key_prefix: str) -> None:
    options = [
        f"{row.transaction_id} - {row.finding}"
        for row in queue.itertuples(index=False)
    ]
    selected = st.selectbox(
        "Select a transaction to review",
        options=options,
        key=f"{key_prefix}_selected_transaction_detail",
    )
    if not selected:
        return

    selected_index = options.index(selected)
    row = queue.iloc[selected_index]
    with st.container(border=True):
        left, right = st.columns([1, 1])
        left.markdown("**Transaction detail**")
        left.write(f"Transaction ID: {row.get('transaction_id', '')}")
        left.write(f"Date: {row.get('transaction_date', '')}")
        left.write(f"Account: {row.get('account_id', '')}")
        left.write(f"Currency: {row.get('currency', '')}")
        left.write(f"Payee / vendor: {row.get('payee_or_vendor', '')}")
        left.write(f"Amount: {row.get('amount', '')}")
        right.markdown("**Why this was flagged**")
        right.write(row.get("review_priority_drivers", ""))

    status_columns = st.columns([1, 2])
    status_columns[0].selectbox(
        "Review status",
        options=[
            "Needs review",
            "Under review",
            "Cleared - no issue",
            "Cleared - adjustment needed",
            "Escalate",
        ],
        key=f"{key_prefix}_review_status_{selected_index}",
    )
    status_columns[1].text_input(
        "Review note",
        placeholder="Add note for this session",
        key=f"{key_prefix}_review_note_{selected_index}",
    )


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
        "Finding type",
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
        "Finding type",
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
        "Payee or vendor",
        options=unique_values(transaction_review, "counterparty"),
        key=f"{key_prefix}_transaction_counterparties",
    )

    filtered = sort_by_review_priority(transaction_review)
    filtered = filter_by_values(filtered, "review_priority_band", bands)
    filtered = filter_by_issue_types(filtered, issue_types)
    filtered = filter_by_values(filtered, "currency", currencies)
    filtered = filter_by_values(filtered, "account_id", accounts)
    return filter_by_values(filtered, "counterparty", counterparties)
