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
    st.subheader("Transactions to review")
    st.caption("Start with high-priority items. Select any row to see the evidence and the next review step.")
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

    selection = st.dataframe(
        queue[["priority", "finding", "amount", "payee_or_vendor", "transaction_id"]],
        width="stretch",
        hide_index=True,
        key=f"{key_prefix}_review_queue_table",
        on_select="rerun",
        selection_mode="single-row",
        row_height=46,
        column_config={
            "priority": st.column_config.TextColumn("Priority", width="small"),
            "finding": st.column_config.TextColumn("Finding", width="large"),
            "amount": st.column_config.NumberColumn("Amount", format="%.2f", width="small"),
            "payee_or_vendor": st.column_config.TextColumn("Payee / vendor", width="medium"),
            "transaction_id": st.column_config.TextColumn("Transaction ID", width="small"),
        },
    )

    selected_rows = list(selection.selection.rows)
    if selected_rows:
        render_selected_transaction_detail(
            queue,
            findings,
            selected_index=selected_rows[0],
        )
    else:
        st.info("Select a transaction above to see why it needs review.")

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


def render_selected_transaction_detail(
    queue: pd.DataFrame,
    findings: pd.DataFrame,
    selected_index: int,
) -> None:
    row = queue.iloc[selected_index]
    transaction_findings = _findings_for_transaction(findings, row)

    with st.container(border=True):
        st.markdown(f"### {row.get('transaction_id', 'Transaction detail')}")
        st.caption(str(row.get("finding", "")))
        detail_column, evidence_column = st.columns([1, 1.5])

        with detail_column:
            st.markdown("**Payment details**")
            _write_detail("Date", _format_date(row.get("transaction_date", "")))
            _write_detail("Payee / vendor", row.get("payee_or_vendor", ""))
            _write_detail("Amount", _format_amount(row.get("amount", ""), row.get("currency", "")))
            _write_detail("Account", row.get("account_id", ""))
            _write_detail("Invoice / reference", row.get("invoice_reference", ""))

        with evidence_column:
            st.markdown("**What needs checking**")
            if transaction_findings.empty:
                for driver in _split_drivers(row.get("review_priority_drivers", "")):
                    st.markdown(f"- {driver}")
                st.caption("Check the payment against its approval and supporting documents.")
            else:
                for finding in transaction_findings.to_dict("records"):
                    st.markdown(f"**{finding.get('finding', 'Finding')}**")
                    evidence = str(finding.get("evidence", "")).strip()
                    if evidence:
                        st.write(evidence)
                    related = str(finding.get("related_record_id", "")).strip()
                    if related:
                        st.write(f"Related record(s): {related}")
                    action = str(finding.get("recommended_action", "")).strip()
                    if action:
                        st.caption(f"Next step: {action}")


def _findings_for_transaction(findings: pd.DataFrame, row: pd.Series) -> pd.DataFrame:
    if findings.empty:
        return findings

    row_number = str(row.get("row_number", "")).strip()
    transaction_id = str(row.get("transaction_id", "")).strip()
    matches = pd.Series(False, index=findings.index)
    if row_number and "row_number" in findings:
        matches |= findings["row_number"].astype("string").str.strip().eq(row_number)
    if transaction_id and "transaction_id" in findings:
        matches |= findings["transaction_id"].astype("string").str.strip().eq(transaction_id)
    return findings[matches].copy()


def _split_drivers(value: object) -> list[str]:
    return [driver.strip() for driver in str(value).split(";") if driver.strip()]


def _write_detail(label: str, value: object) -> None:
    display_value = str(value).strip()
    st.write(f"{label}: {display_value or 'Not provided'}")


def _format_amount(amount: object, currency: object) -> str:
    try:
        amount_text = f"{float(amount):,.2f}"
    except (TypeError, ValueError):
        amount_text = str(amount).strip() or "Not provided"
    currency_text = str(currency).strip()
    return f"{currency_text} {amount_text}".strip()


def _format_date(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value).strip() or "Not provided"
    return parsed.strftime("%d %b %Y")


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
