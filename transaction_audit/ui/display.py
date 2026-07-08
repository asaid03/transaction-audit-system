from __future__ import annotations

import pandas as pd


FINDING_DISPLAY_COLUMNS = [
    "finding_id",
    "review_priority_score",
    "review_priority_band",
    "review_priority_drivers",
    "priority",
    "review_status",
    "finding",
    "recommended_action",
    "dataset",
    "row_number",
    "transaction_id",
    "related_record_id",
]
TRANSACTION_REVIEW_DISPLAY_COLUMNS = [
    "review_priority_score",
    "review_priority_band",
    "review_priority_drivers",
    "row_number",
    "issue_types",
    "transaction_id",
    "transaction_date",
    "account_id",
    "invoice_reference",
    "amount",
    "currency",
    "counterparty",
    "vendor_id",
    "beneficiary_id",
    "budget_line_id",
]
ML_ANOMALY_DISPLAY_COLUMNS = [
    "ml_anomaly_score",
    "ml_anomaly_band",
    "ml_anomaly_drivers",
    "row_number",
    "transaction_id",
    "transaction_date",
    "account_id",
    "invoice_reference",
    "amount",
    "currency",
    "counterparty",
    "vendor_id",
]


def clean_display_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    display = dataframe.copy()
    display = display.fillna("")
    return display.replace({"None": "", "<NA>": "", "nan": ""})


def display_dataset_summary(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary

    display = clean_display_frame(summary)
    if "uploaded" in display:
        display["uploaded"] = (
            display["uploaded"]
            .map({True: "Uploaded", False: "Not uploaded"})
            .fillna(display["uploaded"])
        )
    return display


def display_mapping_summary(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary

    display = clean_display_frame(summary)
    if "status" in display:
        display["status"] = display["status"].astype("string").str.replace("_", " ").str.title()
    return display


def display_findings(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return findings
    display = findings[[column for column in FINDING_DISPLAY_COLUMNS if column in findings.columns]]
    return clean_display_frame(display)


def display_transaction_review(transaction_review: pd.DataFrame) -> pd.DataFrame:
    if transaction_review.empty:
        return transaction_review
    display = reviewable_transaction_rows(transaction_review)[
        [column for column in TRANSACTION_REVIEW_DISPLAY_COLUMNS if column in transaction_review.columns]
    ]
    display = clean_display_frame(display)
    if "review_priority_band" in display:
        display["review_priority_band"] = display["review_priority_band"].astype("string").str.title()
    return display


def display_ml_anomalies(anomalies: pd.DataFrame) -> pd.DataFrame:
    if anomalies.empty:
        return anomalies

    display = anomalies[anomalies["ml_anomaly_score"].ge(50)].copy()
    display = display[[column for column in ML_ANOMALY_DISPLAY_COLUMNS if column in display.columns]]
    display = clean_display_frame(display)
    if "ml_anomaly_band" in display:
        display["ml_anomaly_band"] = display["ml_anomaly_band"].astype("string").str.title()
    return display


def priority_count(dataframe: pd.DataFrame, band: str) -> int:
    if dataframe.empty or "review_priority_band" not in dataframe:
        return 0

    reviewable = reviewable_transaction_rows(dataframe)
    values = reviewable["review_priority_band"].astype("string").str.lower()
    return int(values.eq(band.lower()).sum())


def reviewable_transaction_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "review_priority_score" not in dataframe:
        return dataframe

    scores = pd.to_numeric(dataframe["review_priority_score"], errors="coerce").fillna(0)
    return dataframe[scores.gt(0)].copy()
