from __future__ import annotations

import pandas as pd


BAND_SORT_ORDER = {"high": 0, "medium": 1, "low": 2}


def sort_by_review_priority(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "review_priority_score" not in dataframe:
        return dataframe

    sorted_frame = dataframe.copy()
    band_values = (
        sorted_frame["review_priority_band"]
        if "review_priority_band" in sorted_frame
        else pd.Series([""] * len(sorted_frame), index=sorted_frame.index)
    )
    sorted_frame["_band_order"] = band_values.astype("string").str.lower().map(BAND_SORT_ORDER).fillna(99)
    sort_columns = ["_band_order", "review_priority_score"]
    ascending = [True, False]
    if "row_number" in sorted_frame:
        sort_columns.append("row_number")
        ascending.append(True)

    return (
        sorted_frame.sort_values(sort_columns, ascending=ascending, kind="stable")
        .drop(columns=["_band_order"])
        .reset_index(drop=True)
    )


def filter_by_values(
    dataframe: pd.DataFrame,
    column: str,
    selected_values: list[str] | tuple[str, ...],
) -> pd.DataFrame:
    if dataframe.empty or not selected_values or column not in dataframe:
        return dataframe

    selected = {str(value) for value in selected_values}
    return dataframe[dataframe[column].astype("string").isin(selected)].copy()


def filter_by_issue_types(
    dataframe: pd.DataFrame,
    selected_issue_types: list[str] | tuple[str, ...],
) -> pd.DataFrame:
    if dataframe.empty or not selected_issue_types:
        return dataframe

    selected = {str(value) for value in selected_issue_types}
    if "technical_issue_type" in dataframe:
        return dataframe[dataframe["technical_issue_type"].astype("string").isin(selected)].copy()
    if "issue_types" not in dataframe:
        return dataframe

    mask = dataframe["issue_types"].astype("string").map(
        lambda value: bool(_issue_type_set(value) & selected)
    )
    return dataframe[mask].copy()


def unique_values(dataframe: pd.DataFrame, column: str) -> list[str]:
    if dataframe.empty or column not in dataframe:
        return []

    values = dataframe[column].dropna().astype(str).str.strip()
    return sorted(value for value in values.unique().tolist() if value)


def unique_issue_types(dataframe: pd.DataFrame) -> list[str]:
    if dataframe.empty:
        return []
    if "technical_issue_type" in dataframe:
        return unique_values(dataframe, "technical_issue_type")
    if "issue_types" not in dataframe:
        return []

    issue_types = set()
    for value in dataframe["issue_types"].dropna().astype(str):
        issue_types.update(_issue_type_set(value))
    return sorted(issue_types)


def _issue_type_set(value: str) -> set[str]:
    return {part.strip() for part in value.split(",") if part.strip()}
