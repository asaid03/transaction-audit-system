from __future__ import annotations

import pandas as pd


AUDIT_ROW_NUMBER = "_audit_row_number"
LEFT_JOIN_KEY = "_left_join_key"
RIGHT_JOIN_KEY = "_right_join_key"
MERGE_STATUS = "_merge"


def normalize_join_key(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip().str.lower()
    normalized = normalized.str.replace(r"\s+", " ", regex=True)
    return normalized.mask(normalized.eq(""))


def with_audit_row_numbers(dataframe: pd.DataFrame, start: int = 2) -> pd.DataFrame:
    numbered = dataframe.copy()
    numbered[AUDIT_ROW_NUMBER] = range(start, start + len(numbered))
    return numbered


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: str,
    right_on: str,
    right_prefix: str,
    validate: str = "many_to_one",
) -> pd.DataFrame:
    left_work = with_audit_row_numbers(left)
    right_work = right.copy()
    left_work[LEFT_JOIN_KEY] = normalize_join_key(left_work[left_on])
    right_work[RIGHT_JOIN_KEY] = normalize_join_key(right_work[right_on])

    return left_work.merge(
        right_work,
        how="left",
        left_on=LEFT_JOIN_KEY,
        right_on=RIGHT_JOIN_KEY,
        suffixes=("", f"_{right_prefix}"),
        indicator=True,
        validate=validate,
    )


def unmatched_left_rows(joined: pd.DataFrame) -> pd.DataFrame:
    return joined[joined[MERGE_STATUS].eq("left_only")].copy()
