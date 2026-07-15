from __future__ import annotations

import pandas as pd


SOURCE_ROW_NUMBER = "source_row_number"


def source_row_numbers(dataframe: pd.DataFrame, start: int = 2) -> pd.Series:
    fallback = pd.Series(
        range(start, start + len(dataframe)),
        index=dataframe.index,
        dtype="int64",
    )
    if SOURCE_ROW_NUMBER not in dataframe:
        return fallback

    preserved = pd.to_numeric(dataframe[SOURCE_ROW_NUMBER], errors="coerce")
    usable = preserved.notna() & preserved.ge(1) & preserved.mod(1).eq(0)
    return preserved.where(usable, fallback).astype("int64")


def source_row_number_at_position(
    dataframe: pd.DataFrame,
    position: int,
    start: int = 2,
) -> int:
    return int(source_row_numbers(dataframe, start=start).iloc[position])
