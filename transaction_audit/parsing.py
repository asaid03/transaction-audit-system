from __future__ import annotations

import pandas as pd

from transaction_audit.config import ValidationConfig
from transaction_audit.types import PreparedTransactions


DAYFIRST_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%d %b %Y",
    "%d %B %Y",
)
MONTHFIRST_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m.%d.%Y",
    "%b %d %Y",
    "%B %d %Y",
)


def prepare_transactions(transactions: pd.DataFrame, config: ValidationConfig) -> PreparedTransactions:
    original = transactions.copy()
    checked = transactions.copy()
    checked["transaction_id"] = checked["transaction_id"].astype("string").str.strip()
    checked["currency"] = checked["currency"].astype("string").str.upper().str.strip()
    checked["counterparty"] = checked["counterparty"].astype("string").str.strip()
    checked["account_id"] = checked["account_id"].astype("string").str.strip()
    checked["amount"] = parse_amounts(checked["amount"])
    checked["transaction_date"] = parse_transaction_dates(
        checked["transaction_date"],
        dayfirst=config.date_dayfirst,
    )
    return PreparedTransactions(original=original, checked=checked)


def parse_amounts(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    negative_parentheses = text.str.match(r"^\(.+\)$", na=False)
    cleaned = text.str.replace(r"^\((.+)\)$", r"\1", regex=True)
    cleaned = cleaned.str.replace(",", "", regex=False)
    cleaned = cleaned.str.replace(" ", "", regex=False)
    cleaned = cleaned.str.replace(r"^[£$€]", "", regex=True)
    cleaned = cleaned.str.replace(r"[£$€]$", "", regex=True)

    amounts = pd.to_numeric(cleaned, errors="coerce")
    amounts.loc[negative_parentheses & amounts.notna()] = -amounts.abs()
    return amounts


def parse_transaction_dates(series: pd.Series, dayfirst: bool = True) -> pd.Series:
    text = series.astype("string").str.strip()
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    unparsed = text.notna() & text.ne("")

    for date_format in _date_formats(dayfirst):
        if not unparsed.any():
            break

        attempted = pd.to_datetime(text.loc[unparsed], format=date_format, errors="coerce")
        matched = attempted.notna()
        matched_indexes = attempted.index[matched]
        parsed.loc[matched_indexes] = attempted.loc[matched_indexes]
        unparsed.loc[matched_indexes] = False

    return parsed


def _date_formats(dayfirst: bool) -> tuple[str, ...]:
    return DAYFIRST_DATE_FORMATS if dayfirst else MONTHFIRST_DATE_FORMATS
