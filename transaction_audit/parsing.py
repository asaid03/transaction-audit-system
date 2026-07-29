from __future__ import annotations

from datetime import datetime
import re

import pandas as pd
from dateutil import parser
from dateutil.parser import ParserError

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
    "%d-%b-%y",
    "%d-%b-%Y",
    "%d %b %y",
)
MONTHFIRST_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m.%d.%Y",
    "%b %d %Y",
    "%B %d %Y",
    "%d-%b-%y",
    "%d-%b-%Y",
    "%d %b %y",
)
INLINE_CURRENCY_PATTERN = re.compile(r"(^[A-Z]{3}\s*|\s*[A-Z]{3}$)")


def prepare_transactions(transactions: pd.DataFrame, config: ValidationConfig) -> PreparedTransactions:
    original = transactions.copy()
    checked = transactions.copy()
    checked["transaction_id"] = checked["transaction_id"].astype("string").str.strip()
    checked["currency"] = checked["currency"].astype("string").str.upper().str.strip()
    checked["counterparty"] = checked["counterparty"].astype("string").str.strip()
    checked["account_id"] = checked["account_id"].astype("string").str.strip()
    if "invoice_reference" in checked:
        checked["invoice_reference"] = checked["invoice_reference"].astype("string").str.strip()
    if "invoice_amount" in checked:
        checked["invoice_amount"] = parse_amounts(checked["invoice_amount"])
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
    cleaned = cleaned.str.replace(" ", "", regex=False)
    cleaned = cleaned.str.replace(r"^[^\d\-\.,A-Z]+", "", regex=True)
    cleaned = cleaned.str.replace(r"[^\d\.,A-Z]+$", "", regex=True)
    cleaned = cleaned.str.replace(INLINE_CURRENCY_PATTERN, "", regex=True)
    cleaned = cleaned.map(_normalize_number_separators)

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

    if unparsed.any():
        fallback = text.loc[unparsed].map(lambda value: _parse_flexible_date(value, dayfirst=dayfirst))
        parsed.loc[fallback.index] = fallback

    return parsed


def _date_formats(dayfirst: bool) -> tuple[str, ...]:
    return DAYFIRST_DATE_FORMATS if dayfirst else MONTHFIRST_DATE_FORMATS


def _normalize_number_separators(value: object) -> object:
    if pd.isna(value):
        return value

    text = str(value)
    if "." in text and "," in text:
        if text.rfind(",") > text.rfind("."):
            return text.replace(".", "").replace(",", ".")
        return text.replace(",", "")

    if "," in text:
        if re.fullmatch(r"-?\d+,\d{1,2}", text):
            return text.replace(",", ".")
        return text.replace(",", "")

    return text


def _parse_flexible_date(value: object, dayfirst: bool) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT

    text = str(value).strip()
    if not text or not _has_day_component(text):
        return pd.NaT

    try:
        parsed = parser.parse(
            text,
            dayfirst=dayfirst,
            fuzzy=False,
            default=datetime(1900, 1, 1),
        )
    except (ParserError, OverflowError, ValueError):
        return pd.NaT

    return pd.Timestamp(parsed.date())


def _has_day_component(text: str) -> bool:
    return re.search(r"\b\d{1,2}\b", text) is not None
