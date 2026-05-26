from __future__ import annotations

import re

import pandas as pd


PREFIX_PATTERNS = (
    r"^payment\s+to\s+",
    r"^payment\s+from\s+",
    r"^supplier\s+pmt\s+",
    r"^supplier\s+payment\s+",
    r"^cash\s+transfer\s+to\s+",
    r"^cash\s+transfer\s+",
    r"^evc\s+payment\s+to\s+",
    r"^mpesa\s+paybill\s+\d+\s+",
    r"^paybill\s+\d+\s+",
    r"^online\s+transfer\s+to\s+",
    r"^trf\s+to\s+",
    r"^refund\s+from\s+",
)
TRAILING_REFERENCE_PATTERN = re.compile(
    r"\s+\b(?:inv|invoice|ref|reference|beneficiary|ben|txn|trx|receipt|rrn)\b"
    r"[\s:\/#-]*[a-z0-9\/#-]+.*$",
    flags=re.IGNORECASE,
)
TRAILING_BENEFICIARY_PATTERN = re.compile(
    r"\s+\b(?:ben[\s:\/#-]*\d+|beneficiary[\s:\/#-]*[a-z0-9\/#-]+).*$",
    flags=re.IGNORECASE,
)
TRAILING_DATE_PATTERN = re.compile(
    r"\s+\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{2,4}\b.*$",
    flags=re.IGNORECASE,
)
NOISE_TOKENS = {"ltd", "limited"}


def extract_counterparty_from_narration(value: object) -> str:
    if pd.isna(value):
        return ""

    text = " ".join(str(value).strip().split())
    if not text:
        return ""

    candidate = text
    for pattern in PREFIX_PATTERNS:
        updated = re.sub(pattern, "", candidate, count=1, flags=re.IGNORECASE).strip()
        if updated != candidate:
            candidate = updated
            break

    candidate = TRAILING_BENEFICIARY_PATTERN.sub("", candidate).strip(" ,-")
    candidate = TRAILING_REFERENCE_PATTERN.sub("", candidate).strip(" ,-")
    candidate = TRAILING_DATE_PATTERN.sub("", candidate).strip(" ,-")
    candidate = _remove_noise_tokens(candidate)
    return candidate or text


def extract_counterparties_from_narration(series: pd.Series) -> pd.Series:
    return series.map(extract_counterparty_from_narration).astype("string")


def _remove_noise_tokens(value: str) -> str:
    tokens = [token for token in value.split() if token.lower().strip(".,") not in NOISE_TOKENS]
    return " ".join(tokens).strip()
