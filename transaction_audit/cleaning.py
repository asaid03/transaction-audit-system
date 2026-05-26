from __future__ import annotations

import re
from dataclasses import dataclass, field

import pandas as pd

from transaction_audit.parsing import parse_amounts
from transaction_audit.schema import normalize_column_key


SOURCE_ROW_NUMBER = "source_row_number"
MINIMUM_HEADER_SCORE = 8

TRANSACTION_ID_KEYS = {
    "ref",
    "ref no",
    "reference",
    "reference no",
    "transaction_id",
    "transaction id",
    "transaction ref",
    "txn ref",
}
DATE_KEYS = {"date", "value date", "transaction_date", "transaction date", "txn date", "posting date"}
AMOUNT_KEYS = {"amount", "transaction amount", "value"}
DEBIT_KEYS = {"debit", "debit amount", "withdrawal", "money out", "paid out"}
CREDIT_KEYS = {"credit", "credit amount", "deposit", "money in", "paid in"}
CURRENCY_KEYS = {"currency", "ccy"}
ACCOUNT_KEYS = {"account", "account_id", "account id", "account number", "account no"}
NARRATION_KEYS = {
    "narration",
    "description",
    "details",
    "particulars",
    "narrative",
    "memo",
    "counterparty",
}
BALANCE_KEYS = {"balance", "running balance", "closing balance"}
KNOWN_CURRENCIES = ("USD", "GBP", "EUR", "KES", "SOS")


@dataclass(frozen=True)
class CleaningResult:
    dataframe: pd.DataFrame
    applied: bool
    source_header_row: int | None = None
    dropped_rows: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class HeaderCandidate:
    row_number: int
    values: list[str]
    data_start_position: int
    score: int


def clean_transaction_export(
    dataframe: pd.DataFrame,
    default_currency: str | None = None,
    default_account_id: str | None = None,
) -> CleaningResult:
    rows = _rows_with_source_numbers(dataframe)
    header = _best_header_candidate(rows)
    if header is None:
        return CleaningResult(
            dataframe=dataframe,
            applied=False,
            warnings=["No transaction-like header row was found."],
        )

    source_columns = _unique_columns(header.values)
    body_rows = rows[header.data_start_position :]
    source_frame, dropped_rows = _body_dataframe(body_rows, source_columns)

    if source_frame.empty:
        return CleaningResult(
            dataframe=source_frame,
            applied=True,
            source_header_row=header.row_number,
            dropped_rows=dropped_rows,
            warnings=["Transaction header was found, but no transaction rows were detected."],
            transformations=[f"Detected transaction header on source row {header.row_number}."],
        )

    metadata_text = " ".join(
        " ".join(row_values)
        for _, row_values in rows[: max(header.data_start_position, 0)]
    )
    account_id = default_account_id or _infer_account_id(metadata_text)
    currency = default_currency or _infer_currency(metadata_text)
    cleaned = _canonical_transaction_frame(source_frame, account_id=account_id, currency=currency)

    transformations = [f"Detected transaction header on source row {header.row_number}."]
    if _find_column(source_frame, DEBIT_KEYS) or _find_column(source_frame, CREDIT_KEYS):
        transformations.append("Derived amount from debit/credit columns.")
    if account_id:
        transformations.append("Inferred account_id from statement metadata.")
    if currency:
        transformations.append("Inferred currency from statement metadata.")
    if cleaned["transaction_id"].str.startswith("generated-").any():
        transformations.append("Generated transaction IDs for rows without references.")

    return CleaningResult(
        dataframe=cleaned,
        applied=True,
        source_header_row=header.row_number,
        dropped_rows=dropped_rows,
        transformations=transformations,
    )


def _rows_with_source_numbers(dataframe: pd.DataFrame) -> list[tuple[int, list[str]]]:
    rows = [(1, [_clean_cell(column) for column in dataframe.columns])]
    for position, row in enumerate(dataframe.itertuples(index=False, name=None), start=2):
        rows.append((position, [_clean_cell(value) for value in row]))
    return rows


def _best_header_candidate(rows: list[tuple[int, list[str]]]) -> HeaderCandidate | None:
    candidates = [
        HeaderCandidate(
            row_number=row_number,
            values=values,
            data_start_position=position + 1,
            score=_header_score(values),
        )
        for position, (row_number, values) in enumerate(rows)
    ]
    best = max(candidates, key=lambda candidate: candidate.score, default=None)
    if best is None or best.score < MINIMUM_HEADER_SCORE:
        return None
    return best


def _header_score(values: list[str]) -> int:
    keys = {normalize_column_key(value) for value in values if value}
    score = 0
    if keys & DATE_KEYS:
        score += 4
    if keys & AMOUNT_KEYS:
        score += 4
    if (keys & DEBIT_KEYS) or (keys & CREDIT_KEYS):
        score += 4
    if keys & NARRATION_KEYS:
        score += 3
    if keys & TRANSACTION_ID_KEYS:
        score += 2
    if keys & BALANCE_KEYS:
        score += 1
    return score


def _body_dataframe(
    rows: list[tuple[int, list[str]]],
    source_columns: list[str],
) -> tuple[pd.DataFrame, list[int]]:
    records = []
    dropped_rows = []
    width = len(source_columns)

    footer_seen = False
    for source_row_number, values in rows:
        padded = [*values[:width], *([""] * max(width - len(values), 0))]
        if footer_seen:
            dropped_rows.append(source_row_number)
            continue
        is_footer = _is_footer_row(padded)
        if _is_blank_row(padded) or is_footer:
            dropped_rows.append(source_row_number)
            footer_seen = is_footer
            continue
        record = dict(zip(source_columns, padded))
        record[SOURCE_ROW_NUMBER] = source_row_number
        records.append(record)

    return pd.DataFrame(records), dropped_rows


def _canonical_transaction_frame(
    source: pd.DataFrame,
    account_id: str | None,
    currency: str | None,
) -> pd.DataFrame:
    transaction_id_column = _find_column(source, TRANSACTION_ID_KEYS)
    date_column = _find_column(source, DATE_KEYS)
    narration_column = _find_column(source, NARRATION_KEYS)
    account_column = _find_column(source, ACCOUNT_KEYS)
    currency_column = _find_column(source, CURRENCY_KEYS)
    balance_column = _find_column(source, BALANCE_KEYS)

    cleaned = pd.DataFrame(index=source.index)
    cleaned["transaction_id"] = _transaction_ids(source, transaction_id_column)
    cleaned["transaction_date"] = _column_or_empty(source, date_column)
    cleaned["account_id"] = _column_or_default(source, account_column, account_id)
    cleaned["amount"] = _amounts(source)
    cleaned["currency"] = _column_or_default(source, currency_column, currency)
    cleaned["counterparty"] = _column_or_empty(source, narration_column)
    cleaned["description"] = _column_or_empty(source, narration_column)
    if balance_column:
        cleaned["balance"] = source[balance_column]
    cleaned[SOURCE_ROW_NUMBER] = source[SOURCE_ROW_NUMBER].astype(int)
    return cleaned.reset_index(drop=True)


def _transaction_ids(source: pd.DataFrame, column: str | None) -> pd.Series:
    raw = _column_or_empty(source, column).astype("string").str.strip()
    generated = pd.Series(
        [f"generated-{position:06d}" for position in range(1, len(source) + 1)],
        index=source.index,
        dtype="string",
    )
    return raw.mask(raw.eq("") | raw.isna(), generated)


def _amounts(source: pd.DataFrame) -> pd.Series:
    amount_column = _find_column(source, AMOUNT_KEYS)
    if amount_column:
        return _format_amounts(parse_amounts(source[amount_column]))

    debit_column = _find_column(source, DEBIT_KEYS)
    credit_column = _find_column(source, CREDIT_KEYS)
    debit = parse_amounts(_column_or_empty(source, debit_column))
    credit = parse_amounts(_column_or_empty(source, credit_column))
    combined = debit.where(debit.notna(), -credit)
    return _format_amounts(combined)


def _format_amounts(amounts: pd.Series) -> pd.Series:
    return amounts.map(lambda value: "" if pd.isna(value) else f"{float(value):.2f}")


def _column_or_empty(source: pd.DataFrame, column: str | None) -> pd.Series:
    if column is None:
        return pd.Series([""] * len(source), index=source.index, dtype="string")
    return source[column]


def _column_or_default(source: pd.DataFrame, column: str | None, default: str | None) -> pd.Series:
    values = _column_or_empty(source, column).astype("string").str.strip()
    if default:
        values = values.mask(values.eq("") | values.isna(), default)
    return values


def _find_column(source: pd.DataFrame, keys: set[str]) -> str | None:
    for column in source.columns:
        if normalize_column_key(column) in keys:
            return str(column)
    return None


def _unique_columns(values: list[str]) -> list[str]:
    columns = []
    counts: dict[str, int] = {}
    for index, value in enumerate(values, start=1):
        base = value.strip() or f"unnamed_{index}"
        count = counts.get(base, 0) + 1
        counts[base] = count
        columns.append(base if count == 1 else f"{base}_{count}")
    return columns


def _infer_account_id(text: str) -> str | None:
    currency_choices = "|".join(KNOWN_CURRENCIES)
    match = re.search(
        rf"(?:account|wallet):\s*(.+?)(?:\s+-\s+(?:{currency_choices})\b|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip()


def _infer_currency(text: str) -> str | None:
    for currency in KNOWN_CURRENCIES:
        if re.search(rf"\b{currency}\b", text, flags=re.IGNORECASE):
            return currency
    return None


def _is_blank_row(values: list[str]) -> bool:
    return all(not value.strip() for value in values)


def _is_footer_row(values: list[str]) -> bool:
    first_value = next((value.strip().lower() for value in values if value.strip()), "")
    return first_value.startswith(("total", "grand total", "closing balance", "opening balance"))


def _clean_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()
