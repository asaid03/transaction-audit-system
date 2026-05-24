import csv
import io
from pathlib import Path
from typing import BinaryIO

import pandas as pd


def load_dataset(source: str | Path | BinaryIO, filename: str | None = None) -> pd.DataFrame:
    suffix_source = filename or str(source)
    suffix = Path(suffix_source).suffix.lower()

    if suffix == ".csv":
        dataframe = _read_csv_preserving_raw_values(source)
    elif suffix in {".xlsx", ".xls"}:
        dataframe = pd.read_excel(source, dtype=str, keep_default_na=False)
    else:
        raise ValueError("Unsupported file type. Please provide a CSV or Excel file.")

    return dataframe


def load_transactions(source: str | Path | BinaryIO, filename: str | None = None) -> pd.DataFrame:
    return load_dataset(source, filename=filename)


def _read_csv_preserving_raw_values(source: str | Path | BinaryIO) -> pd.DataFrame:
    encodings = ("utf-8-sig", "utf-8", "cp1252")
    last_error: UnicodeDecodeError | None = None

    for encoding in encodings:
        try:
            _rewind(source)
            text = _read_text(source, encoding=encoding)
            rows = list(csv.reader(io.StringIO(text)))
            if _has_ragged_rows(rows):
                return _dataframe_from_rows(rows)

            _rewind(source)
            return pd.read_csv(source, dtype=str, keep_default_na=False, encoding=encoding)
        except pd.errors.ParserError:
            _rewind(source)
            return _dataframe_from_rows(rows)
        except UnicodeDecodeError as exc:
            last_error = exc

    raise ValueError("Could not decode CSV file with utf-8-sig, utf-8, or cp1252.") from last_error


def _has_ragged_rows(rows: list[list[str]]) -> bool:
    widths = {len(row) for row in rows}
    return len(widths) > 1


def _dataframe_from_rows(rows: list[list[str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    width = max(len(row) for row in rows)
    padded_rows = [[*row, *([""] * (width - len(row)))] for row in rows]
    columns = padded_rows[0]
    return pd.DataFrame(padded_rows[1:], columns=columns, dtype=str)


def _read_text(source: str | Path | BinaryIO, encoding: str) -> str:
    if isinstance(source, (str, Path)):
        return Path(source).read_text(encoding=encoding)

    content = source.read()
    if isinstance(content, bytes):
        return content.decode(encoding)
    return content


def _rewind(source: str | Path | BinaryIO) -> None:
    if hasattr(source, "seek"):
        source.seek(0)
