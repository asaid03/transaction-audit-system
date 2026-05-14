from pathlib import Path
from typing import BinaryIO

import pandas as pd


def load_transactions(source: str | Path | BinaryIO, filename: str | None = None) -> pd.DataFrame:
    suffix_source = filename or str(source)
    suffix = Path(suffix_source).suffix.lower()

    if suffix == ".csv":
        dataframe = _read_csv_preserving_raw_values(source)
    elif suffix in {".xlsx", ".xls"}:
        dataframe = pd.read_excel(source, dtype=str, keep_default_na=False)
    else:
        raise ValueError("Unsupported file type. Please provide a CSV or Excel file.")

    return dataframe


def _read_csv_preserving_raw_values(source: str | Path | BinaryIO) -> pd.DataFrame:
    encodings = ("utf-8-sig", "utf-8", "cp1252")
    last_error: UnicodeDecodeError | None = None

    for encoding in encodings:
        try:
            _rewind(source)
            return pd.read_csv(source, dtype=str, keep_default_na=False, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc

    raise ValueError("Could not decode CSV file with utf-8-sig, utf-8, or cp1252.") from last_error


def _rewind(source: str | Path | BinaryIO) -> None:
    if hasattr(source, "seek"):
        source.seek(0)
