from pathlib import Path
from typing import BinaryIO

import pandas as pd


def load_transactions(source: str | Path | BinaryIO, filename: str | None = None) -> pd.DataFrame:
    suffix_source = filename or str(source)
    suffix = Path(suffix_source).suffix.lower()

    if suffix == ".csv":
        dataframe = pd.read_csv(source)
    elif suffix in {".xlsx", ".xls"}:
        dataframe = pd.read_excel(source)
    else:
        raise ValueError("Unsupported file type. Please provide a CSV or Excel file.")

    return dataframe
