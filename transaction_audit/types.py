from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd


ISSUE_COLUMNS = [
    "dataset",
    "row_number",
    "transaction_id",
    "field",
    "issue_type",
    "severity",
    "related_dataset",
    "related_record_id",
    "message",
]


@dataclass(frozen=True)
class ValidationIssue:
    row_number: int | None
    transaction_id: str | None
    field: str
    issue_type: str
    severity: str
    message: str
    dataset: str = "transactions"
    related_dataset: str | None = None
    related_record_id: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    transactions: pd.DataFrame
    issues: list[ValidationIssue]

    @property
    def is_valid(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    def issues_dataframe(self) -> pd.DataFrame:
        if not self.issues:
            return pd.DataFrame(columns=ISSUE_COLUMNS)
        return pd.DataFrame([asdict(issue) for issue in self.issues], columns=ISSUE_COLUMNS)


@dataclass(frozen=True)
class PreparedTransactions:
    original: pd.DataFrame
    checked: pd.DataFrame
