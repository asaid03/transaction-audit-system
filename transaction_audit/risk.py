from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.config import ValidationConfig
from transaction_audit.parsing import parse_amounts
from transaction_audit.types import ISSUE_COLUMNS


RISK_COLUMNS = [
    "review_priority_score",
    "review_priority_band",
    "review_priority_drivers",
]
BAND_ORDER = {"low": 0, "medium": 1, "high": 2}
BAND_LABELS = {"low": "Low", "medium": "Medium", "high": "High"}
MIN_RARE_COUNTERPARTY_ROWS = 10


@dataclass(frozen=True)
class RiskSignal:
    points: int
    band: str
    driver: str


ISSUE_SIGNALS = {
    "missing_column": RiskSignal(95, "high", "Required column is missing"),
    "duplicate_transaction_id": RiskSignal(90, "high", "Duplicate transaction ID"),
    "beneficiary_overpaid": RiskSignal(90, "high", "Beneficiary received more than expected"),
    "budget_line_overspent": RiskSignal(90, "high", "Budget line is overspent"),
    "invalid_amount": RiskSignal(85, "high", "Amount could not be parsed"),
    "invalid_date": RiskSignal(80, "high", "Invalid transaction date"),
    "invalid_transaction_date": RiskSignal(80, "high", "Invalid transaction date"),
    "unknown_vendor_reference": RiskSignal(80, "high", "Counterparty was not found in the vendor file"),
    "unknown_beneficiary_reference": RiskSignal(80, "high", "Beneficiary was not found in the register"),
    "missing_value": RiskSignal(75, "high", "Required transaction field is missing"),
    "missing_required_value": RiskSignal(75, "high", "Required transaction field is missing"),
    "unknown_budget_line": RiskSignal(75, "high", "Budget line was not found in the budget file"),
    "duplicate_vendor_reference": RiskSignal(75, "high", "Duplicate vendor key in reference file"),
    "duplicate_beneficiary_reference": RiskSignal(75, "high", "Duplicate beneficiary key in reference file"),
    "duplicate_budget_reference": RiskSignal(75, "high", "Duplicate budget key in reference file"),
    "possible_duplicate_payment": RiskSignal(70, "high", "Possible duplicate payment"),
    "large_amount": RiskSignal(65, "medium", "Amount exceeds configured threshold"),
    "unsupported_currency": RiskSignal(60, "medium", "Unsupported currency"),
    "possible_vendor_match": RiskSignal(45, "medium", "Counterparty only matched vendor file fuzzily"),
    "zero_amount": RiskSignal(35, "low", "Zero amount transaction"),
}
SEVERITY_FALLBACKS = {
    "error": RiskSignal(65, "high", "Validation error requires review"),
    "warning": RiskSignal(30, "medium", "Validation warning requires review"),
}
RARE_COUNTERPARTY_SIGNAL = RiskSignal(10, "low", "Counterparty appears once in file")


def score_issues(issues: pd.DataFrame) -> pd.DataFrame:
    if issues.empty:
        return pd.DataFrame(columns=[*ISSUE_COLUMNS, *RISK_COLUMNS])

    scored = issues.copy()
    group_signals = _signals_by_issue_group(scored)
    scores = [
        _score_signals(group_signals[_issue_group_key(issue)])
        for issue in scored.to_dict("records")
    ]

    for column in RISK_COLUMNS:
        scored[column] = [score[column] for score in scores]
    return scored


def score_transactions(
    transactions: pd.DataFrame,
    issues: pd.DataFrame,
    config: ValidationConfig | None = None,
) -> pd.DataFrame:
    config = config or ValidationConfig()
    review = transactions.copy().reset_index(drop=True)
    review.insert(0, "row_number", range(2, len(review) + 2))

    issue_signals = _signals_by_transaction_row(issues)
    issue_types = _issue_types_by_transaction_row(issues)
    amount_signals = _large_amount_signals(review, config.large_amount_threshold)
    counterparty_signals = _rare_counterparty_signals(review)

    score_rows = []
    for row in review.to_dict("records"):
        row_number = int(row["row_number"])
        signals = [
            *issue_signals.get(row_number, []),
            *amount_signals.get(row_number, []),
            *counterparty_signals.get(row_number, []),
        ]
        score = _score_signals(signals)
        score_rows.append(
            {
                **score,
                "issue_types": ", ".join(issue_types.get(row_number, [])),
            }
        )

    score_frame = pd.DataFrame(score_rows, columns=[*RISK_COLUMNS, "issue_types"])
    return pd.concat([score_frame, review], axis=1)


def priority_label_from_band(band: object) -> str:
    return BAND_LABELS.get(str(band).lower(), "Low")


def _signals_by_issue_group(issues: pd.DataFrame) -> dict[tuple[object, ...], list[RiskSignal]]:
    signals_by_group: dict[tuple[object, ...], list[RiskSignal]] = {}
    for issue in issues.to_dict("records"):
        key = _issue_group_key(issue)
        signals_by_group.setdefault(key, []).append(_signal_for_issue(issue))
    return signals_by_group


def _signals_by_transaction_row(issues: pd.DataFrame) -> dict[int, list[RiskSignal]]:
    if issues.empty or "row_number" not in issues:
        return {}

    signals: dict[int, list[RiskSignal]] = {}
    transaction_issues = issues[issues["dataset"].eq("transactions")].copy()
    for issue in transaction_issues.to_dict("records"):
        row_number = _safe_int(issue.get("row_number"))
        if row_number is None:
            continue
        signals.setdefault(row_number, []).append(_signal_for_issue(issue))
    return signals


def _issue_types_by_transaction_row(issues: pd.DataFrame) -> dict[int, list[str]]:
    if issues.empty or "row_number" not in issues:
        return {}

    issue_types: dict[int, list[str]] = {}
    transaction_issues = issues[issues["dataset"].eq("transactions")].copy()
    for issue in transaction_issues.to_dict("records"):
        row_number = _safe_int(issue.get("row_number"))
        if row_number is None:
            continue
        issue_type = str(issue.get("issue_type", ""))
        if issue_type and issue_type not in issue_types.setdefault(row_number, []):
            issue_types[row_number].append(issue_type)
    return issue_types


def _signal_for_issue(issue: dict[str, object]) -> RiskSignal:
    issue_type = str(issue.get("issue_type", ""))
    if issue_type in ISSUE_SIGNALS:
        return ISSUE_SIGNALS[issue_type]
    severity = str(issue.get("severity", "")).lower()
    return SEVERITY_FALLBACKS.get(severity, RiskSignal(10, "low", "Issue should be reviewed"))


def _large_amount_signals(transactions: pd.DataFrame, threshold: float) -> dict[int, list[RiskSignal]]:
    if "amount" not in transactions:
        return {}

    amounts = parse_amounts(transactions["amount"])
    signals: dict[int, list[RiskSignal]] = {}
    for index, amount in amounts.items():
        if pd.notna(amount) and abs(float(amount)) >= threshold:
            row_number = int(transactions.loc[index, "row_number"])
            signals.setdefault(row_number, []).append(ISSUE_SIGNALS["large_amount"])
    return signals


def _rare_counterparty_signals(transactions: pd.DataFrame) -> dict[int, list[RiskSignal]]:
    if "counterparty" not in transactions:
        return {}
    if len(transactions) < MIN_RARE_COUNTERPARTY_ROWS:
        return {}

    normalized = transactions["counterparty"].astype("string").str.strip().str.lower()
    counts = normalized[normalized.notna() & normalized.ne("")].value_counts()
    signals: dict[int, list[RiskSignal]] = {}
    for index, value in normalized.items():
        if pd.notna(value) and str(value) != "" and counts.get(value, 0) == 1:
            row_number = int(transactions.loc[index, "row_number"])
            signals.setdefault(row_number, []).append(RARE_COUNTERPARTY_SIGNAL)
    return signals


def _score_signals(signals: list[RiskSignal]) -> dict[str, object]:
    unique = _unique_signals(signals)
    if not unique:
        return {
            "review_priority_score": 0,
            "review_priority_band": "low",
            "review_priority_drivers": "No priority drivers detected.",
        }

    score = min(sum(signal.points for signal in unique), 100)
    band = max(unique, key=lambda signal: BAND_ORDER[signal.band]).band
    return {
        "review_priority_score": score,
        "review_priority_band": band,
        "review_priority_drivers": "; ".join(signal.driver for signal in unique),
    }


def _unique_signals(signals: list[RiskSignal]) -> list[RiskSignal]:
    unique_by_key: dict[tuple[int, str, str], RiskSignal] = {}
    for signal in signals:
        unique_by_key.setdefault((signal.points, signal.band, signal.driver), signal)

    unique = list(unique_by_key.values())
    return sorted(
        unique,
        key=lambda signal: (-BAND_ORDER[signal.band], -signal.points, signal.driver),
    )


def _issue_group_key(issue: dict[str, object]) -> tuple[object, ...]:
    return (
        issue.get("dataset"),
        _display_key(issue.get("row_number")),
        _display_key(issue.get("transaction_id")),
        _display_key(issue.get("related_dataset")),
        _display_key(issue.get("related_record_id")),
    )


def _display_key(value: object) -> object:
    if pd.isna(value):
        return ""
    return value


def _safe_int(value: object) -> int | None:
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
