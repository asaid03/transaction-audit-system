import pandas as pd
import pytest

from transaction_audit.config import ReviewThreshold, ValidationConfig
from transaction_audit.validation import validate_transactions
from transaction_audit.risk import score_transactions
from transaction_audit.api import analyse_data


def transactions():
    return pd.DataFrame({
        "transaction_id": ["a", "b", "c", "d"],
        "transaction_date": ["2026-01-01"] * 4,
        "account_id": ["ops"] * 4,
        "counterparty": ["A", "B", "C", "D"],
        "currency": ["GBP", "USD", "GBP", "GBP"],
        "amount": [5000, 50000, -5000, 4999.99],
    })


def test_explicit_currency_boundary_and_credit_scope():
    config = ValidationConfig(review_threshold=ReviewThreshold("GBP", 5000))
    result = validate_transactions(transactions(), config)
    issues = result.issues_dataframe()
    assert issues.loc[issues.issue_type.eq("large_amount"), "transaction_id"].tolist() == ["a", "c"]
    scored = score_transactions(result.transactions, issues, config)
    assert scored.review_priority_score.tolist() == [65, 0, 65, 0]


def test_default_and_api_coverage():
    frame = transactions()
    mapping = {column: column for column in frame}
    payload = analyse_data(frame, "test.csv", mapping)
    assert payload["threshold_coverage"] == {"GBP": None, "USD": None}
    assert payload["finding_count"] == 0
    configured = analyse_data(frame, "test.csv", mapping, config=ValidationConfig(review_threshold=ReviewThreshold("GBP", 5000)))
    assert configured["threshold_coverage"] == {"GBP": 5000, "USD": None}
    assert configured["finding_count"] == 2


@pytest.mark.parametrize("currency,amount", [
    ("GBP", 0), ("GBP", -1), ("GBP", float("nan")), ("GBP", float("inf")),
    ("GBP", True), ("gbp", 5), ("GBP", 10**320),
])
def test_invalid_threshold_configuration(currency, amount):
    with pytest.raises(ValueError):
        ReviewThreshold(currency, amount)
