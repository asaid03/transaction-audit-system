import numpy as np
import pandas as pd
import pytest

from transaction_audit.anomaly import (
    ANOMALY_COLUMNS, AnomalyConfig, _feature_matrix, _pattern_values,
    _prepare_features, fit_anomaly_model, score_transaction_anomalies,
)
from tests.test_anomaly import load_benchmark_transactions


def population(n=80, currency="GBP"):
    return pd.DataFrame({
        "transaction_id": [f"T{i}" for i in range(n)],
        "currency": currency,
        "amount": np.linspace(10, 100, n),
        "counterparty": [f"Supplier {i % 4}" for i in range(n)],
        "invoice_reference": [f"INV{i}" for i in range(n)],
    })


def test_row_reordering_does_not_change_assessment():
    _, frame = load_benchmark_transactions()
    first = score_transaction_anomalies(frame).set_index("transaction_id").sort_index()
    second = score_transaction_anomalies(frame.sample(frac=1, random_state=22)).set_index("transaction_id").sort_index()
    pd.testing.assert_frame_equal(first[ANOMALY_COLUMNS], second[ANOMALY_COLUMNS])


def test_scoring_subset_uses_fitted_population_and_display_scale():
    _, frame = load_benchmark_transactions()
    model = fit_anomaly_model(frame)
    full = score_transaction_anomalies(frame, model).set_index("transaction_id")
    for index in [0, 60, 185, 190, 199]:
        single = score_transaction_anomalies(frame.iloc[[index]], model).set_index("transaction_id")
        pd.testing.assert_frame_equal(full.loc[single.index, ANOMALY_COLUMNS], single[ANOMALY_COLUMNS])


def test_invalid_amounts_and_missing_currencies_are_unavailable():
    frame = population()
    frame.loc[0:3, "amount"] = [np.nan, np.inf, -np.inf, np.nan]
    frame.loc[4, "currency"] = ""
    scored = score_transaction_anomalies(frame)
    assert scored.loc[:3, "ml_model_status"].eq("invalid_amount").all()
    assert scored.loc[4, "ml_model_status"] == "missing_currency"
    assert scored.loc[:4, "ml_model_decision"].isna().all()
    assert not scored.loc[:4, "ml_model_flag"].any()
    assert scored.loc[5:, "ml_model_status"].eq("scored").all()


def test_invalid_rows_do_not_change_valid_model():
    frame = population()
    invalid = frame.iloc[:10].copy()
    invalid["amount"] = np.nan
    first = score_transaction_anomalies(frame)
    second = score_transaction_anomalies(pd.concat([frame, invalid], ignore_index=True))
    pd.testing.assert_frame_equal(first[ANOMALY_COLUMNS], second.iloc[:len(frame)][ANOMALY_COLUMNS])


def test_small_currency_group_is_not_pooled_to_meet_minimum():
    frame = pd.concat([population(), population(20, "USD")], ignore_index=True)
    scored = score_transaction_anomalies(frame)
    assert scored.iloc[:80].ml_model_status.eq("scored").all()
    assert scored.iloc[80:].ml_model_status.eq("insufficient_data").all()
    assert scored.iloc[80:].ml_model_decision.isna().all()


def test_identical_repeats_do_not_satisfy_minimum_training_population():
    frame = population(49)
    duplicated = pd.concat([frame, frame.iloc[:20]], ignore_index=True)
    scored = score_transaction_anomalies(duplicated)
    assert scored.ml_model_status.eq("insufficient_data").all()
    assert scored.ml_model_decision.isna().all()


def test_constant_features_produce_no_artificial_anomaly_or_amount_claim():
    frame = population()
    frame["amount"] = 100
    frame["counterparty"] = "Supplier"
    model = fit_anomaly_model(frame)
    assert model.amount_thresholds == {}
    scored = score_transaction_anomalies(frame, model)
    assert scored.ml_model_status.eq("constant_population").all()
    assert not scored.ml_model_flag.any()
    assert scored.ml_model_decision.isna().all()


def test_amount_context_has_no_fixed_floor_and_uses_currency():
    frame = pd.concat([population(), population(currency="USD")], ignore_index=True)
    frame.loc[frame.currency.eq("USD"), "amount"] *= 1000
    model = fit_anomaly_model(frame)
    assert model.amount_thresholds["GBP"] == pytest.approx(95.5)
    assert model.amount_thresholds["USD"] == pytest.approx(95500)

    frame.loc[0, "amount"] = 3000
    row = score_transaction_anomalies(frame).iloc[0]
    assert row.ml_model_flag
    assert "3,000.00 GBP" in row.ml_anomaly_drivers
    assert "80 valid same-currency" in row.ml_anomaly_drivers


def test_constant_currency_does_not_borrow_variation_or_change_other_scores():
    varied = population()
    constant = population(currency="USD").assign(amount=100, counterparty="Supplier")
    mixed = pd.concat([varied, constant], ignore_index=True)
    scored = score_transaction_anomalies(mixed)
    assert scored.iloc[len(varied):].ml_model_status.eq("constant_population").all()
    assert scored.iloc[len(varied):].ml_model_decision.isna().all()
    pd.testing.assert_frame_equal(scored.iloc[:len(varied)][ANOMALY_COLUMNS],
                                  score_transaction_anomalies(varied)[ANOMALY_COLUMNS])


def test_scaling_one_currency_keeps_features_and_flags_equivalent():
    frame = pd.concat([population(), population(currency="USD")], ignore_index=True)
    changed = frame.copy()
    changed.loc[changed.currency.eq("USD"), "amount"] *= 1000
    first, second = _prepare_features(frame), _prepare_features(changed)
    np.testing.assert_allclose(_feature_matrix(first, first), _feature_matrix(second, second), atol=1e-12)
    left, right = score_transaction_anomalies(frame), score_transaction_anomalies(changed)
    assert left.ml_model_flag.tolist() == right.ml_model_flag.tolist()
    np.testing.assert_allclose(left.ml_model_decision, right.ml_model_decision)


def test_repeated_reference_uses_symmetric_currency_identity_and_invoice_keys():
    frame = pd.DataFrame({
        "amount": [100] * 7,
        "currency": ["GBP", "GBP", "USD", "GBP", "GBP", "GBP", "GBP"],
        "counterparty": ["Acme", " acme ", "Acme", "Acme", "Acme", "Other", "Unknown"],
        "vendor_id": ["", None, "", "V1", "V2", "", ""],
        "invoice_reference": ["INV-001", "inv 1", "INV1", "INV1", "INV1", "INV1", "INV1"],
    })
    prepared = _prepare_features(frame)
    _, repeated = _pattern_values(prepared, prepared)
    assert repeated.tolist() == [True, True, False, False, False, False, False]


@pytest.mark.parametrize("frame", [pd.DataFrame(), population().assign(amount=np.nan)])
def test_empty_or_all_invalid_population_does_not_fit(frame):
    scored = score_transaction_anomalies(frame)
    assert len(scored) == len(frame)
    assert scored.ml_model_decision.isna().all()
    assert not scored.ml_model_flag.any()
