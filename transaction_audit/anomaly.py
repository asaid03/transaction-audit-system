from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.parsing import parse_amounts


ANOMALY_COLUMNS = [
    "ml_anomaly_score",
    "ml_anomaly_band",
    "ml_anomaly_drivers",
]
MIN_TRAINING_ROWS = 50


@dataclass(frozen=True)
class AnomalyConfig:
    amount_quantile: float = 0.95
    minimum_amount_threshold: float = 10000.0
    min_training_rows: int = MIN_TRAINING_ROWS
    amount_signal_points: int = 70
    repeated_invoice_points: int = 85
    missing_invoice_points: int = 65


@dataclass(frozen=True)
class AnomalyModel:
    amount_threshold: float | None
    config: AnomalyConfig


def fit_anomaly_model(
    transactions: pd.DataFrame,
    config: AnomalyConfig | None = None,
) -> AnomalyModel:
    config = config or AnomalyConfig()
    amounts = _amounts(transactions)
    amount_threshold = _amount_threshold(amounts, config)
    return AnomalyModel(amount_threshold=amount_threshold, config=config)


def score_transaction_anomalies(
    transactions: pd.DataFrame,
    model: AnomalyModel | None = None,
    config: AnomalyConfig | None = None,
) -> pd.DataFrame:
    model = model or fit_anomaly_model(transactions, config=config)
    scored = transactions.copy().reset_index(drop=True)
    scored.insert(0, "row_number", range(2, len(scored) + 2))

    signals_by_index: dict[int, list[tuple[int, str]]] = {}
    _add_amount_signals(scored, model, signals_by_index)
    _add_repeated_invoice_signals(scored, model.config, signals_by_index)
    _add_missing_invoice_signals(scored, model.config, signals_by_index)

    score_rows = [_score_signals(signals_by_index.get(index, [])) for index in scored.index]
    score_frame = pd.DataFrame(score_rows, columns=ANOMALY_COLUMNS)
    return pd.concat([score_frame, scored], axis=1)


def evaluate_anomaly_detection(
    scored: pd.DataFrame,
    labels: pd.Series,
    threshold: int = 50,
) -> dict[str, float | int]:
    predicted = scored["ml_anomaly_score"].ge(threshold).reset_index(drop=True)
    actual = labels.astype(str).str.strip().isin({"1", "true", "True"}).reset_index(drop=True)

    true_positive = int((predicted & actual).sum())
    false_positive = int((predicted & ~actual).sum())
    false_negative = int((~predicted & actual).sum())
    true_negative = int((~predicted & ~actual).sum())

    precision_denominator = true_positive + false_positive
    recall_denominator = true_positive + false_negative
    precision = true_positive / precision_denominator if precision_denominator else 0.0
    recall = true_positive / recall_denominator if recall_denominator else 0.0

    return {
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "true_negative": true_negative,
        "precision": precision,
        "recall": recall,
    }


def _amounts(transactions: pd.DataFrame) -> pd.Series:
    if "amount" not in transactions:
        return pd.Series(dtype="float64")
    if pd.api.types.is_numeric_dtype(transactions["amount"]):
        return transactions["amount"].astype(float)
    return parse_amounts(transactions["amount"])


def _amount_threshold(amounts: pd.Series, config: AnomalyConfig) -> float | None:
    valid_amounts = amounts.dropna().abs()
    if len(valid_amounts) < config.min_training_rows:
        return None

    threshold = valid_amounts.quantile(config.amount_quantile)
    if pd.isna(threshold):
        return None
    return float(max(config.minimum_amount_threshold, threshold))


def _add_amount_signals(
    transactions: pd.DataFrame,
    model: AnomalyModel,
    signals_by_index: dict[int, list[tuple[int, str]]],
) -> None:
    if model.amount_threshold is None or "amount" not in transactions:
        return

    amounts = _amounts(transactions).abs()
    for index in amounts[amounts.ge(model.amount_threshold)].index:
        _add_signal(
            signals_by_index,
            index,
            model.config.amount_signal_points,
            f"Amount is at or above trained high-amount threshold {model.amount_threshold:,.2f}",
        )


def _add_repeated_invoice_signals(
    transactions: pd.DataFrame,
    config: AnomalyConfig,
    signals_by_index: dict[int, list[tuple[int, str]]],
) -> None:
    if "invoice_reference" not in transactions or "amount" not in transactions:
        return

    invoice_values = transactions["invoice_reference"].astype("string").str.strip()
    usable = transactions.copy()
    usable["_invoice_reference"] = invoice_values
    usable["_amount_key"] = _amounts(transactions).round(2)
    usable = usable[
        usable["_invoice_reference"].notna()
        & usable["_invoice_reference"].ne("")
        & usable["_amount_key"].notna()
    ]
    if usable.empty:
        return

    key_columns = ["_invoice_reference", "_amount_key"]
    for optional_key in ("vendor_id", "counterparty"):
        if optional_key in usable:
            key_columns.append(optional_key)
            break

    duplicate_mask = usable.duplicated(subset=key_columns, keep="first")
    for index in usable.loc[duplicate_mask].index:
        _add_signal(
            signals_by_index,
            index,
            config.repeated_invoice_points,
            "Invoice/reference repeats on a later transaction with matching payment details",
        )


def _add_missing_invoice_signals(
    transactions: pd.DataFrame,
    config: AnomalyConfig,
    signals_by_index: dict[int, list[tuple[int, str]]],
) -> None:
    if "invoice_reference" not in transactions:
        return

    missing = transactions["invoice_reference"].astype("string").str.strip().fillna("").eq("")
    for index in missing[missing].index:
        _add_signal(
            signals_by_index,
            index,
            config.missing_invoice_points,
            "Invoice/reference is missing",
        )


def _add_signal(
    signals_by_index: dict[int, list[tuple[int, str]]],
    index: int,
    points: int,
    driver: str,
) -> None:
    signals_by_index.setdefault(index, []).append((points, driver))


def _score_signals(signals: list[tuple[int, str]]) -> dict[str, object]:
    unique = sorted(set(signals), key=lambda signal: (-signal[0], signal[1]))
    if not unique:
        return {
            "ml_anomaly_score": 0,
            "ml_anomaly_band": "low",
            "ml_anomaly_drivers": "No anomaly drivers detected.",
        }

    score = min(sum(points for points, _ in unique), 100)
    return {
        "ml_anomaly_score": score,
        "ml_anomaly_band": _band_for_score(score),
        "ml_anomaly_drivers": "; ".join(driver for _, driver in unique),
    }


def _band_for_score(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"
