from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from transaction_audit.parsing import parse_amounts
from transaction_audit.provenance import source_row_numbers


ANOMALY_COLUMNS = [
    "ml_anomaly_score",
    "ml_anomaly_band",
    "ml_anomaly_drivers",
    "ml_model_flag",
    "ml_model_decision",
]
MIN_TRAINING_ROWS = 50


@dataclass(frozen=True)
class AnomalyConfig:
    amount_quantile: float = 0.95
    minimum_amount_threshold: float = 10000.0
    min_training_rows: int = MIN_TRAINING_ROWS
    isolation_forest_contamination: float = 0.05
    isolation_forest_estimators: int = 200
    random_state: int = 42


@dataclass(frozen=True)
class AnomalyModel:
    amount_threshold: float | None
    config: AnomalyConfig
    isolation_forest: IsolationForest | None = None


def fit_anomaly_model(
    transactions: pd.DataFrame,
    config: AnomalyConfig | None = None,
) -> AnomalyModel:
    config = config or AnomalyConfig()
    amounts = _amounts(transactions)
    amount_threshold = _amount_threshold(amounts, config)
    model = _fit_isolation_forest(transactions, config)
    return AnomalyModel(
        amount_threshold=amount_threshold,
        config=config,
        isolation_forest=model,
    )


def score_transaction_anomalies(
    transactions: pd.DataFrame,
    model: AnomalyModel | None = None,
    config: AnomalyConfig | None = None,
) -> pd.DataFrame:
    model = model or fit_anomaly_model(transactions, config=config)
    scored = transactions.copy().reset_index(drop=True)
    scored.insert(0, "row_number", source_row_numbers(scored).to_numpy())

    score_frame = _score_with_isolation_forest(scored, model)
    return pd.concat([score_frame, scored], axis=1)


def evaluate_anomaly_detection(
    scored: pd.DataFrame,
    labels: pd.Series,
    threshold: int = 50,
) -> dict[str, float | int]:
    predicted = (
        scored["ml_model_flag"].astype(bool).reset_index(drop=True)
        if "ml_model_flag" in scored
        else scored["ml_anomaly_score"].ge(threshold).reset_index(drop=True)
    )
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


def _fit_isolation_forest(
    transactions: pd.DataFrame,
    config: AnomalyConfig,
) -> IsolationForest | None:
    if len(transactions) < config.min_training_rows:
        return None

    features = _feature_matrix(transactions)
    model = IsolationForest(
        n_estimators=config.isolation_forest_estimators,
        contamination=config.isolation_forest_contamination,
        random_state=config.random_state,
    )
    model.fit(features)
    return model


def _feature_matrix(transactions: pd.DataFrame) -> pd.DataFrame:
    amounts = _amounts(transactions).abs().fillna(0)
    invoice_values = _invoice_values(transactions)
    repeated_invoice = _repeated_invoice_mask(transactions).astype(int)
    missing_invoice = invoice_values.eq("").astype(int) if invoice_values is not None else 0
    entity_frequency = _entity_frequency(transactions)

    return pd.DataFrame(
        {
            "log_amount": np.log1p(amounts),
            "amount_rank": amounts.rank(pct=True).fillna(0),
            "missing_invoice": missing_invoice,
            "repeated_invoice": repeated_invoice,
            "entity_frequency": entity_frequency,
        },
        index=transactions.index,
    )


def _repeated_invoice_mask(transactions: pd.DataFrame) -> pd.Series:
    if "invoice_reference" not in transactions or "amount" not in transactions:
        return pd.Series(False, index=transactions.index)

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
        return pd.Series(False, index=transactions.index)

    key_columns = ["_invoice_reference", "_amount_key"]
    for optional_key in ("vendor_id", "counterparty"):
        if optional_key in usable:
            key_columns.append(optional_key)
            break

    duplicate_mask = usable.duplicated(subset=key_columns, keep="first")
    result = pd.Series(False, index=transactions.index)
    result.loc[usable.loc[duplicate_mask].index] = True
    return result


def _invoice_values(transactions: pd.DataFrame) -> pd.Series | None:
    if "invoice_reference" not in transactions:
        return None
    return transactions["invoice_reference"].astype("string").str.strip().fillna("")


def _entity_frequency(transactions: pd.DataFrame) -> pd.Series:
    for column in ("vendor_id", "counterparty"):
        if column in transactions:
            values = transactions[column].astype("string").str.strip().fillna("")
            return values.map(values.value_counts()).astype(float)
    return pd.Series(0.0, index=transactions.index)


def _score_with_isolation_forest(
    transactions: pd.DataFrame,
    model: AnomalyModel,
) -> pd.DataFrame:
    if model.isolation_forest is None:
        return pd.DataFrame(
            {
                "ml_anomaly_score": [0] * len(transactions),
                "ml_anomaly_band": ["low"] * len(transactions),
                "ml_anomaly_drivers": ["Model not run: fewer than 50 transactions."] * len(transactions),
                "ml_model_flag": [False] * len(transactions),
                "ml_model_decision": [pd.NA] * len(transactions),
            },
            columns=ANOMALY_COLUMNS,
        )

    features = _feature_matrix(transactions)
    decisions = pd.Series(
        model.isolation_forest.decision_function(features),
        index=transactions.index,
        dtype="float64",
    )
    flags = pd.Series(
        model.isolation_forest.predict(features) == -1,
        index=transactions.index,
        dtype="bool",
    )
    scores = _normalized_model_scores(decisions, flags)
    amounts = _amounts(transactions).abs()
    repeated_invoice = _repeated_invoice_mask(transactions)
    invoice_values = _invoice_values(transactions)
    entity_frequency = _entity_frequency(transactions)
    drivers = [
        _model_drivers(
            index,
            model,
            amounts=amounts,
            repeated_invoice=repeated_invoice,
            invoice_values=invoice_values,
            entity_frequency=entity_frequency,
        )
        if flags.loc[index]
        else "Not flagged by the anomaly model."
        for index in transactions.index
    ]
    return pd.DataFrame(
        {
            "ml_anomaly_score": scores,
            "ml_anomaly_band": scores.map(_band_for_score),
            "ml_anomaly_drivers": drivers,
            "ml_model_flag": flags,
            "ml_model_decision": decisions.round(6),
        },
        columns=ANOMALY_COLUMNS,
    )


def _normalized_model_scores(decisions: pd.Series, flags: pd.Series) -> pd.Series:
    unusualness = -decisions
    scores = pd.Series(0, index=decisions.index, dtype="int64")

    inlier_values = unusualness[~flags]
    if len(inlier_values) > 1:
        ranks = inlier_values.rank(method="average")
        scores.loc[inlier_values.index] = ((ranks - 1) / (len(ranks) - 1) * 49).round().astype(int)

    outlier_values = unusualness[flags]
    if len(outlier_values) == 1:
        scores.loc[outlier_values.index] = 100
    elif len(outlier_values) > 1:
        ranks = outlier_values.rank(method="average")
        scores.loc[outlier_values.index] = (
            50 + ((ranks - 1) / (len(ranks) - 1) * 50).round()
        ).astype(int)
    return scores


def _model_drivers(
    index: int,
    model: AnomalyModel,
    *,
    amounts: pd.Series,
    repeated_invoice: pd.Series,
    invoice_values: pd.Series | None,
    entity_frequency: pd.Series,
) -> str:
    drivers = []
    if (
        model.amount_threshold is not None
        and pd.notna(amounts.loc[index])
        and amounts.loc[index] >= model.amount_threshold
    ):
        drivers.append(
            "Amount is in the highest 5% of this file "
            f"(at or above {model.amount_threshold:,.2f})"
        )

    if repeated_invoice.loc[index]:
        drivers.append("The invoice/reference and amount repeat for the same payee")

    if invoice_values is not None and invoice_values.loc[index] == "":
        drivers.append("The invoice/reference is missing")

    if entity_frequency.loc[index] <= 1:
        drivers.append("The payee appears only once in this file")

    if not drivers:
        drivers.append(
            "The combination of amount, invoice status, and payee frequency differs from most payments"
        )
    return "; ".join(drivers)


def _band_for_score(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"
