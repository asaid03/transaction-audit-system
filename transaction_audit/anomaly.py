from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from transaction_audit.parsing import parse_amounts
from transaction_audit.provenance import source_row_numbers
from transaction_audit.rules import _payee_match_keys, invoice_match_key


ANOMALY_COLUMNS = [
    "ml_anomaly_score",
    "ml_anomaly_band",
    "ml_anomaly_drivers",
    "ml_model_flag",
    "ml_model_decision",
    "ml_model_status",
]
MIN_TRAINING_ROWS = 50
REPEAT_KEYS = ["currency", "payee", "invoice", "amount"]


@dataclass(frozen=True)
class AnomalyConfig:
    amount_quantile: float = 0.95
    min_training_rows: int = MIN_TRAINING_ROWS
    isolation_forest_contamination: float = 0.05
    isolation_forest_estimators: int = 200
    random_state: int = 42

    def __post_init__(self):
        if not 0 < self.amount_quantile < 1:
            raise ValueError("Amount quantile must be between zero and one.")
        if self.min_training_rows < 2:
            raise ValueError("At least two training rows are required.")
        if not 0 < self.isolation_forest_contamination <= 0.5:
            raise ValueError("Contamination must be greater than zero and at most 0.5.")
        if self.isolation_forest_estimators < 1:
            raise ValueError("At least one estimator is required.")


@dataclass(frozen=True)
class AnomalyModel:
    reference: pd.DataFrame
    constant_currencies: frozenset[str]
    amount_thresholds: dict[str, float]
    config: AnomalyConfig
    isolation_forest: IsolationForest | None = None
    training_decisions: np.ndarray | None = None


def fit_anomaly_model(
    transactions: pd.DataFrame,
    config: AnomalyConfig | None = None,
) -> AnomalyModel:
    config = config or AnomalyConfig()
    reference = _prepare_features(transactions)
    valid = reference["amount"].notna() & reference["currency"].ne("")
    reference = reference.loc[valid].copy()
    # Identical repeated records contribute one training observation. Each source
    # row is still scored using the same features and retains its own evidence.
    has_match_key = reference["invoice"].ne("") & reference["payee"].ne("")
    representative = ~(has_match_key & reference.duplicated(REPEAT_KEYS))
    counts = reference.loc[representative, "currency"].value_counts()
    eligible_currencies = counts[counts.ge(config.min_training_rows)].index
    reference = reference[reference["currency"].isin(eligible_currencies)]
    features = _feature_matrix(reference.loc[representative], reference)
    variation = features.groupby(reference["currency"]).nunique().gt(1).any(axis=1)
    constant_currencies = frozenset(variation.index[~variation])
    features = features.loc[~reference.loc[features.index, "currency"].isin(constant_currencies)]
    reference = reference.loc[~reference["currency"].isin(constant_currencies)]
    thresholds = {}
    for currency, group in reference.groupby("currency"):
        if group["amount"].abs().nunique() > 1:
            thresholds[currency] = float(group["amount"].abs().quantile(config.amount_quantile))
    # Stable training order makes reordering an upload immaterial to its model output.
    features = features.sort_values(list(features.columns)).reset_index(drop=True)
    model = None
    decisions = None
    if not features.empty:
        model = IsolationForest(
            n_estimators=config.isolation_forest_estimators,
            contamination=config.isolation_forest_contamination,
            random_state=config.random_state,
        ).fit(features)
        decisions = np.sort(model.decision_function(features))
    return AnomalyModel(
        reference=reference,
        constant_currencies=constant_currencies,
        amount_thresholds=thresholds,
        config=config,
        isolation_forest=model,
        training_decisions=decisions,
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
        return pd.Series(float("nan"), index=transactions.index)
    if pd.api.types.is_numeric_dtype(transactions["amount"]):
        return transactions["amount"].astype(float).replace([np.inf, -np.inf], np.nan)
    return parse_amounts(transactions["amount"]).astype(float).replace([np.inf, -np.inf], np.nan)


def _prepare_features(transactions: pd.DataFrame) -> pd.DataFrame:
    frame = transactions.copy().reset_index(drop=True)
    if "counterparty" not in frame:
        frame["counterparty"] = pd.NA
    frame["payee"] = _payee_match_keys(frame).fillna("")
    currency = frame.get("currency", pd.Series("", index=frame.index))
    frame["currency"] = currency.astype("string").fillna("").str.strip().str.upper()
    frame.loc[~frame["currency"].str.fullmatch(r"[A-Z]{3}"), "currency"] = ""
    frame["amount"] = _amounts(frame)
    invoice = frame.get("invoice_reference", pd.Series(pd.NA, index=frame.index))
    frame["invoice"] = invoice.map(invoice_match_key).fillna("")
    frame["missing_invoice"] = 0
    if "invoice_reference" in frame:
        frame["missing_invoice"] = invoice.astype("string").fillna("").str.strip().eq("").astype(int)
    return frame[["amount", "currency", "payee", "invoice", "missing_invoice"]]


def _pattern_values(frame: pd.DataFrame, reference: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    frequencies = reference[reference["payee"].ne("")].groupby(["currency", "payee"]).size().to_dict()
    usable = reference[reference["payee"].ne("") & reference["invoice"].ne("")].copy()
    usable["amount"] = usable["amount"].round(2)
    repeats = usable.groupby(REPEAT_KEYS).size().to_dict()
    frequency = pd.Series(
        [frequencies.get(key, 0) for key in zip(frame["currency"], frame["payee"], strict=True)],
        index=frame.index,
        dtype=float,
    )
    keys = frame[REPEAT_KEYS].assign(amount=frame["amount"].round(2))
    repeated = pd.Series(
        [repeats.get(key, 0) > 1 for key in keys.itertuples(index=False, name=None)],
        index=frame.index,
        dtype=bool,
    )
    return frequency, repeated


def _feature_matrix(frame: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    # Every transform uses the fitted reference, including when scoring one row later.
    frequency, repeated = _pattern_values(frame, reference)
    features = pd.DataFrame(
        0.0, index=frame.index,
        columns=["log_amount", "amount_rank", "missing_invoice", "repeated_invoice", "entity_frequency"],
    )
    groups = reference.groupby("currency")["amount"]
    for currency, group in frame.groupby("currency"):
        population = np.sort(groups.get_group(currency).abs().to_numpy())
        amounts = group["amount"].abs().to_numpy()
        positive = population[population > 0]
        scale = float(np.median(positive)) if len(positive) else 1.0
        # Log ratio avoids nominal currency units dominating pooled model features.
        scaled = np.zeros(len(amounts))
        nonzero = amounts > 0
        scaled[nonzero] = np.logaddexp(0, np.log(amounts[nonzero]) - np.log(scale))
        features.loc[group.index, "log_amount"] = scaled
        left = np.searchsorted(population, amounts, side="left")
        right = np.searchsorted(population, amounts, side="right")
        features.loc[group.index, "amount_rank"] = (left + right) / (2 * len(population))
        features.loc[group.index, "entity_frequency"] = frequency.loc[group.index] / len(population)
    features["missing_invoice"] = frame["missing_invoice"].astype(float)
    features["repeated_invoice"] = repeated.astype(float)
    return features


def _score_with_isolation_forest(
    transactions: pd.DataFrame,
    model: AnomalyModel,
) -> pd.DataFrame:
    frame = _prepare_features(transactions)
    output = pd.DataFrame({
        "ml_anomaly_score": 0,
        "ml_anomaly_band": "low",
        "ml_anomaly_drivers": (
            f"Model needs at least {model.config.min_training_rows} usable transactions in this currency. "
            "Identical repeats count once."
        ),
        "ml_model_flag": False,
        "ml_model_decision": float("nan"),
        "ml_model_status": "insufficient_data",
    }, index=frame.index)
    eligible = frame["currency"].isin(model.reference["currency"].unique())
    constant = frame["currency"].isin(model.constant_currencies)
    output.loc[constant, "ml_model_status"] = "constant_population"
    output.loc[constant, "ml_anomaly_drivers"] = "The reference transactions in this currency have identical model inputs."
    fields = ["ml_model_status", "ml_anomaly_drivers"]
    output.loc[frame["currency"].eq(""), fields] = [
        "missing_currency", "Currency is missing or malformed. Check the source currency.",
    ]
    output.loc[frame["amount"].isna(), fields] = [
        "invalid_amount", "Amount is missing, invalid or non-finite. Check the source amount.",
    ]
    eligible &= frame["amount"].notna()
    if model.isolation_forest is None or not eligible.any():
        return output[ANOMALY_COLUMNS]
    usable = frame.loc[eligible]
    features = _feature_matrix(usable, model.reference)
    decisions = pd.Series(
        model.isolation_forest.decision_function(features),
        index=usable.index,
        dtype="float64",
    )
    flags = decisions.lt(0)
    scores = _normalized_model_scores(decisions, flags, model.training_decisions)
    amount_context = (
        model.reference.assign(magnitude=model.reference["amount"].abs())
        .groupby("currency")["magnitude"].agg(["size", "median"])
        .to_dict(orient="index")
    )
    drivers = [
        _model_drivers(
            model,
            row=usable.loc[index],
            features=features.loc[index],
            amount_context=amount_context,
        )
        if flags.loc[index]
        else "The model did not select this transaction for additional review."
        for index in usable.index
    ]
    output.loc[usable.index, "ml_anomaly_score"] = scores
    output.loc[usable.index, "ml_anomaly_band"] = scores.map(_band_for_score)
    output.loc[usable.index, "ml_anomaly_drivers"] = drivers
    output.loc[usable.index, "ml_model_flag"] = flags
    output.loc[usable.index, "ml_model_decision"] = decisions
    output.loc[usable.index, "ml_model_status"] = "scored"
    return output[ANOMALY_COLUMNS]


def _normalized_model_scores(decisions: pd.Series, flags: pd.Series, training_decisions: np.ndarray) -> pd.Series:
    """Display rank against the fitted population; stable when scoring a subset."""
    scores = pd.Series(0, index=decisions.index, dtype="int64")
    for flagged, lower, width in [(False, 0, 49), (True, 50, 50)]:
        reference = np.sort(-training_decisions[(training_decisions < 0) == flagged])
        values = -decisions.loc[flags.eq(flagged)]
        if not len(values):
            continue
        if not len(reference):
            scores.loc[values.index] = lower
            continue
        left = np.searchsorted(reference, values.to_numpy(), side="left")
        right = np.searchsorted(reference, values.to_numpy(), side="right")
        ranks = (left + right) / (2 * len(reference))
        scores.loc[values.index] = np.rint(lower + width * ranks).astype(int)
    return scores


def _model_drivers(
    model: AnomalyModel,
    *,
    row: pd.Series,
    features: pd.Series,
    amount_context: dict,
) -> str:
    drivers = []
    currency = row["currency"]
    population = amount_context[currency]
    threshold = model.amount_thresholds.get(currency)
    amount = abs(row["amount"])
    if threshold is not None and amount >= threshold and amount > population["median"]:
        drivers.append(
            f"Absolute amount {amount:,.2f} {currency} meets the amount-context threshold "
            f"of {threshold:,.2f} {currency} ({model.config.amount_quantile:.0%} quantile "
            f"of {population['size']} valid same-currency reference transactions)"
        )

    if features["repeated_invoice"]:
        drivers.append("The invoice/reference and amount repeat for the same payee and currency")

    if row["missing_invoice"]:
        drivers.append("The invoice/reference is missing")

    frequency = round(features["entity_frequency"] * population["size"])
    if row["payee"] and frequency == 1:
        drivers.append("The payee appears once in the same-currency reference population")

    if not drivers:
        drivers.append(
            "The model selected the transaction based on its combined inputs"
        )
    return ". ".join(drivers) + "."


def _band_for_score(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"
