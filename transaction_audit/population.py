from __future__ import annotations

import pandas as pd

from transaction_audit.config import ValidationConfig
from transaction_audit.parsing import parse_amounts
from transaction_audit.provenance import source_row_number_at_position
from transaction_audit.types import ValidationIssue


def check_running_balance_integrity(
    transactions: pd.DataFrame,
    config: ValidationConfig,
) -> list[ValidationIssue]:
    required = {"transaction_id", "amount", "balance"}
    if not required.issubset(transactions.columns):
        return []

    amounts = parse_amounts(transactions["amount"])
    balances = parse_amounts(transactions["balance"])
    previous_balances = balances.shift(1)
    valid = amounts.notna() & balances.notna() & previous_balances.notna()
    if int(valid.sum()) < config.balance_inference_min_transitions:
        return []

    direction = _infer_balance_direction(
        amounts[valid],
        balances[valid],
        previous_balances[valid],
        config,
    )
    if direction is None:
        return []

    expected = previous_balances + (direction * amounts)
    mismatch = valid & balances.sub(expected).abs().gt(config.balance_tolerance)
    issues = []
    for position in mismatch[mismatch].index:
        current_transaction_id = _clean_text(transactions.at[position, "transaction_id"])
        previous_position = transactions.index.get_loc(position) - 1
        previous_index = transactions.index[previous_position]
        previous_transaction_id = _clean_text(
            transactions.at[previous_index, "transaction_id"]
        )
        issues.append(
            ValidationIssue(
                row_number=source_row_number_at_position(
                    transactions,
                    int(transactions.index.get_loc(position)),
                ),
                transaction_id=current_transaction_id,
                field="balance",
                issue_type="balance_movement_mismatch",
                severity="warning",
                message=(
                    f"Running balance was expected to be {expected.at[position]:,.2f} "
                    f"after this transaction, but the file reports {balances.at[position]:,.2f}."
                ),
                related_dataset="transactions",
                related_record_id=previous_transaction_id,
            )
        )
    return issues


def check_vendor_concentration(
    transactions: pd.DataFrame,
    config: ValidationConfig,
) -> list[ValidationIssue]:
    required = {"counterparty", "amount"}
    if not required.issubset(transactions.columns):
        return []

    working = pd.DataFrame(index=transactions.index)
    working["counterparty"] = transactions["counterparty"].astype("string").str.strip()
    working["_counterparty_key"] = working["counterparty"].str.lower()
    working["amount"] = parse_amounts(transactions["amount"]).abs()
    if "currency" in transactions:
        working["currency"] = transactions["currency"].astype("string").str.strip().str.upper()
    else:
        working["currency"] = "unspecified currency"

    working = working[
        working["_counterparty_key"].notna()
        & working["_counterparty_key"].ne("")
        & working["amount"].notna()
        & working["amount"].gt(0)
        & working["currency"].notna()
        & working["currency"].ne("")
    ]

    issues = []
    for currency, group in working.groupby("currency", dropna=False):
        if len(group) < config.vendor_concentration_min_rows:
            continue
        if group["_counterparty_key"].nunique() < config.vendor_concentration_min_counterparties:
            continue

        spend_by_vendor = group.groupby("_counterparty_key")["amount"].sum()
        total_spend = float(spend_by_vendor.sum())
        if total_spend <= 0:
            continue

        concentrated = spend_by_vendor[spend_by_vendor.div(total_spend).ge(
            config.vendor_concentration_threshold
        )]
        for counterparty_key, vendor_spend in concentrated.items():
            vendor_rows = group[group["_counterparty_key"].eq(counterparty_key)]
            display_name = str(vendor_rows["counterparty"].iloc[0])
            share = float(vendor_spend / total_spend)
            issues.append(
                ValidationIssue(
                    row_number=None,
                    transaction_id=None,
                    field="counterparty",
                    issue_type="vendor_concentration",
                    severity="warning",
                    message=(
                        f"{display_name} represents {share:.1%} of reviewed {currency} transaction value "
                        f"({vendor_spend:,.2f} of {total_spend:,.2f}), above the "
                        f"{config.vendor_concentration_threshold:.0%} review threshold."
                    ),
                    related_dataset="transactions",
                    related_record_id=display_name,
                )
            )
    return issues


def _infer_balance_direction(
    amounts: pd.Series,
    balances: pd.Series,
    previous_balances: pd.Series,
    config: ValidationConfig,
) -> int | None:
    movement = balances - previous_balances
    positive_matches = movement.sub(amounts).abs().le(config.balance_tolerance)
    negative_matches = movement.add(amounts).abs().le(config.balance_tolerance)
    positive_count = int(positive_matches.sum())
    negative_count = int(negative_matches.sum())
    winning_count = max(positive_count, negative_count)
    if winning_count / len(amounts) < config.balance_inference_min_match_ratio:
        return None
    if positive_count == negative_count:
        return None
    return 1 if positive_count > negative_count else -1


def _clean_text(value: object) -> str | None:
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None
