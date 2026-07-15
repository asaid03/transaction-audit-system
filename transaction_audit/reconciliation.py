from __future__ import annotations

import re
from dataclasses import asdict, dataclass

import pandas as pd
from rapidfuzz import fuzz, process

from transaction_audit.datasets import AuditBundle, AuditDataset
from transaction_audit.joins import (
    AUDIT_ROW_NUMBER,
    normalize_join_key,
    safe_left_join,
    unmatched_left_rows,
    with_audit_row_numbers,
)
from transaction_audit.parsing import parse_amounts
from transaction_audit.types import ISSUE_COLUMNS, ValidationIssue


SKIPPED_CHECK_COLUMNS = ["check_name", "dataset", "reason"]
FUZZY_VENDOR_SCORE_CUTOFF = 90
VENDOR_NAME_SUFFIX_PATTERN = re.compile(
    r"\b(ltd|limited|inc|llc|co|company|corp|corporation|plc)\b",
    re.IGNORECASE,
)
VENDOR_NAME_STOPWORDS = {
    "vendor",
    "supplier",
    "suppliers",
}
RECONCILIATION_CONTEXT_FIELDS = (
    "currency",
    "period",
    "programme_id",
    "project_id",
    "donor",
    "location",
)


@dataclass(frozen=True)
class SkippedCheck:
    check_name: str
    dataset: str
    reason: str


@dataclass(frozen=True)
class ReconciliationResult:
    issues: list[ValidationIssue]
    skipped_checks: list[SkippedCheck]

    def issues_dataframe(self) -> pd.DataFrame:
        if not self.issues:
            return pd.DataFrame(columns=ISSUE_COLUMNS)
        return pd.DataFrame([asdict(issue) for issue in self.issues], columns=ISSUE_COLUMNS)

    def skipped_checks_dataframe(self) -> pd.DataFrame:
        if not self.skipped_checks:
            return pd.DataFrame(columns=SKIPPED_CHECK_COLUMNS)
        return pd.DataFrame([asdict(check) for check in self.skipped_checks], columns=SKIPPED_CHECK_COLUMNS)


def reconcile_bundle(bundle: AuditBundle) -> ReconciliationResult:
    issues: list[ValidationIssue] = []
    skipped_checks: list[SkippedCheck] = []

    if bundle.vendors is None:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_vendor_reference",
                dataset="vendors",
                reason="Vendors file was not uploaded.",
            )
        )
    else:
        vendor_result = check_transaction_vendor_references(bundle.transactions, bundle.vendors)
        issues.extend(vendor_result.issues)
        skipped_checks.extend(vendor_result.skipped_checks)

    if bundle.beneficiaries is None:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_beneficiary_reference",
                dataset="beneficiaries",
                reason="Beneficiaries file was not uploaded.",
            )
        )
    else:
        beneficiary_result = check_transaction_beneficiary_references(
            bundle.transactions,
            bundle.beneficiaries,
        )
        issues.extend(beneficiary_result.issues)
        skipped_checks.extend(beneficiary_result.skipped_checks)
        entitlement_result = check_beneficiary_entitlements(
            bundle.transactions,
            bundle.beneficiaries,
        )
        issues.extend(entitlement_result.issues)
        skipped_checks.extend(entitlement_result.skipped_checks)

    if bundle.budget is None:
        skipped_checks.append(
            SkippedCheck(
                check_name="transaction_budget_reference",
                dataset="budget",
                reason="Budget file was not uploaded.",
            )
        )
    else:
        budget_result = check_budget_reconciliation(bundle.transactions, bundle.budget)
        issues.extend(budget_result.issues)
        skipped_checks.extend(budget_result.skipped_checks)

    return ReconciliationResult(issues=issues, skipped_checks=skipped_checks)


def check_transaction_vendor_references(
    transactions: AuditDataset,
    vendors: AuditDataset,
) -> ReconciliationResult:
    has_id_match = "vendor_id" in transactions.dataframe and "vendor_id" in vendors.dataframe
    has_name_match = "counterparty" in transactions.dataframe and "vendor_name" in vendors.dataframe
    if not has_id_match and not has_name_match:
        return ReconciliationResult(
            issues=[],
            skipped_checks=[
                SkippedCheck(
                    check_name="transaction_vendor_reference",
                    dataset="vendors",
                    reason="No usable vendor reference columns were found.",
                )
            ],
        )

    duplicate_issues = []
    for vendor_field in ("vendor_id", "vendor_name"):
        if vendor_field not in vendors.dataframe:
            continue
        duplicate_issues.extend(
            duplicate_reference_key_issues(
                vendors,
                key_field=vendor_field,
                issue_type="duplicate_vendor_reference",
                message_template=(
                    f"Vendor {vendor_field.replace('_', ' ')} '{{value}}' appears more than "
                    "once in the vendor file."
                ),
            )
        )
    vendor_id_keys = _reference_key_set(vendors.dataframe, "vendor_id") if has_id_match else set()
    vendor_names = _reference_record_lookup(vendors.dataframe, "vendor_name") if has_name_match else {}
    fuzzy_candidates = fuzzy_vendor_candidates(vendors.dataframe) if has_name_match else []

    issues = []
    for row in with_audit_row_numbers(transactions.dataframe).to_dict("records"):
        issue = _vendor_reference_issue_for_row(
            row,
            vendor_id_keys=vendor_id_keys,
            vendor_names=vendor_names,
            fuzzy_candidates=fuzzy_candidates,
        )
        if issue is not None:
            issues.append(issue)
    return ReconciliationResult(issues=duplicate_issues + issues, skipped_checks=[])


def _vendor_reference_issue_for_row(
    row: dict[str, object],
    *,
    vendor_id_keys: set[str],
    vendor_names: dict[str, dict[str, object]],
    fuzzy_candidates: list[dict[str, object]],
) -> ValidationIssue | None:
    vendor_id = _clean_optional(row.get("vendor_id"))
    counterparty = _clean_optional(row.get("counterparty"))
    beneficiary_id = _clean_optional(row.get("beneficiary_id"))

    if vendor_id is None and beneficiary_id is not None:
        return None

    if vendor_id is not None and _normalized_lookup_key(vendor_id) in vendor_id_keys:
        return None

    exact_name_match = (
        vendor_names.get(_normalized_lookup_key(counterparty))
        if counterparty is not None
        else None
    )
    if exact_name_match is not None:
        if vendor_id is None:
            return None
        matched_reference = exact_name_match.get("vendor_id") or exact_name_match.get("vendor_name")
        return ValidationIssue(
            dataset="transactions",
            row_number=int(row[AUDIT_ROW_NUMBER]),
            transaction_id=_clean_optional(row.get("transaction_id")),
            field="vendor_id",
            issue_type="vendor_reference_mismatch",
            severity="warning",
            related_dataset="vendors",
            related_record_id=_clean_optional(matched_reference),
            message=(
                f"Vendor ID '{vendor_id}' was not found, but payee '{counterparty}' "
                f"matches approved vendor '{matched_reference}'."
            ),
        )

    if counterparty is not None:
        possible_match = fuzzy_vendor_match(counterparty, fuzzy_candidates)
        if possible_match is not None:
            return _possible_vendor_match_issue(
                row=row,
                field="counterparty",
                query_value=counterparty,
                candidate=possible_match,
            )

    reference_value = vendor_id or counterparty
    if reference_value is None:
        return None
    reference_field = "vendor_id" if vendor_id is not None else "counterparty"
    return _reference_issue(
        row=row,
        field=reference_field,
        issue_type="unknown_vendor_reference",
        related_dataset="vendors",
        related_record_id=reference_value,
        message=f"Vendor reference '{reference_value}' was not found in the vendor file.",
    )


def _reference_record_lookup(
    dataframe: pd.DataFrame,
    key_field: str,
) -> dict[str, dict[str, object]]:
    if key_field not in dataframe:
        return {}

    records_by_key: dict[str, list[dict[str, object]]] = {}
    for record in dataframe.to_dict("records"):
        key = _normalized_lookup_key(record.get(key_field))
        if key:
            records_by_key.setdefault(key, []).append(record)
    return {
        key: matching_records[0]
        for key, matching_records in records_by_key.items()
        if len(matching_records) == 1
    }


def _reference_key_set(dataframe: pd.DataFrame, key_field: str) -> set[str]:
    if key_field not in dataframe:
        return set()
    return {
        key
        for key in (_normalized_lookup_key(value) for value in dataframe[key_field])
        if key
    }


def _normalized_lookup_key(value: object) -> str:
    cleaned = _clean_optional(value)
    if cleaned is None:
        return ""
    return " ".join(cleaned.lower().split())


def check_transaction_beneficiary_references(
    transactions: AuditDataset,
    beneficiaries: AuditDataset,
) -> ReconciliationResult:
    if "beneficiary_id" not in transactions.dataframe.columns or "beneficiary_id" not in beneficiaries.dataframe.columns:
        return ReconciliationResult(
            issues=[],
            skipped_checks=[
                SkippedCheck(
                    check_name="transaction_beneficiary_reference",
                    dataset="beneficiaries",
                    reason="beneficiary_id is required in both transactions and beneficiaries.",
                )
            ],
        )

    context_fields = _shared_context_fields(transactions.dataframe, beneficiaries.dataframe)
    duplicate_issues = duplicate_reference_key_issues(
        beneficiaries,
        key_field="beneficiary_id",
        context_fields=context_fields,
        issue_type="duplicate_beneficiary_reference",
        message_template="Beneficiary reference '{value}' appears more than once in the beneficiary file.",
    )
    beneficiaries_for_join = deduplicate_reference_rows(beneficiaries.dataframe, "beneficiary_id")
    joined = safe_left_join(
        transactions.dataframe,
        beneficiaries_for_join,
        left_on="beneficiary_id",
        right_on="beneficiary_id",
        right_prefix="beneficiary",
    )
    issues = [
        _reference_issue(
            row=row,
            field="beneficiary_id",
            issue_type="unknown_beneficiary_reference",
            related_dataset="beneficiaries",
            related_record_id=row["beneficiary_id"],
            message=f"Beneficiary reference '{row['beneficiary_id']}' was not found in the beneficiary file.",
        )
        for row in unmatched_left_rows(joined).to_dict("records")
        if _has_value(row["beneficiary_id"])
    ]
    return ReconciliationResult(issues=duplicate_issues + issues, skipped_checks=[])


def check_beneficiary_entitlements(
    transactions: AuditDataset,
    beneficiaries: AuditDataset,
) -> ReconciliationResult:
    required_columns = {"beneficiary_id", "amount"}
    has_transaction_fields = required_columns.issubset(transactions.dataframe.columns)
    has_expected_amount = "expected_amount" in beneficiaries.dataframe.columns
    if not has_transaction_fields or not has_expected_amount:
        return ReconciliationResult(
            issues=[],
            skipped_checks=[
                SkippedCheck(
                    check_name="beneficiary_entitlement",
                    dataset="beneficiaries",
                    reason="beneficiary_id and amount are required in transactions, and expected_amount is required in beneficiaries.",
                )
            ],
        )

    context_fields = _shared_context_fields(transactions.dataframe, beneficiaries.dataframe)
    transaction_amounts = parse_amounts(transactions.dataframe["amount"])
    valid_transactions = transactions.dataframe[
        transactions.dataframe["beneficiary_id"].astype(str).str.strip().ne("")
        & transaction_amounts.notna()
    ].copy()
    if valid_transactions.empty:
        return ReconciliationResult(issues=[], skipped_checks=[])

    valid_transactions["_amount_numeric"] = transaction_amounts.loc[valid_transactions.index]
    valid_transactions["_beneficiary_join_key"] = normalize_join_key(valid_transactions["beneficiary_id"])
    valid_transactions = valid_transactions[valid_transactions["_beneficiary_join_key"].notna()].copy()
    if valid_transactions.empty:
        return ReconciliationResult(issues=[], skipped_checks=[])

    context_key_columns = _add_context_keys(valid_transactions, context_fields)
    group_columns = ["_beneficiary_join_key", *context_key_columns]
    actual_by_beneficiary = (
        valid_transactions.groupby(group_columns, dropna=False)
        .agg(
            actual_amount=("_amount_numeric", "sum"),
            beneficiary_id=("beneficiary_id", "first"),
        )
        .reset_index()
    )

    beneficiaries_for_entitlement = deduplicate_reference_rows(
        beneficiaries.dataframe,
        "beneficiary_id",
        context_fields=context_fields,
    )
    expected = beneficiaries_for_entitlement[
        ["beneficiary_id", "expected_amount", *context_fields]
    ].copy()
    expected["_expected_amount_numeric"] = parse_amounts(expected["expected_amount"])
    expected["_beneficiary_join_key"] = normalize_join_key(expected["beneficiary_id"])
    expected = expected[
        expected["beneficiary_id"].astype(str).str.strip().ne("")
        & expected["_beneficiary_join_key"].notna()
        & expected["_expected_amount_numeric"].notna()
    ].copy()
    if expected.empty:
        return ReconciliationResult(issues=[], skipped_checks=[])

    _add_context_keys(expected, context_fields)

    joined = actual_by_beneficiary.merge(
        expected,
        how="inner",
        on=group_columns,
        suffixes=("_transaction", "_beneficiary"),
        validate="many_to_one",
    )
    overpaid = joined[joined["actual_amount"].gt(joined["_expected_amount_numeric"])]

    issues = [
        ValidationIssue(
            dataset="transactions",
            row_number=None,
            transaction_id=None,
            field="amount",
            issue_type="beneficiary_overpaid",
            severity="error",
            related_dataset="beneficiaries",
            related_record_id=_clean_optional(row["beneficiary_id_beneficiary"]),
            message=(
                f"Beneficiary '{row['beneficiary_id_transaction']}' received "
                f"{row['actual_amount']:,.2f}, above expected amount "
                f"{row['_expected_amount_numeric']:,.2f}"
                f"{_context_message(row, context_fields)}."
            ),
        )
        for row in overpaid.to_dict("records")
    ]
    return ReconciliationResult(issues=issues, skipped_checks=[])


def check_budget_reconciliation(
    transactions: AuditDataset,
    budget: AuditDataset,
) -> ReconciliationResult:
    match = _first_available_match(
        transactions.dataframe,
        budget.dataframe,
        preferred_pairs=(("budget_line_id", "budget_line_id"), ("budget_category", "category")),
    )
    has_transaction_amount = "amount" in transactions.dataframe.columns
    has_approved_amount = "approved_amount" in budget.dataframe.columns
    if match is None or not has_transaction_amount or not has_approved_amount:
        return ReconciliationResult(
            issues=[],
            skipped_checks=[
                SkippedCheck(
                    check_name="transaction_budget_reference",
                    dataset="budget",
                    reason="A budget line/category, transaction amount, and approved_amount are required for budget reconciliation.",
                )
            ],
        )

    transaction_field, budget_field = match
    context_fields = _shared_context_fields(transactions.dataframe, budget.dataframe)
    transaction_amounts = parse_amounts(transactions.dataframe["amount"])
    transactions_with_rows = with_audit_row_numbers(transactions.dataframe)
    valid_transactions = transactions_with_rows[
        transactions_with_rows[transaction_field].astype(str).str.strip().ne("")
        & transaction_amounts.notna()
    ].copy()
    valid_transactions["_amount_numeric"] = transaction_amounts.loc[valid_transactions.index]

    duplicate_issues = duplicate_reference_key_issues(
        budget,
        key_field=budget_field,
        context_fields=context_fields,
        issue_type="duplicate_budget_reference",
        message_template="Budget reference '{value}' appears more than once in the budget file.",
    )
    budget_for_join = deduplicate_reference_rows(
        budget.dataframe,
        budget_field,
        context_fields=context_fields,
    )
    budget_work = budget_for_join[[budget_field, "approved_amount", *context_fields]].copy()
    budget_work["_approved_amount_numeric"] = parse_amounts(budget_work["approved_amount"])
    valid_budget = budget_work[
        budget_work[budget_field].astype(str).str.strip().ne("")
        & budget_work["_approved_amount_numeric"].notna()
    ].copy()

    issues: list[ValidationIssue] = []
    if not valid_transactions.empty and not valid_budget.empty:
        valid_transactions["_budget_join_key"] = normalize_join_key(valid_transactions[transaction_field])
        valid_transactions = valid_transactions[valid_transactions["_budget_join_key"].notna()].copy()
        valid_budget["_budget_join_key"] = normalize_join_key(valid_budget[budget_field])
        valid_budget = valid_budget[valid_budget["_budget_join_key"].notna()].copy()

        context_key_columns = _add_context_keys(valid_transactions, context_fields)
        _add_context_keys(valid_budget, context_fields)
        group_columns = ["_budget_join_key", *context_key_columns]

        valid_budget_keys = set(_composite_keys(valid_budget, group_columns))
        transaction_keys = _composite_keys(valid_transactions, group_columns)
        unknown_transactions = valid_transactions[
            ~transaction_keys.isin(valid_budget_keys)
        ]
        issues.extend(
            _reference_issue(
                row=row,
                field=transaction_field,
                issue_type="unknown_budget_line",
                related_dataset="budget",
                related_record_id=row[transaction_field],
                message=f"Budget reference '{row[transaction_field]}' was not found in the budget file.",
            )
            for row in unknown_transactions.to_dict("records")
        )

        spend_by_line = (
            valid_transactions.groupby(group_columns, dropna=False)
            .agg(
                actual_amount=("_amount_numeric", "sum"),
                budget_reference=(transaction_field, "first"),
            )
            .reset_index()
        )
        joined = spend_by_line.merge(
            valid_budget,
            how="left",
            on=group_columns,
            validate="many_to_one",
        )

        over_budget = joined[
            joined["_approved_amount_numeric"].notna()
            & joined["actual_amount"].gt(joined["_approved_amount_numeric"])
        ]
        issues.extend(
            ValidationIssue(
                dataset="transactions",
                row_number=None,
                transaction_id=None,
                field="amount",
                issue_type="budget_line_overspent",
                severity="error",
                related_dataset="budget",
                related_record_id=_clean_optional(row["budget_reference"]),
                message=(
                    f"Budget line '{row['budget_reference']}' actual spend "
                    f"{row['actual_amount']:,.2f} exceeds approved amount "
                    f"{row['_approved_amount_numeric']:,.2f}"
                    f"{_context_message(row, context_fields)}."
                ),
            )
            for row in over_budget.to_dict("records")
        )

    return ReconciliationResult(issues=duplicate_issues + issues, skipped_checks=[])


def duplicate_reference_key_issues(
    dataset: AuditDataset,
    key_field: str,
    issue_type: str,
    message_template: str,
    context_fields: tuple[str, ...] = (),
) -> list[ValidationIssue]:
    keyed = with_audit_row_numbers(dataset.dataframe)
    normalized_key = normalize_join_key(keyed[key_field])
    keyed["_reference_join_key"] = normalized_key
    context_key_columns = _add_context_keys(keyed, context_fields)
    duplicate_mask = normalized_key.notna() & keyed.duplicated(
        subset=["_reference_join_key", *context_key_columns],
        keep=False,
    )

    return [
        ValidationIssue(
            dataset=dataset.dataset_type.value,
            row_number=int(row[AUDIT_ROW_NUMBER]),
            transaction_id=None,
            field=key_field,
            issue_type=issue_type,
            severity="error",
            related_dataset=None,
            related_record_id=_clean_optional(row[key_field]),
            message=message_template.format(value=row[key_field]),
        )
        for row in keyed.loc[duplicate_mask].to_dict("records")
    ]


def deduplicate_reference_rows(
    dataframe: pd.DataFrame,
    key_field: str,
    context_fields: tuple[str, ...] = (),
) -> pd.DataFrame:
    working = dataframe.copy()
    normalized_key = normalize_join_key(working[key_field])
    working["_reference_join_key"] = normalized_key
    context_key_columns = _add_context_keys(working, context_fields)
    usable_mask = normalized_key.notna()
    keep_mask = usable_mask & ~working.duplicated(
        subset=["_reference_join_key", *context_key_columns],
        keep="first",
    )
    return working.loc[keep_mask, dataframe.columns].copy()


def fuzzy_vendor_candidates(vendors: pd.DataFrame) -> list[dict[str, object]]:
    if "vendor_name" not in vendors.columns:
        return []

    candidates = []
    for record in vendors.to_dict("records"):
        vendor_name = _clean_optional(record.get("vendor_name"))
        if vendor_name is None:
            continue

        candidates.append(
            {
                "vendor_name": vendor_name,
                "vendor_id": _clean_optional(record.get("vendor_id")),
            }
        )
    return candidates


def fuzzy_vendor_match(
    query_value: object,
    candidates: list[dict[str, object]],
    score_cutoff: int = FUZZY_VENDOR_SCORE_CUTOFF,
) -> dict[str, object] | None:
    query = _clean_optional(query_value)
    if query is None or not candidates:
        return None

    choices = {index: str(candidate["vendor_name"]) for index, candidate in enumerate(candidates)}
    match = process.extractOne(
        query,
        choices,
        scorer=fuzz.token_set_ratio,
        processor=normalize_vendor_name_for_fuzzy,
        score_cutoff=score_cutoff,
    )
    if match is None:
        return None

    matched_name, score, candidate_index = match
    candidate = dict(candidates[candidate_index])
    candidate["matched_name"] = matched_name
    candidate["match_score"] = round(float(score), 1)
    return candidate


def normalize_vendor_name_for_fuzzy(value: object) -> str:
    cleaned = _clean_optional(value)
    if cleaned is None:
        return ""

    cleaned = VENDOR_NAME_SUFFIX_PATTERN.sub(" ", cleaned)
    cleaned = re.sub(r"[^a-zA-Z0-9]+", " ", cleaned)
    tokens = [token for token in cleaned.lower().split() if token not in VENDOR_NAME_STOPWORDS]
    return " ".join(tokens)


def _possible_vendor_match_issue(
    row: dict[str, object],
    field: str,
    query_value: object,
    candidate: dict[str, object],
) -> ValidationIssue:
    vendor_name = candidate["vendor_name"]
    vendor_reference = candidate.get("vendor_id") or vendor_name
    return ValidationIssue(
        dataset="transactions",
        row_number=int(row[AUDIT_ROW_NUMBER]),
        transaction_id=_clean_optional(row.get("transaction_id")),
        field=field,
        issue_type="possible_vendor_match",
        severity="warning",
        related_dataset="vendors",
        related_record_id=_clean_optional(vendor_reference),
        message=(
            f"Vendor reference '{query_value}' did not match exactly. "
            f"Possible vendor match: '{vendor_name}' "
            f"(score {candidate['match_score']})."
        ),
    )


def _reference_issue(
    row: dict[str, object],
    field: str,
    issue_type: str,
    related_dataset: str,
    related_record_id: object,
    message: str,
) -> ValidationIssue:
    return ValidationIssue(
        dataset="transactions",
        row_number=int(row[AUDIT_ROW_NUMBER]),
        transaction_id=_clean_optional(row.get("transaction_id")),
        field=field,
        issue_type=issue_type,
        severity="error",
        related_dataset=related_dataset,
        related_record_id=_clean_optional(related_record_id),
        message=message,
    )


def _first_available_match(
    left: pd.DataFrame,
    right: pd.DataFrame,
    preferred_pairs: tuple[tuple[str, str], ...],
) -> tuple[str, str] | None:
    for left_column, right_column in preferred_pairs:
        if left_column in left.columns and right_column in right.columns:
            return left_column, right_column
    return None


def _shared_context_fields(left: pd.DataFrame, right: pd.DataFrame) -> tuple[str, ...]:
    return tuple(
        field
        for field in RECONCILIATION_CONTEXT_FIELDS
        if field in left.columns and field in right.columns
    )


def _add_context_keys(
    dataframe: pd.DataFrame,
    context_fields: tuple[str, ...],
) -> list[str]:
    key_columns = []
    for field in context_fields:
        key_column = f"_context_{field}"
        dataframe[key_column] = normalize_join_key(dataframe[field]).fillna("")
        key_columns.append(key_column)
    return key_columns


def _composite_keys(dataframe: pd.DataFrame, columns: list[str]) -> pd.Series:
    return dataframe[columns].astype("string").agg(tuple, axis=1)


def _context_message(row: dict[str, object], context_fields: tuple[str, ...]) -> str:
    values = []
    for field in context_fields:
        value = _clean_optional(row.get(field))
        if value is None:
            value = _clean_optional(row.get(f"{field}_transaction"))
        if value is None:
            value = _clean_optional(row.get(f"_context_{field}"))
        if value is not None:
            values.append(f"{field.replace('_', ' ')} {value}")
    return f" for {', '.join(values)}" if values else ""


def _has_value(value: object) -> bool:
    return _clean_optional(value) is not None


def _clean_optional(value: object) -> str | None:
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None
