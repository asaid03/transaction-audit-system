from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.config import (
    BENEFICIARY_COLUMN_ALIASES,
    BUDGET_COLUMN_ALIASES,
    DEFAULT_COLUMN_ALIASES,
    REQUIRED_COLUMNS,
    VENDOR_COLUMN_ALIASES,
)
from transaction_audit.datasets import DatasetType, optional_columns_for_dataset, required_columns_for_dataset
from transaction_audit.profiles import ImportProfile
from transaction_audit.schema import SchemaResult, apply_schema, normalize_column_key


_FIELD_HEADER_TERMS: dict[str, tuple[str, ...]] = {
    "transaction_id": ("transaction", "txn", "payment ref", "reference", "ref no"),
    "transaction_date": ("transaction date", "payment date", "posting date", "value date", "date"),
    "account_id": ("account", "acct", "ledger", "nominal", "gl code"),
    "amount": ("payment amount", "paid amount", "amount", "debit", "credit", "value"),
    "currency": ("currency", "ccy", "curr code"),
    "counterparty": (
        "counterparty",
        "payee",
        "vendor",
        "supplier",
        "merchant",
        "recipient",
        "beneficiary",
        "description",
        "narration",
    ),
    "invoice_reference": ("invoice ref", "invoice no", "invoice number", "document no"),
    "invoice_amount": ("invoice amount", "invoice total", "invoice value"),
    "vendor_id": ("vendor id", "vendor code", "supplier id", "supplier code"),
    "beneficiary_id": ("beneficiary id", "beneficiary code", "recipient id", "recipient code"),
    "budget_line_id": ("budget line", "budget code"),
    "budget_category": ("budget category", "spend category", "category"),
    "description": ("description", "narration", "memo", "details"),
    "balance": ("balance",),
    "period": ("period",),
    "programme_id": ("programme", "program"),
    "project_id": ("project",),
    "donor": ("donor",),
    "location": ("location", "site"),
}

_FIELD_HEADER_EXCLUSIONS: dict[str, tuple[str, ...]] = {
    "transaction_id": ("invoice", "document", "vendor", "supplier", "beneficiary", "account"),
    "transaction_date": ("invoice", "due", "created"),
    "amount": ("invoice", "balance", "budget", "approved", "expected", "entitlement"),
}


@dataclass(frozen=True)
class MappingPlan:
    auto_schema: SchemaResult
    canonical_to_source: dict[str, str]
    missing_profile_sources: list[str]


def build_mapping_plan(
    dataframe: pd.DataFrame,
    profile: ImportProfile | None = None,
    dataset_type: DatasetType = DatasetType.TRANSACTIONS,
) -> MappingPlan:
    required_columns = required_mapping_columns(dataset_type)
    allowed_columns = allowed_mapping_columns(dataset_type)
    auto_schema = apply_schema(
        dataframe,
        aliases=aliases_for_dataset(dataset_type),
        required_columns=required_columns,
        allowed_columns=allowed_columns,
    )
    canonical_to_source = {
        canonical: source for source, canonical in auto_schema.source_to_canonical.items()
    }
    missing_profile_sources: list[str] = []

    if profile is not None:
        profile_canonical_to_source = {
            canonical: source
            for source, canonical in profile.source_to_canonical.items()
            if source in dataframe.columns
        }
        canonical_to_source = {**canonical_to_source, **profile_canonical_to_source}
        missing_profile_sources = [
            source for source in profile.source_to_canonical if source not in dataframe.columns
        ]

    return MappingPlan(
        auto_schema=auto_schema,
        canonical_to_source=canonical_to_source,
        missing_profile_sources=missing_profile_sources,
    )


def apply_mapping_plan(
    dataframe: pd.DataFrame,
    canonical_to_source: dict[str, str | None],
    dataset_type: DatasetType = DatasetType.TRANSACTIONS,
) -> SchemaResult:
    from transaction_audit.schema import source_mapping_from_canonical

    return apply_schema(
        dataframe,
        source_to_canonical=source_mapping_from_canonical(dataframe, canonical_to_source),
        aliases=aliases_for_dataset(dataset_type),
        required_columns=required_mapping_columns(dataset_type),
        allowed_columns=allowed_mapping_columns(dataset_type),
    )


def required_mapping_columns(dataset_type: DatasetType) -> tuple[str, ...]:
    if dataset_type == DatasetType.TRANSACTIONS:
        return tuple(REQUIRED_COLUMNS)
    return required_columns_for_dataset(dataset_type)


def allowed_mapping_columns(dataset_type: DatasetType) -> tuple[str, ...]:
    if dataset_type == DatasetType.TRANSACTIONS:
        return tuple(dict.fromkeys([*REQUIRED_COLUMNS, *optional_columns_for_dataset(dataset_type)]))
    return tuple(
        dict.fromkeys(
            [
                *required_columns_for_dataset(dataset_type),
                *optional_columns_for_dataset(dataset_type),
            ]
        )
    )


def aliases_for_dataset(dataset_type: DatasetType) -> dict[str, str]:
    if dataset_type == DatasetType.VENDORS:
        return VENDOR_COLUMN_ALIASES
    if dataset_type == DatasetType.BENEFICIARIES:
        return BENEFICIARY_COLUMN_ALIASES
    if dataset_type == DatasetType.BUDGET:
        return BUDGET_COLUMN_ALIASES
    return DEFAULT_COLUMN_ALIASES


def suggested_mapping_columns(
    dataframe: pd.DataFrame,
    dataset_type: DatasetType = DatasetType.TRANSACTIONS,
) -> dict[str, list[str]]:
    """Return likely source columns for each field without blocking manual fallback."""
    aliases = aliases_for_dataset(dataset_type)
    fields = allowed_mapping_columns(dataset_type)
    suggestions: dict[str, list[str]] = {field: [] for field in fields}

    for source in map(str, dataframe.columns):
        header = normalize_column_key(source)
        exact_field = aliases.get(header)
        if exact_field in suggestions:
            suggestions[exact_field].append(source)

        for field in fields:
            if source in suggestions[field]:
                continue
            terms = _FIELD_HEADER_TERMS.get(field, (field.replace("_", " "),))
            exclusions = _FIELD_HEADER_EXCLUSIONS.get(field, ())
            if any(exclusion in header for exclusion in exclusions):
                continue
            if any(term in header for term in terms):
                suggestions[field].append(source)

    return suggestions
