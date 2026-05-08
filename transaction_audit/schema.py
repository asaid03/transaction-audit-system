from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.config import DEFAULT_COLUMN_ALIASES, REQUIRED_COLUMNS


@dataclass(frozen=True)
class SchemaResult:
    transactions: pd.DataFrame
    source_to_canonical: dict[str, str]
    missing_required: list[str]
    unmapped_columns: list[str]

    @property
    def has_required_schema(self) -> bool:
        return not self.missing_required

    def mapping_dataframe(self) -> pd.DataFrame:
        rows = [
            {"source_column": source, "canonical_field": canonical}
            for source, canonical in self.source_to_canonical.items()
        ]
        return pd.DataFrame(rows, columns=["source_column", "canonical_field"])


def normalize_column_key(column: object) -> str:
    raw = str(column).strip().lower().replace("-", " ").replace(".", " ")
    return " ".join(raw.split())


def default_canonical_name(column: object) -> str:
    key = normalize_column_key(column)
    return DEFAULT_COLUMN_ALIASES.get(key, key.replace(" ", "_"))


def infer_source_to_canonical(
    columns: list[object],
    aliases: dict[str, str] | None = None,
) -> dict[str, str]:
    aliases = aliases or DEFAULT_COLUMN_ALIASES
    mapping: dict[str, str] = {}
    used_canonical_fields: set[str] = set()

    for column in columns:
        source_column = str(column)
        normalized = normalize_column_key(column)
        canonical = aliases.get(normalized, normalized.replace(" ", "_"))

        if canonical in REQUIRED_COLUMNS and canonical not in used_canonical_fields:
            mapping[source_column] = canonical
            used_canonical_fields.add(canonical)

    return mapping


def source_mapping_from_canonical(
    dataframe: pd.DataFrame,
    canonical_to_source: dict[str, str | None],
) -> dict[str, str]:
    existing_columns = {str(column) for column in dataframe.columns}
    selected_sources = [
        source for source in canonical_to_source.values() if source and source in existing_columns
    ]
    duplicate_sources = sorted(
        source for source in set(selected_sources) if selected_sources.count(source) > 1
    )

    if duplicate_sources:
        joined = ", ".join(duplicate_sources)
        raise ValueError(f"Each source column can map to only one field. Duplicate mapping: {joined}")

    return {
        source: canonical
        for canonical, source in canonical_to_source.items()
        if source and source in existing_columns
    }


def apply_schema(
    dataframe: pd.DataFrame,
    source_to_canonical: dict[str, str] | None = None,
) -> SchemaResult:
    mapping = (
        infer_source_to_canonical(list(dataframe.columns))
        if source_to_canonical is None
        else source_to_canonical
    )
    normalized = dataframe.copy()

    rename_map = {
        source: canonical
        for source, canonical in mapping.items()
        if source in normalized.columns and canonical in REQUIRED_COLUMNS
    }
    normalized = normalized.rename(columns=rename_map)

    mapped_sources = set(rename_map)
    mapped_canonical = set(rename_map.values())
    missing_required = [
        column for column in REQUIRED_COLUMNS if column not in mapped_canonical and column not in normalized.columns
    ]
    unmapped_columns = [str(column) for column in dataframe.columns if str(column) not in mapped_sources]

    return SchemaResult(
        transactions=normalized,
        source_to_canonical=rename_map,
        missing_required=missing_required,
        unmapped_columns=unmapped_columns,
    )
