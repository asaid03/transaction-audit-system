from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.profiles import ImportProfile
from transaction_audit.schema import SchemaResult, apply_schema


@dataclass(frozen=True)
class MappingPlan:
    auto_schema: SchemaResult
    canonical_to_source: dict[str, str]
    missing_profile_sources: list[str]


def build_mapping_plan(
    dataframe: pd.DataFrame,
    profile: ImportProfile | None = None,
) -> MappingPlan:
    auto_schema = apply_schema(dataframe)
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
