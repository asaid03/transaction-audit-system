from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from transaction_audit.cleaning import CleaningResult, clean_transaction_export
from transaction_audit.datasets import AuditBundle, AuditDataset, DATASET_DEFINITIONS, DatasetType
from transaction_audit.profiles import ImportProfile
from transaction_audit.reconciliation import ReconciliationResult, reconcile_bundle
from transaction_audit.schema import SchemaResult
from transaction_audit.types import ISSUE_COLUMNS, ValidationResult
from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import apply_mapping_plan, build_mapping_plan


@dataclass(frozen=True)
class BundleAuditResult:
    bundle: AuditBundle
    transaction_validation: ValidationResult
    reference_reconciliation: ReconciliationResult

    def transaction_issues_dataframe(self) -> pd.DataFrame:
        return self.transaction_validation.issues_dataframe()

    def reference_issues_dataframe(self) -> pd.DataFrame:
        return self.reference_reconciliation.issues_dataframe()

    def skipped_checks_dataframe(self) -> pd.DataFrame:
        return self.reference_reconciliation.skipped_checks_dataframe()

    def all_issues_dataframe(self) -> pd.DataFrame:
        frames = [self.transaction_issues_dataframe(), self.reference_issues_dataframe()]
        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return pd.DataFrame(columns=ISSUE_COLUMNS)
        return pd.concat(non_empty, ignore_index=True)


@dataclass(frozen=True)
class DatasetMappingResolution:
    schema_result: SchemaResult
    selected_profile: ImportProfile | None


def run_bundle_audit(bundle: AuditBundle) -> BundleAuditResult:
    return BundleAuditResult(
        bundle=bundle,
        transaction_validation=validate_transactions(bundle.transactions.dataframe),
        reference_reconciliation=reconcile_bundle(bundle),
    )


def maybe_clean_transactions(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, CleaningResult | None]:
    raw_schema_is_ready = build_mapping_plan(
        dataframe,
        dataset_type=DatasetType.TRANSACTIONS,
    ).auto_schema.has_required_schema

    cleaning_result = clean_transaction_export(dataframe)
    if not cleaning_result.applied:
        return dataframe, None if raw_schema_is_ready else cleaning_result

    cleaned_schema_is_ready = build_mapping_plan(
        cleaning_result.dataframe,
        dataset_type=DatasetType.TRANSACTIONS,
    ).auto_schema.has_required_schema
    if not cleaned_schema_is_ready:
        return dataframe, None if raw_schema_is_ready else cleaning_result

    if not raw_schema_is_ready or _cleaning_removed_rows(cleaning_result):
        return cleaning_result.dataframe, cleaning_result

    return dataframe, None


def resolve_dataset_mapping(
    dataset_type: DatasetType,
    raw_dataframe: pd.DataFrame,
    profiles: list[ImportProfile],
) -> DatasetMappingResolution:
    available_profiles = profiles_for_dataset(profiles, dataset_type)
    selected_profile = best_matching_profile(raw_dataframe, dataset_type, available_profiles)
    mapping_plan = build_mapping_plan(raw_dataframe, selected_profile, dataset_type=dataset_type)
    schema_result = apply_mapping_plan(
        raw_dataframe,
        mapping_plan.canonical_to_source,
        dataset_type=dataset_type,
    )
    return DatasetMappingResolution(schema_result=schema_result, selected_profile=selected_profile)


def best_matching_profile(
    raw_dataframe: pd.DataFrame,
    dataset_type: DatasetType,
    profiles: list[ImportProfile],
) -> ImportProfile | None:
    if build_mapping_plan(raw_dataframe, dataset_type=dataset_type).auto_schema.has_required_schema:
        return None

    for profile in profiles:
        try:
            plan = build_mapping_plan(raw_dataframe, profile, dataset_type=dataset_type)
            result = apply_mapping_plan(raw_dataframe, plan.canonical_to_source, dataset_type=dataset_type)
        except ValueError:
            continue
        if result.has_required_schema:
            return profile
    return None


def profiles_for_dataset(
    profiles: list[ImportProfile],
    dataset_type: DatasetType,
) -> list[ImportProfile]:
    return [profile for profile in profiles if profile.dataset_type == dataset_type.value]


def mapping_status_row(
    dataset_type: DatasetType,
    source_name: str,
    schema_result: SchemaResult,
    selected_profile: ImportProfile | None,
) -> dict[str, object]:
    return {
        "dataset": dataset_type.value,
        "source_name": source_name,
        "status": "ready" if schema_result.has_required_schema else "needs mapping",
        "missing_required": ", ".join(schema_result.missing_required),
        "mapped_columns": len(schema_result.source_to_canonical),
        "profile": selected_profile.display_name if selected_profile else "Auto-detect",
    }


def blocked_dataset_names(schema_results: dict[DatasetType, SchemaResult]) -> list[str]:
    return [
        DATASET_DEFINITIONS[dataset_type].display_name
        for dataset_type, schema_result in schema_results.items()
        if not schema_result.has_required_schema
    ]


def build_audit_bundle(prepared_datasets: dict[DatasetType, AuditDataset]) -> AuditBundle:
    return AuditBundle(
        transactions=prepared_datasets[DatasetType.TRANSACTIONS],
        vendors=prepared_datasets.get(DatasetType.VENDORS),
        beneficiaries=prepared_datasets.get(DatasetType.BENEFICIARIES),
        budget=prepared_datasets.get(DatasetType.BUDGET),
    )


def _cleaning_removed_rows(cleaning_result: CleaningResult) -> bool:
    return bool(cleaning_result.dropped_rows)
