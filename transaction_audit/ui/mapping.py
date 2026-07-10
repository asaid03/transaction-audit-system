from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.bundle_workflow import best_matching_profile, profiles_for_dataset
from transaction_audit.datasets import DATASET_DEFINITIONS, DatasetType
from transaction_audit.profiles import save_import_profile
from transaction_audit.workflow import (
    allowed_mapping_columns,
    apply_mapping_plan,
    build_mapping_plan,
    required_mapping_columns,
)


def render_dataset_mapping(
    dataset_type: DatasetType,
    raw_dataframe: pd.DataFrame,
    source_name: str,
    profiles,
):
    definition = DATASET_DEFINITIONS[dataset_type]
    available_profiles = profiles_for_dataset(profiles, dataset_type)
    default_profile = best_matching_profile(raw_dataframe, dataset_type, available_profiles)
    auto_plan = build_mapping_plan(raw_dataframe, dataset_type=dataset_type)
    expanded = default_profile is None and not auto_plan.auto_schema.has_required_schema

    if not expanded:
        st.success(f"{definition.display_name} columns detected automatically.")
    else:
        st.warning(f"{definition.display_name} needs column mapping before review can continue.")

    with st.expander(f"Adjust {definition.display_name.lower()} column mapping", expanded=expanded):
        profile_options = {"Auto-detect": None}
        profile_options.update({profile.display_name: profile for profile in available_profiles})
        option_names = list(profile_options)
        default_name = default_profile.display_name if default_profile else "Auto-detect"
        selected_name = st.selectbox(
            "Saved column mapping",
            options=option_names,
            index=option_names.index(default_name),
            key=f"{dataset_type.value}_{source_name}_profile",
        )
        selected_profile = profile_options[selected_name]
        mapping_plan = build_mapping_plan(raw_dataframe, selected_profile, dataset_type=dataset_type)

        if mapping_plan.missing_profile_sources:
            st.warning(
                "Profile columns not found in this file: "
                + ", ".join(mapping_plan.missing_profile_sources)
            )

        column_options = [""] + [str(column) for column in raw_dataframe.columns]
        required_fields = set(required_mapping_columns(dataset_type))
        canonical_to_source = {}
        mapping_columns = st.columns(2)

        for index, canonical_field in enumerate(allowed_mapping_columns(dataset_type)):
            inferred_source = mapping_plan.canonical_to_source.get(canonical_field, "")
            label = f"{canonical_field} *" if canonical_field in required_fields else canonical_field
            selected_source = mapping_columns[index % 2].selectbox(
                label,
                options=column_options,
                index=column_options.index(inferred_source) if inferred_source in column_options else 0,
                key=f"{dataset_type.value}_{source_name}_{canonical_field}",
            )
            canonical_to_source[canonical_field] = selected_source or None

    schema_result = apply_mapping_plan(raw_dataframe, canonical_to_source, dataset_type=dataset_type)

    if not schema_result.has_required_schema:
        st.warning(
            f"{definition.display_name} needs required fields mapped: "
            + ", ".join(schema_result.missing_required)
        )
    elif selected_profile is None:
        with st.expander(f"Save {definition.display_name.lower()} column mapping"):
            profile_name = st.text_input(
                "Mapping name",
                placeholder=f"Example: {definition.display_name} export",
                key=f"{dataset_type.value}_{source_name}_profile_name",
            )
            profile_description = st.text_input(
                "Description",
                placeholder="Optional source/system note",
                key=f"{dataset_type.value}_{source_name}_profile_description",
            )
            if st.button("Save mapping", key=f"{dataset_type.value}_{source_name}_save_profile"):
                profile = save_import_profile(
                    profile_name,
                    schema_result.source_to_canonical,
                    description=profile_description,
                    dataset_type=dataset_type,
                )
                st.success(f"Saved mapping: {profile.display_name}")
                st.rerun()

    return schema_result, selected_profile
