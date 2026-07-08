import pandas as pd
import streamlit as st

from transaction_audit.audit import build_audit_summary
from transaction_audit.bundle_workflow import (
    blocked_dataset_names,
    build_audit_bundle,
    mapping_status_row,
    maybe_clean_transactions,
    profiles_for_dataset,
    resolve_dataset_mapping,
    run_bundle_audit,
)
from transaction_audit.config import REQUIRED_COLUMNS
from transaction_audit.datasets import AuditDataset, DatasetType
from transaction_audit.ingestion import load_dataset, load_transactions
from transaction_audit.profiles import list_import_profiles, save_import_profile
from transaction_audit.schema import apply_schema, source_mapping_from_canonical
from transaction_audit.ui.components import (
    render_cleaning_notice,
    render_empty_start,
    render_page_header,
    render_run_status,
)
from transaction_audit.ui.display import display_mapping_summary
from transaction_audit.ui.mapping import render_dataset_mapping
from transaction_audit.ui.profiles import render_profile_manager
from transaction_audit.ui.results import render_bundle_results, render_single_file_results
from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import build_mapping_plan


def main() -> None:
    st.set_page_config(page_title="Transaction Audit", layout="wide")
    render_page_header()

    profiles = list_import_profiles()
    workflow = st.radio(
        "Choose audit mode",
        options=["Single-file validation", "Multi-file reconciliation"],
        horizontal=True,
    )

    if workflow == "Multi-file reconciliation":
        render_multi_file_reconciliation(profiles)
        render_profile_manager(profiles)
        return

    st.markdown(
        "Upload one transaction file to check whether the data is usable for review. "
        "The app will clean bank-style exports when it can, map columns, flag exceptions, "
        "and create an audit workbook."
    )
    uploaded_file = st.file_uploader("Transaction file", type=["csv", "xlsx", "xls"])

    if uploaded_file is None:
        render_empty_start(
            "Start with a transaction file",
            [
                "Use a CSV or Excel export from a bank, mobile money platform, ERP, or ledger.",
                "If column names are unusual, auto-detect will try first and manual mapping appears only when needed.",
                "Results will be shown as review tasks, with detailed evidence available separately.",
            ],
        )
        render_profile_manager(profiles)
        st.stop()

    try:
        raw_transactions = load_transactions(uploaded_file, filename=uploaded_file.name)
        raw_transactions, cleaning_result = maybe_clean_transactions(raw_transactions)
        render_cleaning_notice(cleaning_result)
        auto_mapping_plan = build_mapping_plan(raw_transactions)

        column_options = [""] + [str(column) for column in raw_transactions.columns]

        with st.expander("Schema mapping", expanded=not auto_mapping_plan.auto_schema.has_required_schema):
            transaction_profiles = profiles_for_dataset(profiles, DatasetType.TRANSACTIONS)
            profile_options = {"Auto-detect": None}
            profile_options.update({profile.display_name: profile for profile in transaction_profiles})
            selected_profile_name = st.selectbox("Import profile", options=list(profile_options))
            selected_profile = profile_options[selected_profile_name]
            mapping_plan = build_mapping_plan(raw_transactions, selected_profile)

            if mapping_plan.missing_profile_sources:
                st.warning(
                    "Profile columns not found in this file: "
                    + ", ".join(mapping_plan.missing_profile_sources)
                )

            st.caption("Map your file columns to the transaction fields used by the validation engine.")
            canonical_to_source = {}
            mapping_columns = st.columns(2)

            for index, canonical_field in enumerate(REQUIRED_COLUMNS):
                inferred_source = mapping_plan.canonical_to_source.get(canonical_field, "")
                selected_source = mapping_columns[index % 2].selectbox(
                    canonical_field,
                    options=column_options,
                    index=column_options.index(inferred_source) if inferred_source in column_options else 0,
                    key=f"schema_{uploaded_file.name}_{selected_profile_name}_{canonical_field}",
                )
                canonical_to_source[canonical_field] = selected_source or None

        source_to_canonical = source_mapping_from_canonical(raw_transactions, canonical_to_source)
        schema_result = apply_schema(raw_transactions, source_to_canonical=source_to_canonical)
        result = validate_transactions(schema_result.transactions)
        summary = build_audit_summary(result, uploaded_file.name)
        summary["import_profile"] = selected_profile.display_name if selected_profile else "Auto-detect"

        with st.expander("Save import profile"):
            profile_name = st.text_input("Profile name", placeholder="Example: ERP export")
            profile_description = st.text_input("Description", placeholder="Optional source/system note")
            if st.button("Save profile"):
                profile = save_import_profile(
                    profile_name,
                    schema_result.source_to_canonical,
                    description=profile_description,
                )
                st.success(f"Saved profile: {profile.display_name}")
                st.rerun()
    except Exception as exc:
        st.error(f"Could not process file: {exc}")
        st.stop()

    render_single_file_results(result, schema_result, summary)
    render_profile_manager(profiles)


def render_multi_file_reconciliation(profiles) -> None:
    st.markdown(
        "Upload transactions plus any available reference files. The transaction file is required; "
        "vendor, beneficiary, and budget files improve the review by checking whether payments match approved records."
    )

    upload_columns = st.columns(2)
    with upload_columns[0]:
        transaction_file = st.file_uploader(
            "Transactions (required)",
            type=["csv", "xlsx", "xls"],
            key="phase2_transactions",
        )
        beneficiary_file = st.file_uploader(
            "Beneficiaries (optional)",
            type=["csv", "xlsx", "xls"],
            key="phase2_beneficiaries",
        )
    with upload_columns[1]:
        vendor_file = st.file_uploader(
            "Vendors / suppliers (optional)",
            type=["csv", "xlsx", "xls"],
            key="phase2_vendors",
        )
        budget_file = st.file_uploader(
            "Budget (optional)",
            type=["csv", "xlsx", "xls"],
            key="phase2_budget",
        )

    if transaction_file is None:
        render_empty_start(
            "Start with transactions",
            [
                "Add the transaction export first; reference files can be added after.",
                "Vendor files help spot unknown or weakly matched counterparties.",
                "Beneficiary and budget files add entitlement and budget-line checks when available.",
            ],
        )
        return

    if vendor_file is None and beneficiary_file is None and budget_file is None:
        st.warning("Add at least one reference file so the app can reconcile transactions against approved records.")
        return

    adjust_mappings = st.checkbox("Review or adjust schema mappings", value=False)

    try:
        uploaded_files = {
            DatasetType.TRANSACTIONS: transaction_file,
            DatasetType.VENDORS: vendor_file,
            DatasetType.BENEFICIARIES: beneficiary_file,
            DatasetType.BUDGET: budget_file,
        }
        prepared_datasets: dict[DatasetType, AuditDataset] = {}
        schema_results = {}
        mapping_rows = []

        for dataset_type, uploaded_file in uploaded_files.items():
            if uploaded_file is None:
                continue

            raw_dataframe = load_dataset(uploaded_file, filename=uploaded_file.name)
            cleaning_result = None
            if dataset_type == DatasetType.TRANSACTIONS:
                raw_dataframe, cleaning_result = maybe_clean_transactions(raw_dataframe)
                render_cleaning_notice(cleaning_result)
            mapping_resolution = resolve_dataset_mapping(dataset_type, raw_dataframe, profiles)
            schema_result = mapping_resolution.schema_result
            selected_profile = mapping_resolution.selected_profile

            if adjust_mappings or not schema_result.has_required_schema:
                schema_result, selected_profile = render_dataset_mapping(
                    dataset_type=dataset_type,
                    raw_dataframe=raw_dataframe,
                    source_name=uploaded_file.name,
                    profiles=profiles,
                )
            schema_results[dataset_type] = schema_result
            mapping_rows.append(mapping_status_row(dataset_type, uploaded_file.name, schema_result, selected_profile))

            if schema_result.has_required_schema:
                prepared_datasets[dataset_type] = AuditDataset(
                    dataset_type=dataset_type,
                    dataframe=schema_result.transactions,
                    source_name=uploaded_file.name,
                )

        mapping_summary = pd.DataFrame(mapping_rows)

        blocked = blocked_dataset_names(schema_results)
        if blocked:
            render_run_status(
                title="Mapping required",
                message=(
                    "Some files could not be understood yet. Fix the required "
                    "mappings before reconciliation can run."
                ),
                state="error",
            )
            st.dataframe(display_mapping_summary(mapping_summary), use_container_width=True, hide_index=True)
            st.error("Needs mapping: " + ", ".join(blocked))
            return

        bundle = build_audit_bundle(prepared_datasets)
        audit_result = run_bundle_audit(bundle)
    except Exception as exc:
        st.error(f"Could not process audit bundle: {exc}")
        return

    render_bundle_results(audit_result, schema_results, mapping_summary)


if __name__ == "__main__":
    main()
