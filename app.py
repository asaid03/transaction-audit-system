import pandas as pd
import streamlit as st

from transaction_audit.audit import (
    build_audit_summary,
    build_audit_workbook,
    build_bundle_audit_workbook,
    issue_counts_dataframe,
    review_findings_dataframe,
    summary_dataframe,
)
from transaction_audit.bundle_workflow import (
    blocked_dataset_names,
    build_audit_bundle,
    best_matching_profile,
    mapping_status_row,
    maybe_clean_transactions,
    profiles_for_dataset,
    resolve_dataset_mapping,
    run_bundle_audit,
)
from transaction_audit.cleaning import CleaningResult
from transaction_audit.config import REQUIRED_COLUMNS
from transaction_audit.datasets import AuditDataset, DATASET_DEFINITIONS, DatasetType
from transaction_audit.ingestion import load_dataset, load_transactions
from transaction_audit.profiles import delete_import_profile, list_import_profiles, profiles_dataframe, save_import_profile
from transaction_audit.schema import apply_schema, source_mapping_from_canonical
from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import allowed_mapping_columns, apply_mapping_plan, build_mapping_plan, required_mapping_columns


FINDING_DISPLAY_COLUMNS = [
    "finding_id",
    "review_priority_score",
    "review_priority_band",
    "review_priority_drivers",
    "priority",
    "review_status",
    "finding",
    "recommended_action",
    "dataset",
    "row_number",
    "transaction_id",
    "related_record_id",
]


def main() -> None:
    st.set_page_config(page_title="Transaction Audit", layout="wide")
    st.title("Transaction Audit")

    profiles = list_import_profiles()
    workflow = st.radio(
        "Workflow",
        options=["Single-file validation", "Multi-file reconciliation"],
        horizontal=True,
    )

    if workflow == "Multi-file reconciliation":
        render_multi_file_reconciliation(profiles)
        render_profile_manager(profiles)
        return

    uploaded_file = st.file_uploader("Upload transactions", type=["csv", "xlsx", "xls"])

    if uploaded_file is None:
        st.info("Upload a CSV or Excel file to run Phase 1 validation checks.")
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

    render_results(result, schema_result, summary)
    render_profile_manager(profiles)


def render_results(result, schema_result, summary: dict[str, object]) -> None:
    summary_frame = summary_dataframe(summary)
    issues_frame = result.issues_dataframe()
    findings_frame = review_findings_dataframe(issues_frame)
    issue_counts_frame = issue_counts_dataframe(result)
    audit_workbook = build_audit_workbook(result, schema_result, summary)

    metric_columns = st.columns(4)
    metric_columns[0].metric("Rows Checked", summary["rows_checked"])
    metric_columns[1].metric("Issues Found", summary["issues_found"])
    metric_columns[2].metric("Errors", summary["error_count"])
    metric_columns[3].metric("Warnings", summary["warning_count"])

    if result.is_valid:
        st.success("Validation completed with no blocking errors.")
    else:
        st.warning("Validation completed with errors that need review.")

    tab_transactions, tab_issues, tab_audit = st.tabs(["Transactions", "Issues", "Audit Pack"])

    with tab_transactions:
        st.dataframe(result.transactions, use_container_width=True)
        st.caption("Canonical dataframe passed to the pure Python validation engine.")

    with tab_issues:
        st.dataframe(display_findings(findings_frame), use_container_width=True)
        if not issue_counts_frame.empty:
            st.write("Issue counts")
            st.dataframe(issue_counts_frame, use_container_width=True)
        issue_csv = issues_frame.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download issues CSV",
            data=issue_csv,
            file_name="validation_issues.csv",
            mime="text/csv",
        )

    with tab_audit:
        st.dataframe(summary_frame, use_container_width=True)
        st.write("Schema mapping")
        st.dataframe(schema_result.mapping_dataframe(), use_container_width=True)
        st.download_button(
            "Download audit workbook",
            data=audit_workbook,
            file_name="audit_review_pack.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def render_multi_file_reconciliation(profiles) -> None:
    st.caption(
        "Upload a transaction file and optional reference files. Vendors, beneficiaries, "
        "and budget files are reconciled when available."
    )

    transaction_file = st.file_uploader(
        "Transactions",
        type=["csv", "xlsx", "xls"],
        key="phase2_transactions",
    )
    vendor_file = st.file_uploader(
        "Vendors / suppliers",
        type=["csv", "xlsx", "xls"],
        key="phase2_vendors",
    )
    beneficiary_file = st.file_uploader(
        "Beneficiaries",
        type=["csv", "xlsx", "xls"],
        key="phase2_beneficiaries",
    )
    budget_file = st.file_uploader(
        "Budget",
        type=["csv", "xlsx", "xls"],
        key="phase2_budget",
    )

    if transaction_file is None:
        st.info("Upload a transaction file to start multi-file reconciliation.")
        return

    if vendor_file is None and beneficiary_file is None and budget_file is None:
        st.warning("Upload at least one reference file, such as vendors, beneficiaries, or budget.")
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
            st.write("Mapping summary")
            st.dataframe(mapping_summary, use_container_width=True)
            st.error("Fix required schema mappings before reconciliation can run: " + ", ".join(blocked))
            return

        bundle = build_audit_bundle(prepared_datasets)
        audit_result = run_bundle_audit(bundle)
        audit_workbook = build_bundle_audit_workbook(audit_result, schema_results)
    except Exception as exc:
        st.error(f"Could not process audit bundle: {exc}")
        return

    transaction_issues = audit_result.transaction_issues_dataframe()
    reference_issues = audit_result.reference_issues_dataframe()
    skipped = audit_result.skipped_checks_dataframe()
    combined_issues = audit_result.all_issues_dataframe()
    review_findings = review_findings_dataframe(combined_issues)

    metric_columns = st.columns(4)
    metric_columns[0].metric("Transactions", bundle.transactions.row_count)
    metric_columns[1].metric("Transaction Issues", len(transaction_issues))
    metric_columns[2].metric("Reference Issues", len(reference_issues))
    metric_columns[3].metric("Skipped Checks", len(skipped))

    st.download_button(
        "Download audit workbook",
        data=audit_workbook,
        file_name="multi_file_audit_workbook.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    if combined_issues.empty:
        st.success("No audit issues found in the uploaded files.")
    else:
        st.warning("Audit completed with issues for review.")
        st.dataframe(display_findings(review_findings), use_container_width=True)

    with st.expander("Dataset and mapping details"):
        st.dataframe(bundle.dataset_summary(), use_container_width=True)
        st.write("Mapping summary")
        st.dataframe(mapping_summary, use_container_width=True)

    with st.expander("CSV downloads and detailed issue tables"):
        st.write("Transaction issues")
        st.dataframe(transaction_issues, use_container_width=True)
        st.download_button(
            "Download transaction issues CSV",
            data=transaction_issues.to_csv(index=False).encode("utf-8"),
            file_name="transaction_validation_issues.csv",
            mime="text/csv",
        )

        st.write("Reference issues")
        st.dataframe(reference_issues, use_container_width=True)
        st.download_button(
            "Download reference issues CSV",
            data=reference_issues.to_csv(index=False).encode("utf-8"),
            file_name="reference_issues.csv",
            mime="text/csv",
        )

        st.write("Skipped checks")
        st.dataframe(skipped, use_container_width=True)

        st.download_button(
            "Download all issues CSV",
            data=combined_issues.to_csv(index=False).encode("utf-8"),
            file_name="multi_file_audit_issues.csv",
            mime="text/csv",
        )

    with st.expander("Input data previews"):
        st.write("Transactions")
        st.dataframe(bundle.transactions.dataframe, use_container_width=True)
        if bundle.vendors:
            st.write("Vendors / suppliers")
            st.dataframe(bundle.vendors.dataframe, use_container_width=True)
        if bundle.beneficiaries:
            st.write("Beneficiaries")
            st.dataframe(bundle.beneficiaries.dataframe, use_container_width=True)
        if bundle.budget:
            st.write("Budget")
            st.dataframe(bundle.budget.dataframe, use_container_width=True)


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

    with st.expander(f"{definition.display_name} schema mapping", expanded=expanded):
        profile_options = {"Auto-detect": None}
        profile_options.update({profile.display_name: profile for profile in available_profiles})
        option_names = list(profile_options)
        default_name = default_profile.display_name if default_profile else "Auto-detect"
        selected_name = st.selectbox(
            "Import profile",
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
        with st.expander(f"Save {definition.display_name.lower()} import profile"):
            profile_name = st.text_input(
                "Profile name",
                placeholder=f"Example: {definition.display_name} export",
                key=f"{dataset_type.value}_{source_name}_profile_name",
            )
            profile_description = st.text_input(
                "Description",
                placeholder="Optional source/system note",
                key=f"{dataset_type.value}_{source_name}_profile_description",
            )
            if st.button("Save profile", key=f"{dataset_type.value}_{source_name}_save_profile"):
                profile = save_import_profile(
                    profile_name,
                    schema_result.source_to_canonical,
                    description=profile_description,
                    dataset_type=dataset_type,
                )
                st.success(f"Saved profile: {profile.display_name}")
                st.rerun()

    return schema_result, selected_profile


def render_cleaning_notice(cleaning_result: CleaningResult | None) -> None:
    if cleaning_result is None:
        return

    if cleaning_result.applied:
        st.info("Bank-style export cleanup was applied before schema mapping.")
        with st.expander("Cleaning details"):
            if cleaning_result.transformations:
                st.write("Transformations")
                st.dataframe(pd.DataFrame({"transformation": cleaning_result.transformations}), use_container_width=True)
            if cleaning_result.dropped_rows:
                st.write("Dropped source rows")
                st.dataframe(pd.DataFrame({"source_row_number": cleaning_result.dropped_rows}), use_container_width=True)
            if cleaning_result.warnings:
                st.write("Warnings")
                st.dataframe(pd.DataFrame({"warning": cleaning_result.warnings}), use_container_width=True)
        return

    if cleaning_result.warnings:
        st.caption("Cleaning not applied: " + "; ".join(cleaning_result.warnings))


def display_findings(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return findings
    return findings[[column for column in FINDING_DISPLAY_COLUMNS if column in findings.columns]]


def render_profile_manager(profiles) -> None:
    with st.expander("Import profiles"):
        if not profiles:
            st.info("No saved import profiles yet.")
            return

        st.dataframe(profiles_dataframe(profiles), use_container_width=True)
        profile_options = {
            f"{profile.display_name} ({profile.dataset_type})": profile
            for profile in profiles
        }
        selected_name = st.selectbox("View profile", options=list(profile_options), key="profile_manager_select")
        selected_profile = profile_options[selected_name]

        st.write("Profile mapping")
        st.dataframe(
            profile_mapping_dataframe(selected_profile.source_to_canonical),
            use_container_width=True,
        )

        if st.button("Delete selected profile"):
            if delete_import_profile(selected_profile.profile_id):
                st.success(f"Deleted profile: {selected_profile.display_name}")
                st.rerun()
            st.warning("Profile was already missing.")


def profile_mapping_dataframe(source_to_canonical: dict[str, str]):
    rows = [
        {"source_column": source, "canonical_field": canonical}
        for source, canonical in source_to_canonical.items()
    ]
    return pd.DataFrame(rows, columns=["source_column", "canonical_field"])


if __name__ == "__main__":
    main()
