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
from transaction_audit.review import (
    filter_by_issue_types,
    filter_by_values,
    sort_by_review_priority,
    unique_issue_types,
    unique_values,
)
from transaction_audit.risk import score_transactions
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
TRANSACTION_REVIEW_DISPLAY_COLUMNS = [
    "review_priority_score",
    "review_priority_band",
    "review_priority_drivers",
    "row_number",
    "issue_types",
    "transaction_id",
    "transaction_date",
    "account_id",
    "amount",
    "currency",
    "counterparty",
    "vendor_id",
    "beneficiary_id",
    "budget_line_id",
]


def render_page_header() -> None:
    st.title("Transaction Audit")
    st.caption(
        "Audit support for transaction files: validate the data, reconcile reference files, "
        "prioritise exceptions, and export a review pack."
    )


def render_empty_start(title: str, guidance: list[str]) -> None:
    st.info(title)
    for item in guidance:
        st.markdown(f"- {item}")


def render_run_status(title: str, message: str, state: str) -> None:
    if state == "ok":
        st.success(f"{title}. {message}")
    elif state == "error":
        st.error(f"{title}. {message}")
    else:
        st.warning(f"{title}. {message}")


def render_metric_row(metrics: list[tuple[str, object]]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics):
        column.metric(label, value)


def render_priority_overview(
    findings: pd.DataFrame,
    transaction_review: pd.DataFrame,
    skipped_count: int = 0,
) -> None:
    st.write("Review summary")
    high = _priority_count(transaction_review, "high")
    medium = _priority_count(transaction_review, "medium")
    low = _priority_count(transaction_review, "low")
    finding_count = len(findings)
    render_metric_row(
        [
            ("High priority", high),
            ("Medium priority", medium),
            ("Low priority", low),
            ("Findings", finding_count),
        ]
    )
    if skipped_count:
        st.caption(f"{skipped_count} check(s) were skipped because the needed reference file or fields were not available.")


def clean_display_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    display = dataframe.copy()
    display = display.fillna("")
    return display.replace({"None": "", "<NA>": "", "nan": ""})


def display_dataset_summary(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary

    display = clean_display_frame(summary)
    if "uploaded" in display:
        display["uploaded"] = display["uploaded"].map({True: "Uploaded", False: "Not uploaded"}).fillna(display["uploaded"])
    return display


def display_mapping_summary(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary

    display = clean_display_frame(summary)
    if "status" in display:
        display["status"] = display["status"].astype("string").str.replace("_", " ").str.title()
    return display


def _priority_count(dataframe: pd.DataFrame, band: str) -> int:
    if dataframe.empty or "review_priority_band" not in dataframe:
        return 0

    values = dataframe["review_priority_band"].astype("string").str.lower()
    return int(values.eq(band.lower()).sum())


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

    render_results(result, schema_result, summary)
    render_profile_manager(profiles)


def render_results(result, schema_result, summary: dict[str, object]) -> None:
    summary_frame = summary_dataframe(summary)
    issues_frame = result.issues_dataframe()
    findings_frame = review_findings_dataframe(issues_frame)
    transaction_review = score_transactions(result.transactions, issues_frame)
    issue_counts_frame = issue_counts_dataframe(result)
    audit_workbook = build_audit_workbook(result, schema_result, summary)

    render_run_status(
        title="Ready for review" if result.is_valid else "Review required",
        message=(
            "No blocking validation errors were found."
            if result.is_valid
            else "The file loaded successfully, but some records need human review before relying on the data."
        ),
        state="ok" if result.is_valid else "warn",
    )
    render_metric_row(
        [
            ("Rows checked", summary["rows_checked"]),
            ("Issues found", summary["issues_found"]),
            ("Errors", summary["error_count"]),
            ("Warnings", summary["warning_count"]),
        ]
    )
    render_priority_overview(findings_frame, transaction_review)

    st.download_button(
        "Download audit workbook",
        data=audit_workbook,
        file_name="audit_review_pack.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    tab_review, tab_audit, tab_evidence = st.tabs(["Review queue", "Audit pack", "Evidence"])

    with tab_review:
        render_review_queue(findings_frame, transaction_review, key_prefix="single")

    with tab_audit:
        st.write("Run summary")
        st.dataframe(clean_display_frame(summary_frame), use_container_width=True, hide_index=True)
        st.write("Schema mapping")
        st.dataframe(clean_display_frame(schema_result.mapping_dataframe()), use_container_width=True, hide_index=True)
        if not issue_counts_frame.empty:
            st.write("Issue counts by rule")
            st.dataframe(clean_display_frame(issue_counts_frame), use_container_width=True, hide_index=True)
        st.download_button(
            "Download audit workbook",
            data=audit_workbook,
            file_name="audit_review_pack.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="single_audit_pack_download",
        )

    with tab_evidence:
        with st.expander("Detailed findings", expanded=True):
            st.dataframe(display_findings(sort_by_review_priority(findings_frame)), use_container_width=True, hide_index=True)
        issue_csv = issues_frame.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download issues CSV",
            data=issue_csv,
            file_name="validation_issues.csv",
            mime="text/csv",
        )
        with st.expander("Canonical transaction data"):
            st.dataframe(clean_display_frame(result.transactions), use_container_width=True, hide_index=True)
            st.caption("This is the cleaned and mapped dataframe passed to the validation engine.")


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
                message="Some files could not be understood yet. Fix the required mappings before reconciliation can run.",
                state="error",
            )
            st.dataframe(display_mapping_summary(mapping_summary), use_container_width=True, hide_index=True)
            st.error("Needs mapping: " + ", ".join(blocked))
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
    transaction_review = score_transactions(audit_result.transaction_validation.transactions, combined_issues)

    render_run_status(
        title="Ready for review" if combined_issues.empty else "Review required",
        message=(
            "The uploaded files reconciled with no audit issues found."
            if combined_issues.empty
            else "The uploaded files were processed successfully. Review the exceptions below before relying on the results."
        ),
        state="ok" if combined_issues.empty else "warn",
    )
    render_metric_row(
        [
            ("Transactions", bundle.transactions.row_count),
            ("Transaction issues", len(transaction_issues)),
            ("Reference issues", len(reference_issues)),
            ("Skipped checks", len(skipped)),
        ]
    )
    render_priority_overview(review_findings, transaction_review, skipped_count=len(skipped))

    st.download_button(
        "Download audit workbook",
        data=audit_workbook,
        file_name="multi_file_audit_workbook.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    render_review_queue(review_findings, transaction_review, key_prefix="multi")

    with st.expander("Dataset and mapping details"):
        st.dataframe(display_dataset_summary(bundle.dataset_summary()), use_container_width=True, hide_index=True)
        st.write("Mapping summary")
        st.dataframe(display_mapping_summary(mapping_summary), use_container_width=True, hide_index=True)

    with st.expander("CSV downloads and detailed issue tables"):
        st.write("Transaction issues")
        st.dataframe(clean_display_frame(transaction_issues), use_container_width=True, hide_index=True)
        st.download_button(
            "Download transaction issues CSV",
            data=transaction_issues.to_csv(index=False).encode("utf-8"),
            file_name="transaction_validation_issues.csv",
            mime="text/csv",
        )

        st.write("Reference issues")
        st.dataframe(clean_display_frame(reference_issues), use_container_width=True, hide_index=True)
        st.download_button(
            "Download reference issues CSV",
            data=reference_issues.to_csv(index=False).encode("utf-8"),
            file_name="reference_issues.csv",
            mime="text/csv",
        )

        st.write("Skipped checks")
        st.dataframe(clean_display_frame(skipped), use_container_width=True, hide_index=True)

        st.download_button(
            "Download all issues CSV",
            data=combined_issues.to_csv(index=False).encode("utf-8"),
            file_name="multi_file_audit_issues.csv",
            mime="text/csv",
        )

    with st.expander("Input data previews"):
        st.write("Transactions")
        st.dataframe(clean_display_frame(bundle.transactions.dataframe), use_container_width=True, hide_index=True)
        if bundle.vendors:
            st.write("Vendors / suppliers")
            st.dataframe(clean_display_frame(bundle.vendors.dataframe), use_container_width=True, hide_index=True)
        if bundle.beneficiaries:
            st.write("Beneficiaries")
            st.dataframe(clean_display_frame(bundle.beneficiaries.dataframe), use_container_width=True, hide_index=True)
        if bundle.budget:
            st.write("Budget")
            st.dataframe(clean_display_frame(bundle.budget.dataframe), use_container_width=True, hide_index=True)


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
        with st.expander("Data preparation applied"):
            st.write("Bank-style export cleanup was applied before schema mapping.")
            if cleaning_result.transformations:
                st.write("Transformations")
                st.dataframe(
                    pd.DataFrame({"transformation": cleaning_result.transformations}),
                    use_container_width=True,
                    hide_index=True,
                )
            if cleaning_result.dropped_rows:
                st.write("Dropped source rows")
                st.dataframe(
                    pd.DataFrame({"source_row_number": cleaning_result.dropped_rows}),
                    use_container_width=True,
                    hide_index=True,
                )
            if cleaning_result.warnings:
                st.write("Warnings")
                st.dataframe(
                    pd.DataFrame({"warning": cleaning_result.warnings}),
                    use_container_width=True,
                    hide_index=True,
                )
        return

    if cleaning_result.warnings:
        st.caption("Cleaning not applied: " + "; ".join(cleaning_result.warnings))


def display_findings(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return findings
    display = findings[[column for column in FINDING_DISPLAY_COLUMNS if column in findings.columns]]
    return clean_display_frame(display)


def display_transaction_review(transaction_review: pd.DataFrame) -> pd.DataFrame:
    if transaction_review.empty:
        return transaction_review
    display = transaction_review[
        [column for column in TRANSACTION_REVIEW_DISPLAY_COLUMNS if column in transaction_review.columns]
    ]
    display = clean_display_frame(display)
    if "review_priority_band" in display:
        display["review_priority_band"] = display["review_priority_band"].astype("string").str.title()
    return display


def render_review_queue(
    findings: pd.DataFrame,
    transaction_review: pd.DataFrame,
    key_prefix: str,
) -> None:
    st.subheader("Exceptions needing review")
    if findings.empty:
        st.success("No validation or reconciliation findings need review.")
    else:
        with st.expander("Filter findings"):
            filtered_findings = review_findings_with_filters(findings, key_prefix=key_prefix)
        st.dataframe(display_findings(filtered_findings), use_container_width=True, hide_index=True)

    st.subheader("Transactions by review priority")
    if transaction_review.empty:
        st.info("No transaction review rows are available.")
        return

    with st.expander("Filter transactions"):
        filtered_transactions = transaction_review_with_filters(transaction_review, key_prefix=key_prefix)
    st.dataframe(display_transaction_review(filtered_transactions), use_container_width=True, hide_index=True)


def review_findings_with_filters(findings: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    if findings.empty:
        return findings

    band_column, issue_column, dataset_column = st.columns(3)
    bands = band_column.multiselect(
        "Band",
        options=unique_values(findings, "review_priority_band"),
        key=f"{key_prefix}_finding_bands",
    )
    issue_types = issue_column.multiselect(
        "Issue type",
        options=unique_issue_types(findings),
        key=f"{key_prefix}_finding_issue_types",
    )
    datasets = dataset_column.multiselect(
        "Dataset",
        options=unique_values(findings, "dataset"),
        key=f"{key_prefix}_finding_datasets",
    )

    filtered = sort_by_review_priority(findings)
    filtered = filter_by_values(filtered, "review_priority_band", bands)
    filtered = filter_by_issue_types(filtered, issue_types)
    return filter_by_values(filtered, "dataset", datasets)


def transaction_review_with_filters(transaction_review: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    if transaction_review.empty:
        return transaction_review

    first_row = st.columns(3)
    bands = first_row[0].multiselect(
        "Band",
        options=unique_values(transaction_review, "review_priority_band"),
        key=f"{key_prefix}_transaction_bands",
    )
    issue_types = first_row[1].multiselect(
        "Issue type",
        options=unique_issue_types(transaction_review),
        key=f"{key_prefix}_transaction_issue_types",
    )
    currencies = first_row[2].multiselect(
        "Currency",
        options=unique_values(transaction_review, "currency"),
        key=f"{key_prefix}_transaction_currencies",
    )

    second_row = st.columns(2)
    accounts = second_row[0].multiselect(
        "Account",
        options=unique_values(transaction_review, "account_id"),
        key=f"{key_prefix}_transaction_accounts",
    )
    counterparties = second_row[1].multiselect(
        "Counterparty",
        options=unique_values(transaction_review, "counterparty"),
        key=f"{key_prefix}_transaction_counterparties",
    )

    filtered = sort_by_review_priority(transaction_review)
    filtered = filter_by_values(filtered, "review_priority_band", bands)
    filtered = filter_by_issue_types(filtered, issue_types)
    filtered = filter_by_values(filtered, "currency", currencies)
    filtered = filter_by_values(filtered, "account_id", accounts)
    return filter_by_values(filtered, "counterparty", counterparties)


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
