import streamlit as st

from transaction_audit.audit import build_audit_summary, build_audit_workbook, issue_counts_dataframe, summary_dataframe
from transaction_audit.config import REQUIRED_COLUMNS
from transaction_audit.ingestion import load_transactions
from transaction_audit.profiles import delete_import_profile, list_import_profiles, profiles_dataframe, save_import_profile
from transaction_audit.schema import apply_schema, source_mapping_from_canonical
from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import build_mapping_plan


def main() -> None:
    st.set_page_config(page_title="Transaction Audit", layout="wide")
    st.title("Transaction Audit")

    profiles = list_import_profiles()
    uploaded_file = st.file_uploader("Upload transactions", type=["csv", "xlsx", "xls"])

    if uploaded_file is None:
        st.info("Upload a CSV or Excel file to run Phase 1 validation checks.")
        render_profile_manager(profiles)
        st.stop()

    try:
        raw_transactions = load_transactions(uploaded_file, filename=uploaded_file.name)
        auto_mapping_plan = build_mapping_plan(raw_transactions)

        column_options = [""] + [str(column) for column in raw_transactions.columns]

        with st.expander("Schema mapping", expanded=not auto_mapping_plan.auto_schema.has_required_schema):
            profile_options = {"Auto-detect": None}
            profile_options.update({profile.display_name: profile for profile in profiles})
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
        st.dataframe(issues_frame, use_container_width=True)
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


def render_profile_manager(profiles) -> None:
    with st.expander("Import profiles"):
        if not profiles:
            st.info("No saved import profiles yet.")
            return

        st.dataframe(profiles_dataframe(profiles), use_container_width=True)
        profile_options = {profile.display_name: profile for profile in profiles}
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
    import pandas as pd

    rows = [
        {"source_column": source, "canonical_field": canonical}
        for source, canonical in source_to_canonical.items()
    ]
    return pd.DataFrame(rows, columns=["source_column", "canonical_field"])


if __name__ == "__main__":
    main()
