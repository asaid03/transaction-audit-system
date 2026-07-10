from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.profiles import delete_import_profile, profiles_dataframe


def render_profile_manager(profiles) -> None:
    with st.expander("Saved column mappings"):
        if not profiles:
            st.info("No saved column mappings yet.")
            return

        st.dataframe(profiles_dataframe(profiles), width="stretch")
        profile_options = {
            f"{profile.display_name} ({profile.dataset_type})": profile
            for profile in profiles
        }
        selected_name = st.selectbox("View mapping", options=list(profile_options), key="profile_manager_select")
        selected_profile = profile_options[selected_name]

        st.write("Column mapping")
        st.dataframe(
            profile_mapping_dataframe(selected_profile.source_to_canonical),
            width="stretch",
        )

        if st.button("Delete selected mapping"):
            if delete_import_profile(selected_profile.profile_id):
                st.success(f"Deleted mapping: {selected_profile.display_name}")
                st.rerun()
            st.warning("Mapping was already missing.")


def profile_mapping_dataframe(source_to_canonical: dict[str, str]):
    rows = [
        {"file_column": source, "review_field": canonical}
        for source, canonical in source_to_canonical.items()
    ]
    return pd.DataFrame(rows, columns=["file_column", "review_field"])
