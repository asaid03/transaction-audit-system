from __future__ import annotations

import pandas as pd
import streamlit as st

from transaction_audit.profiles import delete_import_profile, profiles_dataframe


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
