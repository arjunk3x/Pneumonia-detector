# --- your filtered_df logic stays as-is ---

# Columns shown in the table (no link column)
cols_to_show = ["Sanction_ID", "Stage", "Status in Stage", "Value", "Risk Level"]

st.subheader("Sanctions")
st.data_editor(
    filtered_df[cols_to_show],
    hide_index=True,
    disabled=True,            # read-only table
    use_container_width=True,
    column_config={
        "Sanction_ID": st.column_config.TextColumn("Sanction ID"),
        "Stage": st.column_config.TextColumn("Stage"),
        "Status in Stage": st.column_config.TextColumn("Status in Stage"),
        "Value": st.column_config.NumberColumn("Value"),
        "Risk Level": st.column_config.TextColumn("Risk Level"),
    },
    key="sanctions_table",
)

st.divider()
st.subheader("Actions")

# Render per-row router links (no full reload)
for _, r in filtered_df.iterrows():
    st.page_link(
        "app_pages/Feedback_Page.py",
        label=f"View {r['Sanction_ID']}",
        args={"sanction_id": str(r["Sanction_ID"])},
        new_tab=False,
    )
