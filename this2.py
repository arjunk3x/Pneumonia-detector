# =========================
# TABLE DATA FOR DASHBOARD
# =========================
display_df = pd.DataFrame({
    "Sanction_ID": pending_df["Sanction_ID"].astype(str),
    "Value":       pending_df["Value"],
    "Stage":       current_role,
    "Status in Stage": pending_df[status_col].fillna("Pending").astype(str),
    "Risk Level":  risk_txt.map(risk_badge),
})

# NOTE: remove LinkColumn – it reloads the whole app and resets login
# FEEDBACK_LABEL = "Feedback Page"
# FEEDBACK_ROUTE = quote(FEEDBACK_LABEL)
# display_df["View"] = display_df["Sanction_ID"].apply(
#     lambda sid: f"./{FEEDBACK_ROUTE}?sanction_id={sid}"
# ).astype(str)

# -------------------------
# Filters (unchanged)
# -------------------------
colA, colB, colC = st.columns(3)

with colA:
    search_id = st.text_input("Search by Sanction_ID")

with colB:
    selected_status = st.multiselect(
        "Filter by Status",
        options=sorted(display_df["Status in Stage"].dropna().unique())
    )

with colC:
    selected_stage = st.multiselect(
        "Filter by Stage",
        options=sorted(display_df["Stage"].dropna().unique()),
        default=[current_role] if current_role in display_df["Stage"].unique() else [],
    )

# -------------------------
# Apply filters
# -------------------------
filtered_df = display_df.copy()
if search_id:
    filtered_df = filtered_df[filtered_df["Sanction_ID"].str.contains(search_id, case=False)]
if selected_status:
    filtered_df = filtered_df[filtered_df["Status in Stage"].isin(selected_status)]
if selected_stage:
    filtered_df = filtered_df[filtered_df["Stage"].isin(selected_stage)]

# -------------------------
# Read-only table (same structure as before)
# -------------------------
st.data_editor(
    filtered_df[["Sanction_ID", "Value", "Stage", "Status in Stage", "Risk Level"]],
    hide_index=True,
    disabled=True,
    use_container_width=True,
    column_config={
        "Sanction_ID":     st.column_config.TextColumn("Sanction_ID"),
        "Value":           st.column_config.NumberColumn("Value"),
        "Stage":           st.column_config.TextColumn("Stage"),
        "Status in Stage": st.column_config.TextColumn("Status in Stage"),
        "Risk Level":      st.column_config.TextColumn("Risk Level"),
    },
    key="sanctions_table",
)

# -------------------------
# Actions: per-row "View" buttons (keep same session)
# -------------------------
st.markdown("#### Actions")
for _, r in filtered_df.reset_index(drop=True).iterrows():
    row_cols = st.columns([4, 1])  # text | button
    with row_cols[0]:
        st.write(
            f"**{r['Sanction_ID']}** — Stage: {r['Stage']} — "
            f"Status: {r['Status in Stage']} — Value: {r['Value']} — Risk: {r['Risk Level']}"
        )
    with row_cols[1]:
        if st.button("View", key=f"view_{r['Sanction_ID']}"):
            # keep selected id in the same session (no login loss)
            st.session_state['selected_sanction_id'] = str(r['Sanction_ID'])
            # IMPORTANT: path must match how you register the page
            st.switch_page("app_pages/Feedback_Page.py")
