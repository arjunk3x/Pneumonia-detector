users = {
    "submit":        {"role": "submitter", "password": "a"},

    "HeadDataAI":    {"role": "approver", "password": "a"},
    "DataGovIA":     {"role": "approver", "password": "a"},
    "ArchAssurance": {"role": "approver", "password": "a"},
    "Finance":       {"role": "approver", "password": "a"},
    "Regulatory":    {"role": "approver", "password": "a"},

    "DigitalGuild":  {"role": "approver", "password": "a"},
    "ETIDM":         {"role": "approver", "password": "a"},
}

TEAM_CODES = (
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
    "DigitalGuild",
    "ETIDM",
)


# ============================================================
# ACTIONS SECTION (cards shown for current role)
# ============================================================

st.markdown(
    f"""
    <div class="section-title" style="font-size: 28px; font-weight: bold;">
        Actions in {role_display_name(current_role)}
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown('<div class="actions-divider"></div>', unsafe_allow_html=True)

# -------------------------------------------------------------------
# 1. Load items that need action for this role
# -------------------------------------------------------------------
if con is None:
    st.info("No database connection configured.")
    actions_df = pd.DataFrame()
else:
    # Map current internal role -> its stage columns
    # stage_cols() should already exist and return:
    #   is_in_col, status_col, assigned_col, decision_col
    is_in_col, status_col, assigned_col, decision_col = stage_cols(current_role)

    query = None

    # ---- Parallel pre-review roles (HeadDataAI, DataGovIA, ArchAssurance, Finance, Regulatory)
    if current_role in PRE_REVIEW_ROLES:
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND COALESCE(TRY_CAST({is_in_col} AS BIGINT), 0) = 1
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """

    # ---- Digital Guild: only see items whose overall current stage is Digital Guild
    elif current_role == "DigitalGuild":
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND "Current Stage" = 'Digital Guild'
              AND COALESCE(TRY_CAST({is_in_col} AS BIGINT), 0) = 1
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """

    # ---- ETIDM: only see items whose overall current stage is ETIDM
    elif current_role == "ETIDM":
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND "Current Stage" = 'ETIDM'
              AND COALESCE(TRY_CAST({is_in_col} AS BIGINT), 0) = 1
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """

    # Fallback: nothing for unknown roles
    if query is not None:
        actions_df = con.execute(query).df()
    else:
        actions_df = pd.DataFrame()

# -------------------------------------------------------------------
# 2. Render action cards
# -------------------------------------------------------------------
if actions_df.empty:
    st.info("No items currently need your action.")
else:
    # You can sort/prioritise however you like here
    filtered_df = actions_df.reset_index(drop=True)

    for _, r in filtered_df.iterrows():
        # --- derive risk badge class
        risk_value = str(r.get("Risk Level", "")).lower()
        if "high" in risk_value or "red" in risk_value:
            risk_class = "risk-high"
        elif "med" in risk_value or "amber" in risk_value:
            risk_class = "risk-medium"
        else:
            risk_class = "risk-low"

        # --- derive status pill class
        status_value = str(r.get("Status in Stage", "")).lower()
        if "approved" in status_value:
            status_class = "status-approved"
        elif "reject" in status_value:
            status_class = "status-rejected"
        else:
            status_class = "status-pending"

        # thin divider between cards
        st.markdown('<hr style="border: 0.5px solid #d3d3d3;">', unsafe_allow_html=True)

        c1, c2 = st.columns([5, 1])

        with c1:
            # Header: ID + stage + status pill (keep your existing CSS classes)
            st.markdown(
                f"""
                <div class="action-header">
                    <div class="action-title">
                        <span class="action-title-icon">📂</span>
                        <span>Sanction ID: {r.get('Sanction_ID', '')}</span>
                        <span class="action-pill-stage">Stage: {r.get('Stage', '')}</span>
                    </div>
                    <div class="status-pill {status_class}">
                        {r.get('Status in Stage', '')}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Second row: value + risk level chips
            st.markdown(
                f"""
                <div class="detail-row">
                    <div>
                        <span class="value-pill">£ {r.get('Value', '')}</span>
                    </div>
                    <div>
                        <span class="risk-pill {risk_class}">
                            {r.get('Risk Level', '')}
                        </span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with c2:
            # "View" button -> open Feedback_Page for this sanction
            if st.button("View ➜", key=f"view_{r.get('Sanction_ID', '')}"):
                st.session_state["selected_sanction_id"] = str(r.get("Sanction_ID", ""))
                st.session_state.navigate_to_feedback = True
                st.rerun()
