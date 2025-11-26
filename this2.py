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

# ============================================================
# ACTIONS SECTION (cards shown for current role)  – NO SQL
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
# 1. Build actions_df from an in-memory DataFrame
# -------------------------------------------------------------------

# 👉 Use whatever dataframe holds your approval table
#    (change `approval_df` to your own variable name if needed)
try:
    base_df = approval_df.copy()
except NameError:
    # Fallback if you used `df` instead of `approval_df`
    base_df = df.copy()

if base_df.empty:
    actions_df = base_df
else:
    # Get the stage-specific column names for this role
    # stage_cols(current_role) must already exist and return:
    #   is_in_col, status_col, assigned_col, decision_col
    is_in_col, status_col, assigned_col, decision_col = stage_cols(current_role)

    # Normalise the helper columns so we can filter safely
    is_submitter = (
        pd.to_numeric(base_df.get("is_submitter", 0), errors="coerce")
        .fillna(0)
        .astype("Int64")
    )

    is_in_stage = (
        pd.to_numeric(base_df.get(is_in_col, 0), errors="coerce")
        .fillna(0)
        .astype("Int64")
    )

    status_in_stage = base_df.get(status_col, "Pending").fillna("Pending").astype(str)
    current_stage_col = base_df.get("Current Stage", "").astype(str)

    # ---------- Build mask per role (NO SQL) ----------
    if current_role in PRE_REVIEW_ROLES:
        # Parallel pre-review teams all pull directly from submitted items
        mask = (
            (is_submitter == 1)
            & (is_in_stage == 1)
            & ~status_in_stage.isin(["Approved", "Rejected"])
        )

    elif current_role == "DigitalGuild":
        mask = (
            (is_submitter == 1)
            & (current_stage_col == "Digital Guild")
            & (is_in_stage == 1)
            & ~status_in_stage.isin(["Approved", "Rejected"])
        )

    elif current_role == "ETIDM":
        mask = (
            (is_submitter == 1)
            & (current_stage_col == "ETIDM")
            & (is_in_stage == 1)
            & ~status_in_stage.isin(["Approved", "Rejected"])
        )

    else:
        # Unknown role – nothing to act on
        mask = pd.Series(False, index=base_df.index)

    actions_df = base_df.loc[mask].copy()

# -------------------------------------------------------------------
# 2. Render the action cards (unchanged styling)
# -------------------------------------------------------------------
if actions_df.empty:
    st.info("No items currently need your action.")
else:
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

        # Thin divider between cards
        st.markdown('<hr style="border: 0.5px solid #d3d3d3;">', unsafe_allow_html=True)

        c1, c2 = st.columns([5, 1])

        with c1:
            # Header: ID + Stage + Status pill
            st.markdown(
                f"""
                <div class="action-header">
                    <div class="action-title">
                        <span class="action-title-icon">📂</span>
                        <span>Sanction ID: {r.get('Sanction_ID', '')}</span>
                        <span class="action-pill-stage">
                            Stage: {r.get('Stage', '')}
                        </span>
                    </div>
                    <div class="status-pill {status_class}">
                        {r.get('Status in Stage', '')}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Value + risk row
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
            if st.button("View ➜", key=f"view_{r.get('Sanction_ID', '')}"):
                st.session_state["selected_sanction_id"] = str(r.get("Sanction_ID", ""))
                st.session_state.navigate_to_feedback = True
                st.rerun()

