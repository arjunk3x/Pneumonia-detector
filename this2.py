st.markdown("""
    <style>

    /* Import Inter font globally */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI",
                     Roboto, Helvetica, Arial, sans-serif !important;
    }

    /* 🔥 Bigger + Bold form labels */
    .stRadio > label,
    .stTextInput > label,
    .stTextArea > label,
    .stSelectbox > label,
    label[data-testid="stMarkdownContainer"] {
        font-weight: 600 !important;
        font-size: 1rem !important;
        margin-bottom: 0.25rem !important;
    }

    /* Compact white text inputs */
    .stTextInput > div > div > input {
        font-size: 0.9rem !important;
        padding: 0.3rem 0.4rem !important;
        background-color: #ffffff !important;
    }

    /* Compact white textarea */
    .stTextArea textarea {
        font-size: 0.9rem !important;
        padding: 0.35rem 0.4rem !important;
        min-height: 90px !important;
        background-color: #ffffff !important;
    }

    /* Pastel periwinkle buttons */
    .stForm button {
        background-color: #A3ACF3 !important;
        color: #ffffff !important;
        border-radius: 999px !important;
        padding: 0.45rem 1.4rem !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        border: none !important;
        cursor: pointer !important;
        transition: 0.15s ease-in-out;
    }

    .stForm button:hover {
        filter: brightness(0.95);
        transform: translateY(-1px);
    }

    </style>
""", unsafe_allow_html=True)


# =====================================================
# STAGE ACTIONS – Sticky Action Bar (ROLE-LOCKED)
# =====================================================

meta = STAGE_KEYS.get(current_stage, {})
existing_status = str(t_row.get(meta.get("status", ""), "Pending"))

# Header (Big + Bold + Grey status)
st.markdown(
    f"""
    <div style="margin-bottom:10px;">
        <span style="font-size:1.5rem; font-weight:700;">
            Stage Actions – {current_stage}
        </span><br>
        <span style="color:#6c757d; font-size:1rem;">
            Current status:
            <span class="badge {_pill_class(existing_status)}">
                {existing_status}
            </span>
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)

# If stage not configured
if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:

    # Permissions
    user_internal_role = _current_internal_role()
    user_stage_label = _current_stage_label_for_role()
    role_can_act = (user_stage_label == current_stage)

    if not role_can_act:
        st.warning(
            f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**."
        )

    # =====================================================
    # DECISION FORM
    # =====================================================
    with st.form(f"form_{current_stage}"):

        # 1. Decision (label now BIG + BOLD)
        decision = st.radio(
            "Decision",
            ["Approve ✅", "Reject 🚫", "Request changes 🔥"],
            index=0,
            disabled=not role_can_act,
        )

        # 2. Assigned To + Decision Time (labels also BIG + BOLD)
        col1, col2 = st.columns(2)

        with col1:
            assigned_to = st.text_input(
                "Assign to (email or name)",
                value=str(t_row.get(meta.get("assigned_to", ""), "")),
                disabled=not role_can_act,
            )

        with col2:
            when = st.text_input(
                "Decision time",
                value=_now_iso(),
                help="Auto-filled; editable",
                disabled=not role_can_act,
            )

        # 3. Comments / Rationale (label BIG + BOLD)
        comment = st.text_area(
            "Comments / Rationale",
            placeholder="Add comments for audit trail (optional)",
            disabled=not role_can_act,
        )

        # 4. Buttons (right-aligned, close together)
        spacer, col_buttons = st.columns([0.6, 0.4])
        b_reset, b_submit = col_buttons.columns([0.48, 0.52])

        with b_reset:
            cancel = st.form_submit_button("Reset form", disabled=not role_can_act)

        with b_submit:
            submitted = st.form_submit_button("Submit decision", disabled=not role_can_act)

    # =====================================================
    # BACKEND SUBMISSION LOGIC
    # =====================================================
    if submitted:
        if not role_can_act:
            st.error("You are not authorised to perform this action.")
            st.stop()

        tracker_df = _ensure_tracker_columns(tracker_df)
        mask = tracker_df["Sanction_ID"] == sid

        # Map decision → status
        dec_lower = decision.lower()
        if "approve" in dec_lower:
            new_status = "Approved"
        elif "reject" in dec_lower:
            new_status = "Rejected"
        else:
            new_status = "Changes requested"

        # Update fields
        tracker_df.loc[mask, meta["status"]] = new_status
        tracker_df.loc[mask, meta.get("assigned_to", "assigned_to")] = assigned_to
        tracker_df.loc[mask, meta.get("decision_at", "decision_at")] = when
        tracker_df.loc[mask, meta.get("comment", "comment")] = comment

        nxt = _next_stage(current_stage) if new_status == "Approved" else None

        if new_status == "Approved" and nxt:
            tracker_df.loc[mask, "Current Stage"] = nxt
            tracker_df.loc[mask, "Overall_status"] = "In progress"
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == nxt)
        elif new_status == "Rejected":
            tracker_df.loc[mask, "Overall_status"] = "Rejected"
        else:
            tracker_df.loc[mask, "Overall_status"] = "Changes requested"

        # Save & sync to files
        _write_csv(tracker_df, APPROVER_TRACKER_PATH)

        if "Sanction ID" in sanctions_df.columns:
            ms = sanctions_df["Sanction ID"] == sid
            sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
            sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
            _write_csv(sanctions_df, SANCTIONS_PATH)

        st.success(f"Decision saved: **{new_status}**")
        st.toast("Updated successfully ✔️")
        st.rerun()
