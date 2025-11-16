# =====================================================
# STAGE ACTIONS – Sticky Action Bar (ROLE-LOCKED)
# =====================================================
meta = STAGE_KEYS.get(current_stage, {})
existing_status = str(t_row.get(meta.get("status", ""), "Pending"))

# Header (Big, sexy blue card)
st.markdown(
    f"""
    <div style="
        margin-bottom:16px;
        background: linear-gradient(120deg,#0b1f4a 0%, #1d4ed8 45%, #38bdf8 100%);
        padding: 1.25rem 1.6rem;
        border-radius: 1rem;
        display:flex;
        justify-content:space-between;
        align-items:center;
        box-shadow:0 10px 25px rgba(15,23,42,0.35);
        color:#ffffff;
    ">
      <div>
        <div style="font-size:0.9rem; opacity:0.85; letter-spacing:0.08em; text-transform:uppercase;">
          Stage Actions
        </div>
        <div style="font-size:1.6rem; font-weight:700; margin-top:0.15rem;">
          {current_stage}
        </div>
      </div>
      <div style="text-align:right;">
        <div style="font-size:0.9rem; opacity:0.9; margin-bottom:0.15rem;">
          Current status
        </div>
        <span class="badge {_pill_class(existing_status)}" style="
            display:inline-block;
            background-color: rgba(15,23,42,0.25);
            border-radius:999px;
            padding:0.25rem 0.9rem;
            font-size:0.85rem;
            font-weight:600;
            color:#ffffff;
            border:1px solid rgba(248,250,252,0.35);
        ">
          {existing_status}
        </span>
      </div>
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

        # 1. Decision
        decision = st.radio(
            "Decision",
            ["Approve ✅", "Reject 🚫", "Request changes 🔥"],
            index=0,
            disabled=not role_can_act,
        )

        # 2. Assigned To + Decision Time
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

        # 3. Comments / Rationale
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

# ==============================
# GLOBAL BLUE/WHITE FORM THEME
# ==============================
st.markdown(
    """
<style>
/* Inter font globally */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
}

/* Form container – soft blue card */
div[data-testid="stForm"] {
    background: #f8fafc;
    border-radius: 1rem;
    padding: 1.1rem 1.3rem;
    border: 1px solid #e2e8f0;
    box-shadow: 0 10px 25px rgba(15,23,42,0.08);
}

/* Bigger + bold labels in deep navy */
.stRadio > label,
.stTextInput > label,
.stTextArea > label,
.stSelectbox > label,
label[data-testid="stMarkdownContainer"] {
    font-weight: 600 !important;
    font-size: 1rem !important;
    margin-bottom: 0.25rem !important;
    color: #0b1f4a !important;
}

/* Compact white text inputs with blue border */
.stTextInput > div > div > input {
    font-size: 0.9rem !important;
    padding: 0.35rem 0.5rem !important;
    background-color: #ffffff !important;
    border-radius: 0.55rem !important;
    border: 1px solid #c4d3ff !important;
}

/* Textarea */
.stTextArea textarea {
    font-size: 0.9rem !important;
    padding: 0.4rem 0.5rem !important;
    min-height: 90px !important;
    background-color: #ffffff !important;
    border-radius: 0.55rem !important;
    border: 1px solid #c4d3ff !important;
}

/* Focus glow for inputs + textareas */
.stTextInput > div > div > input:focus-visible,
.stTextArea textarea:focus-visible {
    outline: none !important;
    border-color: #2563eb !important;
    box-shadow: 0 0 0 1px rgba(37,99,235,0.25) !important;
}

/* Gradient blue pills for all form buttons */
.stForm button {
    background: linear-gradient(135deg, #1f3a8a 0%, #2563eb 50%, #38bdf8 100%) !important;
    color: #ffffff !important;
    border-radius: 999px !important;
    padding: 0.45rem 1.5rem !important;
    font-size: 0.9rem !important;
    font-weight: 600 !important;
    border: none !important;
    cursor: pointer !important;
    box-shadow: 0 8px 16px rgba(37,99,235,0.35) !important;
    transition: transform 0.12s ease-out, box-shadow 0.12s ease-out, filter 0.12s ease-out;
}

/* Hover state – lift + brighten */
.stForm button:hover {
    filter: brightness(1.03);
    transform: translateY(-1px);
    box-shadow: 0 10px 22px rgba(30,64,175,0.45) !important;
}

/* Slightly dim disabled buttons */
.stForm button:disabled {
    filter: grayscale(0.2) brightness(0.9) !important;
    box-shadow: none !important;
    cursor: not-allowed !important;
}
</style>
""",
    unsafe_allow_html=True,
)
