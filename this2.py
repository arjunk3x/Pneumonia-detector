
# =====================================================
# GLOBAL STYLING FOR THIS PAGE
# =====================================================
st.markdown(
    """
    <style>

    /* Compact white text inputs */
    .stTextInput > div > div > input {
        font-size: 0.85rem !important;
        padding-top: 0.25rem !important;
        padding-bottom: 0.25rem !important;
        background-color: #ffffff !important;
        color: #000000 !important;
    }

    /* Compact white textarea */
    .stTextArea textarea {
        font-size: 0.85rem !important;
        padding-top: 0.35rem !important;
        padding-bottom: 0.35rem !important;
        background-color: #ffffff !important;
        min-height: 85px !important;
        color: #000000 !important;
    }

    /* Pastel periwinkle buttons inside FORMS */
    .stForm button {
        background-color: #A3ACF3 !important;
        color: #ffffff !important;
        border-radius: 999px !important;
        border: none !important;
        padding: 0.45rem 1.4rem !important;
        font-size: 0.9rem !important;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
        font-weight: 600 !important;
        cursor: pointer !important;
    }

    .stForm button:hover {
        filter: brightness(0.95);
        transform: translateY(-1px);
    }

    .stForm button:focus {
        outline: 2px solid #7f88f0 !important;
        outline-offset: 1px !important;
    }

    </style>
    """,
    unsafe_allow_html=True,
)



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
    st.stop()

# Permissions
user_internal_role = _current_internal_role()
user_stage_label = _current_stage_label_for_role()
role_can_act = (user_stage_label == current_stage)

if not role_can_act:
    st.warning(
        f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**. "
        f"Only the owning team may perform approval actions."
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

    # 2. Assigned to + Decision time (side-by-side)
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

    # 4. Buttons (right aligned, close together)
    spacer, c_reset, c_submit = st.columns([0.55, 0.2, 0.25])

    with c_reset:
        cancel = st.form_submit_button(
            "Reset form",
            disabled=not role_can_act,
        )

    with c_submit:
        submitted = st.form_submit_button(
            "Submit decision",
            disabled=not role_can_act,
        )

# =====================================================
# BACKEND LOGIC AFTER SUBMISSION
# =====================================================
if submitted:
    if not role_can_act:
        st.error("You are not authorised to perform this action.")
        st.stop()

    if t_row.empty:
        tracker_df = pd.concat(
            [tracker_df, pd.DataFrame([{"Sanction_ID": sid}])],
            ignore_index=True,
        )
        t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

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

    # Update DB fields
    tracker_df.loc[mask, meta["status"]] = new_status
    tracker_df.loc[mask, meta.get("assigned_to", "assigned_to")] = assigned_to
    tracker_df.loc[mask, meta.get("decision_at", "decision_at")] = when or _now_iso()
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

    try:
        _write_csv(tracker_df, APPROVER_TRACKER_PATH)
    except Exception as e:
        st.error(f"Failed to update tracker: {e}")
    else:
        try:
            if "Sanction ID" in sanctions_df.columns:
                ms = sanctions_df["Sanction ID"] == sid

                sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
                sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]

                _write_csv(sanctions_df, SANCTIONS_PATH)
        except Exception as e:
            st.warning(f"Could not update sanctions DB: {e}")

        st.success(f"Decision saved: **{new_status}**")
        st.toast("Updated successfully ✔️")
        st.rerun()
