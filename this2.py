# =====================================================
# STAGE ACTIONS – Sticky Action Bar (ROLE-LOCKED)
# =====================================================

# Global-ish styling for this section
st.markdown(
    """
    <style>
    /* Compact white inputs for text + textarea */
    .compact-input input, .compact-input textarea {
        font-size: 0.85rem;
        padding-top: 0.25rem;
        padding-bottom: 0.25rem;
        background-color: #ffffff !important;
    }

    .compact-input textarea {
        min-height: 80px;
    }

    /* Periwinkle buttons (Reset + Submit) */
    div.stButton > button {
        background-color: #A3ACF3 !important;
        color: #ffffff !important;
        border-radius: 999px !important;
        border: none;
        padding: 0.35rem 1.4rem;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-weight: 500;
        cursor: pointer;
    }

    div.stButton > button:hover {
        filter: brightness(0.97);
        transform: translateY(-0.5px);
    }

    div.stButton > button:focus {
        outline: 2px solid #7f88f0;
        outline-offset: 1px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Guard: if stage not configured
if current_stage not in STAGE_KEYS:
    existing_status = str(
        t_row.get("Overall_status", "Pending")
        if "Overall_status" in t_row.index
        else "Pending"
    )

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

    st.info("This stage has no configured actions.")
else:
    # --- Meta for this stage
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta.get("status", ""), "Pending"))

    # Header: big + bold + grey current status
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

    # --- Permissions
    user_internal_role = _current_internal_role()        # e.g. "DataGuild"
    user_stage_label = _current_stage_label_for_role()   # e.g. "Data Guild"
    role_can_act = (user_stage_label == current_stage)

    if not role_can_act:
        st.warning(
            f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**.  "
            f"Only the owning team may approve / reject / request changes for this stage."
        )

    # =====================================================
    # Decision form
    # =====================================================
    with st.form(f"form_{current_stage}"):

        # Decision – full width
        decision = st.radio(
            "Decision",
            ["Approve ✅", "Reject 🚫", "Request changes 🔥"],
            index=0,
            disabled=not role_can_act,
        )

        # Assigned To + Decision time – same row, compact inputs
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="compact-input">', unsafe_allow_html=True)
            assigned_to = st.text_input(
                "Assign to (email or name)",
                value=str(t_row.get(meta.get("assigned_to", "assigned_to"), "")),
                disabled=not role_can_act,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with col2:
            st.markdown('<div class="compact-input">', unsafe_allow_html=True)
            when = st.text_input(
                "Decision time",
                value=_now_iso(),
                help="Auto-filled; can be edited",
                disabled=not role_can_act,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        # Comments / Rationale – compact textarea below
        st.markdown('<div class="compact-input">', unsafe_allow_html=True)
        comment = st.text_area(
            "Comments / Rationale",
            placeholder="Add comments for the audit trail (optional)",
            disabled=not role_can_act,
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # Buttons row: close together, right aligned
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
    # Server-side enforcement and tracker updates
    # =====================================================
    if submitted:
        if not role_can_act:
            st.error("Action blocked: your role cannot act on this stage.")
            st.stop()

        if t_row.empty:
            tracker_df = pd.concat(
                [tracker_df, pd.DataFrame([{"Sanction_ID": sid}])],
                ignore_index=True,
            )
            t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

        tracker_df = _ensure_tracker_columns(tracker_df)

        dec_lower = decision.lower()
        if "approve" in dec_lower:
            new_status = "Approved"
        elif "reject" in dec_lower:
            new_status = "Rejected"
        else:
            new_status = "Changes requested"

        mask = tracker_df["Sanction_ID"] == sid

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
            st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")
        else:
            try:
                if "Sanction ID" in sanctions_df.columns:
                    ms = sanctions_df["Sanction ID"] == sid

                    if "Current Stage" in sanctions_df.columns and "Current Stage" in tracker_df.columns:
                        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]

                    if "Status" in sanctions_df.columns and "Overall_status" in tracker_df.columns:
                        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]

                    _write_csv(sanctions_df, SANCTIONS_PATH)
            except Exception as e:
                st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

            st.success(f"Saved decision for {sid} at {current_stage}: **{new_status}**")
            st.toast("Updated ✅")
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)
