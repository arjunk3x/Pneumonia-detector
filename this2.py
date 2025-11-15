# =====================================================
# STAGE ACTIONS – Sticky Action Bar (ROLE-LOCKED)
# =====================================================

st.markdown(
    f"""
    <div style="margin-bottom:10px;">
        <span style="font-size:1.5rem; font-weight:700;">
            Stage Actions – {current_stage}
        </span>
        <br>
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

if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:
    # --- Meta + permissions
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta["status"], "Pending"))

    user_internal_role = _current_internal_role()        # e.g. "DataGuild"
    user_stage_label = _current_stage_label_for_role()   # e.g. "Data Guild"
    role_can_act = (user_stage_label == current_stage)

    if not role_can_act:
        st.warning(
            f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**.  "
            f"Only the owning team may approve / reject / request changes for this stage."
        )

    # ---------------------------
    # Decision form
    # ---------------------------
    with st.form(f"form_{current_stage}"):

        # Decision (full width)
        decision = st.radio(
            "Decision",
            ["Approve ✅", "Reject 🚫", "Request changes 🔥"],
            index=0,
            disabled=not role_can_act,
        )

        # Assigned To + Decision time (same row)
        col1, col2 = st.columns(2)
        with col1:
            assigned_to = st.text_input(
                "Assign to (email or name)",
                value=str(t_row.get(meta.get("assigned_to", "assigned_to"), "")),
                disabled=not role_can_act,
            )
        with col2:
            when = st.text_input(
                "Decision time",
                value=_now_iso(),
                help="Auto-filled; can be edited",
                disabled=not role_can_act,
            )

        # Comments / Rationale (full width under the row)
        comment = st.text_area(
            "Comments / Rationale",
            placeholder="Add comments for the audit trail (optional)",
            disabled=not role_can_act,
        )

        # Buttons row: Reset (secondary) then Submit (primary), right aligned
        spacer, c_reset, c_submit = st.columns([0.6, 0.2, 0.2])

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
        # Hard-block if user somehow bypasses the UI lock
        if not role_can_act:
            st.error("Action blocked: your role cannot act on this stage.")
            st.stop()

        # Ensure row exists in tracker
        if t_row.empty:
            tracker_df = pd.concat(
                [tracker_df, pd.DataFrame([{"Sanction_ID": sid}])],
                ignore_index=True,
            )
            t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

        tracker_df = _ensure_tracker_columns(tracker_df)

        # Determine new status from radio selection
        dec_lower = decision.lower()
        if "approve" in dec_lower:
            new_status = "Approved"
        elif "reject" in dec_lower:
            new_status = "Rejected"
        else:
            new_status = "Changes requested"

        mask = tracker_df["Sanction_ID"] == sid

        # Update stage-specific columns
        tracker_df.loc[mask, meta["status"]] = new_status
        tracker_df.loc[mask, meta.get("assigned_to", "assigned_to")] = assigned_to
        tracker_df.loc[mask, meta.get("decision_at", "decision_at")] = when or _now_iso()
        tracker_df.loc[mask, meta.get("comment", "comment")] = comment

        # Decide next stage / overall status
        nxt = _next_stage(current_stage) if new_status == "Approved" else None
        if new_status == "Approved" and nxt:
            tracker_df.loc[mask, "Current Stage"] = nxt
            tracker_df.loc[mask, "Overall_status"] = "In progress"

            # Flip flags so only the next stage is "active"
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == nxt)

        elif new_status == "Rejected":
            tracker_df.loc[mask, "Overall_status"] = "Rejected"
        else:
            tracker_df.loc[mask, "Overall_status"] = "Changes requested"

        # Persist tracker
        try:
            _write_csv(tracker_df, APPROVER_TRACKER_PATH)
        except Exception as e:
            st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")
        else:
            # Optional mirror to sanctions_data
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

