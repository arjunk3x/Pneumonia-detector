# ================================
# STAGE ACTIONS - Sticky Action Bar (ROLE-LOCKED)
# ================================
meta = STAGE_KEYS.get(current_stage, {})
existing_status = str(_row.get(meta.get("status", ""), "Pending"))

# Header (Big + Bold + Grey status)
st.markdown(
    f"""
    <div style="margin-bottom:10px;">
        <span style="font-size:1.8rem; font-weight:700;">
            Stage Actions | {current_stage}
        </span><br>
        <span style="color:#6c757d; font-size:1.1rem;">
            Current status:
            <span class="badge {pill_class(existing_status)}">
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
    # ============================
    # Permissions
    # ============================
    user_internal_role = _current_internal_role()
    user_stage_label = _current_stage_label_for_role()
    role_can_act = (user_stage_label == current_stage)

    if not role_can_act:
        st.warning(
            f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**."
        )

    # ============================
    # DECISION FORM
    # ============================
    with st.form(f"form_{current_stage}"):

        # 1. Decision (label now BIG & BOLD)
        decision = st.radio(
            "**Choose Your Action [Approve/Reject/Request changes]:**",
            ["Approve ✔", "Reject ✖", "Request changes 🔁"],
            index=0,
            disabled=not role_can_act,
        )

        rating_stars = st.selectbox(
            "**Rating (optional):**",
            ["⭐⭐⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐", "⭐⭐", "⭐", "—"],
            index=0,
            disabled=not role_can_act,
        )
        rating = rating_stars.count("⭐")

        # 2. Assigned To + Decision Time (labels also BIG & BOLD)
        col1, col2 = st.columns(2)

        with col1:
            assigned_to = st.text_input(
                "**Assign to [Email/Name]:**",
                disabled=not role_can_act,
            )

        with col2:
            when = st.text_input(
                "**Decision time:**",
                value=_now_iso(),
                help="Auto-filled, editable.",
                disabled=not role_can_act,
            )

        # 3. Comments / Rationale (label BIG & BOLD)
        comment = st.text_area(
            "**Comments / Rationale**",
            placeholder="Add remarks for documentation",
            disabled=not role_can_act,
        )

        # 4. Buttons (right-aligned, close together)
        spacer, col_buttons = st.columns([0.77, 0.23])
        b_reset, b_submit = col_buttons.columns([0.47, 0.53])

        with b_reset:
            cancel = st.form_submit_button(
                "Reset form", disabled=not role_can_act
            )

        with b_submit:
            submitted = st.form_submit_button(
                "Submit decision", disabled=not role_can_act
            )

    # ================================
    # BACKEND SUBMISSION LOGIC
    # ================================
    if submitted:

        # 0. Permission guard
        if not role_can_act:
            st.error("You are not authorised to perform this action.")
            st.stop()

        # 1. Ensure tracker row exists
        if tracker_df.empty:
            tracker_df = pd.concat(
                [
                    tracker_df,
                    pd.DataFrame([{"Sanction_ID": sid}]),
                ],
                ignore_index=True,
            )

        _row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

        tracker_df = _ensure_tracker_columns(tracker_df)
        mask = tracker_df["Sanction_ID"] == sid

        # Keep previous status before overwriting (used to block double-rejection)
        prev_status = str(_row.get(meta.get("status", ""), ""))

        # 2. Map decision -> new_status
        dec_lower = decision.lower()
        new_status = "Changes requested"
        if "approve" in dec_lower:
            new_status = "Approved"
        elif "reject" in dec_lower:
            new_status = "Rejected"

        # Prevent re-submission when already rejected (same behaviour as Approve)
        if new_status == "Rejected" and prev_status == "Rejected":
            st.warning(
                "This sanction has already been rejected. No further actions can be taken."
            )
            st.stop()

        # 3. Basic field updates
        tracker_df.loc[mask, meta["status"]] = new_status
        tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
        tracker_df.loc[mask, "last_comment"] = comment

        # 4. Correct Stage Progression Logic
        flag_field = meta["flag"]
        decision_field = meta["decision_at"]

        nxt = _next_stage(current_stage) if new_status == "Approved" else None

        # --------- APPROVED ---------
        if new_status == "Approved":

            # Write timestamp + this means stage is completed
            tracker_df.loc[mask, decision_field] = when

            # Turn off current stage
            tracker_df.loc[mask, flag_field] = False

            if nxt:
                # Move to next stage
                tracker_df.loc[mask, "Current Stage"] = nxt
                tracker_df.loc[mask, "Overall_status"] = "In progress"

                # Turn ON only the next stage flag, all others off
                for stg, m in STAGE_KEYS.items():
                    tracker_df.loc[mask, m["flag"]] = (stg == nxt)
            else:
                # Last stage fully approved
                tracker_df.loc[mask, "Overall_status"] = "Approved"

        # --------- REJECTED ---------
        elif new_status == "Rejected":

            # Clear timestamp -> means stage NOT completed
            tracker_df.loc[mask, decision_field] = ""

            # Update status fields
            tracker_df.loc[mask, "Overall_status"] = "Rejected"
            tracker_df.loc[mask, "Current Stage"] = current_stage

            # All is_on flags must be FALSE
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = False

        # --------- REQUEST CHANGES ---------
        else:
            # Clear timestamp
            tracker_df.loc[mask, decision_field] = ""

            tracker_df.loc[mask, "Overall_status"] = "Changes requested"
            tracker_df.loc[mask, "Current Stage"] = current_stage

            # Only current stage active
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

        # 5. Save to CSVs (ONLY here)
        _write_csv(tracker_df, APPROVER_TRACKER_PATH)

        if "Sanction ID" in sanctions_df.columns:
            ms = sanctions_df["Sanction ID"] == sid
            sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
            sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
            _write_csv(sanctions_df, SANCTIONS_PATH)

        # 6. Notifications (ONLY inside submit)
        if new_status == "Approved":
            add_notification(
                sanction_id=sid,
                team=current_stage,
                message=f"Sanction {sid} approved by {current_stage}",
            )

        # 7. Save feedback log (ONLY inside submit)
        # -------------------------------------
        import uuid
        feedback = {
            "comment_id": str(uuid.uuid4()),
            "sanction_id": sid,
            "stage": current_stage,
            "rating": rating,
            "comment": comment,
            "username": assigned_to,
            "created_at": _now_iso(),
        }

        save_fb(feedback, "feedback.csv")

        # 8. Finish
        # -------------------------------------
        st.success(f"Saved decision for {sid} at {current_stage}: {new_status}")
        st.toast("Updated ✓")
        st.stop()

# ================================
# TRACKER SNAPSHOT (for next stage)
# (everything below here in your screenshot was commented out)
# ================================
# st.divider()
# st.subheader("Tracker Snapshot (next-stage data)")
# ...
