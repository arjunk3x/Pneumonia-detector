# ============================================================
# BACKEND SUBMISSION LOGIC  (runs ONLY when submit is pressed)
# ============================================================
if submitted:

    # Permission guard
    if not role_can_act:
        st.error("You are not authorised to perform this action.")
        st.stop()

    # Ensure row exists
    if t_row.empty:
        tracker_df = pd.concat([
            tracker_df,
            pd.DataFrame([{"Sanction_ID": sid}])
        ], ignore_index=True)

    tracker_df = _ensure_tracker_columns(tracker_df)
    mask = tracker_df["Sanction_ID"] == sid

    # --------------------------
    # MAP DECISION → STATUS
    # --------------------------
    dec_lower = decision.lower()

    if "approve" in dec_lower:
        new_status = "Approved"
    elif "reject" in dec_lower:
        new_status = "Rejected"
    else:
        new_status = "Changes requested"

    # Update basic fields
    tracker_df.loc[mask, meta["status"]] = new_status
    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
    tracker_df.loc[mask, "Last_comment"] = comment

    # --------------------------
    # STAGE PROGRESSION LOGIC
    # --------------------------
    flag_field = meta["flag"]
    decision_field = meta["decision_at"]

    nxt = _next_stage(current_stage) if new_status == "Approved" else None

    # APPROVED
    if new_status == "Approved":

        # Set timestamp only for approved
        tracker_df.loc[mask, decision_field] = when

        # Disable current stage
        tracker_df.loc[mask, flag_field] = False

        if nxt:
            # Move to next stage
            tracker_df.loc[mask, "Current Stage"] = nxt
            tracker_df.loc[mask, "Overall_status"] = "In progress"

            # Set ALL other is_in flags to False except next stage
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == nxt)

        else:
            # Final approval
            tracker_df.loc[mask, "Overall_status"] = "Approved"

    # REJECTED
    elif new_status == "Rejected":

        tracker_df.loc[mask, decision_field] = ""

        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"] = current_stage

        # Turn ALL flags OFF
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False

    # CHANGES REQUESTED
    else:

        tracker_df.loc[mask, decision_field] = ""

        tracker_df.loc[mask, "Overall_status"] = "Changes requested"
        tracker_df.loc[mask, "Current Stage"] = current_stage

        # Turn ON only current stage
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

    # --------------------------
    # SAVE TRACKER + SANCTIONS
    # --------------------------
    _write_csv(tracker_df, APPROVER_TRACKER_PATH)

    if "Sanction ID" in sanctions_df.columns:
        ms = sanctions_df["Sanction ID"] == sid
        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
        _write_csv(sanctions_df, SANCTIONS_PATH)

    # --------------------------
    # NOTIFICATIONS
    # --------------------------
    if new_status == "Approved":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} approved by {current_stage}"
        )

    # --------------------------
    # FEEDBACK CSV
    # --------------------------
    import uuid
    feedback_row = {
        "comment_id": str(uuid.uuid4()),
        "sanction_id": sid,
        "stage": current_stage,
        "rating": rating,
        "comment": comment,
        "username": assigned_to,
        "created_at": _now_iso()
    }
    save_fb(feedback_row, "feedback.csv")

    # Final UI updates
    st.success(f"Saved decision for {sid} at {current_stage}: {new_status}")
    st.toast("Updated ✓")
    st.rerun()
