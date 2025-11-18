# ============================================================
# BACKEND SUBMISSION LOGIC  (must NOT run unless submitted)
# ============================================================
if submitted:

    # ------------------------------
    # 0. Permission guard
    # ------------------------------
    if not role_can_act:
        st.error("You are not authorised to perform this action.")
        st.stop()

    # ------------------------------
    # 1. Ensure tracker row exists
    # ------------------------------
    if t_row.empty:
        tracker_df = pd.concat([
            tracker_df,
            pd.DataFrame([{"Sanction_ID": sid}])
        ], ignore_index=True)
        t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

    tracker_df = _ensure_tracker_columns(tracker_df)
    mask = tracker_df["Sanction_ID"] == sid

    # ------------------------------
    # 2. Map decision → new_status
    # ------------------------------
    dec_lower = decision.lower()
    if "approve" in dec_lower:
        new_status = "Approved"
    elif "reject" in dec_lower:
        new_status = "Rejected"
    else:
        new_status = "Changes requested"

    # ------------------------------
    # 3. Basic field updates
    # ------------------------------
    tracker_df.loc[mask, meta["status"]] = new_status
    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
    tracker_df.loc[mask, "Last_comment"] = comment

    # NOTE: do NOT set decision_at outside of Approved
    #       otherwise it creates infinite loop

    # ------------------------------
    # 4. Correct Stage Progression Logic
    # ------------------------------
    flag_field = meta["flag"]
    decision_field = meta["decision_at"]

    nxt = _next_stage(current_stage) if new_status == "Approved" else None

    # ---------- APPROVED ----------
    if new_status == "Approved":

        # Write timestamp → this means stage is completed
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

    # ---------- REJECTED ----------
    elif new_status == "Rejected":

        # Clear timestamp → means stage NOT completed
        tracker_df.loc[mask, decision_field] = ""

        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"] = current_stage

        # All is_in flags must be FALSE
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False

    # ---------- REQUEST CHANGES ----------
    else:

        # Clear timestamp
        tracker_df.loc[mask, decision_field] = ""

        tracker_df.loc[mask, "Overall_status"] = "Changes requested"
        tracker_df.loc[mask, "Current Stage"] = current_stage

        # Only current stage active
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

    # ------------------------------
    # 5. Save to CSVs (ONLY here)
    # ------------------------------
    _write_csv(tracker_df, APPROVER_TRACKER_PATH)

    if "Sanction ID" in sanctions_df.columns:
        ms = sanctions_df["Sanction ID"] == sid
        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
        _write_csv(sanctions_df, SANCTIONS_PATH)

    # ------------------------------
    # 6. Notifications (ONLY inside submit)
    # ------------------------------
    if new_status == "Approved":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} approved by {current_stage}"
        )

    # ------------------------------
    # 7. Save feedback log (ONLY inside submit)
    # ------------------------------
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

    # ------------------------------
    # 8. Finish
    # ------------------------------
    st.success(f"Saved decision for {sid} at {current_stage}: {new_status}")
    st.toast("Updated ✓")
    st.rerun()







elif new_status == "Rejected":

    # Store timestamp (so page knows a decision was made)
    tracker_df.loc[mask, decision_field] = when

    tracker_df.loc[mask, "Overall_status"] = "Rejected"
    tracker_df.loc[mask, "Current Stage"] = current_stage

    # Turn OFF this stage so user cannot act again
    tracker_df.loc[mask, flag_field] = False

    # All other stages also false
    for stg, m in STAGE_KEYS.items():
        tracker_df.loc[mask, m["flag"]] = False

