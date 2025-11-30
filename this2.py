if submitted:

    # 0. Permission guard
    if not role_can_act:
        st.error("You are not authorised to perform this action.")
        st.stop()

    # 1. Ensure tracker row exists
    if tracker_df.empty:
        tracker_df = pd.DataFrame([{"Sanction_ID": sid}])

    if "Sanction_ID" not in tracker_df.columns:
        tracker_df["Sanction_ID"] = ""

    if sid not in tracker_df["Sanction_ID"].astype(str).tolist():
        tracker_df = pd.concat(
            [tracker_df, pd.DataFrame([{"Sanction_ID": sid}])],
            ignore_index=True,
        )

    # normalise + locate current row
    tracker_df = _ensure_tracker_columns(tracker_df)
    mask = tracker_df["Sanction_ID"].astype(str) == sid
    _row = tracker_df.loc[mask].iloc[0]

    meta        = STAGE_KEYS[current_stage]
    status_col  = meta["status"]
    assigned_col = meta["assigned_to"]
    flag_field  = meta["flag"]
    decision_field = meta["decision_at"]

    prev_status = str(_row.get(status_col, ""))

    # 2. Map decision -> new_status
    dec_lower = decision.lower()
    if "approve" in dec_lower:
        new_status = "Approved"
    elif "reject" in dec_lower:
        new_status = "Rejected"
    else:
        # your "request changes" label
        new_status = "PT request changes"

    # 2b. FINAL decision guard — ONLY for Approve / Reject
    if new_status in ("Approved", "Rejected"):
        decision_key = f"decision_done_{sid}_{current_stage}"

        if st.session_state.get(decision_key, False):
            st.warning(
                "This sanction has already received a final decision at this stage. "
                "No further Approve/Reject actions can be taken."
            )
            st.stop()

        # Mark this sanction+stage as finally decided
        st.session_state[decision_key] = True

    # 2c. Prevent re-submission when already rejected
    if new_status == "Rejected" and prev_status == "Rejected":
        st.warning("This sanction has already been rejected. No further actions can be taken.")
        st.stop()

    # 3. Basic field updates
    tracker_df.loc[mask, status_col]      = new_status
    tracker_df.loc[mask, assigned_col]    = assigned_to
    tracker_df.loc[mask, "last_comment"]  = comment

    # 4. Stage progression logic (your existing logic kept, just using new_status)

    # ---------- APPROVED ----------
    if new_status == "Approved":
        # Mark this stage as completed
        tracker_df.loc[mask, decision_field] = _now_iso()
        tracker_df.loc[mask, flag_field]     = False

        # Re-read the row after updating this stage
        row_after = tracker_df.loc[mask].iloc[0]

        if current_stage in PRE_REVIEW_STAGES:
            # Parallel reviewers: only move on when ALL 5 have approved
            if all_pre_reviewers_approved(row_after):
                # Move into Digital Guild
                tracker_df.loc[mask, "Current Stage"]   = "DigitalGuild"
                tracker_df.loc[mask, "Overall_status"]  = "In progress"

                # Turn on only Digital Guild flag
                for stg, m in STAGE_KEYS.items():
                    tracker_df.loc[mask, m["flag"]] = (stg == "DigitalGuild")
            else:
                # Still waiting for other reviewers
                tracker_df.loc[mask, "Overall_status"] = "In progress"

        elif current_stage == "DigitalGuild":
            # Digital Guild -> ETIDM
            tracker_df.loc[mask, "Current Stage"]  = "ETIDM"
            tracker_df.loc[mask, "Overall_status"] = "In progress"
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == "ETIDM")

        elif current_stage == "ETIDM":
            # Final approval
            tracker_df.loc[mask, "Overall_status"] = "Approved"
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = False

    # ---------- REJECTED ----------
    elif new_status == "Rejected":
        tracker_df.loc[mask, decision_field]  = _now_iso()
        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"]  = current_stage

        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False

    # ---------- REQUEST CHANGES ----------
    else:  # new_status == "PT request changes"
        tracker_df.loc[mask, decision_field]   = ""
        tracker_df.loc[mask, "Overall_status"] = "PT request changes"
        tracker_df.loc[mask, "Current Stage"]  = current_stage

        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = 0

    # 5. Persist tracker CSV (ONLY here)
    _write_csv(tracker_df, APPROVER_TRACKER_PATH)

    # 6. Mirror to sanctions view
    if "Sanction_ID" in sanctions_df.columns:
        ms = sanctions_df["Sanction_ID"].astype(str) == sid
        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
        sanctions_df.loc[ms, "Status"]        = tracker_df.loc[mask, "Overall_status"].iloc[0]
        _write_csv(sanctions_df, SANCTIONS_PATH)

    # 7. Notifications (ONLY inside submit)
    if new_status == "Approved":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} approved by {current_stage}.",
        )
    elif new_status == "Rejected":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} rejected by {current_stage}.",
        )
    # (no notification for PT request changes unless you want one)

    # 8. Feedback log (CSV with comment_id, sanction_id, stage, rating, comment, username, created_at)
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
    save_fb(feedback)

    # 9. Finish
    st.success(f"Saved decision for {sid} at {current_stage} [{new_status}]")
    st.toast("Updated ✓")
    st.stop()













# ---------- REQUEST CHANGES ----------
else:  # new_status == "Changes requested"
    tracker_df.loc[mask, decision_field] = ""
    tracker_df.loc[mask, "Overall_status"] = "Changes requested"

    # keep stage name so we know which team requested the changes
    tracker_df.loc[mask, "Current Stage"] = current_stage

    # 1) turn OFF this team's "is_in_*" flag
    flag_field = meta["flag"]          # e.g. "is_in_head_data_ai"
    tracker_df.loc[mask, flag_field] = False

    # 2) (optional but recommended) ensure ALL team flags are off
    for stg, m in STAGE_KEYS.items():
        tracker_df.loc[mask, m["flag"]] = False
