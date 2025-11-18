if submitted:
    # Ensure row exists in tracker
    if t_row.empty:
        tracker_df = pd.concat([tracker_df, pd.DataFrame([{"Sanction_ID": sid}])], ignore_index=True)
        t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

    tracker_df = _ensure_tracker_columns(tracker_df)

    dec_lower = decision.lower()
    new_status = (
        "Approved" if "approve" in dec_lower else
        ("Rejected" if "reject" in dec_lower else "Changes requested")
    )

    mask = tracker_df["Sanction_ID"] == sid

    # Update assigned_to & comments
    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
    tracker_df.loc[mask, "Last_comment"] = comment

    # ----------------------------------------------------------------------
    # NEW PROGRESSION LOGIC BASED 100% ON TIMESTAMP MEANING
    # ----------------------------------------------------------------------

    # Fields for current stage
    flag_field = meta["flag"]
    decision_field = meta["decision_at"]

    # =========================
    # 1️⃣ APPROVED
    # =========================
    if new_status == "Approved":

        # Write decision timestamp → means stage completed
        tracker_df.loc[mask, decision_field] = when or _now_iso()

        # Turn OFF current stage flag
        tracker_df.loc[mask, flag_field] = False

        # Move to next stage
        nxt = _next_stage(current_stage)
        if nxt:
            tracker_df.loc[mask, "Current Stage"] = nxt
            tracker_df.loc[mask, "Overall_status"] = "In progress"

            # Enable only next_team.is_in
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == nxt)

        else:
            # Final stage approved (ETIDM)
            tracker_df.loc[mask, "Overall_status"] = "Approved"
            tracker_df.loc[mask, flag_field] = False

    # =========================
    # 2️⃣ REJECTED
    # =========================
    elif new_status == "Rejected":

        # CLEAR the decision timestamp → stage was NOT completed
        tracker_df.loc[mask, decision_field] = ""

        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"] = current_stage

        # Rejected → ALL is_in_* flags must be FALSE
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False

    # =========================
    # 3️⃣ REQUEST CHANGES
    # =========================
    else:  # changes requested

        # CLEAR decision timestamp → stage incomplete
        tracker_df.loc[mask, decision_field] = ""

        tracker_df.loc[mask, "Overall_status"] = "Changes requested"
        tracker_df.loc[mask, "Current Stage"] = current_stage

        # Stay in same stage → only current stage flag TRUE
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

    # ----------------------------------------------------------------------
    # Save CSVs
    # ----------------------------------------------------------------------
    try:
        _write_csv(tracker_df, APPROVER_TRACKER_PATH)
    except Exception as e:
        st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")

    # Mirror change into sanctions_data CSV
    try:
        if "Sanction ID" in sanctions_df.columns:
            ms = sanctions_df["Sanction ID"] == sid
            sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
            sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
            _write_csv(sanctions_df, SANCTIONS_PATH)
    except Exception as e:
        st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

    st.success(f"Saved decision for {sid} at {current_stage}: {new_status}")
    st.toast("Updated ✓")
    st.rerun()
