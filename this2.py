# 4. Stage progression logic (parallel pre-review + DG -> DIDM -> ETIDM)
flag_field = meta["flag"]
decision_field = meta["decision_at"]

# ---------- APPROVED ----------
if new_status == "Approved":
    # Mark this stage as completed
    tracker_df.loc[mask, decision_field] = _now_iso()
    tracker_df.loc[mask, flag_field] = False  # this team no longer has it in Actions

    # Re-read the row after updating this stage
    row_after = tracker_df.loc[mask].iloc[0]

    if current_stage in PRE_REVIEW_STAGES:
        # Parallel reviewers: only move on when ALL 5 have approved
        if all_pre_reviewers_approved(row_after):
            # Move into Digital Guild
            tracker_df.loc[mask, "Current Stage"] = "DigitalGuild"
            tracker_df.loc[mask, "Overall_status"] = "In progress"

            # Turn on only Digital Guild flag
            for stg, m in STAGE_KEYS.items():
                flag_col = m.get("flag")
                if not flag_col:
                    continue
                tracker_df.loc[mask, flag_col] = (stg == "DigitalGuild")
        else:
            # Still waiting for other reviewers
            tracker_df.loc[mask, "Overall_status"] = "In progress"

    elif current_stage == "DigitalGuild":
        # Digital Guild -> DIDM
        tracker_df.loc[mask, "Current Stage"] = "DIDM"
        tracker_df.loc[mask, "Overall_status"] = "In progress"

        for stg, m in STAGE_KEYS.items():
            flag_col = m.get("flag")
            if not flag_col:
                continue
            tracker_df.loc[mask, flag_col] = (stg == "DIDM")

    elif current_stage == "DIDM":
        # DIDM -> ETIDM
        tracker_df.loc[mask, "Current Stage"] = "ETIDM"
        tracker_df.loc[mask, "Overall_status"] = "In progress"

        for stg, m in STAGE_KEYS.items():
            flag_col = m.get("flag")
            if not flag_col:
                continue
            tracker_df.loc[mask, flag_col] = (stg == "ETIDM")

    elif current_stage == "ETIDM":
        # Final approval
        tracker_df.loc[mask, "Overall_status"] = "Approved"

        for stg, m in STAGE_KEYS.items():
            flag_col = m.get("flag")
            if not flag_col:
                continue
            tracker_df.loc[mask, flag_col] = False

# ---------- REJECTED ----------
elif new_status == "Rejected":
    # You can either timestamp or clear; here we timestamp
    tracker_df.loc[mask, decision_field] = _now_iso()
    tracker_df.loc[mask, "Overall_status"] = "Rejected"
    tracker_df.loc[mask, "Current Stage"] = current_stage

    # All stage flags off (nobody can act further)
    for stg, m in STAGE_KEYS.items():
        flag_col = m.get("flag")
        if not flag_col:
            continue
        tracker_df.loc[mask, flag_col] = False

# ---------- REQUEST CHANGES ----------
else:  # "Changes requested"
    tracker_df.loc[mask, decision_field] = ""
    tracker_df.loc[mask, "Overall_status"] = "Request Changes"
    tracker_df.loc[mask, "Current Stage"] = current_stage

    # Only the requesting team keeps the flag = 1 so it goes back to their Actions
    for stg, m in STAGE_KEYS.items():
        flag_col = m.get("flag")
        if not flag_col:
            continue
        tracker_df.loc[mask, flag_col] = (stg == current_stage)
