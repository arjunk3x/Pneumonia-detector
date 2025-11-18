# Determine next stage ONLY for approved decisions
nxt = _next_stage(current_stage) if new_status == "Approved" else None

if new_status == "Approved":
    if nxt:
        # Move to next stage
        tracker_df.loc[mask, "Current Stage"] = nxt
        tracker_df.loc[mask, "Overall_status"] = "In progress"

        # Update stage flags
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (stg == nxt)
    else:
        # Final stage approved
        tracker_df.loc[mask, "Overall_status"] = "Approved"

elif new_status == "Rejected":
    # Stay in same stage, DO NOT move forward
    tracker_df.loc[mask, "Current Stage"] = current_stage
    tracker_df.loc[mask, "Overall_status"] = "Rejected"

    # Keep stage flags unchanged (remain where rejection happened)
    for stg, m in STAGE_KEYS.items():
        tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

else:
    # Request changes → stay in same stage
    tracker_df.loc[mask, "Current Stage"] = current_stage
    tracker_df.loc[mask, "Overall_status"] = "Changes requested"

    # Keep stage flags unchanged
    for stg, m in STAGE_KEYS.items():
        tracker_df.loc[mask, m["flag"]] = (stg == current_stage)
