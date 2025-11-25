STAGES = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
    "DigitalGuild",
    "ETIDM",
]

# Pre-Digital Guild reviewers (parallel)
PRE_REVIEW_STAGES = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
]

# Map logical stage names → underlying tracker columns
# Note: DataGovIA + ArchAssurance reuse the older data_guild_* / SDA_* columns.
STAGE_KEYS = {
    "HeadDataAI": {
        "flag": "is_in_head_data_ai",
        "status": "head_data_ai_status",
        "assigned_to": "head_data_ai_assigned_to",
        "decision_at": "head_data_ai_decision_at",
    },
    "DataGovIA": {
        "flag": "is_in_data_guild",
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "ArchAssurance": {
        "flag": "is_in_SDA",
        "status": "SDA_status",
        "assigned_to": "SDA_assigned_to",
        "decision_at": "SDA_decision_at",
    },
    "Finance": {
        "flag": "is_in_finance",
        "status": "finance_status",
        "assigned_to": "finance_assigned_to",
        "decision_at": "finance_decision_at",
    },
    "Regulatory": {
        "flag": "is_in_regulatory",
        "status": "regulatory_status",
        "assigned_to": "regulatory_assigned_to",
        "decision_at": "regulatory_decision_at",
    },
    "DigitalGuild": {
        "flag": "is_in_digital_guild",
        "status": "digital_guild_status",
        "assigned_to": "digital_guild_assigned_to",
        "decision_at": "digital_guild_decision_at",
    },
    "ETIDM": {
        "flag": "is_in_etidm",
        "status": "etidm_status",
        "assigned_to": "etidm_assigned_to",
        "decision_at": "etidm_decision_at",
    },
}









def save_fb(row):
    """
    Append a feedback row to FEEDBACK_HISTORY_PATH using the schema:
    comment_id, sanction_id, stage, rating, comment, username, created_at
    """
    cols = [
        "comment_id",
        "sanction_id",
        "stage",
        "rating",
        "comment",
        "username",
        "created_at",
    ]

    if os.path.exists(FEEDBACK_HISTORY_PATH):
        df = pd.read_csv(FEEDBACK_HISTORY_PATH)
    else:
        df = pd.DataFrame(columns=cols)

    # Ensure all columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    # Auto-increment comment_id
    if df.empty:
        next_id = 1
    else:
        try:
            next_id = int(pd.to_numeric(df["comment_id"], errors="coerce").max()) + 1
        except Exception:
            next_id = 1

    row_out = {
        "comment_id": next_id,
        "sanction_id": row.get("sanction_id"),
        "stage": row.get("stage"),
        "rating": row.get("rating"),
        "comment": row.get("comment"),
        "username": row.get("username"),
        "created_at": row.get("created_at"),
    }

    df = pd.concat([df, pd.DataFrame([row_out])], ignore_index=True)
    _write_csv(df, FEEDBACK_HISTORY_PATH)







feedback = {
    "sanction_id": sid,
    "stage": current_stage,
    "rating": rating,
    "comment": comment,
    "username": assigned_to or user_internal_role or current_stage,
    "created_at": _now_iso(),
}
save_fb(feedback, "sanction_database/feedback.csv")










def _all_pre_review_approved(row: pd.Series) -> bool:
    """
    True only if all pre-DigitalGuild reviewers have Approved
    (status == 'Approved' and a non-empty decision_at).
    """
    for stg in PRE_REVIEW_STAGES:
        meta = STAGE_KEYS[stg]
        st_status = str(row.get(meta["status"], ""))
        decided = str(row.get(meta["decision_at"], "") or "").strip()
        if st_status != "Approved" or decided == "":
            return False
    return True








overall_stage = str(t_row.get("Current Stage", s_row.get("Current Stage", "HeadDataAI")))
if overall_stage not in STAGES:
    overall_stage = "HeadDataAI"



for idx, stage in enumerate(STAGES):
    flow_html += _stage_block(stage, t_row, overall_stage)



user_internal_role = st.session_state.get("user_role", "")

if user_internal_role in STAGES:
    user_stage_label = user_internal_role
else:
    # Legacy mapping back to new stages if user_role is an old label
    legacy_map = {
        "SDA": "ArchAssurance",
        "DataGuild": "DataGovIA",
        "DigitalGuild": "DigitalGuild",
        "ETIDM": "ETIDM",
    }
    user_stage_label = legacy_map.get(user_internal_role, "HeadDataAI")

current_stage = user_stage_label  # the stage this person is acting for

meta = STAGE_KEYS.get(current_stage, {})
existing_status = str(t_row.get(meta.get("status", ""), "Pending"))

# ...

role_can_act = current_stage in STAGES










meta = STAGE_KEYS[current_stage]
dec_lower = decision.lower()

if "approve" in dec_lower:
    new_status = "Approved"
elif "reject" in dec_lower:
    new_status = "Rejected"
else:
    new_status = "Changes requested"

status_field = meta["status"]
assigned_field = meta["assigned_to"]
flag_field = meta["flag"]
decision_field = meta["decision_at"]

# Update this stage's own fields
tracker_df.loc[mask, status_field] = new_status
tracker_df.loc[mask, assigned_field] = assigned_to
tracker_df.loc[mask, "last_comment"] = comment

if new_status == "Approved":
    tracker_df.loc[mask, decision_field] = when
    tracker_df.loc[mask, flag_field] = 1
else:
    tracker_df.loc[mask, decision_field] = when

# ---- Stage-specific workflow behaviour ----

# 1) Pre-DigitalGuild reviewers (parallel)
if current_stage in PRE_REVIEW_STAGES:
    if new_status == "Rejected":
        tracker_df.loc[mask, "Overall_status"] = f"Rejected at {current_stage}"
        tracker_df.loc[mask, "Current Stage"] = overall_stage
    elif new_status == "Changes requested":
        tracker_df.loc[mask, "Overall_status"] = f"Changes requested by {current_stage}"
        tracker_df.loc[mask, "Current Stage"] = overall_stage
    else:  # Approved by this reviewer
        # After this approval, check if *all* pre-review stages are now Approved
        row_after = tracker_df.loc[mask].iloc[0]
        if _all_pre_review_approved(row_after):
            tracker_df.loc[mask, "Overall_status"] = "Pre-review approved"
            tracker_df.loc[mask, "Current Stage"] = "DigitalGuild"
        else:
            tracker_df.loc[mask, "Overall_status"] = "In pre-review"
            tracker_df.loc[mask, "Current Stage"] = overall_stage

# 2) Digital Guild
elif current_stage == "DigitalGuild":
    if new_status == "Approved":
        tracker_df.loc[mask, "Overall_status"] = "Approved by Digital Guild"
    elif new_status == "Rejected":
        tracker_df.loc[mask, "Overall_status"] = "Rejected at Digital Guild"
    else:
        tracker_df.loc[mask, "Overall_status"] = "Changes requested by Digital Guild"
    tracker_df.loc[mask, "Current Stage"] = "DigitalGuild"

# 3) ETIDM – final approver
elif current_stage == "ETIDM":
    if new_status == "Approved":
        tracker_df.loc[mask, "Overall_status"] = "Fully approved"
    elif new_status == "Rejected":
        tracker_df.loc[mask, "Overall_status"] = "Rejected at ETIDM"
    else:
        tracker_df.loc[mask, "Overall_status"] = "Changes requested by ETIDM"
    tracker_df.loc[mask, "Current Stage"] = "ETIDM"











# ============================================================
# FEEDBACK HISTORY + DOWNLOAD
# ============================================================

st.divider()
st.markdown("<h3>Feedback history</h3>", unsafe_allow_html=True)

if os.path.exists(FEEDBACK_HISTORY_PATH):
    fh = pd.read_csv(FEEDBACK_HISTORY_PATH)
    if "sanction_id" in fh.columns:
        fh_this = fh[fh["sanction_id"].astype(str) == sid]
    else:
        fh_this = pd.DataFrame()

    if fh_this.empty:
        st.info("No feedback recorded for this sanction yet.")
    else:
        display_cols = [
            "comment_id",
            "sanction_id",
            "stage",
            "rating",
            "comment",
            "username",
            "created_at",
        ]
        display_cols = [c for c in display_cols if c in fh_this.columns]
        st.dataframe(fh_this[display_cols], hide_index=True, use_container_width=True)

        csv_bytes = fh_this[display_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download feedback CSV for this sanction",
            data=csv_bytes,
            file_name=f"feedback_{sid}.csv",
            mime="text/csv",
        )
else:
    st.info("No feedback file created yet.")
 
