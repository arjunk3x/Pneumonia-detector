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
 









def _current_internal_role() -> str:
    """
    Returns the internal stage code this user represents.
    E.g. 'HeadDataAI', 'DataGovIA', 'ArchAssurance', 'Finance',
         'Regulatory', 'DigitalGuild', 'ETIDM'.
    """
    raw = (st.session_state.get("user_role") or "HeadDataAI").replace(" ", "")

    # If already one of the new internal names, just use it
    if raw in STAGES:
        return raw

    # Legacy mapping from old roles
    legacy_map = {
        "SDA": "ArchAssurance",
        "DataGuild": "DataGovIA",
        "DigitalGuild": "DigitalGuild",
        "ETIDM": "ETIDM",
    }
    if raw in legacy_map:
        return legacy_map[raw]

    # Fallback from email (optional / best-effort)
    e = st.session_state.get("user_email", "").lower()
    if "headdata" in e:
        return "HeadDataAI"
    if "datagov" in e or "dgia" in e:
        return "DataGovIA"
    if "arch" in e or "sda" in e:
        return "ArchAssurance"
    if "finance" in e:
        return "Finance"
    if "reg" in e:
        return "Regulatory"
    if "digitalguild" in e or "digital" in e:
        return "DigitalGuild"
    if "etidm" in e:
        return "ETIDM"

    return "HeadDataAI"






def _current_stage_label_for_role() -> str:
    # map internal stage code -> UI label
    return {
        "HeadDataAI": "Head of Data & AI",
        "DataGovIA": "Data Governance & IA",
        "ArchAssurance": "Architectural Assurance",
        "Finance": "Finance",
        "Regulatory": "Regulatory",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }[_current_internal_role()]




# =========================
# STAGE ACTIONS – Sticky Action Bar (ROLE-LOCKED)
# =========================

# 1. Work out which stage this *user* is acting for
#    - internal role:  "SDA" | "DataGuild" | "DigitalGuild" | "ETIDM"
#    - stage label:    "SDA" | "Data Guild" | "Digital Guild" | "ETIDM"
user_internal_role = _current_internal_role()
current_stage = _current_stage_label_for_role()   # this is the key used in STAGE_KEYS

meta = STAGE_KEYS.get(current_stage, {})
existing_status = str(t_row.get(meta.get("status", ""), "Pending"))

# 2. Header (unchanged styling)
st.markdown(
    f"""
    <div style='margin-bottom:10px;'>
      <span style='font-size:1.8rem; font-weight:700;'>
        Stage Actions | {current_stage}
      </span><br>
      <span style='color:#6c757d; font-size:1.1rem;'>
        Current status:
        <span class='badge {_pill_class(existing_status)}'>
          {existing_status}
        </span>
      </span>
    </div>
    """,
    unsafe_allow_html=True,
)

# If this stage is not configured in STAGE_KEYS, there’s nothing to do
if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:
    # =========================
    # Permissions
    # =========================
    # At the moment: if we can map the user to a stage, they can act on that stage.
    user_stage_label = current_stage          # same as what we show in the UI
    role_can_act = current_stage in STAGE_KEYS

    if not role_can_act:
        st.warning(
            f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**."
        )

    # =========================
    # DECISION FORM
    # =========================
    with st.form(f"form_{current_stage}"):

        # 1. Decision
        decision = st.radio(
            "**Choose Your Action [Approve/Reject/Request changes]:**",
            ["Approve ✅", "Reject ❌", "Request changes 🟡"],
            index=0,
            disabled=not role_can_act,
        )

        # 2. Rating
        rating_stars = st.selectbox(
            "**Rating (optional):**",
            ["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"],
            index=0,
            disabled=not role_can_act,
        )
        rating = rating_stars.count("⭐")

        # 3. Assigned To + Decision time
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
                help="Auto-filled; editable.",
                disabled=not role_can_act,
            )

        # 4. Comments
        comment = st.text_area(
            "**Comments / Rationale**",
            placeholder="Add remarks for documentation",
            disabled=not role_can_act,
        )

        # 5. Buttons
        col_reset, col_submit = st.columns([0.8, 0.2])
        with col_reset:
            cancel = st.form_submit_button(
                "Reset form", disabled=not role_can_act
            )
        with col_submit:
            submitted = st.form_submit_button(
                "Submit decision", disabled=not role_can_act
            )

# =========================
# BACKEND SUBMISSION LOGIC
# =========================
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

    # Refresh row + mask
    tracker_df = _ensure_tracker_columns(tracker_df)
    mask = tracker_df["Sanction_ID"].astype(str) == sid
    _row = tracker_df.loc[mask].iloc[0]

    meta = STAGE_KEYS[current_stage]
    prev_status = str(_row.get(meta.get("status", ""), ""))

    # 2. Map decision -> new_status
    dec_lower = decision.lower()
    if "approve" in dec_lower:
        new_status = "Approved"
    elif "reject" in dec_lower:
        new_status = "Rejected"
    else:
        new_status = "Changes requested"

    # Prevent re-submission when already rejected
    if new_status == "Rejected" and prev_status == "Rejected":
        st.warning(
            "This sanction has already been rejected. No further actions can be taken."
        )
        st.stop()

    # 3. Basic field updates for this stage
    tracker_df.loc[mask, meta["status"]] = new_status
    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
    tracker_df.loc[mask, "last_comment"] = comment

    # 4. Stage progression logic (same as before: uses current_stage)
    flag_field = meta["flag"]
    decision_field = meta["decision_at"]
    nxt = _next_stage(current_stage) if new_status == "Approved" else None

    # ---------- APPROVED ----------
    if new_status == "Approved":
        # mark this stage as completed
        tracker_df.loc[mask, decision_field] = when
        tracker_df.loc[mask, flag_field] = False

        if nxt:
            # move overall workflow to the next stage
            tracker_df.loc[mask, "Current Stage"] = nxt
            tracker_df.loc[mask, "Overall_status"] = "In progress"
            # only the next stage's flag is True
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == nxt)
        else:
            # last stage in the flow
            tracker_df.loc[mask, "Overall_status"] = "Approved"

    # ---------- REJECTED ----------
    elif new_status == "Rejected":
        tracker_df.loc[mask, decision_field] = ""
        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"] = current_stage
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False

    # ---------- REQUEST CHANGES ----------
    else:
        tracker_df.loc[mask, decision_field] = ""
        tracker_df.loc[mask, "Overall_status"] = "Changes requested"
        tracker_df.loc[mask, "Current Stage"] = current_stage
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

    # 5. Persist tracker + mirror to sanctions view
    _write_csv(tracker_df, APPROVER_TRACKER_PATH)

    if "Sanction ID" in sanctions_df.columns:
        ms = sanctions_df["Sanction ID"].astype(str) == sid
        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
        _write_csv(sanctions_df, SANCTIONS_PATH)

    # 6. Notifications
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

    # 7. Save feedback log
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

    # 8. Finish
    st.success(f"Saved decision for {sid} at {current_stage}: {new_status}")
    st.toast("Updated ✅")
    st.stop()


