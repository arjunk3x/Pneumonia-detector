users = {
    "submit":        {"role": "submitter", "password": "a"},

    "HeadDataAI":    {"role": "approver", "password": "a"},
    "DataGovIA":     {"role": "approver", "password": "a"},
    "ArchAssurance": {"role": "approver", "password": "a"},
    "Finance":       {"role": "approver", "password": "a"},
    "Regulatory":    {"role": "approver", "password": "a"},

    "DigitalGuild":  {"role": "approver", "password": "a"},
    "ETIDM":         {"role": "approver", "password": "a"},
}

TEAM_CODES = (
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
    "DigitalGuild",
    "ETIDM",
)

st.session_state.user_role = username if username in TEAM_CODES else None



# Which reviewer teams exist
PRE_REVIEW_ROLES = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
]

ALL_ROLES = PRE_REVIEW_ROLES + ["DigitalGuild", "ETIDM"]


def role_display_name(code: str) -> str:
    mapping = {
        "HeadDataAI":    "Head of Data & AI",
        "DataGovIA":     "Data Governance & Information Architecture",
        "ArchAssurance": "Architectural Assurance",
        "Finance":       "Finance",
        "Regulatory":    "Regulatory",
        "DigitalGuild":  "Digital Guild",
        "ETIDM":         "ETIDM",
    }
    return mapping.get(code, code)


def stage_cols(role_code: str):
    """Return (is_in_flag, status_col, assigned_to_col, decision_at_col) for a given team."""
    mapping = {
        "HeadDataAI": (
            "is_in_head_data_ai",
            "head_data_ai_status",
            "head_data_ai_assigned_to",
            "head_data_ai_decision_at",
        ),
        "DataGovIA": (
            "is_in_data_gov_ia",
            "data_gov_ia_status",
            "data_gov_ia_assigned_to",
            "data_gov_ia_decision_at",
        ),
        "ArchAssurance": (
            "is_in_arch_assurance",
            "arch_assurance_status",
            "arch_assurance_assigned_to",
            "arch_assurance_decision_at",
        ),
        "Finance": (
            "is_in_finance",
            "finance_status",
            "finance_assigned_to",
            "finance_decision_at",
        ),
        "Regulatory": (
            "is_in_regulatory",
            "regulatory_status",
            "regulatory_assigned_to",
            "regulatory_decision_at",
        ),
        "DigitalGuild": (
            "is_in_digitalguild",
            "digitalguild_status",
            "digitalguild_assigned_to",
            "digitalguild_decision_at",
        ),
        "ETIDM": (
            "is_in_etidm",
            "etidm_status",
            "etidm_assigned_to",
            "etidm_decision_at",
        ),
    }
    return mapping[role_code]


def prev_role(role_code: str) -> str | None:
    """Sequential dependency only for Digital Guild / ETIDM; pre-reviewers are parallel."""
    if role_code == "DigitalGuild":
        # gate Digital Guild on ALL pre-review approvals (handled in SQL, see below)
        return None
    if role_code == "ETIDM":
        return "DigitalGuild"
    return None  # pre-review roles have no single 'previous stage'




is_in, status_col, _, _ = stage_cols(current_role)
pending_df = df[
    (df[is_in] == 1)
    & df[status_col].isin(["", "Pending", "Changes requested"])
]






with st.expander(f"Intake ({role_display_name(current_role)})", expanded=False):

    cur_is_in, cur_status, _, _ = stage_cols(current_role)

    if current_role in PRE_REVIEW_ROLES:
        # Pre-review roles pull directly from submitted items, independent of each other
        backlog_df = con.execute(f"""
            SELECT * FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND {flag_true_sql(cur_is_in)} = FALSE
        """).df()

    elif current_role == "DigitalGuild":
        # Digital Guild only sees items where *all* pre-reviewers have approved
        # and Digital Guild has not yet picked them up.
        conditions = []
        for r in PRE_REVIEW_ROLES:
            r_is_in, r_status, _, r_decision_at = stage_cols(r)
            conditions.append(
                f"""
                {flag_true_sql('{r_is_in}')} = TRUE
                AND CAST({r_status} AS VARCHAR) = 'Approved'
                AND TRY_CAST({r_decision_at} AS TIMESTAMP) IS NOT NULL
                """
            )
        all_pre_ok = " AND ".join(conditions)

        backlog_df = con.execute(f"""
            SELECT * FROM approval
            WHERE {all_pre_ok}
              AND {flag_true_sql(cur_is_in)} = FALSE
              AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
        """).df()

    elif current_role == "ETIDM":
        # ETIDM depends on Digital Guild approval (simple sequential gate)
        dg_is_in, dg_status, _, dg_decision_at = stage_cols("DigitalGuild")
        backlog_df = con.execute(f"""
            SELECT * FROM approval
            WHERE CAST({dg_status} AS VARCHAR) = 'Approved'
              AND TRY_CAST({dg_decision_at} AS TIMESTAMP) IS NOT NULL
              AND {flag_true_sql(cur_is_in)} = FALSE
              AND {flag_true_sql(dg_is_in)} = TRUE
              AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
        """).df()

    else:
        backlog_df = pd.DataFrame()  # just in case

    # (the rest of your Intake UI stays the same)





PRE_REVIEW_ROLES = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
]

STAGE_ORDER = PRE_REVIEW_ROLES + ["DigitalGuild", "ETIDM"]

STAGE_META = {
    "HeadDataAI": {
        "label": "Head of Data & AI",
        "flag": "is_in_head_data_ai",
        "status": "head_data_ai_status",
        "assigned_to": "head_data_ai_assigned_to",
        "decision_at": "head_data_ai_decision_at",
        "icon": "🧠",
    },
    "DataGovIA": {
        "label": "Data Governance & Information Architecture",
        "flag": "is_in_data_gov_ia",
        "status": "data_gov_ia_status",
        "assigned_to": "data_gov_ia_assigned_to",
        "decision_at": "data_gov_ia_decision_at",
        "icon": "📚",
    },
    "ArchAssurance": {
        "label": "Architectural Assurance",
        "flag": "is_in_arch_assurance",
        "status": "arch_assurance_status",
        "assigned_to": "arch_assurance_assigned_to",
        "decision_at": "arch_assurance_decision_at",
        "icon": "🏛️",
    },
    "Finance": {
        "label": "Finance",
        "flag": "is_in_finance",
        "status": "finance_status",
        "assigned_to": "finance_assigned_to",
        "decision_at": "finance_decision_at",
        "icon": "💷",
    },
    "Regulatory": {
        "label": "Regulatory",
        "flag": "is_in_regulatory",
        "status": "regulatory_status",
        "assigned_to": "regulatory_assigned_to",
        "decision_at": "regulatory_decision_at",
        "icon": "⚖️",
    },
    "DigitalGuild": {
        "label": "Digital Guild",
        "flag": "is_in_digitalguild",
        "status": "digitalguild_status",
        "assigned_to": "digitalguild_assigned_to",
        "decision_at": "digitalguild_decision_at",
        "icon": "💻",
    },
    "ETIDM": {
        "label": "ETIDM",
        "flag": "is_in_etidm",
        "status": "etidm_status",
        "assigned_to": "etidm_assigned_to",
        "decision_at": "etidm_decision_at",
        "icon": "🧪",
    },
}







def _ensure_tracker_columns(df: pd.DataFrame) -> pd.DataFrame:
    base = [
        "Sanction_ID", "Title", "Requester_Email", "Department",
        "Submitted_at", "Value", "Currency", "Risk_Level",
        "Overall_status", "Current Stage",
    ]
    for c in base:
        if c not in df.columns:
            df[c] = "" if c not in ["Value"] else 0

    for meta in STAGE_META.values():
        for key in ("flag", "status", "assigned_to", "decision_at"):
            col = meta[key]
            if col not in df.columns:
                # flags as 0, others as empty string
                df[col] = 0 if key == "flag" else ""

    if "Last_comment" not in df.columns:
        df["Last_comment"] = ""

    return df







def _next_stage(current: str) -> str | None:
    if current == "DigitalGuild":
        return "ETIDM"
    return None






def _stage_block(stage_code: str, tr: pd.Series, current_stage: str) -> str:
    meta = STAGE_META[stage_code]
    status = str(tr.get(meta["status"], "Pending"))
    assigned = str(tr.get(meta["assigned_to"], "")) or "-"
    decided = str(tr.get(meta["decision_at"], "")) or "-"
    cls = _pill_class(status)

    # visual state: done if approved, active if matches Current Stage
    state = ""
    if status.lower() == "approved":
        state = "done"
    if current_stage in (meta["label"], stage_code):
        state = "active"

    icon = meta.get("icon", "🧩")
    return f"""
    <div class="step {state}">
      <div class="title">{icon} {meta['label']}</div>
      <div class="meta">Status: <span class="pill {cls}">{status}</span></div>
      <div class="row"><div class="lbl">Assigned</div><div class="val">{assigned}</div></div>
      <div class="row"><div class="lbl">Decided</div><div class="val">{decided}</div></div>
    </div>
    """


st.subheader("Approval Flow")
flow_html = '<div class="flow">'
for idx, stage_code in enumerate(STAGE_ORDER):
    flow_html += _stage_block(stage_code, t_row, current_stage)
    if idx < len(STAGE_ORDER) - 1:
        flow_html += '<div class="arrow">→</div>'
flow_html += '</div>'
st.markdown(flow_html, unsafe_allow_html=True)




# Who is logged in as an approver team?
team = st.session_state.get("user_role")  # e.g. "HeadDataAI", "Finance", "DigitalGuild"

if not team or team not in STAGE_META:
    st.subheader("Stage Actions")
    st.info("Your login is not mapped to a specific approver stage.")
else:
    meta = STAGE_META[team]
    label = meta["label"]
    existing_status = str(t_row.get(meta["status"], "Pending"))

    st.subheader(f"Stage Actions — {label}")

    with st.container():
        st.markdown('<div class="sticky-actions">', unsafe_allow_html=True)
        st.write(
            f"Current status for {label}: "
            f"<span class='badge {_pill_class(existing_status)}'>{existing_status}</span>",
            unsafe_allow_html=True,
        )

        with st.form(f"form_{team}"):
            colA, colB, colC = st.columns([1.2, 1, 1])
            with colA:
                decision = st.radio(
                    "Decision",
                    ["Approve ✅", "Reject ⛔", "Request changes ✍️"],
                    index=0,
                )
            with colB:
                assigned_to = st.text_input(
                    "Assign to (email or name)",
                    value=str(t_row.get(meta["assigned_to"], "")),
                )
            with colC:
                when = st.text_input(
                    "Decision time",
                    value=_now_iso(),
                    help="Auto-filled; can be edited",
                )

            comment = st.text_area(
                "Comments / Rationale",
                placeholder="Add context for the audit trail (optional)",
            )

            c1, c2, _ = st.columns([0.4, 0.4, 0.2])
            with c1:
                submitted = st.form_submit_button(
                    "Submit decision", use_container_width=True
                )
            with c2:
                cancel = st.form_submit_button(
                    "Reset form", use_container_width=True
                )

        if submitted:
            mask = tracker_df["Sanction_ID"] == sid

            dec_lower = decision.lower()
            new_status = (
                "Approved"
                if "approve" in dec_lower
                else ("Rejected" if "reject" in dec_lower else "Changes requested")
            )

            tracker_df.loc[mask, meta["status"]] = new_status
            tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
            tracker_df.loc[mask, meta["decision_at"]] = when or _now_iso()
            tracker_df.loc[mask, "Last_comment"] = comment

            # ----- Update Overall_status logic -----
            if team in PRE_REVIEW_ROLES:
                # Pre-review: all five must approve before Digital Guild
                row = tracker_df.loc[mask].iloc[0]
                pre_statuses = [
                    str(row[STAGE_META[r]["status"]]).lower()
                    for r in PRE_REVIEW_ROLES
                ]

                if any(s == "rejected" for s in pre_statuses):
                    tracker_df.loc[mask, "Overall_status"] = "Rejected"
                elif all(s == "approved" for s in pre_statuses):
                    tracker_df.loc[mask, "Overall_status"] = "Ready for Digital Guild"
                else:
                    tracker_df.loc[mask, "Overall_status"] = "In pre-approval"

            elif team in ("DigitalGuild", "ETIDM"):
                nxt = _next_stage(team) if new_status == "Approved" else None

                if new_status == "Approved" and nxt:
                    tracker_df.loc[mask, "Current Stage"] = nxt
                    tracker_df.loc[mask, "Overall_status"] = "In progress"
                    tracker_df.loc[mask, STAGE_META[nxt]["flag"]] = 1
                elif new_status == "Rejected":
                    tracker_df.loc[mask, "Overall_status"] = "Rejected"
                else:
                    tracker_df.loc[mask, "Overall_status"] = "Changes requested"

            # persist tracker + mirror into sanctions_data as you already do
            try:
                _write_csv(tracker_df, APPROVER_TRACKER_PATH)
            except Exception as e:
                st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")
            else:
                try:
                    if "Sanction ID" in sanctions_df.columns:
                        ms = sanctions_df["Sanction ID"] == sid
                        if "Current Stage" in sanctions_df.columns:
                            sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[
                                mask, "Current Stage"
                            ].iloc[0]
                        if "Status" in sanctions_df.columns:
                            sanctions_df.loc[ms, "Status"] = tracker_df.loc[
                                mask, "Overall_status"
                            ].iloc[0]
                        _write_csv(sanctions_df, SANCTIONS_PATH)
                except Exception as e:
                    st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

            st.success(
                f"Saved decision for {sid} at {label}: **{new_status}**"
            )
            st.toast("Updated ✅")
            st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)





