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



ROLE_FLOW: List[str] = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
    "DigitalGuild",
    "ETIDM",
]


def role_display_name(role: str) -> str:
    return {
        "HeadDataAI": "Head of Data & AI",
        "DataGovIA": "Data Governance & Information Architecture",
        "ArchAssurance": "Architectural Assurance",
        "Finance": "Finance",
        "Regulatory": "Regulatory",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(role, role)


EMAIL_TO_ROLE: Dict[str, str] = {
    "headdataai@company.com": "HeadDataAI",
    "datagovia@company.com": "DataGovIA",
    "arch@company.com": "ArchAssurance",
    "finance@company.com": "Finance",
    "reg@company.com": "Regulatory",
    "dig@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}


mapping = {
    "headdataai": "HeadDataAI",
    "headofdataai": "HeadDataAI",
    "dataandai": "HeadDataAI",
    "datagovia": "DataGovIA",
    "datagovernanceandinformationarchitecture": "DataGovIA",
    "datagovernance": "DataGovIA",
    "archassurance": "ArchAssurance",
    "architecturalassurance": "ArchAssurance",
    "finance": "Finance",
    "regulatory": "Regulatory",
    "digitalguild": "DigitalGuild",
    "digguild": "DigitalGuild",
    "etidm": "ETIDM",
}


def prev_role(role: str) -> Optional[str]:
    if role == "ETIDM":
        return "DigitalGuild"
    return None


STAGE_MAP: Dict[str, Dict[str, str]] = {
    "HeadDataAI": {
        "is_in": "is_in_head_data_ai",
        "status": "head_data_ai_status",
        "assigned_to": "head_data_ai_assigned_to",
        "decision_at": "head_data_ai_decision_at",
    },
    "DataGovIA": {
        "is_in": "is_in_data_guild",           # existing columns
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "ArchAssurance": {
        "is_in": "is_in_SDA",                  # existing columns
        "status": "SDA_status",
        "assigned_to": "SDA_assigned_to",
        "decision_at": "SDA_decision_at",
    },
    "Finance": {
        "is_in": "is_in_finance",
        "status": "finance_status",
        "assigned_to": "finance_assigned_to",
        "decision_at": "finance_decision_at",
    },
    "Regulatory": {
        "is_in": "is_in_regulatory",
        "status": "regulatory_status",
        "assigned_to": "regulatory_assigned_to",
        "decision_at": "regulatory_decision_at",
    },
    "DigitalGuild": {
        "is_in": "is_in_digital_guild",
        "status": "digital_guild_status",
        "assigned_to": "digital_guild_assigned_to",
        "decision_at": "digital_guild_decision_at",
    },
    "ETIDM": {
        "is_in": "is_in_etidm",
        "status": "etidm_status",
        "assigned_to": "etidm_assigned_to",
        "decision_at": "etidm_decision_at",
    },
}


PRE_REVIEW_ROLES = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
]


def visibility_filter_for(role: str) -> str:
    is_in_col, _, _, _ = stage_cols(role)
    return f"{flag_true_sql(is_in_col)} = TRUE"


def set_stage_flags_inplace(df: pd.DataFrame, ids: List[str], stage: str) -> None:
    stage_flag_col, stage_status_col, stage_assigned_col, _ = stage_cols(stage)

    mask = df["Sanction_ID"].astype(str).isin([str(x) for x in ids])

    # Do NOT clear other flags: parallel review allowed.
    df.loc[mask, stage_flag_col] = 1
    df.loc[mask, stage_status_col] = df.loc[mask, stage_status_col].replace(
        {None: "Pending", "": "Pending"}
    )
    df.loc[mask, stage_assigned_col] = df.loc[mask, stage_assigned_col].fillna("")

    # items are no longer raw submissions once any pre-reviewer has picked them up
    if stage in PRE_REVIEW_ROLES and "is_submitter" in df.columns:
        df.loc[mask, "is_submitter"] = 0


initialise_session_state("user_email", "headdataai@company.com")
initialise_session_state("user_role", "HeadDataAI")


"Stage": role_display_name(current_role),






if current_role in PRE_REVIEW_ROLES:
    # Pre-review roles pull directly from submitted items (parallel intake)
    backlog_df = ... WHERE is_submitter = 1 AND this_stage_flag = FALSE

elif current_role == "DigitalGuild":
    # Only items where ALL five pre-reviewers have approved
    # (build AND chain of conditions for HeadDataAI, DataGovIA, ArchAssurance, Finance, Regulatory)
    backlog_df = ... WHERE all_pre_ok AND digitalguild_flag = FALSE ...

elif current_role == "ETIDM":
    # Still sequential on Digital Guild
    backlog_df = ... WHERE digitalguild_approved AND etidm_flag = FALSE ...





with st.expander(f"Intake ({role_display_name(current_role)})", expanded=False):

    if con is None:
        st.info("No database connection configured.")
        backlog_df = pd.DataFrame()
    else:
        # ---------------------------------------------------------
        # 1) Parallel pre-reviewers: HeadDataAI, DataGovIA,
        #    ArchAssurance, Finance, Regulatory
        #    → all pull from submitted items independently
        # ---------------------------------------------------------
        if current_role in PRE_REVIEW_ROLES:
            cur_is_in, cur_status, _, _ = stage_cols(current_role)

            backlog_df = con.execute(f"""
                SELECT *
                FROM approval
                WHERE TRY_CAST(is_submitter AS BIGINT) = 1
                  AND {flag_true_sql(cur_is_in)} = FALSE
            """).df()

        # ---------------------------------------------------------
        # 2) Digital Guild: only see items where ALL five
        #    pre-reviewers have Approved
        # ---------------------------------------------------------
        elif current_role == "DigitalGuild":
            conditions = []
            for rname in PRE_REVIEW_ROLES:
                r_is_in, r_status, _, r_decision_at = stage_cols(rname)
                conditions.append(
                    f"""
                    {flag_true_sql(r_is_in)} = TRUE
                    AND CAST({r_status} AS VARCHAR) = 'Approved'
                    AND TRY_CAST({r_decision_at} AS TIMESTAMP) IS NOT NULL
                    """
                )
            all_pre_ok = " AND ".join(conditions)

            cur_is_in, cur_status, _, _ = stage_cols("DigitalGuild")

            backlog_df = con.execute(f"""
                SELECT *
                FROM approval
                WHERE {all_pre_ok}
                  AND {flag_true_sql(cur_is_in)} = FALSE
                  AND COALESCE(CAST({cur_status} AS VARCHAR), '') IN ('','Pending')
            """).df()

        # ---------------------------------------------------------
        # 3) ETIDM: sequential after Digital Guild
        # ---------------------------------------------------------
        elif current_role == "ETIDM":
            dg_is_in, dg_status, _, dg_decision_at = stage_cols("DigitalGuild")
            cur_is_in, cur_status, _, _ = stage_cols("ETIDM")

            backlog_df = con.execute(f"""
                SELECT *
                FROM approval
                WHERE {flag_true_sql(dg_is_in)} = TRUE
                  AND CAST({dg_status} AS VARCHAR) = 'Approved'
                  AND TRY_CAST({dg_decision_at} AS TIMESTAMP) IS NOT NULL
                  AND {flag_true_sql(cur_is_in)} = FALSE
                  AND COALESCE(CAST({cur_status} AS VARCHAR), '') IN ('','Pending')
            """).df()

        # Anything else → empty
        else:
            backlog_df = con.execute("SELECT * FROM approval WHERE 1=0").df()

    # --------- everything BELOW this stays the same ---------
    if backlog_df.empty:
        st.info("No items available for intake.")
    else:
        st.dataframe(
            backlog_df[["Sanction_ID", "Value", "Overall_status"]],
            use_container_width=True,
        )

        intake_ids = st.multiselect(
            "Select Sanction_IDs to intake",
            backlog_df["Sanction_ID"].astype(str).tolist(),
        )

        if st.button(f"Move selected to {current_role}"):
            if intake_ids:
                set_stage_flags_inplace(df, intake_ids, current_role)

                # Persist and refresh registration so queries see updates immediately
                df.to_csv(CSV_PATH, index=False)
                try:
                    con.unregister("approval")
                except Exception:
                    pass
                con.register("approval", df)

                st.success(f"Moved {len(intake_ids)} to {current_role}")
                st.rerun()


























# =========================
# Session defaults
# =========================
if "user_email" not in st.session_state:
    # default to Head of Data & AI user (just a placeholder)
    st.session_state["user_email"] = "headdataai@company.com"

if "user_role" not in st.session_state:
    # one of: HeadDataAI | DataGovIA | ArchAssurance | Finance | Regulatory | DigitalGuild | ETIDM
    st.session_state["user_role"] = "HeadDataAI"

# =========================
# Session / Current user & role
# =========================
current_user = st.session_state.get("user_email", "headdataai@company.com")

if "user_role" not in st.session_state:
    st.session_state["user_role"] = "HeadDataAI"

current_role = st.session_state.get("user_role")  # "HeadDataAI"|...|"ETIDM"










# Load data + ensure columns
if not CSV_PATH.exists():
    st.error(f"CSV not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Ensure expected columns exist IN THE SAME ORDER AS THE CSV HEADER
for col, default in [
    # core fields
    ("Sanction_ID", ""),
    ("Value", 0.0),
    ("Overall_status", "Submitted"),
    ("is_submitter", 1),

    # Head of Data & AI
    ("is_in_head_data_ai", 0),
    ("head_data_ai_status", "Pending"),
    ("head_data_ai_assigned_to", None),
    ("head_data_ai_decision_at", None),

    # DataGovIA (reusing old data_guild_* columns)
    ("is_in_data_guild", 0),
    ("data_guild_status", "Pending"),
    ("data_guild_assigned_to", None),
    ("data_guild_decision_at", None),

    # ArchAssurance  (reusing old SDA_* columns)
    ("is_in_SDA", 0),
    ("SDA_status", "Pending"),
    ("SDA_assigned_to", None),
    ("SDA_decision_at", None),

    # Finance
    ("is_in_finance", 0),
    ("finance_status", "Pending"),
    ("finance_assigned_to", None),
    ("finance_decision_at", None),

    # Regulatory
    ("is_in_regulatory", 0),
    ("regulatory_status", "Pending"),
    ("regulatory_assigned_to", None),
    ("regulatory_decision_at", None),

    # Digital Guild
    ("is_in_digital_guild", 0),
    ("digital_guild_status", "Pending"),
    ("digital_guild_assigned_to", None),
    ("digital_guild_decision_at", None),

    # ETIDM
    ("is_in_etidm", 0),
    ("etidm_status", "Pending"),
    ("etidm_assigned_to", None),
    ("etidm_decision_at", None),
]:
    if col not in df.columns:
        df[col] = default


