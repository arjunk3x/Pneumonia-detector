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






