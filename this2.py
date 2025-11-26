# Internal stage codes (used in logic)
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

# Map *internal* stage codes -> underlying tracker columns
# (DataGovIA + ArchAssurance reuse the old data_guild_* / SDA_* columns)
STAGE_KEYS = {
    "HeadDataAI": {
        "flag": "is_in_head_data_ai",
        "status": "head_data_ai_status",
        "assigned_to": "head_data_ai_assigned_to",
        "decision_at": "head_data_ai_decision_at",
    },
    "DataGovIA": {  # re-using existing data_guild_* columns
        "flag": "is_in_data_guild",
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "ArchAssurance": {  # re-using existing SDA_* columns
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





def _current_internal_role() -> str:
    """
    Returns the internal stage code this user represents.
    e.g. 'HeadDataAI', 'DataGovIA', 'ArchAssurance', 'Finance',
         'Regulatory', 'DigitalGuild', 'ETIDM'.
    """
    raw = (st.session_state.get("user_role") or "HeadDataAI").strip()

    # 1) Already one of the internal codes?
    if raw in STAGES:
        return raw

    # 2) If it's a UI label, map to internal
    label_map = {
        "Head of Data & AI": "HeadDataAI",
        "DG & IA": "DataGovIA",
        "Architectural Assurance": "ArchAssurance",
        "Finance": "Finance",
        "Regulatory": "Regulatory",
        "Digital Guild": "DigitalGuild",
        "ETIDM": "ETIDM",
    }
    if raw in label_map:
        return label_map[raw]

    # 3) Legacy mapping from old roles (SDA / DataGuild / etc.)
    legacy_map = {
        "SDA": "ArchAssurance",
        "DataGuild": "DataGovIA",
        "DigitalGuild": "DigitalGuild",
        "ETIDM": "ETIDM",
    }
    if raw in legacy_map:
        return legacy_map[raw]

    # 4) Fallback from email (best effort)
    e = (st.session_state.get("user_email", "") or "").lower()
    if "headdata" in e or "head-of-data" in e:
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

    # 5) Hard default
    return "HeadDataAI"





def _current_stage_label_for_role() -> str:
    internal = _current_internal_role()
    return {
        "HeadDataAI": "Head of Data & AI",
        "DataGovIA": "DG & IA",
        "ArchAssurance": "Architectural Assurance",
        "Finance": "Finance",
        "Regulatory": "Regulatory",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(internal, "Head of Data & AI")





"Current Stage": _current_stage_label_for_role(),
current_stage = _current_internal_role()
current_stage_label = _current_stage_label_for_role()

meta = STAGE_KEYS.get(current_stage, {})
existing_status = str(t_row.get(meta.get("status", ""), "Pending"))

st.markdown(
    f"""
    <div style='margin-bottom:10px;'>
      <span style='font-size:1.8rem;font-weight:700;'>
        Stage Actions | {current_stage_label}
      </span><br>
      <span style='color:#6c757d;font-size:1.1rem;'>
        Current status:
        <span class='badge {_pill_class(existing_status)}'>
          {existing_status}
        </span>
      </span>
    </div>
    """,
    unsafe_allow_html=True,
)




if not role_can_act:
    st.warning(
        f"Your role (**{user_stage_label}**) cannot act on **{current_stage_label}**."
    )
