STAGES = [
    "Head of Data & AI",
    "DG & IA",
    "Architectural Assurance",
    "Finance",
    "Regulatory",
    "Digital Guild",
    "ETIDM",
]

STAGE_KEYS = {
    "Head of Data & AI": {
        "flag": "is_in_head_data_ai",
        "status": "head_data_ai_status",
        "assigned_to": "head_data_ai_assigned_to",
        "decision_at": "head_data_ai_decision_at",
    },
    "DG & IA": {
        # re-using existing data_guild_* columns
        "flag": "is_in_data_guild",
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "Architectural Assurance": {
        # re-using existing SDA_* columns
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
    "Digital Guild": {
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














def _current_stage_label_for_role() -> str:
    internal = _current_internal_role()  # e.g. "HeadDataAI", "DataGovIA", etc.
    return {
        "HeadDataAI": "Head of Data & AI",
        "DataGovIA": "DG & IA",
        "ArchAssurance": "Architectural Assurance",
        "Finance": "Finance",
        "Regulatory": "Regulatory",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(internal, "Head of Data & AI")
