# ==========================================================
# SanctionApproverDashboard.py — complete version
# ==========================================================
import streamlit as st
import pandas as pd
import numpy as np
# (keep any other imports: plotly, db connectors, etc.)

# ==========================================================
# ROLE HANDLING SECTION
# ==========================================================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def role_display_name(role: str) -> str:
    """Readable display names for sidebar"""
    return {
        "SDA": "SDA",
        "DataGuild": "Data Guild",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(role, role)


# Optional email→role mapping (use only if your login sets user_email)
EMAIL_TO_ROLE = {
    "sda@company.com": "SDA",
    "dg@company.com": "DataGuild",
    "dig@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}


def _normalize_role(v):
    """Handle spaces, case, or hyphens in role names"""
    if not v:
        return None
    x = str(v).strip().lower().replace("_", "").replace("-", "").replace(" ", "")
    mapping = {
        "sda": "SDA",
        "dataguild": "DataGuild",
        "digitalguild": "DigitalGuild",
        "etidm": "ETIDM",
    }
    return mapping.get(x)


# ----------------------------------------------------------
# BOOTSTRAP: determine current role once
# ----------------------------------------------------------
current_role = st.session_state.get("user_role")

# (1) Try direct role set by login
if current_role not in ROLE_FLOW:
    # (2) Try from login-provided team
    team = st.session_state.get("team")
    norm_team = _normalize_role(team)
    if norm_team in ROLE_FLOW:
        current_role = norm_team

# (3) Try from groups
if current_role not in ROLE_FLOW:
    groups = st.session_state.get("groups")
    if groups:
        if isinstance(groups, (list, tuple, set)):
            for g in groups:
                ng = _normalize_role(g)
                if ng in ROLE_FLOW:
                    current_role = ng
                    break
        else:
            ng = _normalize_role(groups)
            if ng in ROLE_FLOW:
                current_role = ng

# (4) Try email mapping
if current_role not in ROLE_FLOW:
    email = st.session_state.get("user_email")
    mapped = EMAIL_TO_ROLE.get(email) if email else None
    if mapped in ROLE_FLOW:
        current_role = mapped

# (5) Fallback safety (shouldn’t normally trigger)
if current_role not in ROLE_FLOW:
    current_role = "SDA"

# Persist for rest of app
st.session_state["user_role"] = current_role

# ----------------------------------------------------------
# SIDEBAR: locked team display
# ----------------------------------------------------------
st.sidebar.selectbox(
    "Team",
    [current_role],
    index=0,
    format_func=role_display_name,
    disabled=True,
)

# Optional info lines
st.sidebar.caption(f"Signed in as: {st.session_state.get('user_email', 'unknown')}")
st.sidebar.caption(f"Role: {role_display_name(current_role)}")
