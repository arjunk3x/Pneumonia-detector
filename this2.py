# ==========================================================
# SanctionApproverDashboard.py  (role-aware version)
# ==========================================================
import streamlit as st
import pandas as pd
import numpy as np
# (Keep your other imports exactly as they were, e.g., plotly, snowflake, etc.)

# ==========================================================
# ROLE MANAGEMENT & SIDEBAR LOCK
# ==========================================================
# Internal role codes (no spaces)
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

# Display names with spaces for sidebar readability
DISPLAY_NAME = {
    "SDA": "SDA",
    "DataGuild": "Data Guild",
    "DigitalGuild": "Digital Guild",
    "ETIDM": "ETIDM",
}

# Map user email → internal role
EMAIL_TO_ROLE = {
    "sda@company.com": "SDA",
    "dg@company.com": "DataGuild",
    "dig@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}


def _get_query_params_role() -> str | None:
    """Optionally allow ?role=DataGuild in URL for testing."""
    role = None
    try:
        qp = st.query_params  # Streamlit ≥1.31
        val = qp.get("role", None)
        if isinstance(val, list):
            role = val[0]
        else:
            role = val
    except Exception:
        try:
            qp = st.experimental_get_query_params()  # Streamlit <1.31
            role = qp.get("role", [None])[0]
        except Exception:
            role = None

    # Normalize names like "Data Guild" → "DataGuild"
    if role in ("Data Guild", "Digital Guild"):
        role = role.replace(" ", "")

    return role if role in ROLE_FLOW else None


def _bootstrap_user_role():
    """
    Ensures st.session_state['user_role'] is set exactly once.
    Priority:
      1) URL param ?role=DataGuild (for testing)
      2) st.session_state["user_email"] via EMAIL_TO_ROLE mapping
      3) default fallback: "SDA"
    """
    if "user_role" in st.session_state and st.session_state["user_role"] in ROLE_FLOW:
        return

    # 1) Query param override (optional)
    qp_role = _get_query_params_role()
    if qp_role:
        st.session_state["user_role"] = qp_role
        return

    # 2) Email mapping (normal login flow)
    email = st.session_state.get("user_email")
    if email:
        mapped = EMAIL_TO_ROLE.get(email)
        if mapped in ROLE_FLOW:
            st.session_state["user_role"] = mapped
            return

    # 3) Fallback to SDA (safe default)
    st.session_state["user_role"] = "SDA"


# Initialize user role once
_bootstrap_user_role()

# Normalize current role value
current_role = st.session_state["user_role"]
if current_role not in ROLE_FLOW:
    current_role = "SDA"
    st.session_state["user_role"] = current_role

# Locked sidebar select (displays nice names, prevents switching)
st.sidebar.selectbox(
    "Team",
    [current_role],
    index=0,
    format_func=lambda r: DISPLAY_NAME.get(r, r),
    disabled=True,
)

# Optional: show current email & role at bottom of sidebar
st.sidebar.caption(f"Signed in as: {st.session_state.get('user_email', 'unknown')}")
st.sidebar.caption(f"Role: {DISPLAY_NAME.get(current_role, current_role)}")
