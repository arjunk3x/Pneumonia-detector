# ==========================================================
# SanctionApproverDashboard.py  — role-aware (login-driven)
# ==========================================================
import streamlit as st
import pandas as pd
import numpy as np
# (keep your other imports: plotly, db libs, etc.)

# ==========================================================
# ROLE CONSTANTS
# ==========================================================
# Internal role codes (no spaces) used in logic:
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

# Pretty names for UI (sidebar):
DISPLAY_NAME = {
    "SDA": "SDA",
    "DataGuild": "Data Guild",
    "DigitalGuild": "Digital Guild",
    "ETIDM": "ETIDM",
}

# Optional: email → role mapping
# If your login sets st.session_state["user_email"], you can map it here.
# Leave empty if your login already sets user_role/team/groups.
EMAIL_TO_ROLE = {
    # "sda@company.com": "SDA",
    # "dg@company.com": "DataGuild",
    # "dig@company.com": "DigitalGuild",
    # "etidm@company.com": "ETIDM",
}


# ==========================================================
# HELPERS
# ==========================================================
def _normalize_role(value: str | None) -> str | None:
    """Normalize various inputs to internal role codes, case-insensitive."""
    if not value:
        return None
    v = str(value).strip()
    # Accept common variants and spaces
    aliases = {
        "sda": "SDA",
        "data guild": "DataGuild",
        "dataguild": "DataGuild",
        "digital guild": "DigitalGuild",
        "digitalguild": "DigitalGuild",
        "etidm": "ETIDM",
    }
    key = v.lower().replace("_", " ").replace("-", " ")
    return aliases.get(key, v if v in ROLE_FLOW else None)


def _first_nonempty(*vals):
    for v in vals:
        if v:
            return v
    return None


def _coerce_groups_to_list(groups_val):
    """Support groups as list or comma-separated string."""
    if groups_val is None:
        return []
    if isinstance(groups_val, (list, tuple, set)):
        return [str(x) for x in groups_val]
    return [g.strip() for g in str(groups_val).split(",") if g.strip()]


def _bootstrap_user_role_from_login():
    """
    Determine user_role strictly from what login provided.
    Priority:
      1) st.session_state["user_role"]
      2) st.session_state["team"]
      3) st.session_state["groups"] (any group matching a known role)
      4) EMAIL_TO_ROLE via st.session_state["user_email"]
    If none found → show error and stop (do NOT silently default).
    """
    # 1) Explicit role from login
    explicit_role = _normalize_role(st.session_state.get("user_role"))
    if explicit_role in ROLE_FLOW:
        st.session_state["user_role"] = explicit_role
        return

    # 2) Team from login (often a display string like "Data Guild")
    team = _normalize_role(st.session_state.get("team"))
    if team in ROLE_FLOW:
        st.session_state["user_role"] = team
        return

    # 3) Group membership from login (look for any known role)
    groups = _coerce_groups_to_list(st.session_state.get("groups"))
    for g in groups:
        g_norm = _normalize_role(g)
        if g_norm in ROLE_FLOW:
            st.session_state["user_role"] = g_norm
            return

    # 4) Email → role mapping (optional)
    email = st.session_state.get("user_email")
    if email:
        mapped = _normalize_role(EMAIL_TO_ROLE.get(email))
        if mapped in ROLE_FLOW:
            st.session_state["user_role"] = mapped
            return

    # Nothing found — fail loudly so you can fix login wiring
    st.error(
        "No team/role detected from login. "
        "Your auth must set one of: "
        "`st.session_state['user_role']`, `['team']`, `['groups']`, or `['user_email']` (with mapping)."
    )
    st.stop()


# ==========================================================
# BOOTSTRAP ROLE (DO NOT HARD-CODE ANY DEFAULT)
# ==========================================================
_bootstrap_user_role_from_login()
current_role = st.session_state["user_role"]  # guaranteed set by now

# ==========================================================
# SIDEBAR: Locked Team (display only)
# ==========================================================
st.sidebar.selectbox(
    "Team",
    [current_role],  # show only the logged-in team
    index=0,
    format_func=lambda r: DISPLAY_NAME.get(r, r),
    disabled=True,   # can't switch teams manually
)

# Optional: show who is signed in (remove in prod if you like)
signed_in = _first_nonempty(st.session_state.get("display_name"),
                            st.session_state.get("user_name"),
                            st.session_state.get("user_email"),
                            "Unknown user")
st.sidebar.caption(f"Signed in: {signed_in}")
st.sidebar.caption(f"Role: {DISPLAY_NAME.get(current_role, current_role)}")
