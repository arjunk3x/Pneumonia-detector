# ========================
# Flow / Stage helpers
# ========================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def role_display_name(role: str) -> str:
    """
    Converts internal role codes (no spaces) to readable names for the UI.
    """
    return {
        "SDA": "SDA",
        "DataGuild": "Data Guild",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(role, role)


# ========================
# Sidebar role switcher (locked to user's role)
# ========================
# Map user email → internal role (adjust for your real logins)
EMAIL_TO_ROLE = {
    "sda@company.com": "SDA",
    "dg@company.com": "DataGuild",
    "dig@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}

# --- Derive user role once per session ---
if "user_role" not in st.session_state:
    email = st.session_state.get("user_email", "sda@company.com")
    st.session_state["user_role"] = EMAIL_TO_ROLE.get(email, "SDA")

# Get the current role from session
current_role = st.session_state.get("user_role", "SDA")

# Safety fallback if anything goes wrong
if current_role not in ROLE_FLOW:
    current_role = "SDA"

# --- Sidebar UI ---
st.sidebar.selectbox(
    "Team",
    [current_role],                     # only this team visible
    index=0,
    format_func=role_display_name,      # show with spaces for readability
    disabled=True                       # lock it
)

# Persist for rest of page logic
st.session_state["user_role"] = current_role

# (Optional: show info at bottom of sidebar for debugging)
st.sidebar.caption(f"Signed in as: {st.session_state.get('user_email', '')}")
st.sidebar.caption(f"Role: {role_display_name(current_role)}")
