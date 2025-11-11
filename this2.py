# ========================
# Flow / Stage helpers
# ========================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def role_display_name(role: str) -> str:
    """
    Converts internal role codes to readable names for the UI.
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
# Map user email to team role (adjust to your real users)
EMAIL_TO_ROLE = {
    "sda@company.com": "SDA",
    "dg@company.com": "DataGuild",
    "dig@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}

# Derive user role if missing
if "user_role" not in st.session_state:
    email = st.session_state.get("user_email", "sda@company.com")
    st.session_state["user_role"] = EMAIL_TO_ROLE.get(email, "SDA")

# Get the current user's role
current_role = st.session_state.get("user_role", "SDA")

# Safety: if role invalid, default to SDA
if current_role not in ROLE_FLOW:
    current_role = "SDA"

# Show locked role in sidebar (no switching possible)
st.sidebar.selectbox(
    "Team",
    [current_role],
    index=0,
    format_func=role_display_name,
    disabled=True  # prevents switching roles
)

# Persist role for rest of dashboard logic
st.session_state["user_role"] = current_role

# Optional debug info (you can remove these later)
st.sidebar.caption(f"Signed in as: {st.session_state.get('user_email')}")
st.sidebar.caption(f"Role: {role_display_name(current_role)}")
