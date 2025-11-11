# ========================
# Flow / Stage helpers
# ========================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def role_display_name(role: str) -> str:
    return {
        "SDA": "SDA",
        "DataGuild": "Data Guild",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(role, role)


# ========================
# Sidebar role switcher (locked to user's role)
# ========================
# use whatever role was set in session (e.g., during login/bootstrap)
current_role = st.session_state.get("user_role", "SDA")

# safety check: if unknown, fall back to SDA
if current_role not in ROLE_FLOW:
    current_role = "SDA"

# show only the logged-in role, with switching disabled
st.sidebar.selectbox(
    "Team",
    [current_role],                  # single allowed option
    index=0,
    format_func=role_display_name,
    disabled=True                    # prevent manual change
)

# persist for rest of page logic
st.session_state["user_role"] = current_role
