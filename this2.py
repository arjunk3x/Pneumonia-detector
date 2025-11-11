# ==============================
# Sidebar — role-aware Teams (hide others)
# ==============================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
ROLE_LABEL = {
    "SDA": "SDA",
    "DataGuild": "Data Guild",
    "DigitalGuild": "Digital Guild",
    "ETIDM": "ETIDM",
}

# Make sure session has a role
if "user_role" not in st.session_state:
    st.session_state["user_role"] = "SDA"
if "user_email" not in st.session_state:
    st.session_state["user_email"] = "sda@company.com"

with st.sidebar:
    st.markdown("### 👤 Active User")
    st.markdown(f"**{st.session_state['user_email']}**")

    st.markdown("### 🧩 Teams")

    # Admin can switch; everyone else sees only their team
    is_admin = st.session_state["user_email"] == "admin@company.com"

    if is_admin:
        # Admin can change role from the dropdown
        picked_role = st.selectbox(
            "Team",
            ROLE_FLOW,
            index=ROLE_FLOW.index(st.session_state["user_role"]),
            format_func=lambda r: ROLE_LABEL[r],
            key="sb_team_select",
        )
        if picked_role != st.session_state["user_role"]:
            st.session_state["user_role"] = picked_role
            st.cache_data.clear()
            st.rerun()
    else:
        # Non-admin: show locked team badge (no selectbox)
        locked_label = ROLE_LABEL[st.session_state["user_role"]]
        st.markdown(
            f"<div style='padding:8px 12px;background:#f5f6ff;"
            f"border:1px solid #e5e7eb;border-radius:10px;"
            f"font-weight:700;'>🔒 Team: {locked_label}</div>",
            unsafe_allow_html=True,
        )
        st.caption("You can only view and manage data for your assigned team.")

# Use this everywhere below
current_role = st.session_state["user_role"]
