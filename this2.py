import os
import streamlit as st
from common.funcs import *  # keep your helpers (load_and_clean, initialise_session_state, etc.)

# =========================================================
# USER DATA  (as in your screenshot)
# =========================================================
users = {
    "submit1": {"role": "submitter", "password": "a"},
    "SDA": {"role": "approver", "password": "a"},
    "DataGuild": {"role": "approver", "password": "a"},
    "DigitalGuild": {"role": "approver", "password": "a"},
    "ETIDM": {"role": "approver", "password": "a"},
}

TEAM_CODES = {"SDA", "DataGuild", "DigitalGuild", "ETIDM"}  # teams that use the approver dashboard

# =========================================================
# SESSION STATE INIT
# =========================================================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = None               # the username you type on login screen
    st.session_state.role = None                   # 'approver' | 'submitter'
    st.session_state.user_role = None              # TEAM for approver pages: 'SDA'|'DataGuild'|'DigitalGuild'|'ETIDM'
    st.session_state.current_app_page = ""
    st.session_state.refresh = 0

# =========================================================
# LOGIN VIEW (with styling)
# =========================================================
def login_view():
    st.set_page_config(page_title="Digital Portfolio", layout="centered")

    st.markdown(
        """
        <style>
            body { background-color:#f5f6f7; font-family: Inter, Roboto, Helvetica, sans-serif; }
            .login-card { max-width: 420px; margin: 80px auto; padding: 36px 28px; background:#fff; border-radius:16px;
                          box-shadow: 0 6px 18px rgba(0,0,0,0.1); }
            .login-title { text-align:center; font-size: 36px; font-weight:700; color:#87CEFA; margin-bottom:8px; }
            .login-subtitle { text-align:center; font-size:20px; color:#6DAE81; margin-bottom:26px; }
            .stTextInput > div > input { border-radius:8px; border:1px solid #ccc; padding:10px; font-size:16px; }
            .stButton > button { width:100%; background:#0078BF; color:#fff; font-size:16px; padding:10px; border-radius:8px; }
            .stButton > button:hover { background:#006aa6; }
            .internal-note { text-align:center; font-size:12px; color:#6B7280; margin-top:12px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="login-card">', unsafe_allow_html=True)
    st.markdown('<div class="login-title">Digital Portfolio</div>', unsafe_allow_html=True)
    st.markdown('<div class="login-subtitle">Sign in to continue</div>', unsafe_allow_html=True)

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        user = users.get(username)
        if user and user["password"] == password:
            # Session fields you showed in screenshots
            st.session_state.logged_in = True
            st.session_state.username = username
            st.session_state.role = user["role"]  # 'approver' | 'submitter'

            # ---- CRITICAL: store TEAM for approver dashboard & sidebar filtering ----
            # If username is one of the approver teams, set user_role so pages know the team.
            st.session_state.user_role = username if username in TEAM_CODES else None
            # ----------------------------------------------------------------------------

            st.rerun()
        else:
            st.error("Invalid username or password")

    st.markdown("</div>", unsafe_allow_html=True)  # close card

# =========================================================
# LOGOUT
# =========================================================
def logout():
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.role = None
    st.session_state.user_role = None
    st.rerun()

# =========================================================
# DATA LOADING (kept like your screenshots; uses your helpers)
# =========================================================
def load_data_to_session(refresh):
    # Sanctions
    sanction_data = "Sanctions.csv"
    sanction_df = load_and_clean(sanction_data, sheet_name="Sanctions")
    initialise_session_state("sanction_df", sanction_df)

    # JIRA
    jira_data = "JIRA_Field_Portfolio_Column_Mapping.xlsx"
    jira_df = load_and_clean(jira_data, sheet_name="JIRA_Dump")
    jira_df = clean_jira(jira_df)  # assuming you had this helper in common.funcs
    initialise_session_state("jira_df", jira_df)

# =========================================================
# MAIN APP ENTRY
# =========================================================
def main():
    # Gate: show login until authenticated
    if not st.session_state.logged_in:
        login_view()
        st.stop()

    # App Config (once logged in)
    st.set_page_config(page_title="Portfolio App", layout="wide", initial_sidebar_state="expanded")

    # Sidebar: who + logout
    st.sidebar.info(f"Logged in as: **{st.session_state.username}** ({st.session_state.role})")
    if st.sidebar.button("Logout"):
        logout()
    st.sidebar.divider()

    # Toggle data editing (you had something similar)
    st.sidebar.info("Toggle data editing")
    mode_toggle_col, save_edit_col = st.sidebar.columns([0.6, 0.4], gap="small")
    manage_mode_toggle = mode_toggle_col.toggle("Manage Mode", key="manage_mode")
    st.sidebar.divider()

    # Data load
    current_app_page = st.session_state["current_app_page"]
    refresh = st.session_state["refresh"]
    load_data_to_session(refresh)

    # --------------------------
    # Pages dictionary (matches your file names)
    # --------------------------
    pages_dict = {
        "Overview": [
            st.Page("app_pages/Home.py",      title="Home",              icon="material/home")
        ],
        "Roadmap": [
            st.Page("app_pages/JiraRoadmap.py", title="Jira Visualisation", icon="material/route")
        ],
        "Sanctions": [
            st.Page("app_pages/SanctionBuilder.py",       title="Builder",       icon="material/build"),
            st.Page("app_pages/SanctionVisualisation.py", title="Visualisation", icon="material/dashboard"),
            st.Page("app_pages/SanctionListing.py",       title="Listing",       icon="material/list"),
        ],
        "Sanction Approver Dashboard": [
            st.Page("app_pages/SanctionApproverDashboard.py", title="Sanction Approver Dashboard")
        ],
        "Feedback Page": [
            st.Page("app_pages/Feedback_Page.py", title="Feedback Page")
        ],
    }

    # --------------------------
    # Role-based access (like your screenshots)
    # submitter sees: Overview, Roadmap, Sanctions
    # approver sees:  Sanction Approver Dashboard, Feedback Page
    # --------------------------
    page_roles = {
        "Overview": ["submitter"],
        "Roadmap": ["submitter"],
        "Sanctions": ["submitter"],
        "Sanction Approver Dashboard": ["approver"],
        "Feedback Page": ["approver"],
    }

    user_role_simple = st.session_state.role  # 'approver' | 'submitter'

    # Filter allowed sections/pages by role
    allowed_page_dict = {
        name: pages for name, pages in pages_dict.items()
        if user_role_simple in page_roles[name]
    }

    # Navigation
    if manage_mode_toggle:
        # Show all sections/pages in manage mode
        pages = st.navigation(pages_dict)
    else:
        # Only show allowed pages for this user role
        pages = st.navigation(allowed_page_dict)

    pages.run()


# Entrypoint
main()



# --------------------------
# Pages dictionary
# --------------------------
pages_dict = {
    "Overview": [
        st.Page("app_pages/Home.py", title="Home", icon="🏠")
    ],
    "Roadmap": [
        st.Page("app_pages/JiraRoadmap.py", title="Jira Visualisation", icon="🗺️")
    ],
    "Sanctions": [
        st.Page("app_pages/SanctionBuilder.py", title="Builder", icon="🧱"),
        st.Page("app_pages/SanctionVisualisation.py", title="Visualisation", icon="📊"),
        st.Page("app_pages/SanctionListing.py", title="Listing", icon="📋"),
    ],
    "Sanction Approver Dashboard": [
        st.Page("app_pages/SanctionApproverDashboard.py", title="Sanction Approver Dashboard", icon="🧾")
    ],
    "Feedback Page": [
        st.Page("app_pages/Feedback_Page.py", title="Feedback Page", icon="💬")
    ],
}
