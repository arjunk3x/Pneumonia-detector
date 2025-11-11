

# app_pages/SanctionApproverDashboard.py
# Pastel-themed dashboard with seamless sidebar team switching (simulated login)

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# ==============================
# PAGE SETUP
# ==============================
st.set_page_config(
    page_title="Sanction Approver Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================
# PASTEL THEME CSS
# ==============================
st.markdown("""
<style>
:root {
  --bg:#fafbff; --card:#ffffff; --ink:#1a1a1a; --muted:#6b7280; --ring:#e5e7eb;
  --primary:#a3bffa; --ok:#9de8c1; --warn:#ffe3a3; --danger:#ffc2c2;
  --radius:14px; --shadow:0 2px 8px rgba(0,0,0,.05); --shadow-lg:0 6px 16px rgba(0,0,0,.08);
}
html, body { background: var(--bg); color: var(--ink); }
.block-container { padding-top: 1rem; }
h1,h2,h3 { color: var(--ink); }

.card { border:1px solid var(--ring); border-radius: var(--radius); background: var(--card);
  box-shadow: var(--shadow); padding:18px 20px; }
.kpi { background: #fdfdff; border-radius: var(--radius); padding:14px 16px; box-shadow: var(--shadow); }
.kpi .label { font-size:13px; color:var(--muted); }
.kpi .value { font-size:20px; font-weight:700; }

.badge {
  display:inline-flex; align-items:center; font-weight:700; font-size:12px; padding:5px 10px;
  border-radius:999px; border:1px solid var(--ring);
}
.badge.primary { background:var(--primary); color:#202060; }
.badge.ok { background:var(--ok); color:#054f2b; }
.badge.warn { background:var(--warn); color:#4e3b00; }
.badge.danger { background:var(--danger); color:#6a0000; }

.table-card .stDataFrame { border-radius:10px; box-shadow: var(--shadow); }

.stButton>button {
  border-radius:10px; padding:9px 14px; border:1px solid var(--ring);
  background:#f8f9ff; color:#1a1a1a; font-weight:700; box-shadow: var(--shadow);
}
.stButton>button:hover { background:var(--primary); color:white; }

.pill { display:inline-block; padding:2px 10px; font-size:12px; font-weight:700; border-radius:999px; }
.pill.ok { background:var(--ok); color:#054f2b; }
.pill.warn { background:var(--warn); color:#4e3b00; }
.pill.danger { background:var(--danger); color:#6a0000; }
</style>
""", unsafe_allow_html=True)

# ==============================
# FILE PATHS / DATA SOURCES
# ==============================
CSV_PATH = Path(st.session_state.get("tracker_csv_path", "approver_tracker.csv"))

# ==============================
# USER + ROLE MANAGEMENT (Unified Sidebar Switcher)
# ==============================

USER_ROLE_MAP = {
    "sda@company.com": "SDA",
    "dataguild@company.com": "DataGuild",
    "digitalguild@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}

ROLE_LABEL = {
    "SDA": "SDA",
    "DataGuild": "Data Guild",
    "DigitalGuild": "Digital Guild",
    "ETIDM": "ETIDM",
}

# Set defaults on first load
if "user_email" not in st.session_state:
    st.session_state["user_email"] = "sda@company.com"
if "user_role" not in st.session_state:
    st.session_state["user_role"] = USER_ROLE_MAP[st.session_state["user_email"]]

# Sidebar logic (simulated login switcher)
with st.sidebar:
    st.markdown("### 👤 Active User")
    users = list(USER_ROLE_MAP.keys())
    current_user = st.session_state["user_email"]

    selected_user = st.selectbox(
        "Select a user (simulate login)",
        options=users,
        index=users.index(current_user) if current_user in users else 0,
    )

    # If changed → simulate new login
    if selected_user != st.session_state["user_email"]:
        st.session_state["user_email"] = selected_user
        st.session_state["user_role"] = USER_ROLE_MAP[selected_user]
        st.success(f"Switched to {selected_user} ({ROLE_LABEL[st.session_state['user_role']]})")
        st.rerun()

    st.markdown("### 🧩 Team / Role")
    roles = list(USER_ROLE_MAP.values())
    selected_role = st.selectbox(
        "Team",
        options=roles,
        index=roles.index(st.session_state["user_role"]),
        format_func=lambda r: ROLE_LABEL[r],
    )

    # If manually switching team → update and rerun
    if selected_role != st.session_state["user_role"]:
        st.session_state["user_role"] = selected_role
        # infer email associated with this role
        new_user = next((u for u, r in USER_ROLE_MAP.items() if r == selected_role), "sda@company.com")
        st.session_state["user_email"] = new_user
        st.success(f"Switched to {ROLE_LABEL[selected_role]} ({new_user})")
        st.rerun()

current_user = st.session_state["user_email"]
current_role = st.session_state["user_role"]

# ==============================
# DATA LOADING
# ==============================
if not CSV_PATH.exists():
    st.error(f"CSV not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Add missing columns if needed
for col, default in [
    ("Sanction_ID", ""),
    ("Value", 0.0),
    ("overall_status", "submitted"),
    ("is_submitter", 0),
    ("is_in_SDA", 0), ("SDA_status", "Pending"), ("SDA_assigned_to", None), ("SDA_decision_at", None),
    ("is_in_data_guild", 0), ("data_guild_status", "Pending"), ("data_guild_assigned_to", None), ("data_guild_decision_at", None),
    ("is_in_digital_guild", 0), ("digital_guild_status", "Pending"), ("digital_guild_assigned_to", None), ("digital_guild_decision_at", None),
    ("is_in_etidm", 0), ("etidm_status", "Pending"), ("etidm_assigned_to", None), ("etidm_decision_at", None),
]:
    if col not in df.columns:
        df[col] = default

# Connect DuckDB
con = duckdb.connect()
con.register("approval", df)

# ==============================
# STAGE HELPERS
# ==============================
STAGE_MAP = {
    "SDA": {"is_in": "is_in_SDA", "status": "SDA_status", "assigned_to": "SDA_assigned_to", "decision_at": "SDA_decision_at"},
    "DataGuild": {"is_in": "is_in_data_guild", "status": "data_guild_status", "assigned_to": "data_guild_assigned_to", "decision_at": "data_guild_decision_at"},
    "DigitalGuild": {"is_in": "is_in_digital_guild", "status": "digital_guild_status", "assigned_to": "digital_guild_assigned_to", "decision_at": "digital_guild_decision_at"},
    "ETIDM": {"is_in": "is_in_etidm", "status": "etidm_status", "assigned_to": "etidm_assigned_to", "decision_at": "etidm_decision_at"},
}

ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def stage_cols(role: str):
    m = STAGE_MAP[role]
    return m["is_in"], m["status"], m["assigned_to"], m["decision_at"]

def prev_role(role: str):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i - 1] if i > 0 else None

def flag_true_sql(col: str) -> str:
    return f"""
    CASE
      WHEN LOWER(CAST({col} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
      WHEN TRY_CAST({col} AS BIGINT) = 1 THEN TRUE
      ELSE FALSE
    END
    """

def visibility_filter_for(role: str) -> str:
    cur_is_in, cur_status, _, _ = stage_cols(role)
    if role == "SDA":
        return f"""
            {flag_true_sql(cur_is_in)} = TRUE
            AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
        """
    p = prev_role(role)
    if not p:
        return "FALSE"
    _, p_status, _, p_decision_at = stage_cols(p)
    return f"""
        CAST({p_status} AS VARCHAR) = 'Approved'
        AND TRY_CAST({p_decision_at} AS TIMESTAMP) IS NOT NULL
        AND {flag_true_sql(cur_is_in)} = TRUE
        AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
    """

# ==============================
# HEADER
# ==============================
st.markdown(f"""
<div class="card" style="display:flex;justify-content:space-between;align-items:center;">
  <div>
    <h2 style="margin:0;">Sanction Approver Dashboard</h2>
    <div class="badge primary">Team: {ROLE_LABEL[current_role]}</div>
  </div>
  <div class="badge">User: {current_user}</div>
</div>
""", unsafe_allow_html=True)

# ==============================
# KPIs
# ==============================
is_in_col, status_col, _, decision_col = stage_cols(current_role)
pending_df = con.execute(f"SELECT * FROM approval WHERE {visibility_filter_for(current_role)}").df()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="kpi"><div class="label">Pending</div><div class="value">{len(pending_df)}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="kpi"><div class="label">Team</div><div class="value">{ROLE_LABEL[current_role]}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="kpi"><div class="label">Total Records</div><div class="value">{len(df)}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="kpi"><div class="label">Unique IDs</div><div class="value">{df["Sanction_ID"].nunique()}</div></div>', unsafe_allow_html=True)

st.divider()

# ==============================
# PENDING ITEMS
# ==============================
st.markdown(f"### Pending in **{ROLE_LABEL[current_role]}**")

if not pending_df.empty:
    for _, row in pending_df.iterrows():
        c_left, c_right = st.columns([6, 1])
        with c_left:
            st.write(f"**{row['Sanction_ID']}** | Value: {row.get('Value','')} | Status: {row[status_col]}")
        with c_right:
            if st.button("View ↗", key=f"view_{row['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(row["Sanction_ID"])
                st.switch_page("app_pages/Feedback_Page.py")
else:
    st.info(f"No pending sanctions for **{ROLE_LABEL[current_role]}**.")

# ==============================
# INTAKE SECTION
# ==============================
with st.expander(f"Intake ({ROLE_LABEL[current_role]})", expanded=False):
    if current_role == "SDA":
        backlog_df = con.execute(f"""
            SELECT * FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND {flag_true_sql('is_in_SDA')} = FALSE
        """).df()
    else:
        p = prev_role(current_role)
        p_is_in, p_status, _, p_decision_at = stage_cols(p)
        cur_is_in, cur_status, _, _ = stage_cols(current_role)
        backlog_df = con.execute(f"""
            SELECT * FROM approval
            WHERE CAST({p_status} AS VARCHAR) = 'Approved'
              AND TRY_CAST({p_decision_at} AS TIMESTAMP) IS NOT NULL
              AND {flag_true_sql(cur_is_in)} = FALSE
              AND {flag_true_sql(p_is_in)} = TRUE
              AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
        """).df()

    if backlog_df.empty:
        st.info("No items available for intake.")
    else:
        st.dataframe(backlog_df, use_container_width=True)
        ids = st.multiselect("Select Sanction_IDs to move", backlog_df["Sanction_ID"].astype(str).tolist())
        if st.button(f"Move to {ROLE_LABEL[current_role]}") and ids:
            df["Sanction_ID"] = df["Sanction_ID"].astype(str)
            mask = df["Sanction_ID"].isin(ids)
            cur_is_in, cur_status, _, _ = stage_cols(current_role)
            df.loc[mask, cur_is_in] = 1
            df.loc[mask, cur_status] = df.loc[mask, cur_status].fillna("Pending").replace("", "Pending")
            df.to_csv(CSV_PATH, index=False)
            st.success(f"Moved {len(ids)} record(s) to {ROLE_LABEL[current_role]}")
            st.rerun()

# ==============================
# FOOTER
# ==============================
st.caption("✨ Pastel UI • Seamless team switching • Role-aware dashboard")
