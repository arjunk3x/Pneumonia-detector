# app_pages/SanctionApproverDashboard.py
import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide", initial_sidebar_state="expanded")

# ---------- Session init (do this BEFORE any access) ----------
if "user_email" not in st.session_state:
    st.session_state["user_email"] = "sda@company.com"
if "user_role" not in st.session_state:
    st.session_state["user_role"] = "SDA"   # SDA | DataGuild | DigitalGuild | ETIDM

# ---------- Constants ----------
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def role_display_name(role: str) -> str:
    return {
        "SDA": "SDA",
        "DataGuild": "Data Guild",
        "DigitalGuild": "Digital Guild",
        "ETIDM": "ETIDM",
    }.get(role, role)

STAGE_MAP = {
    "SDA": {"is_in": "is_in_SDA", "status": "SDA_status", "assigned_to": "SDA_assigned_to", "decision_at": "SDA_decision_at"},
    "DataGuild": {"is_in": "is_in_data_guild", "status": "data_guild_status", "assigned_to": "data_guild_assigned_to", "decision_at": "data_guild_decision_at"},
    "DigitalGuild": {"is_in": "is_in_digital_guild", "status": "digital_guild_status", "assigned_to": "digital_guild_assigned_to", "decision_at": "digital_guild_decision_at"},
    "ETIDM": {"is_in": "is_in_etidm", "status": "etidm_status", "assigned_to": "etidm_assigned_to", "decision_at": "etidm_decision_at"},
}

def stage_cols(role: str):
    m = STAGE_MAP[role]
    return m["is_in"], m["status"], m["assigned_to"], m["decision_at"]

def prev_role(role: str):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i-1] if i > 0 else None

# robust truthy check for 0/1/yes/true
def flag_true_sql(col_name: str) -> str:
    return f"""
    CASE
        WHEN LOWER(CAST({col_name} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
        WHEN TRY_CAST({col_name} AS BIGINT) = 1 THEN TRUE
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
    p_is_in, p_status, _, p_decision_at = stage_cols(p)
    return f"""
        CAST({p_status} AS VARCHAR) = 'Approved'
        AND TRY_CAST({p_decision_at} AS TIMESTAMP) IS NOT NULL
        AND {flag_true_sql(cur_is_in)} = TRUE
        AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
    """

# ---------- Sidebar: Team switcher (persist to session) ----------
try:
    _idx = ROLE_FLOW.index(st.session_state["user_role"])
except ValueError:
    _idx = 0

picked = st.sidebar.selectbox("Team", ROLE_FLOW, index=_idx, format_func=role_display_name, key="team_selectbox")
# write back to session each run (source of truth)
st.session_state["user_role"] = picked

# ---------- Paths / Data ----------
CSV_PATH = Path(st.session_state.get("tracker_csv_path", "approver_tracker.csv"))
if not CSV_PATH.exists():
    st.error(f"CSV not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# ensure cols exist
for col, default in [
    ("Sanction_ID", ""), ("Value", 0.0), ("overall_status", "submitted"), ("is_submitter", 0),
    ("is_in_SDA", 0), ("SDA_status", "Pending"), ("SDA_assigned_to", None), ("SDA_decision_at", None),
    ("is_in_data_guild", 0), ("data_guild_status", "Pending"), ("data_guild_assigned_to", None), ("data_guild_decision_at", None),
    ("is_in_digital_guild", 0), ("digital_guild_status", "Pending"), ("digital_guild_assigned_to", None), ("digital_guild_decision_at", None),
    ("is_in_etidm", 0), ("etidm_status", "Pending"), ("etidm_assigned_to", None), ("etidm_decision_at", None),
]:
    if col not in df.columns:
        df[col] = default

con = duckdb.connect()
con.register("approval", df)

# ---------- Header ----------
current_role = st.session_state["user_role"]  # read directly from session
st.title("Sanction Approver Dashboard")
st.caption("Filter, review and act on sanctions by stage.")

# small debug chip so you can see what the app thinks
st.write(f"🧭 Current team: **{role_display_name(current_role)}**")

# ---------- Pending ----------
_, status_col, _, _ = stage_cols(current_role)
pending_df = con.execute(f"SELECT * FROM approval WHERE {visibility_filter_for(current_role)}").df()

c1, c2, c3 = st.columns(3)
with c1: st.metric("Pending", len(pending_df))
with c2: st.metric("Team", role_display_name(current_role))
with c3: st.metric("Unique IDs", df["Sanction_ID"].nunique())

st.divider()

# ***** Use session directly in the heading (no local copies) *****
st.markdown(f"### Pending in **{role_display_name(st.session_state['user_role'])}**")

if not pending_df.empty:
    for _, row in pending_df.iterrows():
        c_left, c_right = st.columns([6, 1])
        with c_left:
            st.write(
                f"**{row['Sanction_ID']}** | Value: {row.get('Value','')} | "
                f"Status: {row[status_col]} | Stage: {role_display_name(st.session_state['user_role'])}"
            )
        with c_right:
            if st.button("View ↗", key=f"view_{row['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(row["Sanction_ID"])
                try:
                    st.switch_page("app_pages/Feedback_Page.py")
                except Exception:
                    st.session_state["navigate_to_feedback"] = True
                st.rerun()
else:
    st.info(f"No pending sanctions for **{role_display_name(st.session_state['user_role'])}**.")

# ---------- Intake ----------
with st.expander(f"Intake ({role_display_name(st.session_state['user_role'])})", expanded=False):
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
        cols = [c for c in ["Sanction_ID","Value","overall_status"] if c in backlog_df.columns]
        cols += [c for c in backlog_df.columns if c.endswith("_status")]
        st.dataframe(backlog_df[cols], use_container_width=True)

        ids_text = st.text_input("Sanction_IDs to move (comma-separated)", placeholder="e.g. S001,S002,S010")
        if st.button("Move selected into this stage") and ids_text.strip():
            sel = [x.strip() for x in ids_text.split(",") if x.strip()]
            valid = set(backlog_df["Sanction_ID"].astype(str))
            intake_ids = [x for x in sel if x in valid]
            if not intake_ids:
                st.warning("No valid IDs to move.")
            else:
                cur_is_in, cur_status, _, _ = stage_cols(st.session_state["user_role"])
                df["Sanction_ID"] = df["Sanction_ID"].astype(str)
                mask = df["Sanction_ID"].isin(intake_ids)
                df.loc[mask, cur_is_in] = 1
                df.loc[mask, cur_status] = df.loc[mask, cur_status].fillna("Pending").replace("", "Pending")
                df.to_csv(CSV_PATH, index=False)
                try: con.unregister("approval")
                except Exception: pass
                con.register("approval", df)
                st.success(f"Moved {len(intake_ids)} to {role_display_name(st.session_state['user_role'])}")
                st.rerun()

# ---------- Footer ----------
st.caption(f"Logged in as: **{st.session_state['user_email']}** ({role_display_name(st.session_state['user_role'])})")
