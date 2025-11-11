# app_pages/SanctionApproverDashboard.py
# Pastel-themed dashboard with TRUE identity switching (simulated login via sidebar)

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

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
# IDENTITY MANAGER  ✅ (REAL SWITCH)
# ==============================
USERS = {
    "sda@company.com": "SDA",
    "dataguild@company.com": "DataGuild",
    "digitalguild@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}
ROLE_LABEL = {"SDA":"SDA","DataGuild":"Data Guild","DigitalGuild":"Digital Guild","ETIDM":"ETIDM"}
ROLES = list(ROLE_LABEL.keys())

def _init_identity():
    if "user_email" not in st.session_state:
        st.session_state["user_email"] = "sda@company.com"
    if "user_role" not in st.session_state:
        st.session_state["user_role"] = USERS[st.session_state["user_email"]]
    if "auth_ts" not in st.session_state:     # cache key to invalidate on switch
        st.session_state["auth_ts"] = datetime.now().isoformat()

def _set_identity(email: str=None, role: str=None, *, rerun=True):
    """Single source of truth to switch identity. Clears caches and reruns."""
    if email is not None:
        st.session_state["user_email"] = email
        st.session_state["user_role"] = USERS[email]
    if role is not None:
        st.session_state["user_role"] = role
        # pick first email with this role (demo mapping)
        new_email = next((u for u,r in USERS.items() if r==role), "sda@company.com")
        st.session_state["user_email"] = new_email
    st.session_state["auth_ts"] = datetime.now().isoformat()
    st.cache_data.clear()
    if rerun:
        st.rerun()

_init_identity()

with st.sidebar:
    st.markdown("### 👤 Active User")
    u = st.selectbox(
        "Select a user (simulate login)",
        options=list(USERS.keys()),
        index=list(USERS.keys()).index(st.session_state["user_email"]),
        key="sb_user_select"
    )
    if u != st.session_state["user_email"]:
        _set_identity(email=u, rerun=True)

    st.markdown("### 🧩 Team / Role")
    r = st.selectbox(
        "Team",
        options=ROLES,
        index=ROLES.index(st.session_state["user_role"]),
        format_func=lambda x: ROLE_LABEL[x],
        key="sb_role_select"
    )
    if r != st.session_state["user_role"]:
        _set_identity(role=r, rerun=True)

current_user = st.session_state["user_email"]
current_role = st.session_state["user_role"]
ROLE_FLOW = ["SDA","DataGuild","DigitalGuild","ETIDM"]

# ==============================
# DATA LOADING (cache is identity-scoped)
# ==============================
@st.cache_data
def _load_df(csv_path: str, _identity_key: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df

if not CSV_PATH.exists():
    st.error(f"CSV not found at {CSV_PATH.resolve()}")
    st.stop()

df = _load_df(str(CSV_PATH), st.session_state["auth_ts"])  # <— cache keyed by auth_ts

# Ensure expected columns
for col, default in [
    ("Sanction_ID",""),
    ("Value",0.0),
    ("overall_status","submitted"),
    ("is_submitter",0),
    ("is_in_SDA",0),("SDA_status","Pending"),("SDA_assigned_to",None),("SDA_decision_at",None),
    ("is_in_data_guild",0),("data_guild_status","Pending"),("data_guild_assigned_to",None),("data_guild_decision_at",None),
    ("is_in_digital_guild",0),("digital_guild_status","Pending"),("digital_guild_assigned_to",None),("digital_guild_decision_at",None),
    ("is_in_etidm",0),("etidm_status","Pending"),("etidm_assigned_to",None),("etidm_decision_at",None),
]:
    if col not in df.columns:
        df[col] = default

# DuckDB
con = duckdb.connect()
con.register("approval", df)

# ==============================
# STAGE HELPERS
# ==============================
STAGE_MAP = {
    "SDA": {"is_in":"is_in_SDA","status":"SDA_status","assigned_to":"SDA_assigned_to","decision_at":"SDA_decision_at"},
    "DataGuild": {"is_in":"is_in_data_guild","status":"data_guild_status","assigned_to":"data_guild_assigned_to","decision_at":"data_guild_decision_at"},
    "DigitalGuild": {"is_in":"is_in_digital_guild","status":"digital_guild_status","assigned_to":"digital_guild_assigned_to","decision_at":"digital_guild_decision_at"},
    "ETIDM": {"is_in":"is_in_etidm","status":"etidm_status","assigned_to":"etidm_assigned_to","decision_at":"etidm_decision_at"},
}
def stage_cols(role:str):
    m = STAGE_MAP[role]; return m["is_in"], m["status"], m["assigned_to"], m["decision_at"]
def prev_role(role:str):
    i = ROLE_FLOW.index(role); return ROLE_FLOW[i-1] if i>0 else None
def flag_true_sql(col:str)->str:
    return f"""
    CASE
      WHEN LOWER(CAST({col} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
      WHEN TRY_CAST({col} AS BIGINT) = 1 THEN TRUE
      ELSE FALSE
    END
    """
def visibility_filter_for(role:str)->str:
    cur_is_in, cur_status, _, _ = stage_cols(role)
    if role == "SDA":
        return f"{flag_true_sql(cur_is_in)} = TRUE AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')"
    p = prev_role(role)
    if not p: return "FALSE"
    _, p_status, _, p_decision_at = stage_cols(p)
    return f"""
      CAST({p_status} AS VARCHAR)='Approved'
      AND TRY_CAST({p_decision_at} AS TIMESTAMP) IS NOT NULL
      AND {flag_true_sql(cur_is_in)} = TRUE
      AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
    """

# ==============================
# HEADER & KPIs
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

is_in_col, status_col, _, decision_col = stage_cols(current_role)
pending_df = con.execute(f"SELECT * FROM approval WHERE {visibility_filter_for(current_role)}").df()

c1,c2,c3,c4 = st.columns(4)
with c1: st.markdown(f'<div class="kpi"><div class="label">Pending</div><div class="value">{len(pending_df)}</div></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="kpi"><div class="label">Team</div><div class="value">{ROLE_LABEL[current_role]}</div></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="kpi"><div class="label">Total Records</div><div class="value">{len(df)}</div></div>', unsafe_allow_html=True)
with c4: st.markdown(f'<div class="kpi"><div class="label">Unique IDs</div><div class="value">{df["Sanction_ID"].nunique()}</div></div>', unsafe_allow_html=True)

st.divider()

# ==============================
# PENDING ITEMS (View -> Feedback page)
# ==============================
st.markdown(f"### Pending in **{ROLE_LABEL[current_role]}**")

if not pending_df.empty:
    for _, row in pending_df.iterrows():
        left, right = st.columns([6,1])
        with left:
            st.write(f"**{row['Sanction_ID']}** | Value: {row.get('Value','')} | Status: {row[status_col]}")
        with right:
            if st.button("View ↗", key=f"view_{row['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(row["Sanction_ID"])
                st.switch_page("app_pages/Feedback_Page.py")
else:
    st.info(f"No pending sanctions for **{ROLE_LABEL[current_role]}**.")

# ==============================
# INTAKE
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
            WHERE CAST({p_status} AS VARCHAR)='Approved'
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

st.caption("✨ Pastel UI • TRUE identity switching • Role-aware dashboard")




# app_pages/Feedback_Page.py
# Pastel-themed Feedback page with strict role-based actions, synced to identity switch

import os
from datetime import datetime
import pandas as pd
import streamlit as st

# =========================
# CONFIG
# =========================
SANCTIONS_PATH = os.getenv("SANCTIONS_PATH", "sanctions_data.csv")
APPROVER_TRACKER_PATH = os.getenv("APPROVER_TRACKER_PATH", "approver_tracker.csv")

STAGES = ["SDA", "Data Guild", "Digital Guild", "ETIDM"]
STAGE_KEYS = {
    "SDA": {"flag":"is_in_SDA","status":"SDA_status","assigned_to":"SDA_assigned_to","decision_at":"SDA_decision_at"},
    "Data Guild": {"flag":"is_in_data_guild","status":"data_guild_status","assigned_to":"data_guild_assigned_to","decision_at":"data_guild_decision_at"},
    "Digital Guild": {"flag":"is_in_digital_guild","status":"digital_guild_status","assigned_to":"digital_guild_assigned_to","decision_at":"digital_guild_decision_at"},
    "ETIDM": {"flag":"is_in_etidm","status":"etidm_status","assigned_to":"etidm_assigned_to","decision_at":"etidm_decision_at"},
}

# =========================
# PASTEL THEME
# =========================
st.set_page_config(page_title="Feedback | Sanctions", layout="wide", initial_sidebar_state="expanded")
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
.kpi { background:#fdfdff; border-radius: var(--radius); padding:14px 16px; box-shadow: var(--shadow); }
.kpi .label { font-size:13px; color:var(--muted); }
.kpi .value { font-size:20px; font-weight:800; }
.badge { display:inline-flex; align-items:center; font-weight:700; font-size:12px; padding:5px 10px;
  border-radius:999px; border:1px solid var(--ring); }
.badge.primary { background:var(--primary); color:#202060; }
.badge.ok { background:var(--ok); color:#054f2b; }
.badge.warn { background:var(--warn); color:#4e3b00; }
.badge.danger { background:var(--danger); color:#6a0000; }
.pill { display:inline-block; padding:2px 10px; font-size:12px; font-weight:700; border-radius:999px; }
.pill.ok { background:var(--ok); color:#054f2b; }
.pill.warn { background:var(--warn); color:#4e3b00; }
.pill.danger { background:var(--danger); color:#6a0000; }
.flow { display:flex; gap:12px; flex-wrap:wrap; align-items:stretch; }
.step { flex:1 1 240px; background:#fff; border:1px dashed var(--ring); border-radius: var(--radius);
  padding:14px 16px; box-shadow: var(--shadow); }
.step.active { border-color: var(--primary); background:#f4f7ff; }
.step.done { border-color: var(--ok); background:#f5fff9; }
.step .title { font-weight:700; margin-bottom:4px; }
.step .meta { font-size:12px; color:var(--muted); }
.arrow { font-size:20px; color:#a3a3a3; display:flex; align-items:center; }
.sticky-actions { position:sticky; top:8px; background:var(--card); border:1px solid var(--ring);
  border-radius: var(--radius); box-shadow: var(--shadow-lg); padding:16px; }
.stButton>button { border-radius:10px; padding:10px 16px; border:1px solid var(--ring);
  background:#f8f9ff; color:#1a1a1a; font-weight:700; box-shadow: var(--shadow); }
.stButton>button:hover { background:var(--primary); color:white; }
.table-card .stDataFrame { border-radius:10px; box-shadow: var(--shadow); }
.codechip { font-family: monospace; font-size:12px; background:#f0f3ff; color:#334155; border-radius:6px; padding:2px 6px; }
.small{ font-size:12px; color:var(--muted) }
</style>
""", unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def _read_csv(p): return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()
def _write_csv(df,p): os.makedirs(os.path.dirname(p) or ".", exist_ok=True); df.to_csv(p,index=False)
def _pill_class(txt):
    if not txt: return "ok"
    t = str(txt).lower()
    if "reject" in t: return "danger"
    if "pending" in t or "review" in t or "request" in t: return "warn"
    return "ok"
def _fmt_money(v, ccy="GBP"):
    if pd.isna(v): return "-"
    try: return f"{ccy} {float(v):,.0f}"
    except: return str(v)
def _now_iso(): return datetime.now().isoformat(timespec="seconds")
def _next_stage(current):
    if current not in STAGES: return None
    i = STAGES.index(current)
    return STAGES[i+1] if i+1 < len(STAGES) else None
def _ensure_tracker(df):
    base = ["Sanction_ID","Title","Requester_Email","Department","Submitted_at","Value",
            "Currency","Risk_Level","Overall_status","Current Stage","Last_comment"]
    for c in base:
        if c not in df.columns: df[c] = "" if c!="Value" else 0
    for m in STAGE_KEYS.values():
        for col in m.values():
            if col not in df.columns: df[col] = ""
    return df
def _stage_block(stage, tr, current_stage):
    meta = STAGE_KEYS[stage]
    status = str(tr.get(meta["status"], "Pending"))
    cls = _pill_class(status)
    state = "active" if current_stage==stage else ("done" if status.lower() in ["approved","rejected"] else "")
    icon = {"SDA":"🧮","Data Guild":"📊","Digital Guild":"💻","ETIDM":"🧪"}.get(stage,"🧩")
    return f"""
      <div class="step {state}">
        <div class="title">{icon} {stage}</div>
        <div class="meta">Status: <span class="pill {cls}">{status}</span></div>
      </div>
    """

# =========================
# IDENTITY (trust session; no SDA fallback when present)  ✅
# =========================
if "user_email" not in st.session_state:
    st.session_state["user_email"] = "sda@company.com"
if "user_role" not in st.session_state:
    st.session_state["user_role"] = "SDA"

def _current_internal_role() -> str:
    role = st.session_state.get("user_role")
    if role in {"SDA","DataGuild","DigitalGuild","ETIDM"}:
        return role
    # Fallback only if invalid:
    e = (st.session_state.get("user_email","") or "").lower()
    if "dataguild" in e: return "DataGuild"
    if "digital" in e: return "DigitalGuild"
    if "etidm"   in e: return "ETIDM"
    return "SDA"

def _role_to_stage_label(r: str) -> str:
    return {"SDA":"SDA","DataGuild":"Data Guild","DigitalGuild":"Digital Guild","ETIDM":"ETIDM"}[r]

st.caption(f"Logged in as: **{st.session_state['user_email']}** · Role: **{_role_to_stage_label(_current_internal_role())}**")

# =========================
# GET SELECTED SANCTION
# =========================
sid = st.session_state.get("selected_sanction_id") or st.experimental_get_query_params().get("sanction_id", [None])[0]
if not sid:
    st.warning("No sanction selected. Go back and click **View** on a record.")
    st.stop()
sid = str(sid)

# =========================
# LOAD DATA
# =========================
sanctions_df = _read_csv(SANCTIONS_PATH)
tracker_df   = _ensure_tracker(_read_csv(APPROVER_TRACKER_PATH))
if sanctions_df.empty or tracker_df.empty:
    st.error("Data files missing or empty."); st.stop()

if "Sanction ID" in sanctions_df.columns: sanctions_df["Sanction ID"] = sanctions_df["Sanction ID"].astype(str)
if "Sanction_ID" in tracker_df.columns:   tracker_df["Sanction_ID"]   = tracker_df["Sanction_ID"].astype(str)

s_row = sanctions_df.loc[sanctions_df["Sanction ID"]==sid]
t_row = tracker_df.loc[tracker_df["Sanction_ID"]==sid]
s_row = s_row.iloc[0] if not s_row.empty else pd.Series(dtype="object")
t_row = t_row.iloc[0] if not t_row.empty else pd.Series(dtype="object")

current_stage = str(t_row.get("Current Stage", s_row.get("Current Stage","SDA")))
if current_stage not in STAGES: current_stage = "SDA"

# =========================
# HEADER
# =========================
st.markdown(f"""
<div class="card" style="display:flex;justify-content:space-between;align-items:center;">
  <div>
    <div class="small">Feedback Page</div>
    <h2 style="margin:0;">{s_row.get('Project Name','Untitled')}</h2>
    <div class="small">Sanction <span class="codechip">{sid}</span></div>
  </div>
  <div class="badge primary">{current_stage}</div>
</div>
""", unsafe_allow_html=True)

# =========================
# KPI Rows
# =========================
st.markdown('<div class="card" style="margin-top:12px;">', unsafe_allow_html=True)
c1,c2,c3,c4 = st.columns(4)
with c1: st.markdown('<div class="kpi"><div class="label">Project Name</div><div class="value">{}</div></div>'.format(s_row.get("Project Name","-")), unsafe_allow_html=True)
with c2: st.markdown('<div class="kpi"><div class="label">Directorate</div><div class="value">{}</div></div>'.format(s_row.get("Directorate","-")), unsafe_allow_html=True)
with c3:
    amt = _fmt_money(s_row.get("Amount", t_row.get("Value", None)), t_row.get("Currency","GBP"))
    st.markdown(f'<div class="kpi"><div class="label">Amount</div><div class="value">{amt}</div></div>', unsafe_allow_html=True)
with c4:
    overall = s_row.get("Status", t_row.get("Overall_status", "Pending"))
    st.markdown(f'<div class="kpi"><div class="label">Overall Status</div><div class="value">{overall}</div></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="card" style="margin-top:10px;">', unsafe_allow_html=True)
d1,d2,d3,d4 = st.columns(4)
with d1: st.markdown('<div class="kpi"><div class="label">Submitted</div><div class="value">{}</div></div>'.format(s_row.get("Submitted", t_row.get("Submitted_at","-"))), unsafe_allow_html=True)
with d2: st.markdown('<div class="kpi"><div class="label">Requester</div><div class="value">{}</div></div>'.format(t_row.get("Requester_Email","-")), unsafe_allow_html=True)
with d3: st.markdown('<div class="kpi"><div class="label">Department</div><div class="value">{}</div></div>'.format(t_row.get("Department","-")), unsafe_allow_html=True)
with d4:
    rl = t_row.get("Risk_Level","-")
    st.markdown(f'<div class="kpi"><div class="label">Risk Level</div><div class="value">{rl}</div></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# =========================
# Flow Timeline
# =========================
st.subheader("Approval Flow")
flow_html = '<div class="flow">'
for i, stage in enumerate(STAGES):
    flow_html += _stage_block(stage, t_row, current_stage)
    if i < len(STAGES)-1: flow_html += '<div class="arrow">→</div>'
flow_html += "</div>"
st.markdown(flow_html, unsafe_allow_html=True)

st.divider()

# =========================
# Details + Attachments
# =========================
left, right = st.columns([3,2], gap="large")
with left:
    st.subheader("Details")
    details = {
        "Sanction ID": sid,
        "Project Name": s_row.get("Project Name","-"),
        "Status": s_row.get("Status", t_row.get("Overall_status","-")),
        "Directorate": s_row.get("Directorate","-"),
        "Amount": amt,
        "Current Stage": current_stage,
        "Submitted": s_row.get("Submitted", t_row.get("Submitted_at","-")),
        "Title": t_row.get("Title","-"),
        "Currency": t_row.get("Currency","GBP"),
        "Risk Level": t_row.get("Risk_Level","-"),
        "Linked resanctions": s_row.get("Linked resanctions","-"),
    }
    st.markdown('<div class="table-card">', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame({"Field":list(details.keys()),"Value":list(details.values())}),
                 hide_index=True, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.subheader("Attachments")
    atts = s_row.get("Attachments","")
    if pd.isna(atts) or str(atts).strip()=="":
        st.info("No attachments uploaded.")
    else:
        items = [a.strip() for a in str(atts).replace(";",",").split(",") if a.strip()]
        for i,a in enumerate(items,1):
            st.markdown(f"- 📎 **Attachment {i}:** {a}")

st.divider()

# =========================
# Stage Actions — Role Protected
# =========================
stage_label = current_stage
meta = STAGE_KEYS.get(stage_label)
st.subheader(f"Stage Actions — {stage_label}")

if not meta:
    st.info("This stage has no configured actions.")
else:
    # Only the *matching* stage can act
    user_role_internal = _current_internal_role()                # e.g. DataGuild
    user_stage_label   = _role_to_stage_label(user_role_internal)  # e.g. Data Guild
    role_can_act = (user_stage_label == stage_label)

    with st.container():
        st.markdown('<div class="sticky-actions">', unsafe_allow_html=True)
        if not role_can_act:
            st.warning(f"Only **{stage_label}** can action this stage.")

        with st.form("decision_form"):
            col1,col2,col3 = st.columns(3)
            with col1:
                decision = st.radio("Decision", ["Approve ✅","Reject ⛔","Request changes ✍️"], index=0, disabled=not role_can_act)
            with col2:
                assigned_to = st.text_input("Assign to", value=str(t_row.get(meta["assigned_to"],"")), disabled=not role_can_act)
            with col3:
                when = st.text_input("Decision time", value=_now_iso(), disabled=not role_can_act)
            comment = st.text_area("Comments / Rationale", disabled=not role_can_act)
            submitted = st.form_submit_button("Submit Decision", disabled=not role_can_act)

        if submitted:
            if not role_can_act:
                st.error("You are not authorized to act on this stage.")
                st.stop()

            tracker_df = _ensure_tracker(tracker_df)
            mask = tracker_df["Sanction_ID"] == sid
            dec_lower = decision.lower()
            new_status = "Approved" if "approve" in dec_lower else ("Rejected" if "reject" in dec_lower else "Changes requested")

            tracker_df.loc[mask, meta["status"]] = new_status
            tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
            tracker_df.loc[mask, meta["decision_at"]] = when or _now_iso()
            tracker_df.loc[mask, "Last_comment"] = comment

            nxt = _next_stage(stage_label) if new_status == "Approved" else None
            if new_status == "Approved" and nxt:
                tracker_df.loc[mask, "Current Stage"] = nxt
                tracker_df.loc[mask, "Overall_status"] = "In progress"
                for stg, m in STAGE_KEYS.items():
                    tracker_df.loc[mask, m["flag"]] = (stg == nxt)
            elif new_status == "Rejected":
                tracker_df.loc[mask, "Overall_status"] = "Rejected"
            else:
                tracker_df.loc[mask, "Overall_status"] = "Changes requested"

            _write_csv(tracker_df, APPROVER_TRACKER_PATH)

            # Optional mirror
            try:
                if "Sanction ID" in sanctions_df.columns:
                    ms = sanctions_df["Sanction ID"] == sid
                    if "Current Stage" in sanctions_df.columns:
                        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
                    if "Status" in sanctions_df.columns:
                        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
                    _write_csv(sanctions_df, SANCTIONS_PATH)
            except Exception:
                pass

            st.success(f"Decision saved: {new_status}")
            st.rerun()

# =========================
# Tracker snapshot
# =========================
st.divider()
st.subheader("Tracker Snapshot")
cols = [
    "Sanction_ID","Overall_status","Current Stage",
    "is_in_SDA","SDA_status","SDA_assigned_to","SDA_decision_at",
    "is_in_data_guild","data_guild_status","data_guild_assigned_to","data_guild_decision_at",
    "is_in_digital_guild","digital_guild_status","digital_guild_assigned_to","digital_guild_decision_at",
    "is_in_etidm","etidm_status","etidm_assigned_to","etidm_decision_at",
    "Last_comment"
]
cols = [c for c in cols if c in tracker_df.columns]
st.markdown('<div class="table-card">', unsafe_allow_html=True)
st.dataframe(tracker_df.loc[tracker_df["Sanction_ID"]==sid, cols], hide_index=True, use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)
