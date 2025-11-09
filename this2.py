# ============================================================
# app_pages/SanctionApproverDashboard.py
# Styled, strict stage control, type-safe DuckDB, View→Feedback
# ============================================================
import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# ------------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide")

# -------------------  Global CSS  ---------------------------
st.markdown("""
<style>
:root{
  --bg:#f7f8fb;
  --card:#ffffff;
  --ink:#0f172a;
  --muted:#64748b;
  --ring:#e5e7eb;
  --shadow:0 10px 24px rgba(0,0,0,.06);
  --shadow-sm:0 4px 12px rgba(0,0,0,.05);
  --radius:16px;

  --ind:#4f46e5; --ind-weak:#eef2ff;
  --ok:#10b981;  --ok-weak:#ecfdf5;
  --warn:#f59e0b;--warn-weak:#fff7ed;
  --err:#ef4444; --err-weak:#fef2f2;
}

html, body, .block-container{ background:var(--bg); }
.block-container { padding-top: 1rem; }

.header{
  display:flex; align-items:center; justify-content:space-between;
  padding:18px 20px; background:#fff; border:1px solid var(--ring);
  border-radius:var(--radius); box-shadow:var(--shadow-sm); margin-bottom:10px;
}
.brand{ font-weight:900; letter-spacing:.2px; font-size:22px; color:var(--ink); }
.badge{
  display:inline-flex; align-items:center; gap:8px; padding:6px 12px;
  border-radius:999px; font-weight:800; font-size:12px; border:1px solid var(--ring);
}
.badge.primary{ background:var(--ind-weak); color:#312e81; border-color:#c7d2fe; }
.badge.ok{ background:var(--ok-weak); color:#065f46; border-color:#bef7dc; }
.badge.warn{ background:var(--warn-weak); color:#7c4a02; border-color:#ffe0b2; }
.badge.err{ background:var(--err-weak); color:#7f1d1d; border-color:#fecaca; }

.kpi{
  background:#fff; border:1px solid var(--ring); border-radius:18px; box-shadow:var(--shadow-sm);
  padding:18px; text-align:center; position:relative; overflow:hidden;
}
.kpi .val{ font-size:30px; font-weight:900; color:var(--ink); }
.kpi .lab{ margin-top:10px; font-weight:800; font-size:14px; }
.kpi.ind{ background:linear-gradient(180deg,#fff, #fafaff); }
.kpi.ok{ background:linear-gradient(180deg,#fff, #f8fffb); }
.kpi.warn{ background:linear-gradient(180deg,#fff, #fff8ed); }
.kpi.err{ background:linear-gradient(180deg,#fff, #fff6f6); }

.card{
  background:#fff; border:1px solid var(--ring); border-radius:16px; padding:14px 16px; box-shadow:var(--shadow-sm);
}

.row-item{
  display:flex; align-items:center; justify-content:space-between;
  border:1px solid var(--ring); border-radius:14px; padding:12px 14px; background:#fff; box-shadow:var(--shadow-sm);
}
.row-item .meta{ color:var(--muted); font-size:13px; margin-top:4px; }
.row-item + .row-item{ margin-top:10px; }

.stButton>button{
  border-radius:12px; padding:10px 16px; font-weight:800;
  border:1px solid var(--ring); background:#fff; color:var(--ink);
  transition:.16s ease; box-shadow:var(--shadow-sm);
}
.stButton>button:hover{ transform:translateY(-1px); box-shadow:var(--shadow); }
button[kind="primary"]{ background:var(--ind)!important; color:#fff!important; border-color:transparent!important; }

.section-title{ font-size:16px; font-weight:900; letter-spacing:.2px; color:var(--ink); margin-bottom:8px; }
.small{ color:var(--muted); font-size:12px; }
.table-card .stDataFrame{ border-radius:12px; overflow:hidden; box-shadow: var(--shadow-sm); }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
CSV_PATH = Path("approval_tracker_dummy.csv")  # change to your real tracker

# ------------------------------------------------------------
# SESSION / USER CONTEXT
# ------------------------------------------------------------
current_user = st.session_state.get("user_email", "sda@company.com")
current_role = st.session_state.get("user_role", "SDA")  # "SDA"|"DataGuild"|"DigitalGuild"|"ETIDM"

# Fast navigate: if View was clicked last run, jump immediately to Feedback page
if "navigate_to_feedback" not in st.session_state:
    st.session_state.navigate_to_feedback = False
if st.session_state.navigate_to_feedback:
    st.session_state.navigate_to_feedback = False
    st.switch_page("app_pages/Feedback_Page.py")

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
if not CSV_PATH.exists():
    st.error(f"Tracker file not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Ensure columns exist
required = [
    ("Sanction_ID",""), ("Value",0.0), ("Overall_status","Submitted"), ("is_submitter",1),
    ("is_in_SDA",0), ("SDA_status","Pending"), ("SDA_assigned_to",""), ("SDA_decision_at",""),
    ("is_in_data_guild",0), ("data_guild_status","Pending"), ("data_guild_assigned_to",""), ("data_guild_decision_at",""),
    ("is_in_digital_guild",0), ("digital_guild_status","Pending"), ("digital_guild_assigned_to",""), ("digital_guild_decision_at",""),
    ("is_in_etidm",0), ("etidm_status","Pending"), ("etidm_assigned_to",""), ("etidm_decision_at",""),
]
for c, d in required:
    if c not in df.columns:
        df[c] = d

# Register in DuckDB
con = duckdb.connect()
con.register("approval", df)

# ------------------------------------------------------------
# STAGE HELPERS
# ------------------------------------------------------------
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
STAGE_MAP = {
    "SDA":          {"flag":"is_in_SDA","status":"SDA_status","assigned":"SDA_assigned_to","decision":"SDA_decision_at"},
    "DataGuild":    {"flag":"is_in_data_guild","status":"data_guild_status","assigned":"data_guild_assigned_to","decision":"data_guild_decision_at"},
    "DigitalGuild": {"flag":"is_in_digital_guild","status":"digital_guild_status","assigned":"digital_guild_assigned_to","decision":"digital_guild_decision_at"},
    "ETIDM":        {"flag":"is_in_etidm","status":"etidm_status","assigned":"etidm_assigned_to","decision":"etidm_decision_at"},
}

def stage_cols(role):
    m = STAGE_MAP[role]
    return m["flag"], m["status"], m["assigned"], m["decision"]

def prev_role(role):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i-1] if i > 0 else None

def next_role(role):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i+1] if i < len(ROLE_FLOW)-1 else None

def flag_true_sql(col):
    return f"""
    CASE
      WHEN LOWER(CAST({col} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
      WHEN TRY_CAST({col} AS BIGINT) = 1 THEN TRUE
      ELSE FALSE
    END
    """

def visibility_filter(role):
    """Main lists show only items routed to the current role:
       SDA: items in SDA
       Others: prev stage Approved & decided, and current flag TRUE
    """
    if role == "SDA":
        return f"{flag_true_sql('is_in_SDA')} = TRUE"
    p = prev_role(role)
    p_flag, p_status, _, p_decision = stage_cols(p)
    cur_flag, _, _, _ = stage_cols(role)
    return (
        f"CAST({p_status} AS VARCHAR) = 'Approved' "
        f"AND TRY_CAST({p_decision} AS TIMESTAMP) IS NOT NULL "
        f"AND {flag_true_sql(cur_flag)} = TRUE"
    )

def set_stage_flags(df, ids, stage):
    all_flags = [v["flag"] for v in STAGE_MAP.values()]
    mask = df["Sanction_ID"].astype(str).isin([str(i) for i in ids])
    # Turn OFF all
    for f in all_flags:
        df.loc[mask, f] = 0
    # Turn ON current + reset status/assignee
    df.loc[mask, STAGE_MAP[stage]["flag"]] = 1
    df.loc[mask, STAGE_MAP[stage]]["status"] = "Pending"
    df.loc[mask, STAGE_MAP[stage]]["assigned"] = ""
    if stage == "SDA":
        df.loc[mask, "is_submitter"] = 0

# ------------------------------------------------------------
# HEADER
# ------------------------------------------------------------
st.markdown(f"""
<div class="header">
  <div>
    <div class="brand">Sanction Approver Dashboard</div>
    <div class="small">Signed in as <b>{current_user}</b> · Role: <b>{current_role}</b></div>
  </div>
  <div class="badge primary">Sequential flow: SDA → Data Guild → Digital Guild → ETIDM</div>
</div>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# DATASETS BY ROLE (type-safe)
# ------------------------------------------------------------
flag_col, status_col, assigned_col, decision_col = stage_cols(current_role)
vf = visibility_filter(current_role)

pending_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {vf}
      AND COALESCE(CAST({status_col} AS VARCHAR),'Pending') IN ('Pending','In Progress')
""").df()

approved_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {vf}
      AND CAST({status_col} AS VARCHAR) = 'Approved'
      AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
""").df()

nr = next_role(current_role)
if nr:
    nr_flag, nr_status, _, _ = stage_cols(nr)
    awaiting_df = con.execute(f"""
        SELECT * FROM approval
        WHERE {vf}
          AND CAST({status_col} AS VARCHAR) = 'Approved'
          AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(nr_flag)} = TRUE
          AND COALESCE(CAST({nr_status} AS VARCHAR),'Pending') = 'Pending'
    """).df()
else:
    awaiting_df = pending_df.iloc[0:0].copy()

# ------------------------------------------------------------
# KPI CARDS
# ------------------------------------------------------------
k1,k2,k3,k4 = st.columns(4)
with k1: st.markdown(f'<div class="kpi ind"><div class="val">{len(pending_df)}</div><div class="lab">Pending</div></div>', unsafe_allow_html=True)
with k2: st.markdown(f'<div class="kpi ok"><div class="val">{len(approved_df)}</div><div class="lab">Approved</div></div>', unsafe_allow_html=True)
with k3: st.markdown(f'<div class="kpi warn"><div class="val">{len(awaiting_df)}</div><div class="lab">Awaiting Others</div></div>', unsafe_allow_html=True)
with k4: st.markdown(f'<div class="kpi err"><div class="val">{len(df)}</div><div class="lab">Total</div></div>', unsafe_allow_html=True)

st.divider()

# ------------------------------------------------------------
# PENDING LIST + VIEW
# ------------------------------------------------------------
st.markdown('<div class="section-title">Pending in Current Stage</div>', unsafe_allow_html=True)
if not pending_df.empty:
    for _, r in pending_df.iterrows():
        c1, c2 = st.columns([6,1])
        with c1:
            st.markdown(f"""
            <div class="row-item">
              <div>
                <div><b>{r['Sanction_ID']}</b> · Value: {r['Value']}</div>
                <div class="meta">Status: {r[status_col]}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            if st.button("View →", key=f"view_{r['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
                st.session_state.navigate_to_feedback = True
                st.rerun()
else:
    st.info("No pending sanctions found for this stage.")

st.divider()

# ------------------------------------------------------------
# INTAKE (STRICT)
# ------------------------------------------------------------
st.markdown('<div class="section-title">Intake</div>', unsafe_allow_html=True)

if current_role == "SDA":
    backlog = con.execute(f"""
        SELECT * FROM approval
        WHERE TRY_CAST(is_submitter AS BIGINT) = 1
          AND {flag_true_sql('is_in_SDA')} = FALSE
    """).df()
else:
    p = prev_role(current_role)
    p_flag, p_status, _, p_decision = stage_cols(p)
    cur_flag, cur_status, _, _ = stage_cols(current_role)
    backlog = con.execute(f"""
        SELECT * FROM approval
        WHERE CAST({p_status} AS VARCHAR) = 'Approved'
          AND TRY_CAST({p_decision} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(cur_flag)} = FALSE
    """).df()

if backlog.empty:
    st.info("No items available for intake.")
else:
    st.markdown('<div class="card table-card">', unsafe_allow_html=True)
    st.dataframe(backlog[["Sanction_ID","Value","Overall_status"]], use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

    selected = st.multiselect(
        "Select Sanction_IDs to pull into your stage",
        backlog["Sanction_ID"].astype(str).tolist(),
    )
    cA, cB = st.columns([1,4])
    with cA:
        if st.button(f"Move selected to {current_role}", type="primary", use_container_width=True):
            if selected:
                set_stage_flags(df, selected, current_role)
                df.to_csv(CSV_PATH, index=False)
                # Refresh duckdb binding so queries reflect updates on rerun
                try: con.unregister("approval")
                except Exception: pass
                con.register("approval", df)
                st.success(f"Moved {len(selected)} sanction(s) to {current_role}")
                st.rerun()
    with cB:
        st.caption("After intake, items will appear above in your Pending section.")


























# ============================================================
# app_pages/SanctionApproverDashboard.py
# Styled, strict stage control, type-safe DuckDB, View→Feedback
# ============================================================
import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# ------------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide")

# -------------------  Global CSS  ---------------------------
st.markdown("""
<style>
:root{
  --bg:#f7f8fb;
  --card:#ffffff;
  --ink:#0f172a;
  --muted:#64748b;
  --ring:#e5e7eb;
  --shadow:0 10px 24px rgba(0,0,0,.06);
  --shadow-sm:0 4px 12px rgba(0,0,0,.05);
  --radius:16px;

  --ind:#4f46e5; --ind-weak:#eef2ff;
  --ok:#10b981;  --ok-weak:#ecfdf5;
  --warn:#f59e0b;--warn-weak:#fff7ed;
  --err:#ef4444; --err-weak:#fef2f2;
}

html, body, .block-container{ background:var(--bg); }
.block-container { padding-top: 1rem; }

.header{
  display:flex; align-items:center; justify-content:space-between;
  padding:18px 20px; background:#fff; border:1px solid var(--ring);
  border-radius:var(--radius); box-shadow:var(--shadow-sm); margin-bottom:10px;
}
.brand{ font-weight:900; letter-spacing:.2px; font-size:22px; color:var(--ink); }
.badge{
  display:inline-flex; align-items:center; gap:8px; padding:6px 12px;
  border-radius:999px; font-weight:800; font-size:12px; border:1px solid var(--ring);
}
.badge.primary{ background:var(--ind-weak); color:#312e81; border-color:#c7d2fe; }
.badge.ok{ background:var(--ok-weak); color:#065f46; border-color:#bef7dc; }
.badge.warn{ background:var(--warn-weak); color:#7c4a02; border-color:#ffe0b2; }
.badge.err{ background:var(--err-weak); color:#7f1d1d; border-color:#fecaca; }

.kpi{
  background:#fff; border:1px solid var(--ring); border-radius:18px; box-shadow:var(--shadow-sm);
  padding:18px; text-align:center; position:relative; overflow:hidden;
}
.kpi .val{ font-size:30px; font-weight:900; color:var(--ink); }
.kpi .lab{ margin-top:10px; font-weight:800; font-size:14px; }
.kpi.ind{ background:linear-gradient(180deg,#fff, #fafaff); }
.kpi.ok{ background:linear-gradient(180deg,#fff, #f8fffb); }
.kpi.warn{ background:linear-gradient(180deg,#fff, #fff8ed); }
.kpi.err{ background:linear-gradient(180deg,#fff, #fff6f6); }

.card{
  background:#fff; border:1px solid var(--ring); border-radius:16px; padding:14px 16px; box-shadow:var(--shadow-sm);
}

.row-item{
  display:flex; align-items:center; justify-content:space-between;
  border:1px solid var(--ring); border-radius:14px; padding:12px 14px; background:#fff; box-shadow:var(--shadow-sm);
}
.row-item .meta{ color:var(--muted); font-size:13px; margin-top:4px; }
.row-item + .row-item{ margin-top:10px; }

.stButton>button{
  border-radius:12px; padding:10px 16px; font-weight:800;
  border:1px solid var(--ring); background:#fff; color:var(--ink);
  transition:.16s ease; box-shadow:var(--shadow-sm);
}
.stButton>button:hover{ transform:translateY(-1px); box-shadow:var(--shadow); }
button[kind="primary"]{ background:var(--ind)!important; color:#fff!important; border-color:transparent!important; }

.section-title{ font-size:16px; font-weight:900; letter-spacing:.2px; color:var(--ink); margin-bottom:8px; }
.small{ color:var(--muted); font-size:12px; }
.table-card .stDataFrame{ border-radius:12px; overflow:hidden; box-shadow: var(--shadow-sm); }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
CSV_PATH = Path("approval_tracker_dummy.csv")  # change to your real tracker

# ------------------------------------------------------------
# SESSION / USER CONTEXT
# ------------------------------------------------------------
current_user = st.session_state.get("user_email", "sda@company.com")
current_role = st.session_state.get("user_role", "SDA")  # "SDA"|"DataGuild"|"DigitalGuild"|"ETIDM"

# Fast navigate: if View was clicked last run, jump immediately to Feedback page
if "navigate_to_feedback" not in st.session_state:
    st.session_state.navigate_to_feedback = False
if st.session_state.navigate_to_feedback:
    st.session_state.navigate_to_feedback = False
    st.switch_page("app_pages/Feedback_Page.py")

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
if not CSV_PATH.exists():
    st.error(f"Tracker file not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Ensure columns exist
required = [
    ("Sanction_ID",""), ("Value",0.0), ("Overall_status","Submitted"), ("is_submitter",1),
    ("is_in_SDA",0), ("SDA_status","Pending"), ("SDA_assigned_to",""), ("SDA_decision_at",""),
    ("is_in_data_guild",0), ("data_guild_status","Pending"), ("data_guild_assigned_to",""), ("data_guild_decision_at",""),
    ("is_in_digital_guild",0), ("digital_guild_status","Pending"), ("digital_guild_assigned_to",""), ("digital_guild_decision_at",""),
    ("is_in_etidm",0), ("etidm_status","Pending"), ("etidm_assigned_to",""), ("etidm_decision_at",""),
]
for c, d in required:
    if c not in df.columns:
        df[c] = d

# Register in DuckDB
con = duckdb.connect()
con.register("approval", df)

# ------------------------------------------------------------
# STAGE HELPERS
# ------------------------------------------------------------
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
STAGE_MAP = {
    "SDA":          {"flag":"is_in_SDA","status":"SDA_status","assigned":"SDA_assigned_to","decision":"SDA_decision_at"},
    "DataGuild":    {"flag":"is_in_data_guild","status":"data_guild_status","assigned":"data_guild_assigned_to","decision":"data_guild_decision_at"},
    "DigitalGuild": {"flag":"is_in_digital_guild","status":"digital_guild_status","assigned":"digital_guild_assigned_to","decision":"digital_guild_decision_at"},
    "ETIDM":        {"flag":"is_in_etidm","status":"etidm_status","assigned":"etidm_assigned_to","decision":"etidm_decision_at"},
}

def stage_cols(role):
    m = STAGE_MAP[role]
    return m["flag"], m["status"], m["assigned"], m["decision"]

def prev_role(role):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i-1] if i > 0 else None

def next_role(role):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i+1] if i < len(ROLE_FLOW)-1 else None

def flag_true_sql(col):
    return f"""
    CASE
      WHEN LOWER(CAST({col} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
      WHEN TRY_CAST({col} AS BIGINT) = 1 THEN TRUE
      ELSE FALSE
    END
    """

def visibility_filter(role):
    """Main lists show only items routed to the current role:
       SDA: items in SDA
       Others: prev stage Approved & decided, and current flag TRUE
    """
    if role == "SDA":
        return f"{flag_true_sql('is_in_SDA')} = TRUE"
    p = prev_role(role)
    p_flag, p_status, _, p_decision = stage_cols(p)
    cur_flag, _, _, _ = stage_cols(role)
    return (
        f"CAST({p_status} AS VARCHAR) = 'Approved' "
        f"AND TRY_CAST({p_decision} AS TIMESTAMP) IS NOT NULL "
        f"AND {flag_true_sql(cur_flag)} = TRUE"
    )

def set_stage_flags(df, ids, stage):
    all_flags = [v["flag"] for v in STAGE_MAP.values()]
    mask = df["Sanction_ID"].astype(str).isin([str(i) for i in ids])
    # Turn OFF all
    for f in all_flags:
        df.loc[mask, f] = 0
    # Turn ON current + reset status/assignee
    df.loc[mask, STAGE_MAP[stage]["flag"]] = 1
    df.loc[mask, STAGE_MAP[stage]]["status"] = "Pending"
    df.loc[mask, STAGE_MAP[stage]]["assigned"] = ""
    if stage == "SDA":
        df.loc[mask, "is_submitter"] = 0

# ------------------------------------------------------------
# HEADER
# ------------------------------------------------------------
st.markdown(f"""
<div class="header">
  <div>
    <div class="brand">Sanction Approver Dashboard</div>
    <div class="small">Signed in as <b>{current_user}</b> · Role: <b>{current_role}</b></div>
  </div>
  <div class="badge primary">Sequential flow: SDA → Data Guild → Digital Guild → ETIDM</div>
</div>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# DATASETS BY ROLE (type-safe)
# ------------------------------------------------------------
flag_col, status_col, assigned_col, decision_col = stage_cols(current_role)
vf = visibility_filter(current_role)

pending_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {vf}
      AND COALESCE(CAST({status_col} AS VARCHAR),'Pending') IN ('Pending','In Progress')
""").df()

approved_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {vf}
      AND CAST({status_col} AS VARCHAR) = 'Approved'
      AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
""").df()

nr = next_role(current_role)
if nr:
    nr_flag, nr_status, _, _ = stage_cols(nr)
    awaiting_df = con.execute(f"""
        SELECT * FROM approval
        WHERE {vf}
          AND CAST({status_col} AS VARCHAR) = 'Approved'
          AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(nr_flag)} = TRUE
          AND COALESCE(CAST({nr_status} AS VARCHAR),'Pending') = 'Pending'
    """).df()
else:
    awaiting_df = pending_df.iloc[0:0].copy()

# ------------------------------------------------------------
# KPI CARDS
# ------------------------------------------------------------
k1,k2,k3,k4 = st.columns(4)
with k1: st.markdown(f'<div class="kpi ind"><div class="val">{len(pending_df)}</div><div class="lab">Pending</div></div>', unsafe_allow_html=True)
with k2: st.markdown(f'<div class="kpi ok"><div class="val">{len(approved_df)}</div><div class="lab">Approved</div></div>', unsafe_allow_html=True)
with k3: st.markdown(f'<div class="kpi warn"><div class="val">{len(awaiting_df)}</div><div class="lab">Awaiting Others</div></div>', unsafe_allow_html=True)
with k4: st.markdown(f'<div class="kpi err"><div class="val">{len(df)}</div><div class="lab">Total</div></div>', unsafe_allow_html=True)

st.divider()

# ------------------------------------------------------------
# PENDING LIST + VIEW
# ------------------------------------------------------------
st.markdown('<div class="section-title">Pending in Current Stage</div>', unsafe_allow_html=True)
if not pending_df.empty:
    for _, r in pending_df.iterrows():
        c1, c2 = st.columns([6,1])
        with c1:
            st.markdown(f"""
            <div class="row-item">
              <div>
                <div><b>{r['Sanction_ID']}</b> · Value: {r['Value']}</div>
                <div class="meta">Status: {r[status_col]}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            if st.button("View →", key=f"view_{r['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
                st.session_state.navigate_to_feedback = True
                st.rerun()
else:
    st.info("No pending sanctions found for this stage.")

st.divider()

# ------------------------------------------------------------
# INTAKE (STRICT)
# ------------------------------------------------------------
st.markdown('<div class="section-title">Intake</div>', unsafe_allow_html=True)

if current_role == "SDA":
    backlog = con.execute(f"""
        SELECT * FROM approval
        WHERE TRY_CAST(is_submitter AS BIGINT) = 1
          AND {flag_true_sql('is_in_SDA')} = FALSE
    """).df()
else:
    p = prev_role(current_role)
    p_flag, p_status, _, p_decision = stage_cols(p)
    cur_flag, cur_status, _, _ = stage_cols(current_role)
    backlog = con.execute(f"""
        SELECT * FROM approval
        WHERE CAST({p_status} AS VARCHAR) = 'Approved'
          AND TRY_CAST({p_decision} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(cur_flag)} = FALSE
    """).df()

if backlog.empty:
    st.info("No items available for intake.")
else:
    st.markdown('<div class="card table-card">', unsafe_allow_html=True)
    st.dataframe(backlog[["Sanction_ID","Value","Overall_status"]], use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

    selected = st.multiselect(
        "Select Sanction_IDs to pull into your stage",
        backlog["Sanction_ID"].astype(str).tolist(),
    )
    cA, cB = st.columns([1,4])
    with cA:
        if st.button(f"Move selected to {current_role}", type="primary", use_container_width=True):
            if selected:
                set_stage_flags(df, selected, current_role)
                df.to_csv(CSV_PATH, index=False)
                # Refresh duckdb binding so queries reflect updates on rerun
                try: con.unregister("approval")
                except Exception: pass
                con.register("approval", df)
                st.success(f"Moved {len(selected)} sanction(s) to {current_role}")
                st.rerun()
    with cB:
        st.caption("After intake, items will appear above in your Pending section.")
