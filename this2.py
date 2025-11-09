# ============================================================
# app_pages/SanctionApproverDashboard.py
# Strict stage flow, Intake→Pending handoff, type-safe filters,
# styled UI, View→Feedback with session handoff.
# ============================================================
import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# ------------------- Page config + CSS ----------------------
st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide")
st.markdown("""
<style>
:root{
  --bg:#f7f8fb; --card:#fff; --ring:#e5e7eb; --ink:#0f172a; --muted:#64748b;
  --ind:#4f46e5; --ok:#10b981; --warn:#f59e0b; --err:#ef4444;
  --shadow:0 8px 20px rgba(0,0,0,.06); --shadow-sm:0 4px 12px rgba(0,0,0,.05);
  --radius:16px;
}
html, body, .block-container{ background:var(--bg); }
.block-container{ padding-top: 1rem; }
.header{ display:flex; align-items:center; justify-content:space-between;
  background:#fff; border:1px solid var(--ring); border-radius:var(--radius);
  padding:16px 20px; box-shadow:var(--shadow-sm); margin-bottom:12px; }
.brand{ font-weight:900; letter-spacing:.2px; font-size:20px; color:var(--ink); }
.small{ color:var(--muted); font-size:12px; }
.kpi{ background:#fff; border:1px solid var(--ring); border-radius:14px; padding:16px; text-align:center; box-shadow:var(--shadow-sm); }
.kpi .val{ font-size:28px; font-weight:900; color:var(--ink); }
.kpi .lab{ margin-top:8px; font-weight:800; font-size:13px; color:#111827; }
.section{ background:#fff; border:1px solid var(--ring); border-radius:16px; padding:14px 16px; box-shadow:var(--shadow-sm); }
.row{ display:flex; align-items:center; justify-content:space-between; gap:10px;
  border:1px solid var(--ring); border-radius:12px; padding:12px 14px; background:#fff; box-shadow:var(--shadow-sm); }
.meta{ color:var(--muted); font-size:12px; }
.stButton>button{ border-radius:12px; padding:9px 14px; font-weight:800; border:1px solid var(--ring);
  background:#fff; color:#111827; transition:.16s ease; box-shadow:var(--shadow-sm); }
.stButton>button:hover{ transform:translateY(-1px); box-shadow:var(--shadow); }
button[kind="primary"]{ background:var(--ind)!important; color:#fff!important; border-color:transparent!important; }
</style>
""", unsafe_allow_html=True)

CSV_PATH = Path("approval_tracker_dummy.csv")

# ------------------- Session / user -------------------------
current_user = st.session_state.get("user_email", "sda@company.com")
current_role = st.session_state.get("user_role", "SDA")  # "SDA"|"DataGuild"|"DigitalGuild"|"ETIDM"

# View→Feedback fast navigation
if "navigate_to_feedback" not in st.session_state:
    st.session_state.navigate_to_feedback = False
if st.session_state.navigate_to_feedback:
    st.session_state.navigate_to_feedback = False
    st.switch_page("app_pages/Feedback_Page.py")

# ------------------- Load tracker --------------------------
if not CSV_PATH.exists():
    st.error(f"Tracker not found at {CSV_PATH.resolve()}")
    st.stop()
df = pd.read_csv(CSV_PATH)

# Ensure required columns exist
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

# ------------------- Register to DuckDB --------------------
con = duckdb.connect()
con.register("approval", df)

# ------------------- Stage helpers -------------------------
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
STAGE_MAP = {
    "SDA":          {"flag":"is_in_SDA","status":"SDA_status","assigned":"SDA_assigned_to","decision":"SDA_decision_at"},
    "DataGuild":    {"flag":"is_in_data_guild","status":"data_guild_status","assigned":"data_guild_assigned_to","decision":"data_guild_decision_at"},
    "DigitalGuild": {"flag":"is_in_digital_guild","status":"digital_guild_status","assigned":"digital_guild_assigned_to","decision":"digital_guild_decision_at"},
    "ETIDM":        {"flag":"is_in_etidm","status":"etidm_status","assigned":"etidm_assigned_to","decision":"etidm_decision_at"},
}
def stage_cols(role): m=STAGE_MAP[role]; return m["flag"], m["status"], m["assigned"], m["decision"]
def prev_role(role): i=ROLE_FLOW.index(role); return ROLE_FLOW[i-1] if i>0 else None
def next_role(role): i=ROLE_FLOW.index(role); return ROLE_FLOW[i+1] if i<len(ROLE_FLOW)-1 else None
def flag_true_sql(col):  # tolerant booleanization
    return f"""
    CASE
      WHEN LOWER(CAST({col} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
      WHEN TRY_CAST({col} AS BIGINT) = 1 THEN TRUE
      ELSE FALSE
    END
    """

# Main lists must show ONLY items already routed (flag TRUE) for this role.
def visibility_filter(role):
    if role == "SDA":
        return f"{flag_true_sql('is_in_SDA')} = TRUE"
    p = prev_role(role)
    _, p_status, _, p_decision = stage_cols(p)
    cur_flag, _, _, _ = stage_cols(role)
    return (
        f"CAST({p_status} AS VARCHAR) = 'Approved' "
        f"AND TRY_CAST({p_decision} AS TIMESTAMP) IS NOT NULL "
        f"AND {flag_true_sql(cur_flag)} = TRUE"
    )

# Intake action must set ONLY current flag ON and reset status to Pending.
def set_stage_flags(df_in: pd.DataFrame, ids, stage: str):
    flags = [v["flag"] for v in STAGE_MAP.values()]
    m = df_in["Sanction_ID"].astype(str).isin([str(x) for x in ids])
    for f in flags: df_in.loc[m, f] = 0
    df_in.loc[m, STAGE_MAP[stage]["flag"]] = 1
    df_in.loc[m, STAGE_MAP[stage]["status"]] = "Pending"
    df_in.loc[m, STAGE_MAP[stage]["assigned"]] = ""
    if stage == "SDA":
        df_in.loc[m, "is_submitter"] = 0

# ------------------- Header -------------------------------
st.markdown(f"""
<div class="header">
  <div>
    <div class="brand">Sanction Approver Dashboard</div>
    <div class="small">Logged in as <b>{current_user}</b> · Role: <b>{current_role}</b></div>
  </div>
  <div class="small">Flow: SDA → DataGuild → DigitalGuild → ETIDM</div>
</div>
""", unsafe_allow_html=True)

# ------------------- Build role-scoped datasets ------------
flag_col, status_col, assigned_col, decision_col = stage_cols(current_role)
vf = visibility_filter(current_role)

# PENDING for current team = routed here (flag TRUE) AND this team's status is Pending/In Progress
pending_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {vf}
      AND COALESCE(CAST({status_col} AS VARCHAR),'Pending') IN ('Pending','In Progress')
""").df()

# APPROVED by current team (for KPI only)
approved_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {vf}
      AND CAST({status_col} AS VARCHAR) = 'Approved'
      AND (CAST(COALESCE({assigned_col},'') AS VARCHAR) = CAST(COALESCE({assigned_col},'') AS VARCHAR) OR TRUE)
""").df()

# AWAITING OTHERS = approved by this team and routed to next team with next status Pending
nr = next_role(current_role)
if nr:
    nr_flag, nr_status, _, _ = stage_cols(nr)
    awaiting_df = con.execute(f"""
        SELECT * FROM approval
        WHERE {vf}
          AND CAST({status_col} AS VARCHAR) = 'Approved'
          AND {flag_true_sql(nr_flag)} = TRUE
          AND COALESCE(CAST({nr_status} AS VARCHAR),'Pending') = 'Pending'
    """).df()
else:
    awaiting_df = pending_df.iloc[0:0].copy()

# ------------------- KPIs -------------------------------
k1, k2, k3, k4 = st.columns(4)
with k1: st.markdown(f'<div class="kpi"><div class="val">{len(pending_df)}</div><div class="lab">Pending ({current_role})</div></div>', unsafe_allow_html=True)
with k2: st.markdown(f'<div class="kpi"><div class="val">{len(approved_df)}</div><div class="lab">Approved ({current_role})</div></div>', unsafe_allow_html=True)
with k3: st.markdown(f'<div class="kpi"><div class="val">{len(awaiting_df)}</div><div class="lab">Awaiting Next Team</div></div>', unsafe_allow_html=True)
with k4: st.markdown(f'<div class="kpi"><div class="val">{len(df)}</div><div class="lab">Total</div></div>', unsafe_allow_html=True)

st.divider()

# ------------------- PENDING list (for this team) --------
st.markdown(f"### Pending in {current_role}")
if not pending_df.empty:
    for _, r in pending_df.iterrows():
        c1, c2 = st.columns([6,1])
        with c1:
            st.markdown(f"""
            <div class="row">
              <div>
                <div><b>{r['Sanction_ID']}</b> · Value: {r['Value']}</div>
                <div class="meta">Status in {current_role}: {r[status_col]}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            if st.button("View →", key=f"view_{r['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
                st.session_state.navigate_to_feedback = True
                st.rerun()
else:
    st.info(f"No pending items for {current_role}.")

st.divider()

# ------------------- INTAKE (strict) ---------------------
st.markdown(f"### Intake ({current_role})")
if current_role == "SDA":
    # Only raw submissions, not in SDA yet
    intake_df = con.execute(f"""
        SELECT * FROM approval
        WHERE TRY_CAST(is_submitter AS BIGINT) = 1
          AND {flag_true_sql('is_in_SDA')} = FALSE
    """).df()
else:
    # Only items approved by previous team, with decision timestamp, not yet routed (flag FALSE) to this team
    p = prev_role(current_role)
    p_flag, p_status, _, p_decision = stage_cols(p)
    cur_flag, cur_status, _, _ = stage_cols(current_role)
    intake_df = con.execute(f"""
        SELECT * FROM approval
        WHERE CAST({p_status} AS VARCHAR) = 'Approved'
          AND TRY_CAST({p_decision} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(cur_flag)} = FALSE
    """).df()

if intake_df.empty:
    st.info("Nothing available to intake.")
else:
    st.dataframe(intake_df[["Sanction_ID","Value","Overall_status"]], use_container_width=True, hide_index=True)
    selected = st.multiselect("Select Sanction_IDs to pull into your stage",
                              intake_df["Sanction_ID"].astype(str).tolist())
    if st.button(f"Move selected to {current_role}", type="primary"):
        if selected:
            set_stage_flags(df, selected, current_role)       # turn ONLY this team's flag ON + set this team's status "Pending"
            df.to_csv(CSV_PATH, index=False)                  # persist
            try: con.unregister("approval")
            except Exception: pass
            con.register("approval", df)                      # refresh view
            st.success(f"Moved {len(selected)} item(s) to {current_role}. They now appear in Pending above.")
            st.rerun()

st.caption(f"Logged in as: {current_user} ({current_role})")





# ============================================================
# app_pages/Feedback_Page.py
# Approve/Reject/Request changes; advance textual "Current Stage"
# Flags for next team are set via Intake (not here).
# ============================================================
import os
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Feedback | Sanctions", layout="wide")

# ------------- CSS -------------
st.markdown("""
<style>
:root{ --ring:#e5e7eb; --shadow:0 8px 20px rgba(0,0,0,.06); --radius:16px; --muted:#64748b; }
.card{ background:#fff; border:1px solid var(--ring); border-radius:var(--radius); padding:16px; box-shadow:var(--shadow); }
.header{ display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }
.kpigrid{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; }
.kpi{ background:#fff; border:1px solid var(--ring); border-radius:14px; padding:14px; box-shadow:0 4px 12px rgba(0,0,0,.05); }
.kpi .lab{ color:var(--muted); font-size:12px; }
.kpi .val{ font-size:20px; font-weight:900; }
.stButton>button{ border-radius:12px; padding:10px 16px; font-weight:800; }
</style>
""", unsafe_allow_html=True)

TRACKER_PATH = os.getenv("APPROVER_TRACKER_PATH", "approval_tracker_dummy.csv")
STAGES = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
META = {
    "SDA":          {"flag":"is_in_SDA","status":"SDA_status","assigned":"SDA_assigned_to","decision":"SDA_decision_at"},
    "DataGuild":    {"flag":"is_in_data_guild","status":"data_guild_status","assigned":"data_guild_assigned_to","decision":"data_guild_decision_at"},
    "DigitalGuild": {"flag":"is_in_digital_guild","status":"digital_guild_status","assigned":"digital_guild_assigned_to","decision":"digital_guild_decision_at"},
    "ETIDM":        {"flag":"is_in_etidm","status":"etidm_status","assigned":"etidm_assigned_to","decision":"etidm_decision_at"},
}

def now_iso(): return datetime.now().isoformat(timespec="seconds")
def next_stage(stage): 
    return STAGES[STAGES.index(stage)+1] if stage in STAGES and STAGES.index(stage)<len(STAGES)-1 else None
def infer_stage(row):
    for s,m in META.items():
        v = str(row.get(m["flag"], "")).lower()
        if v in ("1","true","t","yes","y"): return s
    return (row.get("Current Stage") if pd.notna(row.get("Current Stage","")) else "SDA") or "SDA"

# ------------- Load tracker -------------
if not os.path.exists(TRACKER_PATH):
    st.error(f"Tracker not found at {TRACKER_PATH}")
    st.stop()
df = pd.read_csv(TRACKER_PATH)
if "Sanction_ID" not in df.columns:
    st.error("Tracker missing 'Sanction_ID'.")
    st.stop()
df["Sanction_ID"] = df["Sanction_ID"].astype(str)

sid = st.session_state.get("selected_sanction_id")
if not sid:
    st.warning("No sanction selected. Go back and click **View**.")
    st.stop()
sid = str(sid)

rowq = df[df["Sanction_ID"] == sid]
if rowq.empty:
    st.error(f"Sanction `{sid}` not found.")
    st.stop()
row = rowq.iloc[0]
stage = infer_stage(row)
meta = META[stage]

# ------------- Header + KPIs -------------
st.markdown(f"""
<div class="card header">
  <div><b>Feedback</b><br/><span style="color:#64748b">Sanction ID: {sid}</span></div>
  <div><b>Current Stage:</b> {stage}</div>
</div>
""", unsafe_allow_html=True)
st.markdown('<div class="kpigrid">', unsafe_allow_html=True)
c1,c2,c3,c4 = st.columns(4)
with c1: st.markdown(f'<div class="kpi"><div class="lab">Value</div><div class="val">{row.get("Value",0)}</div></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="kpi"><div class="lab">Overall</div><div class="val">{row.get("Overall_status","Submitted")}</div></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="kpi"><div class="lab">Stage</div><div class="val">{stage}</div></div>', unsafe_allow_html=True)
with c4: st.markdown(f'<div class="kpi"><div class="lab">Now</div><div class="val">{now_iso()}</div></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)
st.divider()

# ------------- Decision form -------------
with st.form("decision_form"):
    decision = st.radio("Decision", ["Approve ✅", "Reject ⛔", "Request changes ✍️"], horizontal=True, index=0)
    assign_to = st.text_input("Assign To (optional)", value=str(row.get(meta["assigned"], "")))
    comments  = st.text_area("Comments (optional)")
    when      = st.text_input("Decision time", value=now_iso())
    submitted = st.form_submit_button("Submit decision", type="primary")

if submitted:
    mask = (df["Sanction_ID"] == sid)
    new_status = "Approved" if "Approve" in decision else ("Rejected" if "Reject" in decision else "Changes requested")

    # Update ONLY current stage fields
    df.loc[mask, meta["status"]]   = new_status
    df.loc[mask, meta["assigned"]] = assign_to
    df.loc[mask, meta["decision"]] = when

    if new_status == "Approved":
        nxt = next_stage(stage)
        df.loc[mask, "Overall_status"] = "In progress" if nxt else "Completed"
        if nxt:
            # advance textual stage; the next team still must Intake to bring it to their Pending
            df.loc[mask, "Current Stage"] = nxt
    elif new_status == "Rejected":
        df.loc[mask, "Overall_status"] = "Rejected"
    else:
        df.loc[mask, "Overall_status"] = "Changes requested"

    df.to_csv(TRACKER_PATH, index=False)
    st.success(f"Saved decision: {new_status}")
    st.rerun()

# ------------- Snapshot -------------
st.divider()
show_cols = [
    "Sanction_ID","Overall_status","Current Stage",
    "is_in_SDA","SDA_status","SDA_assigned_to","SDA_decision_at",
    "is_in_data_guild","data_guild_status","data_guild_assigned_to","data_guild_decision_at",
    "is_in_digital_guild","digital_guild_status","digital_guild_assigned_to","digital_guild_decision_at",
    "is_in_etidm","etidm_status","etidm_assigned_to","etidm_decision_at",
]
show_cols = [c for c in show_cols if c in df.columns]
st.dataframe(df.loc[df["Sanction_ID"] == sid, show_cols], use_container_width=True, hide_index=True)



