# ============================================================
# SanctionApproverDashboard.py
# ============================================================
import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# ------------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide")

CSV_PATH = Path("approval_tracker_dummy.csv")  # change to actual tracker if needed

# ------------------------------------------------------------
# SESSION / USER CONTEXT
# ------------------------------------------------------------
current_user = st.session_state.get("user_email", "sda@company.com")
current_role = st.session_state.get("user_role", "SDA")  # SDA | DataGuild | DigitalGuild | ETIDM

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

# ------------------------------------------------------------
# ENSURE ALL REQUIRED COLUMNS EXIST
# ------------------------------------------------------------
required = [
    ("Sanction_ID", ""), ("Value", 0.0), ("Overall_status", "Submitted"), ("is_submitter", 1),
    ("is_in_SDA", 0), ("SDA_status", "Pending"), ("SDA_assigned_to", ""), ("SDA_decision_at", ""),
    ("is_in_data_guild", 0), ("data_guild_status", "Pending"), ("data_guild_assigned_to", ""), ("data_guild_decision_at", ""),
    ("is_in_digital_guild", 0), ("digital_guild_status", "Pending"), ("digital_guild_assigned_to", ""), ("digital_guild_decision_at", ""),
    ("is_in_etidm", 0), ("etidm_status", "Pending"), ("etidm_assigned_to", ""), ("etidm_decision_at", "")
]
for c, d in required:
    if c not in df.columns:
        df[c] = d

# ------------------------------------------------------------
# REGISTER TO DUCKDB (IN-MEMORY)
# ------------------------------------------------------------
con = duckdb.connect()
con.register("approval", df)

# ------------------------------------------------------------
# STAGE MAPPING
# ------------------------------------------------------------
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
STAGE_MAP = {
    "SDA": {"flag": "is_in_SDA", "status": "SDA_status", "assigned": "SDA_assigned_to", "decision": "SDA_decision_at"},
    "DataGuild": {"flag": "is_in_data_guild", "status": "data_guild_status", "assigned": "data_guild_assigned_to", "decision": "data_guild_decision_at"},
    "DigitalGuild": {"flag": "is_in_digital_guild", "status": "digital_guild_status", "assigned": "digital_guild_assigned_to", "decision": "digital_guild_decision_at"},
    "ETIDM": {"flag": "is_in_etidm", "status": "etidm_status", "assigned": "etidm_assigned_to", "decision": "etidm_decision_at"},
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

# ------------------------------------------------------------
# VISIBILITY FILTER
# ------------------------------------------------------------
def visibility_filter(role):
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

# ------------------------------------------------------------
# UPDATE STAGE FLAGS
# ------------------------------------------------------------
def set_stage_flags(df, ids, stage):
    all_flags = [v["flag"] for v in STAGE_MAP.values()]
    all_status = [v["status"] for v in STAGE_MAP.values()]
    mask = df["Sanction_ID"].astype(str).isin([str(i) for i in ids])
    for f in all_flags:
        df.loc[mask, f] = 0
    df.loc[mask, STAGE_MAP[stage]["flag"]] = 1
    df.loc[mask, STAGE_MAP[stage]["status"]] = "Pending"
    df.loc[mask, STAGE_MAP[stage]["assigned"]] = None
    if stage == "SDA":
        df.loc[mask, "is_submitter"] = 0

# ------------------------------------------------------------
# DASHBOARD UI
# ------------------------------------------------------------
st.title("📊 Sanction Approver Dashboard")
st.markdown(f"### Welcome, {current_user} ({current_role})")

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
""").df()

next_r = next_role(current_role)
if next_r:
    next_flag, next_status, _, _ = stage_cols(next_r)
    awaiting_df = con.execute(f"""
        SELECT * FROM approval
        WHERE {vf}
          AND CAST({status_col} AS VARCHAR) = 'Approved'
          AND {flag_true_sql(next_flag)} = TRUE
          AND COALESCE(CAST({next_status} AS VARCHAR),'Pending') = 'Pending'
    """).df()
else:
    awaiting_df = pending_df.iloc[0:0].copy()

# KPI Display
cols = st.columns(4)
metrics = [("Pending", len(pending_df), "#E6F4FF"), ("Approved", len(approved_df), "#E7F8E6"),
           ("Awaiting Others", len(awaiting_df), "#FFE8E8"), ("Total Items", len(df), "#FFF4E5")]
for (t, v, bg), c in zip(metrics, cols):
    with c:
        st.markdown(
            f"""<div style="background:{bg};border-radius:12px;
            padding:15px;text-align:center;box-shadow:0 2px 5px rgba(0,0,0,0.08)">
            <div style='font-size:28px;font-weight:700'>{v}</div>
            <div style='font-size:16px;font-weight:600'>{t}</div></div>""",
            unsafe_allow_html=True,
        )

st.divider()

# ------------------------------------------------------------
# PENDING TABLE
# ------------------------------------------------------------
st.subheader(f"Pending in {current_role}")
if not pending_df.empty:
    for _, r in pending_df.iterrows():
        c1, c2 = st.columns([6, 1])
        with c1:
            st.markdown(f"**{r['Sanction_ID']}** — Value: {r['Value']} | Status: {r[status_col]}")
        with c2:
            if st.button("View →", key=f"view_{r['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
                st.session_state.navigate_to_feedback = True
                st.rerun()
else:
    st.info("No pending sanctions found.")

st.divider()

# ------------------------------------------------------------
# INTAKE BLOCK
# ------------------------------------------------------------
st.subheader(f"Intake ({current_role})")
if current_role == "SDA":
    backlog = con.execute(f"""
        SELECT * FROM approval
        WHERE TRY_CAST(is_submitter AS BIGINT) = 1
          AND {flag_true_sql('is_in_SDA')} = FALSE
    """).df()
else:
    prev_r = prev_role(current_role)
    p_flag, p_status, _, p_decision = stage_cols(prev_r)
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
    st.dataframe(backlog[["Sanction_ID", "Value", "Overall_status"]], use_container_width=True)
    intake_ids = st.multiselect("Select Sanction_IDs to move", backlog["Sanction_ID"].astype(str).tolist())
    if st.button(f"Move selected to {current_role}"):
        set_stage_flags(df, intake_ids, current_role)
        df.to_csv(CSV_PATH, index=False)
        st.success(f"Moved {len(intake_ids)} sanction(s) to {current_role}")
        st.rerun()

st.caption(f"Logged in as {current_user} ({current_role})")




























# ============================================================
# Feedback_Page.py
# ============================================================
import os
from datetime import datetime
import pandas as pd
import streamlit as st

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
TRACKER_PATH = os.getenv("APPROVER_TRACKER_PATH", "approval_tracker_dummy.csv")

STAGES = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
META = {
    "SDA": {"flag": "is_in_SDA", "status": "SDA_status", "assigned": "SDA_assigned_to", "decision": "SDA_decision_at"},
    "DataGuild": {"flag": "is_in_data_guild", "status": "data_guild_status", "assigned": "data_guild_assigned_to", "decision": "data_guild_decision_at"},
    "DigitalGuild": {"flag": "is_in_digital_guild", "status": "digital_guild_status", "assigned": "digital_guild_assigned_to", "decision": "digital_guild_decision_at"},
    "ETIDM": {"flag": "is_in_etidm", "status": "etidm_status", "assigned": "etidm_assigned_to", "decision": "etidm_decision_at"},
}

st.set_page_config(page_title="Feedback Page", layout="wide")

def now_iso(): return datetime.now().isoformat(timespec="seconds")

def next_stage(stage):
    i = STAGES.index(stage)
    return STAGES[i+1] if i < len(STAGES)-1 else None

def infer_stage(row):
    for s, meta in META.items():
        if str(row.get(meta["flag"], "")).lower() in ("1","true","t","yes","y"):
            return s
    return "SDA"

# ------------------------------------------------------------
# LOAD TRACKER
# ------------------------------------------------------------
if not os.path.exists(TRACKER_PATH):
    st.error("Tracker not found.")
    st.stop()

df = pd.read_csv(TRACKER_PATH)
df["Sanction_ID"] = df["Sanction_ID"].astype(str)

sid = st.session_state.get("selected_sanction_id")
if not sid:
    st.warning("No sanction selected. Go back and click 'View'.")
    st.stop()

row = df[df["Sanction_ID"] == str(sid)]
if row.empty:
    st.error(f"Sanction {sid} not found.")
    st.stop()

row = row.iloc[0]
stage = infer_stage(row)

st.title("🧾 Sanction Feedback Page")
st.markdown(f"### Sanction ID: `{sid}` | Current Stage: **{stage}**")

c1, c2, c3, c4 = st.columns(4)
with c1: st.metric("Value", row.get("Value", 0))
with c2: st.metric("Overall Status", row.get("Overall_status", "Submitted"))
with c3: st.metric("Stage", stage)
with c4: st.metric("Time", now_iso())

st.divider()

meta = META[stage]

with st.form("decision_form"):
    choice = st.radio("Decision", ["Approve ✅", "Reject ⛔", "Request changes ✍️"], horizontal=True)
    assign_to = st.text_input("Assign To (optional)", value=str(row.get(meta["assigned"], "")))
    comments = st.text_area("Comments (optional)")
    submitted = st.form_submit_button("Submit Decision")

if submitted:
    mask = df["Sanction_ID"] == sid
    decision_time = now_iso()
    new_status = "Approved" if "Approve" in choice else ("Rejected" if "Reject" in choice else "Changes requested")

    df.loc[mask, meta["status"]] = new_status
    df.loc[mask, meta["assigned"]] = assign_to
    df.loc[mask, meta["decision"]] = decision_time

    if new_status == "Approved":
        nxt = next_stage(stage)
        df.loc[mask, "Overall_status"] = "In progress" if nxt else "Completed"
        if nxt:
            df.loc[mask, "Current Stage"] = nxt
    elif new_status == "Rejected":
        df.loc[mask, "Overall_status"] = "Rejected"
    else:
        df.loc[mask, "Overall_status"] = "Changes requested"

    df.to_csv(TRACKER_PATH, index=False)
    st.success(f"Decision saved: {new_status}")
    st.rerun()

st.divider()
st.subheader("Sanction Record Snapshot")
cols = [c for c in df.columns if "status" in c or "decision" in c or "is_in" in c]
st.dataframe(df[df["Sanction_ID"] == sid][["Sanction_ID"] + cols], use_container_width=True)
