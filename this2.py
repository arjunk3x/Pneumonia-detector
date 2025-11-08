# app_pages/Feedback_Page.py

import os
from datetime import datetime
import pandas as pd
import streamlit as st

# =========================
# CONFIG – tweak these if needed
# =========================
SANCTIONS_PATH       = os.getenv("SANCTIONS_PATH", "sanctions_data.csv")
APPROVER_TRACKER_PATH = os.getenv("APPROVER_TRACKER_PATH", "approver_tracker.csv")
APPROVAL_DUMMY_PATH   = os.getenv("APPROVAL_DUMMY_PATH", "approval_dummy.csv")

STAGES = ["SDA", "Data Guild", "DigitalGuild", "ETIDM"]   # approval flow order
STAGE_KEYS = {  # maps stage to columns used in approver_tracker
    "SDA": {
        "flag": "is_in_SDA",
        "status": "SDA_status",
        "assigned_to": "SDA_assigned_to",
        "decision_at": "SDA_decision_at",
    },
    "Data Guild": {
        "flag": "is_in_data_guild",
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "DigitalGuild": {
        "flag": "is_in_digital_guild",
        "status": "digital_guild_status",
        "assigned_to": "digital_guild_assigned_to",
        "decision_at": "digital_guild_decision_at",
    },
    "ETIDM": {
        "flag": "is_in_etidm",
        "status": "etidm_status",
        "assigned_to": "etidm_assigned_to",
        "decision_at": "etidm_decision_at",
    },
}

# =========================
# BASIC THEME
# =========================
st.set_page_config(page_title="Feedback Page", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
.badge {display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;}
.badge.ok{background:#e8f5e9;color:#1b5e20}
.badge.warn{background:#fff8e1;color:#7b5e00}
.badge.danger{background:#ffebee;color:#b71c1c}
.card{padding:14px 16px;border:1px solid #ececec;border-radius:14px;background:linear-gradient(180deg,#fff, #fafafa);
      box-shadow:0 1px 2px rgba(0,0,0,.05);}
.kv{font-size:13px;color:#6b7280}
.kv b{color:#111827}
.flow-step{padding:10px 12px;border-radius:12px;border:1px dashed #e5e7eb;background:#fff}
.flow-step.active{border-color:#4f46e5;background:#eef2ff}
.flow-step.done{border-color:#10b981;background:#ecfdf5}
.flow-step .title{font-weight:700}
.flow-arrow{font-size:22px;color:#9ca3af;padding:0 8px}
</style>
""", unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)

def _write_csv(df: pd.DataFrame, path: str):
    # Make sure directory exists
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)

def _get_param_safe(name: str):
    """Streamlit-compatible query param getter with backwards compatibility."""
    try:
        # Newer Streamlit
        qp = getattr(st, "query_params", None)
        if qp:
            v = qp.get(name)
            if isinstance(v, list):
                return v[0] if v else None
            return v
        # Older Streamlit
        q = st.experimental_get_query_params()
        return q.get(name, [None])[0]
    except Exception:
        return None

def _pill_class(txt: str) -> str:
    if not txt:
        return "ok"
    t = str(txt).lower()
    if any(k in t for k in ["reject", "block", "high", "critical", "risk 3", "risk3"]):
        return "danger"
    if any(k in t for k in ["pending", "review", "medium", "risk 2", "risk2", "request"]):
        return "warn"
    return "ok"

def _fmt_money(val, currency="GBP"):
    if pd.isna(val):
        return "-"
    try:
        v = float(val)
        return f"{currency} {v:,.0f}"
    except Exception:
        return str(val)

def _now_iso():
    return datetime.now().isoformat(timespec="seconds")

def _next_stage(current: str) -> str | None:
    if current not in STAGES:
        return None
    i = STAGES.index(current)
    return STAGES[i+1] if i + 1 < len(STAGES) else None

def _stage_status_row(stage: str, tracker_row: pd.Series):
    keys = STAGE_KEYS[stage]
    status = str(tracker_row.get(keys["status"], "Pending"))
    assigned = tracker_row.get(keys["assigned_to"], "")
    decided = tracker_row.get(keys["decision_at"], "")
    cls = _pill_class(status)
    return f"""
      <div class="flow-step {'active' if str(tracker_row.get('Current Stage',''))==stage else ('done' if status.lower() in ['approved','rejected'] else '')}">
        <div class="title">{stage}</div>
        <div class="kv">Status: <span class="badge {cls}">{status}</span></div>
        <div class="kv">Assigned to: <b>{assigned if pd.notna(assigned) and assigned!='' else '-'}</b></div>
        <div class="kv">Decision at: <b>{decided if pd.notna(decided) and decided!='' else '-'}</b></div>
      </div>
    """

# =========================
# LOGIN GATE (if you use one)
# =========================
if "logged_in" in st.session_state and not st.session_state.logged_in:
    st.warning("Please login to continue.")
    st.stop()

# =========================
# GET SELECTED SANCTION
# =========================
sid = st.session_state.get("selected_sanction_id") or _get_param_safe("sanction_id")
if not sid:
    st.warning("No sanction selected. Go back to the dashboard and click **View**.")
    st.stop()

# =========================
# LOAD DATA
# =========================
sanctions_df = _read_csv(SANCTIONS_PATH)
tracker_df   = _read_csv(APPROVER_TRACKER_PATH)
dummy_df     = _read_csv(APPROVAL_DUMMY_PATH)  # where we write stage decisions

if sanctions_df.empty:
    st.error(f"`{SANCTIONS_PATH}` not found or empty.")
    st.stop()
if tracker_df.empty:
    st.error(f"`{APPROVER_TRACKER_PATH}` not found or empty.")
    st.stop()

# Normalize keys
for df in (sanctions_df, tracker_df, dummy_df):
    if not df.empty and "Sanction_ID" in df.columns:
        df["Sanction_ID"] = df["Sanction_ID"].astype(str)

sid = str(sid)

# Pull rows
s_row = sanctions_df.loc[sanctions_df["Sanction ID"].astype(str) == sid]
t_row = tracker_df.loc[tracker_df["Sanction_ID"].astype(str) == sid]

if s_row.empty and t_row.empty:
    st.error(f"Sanction ID `{sid}` not found.")
    st.stop()

s_row = s_row.iloc[0] if not s_row.empty else pd.Series(dtype="object")
t_row = t_row.iloc[0] if not t_row.empty else pd.Series(dtype="object")

# Derive current stage
current_stage = str(s_row.get("Current Stage", t_row.get("Current Stage", "SDA")))
if current_stage not in STAGES:
    current_stage = "SDA"

# =========================
# HEADER
# =========================
st.title("Feedback Page")
st.caption(f"Sanction ID: **{sid}**")

# =========================
# OVERVIEW CARDS
# =========================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="card"><div class="kv">Project Name</div><h3>'
                f'{s_row.get("Project Name", "-")}</h3></div>', unsafe_allow_html=True)

with col2:
    status = s_row.get("Status", t_row.get("Overall_status", "Pending"))
    st.markdown(f'<div class="card"><div class="kv">Overall Status</div>'
                f'<div class="badge {_pill_class(status)}">{status}</div></div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="card"><div class="kv">Directorate</div><h3>'
                f'{s_row.get("Directorate","-")}</h3></div>', unsafe_allow_html=True)

with col4:
    amount = _fmt_money(s_row.get("Amount", t_row.get("Value", None)), t_row.get("Currency","GBP"))
    st.markdown(f'<div class="card"><div class="kv">Amount</div><h3>{amount}</h3></div>', unsafe_allow_html=True)

# 2nd row cards
c5, c6, c7, c8 = st.columns(4)
with c5:
    st.markdown('<div class="card"><div class="kv">Submitted</div><h3>'
                f'{s_row.get("Submitted", t_row.get("Submitted_at","-"))}</h3></div>', unsafe_allow_html=True)
with c6:
    st.markdown('<div class="card"><div class="kv">Requester</div><h3>'
                f'{t_row.get("Requester_Email","-")}</h3></div>', unsafe_allow_html=True)
with c7:
    st.markdown('<div class="card"><div class="kv">Department</div><h3>'
                f'{t_row.get("Department","-")}</h3></div>', unsafe_allow_html=True)
with c8:
    st.markdown('<div class="card"><div class="kv">Current Stage</div><h3>'
                f'{current_stage}</h3></div>', unsafe_allow_html=True)

st.divider()

# =========================
# DETAILS (left) + ATTACHMENTS (right)
# =========================
left, right = st.columns([3, 2])

with left:
    st.subheader("Details")
    details = {
        "Sanction ID": sid,
        "Project Name": s_row.get("Project Name", "-"),
        "Status": s_row.get("Status", t_row.get("Overall_status", "-")),
        "Directorate": s_row.get("Directorate", "-"),
        "Amount": amount,
        "Current Stage": current_stage,
        "Submitted": s_row.get("Submitted", t_row.get("Submitted_at","-")),
        "Title": t_row.get("Title", "-"),
        "Currency": t_row.get("Currency", "GBP"),
        "Risk Level": t_row.get("Risk_Level", "-"),
        "Linked resanctions": s_row.get("Linked resanctions", "-"),
    }
    det_df = pd.DataFrame({"Field": details.keys(), "Value": details.values()})
    st.dataframe(det_df, hide_index=True, use_container_width=True)

with right:
    st.subheader("Attachments")
    atts = s_row.get("Attachments", "")
    if pd.isna(atts) or str(atts).strip() == "":
        st.info("No attachments.")
    else:
        # support comma/semicolon separated
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]
        for i, a in enumerate(items, 1):
            st.write(f"{i}. {a}")

st.divider()

# =========================
# FLOW VIEW
# =========================
st.subheader("Approval Flow")
flow_cols = st.columns([1, 0.15, 1, 0.15, 1, 0.15, 1])
for idx, stage in enumerate(STAGES):
    with flow_cols[idx*2]:
        st.markdown(_stage_status_row(stage, pd.concat([s_row, t_row], axis=0)), unsafe_allow_html=True)
    if idx < len(STAGES)-1:
        with flow_cols[idx*2+1]:
            st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)

st.divider()

# =========================
# ACTIONS FOR CURRENT STAGE
# =========================
st.subheader(f"Stage Actions — {current_stage}")

# guard if something is off
if current_stage not in STAGE_KEYS:
    st.warning("This stage is not configured for actions.")
    st.stop()

keys = STAGE_KEYS[current_stage]
current_status = str(t_row.get(keys["status"], "Pending"))

with st.form(f"form_{current_stage}"):
    st.write(f"Current status: <span class='badge {_pill_class(current_status)}'>{current_status}</span>",
             unsafe_allow_html=True)

    decision = st.radio(
        "Decision",
        options=["Approve", "Reject", "Request changes"],
        index=0,
        horizontal=True
    )

    assigned_to = st.text_input("Assign to (email or name)", value=str(t_row.get(keys["assigned_to"], "")))
    comment = st.text_area("Comments / Rationale", placeholder="Add an optional note for audit")
    submitted = st.form_submit_button("Submit decision")

if submitted:
    # --- Update approver_tracker row in-memory dataframe ---
    # ensure row exists in tracker_df (create if needed)
    if t_row.empty:
        # create a new row with minimal required fields
        t_row = pd.Series({"Sanction_ID": sid})
        tracker_df = pd.concat([tracker_df, pd.DataFrame([t_row])], ignore_index=True)
        t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

    # compute new status values
    dec_lower = decision.lower()
    new_status = "Approved" if "approve" in dec_lower else ("Rejected" if "reject" in dec_lower else "Changes requested")
    tracker_df.loc[tracker_df["Sanction_ID"] == sid, keys["status"]] = new_status
    tracker_df.loc[tracker_df["Sanction_ID"] == sid, keys["assigned_to"]] = assigned_to
    tracker_df.loc[tracker_df["Sanction_ID"] == sid, keys["decision_at"]] = _now_iso()

    # If approved, move to next stage; if rejected, overall rejected; if changes requested, stay
    next_stage = _next_stage(current_stage) if new_status == "Approved" else None

    if new_status == "Approved" and next_stage:
        tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Current Stage"] = next_stage
        tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Overall_status"] = "In progress"
        # set next stage flag on; clear other flags
        for stg, meta in STAGE_KEYS.items():
            tracker_df.loc[tracker_df["Sanction_ID"] == sid, meta["flag"]] = (stg == next_stage)
    elif new_status == "Rejected":
        tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Overall_status"] = "Rejected"
    else:  # changes requested
        tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Overall_status"] = "Changes requested"

    # --- Write/append to approval_dummy.csv for downstream stage use ---
    # This file can be consumed by your next-stage dashboard
    row_out = {
        "Sanction_ID": sid,
        "Stage": current_stage,
        "Decision": new_status,
        "Assigned_To": assigned_to,
        "Comment": comment,
        "Decision_At": _now_iso(),
        "Next_Stage": next_stage if next_stage else "",
        "Overall_Status": tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Overall_status"].iloc[0],
    }

    if dummy_df.empty:
        dummy_df = pd.DataFrame([row_out])
    else:
        # upsert by Sanction_ID + Stage
        mask = (dummy_df["Sanction_ID"].astype(str) == sid) & (dummy_df["Stage"] == current_stage)
        if mask.any():
            for k, v in row_out.items():
                dummy_df.loc[mask, k] = v
        else:
            dummy_df = pd.concat([dummy_df, pd.DataFrame([row_out])], ignore_index=True)

    # persist both
    try:
        _write_csv(tracker_df, APPROVER_TRACKER_PATH)
        _write_csv(dummy_df, APPROVAL_DUMMY_PATH)
        st.success(f"Saved decision for {sid} at {current_stage}: **{new_status}**")
    except Exception as e:
        st.error(f"Failed to write updates: {e}")

    # Optional: also update sanctions_df's 'Current Stage' / 'Status'
    try:
        mask_s = sanctions_df["Sanction ID"].astype(str) == sid
        if "Current Stage" in sanctions_df.columns:
            sanctions_df.loc[mask_s, "Current Stage"] = tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Current Stage"].iloc[0]
        if "Status" in sanctions_df.columns:
            sanctions_df.loc[mask_s, "Status"] = tracker_df.loc[tracker_df["Sanction_ID"] == sid, "Overall_status"].iloc[0]
        _write_csv(sanctions_df, SANCTIONS_PATH)
    except Exception as e:
        st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

    st.toast("Updated ✅")
    st.rerun()

# =========================
# HISTORY (from approval_dummy)
# =========================
st.divider()
st.subheader("Decision History")
if dummy_df.empty:
    st.info("No decisions recorded yet.")
else:
    hist = dummy_df.loc[dummy_df["Sanction_ID"].astype(str) == sid]
    if hist.empty:
        st.info("No decisions recorded yet for this sanction.")
    else:
        hist = hist.sort_values("Decision_At", ascending=True)
        st.dataframe(hist, hide_index=True, use_container_width=True)


