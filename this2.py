# app_pages/Feedback_Page.py
# - Loads the selected sanction_id from session/query.
# - Reads sanctions_data.csv and approver_tracker.csv.
# - Shows overview + full details.
# - Displays approval flow (SDA -> Data Guild -> DigitalGuild -> ETIDM).
# - Allows Approve / Reject / Request changes for the *current* stage.
# - Persists updates only to approver_tracker.csv (and optionally mirrors Current Stage / Status back to sanctions_data.csv).

import os
from datetime import datetime
import pandas as pd
import streamlit as st

# -----------------------------
# CONFIG – change paths if needed
# -----------------------------
SANCTIONS_PATH         = os.getenv("SANCTIONS_PATH", "sanctions_data.csv")
APPROVER_TRACKER_PATH  = os.getenv("APPROVER_TRACKER_PATH", "approver_tracker.csv")

STAGES = ["SDA", "Data Guild", "DigitalGuild", "ETIDM"]
STAGE_KEYS = {
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

# -----------------------------
# THEME
# -----------------------------
st.set_page_config(page_title="Feedback Page", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
.badge {display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;}
.badge.ok{background:#e8f5e9;color:#1b5e20}
.badge.warn{background:#fff8e1;color:#7b5e00}
.badge.danger{background:#ffebee;color:#b71c1c}
.card{padding:14px 16px;border:1px solid #ececec;border-radius:14px;background:linear-gradient(180deg,#fff, #fafafa);
      box-shadow:0 1px 2px rgba(0,0,0,.05);}
.kv{font-size:13px;color:#6b7280}.kv b{color:#111827}
.flow-step{padding:10px 12px;border-radius:12px;border:1px dashed #e5e7eb;background:#fff}
.flow-step.active{border-color:#4f46e5;background:#eef2ff}
.flow-step.done{border-color:#10b981;background:#ecfdf5}
.flow-step .title{font-weight:700}
.flow-arrow{font-size:22px;color:#9ca3af;padding:0 8px}
.stButton>button{border-radius:12px;padding:8px 14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;
  box-shadow:0 1px 2px rgba(0,0,0,.04);transition:all .15s ease}
.stButton>button:hover{transform:translateY(-1px);box-shadow:0 8px 18px rgba(0,0,0,.08)}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# HELPERS
# -----------------------------
def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)

def _write_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)

def _get_param_safe(name: str):
    # query param fallback for older/newer Streamlit
    try:
        qp = getattr(st, "query_params", None)
        if qp:
            v = qp.get(name)
            if isinstance(v, list):
                return v[0] if v else None
            return v
        q = st.experimental_get_query_params()
        return q.get(name, [None])[0]
    except Exception:
        return None

def _pill_class(txt: str) -> str:
    if not txt: return "ok"
    t = str(txt).lower()
    if any(k in t for k in ["reject", "blocked", "high", "critical", "risk 3", "risk3"]): return "danger"
    if any(k in t for k in ["pending", "review", "medium", "risk 2", "risk2", "request"]): return "warn"
    return "ok"

def _fmt_money(val, currency="GBP"):
    if pd.isna(val): return "-"
    try:
        v = float(val); return f"{currency} {v:,.0f}"
    except Exception:
        return str(val)

def _now_iso():
    return datetime.now().isoformat(timespec="seconds")

def _next_stage(current: str) -> str | None:
    if current not in STAGES: return None
    i = STAGES.index(current)
    return STAGES[i+1] if i+1 < len(STAGES) else None

def _ensure_tracker_columns(df: pd.DataFrame) -> pd.DataFrame:
    must_have = [
        "Sanction_ID","Title","Requester_Email","Department","Submitted_at","Value",
        "Currency","Risk_Level","Overall_status","Current Stage"
    ]
    for col in must_have:
        if col not in df.columns:
            df[col] = "" if col not in ["Value"] else 0
    # stage columns
    for meta in STAGE_KEYS.values():
        for c in meta.values():
            if c not in df.columns:
                df[c] = ""
    # flags should be boolean-ish
    for stg in STAGE_KEYS.values():
        f = stg["flag"]
        if f in df.columns and df[f].dtype == object:
            # leave as-is; could be '', True/False, 0/1; we won't convert hard
            pass
    return df

def _stage_status_block(stage: str, tr: pd.Series, current_stage: str) -> str:
    meta = STAGE_KEYS[stage]
    status = str(tr.get(meta["status"], "Pending"))
    assigned = tr.get(meta["assigned_to"], "")
    decided = tr.get(meta["decision_at"], "")
    cls = _pill_class(status)
    state_class = "active" if current_stage == stage else ("done" if status.lower() in ["approved","rejected"] else "")
    return f"""
      <div class="flow-step {state_class}">
        <div class="title">{stage}</div>
        <div class="kv">Status: <span class="badge {cls}">{status}</span></div>
        <div class="kv">Assigned to: <b>{assigned if assigned else '-'}</b></div>
        <div class="kv">Decision at: <b>{decided if decided else '-'}</b></div>
      </div>
    """

# -----------------------------
# LOGIN GATE (optional)
# -----------------------------
if "logged_in" in st.session_state and not st.session_state.logged_in:
    st.warning("Please login to continue.")
    st.stop()

# -----------------------------
# GET SELECTED SANCTION
# -----------------------------
sid = st.session_state.get("selected_sanction_id") or _get_param_safe("sanction_id")
if not sid:
    st.warning("No sanction selected. Use the dashboard and click **View**.")
    st.stop()
sid = str(sid)

# -----------------------------
# LOAD DATA
# -----------------------------
sanctions_df = _read_csv(SANCTIONS_PATH)
tracker_df   = _ensure_tracker_columns(_read_csv(APPROVER_TRACKER_PATH))

if sanctions_df.empty:
    st.error(f"`{SANCTIONS_PATH}` not found or empty.")
    st.stop()
if tracker_df.empty:
    st.error(f"`{APPROVER_TRACKER_PATH}` not found or empty.")
    st.stop()

# Normalize ids
if "Sanction ID" in sanctions_df.columns:
    sanctions_df["Sanction ID"] = sanctions_df["Sanction ID"].astype(str)
if "Sanction_ID" in tracker_df.columns:
    tracker_df["Sanction_ID"] = tracker_df["Sanction_ID"].astype(str)

s_row = sanctions_df.loc[sanctions_df["Sanction ID"] == sid]
t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid]

if s_row.empty and t_row.empty:
    st.error(f"Sanction `{sid}` not found.")
    st.stop()

s_row = s_row.iloc[0] if not s_row.empty else pd.Series(dtype="object")
t_row = t_row.iloc[0] if not t_row.empty else pd.Series(dtype="object")

# Current stage preference: tracker -> sanctions -> SDA
current_stage = str(t_row.get("Current Stage", s_row.get("Current Stage", "SDA")))
if current_stage not in STAGES:
    current_stage = "SDA"

# -----------------------------
# HEADER & OVERVIEW
# -----------------------------
st.title("Feedback Page")
st.caption(f"Sanction ID: **{sid}**")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f'<div class="card"><div class="kv">Project Name</div><h3>{s_row.get("Project Name","-")}</h3></div>', unsafe_allow_html=True)
with col2:
    overall = s_row.get("Status", t_row.get("Overall_status", "Pending"))
    st.markdown(f'<div class="card"><div class="kv">Overall Status</div><div class="badge {_pill_class(overall)}">{overall}</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="card"><div class="kv">Directorate</div><h3>{s_row.get("Directorate","-")}</h3></div>', unsafe_allow_html=True)
with col4:
    amount = _fmt_money(s_row.get("Amount", t_row.get("Value", None)), t_row.get("Currency","GBP"))
    st.markdown(f'<div class="card"><div class="kv">Amount</div><h3>{amount}</h3></div>', unsafe_allow_html=True)

c5, c6, c7, c8 = st.columns(4)
with c5:
    st.markdown(f'<div class="card"><div class="kv">Submitted</div><h3>{s_row.get("Submitted", t_row.get("Submitted_at","-"))}</h3></div>', unsafe_allow_html=True)
with c6:
    st.markdown(f'<div class="card"><div class="kv">Requester</div><h3>{t_row.get("Requester_Email","-")}</h3></div>', unsafe_allow_html=True)
with c7:
    st.markdown(f'<div class="card"><div class="kv">Department</div><h3>{t_row.get("Department","-")}</h3></div>', unsafe_allow_html=True)
with c8:
    st.markdown(f'<div class="card"><div class="kv">Current Stage</div><h3>{current_stage}</h3></div>', unsafe_allow_html=True)

st.divider()

# -----------------------------
# DETAILS & ATTACHMENTS
# -----------------------------
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
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]
        for i, a in enumerate(items, 1):
            st.write(f"{i}. {a}")

st.divider()

# -----------------------------
# FLOW VIEW
# -----------------------------
st.subheader("Approval Flow")
flow_cols = st.columns([1, 0.15, 1, 0.15, 1, 0.15, 1])
for idx, stage in enumerate(STAGES):
    with flow_cols[idx*2]:
        st.markdown(_stage_status_block(stage, t_row, current_stage), unsafe_allow_html=True)
    if idx < len(STAGES)-1:
        with flow_cols[idx*2+1]:
            st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)

st.divider()

# -----------------------------
# STAGE ACTIONS (writes approver_tracker.csv)
# -----------------------------
st.subheader(f"Stage Actions — {current_stage}")

if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta["status"], "Pending"))
    with st.form(f"form_{current_stage}"):
        st.write(f"Current status: <span class='badge {_pill_class(existing_status)}'>{existing_status}</span>",
                 unsafe_allow_html=True)
        decision = st.radio("Decision", ["Approve", "Reject", "Request changes"], index=0, horizontal=True)
        assigned_to = st.text_input("Assign to (email or name)", value=str(t_row.get(meta["assigned_to"], "")))
        comment = st.text_area("Comments / Rationale (optional)")
        submitted = st.form_submit_button("Submit decision")

    if submitted:
        # ensure row exists in tracker
        if t_row.empty:
            tracker_df = pd.concat([tracker_df, pd.DataFrame([{"Sanction_ID": sid}])], ignore_index=True)
            t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

        # normalize required columns
        tracker_df = _ensure_tracker_columns(tracker_df)

        # compute new status
        dec_lower = decision.lower()
        new_status = "Approved" if "approve" in dec_lower else ("Rejected" if "reject" in dec_lower else "Changes requested")

        # update current stage columns
        mask = tracker_df["Sanction_ID"] == sid
        tracker_df.loc[mask, meta["status"]] = new_status
        tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
        tracker_df.loc[mask, meta["decision_at"]] = _now_iso()

        # stage flow routing
        next_stage = _next_stage(current_stage) if new_status == "Approved" else None

        if new_status == "Approved" and next_stage:
            tracker_df.loc[mask, "Current Stage"] = next_stage
            tracker_df.loc[mask, "Overall_status"] = "In progress"
            # flip flags
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == next_stage)
        elif new_status == "Rejected":
            tracker_df.loc[mask, "Overall_status"] = "Rejected"
        else:  # changes requested
            tracker_df.loc[mask, "Overall_status"] = "Changes requested"

        # optionally store comment in a free column; create if missing
        if "Last_comment" not in tracker_df.columns:
            tracker_df["Last_comment"] = ""
        tracker_df.loc[mask, "Last_comment"] = comment

        # persist tracker
        try:
            _write_csv(tracker_df, APPROVER_TRACKER_PATH)
        except Exception as e:
            st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")
        else:
            # mirror minimal fields into sanctions_data (optional)
            try:
                if "Sanction ID" in sanctions_df.columns:
                    ms = sanctions_df["Sanction ID"] == sid
                    if "Current Stage" in sanctions_df.columns:
                        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
                    if "Status" in sanctions_df.columns:
                        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
                    _write_csv(sanctions_df, SANCTIONS_PATH)
            except Exception as e:
                st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

            st.success(f"Saved decision for {sid} at {current_stage}: **{new_status}**")
            st.toast("Updated ✅")
            st.rerun()

# -----------------------------
# HISTORY (derived live from tracker row)
# -----------------------------
st.divider()
st.subheader("Tracker Snapshot (for next stage use)")
# Show only useful columns for the selected sanction; this df can be used by your next stage directly.
cols_to_show = [
    "Sanction_ID","Overall_status","Current Stage",
    "is_in_SDA","SDA_status","SDA_assigned_to","SDA_decision_at",
    "is_in_data_guild","data_guild_status","data_guild_assigned_to","data_guild_decision_at",
    "is_in_digital_guild","digital_guild_status","digital_guild_assigned_to","digital_guild_decision_at",
    "is_in_etidm","etidm_status","etidm_assigned_to","etidm_decision_at",
    "Last_comment"
]
cols_to_show = [c for c in cols_to_show if c in tracker_df.columns]
snap = tracker_df.loc[tracker_df["Sanction_ID"] == sid, cols_to_show]
st.dataframe(snap, hide_index=True, use_container_width=True)
