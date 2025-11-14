# app_pages/Feedback_Page.py
# Ultra-styled feedback page with stage actions that persist to approver_tracker.csv

import os
from datetime import datetime

import pandas as pd
import streamlit as st

# =========================
# CONFIG
# =========================
SANCTIONS_PATH = os.getenv("SANCTIONS_PATH", "sanctions_data.csv")
APPROVER_TRACKER_PATH = os.getenv("APPROVER_TRACKER_PATH", "approver_tracker.csv")

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

# =========================
# PAGE + THEME
# =========================
st.set_page_config(
    page_title="Feedback | Sanctions",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Global CSS — base light theme + dark-blue styling for the Stage Actions area
st.markdown(
    """
<style>
:root{
  --bg:#f7f8fb;
  --card:#ffffff;
  --muted:#6b7280;
  --ink:#111827;
  --primary:#4f46e5;      /* indigo-600 */
  --primary-weak:#eef2ff;
  --ok:#10b981;           /* emerald-500 */
  --ok-weak:#ecfdf5;
  --warn:#f59e0b;         /* amber-500 */
  --warn-weak:#fff7ed;
  --danger:#ef4444;       /* red-500 */
  --danger-weak:#fef2f2;
  --ring:#e5e7eb;
  --shadow:0 10px 25px rgba(0,0,0,.07);
  --shadow-sm:0 2px 8px rgba(0,0,0,.06);
  --radius:16px;
  --radius-sm:12px;
}

html, body { background: var(--bg); }

.block-container { padding-top: 1.2rem; }

h1, h2, h3 { letter-spacing:.2px }

/* Generic cards / KPIs (existing design) */
.card{
  border:1px solid var(--ring);
  background: var(--card);
  border-radius: var(--radius);
  padding:16px 18px;
  box-shadow: var(--shadow-sm);
}

.kpi{
  position:relative;
  overflow:hidden;
  background:linear-gradient(180deg,#fff, #fafafa);
}

.kpi .label{ font-size:13px; color:var(--muted); }
.kpi .value{ font-size:22px; font-weight:800; color:var(--ink); }

.kpi .badge{
  position:absolute;
  right:12px;
  top:12px;
}

/* Badges / Pills */
.badge{
  display:inline-flex;
  align-items:center;
  gap:6px;
  font-size:12px;
  font-weight:700;
  padding:4px 10px;
  border-radius:999px;
  border:1px solid var(--ring);
  background:#fff;
  color:var(--ink);
}

.badge i{ font-style:normal; opacity:.9; }

.badge.ok{ background:var(--ok-weak); color:#065f46; border-color:#bdf3dd; }
.badge.warn{ background:var(--warn-weak); color:#7c4a02; border-color:#ffe1b3; }
.badge.danger{ background:var(--danger-weak); color:#7f1d1d; border-color:#fecaca; }
.badge.primary{ background:var(--primary-weak); color:#312e81; border-color:#c7d2fe; }

.pill{
  display:inline-block;
  padding:2px 10px;
  font-size:12px;
  font-weight:700;
  border-radius:999px;
}
.pill.ok{background:var(--ok-weak); color:#065f46}
.pill.warn{background:var(--warn-weak); color:#7c4a02}
.pill.danger{background:var(--danger-weak); color:#7f1d1d}
.pill.info{background:var(--primary-weak); color:#3730a3}

/* Grid helpers */
.grid{
  display:grid;
  gap:16px;
  grid-template-columns: repeat(12, minmax(0,1fr));
}
.col-3{grid-column: span 3 / span 3;}
.col-4{grid-column: span 4 / span 4;}
.col-6{grid-column: span 6 / span 6;}
.col-12{grid-column: span 12 / span 12;}

@media (max-width:1100px){
  .col-3, .col-4, .col-6 { grid-column: span 12 / span 12; }
}

/* Flow steps */
.flow{
  display:flex;
  align-items:stretch;
  gap:12px;
  flex-wrap:wrap;
}

.step{
  flex:1 1 220px;
  background:#fff;
  border:1px dashed var(--ring);
  border-radius:var(--radius-sm);
  padding:12px 14px;
  box-shadow:var(--shadow-sm);
}

.step .title{ font-weight:800; margin-bottom:4px; }
.step .meta{ font-size:12px; color:var(--muted); }

.step.active{ border-color:var(--primary); background:var(--primary-weak); }
.step.done{ border-color:var(--ok); background:var(--ok-weak); }

.step .row{
  display:flex;
  gap:8px;
  align-items:center;
  margin-top:6px;
}
.step .row .lbl{ width:78px; font-size:12px; color:var(--muted); }
.step .row .val{ font-weight:700; color:var(--ink); font-size:13px; }

.arrow{
  display:flex;
  align-items:center;
  color:#9ca3af;
  font-size:22px;
  padding:0 4px
}

/* Generic buttons (rest of app) */
.stButton>button{
  border-radius:12px;
  padding:10px 16px;
  font-weight:800;
  border:1px solid var(--ring);
  background:#fff;
  color:#111827;
  transition:.16s ease;
  box-shadow: var(--shadow-sm);
}
.stButton>button:hover{
  transform:translateY(-1px);
  box-shadow: var(--shadow);
}
button[kind="primary"]{
  background: var(--primary) !important;
  color:#fff !important;
  border-color: transparent !important;
}
button.danger{
  background: var(--danger) !important;
  color:#fff !important;
  border-color: transparent !important;
}
button.warn{
  background: var(--warn) !important;
  color:#111827 !important;
  border-color: transparent !important;
}

.small{ font-size:12px; color:var(--muted) }

/* Dataframe wrapper */
.table-card .stDataFrame{
  border-radius:12px;
  overflow:hidden;
  box-shadow: var(--shadow-sm);
}

/* Little code chip for ID */
.codechip{
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono","Courier New", monospace;
  background:#111827;
  color:#fff;
  padding:2px 8px;
  border-radius:8px;
  font-size:12px;
}

/* =========================
   DARK-BLUE ZONE: Stage Actions
   ========================= */

/* Container gets dark-blue treatment, but headings above stay as-is */
.sticky-actions.stage-dark{
  background: linear-gradient(135deg,#0b1220,#111827);
  border-radius: 18px;
  border:1px solid #1f2937;
  padding:18px 18px 14px;
  box-shadow:0 18px 45px rgba(15,23,42,0.55);
  color:#e5e7eb;
}

/* Status badge in dark zone */
.sticky-actions.stage-dark .badge{
  background:#020617;
  color:#e5e7eb;
  border-color:#1f2937;
}

/* Inputs inside dark zone */
.sticky-actions.stage-dark .stTextInput > div > div > input,
.sticky-actions.stage-dark .stTextArea textarea {
  background:#020617 !important;
  color:#e5e7eb !important;
  border-radius:10px !important;
  border:1px solid #1f2937 !important;
  box-shadow: inset 0 0 0 1px rgba(15,23,42,0.6);
}

.sticky-actions.stage-dark .stTextInput > div > div > input:focus,
.sticky-actions.stage-dark .stTextArea textarea:focus {
  outline:none !important;
  border-color:#2563eb !important;
  box-shadow:0 0 0 1px #2563eb;
}

/* Radio labels */
.sticky-actions.stage-dark .stRadio > label,
.sticky-actions.stage-dark .stRadio div[role="radiogroup"] label {
  color:#e5e7eb !important;
}

/* Radio options hover */
.sticky-actions.stage-dark .stRadio div[role="radio"]{
  background:transparent !important;
}

/* Buttons inside Stage Actions bar */
.sticky-actions.stage-dark .stButton>button{
  border-radius:999px;
  background:#1d4ed8 !important;
  color:#e5e7eb !important;
  border:none;
  padding:8px 18px;
  box-shadow:0 10px 25px rgba(15,23,42,0.55);
}

.sticky-actions.stage-dark .stButton>button:hover{
  background:#2563eb !important;
  transform:translateY(-1px);
}

/* Secondary button (reset) – slightly dimmer blue */
.sticky-actions.stage-dark .stButton:nth-of-type(2)>button{
  background:#0f172a !important;
  box-shadow:0 6px 18px rgba(15,23,42,0.55);
}

/* Dark table card for snapshot */
.table-card-dark{
  background: linear-gradient(135deg,#020617,#020617);
  border-radius:18px;
  padding:14px 16px;
  border:1px solid #111827;
  box-shadow:0 18px 45px rgba(15,23,42,0.6);
}

/* Make the dataframe look like it sits on dark glass */
.table-card-dark .stDataFrame{
  background:#020617;
  border-radius:12px;
  box-shadow:none;
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# HELPERS
# =========================
def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def _write_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)


def _get_param_safe(name: str):
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
    if not txt:
        return "ok"
    t = str(txt).lower()
    if any(k in t for k in ["reject", "blocked", "high", "critical", "risk 3", "risk3"]):
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
    return STAGES[i + 1] if i + 1 < len(STAGES) else None


def _ensure_tracker_columns(df: pd.DataFrame) -> pd.DataFrame:
    base = [
        "Sanction_ID",
        "Title",
        "Requester_Email",
        "Department",
        "Submitted_at",
        "Value",
        "Currency",
        "Risk_Level",
        "Overall_status",
        "Current Stage",
    ]
    for c in base:
        if c not in df.columns:
            df[c] = "" if c not in ["Value"] else 0

    for meta in STAGE_KEYS.values():
        for c in meta.values():
            if c not in df.columns:
                df[c] = ""

    if "Last_comment" not in df.columns:
        df["Last_comment"] = ""

    return df


def _stage_block(stage: str, tr: pd.Series, current_stage: str) -> str:
    meta = STAGE_KEYS[stage]
    status = str(tr.get(meta["status"], "Pending"))
    assigned = str(tr.get(meta["assigned_to"], "")) or "-"
    decided = str(tr.get(meta["decision_at"], "")) or "-"
    cls = _pill_class(status)
    state = (
        "active"
        if current_stage == stage
        else ("done" if status.lower() in ["approved", "rejected"] else "")
    )
    icon = {
        "SDA": "🧮",
        "Data Guild": "📊",
        "DigitalGuild": "💻",
        "ETIDM": "🧪",
    }.get(stage, "🧩")

    return f"""
    <div class="step {state}">
      <div class="title">{icon} {stage}</div>
      <div class="meta">Status: <span class="pill {cls}">{status}</span></div>
      <div class="row"><div class="lbl">Assigned</div><div class="val">{assigned}</div></div>
      <div class="row"><div class="lbl">Decided</div><div class="val">{decided}</div></div>
    </div>
    """


# =========================
# LOGIN GATE (optional)
# =========================
if "logged_in" in st.session_state and not st.session_state.logged_in:
    st.warning("Please login to continue.")
    st.stop()

# =========================
# GET SELECTED SANCTION
# =========================
sid = st.session_state.get("selected_sanction_id") or _get_param_safe("sanction_id")
if not sid:
    st.warning("No sanction selected. Go back and click **View** on a record.")
    st.stop()

sid = str(sid)

# =========================
# LOAD DATA
# =========================
sanctions_df = _read_csv(SANCTIONS_PATH)
tracker_df = _ensure_tracker_columns(_read_csv(APPROVER_TRACKER_PATH))

if sanctions_df.empty:
    st.error(f"{SANCTIONS_PATH} not found or empty.")
    st.stop()
if tracker_df.empty:
    st.error(f"{APPROVER_TRACKER_PATH} not found or empty.")
    st.stop()

if "Sanction ID" in sanctions_df.columns:
    sanctions_df["Sanction ID"] = sanctions_df["Sanction ID"].astype(str)
if "Sanction_ID" in tracker_df.columns:
    tracker_df["Sanction_ID"] = tracker_df["Sanction_ID"].astype(str)

s_row = sanctions_df.loc[sanctions_df["Sanction ID"] == sid]
t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid]

s_row = s_row.iloc[0] if not s_row.empty else pd.Series(dtype="object")
t_row = t_row.iloc[0] if not t_row.empty else pd.Series(dtype="object")

if s_row.empty and t_row.empty:
    st.error(f"Sanction {sid} not found.")
    st.stop()

current_stage = str(t_row.get("Current Stage", s_row.get("Current Stage", "SDA")))
if current_stage not in STAGES:
    current_stage = "SDA"

# =========================
# HEADER
# =========================
st.markdown(
    f"""
<div class="card" style="display:flex;justify-content:space-between;align-items:center;">
  <div>
    <div class="small">Feedback Page</div>
    <h1 style="margin:.2rem 0 .2rem">{s_row.get('Project Name','Untitled')}</h1>
    <div class="small">Sanction <span class="codechip">{sid}</span></div>
  </div>
  <div class="badge primary"><i>Stage</i> {current_stage}</div>
</div>
""",
    unsafe_allow_html=True,
)

st.markdown('<div class="grid" style="margin-top:16px">', unsafe_allow_html=True)

# KPIs row
def _badge_html(text, kind="ok"):
    return f'<span class="badge {kind}">{text}</span>'


amount = _fmt_money(
    s_row.get("Amount", t_row.get("Value", None)), t_row.get("Currency", "GBP")
)
overall = s_row.get("Status", t_row.get("Overall_status", "Pending"))

kpis = [
    ("Project Name", str(s_row.get("Project Name", "-")), None),
    ("Directorate", str(s_row.get("Directorate", "-")), None),
    ("Amount", amount, None),
    ("Overall Status", f"{overall}", _pill_class(overall)),
]

for i, (label, val, badge) in enumerate(kpis, start=1):
    st.markdown(
        f'<div class="kpi card col-3"><div class="label">{label}</div>'
        f'<div class="value">{val}</div>'
        f'{"<div class=\\"badge "+badge+"\\">"+overall+"</div>" if badge else ""}</div>',
        unsafe_allow_html=True,
    )

st.markdown("</div>", unsafe_allow_html=True)

# Row 2 KPIs
st.markdown('<div class="grid" style="margin-top:8px">', unsafe_allow_html=True)

kpis2 = [
    ("Submitted", str(s_row.get("Submitted", t_row.get("Submitted_at", "-")))),
    ("Requester", str(t_row.get("Requester_Email", "-"))),
    ("Department", str(t_row.get("Department", "-"))),
    ("Risk Level", f"{t_row.get('Risk_Level', '-') }"),
]

for i, (label, val) in enumerate(kpis2, start=1):
    pill = _pill_class(val) if label == "Risk Level" else None
    st.markdown(
        f'<div class="kpi card col-3"><div class="label">{label}</div>'
        f'<div class="value">{val}</div>'
        f'{"<div class=\\"badge "+pill+"\\">"+val+"</div>" if pill else ""}</div>',
        unsafe_allow_html=True,
    )

st.markdown("</div>", unsafe_allow_html=True)
st.divider()

# =========================
# FLOW TIMELINE
# =========================
st.subheader("Approval Flow")

flow_html = '<div class="flow">'
for idx, stage in enumerate(STAGES):
    flow_html += _stage_block(stage, t_row, current_stage)
    if idx < len(STAGES) - 1:
        flow_html += '<div class="arrow">→</div>'
flow_html += "</div>"

st.markdown(flow_html, unsafe_allow_html=True)
st.divider()

# =========================
# DETAILS + ATTACHMENTS
# =========================
left, right = st.columns([3, 2], gap="large")

with left:
    st.subheader("Details")
    details = {
        "Sanction ID": sid,
        "Project Name": s_row.get("Project Name", "-"),
        "Status": s_row.get("Status", t_row.get("Overall_status", "-")),
        "Directorate": s_row.get("Directorate", "-"),
        "Amount": amount,
        "Current Stage": current_stage,
        "Submitted": s_row.get("Submitted", t_row.get("Submitted_at", "-")),
        "Title": t_row.get("Title", "-"),
        "Currency": t_row.get("Currency", "GBP"),
        "Risk Level": t_row.get("Risk_Level", "-"),
        "Linked resanctions": s_row.get("Linked resanctions", "-"),
    }
    det_df = pd.DataFrame(
        {"Field": list(details.keys()), "Value": list(details.values())}
    )

    with st.container():
        st.markdown('<div class="table-card">', unsafe_allow_html=True)
        st.dataframe(det_df, hide_index=True, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.subheader("Attachments")
    atts = s_row.get("Attachments", "")
    if pd.isna(atts) or str(atts).strip() == "":
        st.info("No attachments uploaded.")
    else:
        items = [
            a.strip()
            for a in str(atts).replace(";", ",").split(",")
            if a.strip()
        ]
        for i, a in enumerate(items, 1):
            st.markdown(f"- 📎 **Attachment {i}:** {a}")

st.divider()

# =========================
# STAGE ACTIONS — Sticky Action Bar (DARK BLUE STYLED)
# =========================
st.subheader(f"Stage Actions — {current_stage}")

if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta["status"], "Pending"))

    with st.container():
        # dark-blue, glassy bar
        st.markdown(
            '<div class="sticky-actions stage-dark">', unsafe_allow_html=True
        )

        st.write(
            f"Current status: <span class='badge {_pill_class(existing_status)}'>{existing_status}</span>",
            unsafe_allow_html=True,
        )

        # Role guard (optional): allow only the matching role to act
        role = st.session_state.get("role", "")
        # Customize if you want to map roles to stages
        role_can_act = True  # e.g. role_can_act = (role == current_stage)

        if not role_can_act:
            st.warning(f"Your role ({role}) cannot act on {current_stage}.")
        else:
            with st.form(f"form_{current_stage}"):
                colA, colB, colC = st.columns([1.2, 1, 1])

                with colA:
                    decision = st.radio(
                        "Decision",
                        ["Approve ✅", "Reject ⛔", "Request changes ✍️"],
                        index=0,
                    )

                with colB:
                    assigned_to = st.text_input(
                        "Assign to (email or name)",
                        value=str(t_row.get(meta["assigned_to"], "")),
                    )

                with colC:
                    when = st.text_input(
                        "Decision time",
                        value=_now_iso(),
                        help="Auto-filled; can be edited",
                    )

                comment = st.text_area(
                    "Comments / Rationale",
                    placeholder="Add context for the audit trail (optional)",
                )

                c1, c2, _ = st.columns([0.4, 0.4, 0.2])
                with c1:
                    submitted = st.form_submit_button(
                        "Submit decision", use_container_width=True
                    )
                with c2:
                    cancel = st.form_submit_button(
                        "Reset form", use_container_width=True
                    )

                if submitted:
                    # Ensure row exists
                    if t_row.empty:
                        tracker_df = pd.concat(
                            [tracker_df, pd.DataFrame([{"Sanction_ID": sid}])],
                            ignore_index=True,
                        )
                        t_row = tracker_df.loc[
                            tracker_df["Sanction_ID"] == sid
                        ].iloc[0]
                    tracker_df = _ensure_tracker_columns(tracker_df)

                    dec_lower = decision.lower()
                    new_status = (
                        "Approved"
                        if "approve" in dec_lower
                        else ("Rejected" if "reject" in dec_lower else "Changes requested")
                    )

                    mask = tracker_df["Sanction_ID"] == sid
                    tracker_df.loc[mask, meta["status"]] = new_status
                    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
                    tracker_df.loc[mask, meta["decision_at"]] = when or _now_iso()
                    tracker_df.loc[mask, "Last_comment"] = comment

                    nxt = _next_stage(current_stage) if new_status == "Approved" else None

                    if new_status == "Approved" and nxt:
                        tracker_df.loc[mask, "Current Stage"] = nxt
                        tracker_df.loc[mask, "Overall_status"] = "In progress"
                        for stg, m in STAGE_KEYS.items():
                            tracker_df.loc[mask, m["flag"]] = stg == nxt
                    elif new_status == "Rejected":
                        tracker_df.loc[mask, "Overall_status"] = "Rejected"
                    else:
                        tracker_df.loc[mask, "Overall_status"] = "Changes requested"

                    # Persist tracker
                    try:
                        _write_csv(tracker_df, APPROVER_TRACKER_PATH)
                    except Exception as e:
                        st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")
                    else:
                        # Optional mirror to sanctions_data
                        try:
                            if "Sanction ID" in sanctions_df.columns:
                                ms = sanctions_df["Sanction ID"] == sid
                                if "Current Stage" in sanctions_df.columns:
                                    sanctions_df.loc[
                                        ms, "Current Stage"
                                    ] = tracker_df.loc[mask, "Current Stage"].iloc[0]
                                if "Status" in sanctions_df.columns:
                                    sanctions_df.loc[
                                        ms, "Status"
                                    ] = tracker_df.loc[mask, "Overall_status"].iloc[0]
                                _write_csv(sanctions_df, SANCTIONS_PATH)
                        except Exception as e:
                            st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

                        st.success(
                            f"Saved decision for {sid} at {current_stage}: **{new_status}**"
                        )
                        st.toast("Updated ✅")
                        st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

# =========================
# TRACKER SNAPSHOT (for next stage)
# =========================
st.divider()
st.subheader("Tracker Snapshot (next-stage data)")

cols_to_show = [
    "Sanction_ID",
    "Overall_status",
    "Current Stage",
    "is_in_SDA",
    "SDA_status",
    "SDA_assigned_to",
    "SDA_decision_at",
    "is_in_data_guild",
    "data_guild_status",
    "data_guild_assigned_to",
    "data_guild_decision_at",
    "is_in_digital_guild",
    "digital_guild_status",
    "digital_guild_assigned_to",
    "digital_guild_decision_at",
    "is_in_etidm",
    "etidm_status",
    "etidm_assigned_to",
    "etidm_decision_at",
    "Last_comment",
]
cols_to_show = [c for c in cols_to_show if c in tracker_df.columns]

snap = tracker_df.loc[tracker_df["Sanction_ID"] == sid, cols_to_show]

with st.container():
    # dark-blue glassy container, but dataframe itself still readable
    st.markdown('<div class="table-card table-card-dark">', unsafe_allow_html=True)
    st.dataframe(snap, hide_index=True, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
