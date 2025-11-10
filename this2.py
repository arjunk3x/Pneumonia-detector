# ---- Internal roles (keys) and pretty labels for UI
INTERNAL_ROLES = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]
ROLE_LABEL = {
    "SDA": "SDA",
    "DataGuild": "Data Guild",
    "DigitalGuild": "Digital Guild",
    "ETIDM": "ETIDM",
}
# Stage keys are indexed by the UI label (not the internal key)
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
    "Digital Guild": {
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
# Display order for the flow (UI labels)
STAGES = ["SDA", "Data Guild", "Digital Guild", "ETIDM"]

# =========================
# PAGE + THEME
# =========================
st.set_page_config(page_title="Feedback | Sanctions", layout="wide", initial_sidebar_state="expanded")

# --- Modern CSS (cards, badges, sticky actions, responsive grid)
st.markdown("""
<style>
:root{
  --bg:#0b1020;           /* app bg */
  --card:#0f162d;         /* card bg */
  --ink:#e5ecff;          /* text */
  --muted:#93a4c8;        /* muted text */
  --ring:#1d2440;         /* borders */
  --primary:#6d7cff;      /* indigo-blue */
  --ok:#22c55e;           /* green */
  --warn:#f59e0b;         /* amber */
  --danger:#ef4444;       /* red */
  --shadow:0 10px 30px rgba(0,0,0,.35);
  --radius:16px; --radius-sm:12px;
}
html, body { background: var(--bg); }
.block-container { padding-top: 1.2rem; }
h1,h2,h3 { letter-spacing:.2px; color:var(--ink) }

.card{
  border:1px solid var(--ring); background:var(--card);
  border-radius:var(--radius); padding:18px 20px; box-shadow: var(--shadow);
}
.kpi{ position:relative; overflow:hidden; background:linear-gradient(180deg,#101739,#0f162d); }
.kpi .label{ font-size:13px; color:var(--muted); }
.kpi .value{ font-size:22px; font-weight:800; color:var(--ink); }

.badge{
  display:inline-flex; align-items:center; gap:8px;
  padding:6px 12px; border-radius:999px; font-size:12px; font-weight:800;
  border:1px solid var(--ring); background:#0b1330; color:var(--ink);
}
.badge.ok{ background:rgba(34,197,94,.12); color:#8ff0b4; border-color:rgba(34,197,94,.35) }
.badge.warn{ background:rgba(245,158,11,.12); color:#ffd79a; border-color:rgba(245,158,11,.35) }
.badge.danger{ background:rgba(239,68,68,.12); color:#ffb4b4; border-color:rgba(239,68,68,.35) }
.badge.primary{ background:rgba(109,124,255,.14); color:#b6c1ff; border-color:rgba(109,124,255,.35) }

.pill{ display:inline-block; padding:2px 10px; font-size:12px; font-weight:700; border-radius:999px; }
.pill.ok{background:rgba(34,197,94,.12); color:#8ff0b4}
.pill.warn{background:rgba(245,158,11,.12); color:#ffd79a}
.pill.danger{background:rgba(239,68,68,.12); color:#ffb4b4}
.pill.info{background:rgba(109,124,255,.12); color:#cbd2ff}

.grid{ display:grid; gap:16px; grid-template-columns: repeat(12, minmax(0,1fr)); }
.col-3{grid-column: span 3 / span 3;} .col-4{grid-column: span 4 / span 4;}
.col-6{grid-column: span 6 / span 6;} .col-12{grid-column: span 12 / span 12;}
@media (max-width:1100px){ .col-3, .col-4, .col-6 { grid-column: span 12 / span 12; } }

.flow{ display:flex; align-items:stretch; gap:12px; flex-wrap:wrap; }
.step{ flex:1 1 220px; background:#0d1431; border:1px dashed var(--ring);
  border-radius:var(--radius-sm); padding:12px 14px; box-shadow: var(--shadow); }
.step .title{ font-weight:800; margin-bottom:4px; color:var(--ink);}
.step .meta{ font-size:12px; color:var(--muted); }
.step.active{ border-color:var(--primary); background:rgba(109,124,255,.08); }
.step.done{ border-color:var(--ok); background:rgba(34,197,94,.08); }
.step .row{ display:flex; gap:8px; align-items:center; margin-top:6px; }
.step .row .lbl{ width:88px; font-size:12px; color:var(--muted); }
.step .row .val{ font-weight:700; color:var(--ink); font-size:13px; }
.arrow{ display:flex; align-items:center; color:#7b8ab7; font-size:22px; padding:0 4px }

.sticky-actions{
  position:sticky; top:8px; z-index:10; border:1px solid var(--ring); background:#0f162d;
  border-radius:var(--radius); padding:12px; box-shadow: var(--shadow);
}

.stButton>button{
  border-radius:12px; padding:10px 16px; font-weight:800; border:1px solid var(--ring);
  background:#0b1330; color:#e5ecff; transition:.16s ease; box-shadow: 0 2px 10px rgba(0,0,0,.2);
}
.stButton>button:hover{ transform:translateY(-1px); box-shadow: 0 8px 20px rgba(0,0,0,.35); }
button[kind="primary"]{ background: var(--primary) !important; color:#fff !important; border-color: transparent !important; }
button.danger{ background: var(--danger) !important; color:#fff !important; border-color: transparent !important; }
button.warn{ background: var(--warn) !important; color:#111827 !important; border-color: transparent !important; }

.small{ font-size:12px; color:var(--muted) }
.table-card .stDataFrame{ border-radius:12px; overflow:hidden; box-shadow: var(--shadow); }

.codechip{
  font-family: ui-monospace, Menlo, Consolas, "Liberation Mono", monospace;
  background:#11183a; color:#a9b9ff; padding:2px 8px; border-radius:8px; font-size:12px;
}
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

def _now_iso(): return datetime.now().isoformat(timespec="seconds")

def _next_stage(current_label: str) -> str | None:
    if current_label not in STAGES: return None
    i = STAGES.index(current_label)
    return STAGES[i+1] if i+1 < len(STAGES) else None

def _ensure_tracker_columns(df: pd.DataFrame) -> pd.DataFrame:
    base = ["Sanction_ID","Title","Requester_Email","Department",
            "Submitted_at","Value","Currency","Risk_Level","Overall_status","Current Stage"]
    for c in base:
        if c not in df.columns: df[c] = "" if c != "Value" else 0
    for meta in STAGE_KEYS.values():
        for c in meta.values():
            if c not in df.columns: df[c] = ""
    if "Last_comment" not in df.columns: df["Last_comment"] = ""
    return df

def _stage_block(stage_label: str, tr: pd.Series, current_stage: str) -> str:
    meta = STAGE_KEYS[stage_label]
    status  = str(tr.get(meta["status"], "Pending"))
    assigned = str(tr.get(meta["assigned_to"], "")) or "-"
    decided  = str(tr.get(meta["decision_at"], "")) or "-"
    cls = _pill_class(status)
    state = "active" if current_stage == stage_label else ("done" if status.lower() in ["approved","rejected"] else "")
    icon = {"SDA":"🧮","Data Guild":"📊","Digital Guild":"💻","ETIDM":"🧪"}.get(stage_label,"🧩")
    return f"""
    <div class="step {state}">
      <div class="title">{icon} {stage_label}</div>
      <div class="meta">Status: <span class="pill {cls}">{status}</span></div>
      <div class="row"><div class="lbl">Assigned</div><div class="val">{assigned}</div></div>
      <div class="row"><div class="lbl">Decided</div><div class="val">{decided}</div></div>
    </div>
    """

# =========================
# IDENTITY & ROLE (security)
# =========================
# Inherit from session (set by Dashboard) or default demo user.
st.session_state.setdefault("user_email", "sda@company.com")
st.session_state.setdefault("user_role", "SDA")  # internal key

def _current_internal_role() -> str:
    r = (st.session_state.get("user_role") or "SDA").replace(" ", "")
    if r in ["SDA","DataGuild","DigitalGuild","ETIDM"]:
        return r
    # lightweight inference
    e = st.session_state.get("user_email","").lower()
    if "dataguild" in e: return "DataGuild"
    if "digital" in e:   return "DigitalGuild"
    if "etidm" in e:     return "ETIDM"
    return "SDA"

def _current_stage_label_for_role() -> str:
    # map internal role -> UI stage label
    return {
        "SDA":"SDA",
        "DataGuild":"Data Guild",
        "DigitalGuild":"Digital Guild",
        "ETIDM":"ETIDM",
    }[_current_internal_role()]

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
tracker_df   = _ensure_tracker_columns(_read_csv(APPROVER_TRACKER_PATH))

if sanctions_df.empty: st.error(f"{SANCTIONS_PATH} not found or empty."); st.stop()
if tracker_df.empty:   st.error(f"{APPROVER_TRACKER_PATH} not found or empty."); st.stop()

if "Sanction ID" in sanctions_df.columns:
    sanctions_df["Sanction ID"] = sanctions_df["Sanction ID"].astype(str)
if "Sanction_ID" in tracker_df.columns:
    tracker_df["Sanction_ID"] = tracker_df["Sanction_ID"].astype(str)

s_row = sanctions_df.loc[sanctions_df["Sanction ID"] == sid]
t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid]
s_row = s_row.iloc[0] if not s_row.empty else pd.Series(dtype="object")
t_row = t_row.iloc[0] if not t_row.empty else pd.Series(dtype="object")

if s_row.empty and t_row.empty:
    st.error(f"Sanction {sid} not found."); st.stop()

# Figure out the current stage (UI label) robustly
current_stage = str(
    t_row.get("Current Stage", s_row.get("Current Stage", "SDA"))
)
if current_stage not in STAGES:
    # Map any internal names that may have been stored
    map_back = {"DataGuild":"Data Guild", "DigitalGuild":"Digital Guild"}
    current_stage = map_back.get(current_stage, "SDA")

# =========================
# HEADER
# =========================
st.markdown(f"""
<div class="card" style="display:flex;justify-content:space-between;align-items:center;">
  <div>
    <div class="small">Feedback Page</div>
    <h1 style="margin:.2rem 0 .2rem">{s_row.get('Project Name','Untitled')}</h1>
    <div class="small">Sanction <span class="codechip">{sid}</span></div>
  </div>
  <div class="badge primary"><i>Stage</i>&nbsp; {current_stage}</div>
</div>
""", unsafe_allow_html=True)

# KPIs — row 1
st.markdown('<div class="grid" style="margin-top:16px">', unsafe_allow_html=True)
def _badge(text, kind="ok"): return f'<span class="badge {kind}">{text}</span>'

amount  = _fmt_money(s_row.get("Amount", t_row.get("Value", None)), t_row.get("Currency","GBP"))
overall = s_row.get("Status", t_row.get("Overall_status", "Pending"))
kpis = [
    ("Project Name", str(s_row.get("Project Name","-")), None),
    ("Directorate",  str(s_row.get("Directorate","-")),  None),
    ("Amount",       amount,                             None),
    ("Overall Status", f'{overall}', _pill_class(overall))
]
for label,val,badge in kpis:
    st.markdown(
        f'<div class="kpi card col-3"><div class="label">{label}</div>'
        f'<div class="value">{val}</div>'
        f'{"<div class=\\"badge "+badge+"\\">"+overall+"</div>" if badge else ""}</div>',
        unsafe_allow_html=True
    )
st.markdown('</div>', unsafe_allow_html=True)

# KPIs — row 2
st.markdown('<div class="grid" style="margin-top:8px">', unsafe_allow_html=True)
kpis2 = [
    ("Submitted",  str(s_row.get("Submitted", t_row.get("Submitted_at","-")))),
    ("Requester",  str(t_row.get("Requester_Email","-"))),
    ("Department", str(t_row.get("Department","-"))),
    ("Risk Level", f'{t_row.get("Risk_Level","-")}')
]
for label,val in kpis2:
    pill = _pill_class(val) if label=="Risk Level" else None
    st.markdown(
        f'<div class="kpi card col-3"><div class="label">{label}</div>'
        f'<div class="value">{val}</div>'
        f'{"<div class=\\"badge "+pill+"\\">"+val+"</div>" if pill else ""}</div>',
        unsafe_allow_html=True
    )
st.markdown('</div>', unsafe_allow_html=True)

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
flow_html += '</div>'
st.markdown(flow_html, unsafe_allow_html=True)

st.divider()

# =========================
# DETAILS + ATTACHMENTS
# =========================
left, right = st.columns([3,2], gap="large")

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
    det_df = pd.DataFrame({"Field": list(details.keys()), "Value": list(details.values())})
    with st.container():
        st.markdown('<div class="table-card">', unsafe_allow_html=True)
        st.dataframe(det_df, hide_index=True, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.subheader("Attachments")
    atts = s_row.get("Attachments", "")
    if pd.isna(atts) or str(atts).strip()=="":
        st.info("No attachments uploaded.")
    else:
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]
        for i,a in enumerate(items,1):
            st.markdown(f"- 📎 **Attachment {i}:** {a}")

st.divider()

# =========================
# STAGE ACTIONS — Sticky Action Bar (ROLE-LOCKED)
# =========================
st.subheader(f"Stage Actions — {current_stage}")

if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta["status"], "Pending"))

    # --- Who is allowed?
    user_internal_role = _current_internal_role()            # e.g. "DataGuild"
    user_stage_label   = _current_stage_label_for_role()     # "Data Guild"
    role_can_act = (user_stage_label == current_stage)

    with st.container():
        st.markdown('<div class="sticky-actions">', unsafe_allow_html=True)
        st.write(
            f"Current status: <span class='badge {_pill_class(existing_status)}'>{existing_status}</span>",
            unsafe_allow_html=True
        )

        if not role_can_act:
            st.warning(
                f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**. "
                f"Only the owning team may approve/reject/request changes for this stage."
            )

        with st.form(f"form_{current_stage}"):
            colA, colB, colC = st.columns([1.2, 1, 1])
            with colA:
                decision = st.radio(
                    "Decision",
                    ["Approve ✅", "Reject ⛔", "Request changes ✍️"],
                    index=0,
                    disabled=not role_can_act
                )
            with colB:
                assigned_to = st.text_input(
                    "Assign to (email or name)",
                    value=str(t_row.get(meta["assigned_to"], "")),
                    disabled=not role_can_act
                )
            with colC:
                when = st.text_input(
                    "Decision time", value=_now_iso(), help="Auto-filled; can be edited",
                    disabled=not role_can_act
                )
            comment = st.text_area(
                "Comments / Rationale",
                placeholder="Add context for the audit trail (optional)",
                disabled=not role_can_act
            )

            c1, c2, _ = st.columns([0.4,0.4,0.2])
            with c1:
                submitted = st.form_submit_button(
                    "Submit decision", use_container_width=True,
                    disabled=not role_can_act
                )
            with c2:
                cancel = st.form_submit_button(
                    "Reset form", use_container_width=True,
                    disabled=not role_can_act
                )

        # --- Server-side enforcement (can’t be bypassed)
        if submitted:
            if not role_can_act:
                st.error("Action blocked: your role cannot act on this stage.")
                st.stop()

            # Ensure row exists in tracker
            if t_row.empty:
                tracker_df = pd.concat([tracker_df, pd.DataFrame([{"Sanction_ID": sid}])], ignore_index=True)
                t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]
            tracker_df = _ensure_tracker_columns(tracker_df)

            dec_lower = decision.lower()
            new_status = "Approved" if "approve" in dec_lower else ("Rejected" if "reject" in dec_lower else "Changes requested")

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
                    tracker_df.loc[mask, m["flag"]] = (stg == nxt)
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
                        if "Current Stage" in sanctions_df.columns and "Current Stage" in tracker_df.columns:
                            sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
                        if "Status" in sanctions_df.columns and "Overall_status" in tracker_df.columns:
                            sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
                        _write_csv(sanctions_df, SANCTIONS_PATH)
                except Exception as e:
                    st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

                st.success(f"Saved decision for {sid} at {current_stage}: **{new_status}**")
                st.toast("Updated ✅")
                st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

# =========================
# TRACKER SNAPSHOT (for next stage)
# =========================
st.divider()
st.subheader("Tracker Snapshot (next-stage data)")

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

with st.container():
    st.markdown('<div class="table-card">', unsafe_allow_html=True)
    st.dataframe(snap, hide_index=True, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)
