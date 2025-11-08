

# app_pages/Feedback_Page.py
# Complete feedback page (dynamic by sanction_id query param)

from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Sanction Management", layout="wide")

# -----------------------------
# Paths (update if your files live elsewhere)
# -----------------------------
DETAILS_CSV = Path("sanction_details.csv")              # data used to render this page
WORKFLOW_CSV = Path("approval_tracker_dummy.csv")       # mock DB that tracks stage movement

# -----------------------------
# CSS (keep or tweak)
# -----------------------------
st.markdown(
    """
    <style>
      .header-title { font-size: 34px; font-weight: 700; margin: 0 0 20px; font-family: 'Segoe UI', sans-serif; color: #1F2937;}
      .chip { display: inline-block; padding: 6px 10px; border-radius: 10px; color: #fff; font-weight: 600; }
      .card {
        border: 1px solid #E5E7EB; border-radius: 12px; background: #FFFFFF; padding: 22px; 
        box-shadow: 0 2px 8px rgba(0,0,0,0.03); margin-bottom: 18px;
        font-family: 'Segoe UI', sans-serif;
      }
      .section-title { color:#6B7280; font-size:14px; }
      .section-value { color:#1F2937; font-size:18px; font-weight:600; }
      .button-row { display:flex; gap:16px; margin: 6px 0 18px; }
      .soft { color:#6B7280; }
      .spacer-8 { height: 8px; }
      .spacer-16 { height: 16px; }
      .spacer-24 { height: 24px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Helpers
# -----------------------------
def get_query_sanction_id() -> str:
    """Read ?sanction_id=... from URL (supports old/new Streamlit APIs)."""
    try:
        # Streamlit >= 1.33 style
        sid = st.query_params.get("sanction_id", "")
        if isinstance(sid, list):
            sid = sid[0] if sid else ""
    except Exception:
        qp = st.experimental_get_query_params()
        sid = qp.get("sanction_id", [""])
        sid = sid[0] if sid else ""
    return str(sid).strip()

def load_details(sid: str) -> pd.Series | None:
    if not DETAILS_CSV.exists():
        st.error(f"Details file not found: {DETAILS_CSV}")
        return None
    df = pd.read_csv(DETAILS_CSV, dtype=str).fillna("")
    df["Sanction_ID"] = df["Sanction_ID"].astype(str)
    row = df.loc[df["Sanction_ID"] == sid]
    if row.empty:
        return None
    return row.iloc[0]

def load_workflow_row(sid: str) -> pd.Series | None:
    if not WORKFLOW_CSV.exists():
        return None
    wf = pd.read_csv(WORKFLOW_CSV)
    if "Sanction_ID" not in wf.columns:
        return None
    wf["Sanction_ID"] = wf["Sanction_ID"].astype(str)
    row = wf.loc[wf["Sanction_ID"] == sid]
    if row.empty:
        return None
    return row.iloc[0]

def current_user_and_role() -> tuple[str, str]:
    """Role must be one of: SDA, DataGuild, DigitalGuild, ETIDM."""
    user = st.session_state.get("user", "sda@company.com")
    role = st.session_state.get("role", "SDA")
    return user, role

def advance_to_next_stage(sid: str, role: str):
    """Mark current stage Completed and open next stage as Pending."""
    if not WORKFLOW_CSV.exists():
        st.error(f"Workflow file not found: {WORKFLOW_CSV}")
        return
    wf = pd.read_csv(WORKFLOW_CSV)
    if "Sanction_ID" not in wf.columns:
        st.error("Workflow CSV missing 'Sanction_ID' column.")
        return

    wf["Sanction_ID"] = wf["Sanction_ID"].astype(str)
    if sid not in set(wf["Sanction_ID"]):
        st.error(f"Sanction_ID {sid} not found in workflow.")
        return

    now = datetime.now().isoformat()

    if role == "SDA":
        wf.loc[wf["Sanction_ID"] == sid, "SDA_status"] = "Completed"
        wf.loc[wf["Sanction_ID"] == sid, "SDA_decision_at"] = now
        wf.loc[wf["Sanction_ID"] == sid, "is_in_SDA"] = 0
        wf.loc[wf["Sanction_ID"] == sid, "is_in_data_guild"] = 1
        wf.loc[wf["Sanction_ID"] == sid, "data_guild_status"] = "Pending"

    elif role == "DataGuild":
        wf.loc[wf["Sanction_ID"] == sid, "data_guild_status"] = "Completed"
        wf.loc[wf["Sanction_ID"] == sid, "data_guild_decision_at"] = now
        wf.loc[wf["Sanction_ID"] == sid, "is_in_data_guild"] = 0
        wf.loc[wf["Sanction_ID"] == sid, "is_in_digital_guild"] = 1
        wf.loc[wf["Sanction_ID"] == sid, "digital_guild_status"] = "Pending"

    elif role == "DigitalGuild":
        wf.loc[wf["Sanction_ID"] == sid, "digital_guild_status"] = "Completed"
        wf.loc[wf["Sanction_ID"] == sid, "digital_guild_decision_at"] = now
        wf.loc[wf["Sanction_ID"] == sid, "is_in_digital_guild"] = 0
        wf.loc[wf["Sanction_ID"] == sid, "is_in_etidm"] = 1
        wf.loc[wf["Sanction_ID"] == sid, "etidm_status"] = "Pending"

    elif role == "ETIDM":
        wf.loc[wf["Sanction_ID"] == sid, "etidm_status"] = "Completed"
        wf.loc[wf["Sanction_ID"] == sid, "etidm_decision_at"] = now
        wf.loc[wf["Sanction_ID"] == sid, "is_in_etidm"] = 0
        wf.loc[wf["Sanction_ID"] == sid, "Overall_status"] = "Completed"

    wf.to_csv(WORKFLOW_CSV, index=False)

def reject_in_stage(sid: str, role: str):
    if not WORKFLOW_CSV.exists():
        st.error(f"Workflow file not found: {WORKFLOW_CSV}")
        return
    wf = pd.read_csv(WORKFLOW_CSV)
    wf["Sanction_ID"] = wf["Sanction_ID"].astype(str)
    if role == "SDA":
        wf.loc[wf["Sanction_ID"] == sid, "SDA_status"] = "Rejected"
    elif role == "DataGuild":
        wf.loc[wf["Sanction_ID"] == sid, "data_guild_status"] = "Rejected"
    elif role == "DigitalGuild":
        wf.loc[wf["Sanction_ID"] == sid, "digital_guild_status"] = "Rejected"
    elif role == "ETIDM":
        wf.loc[wf["Sanction_ID"] == sid, "etidm_status"] = "Rejected"
    wf.loc[wf["Sanction_ID"] == sid, "Overall_status"] = "Rejected"
    wf.to_csv(WORKFLOW_CSV, index=False)

def role_gate_allows_action(role: str, wf_row: pd.Series | None) -> bool:
    """Only allow Approve when the item is actually at this role's stage."""
    if wf_row is None:
        return True  # if workflow row missing, don't block
    try:
        if role == "SDA":
            return int(wf_row.get("is_in_SDA", 0)) == 1 and (wf_row.get("SDA_status", "Pending") in ("Pending","In Progress"))
        if role == "DataGuild":
            return int(wf_row.get("is_in_data_guild", 0)) == 1 and (wf_row.get("data_guild_status", "Pending") in ("Pending","In Progress"))
        if role == "DigitalGuild":
            return int(wf_row.get("is_in_digital_guild", 0)) == 1 and (wf_row.get("digital_guild_status", "Pending") in ("Pending","In Progress"))
        if role == "ETIDM":
            return int(wf_row.get("is_in_etidm", 0)) == 1 and (wf_row.get("etidm_status", "Pending") in ("Pending","In Progress"))
    except Exception:
        return True
    return False

# -----------------------------
# Read sanction id & data
# -----------------------------
sid = get_query_sanction_id()
if not sid:
    st.error("No sanction_id provided in the URL.")
    st.stop()

details = load_details(sid)
if details is None:
    st.error(f"No details found for sanction_id = {sid}.")
    st.stop()

wf_row = load_workflow_row(sid)
user, role = current_user_and_role()

# -----------------------------
# Header
# -----------------------------
st.markdown(f"<div class='header-title'>Sanction: {details['Sanction_Code']}</div>", unsafe_allow_html=True)

# -----------------------------
# Summary cards (using your fields)
# -----------------------------
risk_colors = {"High": "#FF4D4F", "Medium": "#F4AD14", "Low": "#22DD22"}
risk_bg = risk_colors.get(details.get("Risk_Level",""), "#9CA3AF")

colA, colB, colC = st.columns([1.2, 1.5, 1.2])

with colA:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Sanction ID</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Sanction_Code']}</div>", unsafe_allow_html=True)
    st.markdown("<div class='spacer-16'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Directorate</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Directorate']}</div>", unsafe_allow_html=True)
    st.markdown("<div class='spacer-16'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Current Stage</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Current_Stage']}</div>", unsafe_allow_html=True)
    st.markdown("<div class='spacer-16'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Linked Resanction</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details.get('Linked_Resanction','N/A') or 'N/A'}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with colB:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Project Name</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Project_Name']}</div>", unsafe_allow_html=True)
    st.markdown("<div class='spacer-16'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Amount</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Amount']}</div>", unsafe_allow_html=True)
    st.markdown("<div class='spacer-16'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Submitted By</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Submitted_By']}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with colC:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Status</div>", unsafe_allow_html=True)
    st.markdown(
        f"<span class='chip' style='background:{risk_bg};'>{details['Status']}</span>",
        unsafe_allow_html=True
    )
    st.markdown("<div class='spacer-16'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Risk Level</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-value'>{details['Risk_Level']}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("")

# -----------------------------
# Attachments
# -----------------------------
st.markdown("<div class='card'><div class='section-title'>Attachments</div>", unsafe_allow_html=True)
for key in ["Attachment_1", "Attachment_2"]:
    url = details.get(key, "")
    if url:
        name = Path(url).name
        st.markdown(f"- [{name}]({url})")
st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------
# Actions
# -----------------------------
st.markdown("<div class='button-row'>", unsafe_allow_html=True)

col1, col2, col3 = st.columns([1, 1, 1])

with col1:
    if st.button("⬅️  Back to Dashboard"):
        try:
            st.switch_page("app_pages/SanctionApproverDashboard.py")
        except Exception:
            st.warning("Couldn't switch page. Use the sidebar to return to Dashboard.")

with col2:
    approve_disabled = not role_gate_allows_action(role, wf_row)
    if approve_disabled:
        st.info(f"This sanction is not currently at the {role} stage. Approval is disabled.")
    if st.button(f"✅ Approve as {role}", disabled=approve_disabled):
        advance_to_next_stage(sid, role)
        st.success(f"{role} marked as Completed. Item moved to the next stage.")
        st.rerun()

with col3:
    if st.button("❌ Reject"):
        reject_in_stage(sid, role)
        st.error("Sanction rejected.")
        st.rerun()

st.markdown("</div>", unsafe_allow_html=True)






















import streamlit as st

def ensure_auth_keys():
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("username", None)
    st.session_state.setdefault("role", None)

ensure_auth_keys()
if not st.session_state.logged_in:
    st.switch_page("Home.py")  # your login page
    st.stop()

def go_to_feedback(sid: str):
    st.session_state["open_sanction_id"] = str(sid)
    st.switch_page("app_pages/Feedback_Page.py")

st.write("### Pending Sanctions")
st.dataframe(display_df[["Sanction_ID","Value","Stage","Status in Stage","Risk Level"]], 
             use_container_width=True)

st.write("#### Actions")
for _, r in display_df.iterrows():
    cols = st.columns([2,2,2,2,1])
    cols[0].write(f"**{r['Sanction_ID']}**")
    cols[1].write(f"{r['Value']}")
    cols[2].write(r["Stage"])
    cols[3].write(r["Status in Stage"])
    if cols[4].button("View", key=f"view_{r['Sanction_ID']}"):
        go_to_feedback(r["Sanction_ID"])

import streamlit as st

def ensure_auth_keys():
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("username", None)
    st.session_state.setdefault("role", None)

ensure_auth_keys()
if not st.session_state.logged_in:
    st.switch_page("Home.py")
    st.stop()

# Get sanction id – prefer session (from switch_page), then URL query as fallback
sid = st.session_state.get("open_sanction_id")
if not sid:
    # Optional fallback if someone navigates via a raw URL with ?sanction_id=...
    qp = st.query_params
    sid = qp.get("sanction_id", [None])[0] if isinstance(qp.get("sanction_id"), list) else qp.get("sanction_id")
    st.session_state["open_sanction_id"] = sid

if not sid:
    st.warning("No sanction selected.")
    st.stop()

