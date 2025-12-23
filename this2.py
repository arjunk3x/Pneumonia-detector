# app_pages/MandateApproverDashboard.py

import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime

# =========================
# CONFIG
# =========================
CSV_PATH = "mandate_approver_tracker.csv"   # or wherever you load it from
MANDATE_ID_COL = "Mandate_Approver_Tracker_ID"  # <-- CHANGE to your real column name

ROLES = ["James", "Tim"]
ROLE_FLOW = ["James", "Tim"]

STAGE_MAP = {
    "James": {
        "is_in": "is_in_james",
        "status": "james_status",
        "assigned_to": "james_assigned_to",
        "decision_at": "james_decision_at",
    },
    "Tim": {
        "is_in": "is_in_tim",
        "status": "tim_status",
        "assigned_to": "tim_assigned_to",
        "decision_at": "tim_decision_at",
    },
}

# =========================
# HELPERS
# =========================
def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    required_defaults = {
        "overall_status": "New",
        "is_in_james": 0,
        "james_status": "Pending",
        "james_assigned_to": "",
        "james_decision_at": None,
        "is_in_tim": 0,
        "tim_status": "Pending",
        "tim_assigned_to": "",
        "tim_decision_at": None,
    }
    for c, default in required_defaults.items():
        if c not in df.columns:
            df[c] = default

    # If mandates have never been staged, start them in James
    if "is_in_james" in df.columns:
        df.loc[df["is_in_james"].isna(), "is_in_james"] = 0
    df.loc[df["is_in_james"].astype(int) == 0, "is_in_james"] = 1

    # Keep overall_status aligned for brand new rows
    df.loc[df["overall_status"].isna(), "overall_status"] = "New"
    return df

def stage_cols(role: str):
    m = STAGE_MAP[role]
    return m["is_in"], m["status"], m["assigned_to"], m["decision_at"]

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def persist_and_reregister(con, df):
    df.to_csv(CSV_PATH, index=False)
    try:
        con.unregister("mandate")
    except Exception:
        pass
    con.register("mandate", df)

def visibility_where(role: str) -> str:
    is_in, status_col, _, _ = stage_cols(role)
    # items visible if in stage and not completed
    return f"""
        TRY_CAST({is_in} AS BIGINT) = 1
        AND COALESCE(CAST({status_col} AS VARCHAR), 'Pending') IN ('Pending','In Progress')
    """

def apply_decision(df: pd.DataFrame, mandate_id: str, role: str, decision: str, actor: str) -> pd.DataFrame:
    is_in, status_col, assigned_col, decision_at_col = stage_cols(role)
    mask = df[MANDATE_ID_COL].astype(str) == str(mandate_id)

    # mark decision on current role
    df.loc[mask, status_col] = "Approved" if decision == "Approve" else "Rejected"
    df.loc[mask, assigned_col] = actor
    df.loc[mask, decision_at_col] = now_ts()

    if role == "James":
        if decision == "Approve":
            # route to Tim
            df.loc[mask, "is_in_tim"] = 1
            df.loc[mask, "tim_status"] = "Pending"
            df.loc[mask, "overall_status"] = "Awaiting Tim Approval"
        else:
            df.loc[mask, "overall_status"] = "Rejected by James"

    if role == "Tim":
        if decision == "Approve":
            df.loc[mask, "overall_status"] = "Approved"
        else:
            df.loc[mask, "overall_status"] = "Rejected by Tim"

    return df

# =========================
# UI
# =========================
st.title("Mandate Approver Dashboard")

# You likely already have auth/user-role in session_state.
current_user = st.session_state.get("current_user", "UnknownUser")
current_role = st.session_state.get("current_role", "James")

if current_role not in ROLES:
    st.warning("You don’t have access to the Mandate Approver Dashboard.")
    st.stop()

# Load tracker
df = pd.read_csv(CSV_PATH)
df = ensure_cols(df)

con = duckdb.connect()
con.register("mandate", df)

vf = visibility_where(current_role)

pending_df = con.execute(f"""
    SELECT *
    FROM mandate
    WHERE {vf}
""").df()

approved_or_done_df = con.execute(f"""
    SELECT *
    FROM mandate
    WHERE TRY_CAST({STAGE_MAP[current_role]["is_in"]} AS BIGINT) = 1
      AND COALESCE(CAST({STAGE_MAP[current_role]["status"]} AS VARCHAR), 'Pending')
          IN ('Approved','Rejected')
""").df()

# KPI Row
c1, c2, c3 = st.columns(3)
c1.metric("Pending", len(pending_df))
c2.metric("Completed", len(approved_or_done_df))
c3.metric("Total in Stage", len(pending_df) + len(approved_or_done_df))

st.subheader(f"Pending for {current_role}")
show_cols = [MANDATE_ID_COL]
for optional in ["Value", "overall_status"]:
    if optional in pending_df.columns:
        show_cols.append(optional)

st.dataframe(pending_df[show_cols], use_container_width=True)

# Actions
st.subheader("Actions")
if pending_df.empty:
    st.info("No mandates awaiting your action.")
else:
    selected_id = st.selectbox(
        "Select Mandate Tracker ID",
        pending_df[MANDATE_ID_COL].astype(str).tolist()
    )

    colA, colB, colC = st.columns([1,1,2])
    with colA:
        if st.button("Approve", use_container_width=True):
            df = apply_decision(df, selected_id, current_role, "Approve", current_user)
            persist_and_reregister(con, df)
            st.success(f"Approved {selected_id} ({current_role})")
            st.rerun()

    with colB:
        if st.button("Reject", use_container_width=True):
            df = apply_decision(df, selected_id, current_role, "Reject", current_user)
            persist_and_reregister(con, df)
            st.error(f"Rejected {selected_id} ({current_role})")
            st.rerun()

    with colC:
        if st.button("View / Add Feedback", use_container_width=True):
            st.session_state["selected_mandate_tracker_id"] = str(selected_id)
            st.session_state["navigate_to_feedback"] = True
            st.rerun()

st.caption(f"Logged in as: **{current_user}** ({current_role})")
