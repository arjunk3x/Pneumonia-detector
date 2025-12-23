# app_pages/MandateApproverDashboard.py
# -----------------------------------------------------------------------------
# Mandate Approver Dashboard (repurposed from Sanction Approver concept)
# Two-level approval workflow:
#   Level 1: James  -> approves/rejects
#   Level 2: Tim    -> approves/rejects (only after James approves)
#
# Tracker integration:
#   mandate_approver_tracker is the source-of-truth log for each mandate’s
#   approval trail. Each mandate row MUST include Mandate_Approver_Tracker_ID.
#
# Notes:
# - Styling intentionally kept minimal (as requested) to avoid long CSS blocks.
# - File-based persistence via CSV (safe for small/medium internal apps).
# - Uses DuckDB in-memory for queries (optional but consistent with your pattern).
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import duckdb
import pandas as pd
import streamlit as st


# =========================
# Configuration
# =========================

APP_TITLE = "Mandate Approver Dashboard"
APP_ICON = "✅"

# Adjust paths if your repo structure differs
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root (one up from app_pages)
DB_DIR = os.path.join(BASE_DIR, "sanction_database")  # keep consistent with your existing folder name

MANDATES_CSV = os.path.join(DB_DIR, "mandates.csv")
TRACKER_CSV = os.path.join(DB_DIR, "mandate_approver_tracker.csv")

# Two approvers in this workflow
APPROVERS = ["James", "Tim"]

# Status values (keep consistent across mandates + tracker)
STATUS_PENDING = "Pending"
STATUS_IN_PROGRESS = "In Progress"
STATUS_APPROVED = "Approved"
STATUS_REJECTED = "Rejected"

# "Stage" / routing
STAGE_JAMES = "James"
STAGE_TIM = "Tim"
STAGE_DONE = "Completed"


# =========================
# Utilities
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def new_tracker_id() -> str:
    # You said: "new mandate_approver_tracker with values available"
    # This generates a unique tracker id when needed.
    return f"MTRK-{uuid.uuid4().hex[:10].upper()}"


def safe_str(x) -> str:
    return "" if pd.isna(x) else str(x)


def normalize_status(s: str) -> str:
    s = safe_str(s).strip()
    if not s:
        return STATUS_PENDING
    # allow loose inputs
    low = s.lower()
    if "pend" in low:
        return STATUS_PENDING
    if "progress" in low:
        return STATUS_IN_PROGRESS
    if "approve" in low:
        return STATUS_APPROVED
    if "reject" in low:
        return STATUS_REJECTED
    return s


# =========================
# Data Schemas
# =========================

MANDATES_COLUMNS = [
    "Mandate_ID",
    "Title",
    "Description",
    "Value",
    "Requested_By",
    "Requested_At",
    "Mandate_Approver_Tracker_ID",  # REQUIRED: included as part of mandate workflow
    "Current_Stage",               # James / Tim / Completed
    "Overall_Status",              # Pending / Approved / Rejected / In Progress
]

TRACKER_COLUMNS = [
    "Mandate_Approver_Tracker_ID",  # primary key
    "Mandate_ID",                   # foreign key
    "Created_At",
    "Updated_At",
    "James_Status",
    "James_Decision_At",
    "James_Comment",
    "Tim_Status",
    "Tim_Decision_At",
    "Tim_Comment",
]


def empty_mandates_df() -> pd.DataFrame:
    return pd.DataFrame(columns=MANDATES_COLUMNS)


def empty_tracker_df() -> pd.DataFrame:
    return pd.DataFrame(columns=TRACKER_COLUMNS)


# =========================
# Load / Save
# =========================

def load_csv_or_empty(path: str, cols: list[str]) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(path, dtype=str)
    # Ensure all required columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = None
    # Keep only expected columns + any extras (preserve user data)
    # We do NOT drop extras; we preserve them as user may have more fields.
    return df


def save_df_csv(df: pd.DataFrame, path: str) -> None:
    ensure_dir(os.path.dirname(path))
    df.to_csv(path, index=False)


def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    mandates = load_csv_or_empty(MANDATES_CSV, MANDATES_COLUMNS)
    tracker = load_csv_or_empty(TRACKER_CSV, TRACKER_COLUMNS)

    # Normalize statuses + fill defaults
    if not mandates.empty:
        if "Overall_Status" in mandates.columns:
            mandates["Overall_Status"] = mandates["Overall_Status"].apply(normalize_status)
        if "Current_Stage" in mandates.columns:
            mandates["Current_Stage"] = mandates["Current_Stage"].fillna(STAGE_JAMES)

    if not tracker.empty:
        for col in ["James_Status", "Tim_Status"]:
            if col in tracker.columns:
                tracker[col] = tracker[col].apply(normalize_status)
        if "Updated_At" in tracker.columns:
            tracker["Updated_At"] = tracker["Updated_At"].fillna(utc_now_iso)
        if "Created_At" in tracker.columns:
            tracker["Created_At"] = tracker["Created_At"].fillna(utc_now_iso)

    return mandates, tracker


def persist_data(mandates: pd.DataFrame, tracker: pd.DataFrame) -> None:
    save_df_csv(mandates, MANDATES_CSV)
    save_df_csv(tracker, TRACKER_CSV)


# =========================
# Tracker + Workflow Logic
# =========================

def ensure_tracker_row(
    tracker: pd.DataFrame,
    mandate_id: str,
    tracker_id: Optional[str] = None
) -> Tuple[pd.DataFrame, str]:
    """
    Ensure that a tracker row exists for a given mandate.
    Returns (updated_tracker_df, tracker_id).
    """
    tracker = tracker.copy()

    # If tracker_id not given, try to find by Mandate_ID
    if not tracker_id:
        existing = tracker.loc[tracker["Mandate_ID"].astype(str) == str(mandate_id)]
        if not existing.empty:
            tracker_id = safe_str(existing.iloc[0]["Mandate_Approver_Tracker_ID"]).strip()

    # If still none, create a new tracker row
    if not tracker_id:
        tracker_id = new_tracker_id()

    existing = tracker.loc[tracker["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)]
    if existing.empty:
        now = utc_now_iso()
        row = {
            "Mandate_Approver_Tracker_ID": tracker_id,
            "Mandate_ID": str(mandate_id),
            "Created_At": now,
            "Updated_At": now,
            "James_Status": STATUS_PENDING,
            "James_Decision_At": "",
            "James_Comment": "",
            "Tim_Status": STATUS_PENDING,
            "Tim_Decision_At": "",
            "Tim_Comment": "",
        }
        tracker = pd.concat([tracker, pd.DataFrame([row])], ignore_index=True)

    return tracker, tracker_id


def get_tracker_row(tracker: pd.DataFrame, tracker_id: str) -> pd.Series:
    row = tracker.loc[tracker["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)]
    if row.empty:
        return pd.Series(dtype=object)
    return row.iloc[0]


def update_tracker(
    tracker: pd.DataFrame,
    tracker_id: str,
    approver: str,
    status: str,
    comment: str
) -> pd.DataFrame:
    tracker = tracker.copy()
    status = normalize_status(status)
    now = utc_now_iso()

    idx = tracker.index[tracker["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)]
    if len(idx) == 0:
        return tracker  # nothing to update

    i = idx[0]
    tracker.at[i, "Updated_At"] = now

    if approver == "James":
        tracker.at[i, "James_Status"] = status
        tracker.at[i, "James_Decision_At"] = now
        tracker.at[i, "James_Comment"] = comment
    elif approver == "Tim":
        tracker.at[i, "Tim_Status"] = status
        tracker.at[i, "Tim_Decision_At"] = now
        tracker.at[i, "Tim_Comment"] = comment

    return tracker


def compute_overall_from_tracker(trow: pd.Series) -> Tuple[str, str]:
    """
    Returns (overall_status, current_stage) based on sequential workflow:
      - If James rejected -> Rejected, Completed
      - If James approved and Tim pending -> In Progress, Tim
      - If Tim rejected -> Rejected, Completed
      - If both approved -> Approved, Completed
      - Otherwise -> Pending, James
    """
    if trow is None or trow.empty:
        return STATUS_PENDING, STAGE_JAMES

    j = normalize_status(trow.get("James_Status", STATUS_PENDING))
    t = normalize_status(trow.get("Tim_Status", STATUS_PENDING))

    if j == STATUS_REJECTED:
        return STATUS_REJECTED, STAGE_DONE

    if j == STATUS_APPROVED and t in [STATUS_PENDING, STATUS_IN_PROGRESS, ""]:
        return STATUS_IN_PROGRESS, STAGE_TIM

    if t == STATUS_REJECTED:
        return STATUS_REJECTED, STAGE_DONE

    if j == STATUS_APPROVED and t == STATUS_APPROVED:
        return STATUS_APPROVED, STAGE_DONE

    # default
    return STATUS_PENDING, STAGE_JAMES


def sync_mandates_from_tracker(mandates: pd.DataFrame, tracker: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure each mandate row has a valid tracker id, and update mandate stage/overall
    from tracker state.
    """
    mandates = mandates.copy()

    if mandates.empty:
        return mandates

    # Ensure Mandate_Approver_Tracker_ID exists for each mandate
    for i in mandates.index:
        mid = safe_str(mandates.at[i, "Mandate_ID"])
        tid = safe_str(mandates.at[i, "Mandate_Approver_Tracker_ID"]).strip()

        tracker, ensured_tid = ensure_tracker_row(tracker, mandate_id=mid, tracker_id=tid)
        mandates.at[i, "Mandate_Approver_Tracker_ID"] = ensured_tid

        trow = get_tracker_row(tracker, ensured_tid)
        overall, stage = compute_overall_from_tracker(trow)
        mandates.at[i, "Overall_Status"] = overall
        mandates.at[i, "Current_Stage"] = stage

    return mandates


def route_next_after_james_approval(mandates: pd.DataFrame, tracker: pd.DataFrame, tracker_id: str) -> pd.DataFrame:
    """
    When James approves, move mandate stage to Tim (sequential routing).
    (Stage/overall are derived from tracker, but we set them explicitly too.)
    """
    mandates = mandates.copy()
    mask = mandates["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)
    if mask.any():
        trow = get_tracker_row(tracker, tracker_id)
        overall, stage = compute_overall_from_tracker(trow)
        mandates.loc[mask, "Overall_Status"] = overall
        mandates.loc[mask, "Current_Stage"] = stage
    return mandates


def finalize_from_tracker(mandates: pd.DataFrame, tracker: pd.DataFrame, tracker_id: str) -> pd.DataFrame:
    mandates = mandates.copy()
    mask = mandates["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)
    if mask.any():
        trow = get_tracker_row(tracker, tracker_id)
        overall, stage = compute_overall_from_tracker(trow)
        mandates.loc[mask, "Overall_Status"] = overall
        mandates.loc[mask, "Current_Stage"] = stage
    return mandates


# =========================
# UI Helpers
# =========================

def minimal_page_setup() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON, layout="wide")


def get_current_user_and_role() -> Tuple[str, str]:
    """
    If your app.py sets session state (recommended), we use it.
    Otherwise we present a simple selector (for local testing).
    """
    # Prefer upstream login if present
    current_user = safe_str(st.session_state.get("current_user")).strip()
    current_role = safe_str(st.session_state.get("current_role")).strip()

    # If not provided, allow selecting for dev/test
    if not current_user or current_user.lower() in ["none", "null"]:
        current_user = st.sidebar.selectbox("Select approver user", APPROVERS, index=0)

    # Role is same as user in this simplified 2-level approver page
    if current_user in APPROVERS:
        current_role = current_user
    else:
        # fallback: allow user to select role explicitly
        current_role = st.sidebar.selectbox("Select approver role", APPROVERS, index=0)

    st.session_state["current_user"] = current_user
    st.session_state["current_role"] = current_role

    return current_user, current_role


def kpi_cards(mandates: pd.DataFrame, role: str) -> None:
    """
    Simple KPIs for the approver.
    """
    if mandates.empty:
        st.info("No mandates found.")
        return

    # "My Queue" = mandates in my stage and not completed
    my_queue = mandates[
        (mandates["Current_Stage"].astype(str) == role) &
        (mandates["Overall_Status"].astype(str).isin([STATUS_PENDING, STATUS_IN_PROGRESS]))
    ]

    approved = mandates[mandates["Overall_Status"].astype(str) == STATUS_APPROVED]
    rejected = mandates[mandates["Overall_Status"].astype(str) == STATUS_REJECTED]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("My Pending", int(len(my_queue)))
    with c2:
        st.metric("Approved (All)", int(len(approved)))
    with c3:
        st.metric("Rejected (All)", int(len(rejected)))
    with c4:
        st.metric("Total Mandates", int(len(mandates)))


def show_table(df: pd.DataFrame, title: str) -> None:
    st.markdown(f"### {title}")
    if df.empty:
        st.info("No rows to display.")
        return
    st.dataframe(df, use_container_width=True)


def register_duckdb(mandates: pd.DataFrame, tracker: pd.DataFrame) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.register("mandates", mandates)
    con.register("tracker", tracker)
    return con


def query_my_queue(con: duckdb.DuckDBPyConnection, role: str) -> pd.DataFrame:
    # Sequential workflow:
    # James sees mandates where Current_Stage=James and not completed
    # Tim sees mandates where Current_Stage=Tim and not completed
    q = """
    SELECT
        Mandate_ID,
        Title,
        Value,
        Requested_By,
        Requested_At,
        Mandate_Approver_Tracker_ID,
        Current_Stage,
        Overall_Status
    FROM mandates
    WHERE Current_Stage = ?
      AND COALESCE(Overall_Status, 'Pending') IN ('Pending', 'In Progress')
    ORDER BY Requested_At DESC
    """
    return con.execute(q, [role]).df()


def query_all(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    q = """
    SELECT
        Mandate_ID,
        Title,
        Value,
        Requested_By,
        Requested_At,
        Mandate_Approver_Tracker_ID,
        Current_Stage,
        Overall_Status
    FROM mandates
    ORDER BY Requested_At DESC
    """
    return con.execute(q).df()


def build_detail_view(mandates: pd.DataFrame, tracker: pd.DataFrame, tracker_id: str) -> Tuple[pd.Series, pd.Series]:
    mrow = mandates.loc[mandates["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)]
    trow = tracker.loc[tracker["Mandate_Approver_Tracker_ID"].astype(str) == str(tracker_id)]

    m = mrow.iloc[0] if not mrow.empty else pd.Series(dtype=object)
    t = trow.iloc[0] if not trow.empty else pd.Series(dtype=object)

    return m, t


# =========================
# Actions (Approve / Reject)
# =========================

def can_act(role: str, trow: pd.Series) -> bool:
    """
    Enforce sequential routing:
      - James can act if James status is Pending/In Progress and Tim not decided
      - Tim can act only after James Approved, and Tim Pending/In Progress
    """
    if trow is None or trow.empty:
        return False

    j = normalize_status(trow.get("James_Status", STATUS_PENDING))
    t = normalize_status(trow.get("Tim_Status", STATUS_PENDING))

    if role == "James":
        return j in [STATUS_PENDING, STATUS_IN_PROGRESS] and t in [STATUS_PENDING, STATUS_IN_PROGRESS, ""]
    if role == "Tim":
        return j == STATUS_APPROVED and t in [STATUS_PENDING, STATUS_IN_PROGRESS, ""]
    return False


def apply_decision(
    mandates: pd.DataFrame,
    tracker: pd.DataFrame,
    tracker_id: str,
    role: str,
    decision: str,
    comment: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Update tracker + sync mandates stage/overall.
    """
    tracker = update_tracker(tracker, tracker_id, approver=role, status=decision, comment=comment)

    # Update mandate stage/overall from tracker
    mandates = finalize_from_tracker(mandates, tracker, tracker_id)

    # If James approved, route to Tim (sequential)
    if role == "James" and normalize_status(decision) == STATUS_APPROVED:
        mandates = route_next_after_james_approval(mandates, tracker, tracker_id)

    return mandates, tracker


# =========================
# Main Page
# =========================

def main() -> None:
    minimal_page_setup()

    st.title(APP_TITLE)

    # Current user / role
    current_user, current_role = get_current_user_and_role()

    # Load data
    mandates, tracker = load_data()

    # Ensure every mandate has a tracker id and synced state
    # (Also ensures tracker rows exist for existing mandates)
    if not mandates.empty:
        # ensure tracker rows for mandates
        mandates = sync_mandates_from_tracker(mandates, tracker)
        # after sync, persist to guarantee tracker ids are embedded in mandates
        persist_data(mandates, tracker)

    # DuckDB connection (optional; kept for consistent querying pattern)
    con = register_duckdb(mandates, tracker)

    # KPIs
    kpi_cards(mandates, current_role)

    st.divider()

    # Tabs
    tab1, tab2, tab3 = st.tabs(["My Queue", "All Mandates", "Tracker Lookup"])

    # -------------------------
    # Tab 1: My Queue
    # -------------------------
    with tab1:
        myq = query_my_queue(con, current_role)

        # Search & filter
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            search_id = st.text_input("Search by Mandate_ID", "")
        with c2:
            search_title = st.text_input("Search by Title", "")
        with c3:
            status_filter = st.multiselect("Filter by Overall_Status", [STATUS_PENDING, STATUS_IN_PROGRESS, STATUS_APPROVED, STATUS_REJECTED], default=[STATUS_PENDING, STATUS_IN_PROGRESS])

        filtered = myq.copy()
        if search_id.strip():
            filtered = filtered[filtered["Mandate_ID"].astype(str).str.contains(search_id.strip(), case=False, na=False)]
        if search_title.strip():
            filtered = filtered[filtered["Title"].astype(str).str.contains(search_title.strip(), case=False, na=False)]
        if status_filter:
            filtered = filtered[filtered["Overall_Status"].astype(str).isin(status_filter)]

        show_table(filtered, f"Pending in {current_role}")

        # Row selection & action
        if not filtered.empty:
            st.markdown("### Review & Decide")
            tracker_id = st.selectbox(
                "Select an item to review (by Tracker ID)",
                options=filtered["Mandate_Approver_Tracker_ID"].astype(str).tolist()
            )

            m, t = build_detail_view(mandates, tracker, tracker_id)

            if not m.empty:
                st.markdown("#### Mandate Details")
                left, right = st.columns([2, 1])
                with left:
                    st.write(f"**Mandate_ID:** {safe_str(m.get('Mandate_ID'))}")
                    st.write(f"**Title:** {safe_str(m.get('Title'))}")
                    st.write(f"**Description:** {safe_str(m.get('Description'))}")
                    st.write(f"**Requested_By:** {safe_str(m.get('Requested_By'))}")
                    st.write(f"**Requested_At:** {safe_str(m.get('Requested_At'))}")
                with right:
                    st.write(f"**Value:** {safe_str(m.get('Value'))}")
                    st.write(f"**Current_Stage:** {safe_str(m.get('Current_Stage'))}")
                    st.write(f"**Overall_Status:** {safe_str(m.get('Overall_Status'))}")
                    st.write(f"**Tracker_ID:** {safe_str(m.get('Mandate_Approver_Tracker_ID'))}")

                st.markdown("#### Approval Tracker (James → Tim)")
                t_show = pd.DataFrame([{
                    "Tracker_ID": safe_str(t.get("Mandate_Approver_Tracker_ID")),
                    "James_Status": safe_str(t.get("James_Status")),
                    "James_Decision_At": safe_str(t.get("James_Decision_At")),
                    "James_Comment": safe_str(t.get("James_Comment")),
                    "Tim_Status": safe_str(t.get("Tim_Status")),
                    "Tim_Decision_At": safe_str(t.get("Tim_Decision_At")),
                    "Tim_Comment": safe_str(t.get("Tim_Comment")),
                    "Updated_At": safe_str(t.get("Updated_At")),
                }])
                st.dataframe(t_show, use_container_width=True)

                allowed = can_act(current_role, t)

                st.markdown("#### Decision")
                if not allowed:
                    st.warning("You cannot take action on this item right now (sequential routing enforced).")
                else:
                    decision = st.radio("Choose decision", [STATUS_APPROVED, STATUS_REJECTED], horizontal=True)
                    comment = st.text_area("Comment (required for Reject; optional for Approve)", value="", height=80)

                    if decision == STATUS_REJECTED and not comment.strip():
                        st.info("Please add a comment for rejection.")

                    colA, colB, colC = st.columns([1, 1, 6])
                    with colA:
                        if st.button("Submit Decision", type="primary"):
                            if decision == STATUS_REJECTED and not comment.strip():
                                st.error("Rejection requires a comment.")
                            else:
                                mandates2, tracker2 = apply_decision(
                                    mandates=mandates,
                                    tracker=tracker,
                                    tracker_id=tracker_id,
                                    role=current_role,
                                    decision=decision,
                                    comment=comment.strip()
                                )
                                persist_data(mandates2, tracker2)
                                st.success(f"Decision recorded: {current_role} → {decision}")
                                st.rerun()
                    with colB:
                        if st.button("View in Feedback Page"):
                            # Compatible with your existing pattern:
                            # Feedback page can read selected_mandate_id / selected_tracker_id
                            st.session_state["selected_mandate_id"] = safe_str(m.get("Mandate_ID"))
                            st.session_state["selected_tracker_id"] = tracker_id
                            st.session_state["navigate_to_feedback"] = True
                            st.rerun()

    # -------------------------
    # Tab 2: All Mandates
    # -------------------------
    with tab2:
        all_df = query_all(con)

        # Filters
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            s_id = st.text_input("Search Mandate_ID", key="all_search_id")
        with c2:
            s_stage = st.multiselect("Stage", [STAGE_JAMES, STAGE_TIM, STAGE_DONE], default=[STAGE_JAMES, STAGE_TIM, STAGE_DONE])
        with c3:
            s_overall = st.multiselect("Overall Status", [STATUS_PENDING, STATUS_IN_PROGRESS, STATUS_APPROVED, STATUS_REJECTED], default=[STATUS_PENDING, STATUS_IN_PROGRESS, STATUS_APPROVED, STATUS_REJECTED])

        filt = all_df.copy()
        if s_id.strip():
            filt = filt[filt["Mandate_ID"].astype(str).str.contains(s_id.strip(), case=False, na=False)]
        if s_stage:
            filt = filt[filt["Current_Stage"].astype(str).isin(s_stage)]
        if s_overall:
            filt = filt[filt["Overall_Status"].astype(str).isin(s_overall)]

        show_table(filt, "All Mandates (Read-only)")

    # -------------------------
    # Tab 3: Tracker Lookup
    # -------------------------
    with tab3:
        st.markdown("### Lookup by Tracker ID")
        if tracker.empty:
            st.info("No tracker data available.")
        else:
            tid = st.text_input("Enter Mandate_Approver_Tracker_ID", "")
            if tid.strip():
                trow = tracker.loc[tracker["Mandate_Approver_Tracker_ID"].astype(str) == tid.strip()]
                if trow.empty:
                    st.warning("Tracker ID not found.")
                else:
                    st.dataframe(trow, use_container_width=True)

                mrow = mandates.loc[mandates["Mandate_Approver_Tracker_ID"].astype(str) == tid.strip()]
                if not mrow.empty:
                    st.markdown("### Linked Mandate")
                    st.dataframe(mrow, use_container_width=True)

    st.divider()
    st.caption(f"Logged in as: **{current_user}** ({current_role})")


# Streamlit entry
if __name__ == "__main__":
    main()
