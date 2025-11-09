# app_pages/SanctionApproverDashboard.py
# Complete Streamlit page with dynamic role-driven UI

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# ==============================
# Config
# ==============================
st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide", initial_sidebar_state="expanded")

# Path to your tracker file (can be overridden by st.session_state["tracker_csv_path"])
CSV_PATH = Path(st.session_state.get("tracker_csv_path", r"C:\Users\Arjun.Krishna\Downloads\approval_tracker_dummy.csv"))

# ==============================
# >>> NEW: Identity + Role (single source of truth)
# ==============================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]  # internal keys

ROLE_LABEL = {  # pretty labels for headings
    "SDA": "SDA",
    "DataGuild": "Data Guild",
    "DigitalGuild": "Digital Guild",
    "ETIDM": "ETIDM",
}

DEFAULT_ROLE_BY_EMAIL = {  # optional mapping for first login defaults
    "sda@company.com": "SDA",
    "dataguild@company.com": "DataGuild",
    "digitalguild@company.com": "DigitalGuild",
    "etidm@company.com": "ETIDM",
}

def infer_role_from_email(email: str) -> str:
    e = (email or "").lower()
    if e in DEFAULT_ROLE_BY_EMAIL:
        return DEFAULT_ROLE_BY_EMAIL[e]
    if "dataguild" in e or e.startswith("dg_"):  # heuristic
        return "DataGuild"
    if "digital" in e or e.startswith("dig_"):
        return "DigitalGuild"
    if "etidm" in e:
        return "ETIDM"
    return "SDA"

# initialize once
st.session_state.setdefault("user_email", "sda@company.com")        # >>> NEW
st.session_state.setdefault("user_role", infer_role_from_email(st.session_state["user_email"]))  # >>> NEW

# quick user switch (optional helper when “logging in” as different personas)  >>> NEW
with st.sidebar:
    st.markdown("#### User")
    quick_user = st.selectbox(
        "Quick select (demo users)",
        ["sda@company.com", "dataguild@company.com", "digitalguild@company.com", "etidm@company.com"],
        index=["sda@company.com","dataguild@company.com","digitalguild@company.com","etidm@company.com"].index(st.session_state["user_email"])
        if st.session_state.get("user_email") in DEFAULT_ROLE_BY_EMAIL else 0
    )
    if quick_user != st.session_state["user_email"]:
        st.session_state["user_email"] = quick_user
        st.session_state["user_role"] = infer_role_from_email(quick_user)

    st.markdown("---")
    st.markdown("#### Team")

    # BIND directly to the same session key so the widget writes to session
    st.selectbox(
        "Role",
        ROLE_FLOW,
        index=ROLE_FLOW.index(st.session_state["user_role"]),
        format_func=lambda r: ROLE_LABEL[r],
        key="user_role",  # <-- binding to 'user_role' is critical
    )

# always read live values from session (no hardcoded defaults)  >>> NEW
current_user = st.session_state["user_email"]
current_role = st.session_state["user_role"]

# ==============================
# Fast navigate to Feedback page (if flag was set)
# ==============================
if "navigate_to_feedback" not in st.session_state:
    st.session_state.navigate_to_feedback = False
if st.session_state.navigate_to_feedback:
    st.session_state.navigate_to_feedback = False
    try:
        st.switch_page("app_pages/Feedback_Page.py")
    except Exception:
        pass

# ==============================
# Load data + ensure columns
# ==============================
if not CSV_PATH.exists():
    st.error(f"CSV not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Ensure expected columns exist (fill missing)
for col, default in [
    ("Sanction_ID", ""),
    ("Value", 0.0),
    ("overall_status", "submitted"),
    ("is_submitter", 0),
    ("is_in_SDA", 0), ("SDA_status", "Pending"), ("SDA_assigned_to", None), ("SDA_decision_at", None),
    ("is_in_data_guild", 0), ("data_guild_status", "Pending"), ("data_guild_assigned_to", None), ("data_guild_decision_at", None),
    ("is_in_digital_guild", 0), ("digital_guild_status", "Pending"), ("digital_guild_assigned_to", None), ("digital_guild_decision_at", None),
    ("is_in_etidm", 0), ("etidm_status", "Pending"), ("etidm_assigned_to", None), ("etidm_decision_at", None),
]:
    if col not in df.columns:
        df[col] = default

# Register into DuckDB in-memory
con = duckdb.connect()
con.register("approval", df)

# ==============================
# Flow / Stage helpers
# ==============================
STAGE_MAP = {
    "SDA": {
        "is_in": "is_in_SDA",
        "status": "SDA_status",
        "assigned_to": "SDA_assigned_to",
        "decision_at": "SDA_decision_at",
    },
    "DataGuild": {
        "is_in": "is_in_data_guild",
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "DigitalGuild": {
        "is_in": "is_in_digital_guild",
        "status": "digital_guild_status",
        "assigned_to": "digital_guild_assigned_to",
        "decision_at": "digital_guild_decision_at",
    },
    "ETIDM": {
        "is_in": "is_in_etidm",
        "status": "etidm_status",
        "assigned_to": "etidm_assigned_to",
        "decision_at": "etidm_decision_at",
    },
}

def stage_cols(role: str):
    m = STAGE_MAP[role]
    return m["is_in"], m["status"], m["assigned_to"], m["decision_at"]

def prev_role(role: str):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i - 1] if i > 0 else None

def next_role(role: str):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i + 1] if i < len(ROLE_FLOW) - 1 else None

# Boolean SQL helper (works with 1/0, true/false, yes/no, strings)
def flag_true_sql(col_name: str) -> str:
    return f"""
    CASE
        WHEN LOWER(CAST({col_name} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
        WHEN TRY_CAST({col_name} AS BIGINT) = 1 THEN TRUE
        ELSE FALSE
    END
    """

# Visibility filter (who sees what)
# SDA: items currently in SDA & pending
# Others: items where previous stage is Approved (and decided), and current flag is set & pending
def visibility_filter_for(role: str) -> str:
    is_in_col, status_col, _, decision_col = stage_cols(role)
    if role == "SDA":
        return f"""
            {flag_true_sql(is_in_col)} = TRUE
            AND COALESCE(CAST({status_col} AS VARCHAR),'') IN ('','Pending')
        """
    p = prev_role(role)
    p_is_in, p_status, _, p_decision_at = stage_cols(p)
    return (
        f"CAST({p_status} AS VARCHAR) = 'Approved' "
        f"AND TRY_CAST({p_decision_at} AS TIMESTAMP) IS NOT NULL "
        f"AND {flag_true_sql(is_in_col)} = TRUE "
        f"AND COALESCE(CAST({status_col} AS VARCHAR),'') IN ('','Pending')"
    )

# Helper to set stage flags properly (for Intake / routing)
def set_stage_flags_inplace(df: pd.DataFrame, ids: list[str], stage: str):
    flags = {
        "SDA": "is_in_SDA",
        "DataGuild": "is_in_data_guild",
        "DigitalGuild": "is_in_digital_guild",
        "ETIDM": "is_in_etidm",
    }
    statuses = {
        "SDA": "SDA_status",
        "DataGuild": "data_guild_status",
        "DigitalGuild": "digital_guild_status",
        "ETIDM": "etidm_status",
    }
    assignees = {
        "SDA": "SDA_assigned_to",
        "DataGuild": "data_guild_assigned_to",
        "DigitalGuild": "digital_guild_assigned_to",
        "ETIDM": "etidm_assigned_to",
    }

    mask = df["Sanction_ID"].astype(str).isin([str(x) for x in ids])

    # turn OFF all stage flags first
    for f in ["is_in_SDA", "is_in_data_guild", "is_in_digital_guild", "is_in_etidm"]:
        if f in df.columns:
            df.loc[mask, f] = 0

    # turn ON current stage flag & reset status/assignee
    df.loc[mask, flags[stage]] = 1
    df.loc[mask, statuses[stage]] = "Pending"
    df.loc[mask, assignees[stage]] = None

    # items are no longer raw submissions after entering SDA
    if stage == "SDA" and "is_submitter" in df.columns:
        df.loc[mask, "is_submitter"] = 0

# ==============================
# UI Title + KPI cards
# ==============================
st.title("Sanction Approver Dashboard")
st.caption(f"Logged in as: **{current_user}**  •  Team: **{ROLE_LABEL[current_role]}**")  # >>> CHANGED

def kpi_card(title, value, bg="#E6F4FF", badge_bg="#1D4ED8", badge_color="#FFF"):
    st.markdown(
        f"""
        <div style="
            background:{bg};
            border:1px solid #E5E7EB;
            border-radius:12px;
            padding:18px;
            text-align:center;
            box-shadow:0 2px 6px rgba(0,0,0,0.06);
        ">
            <div style="font-size:30px; font-weight:800;">{value}</div>
            <span style="display:inline-block; padding:6px 12px; border-radius:999px;
                         background:{badge_bg}; color:{badge_color}; font-weight:700;">
                {title}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ==============================
# Role-scoped datasets
# ==============================
is_in_col, status_col, assigned_col, decision_col = stage_cols(current_role)
vf = visibility_filter_for(current_role)

pending_df = con.execute(
    f"""
    SELECT *
    FROM approval
    WHERE {vf}
      AND COALESCE(CAST({status_col} AS VARCHAR), 'Pending') IN ('Pending','In Progress')
    """
).df()

approved_df = con.execute(
    f"""
    SELECT *
    FROM approval
    WHERE {vf}
      AND CAST({status_col} AS VARCHAR) = 'Approved'
      AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
    """
).df()

nr = next_role(current_role)
if nr:
    nr_is_in, nr_status, _, _ = stage_cols(nr)
    awaiting_df = con.execute(
        f"""
        SELECT *
        FROM approval
        WHERE {vf}
          AND CAST({status_col} AS VARCHAR) = 'Approved'
          AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(nr_is_in)} = TRUE
          AND COALESCE(CAST({nr_status} AS VARCHAR), 'Pending') = 'Pending'
        """
    ).df()
else:
    awaiting_df = pending_df.iloc[0:0].copy()

# ==============================
# KPI Cards
# ==============================
c1, c2, c3, c4 = st.columns(4)
with c1: kpi_card("Pending", len(pending_df), "#E6F4FF", "#1D4ED8", "#FFF")
with c2: kpi_card("Approved", len(approved_df), "#E7F8E6", "#16A34A", "#FFF")
with c3: kpi_card("Awaiting Others", len(awaiting_df), "#FFE8D8", "#DC2626", "#FFF")
with c4: kpi_card("Total Items", len(df), "#FFF4E5", "#CBA048", "#1F2937")

st.divider()

# ==============================
# >>> CHANGED: Pending table + View (uses pretty label)
# ==============================
st.markdown(f"### Pending in **{ROLE_LABEL[current_role]}**")

if not pending_df.empty:
    for _, row in pending_df.iterrows():
        c1, c2 = st.columns([6, 1])
        with c1:
            st.write(
                f"**{row['Sanction_ID']}** | Value: {row.get('Value', '')} | "
                f"Status: {row[status_col]} | Stage: {ROLE_LABEL[current_role]}"
            )
        with c2:
            if st.button("View ↗", key=f"view_{row['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(row["Sanction_ID"])
                st.session_state.navigate_to_feedback = True
                st.rerun()
else:
    st.info(f"No pending sanctions for **{ROLE_LABEL[current_role]}**.")

# ==============================
# >>> CHANGED: Intake (role-aware) with label
# ==============================
with st.expander(f"Intake ({ROLE_LABEL[current_role]})", expanded=False):
    if current_role == "SDA":
        backlog_df = con.execute(f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND {flag_true_sql('is_in_SDA')} = FALSE
        """).df()
    else:
        p = prev_role(current_role)
        p_is_in, p_status, _, p_decision_at = stage_cols(p)
        cur_is_in, cur_status, _, _ = stage_cols(current_role)
        backlog_df = con.execute(f"""
            SELECT *
            FROM approval
            WHERE CAST({p_status} AS VARCHAR) = 'Approved'
              AND TRY_CAST({p_decision_at} AS TIMESTAMP) IS NOT NULL
              AND {flag_true_sql(cur_is_in)} = FALSE
              AND {flag_true_sql(p_is_in)} = TRUE
              AND COALESCE(CAST({cur_status} AS VARCHAR),'') IN ('','Pending')
        """).df()

    if backlog_df.empty:
        st.info("No items available for intake.")
    else:
        show_cols = [c for c in ["Sanction_ID","Value","overall_status"] if c in backlog_df.columns]
        show_cols += [c for c in backlog_df.columns if c.endswith("_status")]
        st.dataframe(backlog_df[show_cols], use_container_width=True)

        intake_ids = st.multiselect(
            "Select Sanction_IDs to intake",
            backlog_df["Sanction_ID"].astype(str).tolist(),
        )

        if st.button(f"Move selected to {ROLE_LABEL[current_role]}"):
            if intake_ids:
                set_stage_flags_inplace(df, intake_ids, current_role)
                df.to_csv(CSV_PATH, index=False)
                try:
                    con.unregister("approval")
                except Exception:
                    pass
                con.register("approval", df)
                st.success(f"Moved {len(intake_ids)} to {ROLE_LABEL[current_role]}")
                st.rerun()

# ==============================
# Footer
# ==============================
st.caption(f"Logged in as: **{current_user}** ({ROLE_LABEL[current_role]})")
