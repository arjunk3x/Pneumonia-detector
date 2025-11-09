# SanctionApproverDashboard.py
import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

# =========================
# Config
# =========================
st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide")

CSV_PATH = Path("approval_tracker_dummy.csv")  # mock DB

# =========================
# Session / Current user & role
# =========================
current_user = st.session_state.get("user_email", "sda@company.com")
current_role = st.session_state.get("user_role", "SDA")  # "SDA"|"DataGuild"|"DigitalGuild"|"ETIDM"

# ---- Fast navigate: if a View button was clicked, jump immediately to Feedback page
if "navigate_to_feedback" not in st.session_state:
    st.session_state.navigate_to_feedback = False
if st.session_state.navigate_to_feedback:
    st.session_state.navigate_to_feedback = False
    st.switch_page("app_pages/Feedback_Page.py")

# =========================
# Load data + ensure columns
# =========================
if not CSV_PATH.exists():
    st.error(f"CSV not found at {CSV_PATH.resolve()}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Ensure expected columns exist (no crash on empty/new file)
for col, default in [
    ("Sanction_ID", ""), ("Value", 0.0), ("Overall_status", "Submitted"),
    ("is_submitter", 1),
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

# =========================
# Flow / Stage helpers
# =========================
ROLE_FLOW = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

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
    return ROLE_FLOW[i-1] if i > 0 else None

def next_role(role: str):
    i = ROLE_FLOW.index(role)
    return ROLE_FLOW[i+1] if i < len(ROLE_FLOW)-1 else None

# ---- NEW: robust booleanizer for any is_in_* column (works with 1/0, true/false, yes/no)
def flag_true_sql(col_name: str) -> str:
    return f"""
    CASE
      WHEN LOWER(CAST({col_name} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
      WHEN TRY_CAST({col_name} AS BIGINT) = 1 THEN TRUE
      ELSE FALSE
    END
    """

def visibility_filter_for(role: str) -> str:
    """WHERE clause fragment enforcing visibility by stage (type-safe)."""
    is_in_col, status_col, _, _ = stage_cols(role)
    if role == "SDA":
        # was: f"{is_in_col} = 1"
        return f"{flag_true_sql(is_in_col)} = TRUE"
    p = prev_role(role)
    p_is_in, p_status, _, _ = stage_cols(p)
    # was: f"{p_status} = 'Approved' AND {is_in_col} = 1"
    return f"CAST({p_status} AS VARCHAR) = 'Approved' AND {flag_true_sql(is_in_col)} = TRUE"

# =========================
# UI bits: Title & KPI card
# =========================
st.title("Sanction Approver Dashboard")
st.markdown('<div class="overview-text">Overview</div>', unsafe_allow_html=True)

def create_card(
    title, value, subtext=None,
    bg_color="#E6F4FF", size="lg",
    title_as_badge=True, badge_bg="#1D4ED8", badge_color="#ffffff"
):
    sizes = {
        "sm": {"pad": 14, "title": 14, "value": 22, "sub": 12, "radius": 10, "badge_py": 4, "badge_px": 10},
        "md": {"pad": 18, "title": 16, "value": 30, "sub": 13, "radius": 12, "badge_py": 5, "badge_px": 12},
        "lg": {"pad": 22, "title": 18, "value": 36, "sub": 14, "radius": 14, "badge_py": 6, "badge_px": 14},
        "xl": {"pad": 26, "title": 20, "value": 40, "sub": 16, "radius": 16, "badge_py": 7, "badge_px": 16},
    }
    s = sizes.get(size, sizes["lg"])

    if title_as_badge:
        title_html = (
            f'<span style="display:inline-block;'
            f' padding:{s["badge_py"]}px {s["badge_px"]}px;'
            f' border-radius:999px;'
            f' background:{badge_bg};'
            f' color:{badge_color};'
            f' font-size:{s["title"]}px;'
            f' font-weight:700;'
            f' letter-spacing:.3px;">{title}</span>'
        )
    else:
        title_html = (
            f'<div style="color:#374151; font-size:{s["title"]}px; font-weight:600; margin-top:6px;">'
            f'{title}</div>'
        )

    sub_html = (
        f'<div style="color:#6B7280; font-size:{s["sub"]}px; margin-top:6px;">{subtext}</div>'
        if subtext else ""
    )

    st.markdown(
        f"""
        <div style="
            background:{bg_color};
            border:1px solid #E5E7EB;
            border-radius:{s['radius']}px;
            padding:{s['pad']}px;
            margin:10px 4px;
            text-align:center;
            box-shadow:0 2px 6px rgba(0,0,0,0.06);
        ">
            <div style="font-size:{s['value']}px; font-weight:700; margin:0;">{value}</div>
            <div style="margin-top:10px;">{title_html}</div>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )

# =========================
# Role-scoped datasets (TYPE-SAFE)
# =========================
is_in_col, status_col, assigned_col, decision_col = stage_cols(current_role)
vf = visibility_filter_for(current_role)

pending_df = con.execute(
    f"""
    SELECT *
    FROM approval
    WHERE {vf}
      AND COALESCE(CAST({status_col} AS VARCHAR), 'Pending') IN ('Pending','In Progress')
      AND (
            {assigned_col} IS NULL
         OR COALESCE(CAST({assigned_col} AS VARCHAR), '') = ?
      )
    """,
    [current_user],
).df()

approved_df = con.execute(
    f"""
    SELECT *
    FROM approval
    WHERE {vf}
      AND CAST({status_col} AS VARCHAR) = 'Approved'
      AND COALESCE(CAST({assigned_col} AS VARCHAR), '') = ?
      AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
    """,
    [current_user],
).df()

to_review_df = con.execute(
    f"""
    SELECT *
    FROM approval
    WHERE {vf}
      AND COALESCE(CAST({status_col} AS VARCHAR), 'Pending') = 'Pending'
      AND (
            {assigned_col} IS NULL
         OR COALESCE(CAST({assigned_col} AS VARCHAR), '') <> ?
      )
    """,
    [current_user],
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
          AND (
                {flag_true_sql(nr_is_in)} = TRUE
            AND COALESCE(CAST({nr_status} AS VARCHAR), 'Pending') = 'Pending'
          )
        """
    ).df()
else:
    awaiting_df = pending_df.iloc[0:0].copy()

# =========================
# KPI Cards
# =========================
c1, c2, c3, c4 = st.columns([1.3, 1.3, 1.3, 1.3])
with c1:
    create_card("Pending Approvals", len(pending_df), bg_color="#E6F4FF",
                size="xl", badge_bg="#1D4ED8", badge_color="#FFFFFF")
with c2:
    create_card("Sanctions to Review", len(to_review_df), bg_color="#FFF4E5",
                size="xl", badge_bg="#CA8A04", badge_color="#1F2937")
with c3:
    create_card(f"Approved by {current_role}", len(approved_df), bg_color="#E7F8E6",
                size="xl", badge_bg="#16A34A", badge_color="#FFFFFF")
with c4:
    create_card("Awaiting Others", len(awaiting_df), bg_color="#FFE8E8",
                size="xl", badge_bg="#DC2626", badge_color="#FFFFFF")

st.divider()

# =========================
# Pending table + Filters
# =========================
st.markdown(f'<div class="section-title">Pending in {current_role}</div>', unsafe_allow_html=True)

# Build display DF from pending_df only (already role-limited)
risk_series = df.get("Risk_Level", pd.Series(["Medium"] * len(df)))
risk_txt = (
    pending_df.index.to_series()
    .map(lambda i: risk_series.iloc[i] if i in risk_series.index else "Medium")
    .fillna("Medium")
    .astype(str)
)

def risk_badge(v: str) -> str:
    s = str(v).strip().lower()
    if s == "high":
        return "🔴 High"
    if s == "low":
        return "🟢 Low"
    return "🟠 Medium"

display_df = pd.DataFrame({
    "Sanction_ID": pending_df["Sanction_ID"].astype(str),
    "Value": pending_df["Value"],
    "Stage": current_role,
    "Status in Stage": pending_df[status_col].fillna("Pending").astype(str),
    "Risk Level": risk_txt.map(risk_badge),
})

# ---- REMOVE LinkColumn (caused full reload & login loss)
# display_df["View"] = display_df["Sanction_ID"].apply(lambda sid: f"./Feedback?sanction_id={sid}").astype(str)

colA, colB, colC = st.columns(3)
with colA:
    search_id = st.text_input("Search by Sanction_ID")
with colB:
    selected_status = st.multiselect(
        "Filter by Status", options=sorted(display_df["Status in Stage"].dropna().unique())
    )
with colC:
    selected_stage = st.multiselect(
        "Filter by Stage",
        options=sorted(display_df["Stage"].dropna().unique()),
        default=[current_role] if current_role in display_df["Stage"].unique() else [],
    )

filtered_df = display_df.copy()
if search_id:
    filtered_df = filtered_df[filtered_df["Sanction_ID"].str.contains(search_id, case=False)]
if selected_status:
    filtered_df = filtered_df[filtered_df["Status in Stage"].isin(selected_status)]
if selected_stage:
    filtered_df = filtered_df[filtered_df["Stage"].isin(selected_stage)]

st.data_editor(
    filtered_df[["Sanction_ID", "Value", "Stage", "Status in Stage", "Risk Level"]],
    hide_index=True,
    disabled=True,
    use_container_width=True,
    column_config={
        "Sanction_ID": st.column_config.TextColumn("Sanction_ID"),
        "Value": st.column_config.NumberColumn("Value"),
        "Stage": st.column_config.TextColumn("Stage"),
        "Status in Stage": st.column_config.TextColumn("Status in Stage"),
        "Risk Level": st.column_config.TextColumn("Risk Level"),
    },
)

# -------------------------
# Actions: per-row "View" (keeps same session)
# -------------------------
st.markdown("#### Actions")
for _, r in filtered_df.reset_index(drop=True).iterrows():
    c1, c2 = st.columns([5, 1])
    with c1:
        st.write(
            f"**{r['Sanction_ID']}** — Stage: {r['Stage']} — "
            f"Status: {r['Status in Stage']} — Value: {r['Value']} — Risk: {r['Risk Level']}"
        )
    with c2:
        if st.button("View →", key=f"view_{r['Sanction_ID']}"):
            st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
            st.session_state.navigate_to_feedback = True
            st.rerun()

# =========================
# Intake (SDA only)
# =========================
if current_role == "SDA":
    with st.expander("Intake (SDA only): Submitted → SDA", expanded=False):
        backlog_df = con.execute("""
            SELECT *
            FROM approval
            WHERE CAST(is_submitter AS INTEGER) = 1
              AND ( {flag} = FALSE OR {flag} IS NULL )
        """.format(flag=flag_true_sql("is_in_SDA"))).df()

        if backlog_df.empty:
            st.info("No submitted items waiting for SDA.")
        else:
            st.dataframe(
                backlog_df[["Sanction_ID", "Value", "Overall_status"]],
                use_container_width=True,
            )
            intake_ids = st.multiselect(
                "Select Sanction_IDs to intake",
                backlog_df["Sanction_ID"].astype(str).tolist(),
            )

            if st.button("Move selected to SDA"):
                if intake_ids:
                    mask = df["Sanction_ID"].astype(str).isin(intake_ids)
                    df.loc[mask, "is_in_SDA"] = 1
                    df.loc[mask, "SDA_status"] = "Pending"
                    df.loc[mask, "SDA_assigned_to"] = None
                    df.to_csv(CSV_PATH, index=False)
                    st.success(f"Moved {len(intake_ids)} to SDA")
                    st.rerun()

st.caption(f"Logged in as: **{current_user}** ({current_role})")
