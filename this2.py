# ============================================================
# SanctionApproverDashboard.py  (Option B: st.data_editor + LinkColumn)
# ============================================================

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
import plotly.express as px

st.set_page_config(page_title="Sanction Approver Dashboard", layout="wide")

# ----------------------------
# Roles & Stage Flow
# ----------------------------
STAGES = ["SDA", "DataGuild", "DigitalGuild", "ETIDM"]

def stage_cols(stage: str):
    if stage == "SDA":
        return dict(in_flag="is_in_SDA", status="SDA_status",
                    assigned_to="SDA_assigned_to", decision_at="SDA_decision_at",
                    next_stage="DataGuild")
    if stage == "DataGuild":
        return dict(in_flag="is_in_data_guild", status="data_guild_status",
                    assigned_to="data_guild_assigned_to", decision_at="data_guild_decision_at",
                    next_stage="DigitalGuild")
    if stage == "DigitalGuild":
        return dict(in_flag="is_in_digital_guild", status="digital_guild_status",
                    assigned_to="digital_guild_assigned_to", decision_at="digital_guild_decision_at",
                    next_stage="ETIDM")
    if stage == "ETIDM":
        return dict(in_flag="is_in_etidm", status="etidm_status",
                    assigned_to="etidm_assigned_to", decision_at="etidm_decision_at",
                    next_stage=None)
    raise ValueError("Unknown stage")

def later_stages(stage: str):
    i = STAGES.index(stage)
    return STAGES[i + 1:]

# ----------------------------
# Current role (from session; dev fallback)
# ----------------------------
def get_current_role() -> str:
    role = (st.session_state.get("role") or "SDA").strip()
    norm = role.lower().replace(" ", "")
    if norm in ("sda",): return "SDA"
    if norm in ("dataguild","data_guild"): return "DataGuild"
    if norm in ("digitalguild","digital_guild"): return "DigitalGuild"
    if norm in ("etidm","edidm","eidm"): return "ETIDM"
    return "SDA"

if "role" not in st.session_state:
    st.sidebar.info("No role in session — using dev fallback.")
    st.session_state["role"] = st.sidebar.selectbox("Select role (dev)", STAGES, index=0)

current_role = get_current_role()
current_user = current_role  # one approver per role

# ----------------------------
# Look & Feel (light styles)
# ----------------------------
st.markdown("""
<style>
.main { background-color: #F9FAFB; }
.section-title { display:inline-block;font-size:20px;font-weight:600;
  border-bottom:3px solid #1677FF;padding-bottom:4px;margin-bottom:16px;}
.overview-text {font-size:24px;font-weight:600;margin-bottom:10px;
  border-bottom:3px solid #1677FF;display:inline-block;padding-bottom:4px;}
.activity-box {background-color:#F3F4F6;border-radius:8px;padding:12px;margin-bottom:8px;
  box-shadow:0 1px 3px rgba(0,0,0,0.05);}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.title("Navigation")
    st.markdown("**Dashboard**")
    st.markdown("**Sanction Requests**")
    st.markdown("**Audit History**")
    st.markdown("**Reports**")
    st.markdown("**Settings**")
    st.caption(f"Role: {current_role}")

st.title("Sanction Approver Dashboard")
st.markdown('<div class="overview-text">Overview</div>', unsafe_allow_html=True)

def create_card(title, value, subtext="", bg_color="#E6F4FF"):
    st.markdown(
        f"""
        <div style="
            background:{bg_color}; border:1px solid #E5E7EB; border-radius:12px;
            padding:16px; margin:6px; text-align:center; box-shadow:0 2px 6px rgba(0,0,0,0.06);
        ">
            <div style="font-size:28px;font-weight:700;margin:0;">{value}</div>
            <div style="color:#374151;margin-top:4px;">{title}</div>
            <div style="color:#6B7280;margin-top:2px;">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# ----------------------------
# CSV “Mock DB”
# ----------------------------
CSV_PATH = Path("approval_tracker_dummy.csv")

@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found at {path.resolve()}")
    return pd.read_csv(path)

def save_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)
    load_csv.clear()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # legacy flags to is_in_*
    legacy = {
        "is_SDA_required": "is_in_SDA",
        "is_data_guild_required": "is_in_data_guild",
        "is_digital_guild_required": "is_in_digital_guild",
        "is_etidm_required": "is_in_etidm",
    }
    for old_c, new_c in legacy.items():
        if old_c in df.columns and new_c not in df.columns:
            df.rename(columns={old_c: new_c}, inplace=True)

    defaults = {
        "Overall_status": "Submitted",
        "is_submitter": 1,
        "is_in_SDA": 0, "SDA_status": "Pending",
        "SDA_assigned_to": None, "SDA_decision_at": None,
        "is_in_data_guild": 0, "data_guild_status": "Pending",
        "data_guild_assigned_to": None, "data_guild_decision_at": None,
        "is_in_digital_guild": 0, "digital_guild_status": "Pending",
        "digital_guild_assigned_to": None, "digital_guild_decision_at": None,
        "is_in_etidm": 0, "etidm_status": "Pending",
        "etidm_assigned_to": None, "etidm_decision_at": None,
        "Value": None,
    }
    for c, v in defaults.items():
        if c not in df.columns:
            df[c] = v

    # ensure *_assigned_to are strings (avoid DuckDB casting issue)
    for col in df.columns:
        if "assigned_to" in col:
            df[col] = df[col].astype("string").fillna("")
    return df

df = normalize_columns(load_csv(CSV_PATH))

# ----------------------------
# Role-aware Queries (DuckDB)
# ----------------------------
C = stage_cols(current_role)
in_flag, status_col, assigned_col, decided_col = C["in_flag"], C["status"], C["assigned_to"], C["decision_at"]
next_stage = C["next_stage"]
later = later_stages(current_role)

con = duckdb.connect()
con.register("approval", df)

if current_role == "SDA":
    # SDA sees in-SDA and also brand-new submitted (not yet in-SDA)
    pending_df = con.execute(f"""
        SELECT * FROM approval
        WHERE
          ( {in_flag} = 1
            AND {status_col} IN ('Pending','In Progress')
            AND ({assigned_col} IS NULL OR {assigned_col} = ?)
          )
          OR
          ( COALESCE(is_submitter,1) = 1
            AND COALESCE(is_in_SDA,0) = 0
            AND COALESCE(SDA_status,'Pending') = 'Pending'
          )
    """, [current_user]).df()
else:
    pending_df = con.execute(f"""
        SELECT * FROM approval
        WHERE {in_flag} = 1
          AND {status_col} IN ('Pending','In Progress')
          AND ({assigned_col} IS NULL OR {assigned_col} = ?)
    """, [current_user]).df()

approved_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {status_col} = 'Approved'
      AND {decided_col} IS NOT NULL
""").df()

awaiting_condition = " OR ".join(
    [f"(COALESCE({stage_cols(s)['in_flag']},0)=1 AND COALESCE({stage_cols(s)['status']},'Pending')='Pending')"
     for s in later]
) or "FALSE"
awaiting_df = con.execute(f"""
    SELECT * FROM approval
    WHERE {status_col}='Approved'
      AND ({awaiting_condition})
""").df()

# ----------------------------
# KPIs
# ----------------------------
c1, c2, c3, c4 = st.columns(4)
with c1: create_card("Pending Approvals", len(pending_df))
with c2: create_card("Sanctions to Review", 0)  # optional: add if you want a separate backlog
with c3: create_card(f"Approved by {current_role}", len(approved_df))
with c4: create_card("Awaiting Others", len(awaiting_df))

st.divider()

# ============================================================
# Compact Pending Table (st.data_editor + LinkColumn)
# ============================================================
st.markdown(f'<div class="section-title">Pending in {current_role}</div>', unsafe_allow_html=True)

# Filters
colA, colB = st.columns(2)
with colA:
    search_id = st.text_input("Search by Sanction_ID")
with colB:
    status_opts = sorted(pending_df[status_col].dropna().unique().tolist())
    status_filter = st.multiselect(f"Filter by {current_role} Status", options=status_opts, default=[])

filtered = pending_df.copy()
if search_id:
    if search_id.isdigit():
        filtered = filtered[filtered["Sanction_ID"] == int(search_id)]
    else:
        filtered = filtered[filtered["Sanction_ID"].astype(str).str.contains(search_id, case=False)]
if status_filter:
    filtered = filtered[filtered[status_col].isin(status_filter)]

# Risk label with emoji (since data_editor can't color cells)
def risk_label_from_value(v):
    try:
        v = float(v)
        if v >= 1_000_000: return "🔴 High"
        if v >= 250_000:   return "🟠 Medium"
        return "🟢 Low"
    except Exception:
        return "🟠 Medium"

risk_col_name = "Risk Level"
if risk_col_name not in filtered.columns:
    if "Value" in filtered.columns:
        filtered[risk_col_name] = filtered["Value"].apply(risk_label_from_value)
    else:
        filtered[risk_col_name] = "🟠 Medium"

# Build compact view
display_df = pd.DataFrame({
    "Sanction_ID": filtered["Sanction_ID"],
    "Value": filtered["Value"] if "Value" in filtered.columns else "",
    "Stage": current_role,
    "Status in Stage": filtered[status_col],
    "Risk Level": filtered[risk_col_name],
})

# Add link column for navigation to Feedback page
display_df["View"] = display_df["Sanction_ID"].apply(
    lambda sid: f"./Feedback?sanction_id={sid}"
)

# Render (single table)
st.data_editor(
    display_df,
    hide_index=True,
    disabled=True,                 # make the whole table read-only
    use_container_width=True,
    column_config={
        "View": st.column_config.LinkColumn("View", display_text="View"),
        "Sanction_ID": st.column_config.TextColumn("Sanction_ID"),
        "Value": st.column_config.NumberColumn("Value"),
        "Stage": st.column_config.TextColumn("Stage"),
        "Status in Stage": st.column_config.TextColumn("Status in Stage"),
        "Risk Level": st.column_config.TextColumn("Risk Level"),
    },
)

# ----------------------------
# SDA Intake (optional)
# ----------------------------
if current_role == "SDA":
    backlog_df = duckdb.query("""
        SELECT * FROM approval
        WHERE COALESCE(is_submitter,1)=1
          AND COALESCE(is_in_SDA,0)=0
          AND COALESCE(SDA_status,'Pending')='Pending'
    """).to_df()

    with st.expander("Intake: move submitted items into SDA"):
        st.caption("These are submitted but not yet in the SDA queue.")
        st.dataframe(backlog_df, use_container_width=True, height=220)
        intake_ids = st.multiselect("Select Sanction_IDs to intake",
                                    backlog_df.get("Sanction_ID", pd.Series(dtype=int)).tolist())
        if st.button("Move selected to SDA"):
            if intake_ids:
                idx = df["Sanction_ID"].isin(intake_ids)
                df.loc[idx, "is_in_SDA"] = 1
                df.loc[idx, "SDA_status"] = "Pending"
                df.loc[idx, "SDA_assigned_to"] = ""
                save_csv(df, CSV_PATH)
                st.success("Moved to SDA queue.")
                st.rerun()
            else:
                st.info("Nothing selected.")

st.divider()

# ----------------------------
# Overview & Activity
# ----------------------------
left, right = st.columns(2)

with left:
    st.markdown('<div class="section-title">Stage Status Overview</div>', unsafe_allow_html=True)
    ov = (df[status_col].fillna("Pending").value_counts().reset_index())
    if not ov.empty:
        ov.columns = ["Status", "Count"]
        fig = px.pie(ov, values="Count", names="Status", title=f"{current_role} Today")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to chart yet.")

with right:
    st.markdown('<div class="section-title">Recent Activities</div>', unsafe_allow_html=True)
    for entry in [
        f"{current_role} viewed dashboard",
        "CSV state read for KPIs",
        "Awaiting updates from approval page",
    ]:
        st.markdown(f"<div class='activity-box'><p>{entry}</p></div>", unsafe_allow_html=True)
