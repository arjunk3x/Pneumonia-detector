# ==========================
# Pending table + View (PASTEL STYLING)
# ==========================

import streamlit as st
import pandas as pd

# ---------- Pastel CSS for this section ----------
st.markdown(
    """
    <style>
    /* Section wrapper */
    .pending-wrapper {
        background: #f8fafc;                 /* very light blue/grey */
        border-radius: 18px;
        padding: 18px 20px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);
        margin-bottom: 18px;
    }

    .section-title {
        font-size: 1.35rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        color: #111827;
        margin-bottom: 4px;
        text-transform: uppercase;
    }

    .section-subtitle {
        font-size: 0.9rem;
        color: #6b7280;
        margin-bottom: 12px;
    }

    /* Filters row label styling (Streamlit text_inputs / multiselects sit next to this) */
    .filter-help {
        font-size: 0.82rem;
        color: #9ca3af;
        margin-bottom: 6px;
    }

    /* Data editor/table pastel styling */
    [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
        background-color: #f9fafb !important;
        border-radius: 14px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 2px 8px rgba(148, 163, 184, 0.2);
        padding: 4px;
    }
    [data-testid="stDataFrame"] table,
    [data-testid="stDataEditor"] table {
        color: #111827;
        font-size: 0.9rem;
    }

    /* Optional: make column headers slightly bolder (depends on Streamlit version) */
    [data-testid="stDataFrame"] thead tr th,
    [data-testid="stDataEditor"] thead tr th {
        font-weight: 600;
        background: #e5e7eb;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- Section wrapper ----------
with st.container():
    st.markdown('<div class="pending-wrapper">', unsafe_allow_html=True)

    # Section title
    st.markdown(
        f"""
        <div class="section-title">Pending in {current_role}</div>
        <div class="section-subtitle">
            Sanctions currently at your stage. Use the filters to focus on specific IDs, statuses, or stages.
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------- Build display DF ----------
    # risk_series from full df; default to "Medium"
    risk_series = df.get("Risk Level", pd.Series(["Medium"] * len(df)))

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

    display_df = pd.DataFrame(
        {
            "Sanction_ID": pending_df["Sanction_ID"].astype(str),
            "Value": pending_df["Value"],
            "Stage": current_role,
            "Status in Stage": pending_df[status_col].fillna("Pending").astype(str),
            "Risk Level": risk_txt.map(risk_badge),
        }
    )

    # ---------- Filters (search, status, stage) ----------
    st.markdown('<div class="filter-help">Filters</div>', unsafe_allow_html=True)

    colA, colB, colC = st.columns(3)

    with colA:
        search_id = st.text_input("Search by Sanction_ID", "")
    with colB:
        selected_status = st.multiselect(
            "Filter by Status",
            options=sorted(display_df["Status in Stage"].dropna().unique()),
        )
    with colC:
        selected_stage = st.multiselect(
            "Filter by Stage",
            options=sorted(display_df["Stage"].dropna().unique()),
            default=[current_role] if current_role in display_df["Stage"].unique() else [],
        )

    filtered_df = display_df.copy()
    if search_id:
        filtered_df = filtered_df[
            filtered_df["Sanction_ID"].str.contains(search_id, case=False)
        ]
    if selected_status:
        filtered_df = filtered_df[
            filtered_df["Status in Stage"].isin(selected_status)
        ]
    if selected_stage:
        filtered_df = filtered_df[filtered_df["Stage"].isin(selected_stage)]

    # ---------- Pastel data editor ----------
    st.data_editor(
        filtered_df[["Sanction_ID", "Value", "Stage", "Status in Stage", "Risk Level"]],
        hide_index=True,
        disabled=True,
        use_container_width=True,
        column_config={
            "Sanction_ID": st.column_config.TextColumn("Sanction ID"),
            "Value": st.column_config.NumberColumn("Value"),
            "Stage": st.column_config.TextColumn("Stage"),
            "Status in Stage": st.column_config.TextColumn("Status in Stage"),
            "Risk Level": st.column_config.TextColumn("Risk Level"),
        },
    )

    st.markdown("</div>", unsafe_allow_html=True)  # close pending-wrapper
