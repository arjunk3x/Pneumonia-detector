import pandas as pd
import streamlit as st

# ... your other imports + functions above (including risk_badge, pending_df, df, current_role, status_col) ...

# ============================
# Pending in {current_role} section
# ============================
with st.container():
    # open wrapper so CSS is scoped only to this block
    st.markdown('<div class="first-table-wrapper">', unsafe_allow_html=True)

    # ---------- Section title card ----------
    st.markdown(
        f"""
        <div class="first-section-card">
            <div>
                <div class="first-section-kicker">Sanctions Queue</div>
                <div class="first-section-title">
                    Pending in {current_role}
                </div>
                <div class="first-section-subtitle">
                    Sanctions currently at your stage. Use the filters to focus on specific IDs, statuses, or stages.
                </div>
            </div>
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
    st.markdown(
        '<div class="first-filter-help">Filters</div>',
        unsafe_allow_html=True,
    )

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
            default=[current_role]
            if current_role in display_df["Stage"].unique()
            else [],
        )

    # Apply filters
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

    # ---------- Pastel / blue data editor ----------
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

    # close wrapper
    st.markdown("</div>", unsafe_allow_html=True)

# ============================
# SCOPED CSS – ONLY affects this section
# ============================
st.markdown(
    """
<style>
/* Wrap everything so styles are local to FIRST table block */
.first-table-wrapper {
    margin-top: 0.5rem;
}

/* ======= Section header card (dark blue gradient) ======= */
.first-table-wrapper .first-section-card {
    margin-bottom: 1.2rem;
    padding: 1.25rem 1.7rem;
    border-radius: 1rem;
    background: linear-gradient(115deg, #0b1f4a 0%, #1d4ed8 45%, #38bdf8 100%);
    color: #ffffff;
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.35);
}

.first-table-wrapper .first-section-kicker {
    font-size: 0.9rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    opacity: 0.85;
}

.first-table-wrapper .first-section-title {
    font-size: 1.8rem;
    font-weight: 700;
    margin-top: 0.15rem;
}

.first-table-wrapper .first-section-subtitle {
    margin-top: 0.35rem;
    font-size: 0.95rem;
    color: #e5e7eb;
}

/* ======= Filters label ======= */
.first-table-wrapper .first-filter-help {
    font-size: 0.9rem;
    color: #0b1f4a;
    font-weight: 600;
    margin-bottom: 0.3rem;
}

/* ======= Data editor styling (scoped) ======= */

/* Outer container of this specific data editor */
.first-table-wrapper div[data-testid="stDataEditor"] {
    border-radius: 0.9rem;
    overflow: hidden;
    box-shadow: 0 10px 25px rgba(15, 23, 42, 0.12);
    background: #ffffff;
}

/* Header cells – dark blue with white text */
.first-table-wrapper div[data-testid="stDataEditor"] thead tr th {
    background: #0b1f4a !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid #1e293b !important;
}

/* Body cells */
.first-table-wrapper div[data-testid="stDataEditor"] tbody tr td {
    font-size: 0.9rem;
    padding: 0.35rem 0.5rem;
    border-bottom: 1px solid #e5e7eb;
    background: #ffffff;
    color: #111827;
    transition: background-color 0.18s ease, color 0.18s ease;
}

/* Zebra striping */
.first-table-wrapper div[data-testid="stDataEditor"] tbody tr:nth-child(even) td {
    background: #f9fafb;
}

/* Hover state – blue/white transition */
.first-table-wrapper div[data-testid="stDataEditor"] tbody tr:hover td {
    background: #e0edff !important;    /* light blue */
    color: #0b1f4a !important;         /* navy text */
}

/* Make first column (Sanction ID) a bit bolder */
.first-table-wrapper div[data-testid="stDataEditor"] tbody tr td:first-child {
    font-weight: 600;
}

/* Custom scrollbars only inside this table */
.first-table-wrapper div[data-testid="stDataEditor"] ::-webkit-scrollbar {
    height: 8px;
}
.first-table-wrapper div[data-testid="stDataEditor"] ::-webkit-scrollbar-thumb {
    background: #cbd5f5;
    border-radius: 999px;
}
.first-table-wrapper div[data-testid="stDataEditor"] ::-webkit-scrollbar-thumb:hover {
    background: #9ca3e3;
}
</style>
""",
    unsafe_allow_html=True,
)
