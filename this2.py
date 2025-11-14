# ==========================
# ACTIONS SECTION (CLEAN + STYLISH)
# ==========================

import streamlit as st

# ---- STYLES ----
st.markdown(
    """
    <style>
    .actions-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #1f2937;
        margin: 16px 0 10px 0;
    }

    /* Pastel action cards – thin grey border around each sanction */
    .action-card {
        background: #f8fafc;              /* very light grey/blue */
        border-radius: 18px;
        padding: 16px 20px;
        margin-bottom: 12px;
        border: 1px solid #d1d5db;        /* thin grey border */
        box-shadow: 0 4px 10px rgba(15, 23, 42, 0.04);
        transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease;
    }
    .action-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 22px rgba(15, 23, 42, 0.12);
        border-color: #c7d2fe;           /* subtle pastel highlight */
    }

    /* Header row: title + stage + status chip */
    .action-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 6px;
    }
    .action-title {
        font-weight: 700;
        color: #1e293b;
        font-size: 1.05rem;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .action-title-icon {
        font-size: 1.1rem;
    }
    .action-pill-stage {
        background: #e0f2fe;
        color: #0369a1;
        border-radius: 999px;
        padding: 3px 9px;
        font-size: 0.78rem;
        font-weight: 600;
    }

    /* Status chip */
    .status-pill {
        border-radius: 999px;
        padding: 4px 11px;
        font-size: 0.78rem;
        font-weight: 600;
    }
    .status-pending {
        background: #fef9c3;
        color: #854d0e;
    }
    .status-approved {
        background: #dcfce7;
        color: #166534;
    }
    .status-rejected {
        background: #fee2e2;
        color: #b91c1c;
    }

    /* Detail row: value + risk pills */
    .detail-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 4px;
        font-size: 0.9rem;
        color: #475569;
    }

    .value-pill {
        background: #fff7ed;
        color: #9a3412;
        border-radius: 999px;
        padding: 3px 10px;
        font-size: 0.8rem;
        font-weight: 600;
    }

    .risk-pill {
        border-radius: 999px;
        padding: 3px 10px;
        font-size: 0.8rem;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    .risk-low  { background: #dcfce7; color: #15803d; }
    .risk-medium { background: #fef9c3; color: #a16207; }
    .risk-high { background: #fee2e2; color: #b91c1c; }

    /* Pastel View button */
    .stButton > button {
        background: linear-gradient(135deg, #c7d2fe, #e0e7ff);
        color: #1e293b;
        border-radius: 999px;
        border: none;
        padding: 0.45rem 1.0rem;
        font-size: 0.90rem;
        font-weight: 600;
        box-shadow: 0 4px 10px rgba(148, 163, 184, 0.35);
        transition: transform 0.10s ease, box-shadow 0.10s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 18px rgba(148, 163, 184, 0.55);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---- TITLE ----
st.markdown('<div class="actions-title">Actions</div>', unsafe_allow_html=True)

# ---- CARDS ----
for _, r in filtered_df.reset_index(drop=True).iterrows():

    # Risk colour class
    risk_value = str(r["Risk Level"]).lower()
    if "high" in risk_value or "red" in risk_value:
        risk_class = "risk-high"
    elif "med" in risk_value or "amber" in risk_value:
        risk_class = "risk-medium"
    else:
        risk_class = "risk-low"

    # Status colour class
    status_value = str(r["Status in Stage"]).lower()
    if "approved" in status_value:
        status_class = "status-approved"
    elif "reject" in status_value:
        status_class = "status-rejected"
    else:
        status_class = "status-pending"

    st.markdown('<div class="action-card">', unsafe_allow_html=True)

    c1, c2 = st.columns([5, 1])

    with c1:
        # Header row
        st.markdown(
            f"""
            <div class="action-header">
                <div class="action-title">
                    <span class="action-title-icon">📁</span>
                    <span>Sanction ID: {r['Sanction_ID']}</span>
                    <span class="action-pill-stage">Stage: {r['Stage']}</span>
                </div>
                <div class="status-pill {status_class}">
                    {r['Status in Stage']}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Detail row
        st.markdown(
            f"""
            <div class="detail-row">
                <div>
                    <span class="value-pill">£ {r['Value']}</span>
                </div>
                <div>
                    <span class="risk-pill {risk_class}">
                        ● {r['Risk Level']}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        if st.button("View →", key=f"view_{r['Sanction_ID']}"):
            st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
            st.session_state.navigate_to_feedback = True
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)  # close .action-card
