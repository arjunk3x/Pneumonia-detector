# ==========================
# Enhanced Pastel UI Styling
# ==========================

st.markdown(
    """
    <style>

    /* Bigger pastel action cards */
    .action-card {
        background: #f4f7ff;   /* light pastel lavender-blue */
        border-radius: 22px;
        padding: 18px 22px;
        margin-bottom: 14px;
        border: 1px solid #dbe4ff;
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.08);
        transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
    }
    .action-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.15);
        border-color: #c7d2fe;
    }

    /* Title (bigger font for Sanction ID) */
    .action-title {
        font-weight: 700;
        color: #1e293b;
        font-size: 1.10rem;   /* Bigger */
        margin-bottom: 6px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    /* Stage pill */
    .action-pill-stage {
        background: #e0f2fe;
        color: #0369a1;
        border-radius: 999px;
        padding: 4px 10px;
        font-size: 0.78rem;
        font-weight: 600;
    }

    /* Metadata text (slightly larger) */
    .action-meta {
        font-size: 0.95rem;
        color: #475569;
        margin-top: 4px;
        line-height: 1.4;
    }

    /* Risk pill */
    .risk-pill {
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 0.80rem;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    .risk-low {
        background: #dcfce7;
        color: #15803d;
    }
    .risk-medium {
        background: #fef9c3;
        color: #a16207;
    }
    .risk-high {
        background: #fee2e2;
        color: #b91c1c;
    }

    /* Pastel button */
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

# ==========================
# Actions Section
# ==========================

st.markdown("## Actions")   # Made bigger

for _, r in filtered_df.reset_index(drop=True).iterrows():
    
    # Dynamic risk color class
    risk_value = str(r["Risk Level"]).lower()
    if "high" in risk_value or "red" in risk_value:
        risk_class = "risk-high"
    elif "med" in risk_value or "amber" in risk_value:
        risk_class = "risk-medium"
    else:
        risk_class = "risk-low"

    with st.container():
        st.markdown('<div class="action-card">', unsafe_allow_html=True)

        c1, c2 = st.columns([5, 1])

        with c1:
            st.markdown(
                f"""
                <div class="action-title">
                    🗂️ Sanction ID: {r['Sanction_ID']}
                    <span class="action-pill-stage">Stage: {r['Stage']}</span>
                </div>

                <div class="action-meta">
                    <b>Status:</b> {r['Status in Stage']}<br>
                    <b>Value:</b> {r['Value']}<br>
                    <span class="risk-pill {risk_class}">
                        ● Risk: {r['Risk Level']}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with c2:
            if st.button("View →", key=f"view_{r['Sanction_ID']}"):
                st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
                st.session_state.navigate_to_feedback = True
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)
