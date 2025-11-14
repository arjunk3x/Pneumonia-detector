# ==========================
# Actions: per-row "View"
# ==========================

# Pastel + interactive styling just for this section
st.markdown(
    """
    <style>
    .action-card {
        background: #ffffff;
        border-radius: 18px;
        padding: 12px 16px;
        margin-bottom: 10px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 10px rgba(15, 23, 42, 0.04);
        transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease;
    }
    .action-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.10);
        border-color: #c4b5fd;
    }
    .action-title {
        font-weight: 600;
        color: #1f2937;
        font-size: 0.95rem;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .action-pill-stage {
        background: #e0f2fe;
        color: #0369a1;
        border-radius: 999px;
        padding: 2px 8px;
        font-size: 0.7rem;
        font-weight: 500;
    }
    .action-meta {
        font-size: 0.85rem;
        color: #475569;
        margin-top: 2px;
    }
    .risk-pill {
        display: inline-flex;
        align-items: center;
        padding: 2px 10px;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 500;
        background: #e0f2fe;
        color: #0369a1;
        gap: 4px;
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
    .stButton > button {
        background: linear-gradient(135deg, #a5b4fc, #bfdbfe);
        color: #0f172a;
        border-radius: 999px;
        border: none;
        padding: 0.35rem 0.9rem;
        font-size: 0.82rem;
        font-weight: 600;
        box-shadow: 0 4px 10px rgba(148, 163, 184, 0.35);
        transition: transform 0.08s ease, box-shadow 0.08s ease, filter 0.08s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 8px 18px rgba(148, 163, 184, 0.55);
        filter: brightness(1.03);
    }
    .stButton > button:active {
        transform: translateY(0px) scale(0.99);
        box-shadow: 0 3px 8px rgba(148, 163, 184, 0.45);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("### Actions")

for _, r in filtered_df.reset_index(drop=True).iterrows():
    # Decide risk colour class
    risk_value = str(r["Risk Level"]).strip().lower()
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
                    ⚖️ Sanction ID: {r['Sanction_ID']}
                    <span class="action-pill-stage">Stage: {r['Stage']}</span>
                </div>
                <div class="action-meta">
                    Status: {r['Status in Stage']} ·
                    Value: {r['Value']} ·
                    <span class="risk-pill {risk_class}">
                        <span>●</span> Risk: {r['Risk Level']}
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
