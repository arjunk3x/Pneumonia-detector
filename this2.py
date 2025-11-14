# ==========================
# Actions: per-row "View"
# ==========================

# Pastel styling just for this section
st.markdown(
    """
    <style>
    .action-card {
        background: #ffffff;
        border-radius: 18px;
        padding: 12px 16px;
        margin-bottom: 10px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 6px 14px rgba(15, 23, 42, 0.04);
    }
    .action-title {
        font-weight: 600;
        color: #1f2937;
        font-size: 0.95rem;
        margin-bottom: 4px;
    }
    .action-meta {
        font-size: 0.85rem;
        color: #475569;
    }
    .risk-pill {
        display: inline-flex;
        align-items: center;
        padding: 2px 10px;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 500;
        background: #e0f2fe;   /* pastel blue */
        color: #0369a1;
    }
    .stButton > button {
        background: linear-gradient(135deg, #a5b4fc, #bfdbfe);
        color: #0f172a;
        border-radius: 999px;
        border: none;
        padding: 0.35rem 0.9rem;
        font-size: 0.82rem;
        font-weight: 600;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #93c5fd, #c4b5fd);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("### Actions")

for _, r in filtered_df.reset_index(drop=True).iterrows():
    # One pastel "card" per sanction row
    with st.container():
        st.markdown('<div class="action-card">', unsafe_allow_html=True)

        c1, c2 = st.columns([5, 1])

        with c1:
            st.markdown(
                f"""
                <div class="action-title">
                    Sanction ID: {r['Sanction_ID']} · Stage: {r['Stage']}
                </div>
                <div class="action-meta">
                    Status: {r['Status in Stage']} ·
                    Value: {r['Value']} ·
                    <span class="risk-pill">Risk: {r['Risk Level']}</span>
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
