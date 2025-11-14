# ==========================
# Actions: Clean Pastel Table UI
# ==========================

# Styling for a boxed pastel table look
st.markdown(
    """
    <style>
    .actions-table {
        display: flex;
        flex-direction: column;
        gap: 10px;
        margin-top: 8px;
    }
    .action-row {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 14px 18px;
        box-shadow: 0 4px 10px rgba(15, 23, 42, 0.03);
        display: grid;
        grid-template-columns: 2fr 1fr 1fr 1fr auto;
        align-items: center;
        gap: 12px;
    }
    .action-header {
        font-weight: 600;
        color: #1e293b;
        padding: 6px 0;
        font-size: 0.9rem;
        border-bottom: 1px solid #e2e8f0;
        margin-bottom: 6px;
    }
    .action-cell {
        color: #475569;
        font-size: 0.85rem;
    }
    .risk-pill {
        padding: 3px 12px;
        background: #e0f2fe;
        color: #0369a1;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 500;
    }
    .stButton > button {
        background: linear-gradient(135deg, #a5b4fc, #bfdbfe);
        color: #0f172a !important;
        border-radius: 999px;
        border: none;
        padding: 0.3rem 0.8rem;
        font-size: 0.8rem;
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

# Header row (mimics table header)
st.markdown(
    """
    <div class="action-row action-header">
        <div>Sanction ID</div>
        <div>Stage</div>
        <div>Status</div>
        <div>Value</div>
        <div>Risk</div>
        <div></div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Table body container
st.markdown('<div class="actions-table">', unsafe_allow_html=True)

# Actual rows from dataframe
for _, r in filtered_df.reset_index(drop=True).iterrows():
    # Row container
    st.markdown('<div class="action-row">', unsafe_allow_html=True)

    # Table-like cells
    st.markdown(f'<div class="action-cell">{r["Sanction_ID"]}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="action-cell">{r["Stage"]}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="action-cell">{r["Status in Stage"]}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="action-cell">{r["Value"]}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="risk-pill">{r["Risk Level"]}</div>', unsafe_allow_html=True)

    # Button cell
    if st.button("View →", key=f"view_{r['Sanction_ID']}"):
        st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
        st.session_state.navigate_to_feedback = True
        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)
