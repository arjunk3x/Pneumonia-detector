@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif !important;
}


/* ===== Details table styling ===== */
.details-card {
    margin-top: 0.5rem;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08);
    background: #ffffff;
}

.details-card table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}

.details-card thead th {
    background: #E7F3D9;              /* light green header */
    padding: 0.55rem 0.75rem;
    font-weight: 600;
}

.details-card tbody td {
    padding: 0.45rem 0.75rem;
    border-top: 1px solid #edf1f7;
}

.details-card tbody tr:nth-child(even) {
    background: #fafafa;
}

/* ===== Attachments list styling ===== */
.attachments-list {
    margin-top: 0.5rem;
}

.attachment-card {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.55rem 0.8rem;
    margin-bottom: 0.45rem;
    background: #ffffff;
    border-radius: 10px;
    box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08);
}

.attachment-left {
    display: flex;
    align-items: center;
    gap: 0.55rem;
}

.attachment-icon {
    width: 26px;
    height: 26px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.9rem;
    background: #FDECEA;              /* light red background */
    color: #C53030;                    /* red icon colour */
}

.attachment-name {
    font-size: 0.9rem;
}

.attachment-download {
    font-size: 1.1rem;
    color: #9ca3af;
}






# ================================
# DETAILS + ATTACHMENTS
# ================================
left, right = st.columns([3, 2], gap="large")

# ---------- LEFT: DETAILS ----------
with left:
    st.markdown(
        '<h3 style="font-weight:700; margin-bottom:0.5rem;">Details</h3>',
        unsafe_allow_html=True,
    )

    details = {
        "Sanction ID": sid,
        "ART/Delivery Vehicle": s_row.get("ART/Delivery Vehicle", "-"),
        "Status": s_row.get("Status", t_row.get("Overall_status", "-")),
        "Sponsor": s_row.get("Sponsor", "-"),
        "Amount": amount,
        "Current Stage": current_stage,
        "Submitted": s_row.get("Submitted", t_row.get("Submitted_at", "-")),
        "Title": t_row.get("Title", "-"),
        "Currency": t_row.get("Currency", "GBP"),
        "Risk Level": t_row.get("Risk_Level", "-"),
        "Linked resanctions": s_row.get("Linked resanctions", "-"),
    }

    # Build an HTML table so we can style it like the mock
    rows_html = "".join(
        f"<tr><td>{field}</td><td>{value}</td></tr>"
        for field, value in details.items()
    )

    st.markdown(
        f"""
        <div class="details-card">
            <table>
                <thead>
                    <tr>
                        <th>Field</th>
                        <th>Value</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------- RIGHT: ATTACHMENTS ----------
with right:
    st.markdown(
        '<h3 style="font-weight:700; margin-bottom:0.5rem;">Attachments</h3>',
        unsafe_allow_html=True,
    )

    atts = s_row.get("Attachments", "")

    if pd.isna(atts) or str(atts).strip() == "":
        st.info("No attachments uploaded.")
    else:
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]

        st.markdown('<div class="attachments-list">', unsafe_allow_html=True)

        for i, a in enumerate(items, 1):
            st.markdown(
                f"""
                <div class="attachment-card">
                    <div class="attachment-left">
                        <div class="attachment-icon">📄</div>
                        <div class="attachment-name">{i}. {a}</div>
                    </div>
                    <div class="attachment-download">☁️</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown('</div>', unsafe_allow_html=True)

st.divider()

