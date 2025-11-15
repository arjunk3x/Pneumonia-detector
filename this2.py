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


/* ===== HIGHLIGHTED ROWS (Sponsor, Amount, etc.) ===== */
.details-card tbody tr.highlight-row td {
    background: #FFF7E0 !important;   /* soft yellow */
}

/* Hover effect */
.details-card tbody tr.highlight-row:hover td {
    background: #FFE8B3 !important;
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
# DETAILS SECTION (LEFT SIDE)
# ================================
with left:
    st.markdown(
        '<h3 style="font-weight:700; margin-bottom:0.5rem;">Details</h3>',
        unsafe_allow_html=True,
    )

    # Build the Details dictionary
    details = {
        "Sanction ID": sid,
        "ART/Delivery Vehicle": s_row.get("ART/Delivery Vehicle", "-"),
        "Status": s_row.get("Status", t_row.get("Overall_status", "-")),
        "Sponsor": s_row.get("Sponsor", "-"),
        "Amount": amount,
        "Current Stage": current_stage,
        "Submitted": s_row.get("Submitted", t_row.get("Submitted_at", "-")),
        "Title": t_row.get("Title", "-"),
        "Currency": t_row.get("Currency", "-"),
        "Risk Level": t_row.get("Risk_Level", "-"),
        "Linked resanctions": s_row.get("Linked resanctions", "-"),
    }

    # Fields to highlight in yellow
    highlight_fields = {"Sponsor", "Amount"}

    # Build table rows
    rows_html = ""
    for field, value in details.items():
        row_class = "highlight-row" if field in highlight_fields else ""
        rows_html += f'<tr class="{row_class}"><td>{field}</td><td>{value}</td></tr>'

    # Render the table
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



