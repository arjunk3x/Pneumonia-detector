st.markdown(
    """
    <style>
    .details-card {
        width: 100%;
        margin-top: 1rem;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    .details-card table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.9rem;
        border-radius: 0.6rem;
        overflow: hidden;
    }

    /* ===== HEADER (dark blue + white text) ===== */
    .details-card thead th {
        background: #1e3a8a;          /* dark blue header */
        color: #ffffff;               /* white text in 'Field' and 'Value' */
        padding: 0.55rem 0.75rem;
        font-weight: 600;
        text-align: left;
    }

    /* ===== BODY CELLS ===== */
    .details-card tbody td {
        padding: 0.45rem 0.75rem;
        border-top: 1px solid #edf1f7;
        color: #111827;
        background: #ffffff;
        transition: background-color 0.2s ease, color 0.2s ease;
    }

    /* zebra striping for readability */
    .details-card tbody tr:nth-child(even) td {
        background: #f9fafb;
    }

    /* ===== HOVER: ALL ROWS (blue tint + dark text) ===== */
    .details-card tbody tr:hover td {
        background: #e0edff;          /* light blue hover */
        color: #111827;
    }

    /* ===== HIGHLIGHTED ROWS (Sponsor, Amount, etc.) ===== */
    .details-card tbody tr.highlight-row td {
        background: #bfdbfe !important;  /* soft blue base */
        color: #111827 !important;
        font-weight: 600;
    }

    /* Hover effect for highlighted rows: dark blue + white text */
    .details-card tbody tr.highlight-row:hover td {
        background: #1e40af !important;  /* darker blue on hover */
        color: #ffffff !important;       /* white text on hover */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

