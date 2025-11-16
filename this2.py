/* ============================
   DETAILS CARD CONTAINER
============================ */
.details-card {
    margin-top: 0.5rem;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08);
    background: #ffffff;
}

/* ============================
   TABLE GENERAL
============================ */
.details-card table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}

/* ============================
   TABLE HEADER (MAKE IT DARK BLUE + WHITE TEXT)
============================ */
.details-card thead th {
    background: #0B1F4A !important;   /* Dark Navy Blue */
    color: white !important;          /* White text */
    padding: 0.55rem 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* ============================
   TABLE BODY CELLS
============================ */
.details-card tbody td {
    padding: 0.45rem 0.75rem;
    border-top: 1px solid #dfe3ef;
    color: #111827;
}

/* Alternate row coloring */
.details-card tbody tr:nth-child(even) {
    background: #f8f9fc;
}

/* ============================
   HIGHLIGHTED ROWS (Sponsor, Amount)
============================ */
.details-card tbody tr.highlight-row td {
    background: #FFF7E0 !important;
    font-weight: 600;
}

/* Hover effect for highlighted rows */
.details-card tbody tr.highlight-row:hover td {
    background: #FFE8B3 !important;
    transition: 0.3s;
}

/* ============================
   NEW HOVER EFFECT FOR ALL ROWS
   (Blue-white transition)
============================ */
.details-card tbody tr:hover td {
    background: #E5EEFF !important;   /* Light soft blue */
    color: #0B1F4A !important;         /* Navy text */
    transition: all 0.25s ease;
}
