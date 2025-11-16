/* ===========================
   HEADER BAR (Dark Blue)
   =========================== */
[data-testid="stDataFrame"] thead th,
[data-testid="stDataEditor"] thead th {
    background-color: #1f3a65 !important;    /* dark navy blue */
    color: white !important;
    font-weight: 700 !important;
    font-size: 0.92rem !important;
    padding: 10px 6px !important;
    border-bottom: 2px solid #102544 !important;
}

/* ===========================
   ALTERNATING ROW COLORS
   =========================== */
[data-testid="stDataFrame"] tbody tr:nth-child(odd),
[data-testid="stDataEditor"] tbody tr:nth-child(odd) {
    background-color: #f7f8fb !important;    /* light grey-white */
}

[data-testid="stDataFrame"] tbody tr:nth-child(even),
[data-testid="stDataEditor"] tbody tr:nth-child(even) {
    background-color: #eef4ff !important;    /* very soft blue */
}

/* Highlight specific rows (optional, matches screenshot) */
.highlight-yellow {
    background-color: #f7f1d1 !important;    /* pale cream */
}

.highlight-blue {
    background-color: #d6e4ff !important;    /* soft blue section */
}

/* ===========================
   TABLE BORDERS & TEXT
   =========================== */
[data-testid="stDataFrame"] td,
[data-testid="stDataEditor"] td {
    color: #2b3b57 !important;
    font-size: 0.93rem !important;
    padding: 8px 10px !important;
    border-bottom: 1px solid #d8e2f2 !important;
}

/* ===========================
   HOVER EFFECT (matching example)
   =========================== */
[data-testid="stDataFrame"] tbody tr:hover,
[data-testid="stDataEditor"] tbody tr:hover {
    background-color: #dce9ff !important;   /* soft bright blue */
    transition: background 0.15s ease-in-out;
    cursor: pointer;
}

/* ===========================
   TABLE CONTAINER
   =========================== */
[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {
    border: 1px solid #c7d3e8 !important;
    border-radius: 10px;
    overflow: hidden !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.05);
}
