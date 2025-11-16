/* =============  GLOBAL RESET / BASE  ============= */
body {
    font-family: "Inter", sans-serif;
}

/* =============  SECTION WRAPPER  ============= */
.pending-wrapper {
    background: #f4f8ff;       /* very soft blue background */
    border-radius: 18px;
    padding: 20px 26px;
    border: 1px solid #d9e6ff;
    box-shadow: 0 4px 14px rgba(20, 60, 120, 0.08);
    margin-bottom: 22px;
}

/* =============  HEADERS (DARK BLUE)  ============= */
.section-title {
    font-size: 1.6rem;
    font-weight: 700;
    padding: 14px 18px;
    background: #003366;       /* dark navy blue */
    color: white;              /* white text */
    border-radius: 10px;
    margin-bottom: 10px;
    letter-spacing: 0.4px;
}

.section-subtitle {
    font-size: 1rem;
    color: #0f1c33;            /* deep grey-blue */
    margin-bottom: 14px;
}

/* =============  FILTER LABELS  ============= */
.filter-help {
    font-size: 0.9rem;
    color: #3b5b8f;
    font-weight: 500;
    margin-top: 4px;
    margin-bottom: 8px;
}

/* ============= PASTEL DATA EDITOR (TABLE) ============= */
[data-testid="stDataFrame"], 
[data-testid="stDataEditor"] {
    background: white !important;
    border-radius: 12px;
    border: 1px solid #c7d9ff;
    box-shadow: 0 2px 10px rgba(40, 80, 140, 0.10);
}

/* Table Header */
[data-testid="stDataFrame"] thead tr th,
[data-testid="stDataEditor"] thead tr th {
    font-weight: 700 !important;
    background: #e7f0ff !important;   /* light sky blue */
    color: #003366 !important;
    border-bottom: 2px solid #b8ceff !important;
    text-transform: uppercase;
    font-size: 0.85rem;
}

/* Table Rows */
[data-testid="stDataFrame"] tbody tr,
[data-testid="stDataEditor"] tbody tr {
    transition: background 0.20s ease, transform 0.10s ease;
    font-size: 0.94rem;
}

/* Hover Effect */
[data-testid="stDataFrame"] tbody tr:hover,
[data-testid="stDataEditor"] tbody tr:hover {
    background: #eef5ff !important;
    cursor: pointer;
    transform: scale(1.002);
}

/* Table Cell Text */
[data-testid="stDataFrame"] td,
[data-testid="stDataEditor"] td {
    color: #0d2247 !important;        /* dark blue text */
    padding: 8px 6px !important;
    border-bottom: 1px solid #dce7ff !important;
}

/* Risk badges inside cell text */
.badge-high {
    color: #e53935;
    font-weight: 700;
}
.badge-medium {
    color: #fb8c00;
    font-weight: 700;
}
.badge-low {
    color: #43a047;
    font-weight: 700;
}
