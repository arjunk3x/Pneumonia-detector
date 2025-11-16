<style>

/* =========================================================
   GLOBAL BASE FONT
   ========================================================= */
body {
    font-family: "Inter", sans-serif;
}

/* =========================================================
   KEEP THESE EXACTLY (requested)
   ========================================================= */
.pending-wrapper {
    background: #f4f8ff;
    border-radius: 18px;
    padding: 20px 26px;
    border: 1px solid #d9e6ff;
    box-shadow: 0 4px 14px rgba(20, 60, 120, 0.08);
    margin-bottom: 22px;
}

.section-title {
    font-size: 28px;
    font-weight: 700;
    margin-bottom: 10px;
    color: white;
    background: #003366;
    padding: 14px 18px;
    border-radius: 10px;
}

.section-subtitle {
    font-size: 1rem;
    color: #0f1c33;
    margin-bottom: 14px;
}

/* =========================================================
   FILTER LABEL
   ========================================================= */
.pendingtable-filter {
    font-size: 0.9rem;
    font-weight: 600;
    color: #3b5b8f;
    margin-bottom: 6px;
}

/* =========================================================
   PENDINGTABLE TABLE CONTAINER
   ========================================================= */
.pendingtable-container,
[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {
    background: white !important;
    border-radius: 12px !important;
    border: 1px solid #c7d9ff !important;
    box-shadow: 0 2px 10px rgba(40, 80, 140, 0.10) !important;
    overflow: hidden !important;
}

/* =========================================================
   TABLE HEADER — pendingtable terminology
   ========================================================= */
[data-testid="stDataFrame"] thead th,
[data-testid="stDataEditor"] thead th,
.pendingtable-header-cell {
    background-color: #1f3a65 !important;
    color: white !important;
    text-transform: uppercase;
    font-size: 0.88rem !important;
    font-weight: 700 !important;
    padding: 10px 6px !important;
    border-bottom: 2px solid #102544 !important;
}

/* =========================================================
   ALTERNATING ROW COLORS
   ========================================================= */
.pendingtable-row-even,
[data-testid="stDataFrame"] tbody tr:nth-child(even),
[data-testid="stDataEditor"] tbody tr:nth-child(even) {
    background-color: #eef4ff !important;
}

.pendingtable-row-odd,
[data-testid="stDataFrame"] tbody tr:nth-child(odd),
[data-testid="stDataEditor"] tbody tr:nth-child(odd) {
    background-color: #f7f8fb !important;
}

/* =========================================================
   HOVER EFFECT (renamed)
   ========================================================= */
.pendingtable-hover:hover,
[data-testid="stDataFrame"] tbody tr:hover,
[data-testid="stDataEditor"] tbody tr:hover {
    background-color: #dce9ff !important;
    cursor: pointer;
    transition: 0.15s ease-in-out;
}

/* =========================================================
   CELL TEXT (pendingtable terminology)
   ========================================================= */
.pendingtable-cell,
[data-testid="stDataFrame"] td,
[data-testid="stDataEditor"] td {
    color: #0d2247 !important;
    padding: 8px 10px !important;
    font-size: 0.95rem !important;
    border-bottom: 1px solid #d8e2f2 !important;
}

/* =========================================================
   RISK BADGES (pendingtable naming)
   ========================================================= */
.pendingtable-badge-high {
    color: #e53935 !important;
    font-weight: 700 !important;
}

.pendingtable-badge-medium {
    color: #fb8c00 !important;
    font-weight: 700 !important;
}

.pendingtable-badge-low {
    color: #43a047 !important;
    font-weight: 700 !important;
}

/* =========================================================
   OPTIONAL HIGHLIGHT ROWS
   ========================================================= */
.pendingtable-highlight-yellow {
    background-color: #fff6d5 !important;
}

.pendingtable-highlight-blue {
    background-color: #dce8ff !important;
}

/* =========================================================
   OPTIONAL PENDINGTABLE TITLE BAR (if needed later)
   ========================================================= */
.pendingtable-headerbar {
    padding: 10px 14px;
    background: #003366;
    color: white;
    font-size: 1.2rem;
    font-weight: 700;
    border-radius: 10px;
}

</style>
