import streamlit as st
import streamlit.components.v1 as components

# ---- compute values from your rows ----
amount = _fmt_money(
    s_row.get("Amount", t_row.get("Value", None)),
    t_row.get("Currency", "GBP"),
)
overall = s_row.get("Status", t_row.get("overall_status", "Pending"))
art_delivery = str(s_row.get("ART/Delivery Vehicle", "Untitled"))

submitted = str(s_row.get("Submitted", t_row.get("Submitted_at", "-")))
requester = str(t_row.get("Requester_Email", "-"))
department = str(t_row.get("Department", "-"))
risk_level = str(t_row.get("Risk_Level", "-"))

def _status_bg(val):
    v = (val or "").lower()
    if any(x in v for x in ["in progress", "approved", "green", "ok"]):
        return "#bbf7d0"
    if any(x in v for x in ["high", "red", "rejected"]):
        return "#fecaca"
    if any(x in v for x in ["medium", "amber", "warning"]):
        return "#fef3c7"
    return "#f3f4f6"

overall_bg = _status_bg(overall)
risk_bg = _status_bg(risk_level)

html = """
<html>
<head>
  <style>
    .initial-wrapper {
      font-family: system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
      color:#111827;
      padding:1.5rem 0;
      box-sizing:border-box;
      width:100%;
  }

  /* =======================
     HEADER (UPDATED)
     ======================= */
  .initial-header-card {
      background:#1e3a8a;       /* DARK BLUE */
      border-radius:1.2rem;
      padding:2.5rem 2.5rem;    /* MORE PADDING – now works */
      display:flex;
      justify-content:space-between;
      align-items:center;       /* fix: FLEX START made height collapse */
      min-height:130px;         /* FORCED HEIGHT */
      box-shadow:0 4px 12px rgba(15,23,42,0.25);
      margin-bottom:2rem;
      color:white;              /* ALL TEXT WHITE */
  }

    .initial-header-title {{
        font-size:2.1rem;
        font-weight:600;
        margin-top:0.35rem;
    }}

    .initial-subtitle {{
        font-size:0.8rem;
        color:#6b7280;
        margin-top:0.35rem;
    }}

    .initial-codechip {{
        font-family: ui-monospace,Menlo,Monaco,Consolas,'Liberation Mono','Courier New',monospace;
        background:#f9fafb;
        border-radius:999px;
        padding:0.1rem 0.6rem;
        border:1px solid #e5e7eb;
        margin-left:0.3rem;
    }}

    .initial-stage-chip {{
        background:#1d4ed8;
        color:#ffffff;
        border-radius:999px;
        padding:0.25rem 0.9rem;
        font-size:0.8rem;
        font-weight:500;
    }}

    .initial-main-card {{
        background:#ffffff;
        border-radius:0.75rem;
        padding:1.5rem 1.75rem;
        box-shadow:0 1px 3px rgba(15,23,42,0.05);
    }}

    /* Details sections */
    .initial-section {{
        padding-bottom:1rem;
        margin-bottom:1rem;
        border-bottom:1px solid #e5e7eb;
        font-family:"Segoe UI", system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    }}

    .initial-label-large {{
        font-size:1.1rem;
        font-weight:500;
        color:#111827;
        margin-bottom:0.25rem;
    }}

    .initial-value {{
        font-size:1rem;
        font-weight:600;
        color:#111827;
    }}

    /* Small label */
    .initial-label {{
        font-size:0.8rem;
        text-transform:uppercase;
        letter-spacing:0.06em;
        color:#6b7280;
        margin-bottom:0.35rem;
    }}

    /* KPI row */
    .initial-kpi-row {{
        display:flex;
        gap:1.25rem;
        margin-top:0.75rem;
        margin-bottom:0.5rem;
    }}

    /* KPI card: DARK BLUE BACKGROUND + WHITE TEXT */
    .initial-kpi-card {{
        flex:1;
        border-radius:0.9rem;
        padding:1.25rem 1.5rem;
        background:#1e3a8a;  /* DARK BLUE */
        box-shadow:0 1px 2px rgba(15,23,42,0.05);
        display:flex;
        flex-direction:column;
        justify-content:center;
        color:white;   /* Make all text white */
    }}

    .initial-kpi-card .initial-label {{
        color:white;   /* White label text */
    }}

    .initial-kpi-card .initial-value {{
        color:white;   /* White value text */
    }}

    .initial-footer-row {{
        display:grid;
        grid-template-columns:repeat(3,minmax(0,1fr));
        gap:1.25rem;
        margin-top:1.5rem;
    }}
  </style>
</head>
<body>
<div class="initial-wrapper">

  <!-- HEADER -->
  <div class="initial-header-card">
    <div>
      <div class="initial-subtitle">Project Page</div>
      <div class="initial-header-title">{0}</div>
      <div class="initial-subtitle">
        Sanction <span class="initial-codechip">{1}</span>
      </div>
    </div>

    <div style="display:flex; flex-direction:column; align-items:flex-end;">
      <div class="initial-subtitle">Stage</div>
      <div class="initial-stage-chip">{2}</div>
    </div>
  </div>

  <!-- MAIN CARD -->
  <div class="initial-main-card">

    <div class="initial-section">
      <div class="initial-label-large">ART/Delivery Vehicle</div>
      <div class="initial-value">{0}</div>
    </div>

    <div class="initial-section">
      <div class="initial-label-large">Sponsor</div>
      <div class="initial-value">{3}</div>
    </div>

    <div class="initial-section">
      <div class="initial-label-large">Amount</div>
      <div class="initial-value">{4}</div>
    </div>

    <!-- KPI CARDS (DARK BLUE) -->
    <div class="initial-kpi-row">

      <div class="initial-kpi-card">
        <div class="initial-label">Amount</div>
        <div class="initial-value">{4}</div>
      </div>

      <div class="initial-kpi-card">
        <div class="initial-label">Overall Status</div>
        <div class="initial-value">{6}</div>
      </div>

      <div class="initial-kpi-card">
        <div class="initial-label">Risk Level</div>
        <div class="initial-value">{8}</div>
      </div>

    </div>

    <!-- FOOTER DETAILS -->
    <div class="initial-footer-row">
      <div>
        <div class="initial-label">Submitted</div>
        <div class="initial-value">{9}</div>
      </div>

      <div>
        <div class="initial-label">Requester</div>
        <div class="initial-value">{10}</div>
      </div>

      <div>
        <div class="initial-label">Department</div>
        <div class="initial-value">{11}</div>
      </div>
    </div>

  </div>
</div>
</body>
</html>
""".format(
    art_delivery,              # 0
    sid,                       # 1
    current_stage,             # 2
    s_row.get("Sponsor","-"),  # 3
    amount,                    # 4
    overall_bg,                # 5
    overall,                   # 6
    risk_bg,                   # 7
    risk_level,                # 8
    submitted,                 # 9
    requester,                 # 10
    department                 # 11
)

components.html(html, height=900, scrolling=True)

