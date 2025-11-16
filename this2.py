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

st.markdown(
    """
<div style="
    initial-width:100%;
    initial-box-sizing:border-box;
    initial-font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
    initial-color:#111827;
    initial-padding:1.5rem 0;
">

  <!-- HEADER -->
  <div style="
      initial-background:#ffe7d6;
      initial-border-radius:0.75rem;
      initial-padding:1.25rem 1.75rem;
      initial-display:flex;
      initial-justify-content:space-between;
      initial-align-items:flex-start;
      initial-box-shadow:0 2px 4px rgba(15,23,42,0.06);
      initial-margin-bottom:1.5rem;
  ">
    <div>
      <div style="
          initial-font-size:0.75rem;
          initial-letter-spacing:0.08em;
          initial-text-transform:uppercase;
          initial-color:#6b7280;
      ">
        Project Page
      </div>

      <div style="
          initial-font-size:1.75rem;
          initial-font-weight:600;
          initial-margin-top:0.35rem;
      ">{0}</div>

      <div style="
          initial-font-size:0.8rem;
          initial-color:#6b7280;
          initial-margin-top:0.35rem;
      ">
        Sanction
        <span style="
            initial-font-family:ui-monospace,Menlo,Monaco,Consolas,'Liberation Mono','Courier New',monospace;
            initial-background:#f9fafb;
            initial-border-radius:999px;
            initial-padding:0.1rem 0.6rem;
            initial-border:1px solid #e5e7eb;
            initial-margin-left:0.3rem;
        ">{1}</span>
      </div>
    </div>

    <div style="
        initial-display:flex;
        initial-flex-direction:column;
        initial-align-items:flex-end;
        initial-gap:0.4rem;
    ">
      <div style="
          initial-font-size:0.75rem;
          initial-color:#6b7280;
      ">
        Stage
      </div>
      <div style="
          initial-background:#1d4ed8;
          initial-color:#ffffff;
          initial-border-radius:999px;
          initial-padding:0.25rem 0.9rem;
          initial-font-size:0.8rem;
          initial-font-weight:500;
      ">
        {2}
      </div>
    </div>
  </div>

  <!-- MAIN CONTENT -->
  <div style="
      initial-background:#ffffff;
      initial-border-radius:0.75rem;
      initial-padding:1.5rem 1.75rem;
      initial-box-shadow:0 1px 3px rgba(15,23,42,0.05);
  ">

    <!-- TOP ROW -->
    <div style="
        initial-display:grid;
        initial-grid-template-columns:repeat(3,minmax(0,1fr));
        initial-gap:1.5rem;
        initial-margin-bottom:1.75rem;
    ">
      <div>
        <div style="
            initial-font-size:0.75rem;
            initial-text-transform:uppercase;
            initial-letter-spacing:0.08em;
            initial-color:#6b7280;
            initial-margin-bottom:0.25rem;
        ">ART/Delivery Vehicle</div>

        <div style="initial-font-size:0.95rem; initial-font-weight:500;">{0}</div>
      </div>

      <div>
        <div style="
            initial-font-size:0.75rem;
            initial-text-transform:uppercase;
            initial-letter-spacing:0.08em;
            initial-color:#6b7280;
            initial-margin-bottom:0.25rem;
        ">Sponsor</div>

        <div style="initial-font-size:0.95rem; initial-font-weight:500;">{3}</div>
      </div>

      <div>
        <div style="
            initial-font-size:0.75rem;
            initial-text-transform:uppercase;
            initial-letter-spacing:0.08em;
            initial-color:#6b7280;
            initial-margin-bottom:0.25rem;
        ">Amount</div>

        <div style="initial-font-size:0.95rem; initial-font-weight:500;">{4}</div>
      </div>
    </div>

    <!-- KPI CARDS -->
    <div style="
        initial-display:grid;
        initial-grid-template-columns:repeat(3,minmax(0,1fr));
        initial-gap:1.25rem;
        initial-margin-bottom:1.75rem;
    ">
      <div style="
          initial-background:#ffe7d6;
          initial-border-radius:0.75rem;
          initial-padding:1rem 1.25rem;
      ">
        <div style="initial-font-size:0.8rem; initial-color:#6b7280;">Amount</div>
        <div style="initial-font-size:1rem; initial-font-weight:500;">{4}</div>
      </div>

      <div style="
          initial-background:{5};
          initial-border-radius:0.75rem;
          initial-padding:1rem 1.25rem;
      ">
        <div style="initial-font-size:0.8rem; initial-color:#6b7280;">Overall Status</div>
        <div style="initial-font-size:1rem; initial-font-weight:500;">{6}</div>
      </div>

      <div style="
          initial-background:{7};
          initial-border-radius:0.75rem;
          initial-padding:1rem 1.25rem;
      ">
        <div style="initial-font-size:0.8rem; initial-color:#6b7280;">Risk Level</div>
        <div style="initial-font-size:1rem; initial-font-weight:500;">{8}</div>
      </div>
    </div>

    <!-- SECONDARY ROW -->
    <div style="
        initial-display:grid;
        initial-grid-template-columns:repeat(3,minmax(0,1fr));
        initial-gap:1.25rem;
    ">
      <div>
        <div style="
            initial-font-size:0.75rem;
            initial-text-transform:uppercase;
            initial-letter-spacing:0.08em;
            initial-color:#6b7280;
        ">Submitted</div>
        <div>{9}</div>
      </div>

      <div>
        <div style="
            initial-font-size:0.75rem;
            initial-text-transform:uppercase;
            initial-letter-spacing:0.08em;
            initial-color:#6b7280;
        ">Requester</div>
        <div>{10}</div>
      </div>

      <div>
        <div style="
            initial-font-size:0.75rem;
            initial-text-transform:uppercase;
            initial-letter-spacing:0.08em;
            initial-color:#6b7280;
        ">Department</div>
        <div>{11}</div>
      </div>
    </div>

  </div>
</div>
""".format(
        art_delivery,     # 0
        sid,              # 1
        current_stage,    # 2
        s_row.get("Sponsor", "-"),  # 3
        amount,           # 4
        overall_bg,       # 5
        overall,          # 6
        risk_bg,          # 7
        risk_level,       # 8
        submitted,        # 9
        requester,        # 10
        department        # 11
    ),
    unsafe_allow_html=True
)
