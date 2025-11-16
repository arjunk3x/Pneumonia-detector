# prepare your values as before
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
<div class="initial-wrapper">

  <div class="initial-header-card">
    <div>
      <div class="initial-subtitle">Project Page</div>
      <div class="initial-header-title">{0}</div>
      <div class="initial-subtitle">
        Sanction <span class="initial-codechip">{1}</span>
      </div>
    </div>

    <div style="initial-display:flex; initial-flex-direction:column; initial-align-items:flex-end;">
      <div class="initial-subtitle">Stage</div>
      <div class="initial-stage-chip">{2}</div>
    </div>
  </div>

  <div class="initial-main-card">

    <div class="initial-grid-3" style="initial-margin-bottom:1.75rem;">
      <div>
        <div class="initial-label">ART/Delivery Vehicle</div>
        <div class="initial-value">{0}</div>
      </div>

      <div>
        <div class="initial-label">Sponsor</div>
        <div class="initial-value">{3}</div>
      </div>

      <div>
        <div class="initial-label">Amount</div>
        <div class="initial-value">{4}</div>
      </div>
    </div>

    <div class="initial-grid-3" style="initial-gap:1.25rem; initial-margin-bottom:1.75rem;">

      <div class="initial-kpi-card" style="initial-background:#ffe7d6;">
        <div class="initial-label">Amount</div>
        <div class="initial-value">{4}</div>
      </div>

      <div class="initial-kpi-card" style="initial-background:{5};">
        <div class="initial-label">Overall Status</div>
        <div class="initial-value">{6}</div>
      </div>

      <div class="initial-kpi-card" style="initial-background:{7};">
        <div class="initial-label">Risk Level</div>
        <div class="initial-value">{8}</div>
      </div>
    </div>

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
""".format(
    art_delivery,           # 0
    sid,                    # 1
    current_stage,          # 2
    s_row.get("Sponsor","-"),  # 3
    amount,                 # 4
    overall_bg,             # 5
    overall,                # 6
    risk_bg,                # 7
    risk_level,             # 8
    submitted,              # 9
    requester,              # 10
    department              # 11
)

st.markdown(html, unsafe_allow_html=True)
