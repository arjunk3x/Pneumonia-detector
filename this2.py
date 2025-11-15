# ==============================
# FEEDBACK HEADER + KPIs SECTION
# ==============================

project_title = s_row.get("Project_Title", s_row.get("ART/Delivery Vehicle", "Untitled"))
art_vehicle   = s_row.get("ART/Delivery Vehicle", "-")
sponsor       = s_row.get("Sponsor", "-")
amount        = _fmt_money(
    s_row.get("Amount", t_row.get("Value", None)),
    t_row.get("Currency", "USD"),
)
overall       = s_row.get("Status", t_row.get("overall_status", "In progress"))

submitted  = str(s_row.get("Submitted",  t_row.get("Submitted_at", "-")))
requester  = str(t_row.get("Requester_Email", "-"))
department = str(t_row.get("Department", "-"))
risk_level = str(t_row.get("Risk_Level", "-"))

st.markdown(
    f"""
<style>
/* === local styling only for this section (all prefixed with initial-) === */

.initial-feedback-wrapper {{
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: #222222;
  font-size: 14px;
}}

.initial-header-card {{
  background: #ffe7d6;
  border-radius: 18px;
  padding: 20px 24px;
  margin-bottom: 18px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}}

.initial-header-left-eyebrow {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: .12em;
  color: #666666;
  margin-bottom: 4px;
}}

.initial-header-title {{
  font-size: 26px;
  font-weight: 600;
  margin: 0;
}}

.initial-header-subrow {{
  margin-top: 6px;
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: #555555;
}}

.initial-codechip {{
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 11px;
  background: #ffffffaa;
  border: 1px solid #ffffffcc;
}}

.initial-stage-chip {{
  padding: 6px 12px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 500;
  background: #ffffffaa;
  border: 1px solid #ffffffcc;
}}

.initial-details-block {{
  margin-bottom: 18px;
}}

.initial-detail-row {{
  display: grid;
  grid-template-columns: 1fr;
  row-gap: 4px;
}}

.initial-detail-field {{
  margin-bottom: 6px;
}}

.initial-detail-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: .12em;
  color: #777777;
  margin-bottom: 2px;
}}

.initial-detail-value {{
  font-size: 14px;
  font-weight: 500;
}}

.initial-kpi-grid {{
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
  margin: 8px 0 18px 0;
}}

.initial-kpi-card {{
  border-radius: 14px;
  padding: 12px 16px;
  border: 1px solid #f4d7c5;
  background: #fff7f1;
}}

.initial-kpi-card-green {{
  background: #e6f7ea;
  border-color: #b9e3c4;
}}

.initial-kpi-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: .12em;
  color: #777777;
  margin-bottom: 6px;
}}

.initial-kpi-value {{
  font-size: 14px;
  font-weight: 500;
}}

.initial-pill {{
  display: inline-block;
  margin-top: 6px;
  padding: 2px 10px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 500;
  border: 1px solid transparent;
}}

.initial-pill-in-progress {{
  background: #bbf7d0;
  border-color: #4ade80;
  color: #166534;
}}

.initial-meta-grid {{
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 16px;
  margin-bottom: 12px;
}}

.initial-meta-card {{
  border-radius: 10px;
  padding: 10px 14px;
  border: 1px solid #e5e7eb;
  background: #ffffff;
}}
</style>

<div class="initial-feedback-wrapper">

  <!-- HEADER STRIP -->
  <div class="initial-header-card">
    <div>
      <div class="initial-header-left-eyebrow">Project Page</div>
      <h1 class="initial-header-title">{project_title}</h1>
      <div class="initial-header-subrow">
        <span>Sanction</span>
        <span class="initial-codechip">{sid}</span>
      </div>
    </div>
    <div class="initial-stage-chip">Stage {current_stage}</div>
  </div>

  <!-- TOP DETAILS (ART, Sponsor, Amount) -->
  <div class="initial-details-block">
    <div class="initial-detail-row">
      <div class="initial-detail-field">
        <div class="initial-detail-label">ART/Delivery Vehicle</div>
        <div class="initial-detail-value">{art_vehicle}</div>
      </div>
      <div class="initial-detail-field">
        <div class="initial-detail-label">Sponsor</div>
        <div class="initial-detail-value">{sponsor}</div>
      </div>
      <div class="initial-detail-field">
        <div class="initial-detail-label">Amount</div>
        <div class="initial-detail-value">{amount}</div>
      </div>
    </div>
  </div>

  <!-- KPI CARDS ROW (like screenshot) -->
  <div class="initial-kpi-grid">
    <div class="initial-kpi-card">
      <div class="initial-kpi-label">Amount</div>
      <div class="initial-kpi-value">{amount}</div>
      <div class="initial-pill">In progress</div>
    </div>

    <div class="initial-kpi-card initial-kpi-card-green">
      <div class="initial-kpi-label">Overall Status</div>
      <div class="initial-kpi-value">{overall}</div>
      <div class="initial-pill initial-pill-in-progress">{overall}</div>
    </div>

    <div class="initial-kpi-card">
      <div class="initial-kpi-label">Overall Status</div>
      <div class="initial-kpi-value">{overall}</div>
      <div class="initial-pill">In progress</div>
    </div>
  </div>

  <!-- META ROW (Submitted / Requester / Department / Risk Level) -->
  <div class="initial-meta-grid">
    <div class="initial-meta-card">
      <div class="initial-kpi-label">Submitted</div>
      <div class="initial-kpi-value">{submitted}</div>
    </div>
    <div class="initial-meta-card">
      <div class="initial-kpi-label">Requester</div>
      <div class="initial-kpi-value">{requester}</div>
    </div>
    <div class="initial-meta-card">
      <div class="initial-kpi-label">Department</div>
      <div class="initial-kpi-value">{department}</div>
    </div>
    <div class="initial-meta-card">
      <div class="initial-kpi-label">Risk Level</div>
      <div class="initial-kpi-value">{risk_level}</div>
    </div>
  </div>

</div>
""",
    unsafe_allow_html=True,
)

st.divider()
