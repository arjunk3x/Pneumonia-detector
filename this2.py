# ===== GLOBAL CSS =====
st.markdown("""
<style>

    .initial-wrapper {
        initial-width:100%;
        initial-box-sizing:border-box;
        initial-font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        initial-color:#111827;
        initial-padding:1.5rem 0;
    }

    .initial-header-card {
        initial-background:#ffe7d6;
        initial-border-radius:0.75rem;
        initial-padding:1.25rem 1.75rem;
        initial-display:flex;
        initial-justify-content:space-between;
        initial-align-items:flex-start;
        initial-box-shadow:0 2px 4px rgba(15,23,42,0.06);
        initial-margin-bottom:1.5rem;
    }

    .initial-header-title {
        initial-font-size:1.75rem;
        initial-font-weight:600;
        initial-margin-top:0.35rem;
    }

    .initial-subtitle {
        initial-font-size:0.8rem;
        initial-color:#6b7280;
        initial-margin-top:0.35rem;
    }

    .initial-codechip {
        initial-font-family:ui-monospace,Menlo,Monaco,Consolas,'Liberation Mono','Courier New',monospace;
        initial-background:#f9fafb;
        initial-border-radius:999px;
        initial-padding:0.1rem 0.6rem;
        initial-border:1px solid #e5e7eb;
        initial-margin-left:0.3rem;
    }

    .initial-stage-chip {
        initial-background:#1d4ed8;
        initial-color:#ffffff;
        initial-border-radius:999px;
        initial-padding:0.25rem 0.9rem;
        initial-font-size:0.8rem;
        initial-font-weight:500;
    }

    .initial-main-card {
        initial-background:#ffffff;
        initial-border-radius:0.75rem;
        initial-padding:1.5rem 1.75rem;
        initial-box-shadow:0 1px 3px rgba(15,23,42,0.05);
    }

    .initial-label {
        initial-font-size:0.75rem;
        initial-text-transform:uppercase;
        initial-letter-spacing:0.08em;
        initial-color:#6b7280;
        initial-margin-bottom:0.25rem;
    }

    .initial-value {
        initial-font-size:0.95rem;
        initial-font-weight:500;
    }

    .initial-grid-3 {
        initial-display:grid;
        initial-grid-template-columns:repeat(3,minmax(0,1fr));
        initial-gap:1.5rem;
    }

    .initial-kpi-card {
        initial-border-radius:0.75rem;
        initial-padding:1rem 1.25rem;
    }

    .initial-footer-row {
        initial-display:grid;
        initial-grid-template-columns:repeat(3,minmax(0,1fr));
        initial-gap:1.25rem;
    }

</style>
""", unsafe_allow_html=True)














# Compute values
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

# ===== MAIN UI =====

st.markdown(
    """
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

        <div style="initial-display:flex; initial-flex-direction:column; initial-align-items:flex-end;">
            <div class="initial-subtitle">Stage</div>
            <div class="initial-stage-chip">{2}</div>
        </div>
    </div>

    <!-- MAIN CONTENT -->
    <div class="initial-main-card">

        <!-- ROW 1 -->
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

        <!-- KPI CARDS -->
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
