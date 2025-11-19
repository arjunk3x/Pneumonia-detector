/* ===================================================== */
/* NEW — REJECTED STATUS (RED THEME)                     */
/* ===================================================== */
.step.rejected {
    border-color: #ff4d4f !important;     /* Strong red */
    background: #ffecec !important;       /* Soft red background */
}

.step.rejected .val {
    color: #b30000 !important;            /* Darker red text */
    font-weight: 700 !important;
}

.arrow {
    display: flex;
    align-items: center;
    color: #888;
    font-size: 22px;
    padding: 0 4px;
    font-family: 'Inter', 'Segoe UI', sans-serif;
}



def _stage_block(stage, t_row, current_stage):
    stage_key = STAGE_KEYS[stage]
    status_col = stage_key["status"]
    assigned_col = stage_key["assigned_to"]
    decision_at_col = stage_key["decision_at"]

    status = str(t_row.get(status_col, "Pending"))
    assigned_to = t_row.get(assigned_col, "")
    decided_at = t_row.get(decision_at_col, "")

    # Determine CSS class
    cls = "step"
    if status == "Approved":
        cls += " done"         # GREEN
    elif status == "Rejected":
        cls += " rejected"     # RED

    html = f"""
    <div class="{cls}">
        <div class="title">{stage}</div>

        <div class="row">
            <div class="lbl">Status:</div>
            <div class="val">{status}</div>
        </div>

        <div class="row">
            <div class="lbl">Assigned:</div>
            <div class="val">{assigned_to or "--"}</div>
        </div>

        <div class="row">
            <div class="lbl">Updated:</div>
            <div class="val">{decided_at or "--"}</div>
        </div>
    </div>
    """
    return html
