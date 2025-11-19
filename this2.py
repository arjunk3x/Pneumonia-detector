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



def stage_block(stage_label: str, tr: pd.Series, current_stage: str) -> str:
    meta = STAGE_KEYS[stage_label]

    status = str(tr.get(meta["status"], "Pending"))
    assigned = str(tr.get(meta["assigned_to"], "")) or "--"
    decided = str(tr.get(meta["decision_at"], "")) or "--"

    # ========================================
    # STATUS COLOR USING PILL CLASS
    # ========================================
    # pill_class() already supports metadata classes for:
    # approved → green
    # rejected → red
    # pending → grey
    # changes requested → yellow (if defined in your CSS)
    cls = _pill_class(status)   # <span class="pill {cls}">...</span>

    # ========================================
    # STEP BLOCK COLOR (BIG BOX)
    # ========================================
    # Your step box uses: active / done / rejected / default
    status_lower = status.lower()

    if current_stage == stage_label:
        state = "active"              # Blue-ish current stage
    elif status_lower == "approved":
        state = "done"                # Green big box
    elif status_lower == "rejected":
        state = "rejected"            # NEW → Red big box
    else:
        state = ""                    # Pending or Changes requested

    # ========================================
    # ICON MAP
    # ========================================
    icon = {
        "SDA": "🟦",
        "Data Guild": "🟪",
        "Digital Guild": "🟧",
        "ETIDM": "🟩"
    }.get(stage_label, "⭐")

    # ========================================
    # HTML OUTPUT
    # ========================================
    return f"""
    <div class="step {state}">
        <div class="title">{icon} {stage_label}</div>

        <div class="meta">
            Status:
            <span class="pill {cls}">{status}</span>
        </div>

        <div class="row">
            <div class="lbl">Assigned</div>
            <div class="val">{assigned}</div>
        </div>

        <div class="row">
            <div class="lbl">Decided</div>
            <div class="val">{decided}</div>
        </div>
    </div>
    """
