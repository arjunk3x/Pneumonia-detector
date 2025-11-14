# ================================
# STAGE ACTIONS – Sticky Action Bar
# ================================

# Pastel CSS for this section
st.markdown(
    """
    <style>
    /* Outer wrapper – light pastel card, can be sticky if desired */
    .stage-actions-wrapper {
        background: #fdfbff; /* soft lilac */
        border-radius: 18px;
        padding: 16px 20px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 4px 14px rgba(148, 163, 184, 0.35);
        margin-bottom: 22px;
        position: sticky;
        top: 0;
        z-index: 20;
        backdrop-filter: blur(6px);
    }

    .stage-actions-header-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 12px;
        margin-bottom: 10px;
    }

    .stage-actions-title {
        font-size: 1.1rem;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: #111827;
        margin-bottom: 4px;
    }

    .stage-actions-sub {
        font-size: 0.9rem;
        color: #6b7280;
        max-width: 520px;
    }

    .stage-pill-label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #9ca3af;
    }

    /* Status badge */
    .badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 4px 11px;
        border-radius: 999px;
        font-size: 0.8rem;
        font-weight: 600;
        background: #e0f2fe;
        color: #0369a1;
        margin-top: 4px;
    }

    /* Role chip */
    .role-chip {
        display: inline-flex;
        align-items: center;
        padding: 4px 11px;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 600;
        background: #ecfeff;
        color: #0f766e;
        border: 1px solid #a5f3fc;
        gap: 6px;
        margin-top: 4px;
    }

    /* Inner form block */
    .stage-actions-form {
        background: #eef2ff;
        border-radius: 14px;
        padding: 12px 14px 14px 14px;
        border: 1px solid #e0e7ff;
        box-shadow: inset 0 1px 3px rgba(79, 70, 229, 0.12);
        margin-top: 8px;
    }

    /* Make labels slightly stronger for this section */
    .stage-actions-form label[data-testid="stWidgetLabel"] > div {
        font-weight: 600;
        color: #4338ca !important;
    }

    /* Buttons (submit / reset) – pastel pills */
    .stButton > button {
        background: linear-gradient(135deg, #c7d2fe, #e0e7ff);
        color: #1e293b;
        border-radius: 999px;
        border: none;
        padding: 0.45rem 1.1rem;
        font-size: 0.9rem;
        font-weight: 600;
        box-shadow: 0 4px 10px rgba(148, 163, 184, 0.35);
        transition: transform 0.1s ease, box-shadow 0.1s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 18px rgba(148, 163, 184, 0.55);
    }
    .stButton > button:active {
        transform: translateY(0) scale(0.99);
        box-shadow: 0 3px 8px rgba(148, 163, 184, 0.45);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if current_stage not in STAGE_KEYS:
    st.subheader(f"Stage Actions – {current_stage}")
    st.info("This stage has no configured actions.")
else:
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta["status"], "Pending"))

    # Who is allowed?
    user_internal_role = _current_internal_role()        # e.g. "DataGuild"
    user_stage_label = _current_stage_label_for_role()   # e.g. "Data Guild"
    role_can_act = (user_stage_label == current_stage)

    with st.container():
        st.markdown('<div class="stage-actions-wrapper">', unsafe_allow_html=True)

        # ---------- HEADER / STATUS ROW ----------
        st.markdown(
            f"""
            <div class="stage-actions-header-row">
                <div>
                    <div class="stage-actions-title">
                        Stage Actions – {current_stage}
                    </div>
                    <div class="stage-actions-sub">
                        Decide what to do with this sanction at the current stage,
                        optionally assign it to someone, and add comments for the audit trail.
                    </div>
                </div>
                <div>
                    <div class="stage-pill-label">CURRENT STATUS</div>
                    <span class="badge">{existing_status}</span>
                </div>
            </div>

            <div>
                <span class="stage-pill-label">YOUR ROLE</span><br/>
                <span class="role-chip">
                    👤 {user_stage_label or user_internal_role}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if not role_can_act:
            st.warning(
                f"Your role (**{user_stage_label}**) cannot act on **{current_stage}**.  "
                "Only the owning team may approve/reject/request changes for this stage."
            )

        # ---------- FORM ----------
        with st.form(f"form_{current_stage}"):
            st.markdown('<div class="stage-actions-form">', unsafe_allow_html=True)

            colA, colB, colC = st.columns((1.2, 1, 1))

            with colA:
                decision = st.radio(
                    "Decision",
                    ["Approve ✅", "Reject ⛔", "Request changes ✏️"],
                    index=0,
                    disabled=not role_can_act,
                )

            with colB:
                assigned_to = st.text_input(
                    "Assign to (email or name)",
                    value=str(t_row.get(meta["assigned_to"], "")),
                    disabled=not role_can_act,
                )

            with colC:
                when = st.text_input(
                    "Decision time",
                    value=value_now_iso(),
                    help="Auto-filled; can be edited",
                    disabled=not role_can_act,
                )

            comment = st.text_area(
                "Comments / Rationale",
                placeholder="Add context for the audit trail (optional)",
                disabled=not role_can_act,
            )

            st.markdown("</div>", unsafe_allow_html=True)  # close .stage-actions-form

            # Buttons row
            c1, c2, _ = st.columns((0.4, 0.4, 0.2))
            with c1:
                submitted = st.form_submit_button(
                    "Submit decision",
                    use_container_width=True,
                    disabled=not role_can_act,
                )
            with c2:
                cancel = st.form_submit_button(
                    "Reset form",
                    use_container_width=True,
                    disabled=not role_can_act,
                )

        st.markdown("</div>", unsafe_allow_html=True)  # close .stage-actions-wrapper

# ---- keep your existing server-side logic here ----
# e.g.
# if submitted:
#     if not role_can_act:
#         st.error("Action blocked: your role cannot act on this stage.")
#         st.stop()
#     ...
#     (tracker updates, CSV writes, st.success, st.toast, st.rerun, etc.)
