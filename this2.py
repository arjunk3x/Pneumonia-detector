PRE_REVIEW_STAGES = [
    "Head of Data & AI",
    "DG & IA",
    "Architectural Assurance",
    "Finance",
    "Regulatory",
]

def pre_review_approval_count(row) -> int:
    """How many of the 5 pre-Digital-Guild reviewers have status == Approved?"""
    count = 0
    for stage in PRE_REVIEW_STAGES:
        status_col = STAGE_KEYS[stage]["status"]
        status_val = str(row.get(status_col, "")).lower()
        if status_val == "approved":
            count += 1
    return count

def all_pre_reviewers_approved(row) -> bool:
    """True only when *all 5* pre-reviewers have approved."""
    return pre_review_approval_count(row) == len(PRE_REVIEW_STAGES)



tracker_df.loc[mask, "pre_review_approved_count"] = pre_review_approval_count(row_now)




# after tracker_df.loc[mask, meta["status"]] = new_status, etc.

row_now = tracker_df.loc[mask].iloc[0]  # fresh snapshot including this decision

if new_status == "Approved":
    tracker_df.loc[mask, decision_field] = when

    # PRE-DIGITAL GUILD reviewers
    if current_stage in PRE_REVIEW_STAGES:
        # Optional: keep a numeric count column
        tracker_df.loc[mask, "pre_review_approved_count"] = pre_review_approval_count(row_now)

        if all_pre_reviewers_approved(row_now):
            # All 5 have approved -> move into Digital Guild
            tracker_df.loc[mask, "Current Stage"] = "Digital Guild"
            tracker_df.loc[mask, "Overall_status"] = "In progress"

            # only Digital Guild sees it in Actions now
            for label, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (label == "Digital Guild")
        else:
            # Still waiting for other teams -> do NOT change Current Stage or flags
            pass

    # DIGITAL GUILD -> ETIDM
    elif current_stage == "Digital Guild":
        # this is your "digital guild flag == 1" condition in practice:
        # they clicked Approve on their own stage.
        tracker_df.loc[mask, "Current Stage"] = "ETIDM"
        tracker_df.loc[mask, "Overall_status"] = "In progress"
        for label, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (label == "ETIDM")

    # ETIDM -> fully approved
    elif current_stage == "ETIDM":
        tracker_df.loc[mask, "Overall_status"] = "Approved"
        for label, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False








# =========================================================
# BACKEND SUBMISSION LOGIC
# =========================================================
if submitted:

    # 0. Permission guard
    if not role_can_act:
        st.error("You are not authorised to perform this action.")
        st.stop()

    # 1. Ensure tracker row exists
    if tracker_df.empty:
        tracker_df = pd.DataFrame([{"Sanction_ID": sid}])

    if "Sanction_ID" not in tracker_df.columns:
        tracker_df["Sanction_ID"] = ""

    if sid not in tracker_df["Sanction_ID"].astype(str).tolist():
        tracker_df = pd.concat(
            [tracker_df, pd.DataFrame([{"Sanction_ID": sid}])],
            ignore_index=True,
        )

    mask = tracker_df["Sanction_ID"].astype(str) == sid
    tracker_df = _ensure_tracker_columns(tracker_df)
    _row = tracker_df.loc[mask].iloc[0]

    meta = STAGE_KEYS[current_stage]
    prev_status = str(_row.get(meta["status"], ""))

    # 2. Map decision -> new_status
    dec_lower = decision.lower()
    if "approve" in dec_lower:
        new_status = "Approved"
    elif "reject" in dec_lower:
        new_status = "Rejected"
    else:
        new_status = "Changes requested"

    # Prevent re-submission when already rejected
    if new_status == "Rejected" and prev_status == "Rejected":
        st.warning("This sanction has already been rejected. "
                   "No further actions can be taken.")
        st.stop()

    # 3. Basic field updates
    tracker_df.loc[mask, meta["status"]] = new_status
    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
    tracker_df.loc[mask, "last_comment"] = comment

    # 4. Stage progression logic (parallel pre-review + DG + ETIDM)
    flag_field = meta["flag"]
    decision_field = meta["decision_at"]

    # ---------- APPROVED ----------
    if new_status == "Approved":
        # Mark this stage as completed
        tracker_df.loc[mask, decision_field] = when
        tracker_df.loc[mask, flag_field] = False

        # Re-read the row after updating this stage
        row_after = tracker_df.loc[mask].iloc[0]

        if current_stage in PRE_REVIEW_STAGES:
            # Parallel reviewers: only move on when ALL 5 have approved
            if all_pre_reviewers_approved(row_after):
                # Move into Digital Guild
                tracker_df.loc[mask, "Current Stage"] = "DigitalGuild"
                tracker_df.loc[mask, "Overall_status"] = "In progress"

                # Turn on only Digital Guild flag
                for stg, m in STAGE_KEYS.items():
                    tracker_df.loc[mask, m["flag"]] = (stg == "DigitalGuild")
            else:
                # Still waiting for other reviewers
                tracker_df.loc[mask, "Overall_status"] = "In progress"

        elif current_stage == "DigitalGuild":
            # Digital Guild -> ETIDM
            tracker_df.loc[mask, "Current Stage"] = "ETIDM"
            tracker_df.loc[mask, "Overall_status"] = "In progress"
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = (stg == "ETIDM")

        elif current_stage == "ETIDM":
            # Final approval
            tracker_df.loc[mask, "Overall_status"] = "Approved"
            for stg, m in STAGE_KEYS.items():
                tracker_df.loc[mask, m["flag"]] = False

    # ---------- REJECTED ----------
    elif new_status == "Rejected":
        tracker_df.loc[mask, decision_field] = ""
        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"] = current_stage
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = False

    # ---------- REQUEST CHANGES ----------
    else:  # "Changes requested"
        tracker_df.loc[mask, decision_field] = ""
        tracker_df.loc[mask, "Overall_status"] = "Changes requested"
        tracker_df.loc[mask, "Current Stage"] = current_stage
        for stg, m in STAGE_KEYS.items():
            tracker_df.loc[mask, m["flag"]] = (stg == current_stage)

    # 5. Persist tracker CSV (ONLY here)
    _write_csv(tracker_df, APPROVER_TRACKER_PATH)

    # 6. Mirror to sanctions view
    if "Sanction ID" in sanctions_df.columns:
        ms = sanctions_df["Sanction ID"].astype(str) == sid
        sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
        sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
        _write_csv(sanctions_df, SANCTIONS_PATH)

    # 7. Notifications (ONLY inside submit)
    if new_status == "Approved":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} approved by {current_stage}.",
        )
    elif new_status == "Rejected":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} rejected by {current_stage}.",
        )

    # 8. Feedback log (CSV with comment_id, sanction_id, stage, rating, comment, username, created_at)
    import uuid
    feedback = {
        "comment_id": str(uuid.uuid4()),
        "sanction_id": sid,
        "stage": current_stage,
        "rating": rating,
        "comment": comment,
        "username": assigned_to,
        "created_at": _now_iso(),
    }
    save_fb(feedback)

    # 9. Finish
    st.success(f"Saved decision for {sid} at {current_stage_label}: {new_status}")
    st.toast("Updated ✓")
    st.stop()











st.markdown(
    f"""
    <div class="section-title" style="font-size: 28px; font-weight: bold;">
        Actions in {role_display_name(current_role)}
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="actions-divider"></div>', unsafe_allow_html=True)

for _, r in filtered_df.reset_index(drop=True).iterrows():
    risk_value = str(r["Risk Level"]).lower()
    if "high" in risk_value or "red" in risk_value:
        risk_class = "risk-high"
    elif "med" in risk_value or "amber" in risk_value:
        risk_class = "risk-medium"
    else:
        risk_class = "risk-low"

    status_value = str(r["Status in Stage"]).lower()
    if "approved" in status_value:
        status_class = "status-approved"
    elif "reject" in status_value:
        status_class = "status-rejected"
    else:
        status_class = "status-pending"

    st.markdown('<hr style="border: 0.5px solid #d3d3d3;">', unsafe_allow_html=True)

    c1, c2 = st.columns([5, 1])

    with c1:
        st.markdown(
            f"""
            <div class="action-header">
                <div class="action-title">
                    <span class="action-title-icon">📄</span>
                    <span>Sanction ID: {r['Sanction_ID']}</span>
                    <span class="action-pill-stage">Stage: {r['Stage']}</span>
                </div>
                <div class="status-pill {status_class}">
                    {r['Status in Stage']}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            f"""
            <div class="detail-row">
                <div>
                    <span class="value-pill">£ {r['Value']}</span>
                </div>
                <div>
                    <span class="risk-pill {risk_class}">
                        {r['Risk Level']}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        if st.button("View ➜", key=f"view_{r['Sanction_ID']}"):
            st.session_state["selected_sanction_id"] = str(r["Sanction_ID"])
            st.session_state.navigate_to_feedback = True
            st.rerun()

st.markdown("</div>", unsafe_allow_html=True)
