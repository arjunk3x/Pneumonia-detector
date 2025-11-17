rating = st.selectbox(
    "Rating (optional)", 
    ["", "⭐️", "⭐️⭐️", "⭐️⭐️⭐️", "⭐️⭐️⭐️⭐️", "⭐️⭐️⭐️⭐️⭐️"]
)





import uuid

# Build feedback record
feedback_df = pd.DataFrame([{
    "comment_id": str(uuid.uuid4()),
    "sanction_id": sid,
    "stage": current_stage,
    "rating": rating,
    "comment": comment,
    "username": assigned_to,  # using assigned_to as “username” as you said
    "created_at": _now_iso()
}])




csv_data = feedback_df.to_csv(index=False)

st.download_button(
    label="⬇️ Download Feedback CSV",
    data=csv_data,
    file_name=f"feedback_{sid}.csv",
    mime="text/csv"
)






import uuid

# ---- FEEDBACK CSV GENERATION ----
feedback_df = pd.DataFrame([{
    "comment_id": str(uuid.uuid4()),
    "sanction_id": sid,
    "stage": current_stage,
    "rating": rating,
    "comment": comment,
    "username": assigned_to,
    "created_at": _now_iso()
}])

csv_data = feedback_df.to_csv(index=False)

st.download_button(
    label="⬇️ Download Feedback CSV",
    data=csv_data,
    file_name=f"feedback_{sid}.csv",
    mime="text/csv",
    use_container_width=True
)

























# =========================
# STAGE ACTIONS — Sticky Action Bar (UPDATED)
# =========================
st.subheader(f"Stage Actions — {current_stage}")
if current_stage not in STAGE_KEYS:
    st.info("This stage has no configured actions.")
else:
    meta = STAGE_KEYS[current_stage]
    existing_status = str(t_row.get(meta["status"], "Pending"))

    with st.container():
        st.markdown('<div class="sticky-actions">', unsafe_allow_html=True)

        st.write(
            f"Current status: <span class='badge {_pill_class(existing_status)}'>{existing_status}</span>",
            unsafe_allow_html=True
        )

        role = st.session_state.get("role", "")
        role_can_act = True  # change logic if you need strict roles

        if not role_can_act:
            st.warning(f"Your role ({role}) cannot act on {current_stage}.")
        else:
            with st.form(f"form_{current_stage}"):

                colA, colB, colC = st.columns([1.2, 1, 1])

                # --- Decision select ---
                with colA:
                    decision = st.radio(
                        "Decision",
                        ["Approve ✅", "Reject ⛔", "Request changes ✍️"],
                        index=0
                    )

                with colB:
                    assigned_to = st.text_input(
                        "Assign to (email or name)",
                        value=str(t_row.get(meta["assigned_to"], ""))
                    )

                with colC:
                    when = st.text_input(
                        "Decision time",
                        value=_now_iso(),
                        help="Auto-filled; can be edited"
                    )

                # --- ⭐ Star Rating (numeric) ---
                rating_stars = st.selectbox(
                    "Rating",
                    ["", "⭐️", "⭐️⭐️", "⭐️⭐️⭐️", "⭐️⭐️⭐️⭐️", "⭐️⭐️⭐️⭐️⭐️"],
                    index=0
                )
                rating = len(rating_stars)  # converts ⭐⭐⭐ to 3

                # --- Comment ---
                comment = st.text_area(
                    "Comments / Rationale",
                    placeholder="Add context for the audit trail (optional)"
                )

                c1, c2, _ = st.columns([0.4, 0.4, 0.2])

                with c1:
                    submitted = st.form_submit_button("Submit decision", use_container_width=True)

                with c2:
                    cancel = st.form_submit_button("Reset form", use_container_width=True)

                if submitted:
                    # Ensure row exists
                    if t_row.empty:
                        tracker_df = pd.concat([
                            tracker_df,
                            pd.DataFrame([{"Sanction_ID": sid}])
                        ], ignore_index=True)
                        t_row = tracker_df.loc[tracker_df["Sanction_ID"] == sid].iloc[0]

                    tracker_df = _ensure_tracker_columns(tracker_df)

                    dec_lower = decision.lower()
                    new_status = (
                        "Approved"
                        if "approve" in dec_lower else
                        ("Rejected" if "reject" in dec_lower else "Changes requested")
                    )

                    mask = tracker_df["Sanction_ID"] == sid
                    tracker_df.loc[mask, meta["status"]] = new_status
                    tracker_df.loc[mask, meta["assigned_to"]] = assigned_to
                    tracker_df.loc[mask, meta["decision_at"]] = when or _now_iso()
                    tracker_df.loc[mask, "Last_comment"] = comment

                    nxt = _next_stage(current_stage) if new_status == "Approved" else None

                    if new_status == "Approved" and nxt:
                        tracker_df.loc[mask, "Current Stage"] = nxt
                        tracker_df.loc[mask, "Overall_status"] = "In progress"
                        for stg, m in STAGE_KEYS.items():
                            tracker_df.loc[mask, m["flag"]] = (stg == nxt)

                    elif new_status == "Rejected":
                        tracker_df.loc[mask, "Overall_status"] = "Rejected"

                    else:
                        tracker_df.loc[mask, "Overall_status"] = "Changes requested"

                    # Persist tracker
                    try:
                        _write_csv(tracker_df, APPROVER_TRACKER_PATH)
                    except Exception as e:
                        st.error(f"Failed to update {APPROVER_TRACKER_PATH}: {e}")

                    # Mirror to sanctions_data
                    try:
                        if "Sanction ID" in sanctions_df.columns:
                            ms = sanctions_df["Sanction ID"] == sid
                            if "Current Stage" in sanctions_df.columns:
                                sanctions_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
                            if "Status" in sanctions_df.columns:
                                sanctions_df.loc[ms, "Status"] = tracker_df.loc[mask, "Overall_status"].iloc[0]
                            _write_csv(sanctions_df, SANCTIONS_PATH)
                    except Exception as e:
                        st.warning(f"Could not update {SANCTIONS_PATH}: {e}")

                    # --- 🔥 Store feedback for download after rerun ---
                    import uuid
                    st.session_state["latest_feedback"] = {
                        "comment_id": str(uuid.uuid4()),
                        "sanction_id": sid,
                        "stage": current_stage,
                        "rating": rating,            # numeric
                        "comment": comment,
                        "username": assigned_to,
                        "created_at": _now_iso()
                    }

                    st.success(f"Saved decision for {sid} at {current_stage}: **{new_status}**")
                    st.toast("Updated ✅")

                    st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

# =========================
# FEEDBACK CSV DOWNLOAD (after rerun)
# =========================
if "latest_feedback" in st.session_state:
    fb = st.session_state["latest_feedback"]
    feedback_df = pd.DataFrame([fb])

    st.success("Feedback submitted. Download CSV below 👇")

    st.download_button(
        label="⬇️ Download Feedback CSV",
        data=feedback_df.to_csv(index=False),
        file_name=f"feedback_{fb['sanction_id']}.csv",
        mime="text/csv",
        use_container_width=True
    )

