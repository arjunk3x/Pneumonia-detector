 # -------------------------------
    # 🔥 Create a notification entry
    # -------------------------------
    if new_status == "Approved":
        add_notification(
            sanction_id=sid,
            team=current_stage,
            message=f"Sanction {sid} approved by {current_stage}"


def add_notification(sanction_id, team, message):
    notif_path = "notifications.csv"

    row = {
        "id": str(uuid.uuid4()),
        "sanction_id": sanction_id,
        "team": team,
        "message": message,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "read": 0
    }

    if os.path.exists(notif_path):
        df = pd.read_csv(notif_path)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(notif_path, index=False)




with action_col4:
    st.markdown("**Notifications**")

    notif_path = "notifications.csv"

    # Load notifications
    if os.path.exists(notif_path):
        df_notif = pd.read_csv(notif_path).sort_values("created_at", ascending=False)
    else:
        df_notif = pd.DataFrame(columns=["id", "sanction_id", "team", "message", "created_at", "read"])

    unread_count = df_notif[df_notif["read"] == 0].shape[0]

    with st.popover(f"Notifications ({unread_count})", use_container_width=True):
        if df_notif.empty:
            st.write("No notifications.")
        else:
            for _, row in df_notif.iterrows():
                highlight = "#F0F0F0" if row["read"] == 0 else "#FFFFFF"
                st.markdown(
                    f"""
                    <div style="padding:8px; background:{highlight}; border:1px solid #DDD; border-radius:6px; margin-bottom:6px;">
                        <b>{row['message']}</b><br>
                        <span style="font-size:0.75em; color:#666;">{row['created_at']}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    # Mark all as read after viewing
    if not df_notif.empty:
        df_notif["read"] = 1
        df_notif.to_csv(notif_path, index=False)


