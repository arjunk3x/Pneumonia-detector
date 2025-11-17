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
