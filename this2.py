import uuid

# ---- VERSION LOOKUP FROM SANCTION VIEW ----
version_val = ""

try:
    # make sure df is not empty
    if sanctions_df is not None and not sanctions_df.empty:
        # ID column is Sanction_ID in sanction_view
        if "Sanction_ID" in sanctions_df.columns and "Version" in sanctions_df.columns:
            sid_str = str(sid).strip()

            mask_sv = sanctions_df["Sanction_ID"].astype(str).str.strip() == sid_str

            if mask_sv.any():
                # take highest numeric Version for this sanction
                v = pd.to_numeric(
                    sanctions_df.loc[mask_sv, "Version"],
                    errors="coerce",
                ).max()

                if pd.notna(v):
                    version_val = int(v)
                else:
                    # fallback to raw value if numeric fails
                    version_val = str(
                        sanctions_df.loc[mask_sv, "Version"].max()
                    )
except Exception as e:
    # optional debug
    st.write("DEBUG version lookup error:", e)

# ---- BUILD FEEDBACK ROW ----
feedback = {
    "sanction_id": sid,
    "stage": current_stage,
    "rating": rating,
    "comment": comment,
    "username": assigned_to,
    "version": version_val,
    "created_at": _now_iso(),
}

save_fb(feedback)
