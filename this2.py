# 8. Feedback log (includes version from Sanction view)
import uuid
version_val = ""

# Try to read version from sanctions_df (Sanction view CSV)
if "Sanction ID" in sanctions_df.columns:
    mask_sv = sanctions_df["Sanction ID"].astype(str) == sid

    if mask_sv.any():
        # Support both 'version' and 'Version' column names
        version_col = None
        if "version" in sanctions_df.columns:
            version_col = "version"
        elif "Version" in sanctions_df.columns:
            version_col = "Version"

        if version_col is not None:
            try:
                # Take the highest numeric version for this sanction
                v = (
                    pd.to_numeric(
                        sanctions_df.loc[mask_sv, version_col],
                        errors="coerce",
                    )
                    .max()
                )
                if pd.notna(v):
                    version_val = int(v)
                else:
                    # fall back to raw value if numeric conversion fails
                    version_val = str(
                        sanctions_df.loc[mask_sv, version_col].max()
                    )
            except Exception:
                version_val = str(sanctions_df.loc[mask_sv, version_col].max())

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
