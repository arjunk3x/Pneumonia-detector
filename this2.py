elif current_stage == "DIDM":
    # DIDM -> ETIDM only if Value > £3m
    sanction_value = None
    if "Sanction ID" in sanctions_df.columns and "Value" in sanctions_df.columns:
        ms_val = sanctions_df["Sanction ID"].astype(str) == sid
        if ms_val.any():
            raw_val = str(sanctions_df.loc[ms_val, "Value"].iloc[0])
            try:
                sanction_value = float(raw_val.replace(",", ""))
            except Exception:
                sanction_value = None

    if sanction_value is not None and sanction_value > 3_000_000:
        # -------- > £3m: move into ETIDM --------
        tracker_df.loc[mask, "Current Stage"] = "ETIDM"
        tracker_df.loc[mask, "Overall_status"] = "In progress"

        # 1) initialise ETIDM stage columns
        etidm_meta = STAGE_KEYS["ETIDM"]
        etidm_status_col = etidm_meta["status"]        # e.g. "etidm_status"
        etidm_assigned_col = etidm_meta["assigned_to"] # e.g. "etidm_assigned_to"
        etidm_decision_col = etidm_meta["decision_at"] # e.g. "etidm_decision_at"

        tracker_df.loc[mask, etidm_status_col] = "Pending"
        tracker_df.loc[mask, etidm_assigned_col] = ""
        tracker_df.loc[mask, etidm_decision_col] = ""

        # 2) flags: only ETIDM is active (is_in_etidm = 1)
        for stg, m in STAGE_KEYS.items():
            flag_col = m.get("flag")
            if not flag_col:
                continue
            tracker_df.loc[mask, flag_col] = int(stg == "ETIDM")

    else:
        # -------- ≤ £3m: process ends at DIDM --------
        tracker_df.loc[mask, "Current Stage"] = "DIDM"
        tracker_df.loc[mask, "Overall_status"] = "Approved"

        didm_meta = STAGE_KEYS["DIDM"]
        didm_status_col = didm_meta["status"]          # e.g. "didm_status"
        tracker_df.loc[mask, didm_status_col] = "Approved"

        # clear all is_in_* flags (no more Actions anywhere)
        for stg, m in STAGE_KEYS.items():
            flag_col = m.get("flag")
            if not flag_col:
                continue
            tracker_df.loc[mask, flag_col] = 0
