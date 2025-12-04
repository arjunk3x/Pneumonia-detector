    elif current_stage == "DIDM":
        # -------------------------------------------------
        # DIDM -> ETIDM only if Value > £3m
        # Value is stored in approver_tracker as column "Value"
        # -------------------------------------------------
        sanction_value = None

        if "Value" in tracker_df.columns:
            try:
                # take the value for this sanction_id
                raw_val = str(tracker_df.loc[mask, "Value"].iloc[0])
                # strip commas etc and cast to float
                sanction_value = float(raw_val.replace(",", ""))
            except Exception:
                sanction_value = None  # parsing failed, treat as unknown

        if sanction_value is not None and sanction_value > 3_000_000:
            # -------- > £3m: move into ETIDM --------
            tracker_df.loc[mask, "Current Stage"] = "ETIDM"
            tracker_df.loc[mask, "Overall_status"] = "In progress"

            # Initialise ETIDM stage columns
            etidm_meta = STAGE_KEYS["ETIDM"]
            etidm_status_col   = etidm_meta["status"]        # e.g. "etidm_status"
            etidm_assigned_col = etidm_meta["assigned_to"]   # e.g. "etidm_assigned_to"
            etidm_decision_col = etidm_meta["decision_at"]   # e.g. "etidm_decision_at"
            etidm_flag_col     = etidm_meta["flag"]          # e.g. "is_in_etidm"

            tracker_df.loc[mask, etidm_status_col]   = "Pending"
            tracker_df.loc[mask, etidm_assigned_col] = ""
            tracker_df.loc[mask, etidm_decision_col] = ""

            # flags: only ETIDM is active
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
            didm_status_col = didm_meta["status"]     # e.g. "didm_status"
            didm_flag_col   = didm_meta["flag"]       # e.g. "is_in_didm"

            # Mark DIDM as fully approved
            tracker_df.loc[mask, didm_status_col] = "Approved"

            # clear **all** flags (no more Actions anywhere)
            for stg, m in STAGE_KEYS.items():
                flag_col = m.get("flag")
                if not flag_col:
                    continue
                tracker_df.loc[mask, flag_col] = 0
