if role == "DIDM":          # <-- existing branch, logic updated
    return (
        f"{flag_true_sql('is_in_didm')} = TRUE "
        f"AND {flag_true_sql('is_in_etidm')} = FALSE "  # <-- ADDED
    )



if role == "ETIDM":         # <-- NEW BLOCK
    return f"{flag_true_sql('is_in_etidm')} = TRUE "



elif current_role == "DIDM":
    dg_is_in, dg_status, _, dg_decision_at = stage_cols("DigitalGuild")
    cur_is_in, cur_status, _, _ = stage_cols("DIDM")

    backlog_df = con.execute(f"""
        SELECT *
        FROM approval
        WHERE {flag_true_sql(dg_is_in)} = TRUE
          AND CAST({dg_status} AS VARCHAR) = 'Approved'
          AND TRY_CAST({dg_decision_at} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(cur_is_in)} = FALSE
          AND COALESCE(CAST({cur_status} AS VARCHAR), '') IN ('', 'Pending')
    """).df()




elif current_role == "ETIDM":        # <-- NEW
    didm_is_in, didm_status, _, didm_decision_at = stage_cols("DIDM")
    cur_is_in, cur_status, _, _ = stage_cols("ETIDM")

    backlog_df = con.execute(f"""
        SELECT *
        FROM approval
        WHERE {flag_true_sql(didm_is_in)} = TRUE
          AND CAST({didm_status} AS VARCHAR) = 'Approved'
          AND TRY_CAST({didm_decision_at} AS TIMESTAMP) IS NOT NULL
          AND {flag_true_sql(cur_is_in)} = FALSE
          AND COALESCE(CAST({cur_status} AS VARCHAR), '') IN ('', 'Pending')
    """).df()



else:
    backlog_df = con.execute("SELECT * FROM approval WHERE 1=0").df()




for stg, m in STAGE_KEYS.items():
    flag_col = m.get("flag")
    if not flag_col:
        continue
    tracker_df.loc[mask, flag_col] = (stg == "DigitalGuild")




