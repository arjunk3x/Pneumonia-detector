# End-to-end flow ordering (internal codes)
PRE_REVIEW_ROLES: List[str] = [
    "HeadDataAI",
    "DataGovIA",
    "ArchAssurance",
    "Finance",
    "Regulatory",
]

ROLE_FLOW: List[str] = PRE_REVIEW_ROLES + ["DigitalGuild", "ETIDM"]



STAGE_MAP: Dict[str, Dict[str, str]] = {
    "HeadDataAI": {
        "is_in": "is_in_head_data_ai",
        "status": "head_data_ai_status",
        "assigned_to": "head_data_ai_assigned_to",
        "decision_at": "head_data_ai_decision_at",
    },
    "DataGovIA": {
        "is_in": "is_in_data_guild",           # reuse old data_guild_* columns
        "status": "data_guild_status",
        "assigned_to": "data_guild_assigned_to",
        "decision_at": "data_guild_decision_at",
    },
    "ArchAssurance": {
        "is_in": "is_in_SDA",                  # reuse old SDA_* columns
        "status": "SDA_status",
        "assigned_to": "SDA_assigned_to",
        "decision_at": "SDA_decision_at",
    },
    "Finance": {
        "is_in": "is_in_finance",
        "status": "finance_status",
        "assigned_to": "finance_assigned_to",
        "decision_at": "finance_decision_at",
    },
    "Regulatory": {
        "is_in": "is_in_regulatory",
        "status": "regulatory_status",
        "assigned_to": "regulatory_assigned_to",
        "decision_at": "regulatory_decision_at",
    },
    "DigitalGuild": {
        "is_in": "is_in_digital_guild",
        "status": "digital_guild_status",
        "assigned_to": "digital_guild_assigned_to",
        "decision_at": "digital_guild_decision_at",
    },
    "ETIDM": {
        "is_in": "is_in_etidm",
        "status": "etidm_status",
        "assigned_to": "etidm_assigned_to",
        "decision_at": "etidm_decision_at",
    },
}











def visibility_filter_for(role: str) -> str:
    """
    Return a SQL WHERE fragment controlling what this role sees
    in their dashboard.
    """
    is_in_col, status_col, _, decision_col = stage_cols(role)

    # 1) Pre-Digital reviewers:
    #   - see anything flagged into *their* stage
    #   - that has not yet entered Digital Guild or ETIDM
    if role in PRE_REVIEW_ROLES:
        return (
            f"{flag_true_sql(is_in_col)} = TRUE "
            f"AND {flag_true_sql('is_in_digital_guild')} = FALSE "
            f"AND {flag_true_sql('is_in_etidm')} = FALSE"
        )

    # 2) Digital Guild: only show items that are in Digital Guild but not yet in ETIDM
    if role == "DigitalGuild":
        return (
            f"{flag_true_sql('is_in_digital_guild')} = TRUE "
            f"AND {flag_true_sql('is_in_etidm')} = FALSE"
        )

    # 3) ETIDM: only show items that are in ETIDM
    if role == "ETIDM":
        return f"{flag_true_sql('is_in_etidm')} = TRUE"

    # Fallback (shouldn't really be hit)
    return f"{flag_true_sql(is_in_col)} = TRUE"






def set_pre_review_flags_inplace(df: pd.DataFrame, ids: List[str]) -> None:
    """
    When we move items out of submitter intake, we push them
    into ALL 5 pre-Digital stages at once.
    """
    id_list = [str(x) for x in ids]
    mask = df["Sanction_ID"].astype(str).isin(id_list)

    # (flag_col, status_col, assigned_col)
    pre_cols = [
        ("is_in_head_data_ai", "head_data_ai_status", "head_data_ai_assigned_to"),
        ("is_in_data_guild",   "data_guild_status",   "data_guild_assigned_to"),
        ("is_in_SDA",          "SDA_status",          "SDA_assigned_to"),
        ("is_in_finance",      "finance_status",      "finance_assigned_to"),
        ("is_in_regulatory",   "regulatory_status",   "regulatory_assigned_to"),
    ]

    for flag_col, status_col, assigned_col in pre_cols:
        if flag_col in df.columns:
            df.loc[mask, flag_col] = 1
        if status_col in df.columns:
            df.loc[mask, status_col] = (
                df.loc[mask, status_col]
                .fillna("Pending")
                .replace("", "Pending")
            )
        if assigned_col in df.columns:
            df.loc[mask, assigned_col] = None

    # no longer considered raw submissions
    if "is_submitter" in df.columns:
        df.loc[mask, "is_submitter"] = 0

    # set high-level status
    if "Overall_status" in df.columns:
        df.loc[mask, "Overall_status"] = "In pre-review"







with st.expander(f"Intake ({role_display_name(current_role)})", expanded=False):

    if con is None:
        st.info("No database connection configured.")
        backlog_df = pd.DataFrame()
    else:
        # -----------------------------------------------------------------
        # 1) PARALLEL PRE-REVIEWERS
        #    HeadDataAI, DataGovIA, ArchAssurance, Finance, Regulatory
        #    All see the SAME backlog: raw submissions not yet in pre-review
        # -----------------------------------------------------------------
        if current_role in PRE_REVIEW_ROLES:
            backlog_df = con.execute(
                f"""
                SELECT *
                FROM approval
                WHERE TRY_CAST(is_submitter AS BIGINT) = 1
                  AND {flag_true_sql('is_in_head_data_ai')} = FALSE
                  AND {flag_true_sql('is_in_data_guild')} = FALSE
                  AND {flag_true_sql('is_in_SDA')} = FALSE
                  AND {flag_true_sql('is_in_finance')} = FALSE
                  AND {flag_true_sql('is_in_regulatory')} = FALSE
                """
            ).df()

        # -----------------------------------------------------------------
        # 2) DIGITAL GUILD: items that have *finished* pre-review
        #    (all 5 pre-review statuses == Approved),
        #    not yet flagged into DigitalGuild
        # -----------------------------------------------------------------
        elif current_role == "DigitalGuild":
            conditions = []
            for role_name in PRE_REVIEW_ROLES:
                r_is_in, r_status, _, r_decision_at = stage_cols(role_name)
                conditions.append(
                    f"""
                    {flag_true_sql(r_is_in)} = TRUE
                    AND CAST({r_status} AS VARCHAR) = 'Approved'
                    AND TRY_CAST({r_decision_at} AS TIMESTAMP) IS NOT NULL
                    """
                )
            all_pre_ok = " AND ".join(conditions)

            dg_is_in, dg_status, _, _ = stage_cols("DigitalGuild")

            backlog_df = con.execute(
                f"""
                SELECT *
                FROM approval
                WHERE ({all_pre_ok})
                  AND {flag_true_sql(dg_is_in)} = FALSE
                  AND COALESCE(CAST({dg_status} AS VARCHAR), '') IN ('', 'Pending')
                """
            ).df()

        # -----------------------------------------------------------------
        # 3) ETIDM: items approved by Digital Guild,
        #    not yet flagged into ETIDM
        # -----------------------------------------------------------------
        elif current_role == "ETIDM":
            dg_is_in, dg_status, _, dg_decision_at = stage_cols("DigitalGuild")
            et_is_in, et_status, _, _ = stage_cols("ETIDM")

            backlog_df = con.execute(
                f"""
                SELECT *
                FROM approval
                WHERE {flag_true_sql(dg_is_in)} = TRUE
                  AND CAST({dg_status} AS VARCHAR) = 'Approved'
                  AND TRY_CAST({dg_decision_at} AS TIMESTAMP) IS NOT NULL
                  AND {flag_true_sql(et_is_in)} = FALSE
                  AND COALESCE(CAST({et_status} AS VARCHAR), '') IN ('', 'Pending')
                """
            ).df()

        # -----------------------------------------------------------------
        # Anything else ⇒ empty
        # -----------------------------------------------------------------
        else:
            backlog_df = con.execute("SELECT * FROM approval WHERE 1=0").df()

    # -------- everything BELOW this stays very similar --------
    if backlog_df.empty:
        st.info("No items available for intake.")
    else:
        st.dataframe(
            backlog_df[["Sanction_ID", "Value", "Overall_status"]],
            use_container_width=True,
        )

        intake_ids = st.multiselect(
            "Select Sanction_IDs to intake:",
            backlog_df["Sanction_ID"].astype(str).tolist(),
        )

        if st.button(f"Move selected to {role_display_name(current_role)}"):
            if intake_ids:
                if current_role in PRE_REVIEW_ROLES:
                    # 🔹 push into ALL 5 pre-review stages at once
                    set_pre_review_flags_inplace(df, intake_ids)
                elif current_role in ("DigitalGuild", "ETIDM"):
                    # 🔹 for these, move into their single stage
                    set_stage_flags_inplace(df, intake_ids, current_role)

                # Persist and refresh registration so queries see updates immediately
                df.to_csv(CSV_PATH, index=False)
                try:
                    con.unregister("approval")
                except Exception:
                    pass
                con.register("approval", df)

                st.success(f"Moved {len(intake_ids)} to {role_display_name(current_role)}")
                st.rerun()


