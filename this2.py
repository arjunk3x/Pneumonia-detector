# -----------------------------------------
# ACTIONS SECTION – what appears in "Actions in {current_role}"
# -----------------------------------------

# Figure out which tracker columns belong to this role
is_in_col, status_col, assigned_col, decision_col = stage_cols(current_role)

if con is None:
    filtered_df = pd.DataFrame()
else:
    # Pull from DuckDB
    if current_role in PRE_REVIEW_ROLES:
        # 🔹 Pre-review teams: see anything that is active for them,
        # regardless of overall "Current Stage"
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND COALESCE({is_in_col}, 0) = 1
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """
    elif current_role == "DigitalGuild":
        # 🔹 Digital Guild: only see items whose overall Current Stage is Digital Guild
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND "Current Stage" = 'Digital Guild'
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """
    elif current_role == "ETIDM":
        # 🔹 ETIDM: only see items whose overall Current Stage is ETIDM
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND "Current Stage" = 'ETIDM'
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """
    else:
        # Fallback (if some other role ever appears)
        query = f"""
            SELECT *
            FROM approval
            WHERE TRY_CAST(is_submitter AS BIGINT) = 1
              AND COALESCE({is_in_col}, 0) = 1
              AND COALESCE({status_col}, 'Pending') NOT IN ('Approved', 'Rejected')
        """

    actions_df = con.execute(query).df()
    filtered_df = actions_df
