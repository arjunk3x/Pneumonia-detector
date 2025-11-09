

awaiting_df = con.execute(
    f"""
    WITH typed AS (
      SELECT
        CAST(Sanction_ID AS VARCHAR)          AS Sanction_ID,
        TRY_CAST(Value AS DOUBLE)             AS Value,
        CAST(Stage AS VARCHAR)                AS Stage,
        CAST("Status in Stage" AS VARCHAR)    AS status_in_stage,
        CAST("Risk Level" AS VARCHAR)         AS risk_level,
        CAST({status_col} AS VARCHAR)         AS status_txt,
        CAST({nr_status} AS VARCHAR)          AS next_status_txt,
        TRY_CAST({decision_col} AS TIMESTAMP) AS decision_at,
        CASE
          WHEN LOWER(CAST({nr_is_in} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
          ELSE FALSE
        END                                   AS in_next_stage
      FROM approval
    )
    SELECT *
    FROM typed
    WHERE {vf}
      AND status_txt = 'Approved'
      AND decision_at IS NOT NULL
      AND in_next_stage = TRUE
      AND COALESCE(next_status_txt, 'Pending') = 'Pending'
    """
).df()



awaiting_df = con.execute(
    f"""
    SELECT *
    FROM approval
    WHERE {vf}
      AND CAST({status_col} AS VARCHAR) = 'Approved'
      AND TRY_CAST({decision_col} AS TIMESTAMP) IS NOT NULL
      -- was: AND {nr_is_in} = 1
      AND CASE
            WHEN LOWER(CAST({nr_is_in} AS VARCHAR)) IN ('1','true','t','yes','y') THEN TRUE
            ELSE FALSE
          END = TRUE
      AND COALESCE(CAST({nr_status} AS VARCHAR), 'Pending') = 'Pending'
    """
).df()
