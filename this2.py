from pandasql import sqldf
pysqldf = lambda q: sqldf(q, {"df": df})

# pending rows
pending_query = f"""
SELECT *
FROM df
WHERE {vf}
  AND COALESCE(CAST({status_col} AS TEXT), 'Pending') IN ('Pending', 'In Progress')
"""

pending_df = pysqldf(pending_query)

# approved rows
approved_query = f"""
SELECT *
FROM df
WHERE {vf}
  AND CAST({status_col} AS TEXT) = 'Approved'
"""

approved_df = pysqldf(approved_query)
approved_df = approved_df[approved_df[decision_col].notna()]


