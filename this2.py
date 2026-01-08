from pyspark.sql import functions as F, Window

def pick_first_existing(df, candidates, required=True):
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    if required:
        raise ValueError(f"None of these columns exist: {candidates}")
    return None





# Required: project id and gate code
project_col = pick_first_existing(df_activity_full, ["PROJECTOBJECTID", "PROJECTID"])
gate_col    = pick_first_existing(df_activity_full, ["ID"])  # per your context: ID is gate code
activity_col = pick_first_existing(df_activity_full, ["OBJECTID", "ACTIVITYOBJECTID", "ACTIVITYID"], required=False)

# Actual date we use to place gate event on a timeline
actual_date_col = pick_first_existing(
    df_activity_full,
    ["ACTUALSTARTDATE", "STARTDATE", "PLANNEDSTARTDATE", "BASELINESTARTDATE"],
    required=True
)

# Optional: baseline date for delay analysis
baseline_date_col = pick_first_existing(df_activity_full, ["BASELINESTARTDATE"], required=False)






act = (
    df_activity_full
    .withColumn(project_col, F.col(project_col).cast("string"))
    .withColumn(gate_col, F.trim(F.col(gate_col).cast("string")))
    .withColumn(actual_date_col, F.to_timestamp(F.col(actual_date_col)))
)

if baseline_date_col:
    act = act.withColumn(baseline_date_col, F.to_timestamp(F.col(baseline_date_col)))






phase_gate_code_col = pick_first_existing(phase_seq, ["Activity Code", "ActivityCode", "GATE_CODE"])

# try to find an order column in phase_seq
gate_order_col = pick_first_existing(
    phase_seq,
    ["SEQUENCE", "SEQ", "ORDER", "STEP", "GATE_ORDER", "PHASE_SEQ"],
    required=False
)

gate_dim = (
    phase_seq
    .withColumn("gate_code", F.trim(F.col(phase_gate_code_col).cast("string")))
    .select(
        "gate_code",
        (F.col(gate_order_col).cast("int").alias("gate_order") if gate_order_col else F.lit(None).cast("int").alias("gate_order"))
    )
    .dropDuplicates(["gate_code"])
)





act_gate = act.join(gate_dim.select("gate_code"), act[gate_col] == F.col("gate_code"), "left_semi")





project_gate_events = (
    act_gate
    .where(F.col(actual_date_col).isNotNull())
    .groupBy(project_col, F.col(gate_col).alias("gate_code"))
    .agg(F.min(F.col(actual_date_col)).alias("gate_date"))
    .join(gate_dim, "gate_code", "left")
)





project_gate_events_latest = (
    act_gate
    .where(F.col(actual_date_col).isNotNull())
    .groupBy(project_col, F.col(gate_col).alias("gate_code"))
    .agg(F.max(F.col(actual_date_col)).alias("gate_date"))
    .join(gate_dim, "gate_code", "left")
)





if gate_order_col:
    w = Window.partitionBy(project_col).orderBy(F.col("gate_order").asc_nulls_last(), F.col("gate_date"))
else:
    w = Window.partitionBy(project_col).orderBy(F.col("gate_date"))

project_timeline = (
    project_gate_events
    .withColumn("next_gate_code", F.lead("gate_code").over(w))
    .withColumn("next_gate_date", F.lead("gate_date").over(w))
    .withColumn("gate_duration_days", F.datediff("next_gate_date", "gate_date"))
)





gate_summary = (
    project_timeline
    .where(F.col("gate_duration_days").isNotNull())
    .groupBy("gate_code")
    .agg(
        F.count("*").alias("n"),
        F.avg("gate_duration_days").alias("avg_days"),
        F.expr("percentile_approx(gate_duration_days, 0.5)").alias("median_days"),
        F.expr("percentile_approx(gate_duration_days, 0.9)").alias("p90_days"),
        F.expr("percentile_approx(gate_duration_days, 0.95)").alias("p95_days"),
        F.max("gate_duration_days").alias("max_days"),
        F.avg(F.when(F.col("gate_duration_days") == 0, 1).otherwise(0)).alias("share_zero")
    )
    .orderBy(F.col("p95_days").desc())
)

display(gate_summary)   # Databricks table; you can switch to bar chart in UI







total_projects = project_gate_events.select(project_col).distinct().count()

completion_funnel = (
    project_gate_events
    .groupBy("gate_code")
    .agg(F.countDistinct(project_col).alias("projects_reached"))
    .withColumn("total_projects", F.lit(total_projects))
    .withColumn("reach_rate", F.col("projects_reached") / F.col("total_projects"))
    .orderBy(F.col("reach_rate").desc())
)

display(completion_funnel)






transitions = (
    project_timeline
    .where(F.col("next_gate_code").isNotNull())
    .groupBy("gate_code", "next_gate_code")
    .agg(F.count("*").alias("n_transitions"))
    .orderBy(F.col("n_transitions").desc())
)

display(transitions)






w_last = Window.partitionBy(project_col).orderBy(
    (F.col("gate_order").desc_nulls_last() if gate_order_col else F.col("gate_date").desc())
)

stuck_projects = (
    project_gate_events
    .withColumn("rn", F.row_number().over(w_last))
    .where(F.col("rn") == 1)
    .drop("rn")
    .withColumn("days_since_last_gate", F.datediff(F.current_timestamp(), F.col("gate_date")))
    .orderBy(F.col("days_since_last_gate").desc())
)

display(stuck_projects)







rework = (
    act_gate
    .groupBy(project_col, F.col(gate_col).alias("gate_code"))
    .agg(
        F.count("*").alias("gate_rows"),
        (F.countDistinct(activity_col).alias("distinct_activities") if activity_col else F.lit(None).cast("long").alias("distinct_activities")),
        F.countDistinct(actual_date_col).alias("distinct_gate_dates")
    )
)

rework_summary = (
    rework
    .groupBy("gate_code")
    .agg(
        F.count("*").alias("project_gate_pairs"),
        F.avg("gate_rows").alias("avg_rows_per_project_gate"),
        F.expr("percentile_approx(gate_rows, 0.95)").alias("p95_rows"),
        F.avg(F.when(F.col("gate_rows") > 1, 1).otherwise(0)).alias("share_rework")
    )
    .orderBy(F.col("share_rework").desc())
)

display(rework_summary)







if baseline_date_col:
    baseline_gate = (
        act_gate
        .where(F.col(baseline_date_col).isNotNull())
        .groupBy(project_col, F.col(gate_col).alias("gate_code"))
        .agg(F.min(F.col(baseline_date_col)).alias("baseline_gate_date"))
    )

    gate_delay = (
        project_gate_events
        .join(baseline_gate, [project_col, "gate_code"], "left")
        .withColumn("delay_days", F.datediff("gate_date", "baseline_gate_date"))
    )

    gate_delay_summary = (
        gate_delay
        .where(F.col("delay_days").isNotNull())
        .groupBy("gate_code")
        .agg(
            F.count("*").alias("n"),
            F.avg("delay_days").alias("avg_delay"),
            F.expr("percentile_approx(delay_days, 0.5)").alias("median_delay"),
            F.expr("percentile_approx(delay_days, 0.9)").alias("p90_delay"),
            F.avg(F.when(F.col("delay_days") > 0, 1).otherwise(0)).alias("share_late")
        )
        .orderBy(F.col("avg_delay").desc())
    )

    display(gate_delay_summary)
else:
    print("BASELINESTARTDATE not found; skipping baseline delay analysis.")







gate_bounds = (
    project_timeline
    .where(F.col("gate_duration_days").isNotNull())
    .groupBy("gate_code")
    .agg(
        F.expr("percentile_approx(gate_duration_days, 0.25)").alias("q1"),
        F.expr("percentile_approx(gate_duration_days, 0.75)").alias("q3"),
    )
    .withColumn("iqr", F.col("q3") - F.col("q1"))
    .withColumn("lower", F.col("q1") - F.lit(1.5) * F.col("iqr"))
    .withColumn("upper", F.col("q3") + F.lit(1.5) * F.col("iqr"))
)

outliers = (
    project_timeline
    .join(gate_bounds, "gate_code", "left")
    .where(F.col("gate_duration_days").isNotNull())
    .where((F.col("gate_duration_days") < F.col("lower")) | (F.col("gate_duration_days") > F.col("upper")))
    .select(project_col, "gate_code", "gate_date", "next_gate_code", "next_gate_date", "gate_duration_days", "lower", "upper")
    .orderBy(F.col("gate_duration_days").desc())
)

display(outliers)






# Null rates for key date fields in gate rows
date_cols_to_check = [c for c in ["DATADATE", "CREATEDATE", "LASTUPDATEDATE",
                                 "BASELINESTARTDATE", "PLANNEDSTARTDATE", "ACTUALSTARTDATE",
                                 "STARTDATE", "EARLYSTARTDATE", "LATESTARTDATE"] if c in df_activity_full.columns]

hygiene_nulls = (
    df_activity_full
    .select([F.avg(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"null_rate_{c}") for c in date_cols_to_check])
)

display(hygiene_nulls)

# Suspicious years in actual gate date (gate events)
hygiene_years = (
    project_gate_events
    .withColumn("year", F.year("gate_date"))
    .groupBy("year")
    .agg(F.count("*").alias("n"))
    .orderBy("year")
)

display(hygiene_years)

# Zero-duration share per gate (already in gate_summary) - but here's explicit:
zero_duration_by_gate = (
    project_timeline
    .where(F.col("gate_duration_days").isNotNull())
    .groupBy("gate_code")
    .agg(F.avg(F.when(F.col("gate_duration_days") == 0, 1).otherwise(0)).alias("share_zero"))
    .orderBy(F.col("share_zero").desc())
)

display(zero_duration_by_gate)
