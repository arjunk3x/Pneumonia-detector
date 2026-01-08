from pyspark.sql import functions as F, Window

proj_col = "PROJECTOBJECTID"

w = (
    Window
    .partitionBy(proj_col, "OBJECTID")
    .orderBy(F.col("ACTUALSTARTDATE").asc_nulls_last())
)

df = (
    df_activity_full
    .withColumn("NEXT_START", F.lead("ACTUALSTARTDATE").over(w))
    .withColumn(
        "PHASE_DURATION",
        F.datediff("NEXT_START", "ACTUALSTARTDATE")
    )
    .withColumn(
        "DELAY_DAYS",
        F.datediff("ACTUALSTARTDATE", "BASELINESTARTDATE")
    )
)






gate_stats = (
    df
    .where(F.col("PHASE_DURATION").isNotNull())
    .groupBy("ID")
    .agg(
        F.count("*").alias("n_phases"),
        F.expr("percentile_approx(PHASE_DURATION, 0.5)").alias("median_duration"),
        F.expr("percentile_approx(PHASE_DURATION, 0.9)").alias("p90_duration"),
        F.expr("percentile_approx(PHASE_DURATION, 0.95)").alias("p95_duration")
    )
    .orderBy(F.desc("p90_duration"))
)







p95 = (
    df
    .selectExpr("percentile_approx(PHASE_DURATION, 0.95) as p95")
    .collect()[0]["p95"]
)

df = df.withColumn(
    "STALLED",
    F.when(F.col("PHASE_DURATION") > F.lit(p95), 1).otherwise(0)
)








project_health = (
    df
    .groupBy("PROJECTOBJECTID")
    .agg(
        F.countDistinct("OBJECTID").alias("n_activities"),
        F.avg("DELAY_DAYS").alias("mean_delay"),
        F.expr("avg(case when DELAY_DAYS > 0 then 1 else 0 end)").alias("share_late"),
        F.expr("percentile_approx(PHASE_DURATION, 0.5)").alias("median_phase_duration"),
        F.max("PHASE_DURATION").alias("max_phase_duration")
    )
)








total_activities = df.select("OBJECTID").distinct().count()

gate_reach = (
    df
    .select("ID", "OBJECTID")
    .distinct()
    .groupBy("ID")
    .count()
    .withColumn("reach_pct", F.col("count") / F.lit(total_activities))
    .orderBy("ID")
)
