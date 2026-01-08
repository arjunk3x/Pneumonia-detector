from pyspark.sql import functions as F, Window

# Window: activity progression inside a project
w = (
    Window
    .partitionBy("PROJECTOBJECTID", "OBJECTID")
    .orderBy(F.col("ACTUALSTARTDATE").asc_nulls_last())
)

df_phases = (
    df_activity_full
    .withColumn("NEXT_START", F.lead("ACTUALSTARTDATE").over(w))
    .withColumn(
        "PHASE_DURATION",
        F.datediff("NEXT_START", "ACTUALSTARTDATE")
    )
    .where(F.col("PHASE_DURATION").isNotNull())
)

gate_perf = (
    df_phases
    .groupBy("ID")  # ID = gate
    .agg(
        F.count("*").alias("n_phases"),
        F.expr("percentile_approx(PHASE_DURATION, 0.5)").alias("median_days"),
        F.expr("percentile_approx(PHASE_DURATION, 0.9)").alias("p90_days")
    )
    .orderBy(F.desc("p90_days"))
)


pdf = gate_perf.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(12,5))
plt.bar(pdf["ID"].astype(str), pdf["p90_days"])
plt.ylabel("P90 Phase Duration (days)")
plt.xlabel("Gate (ID)")
plt.title("Gate Performance Profile (P90 Duration)")
plt.xticks(rotation=90)
plt.show()




gate_dist = (
    df_phases
    .select("ID", "PHASE_DURATION")
)


import seaborn as sns

top_gates = pdf["ID"].head(5).tolist()

pdf_dist = gate_dist.where(F.col("ID").isin(top_gates)).toPandas()

plt.figure(figsize=(12,6))
sns.histplot(
    data=pdf_dist,
    x="PHASE_DURATION",
    hue="ID",
    bins=40,
    element="step",
    stat="count"
)
plt.xlim(0, 100)
plt.title("Phase Duration Distribution (Top Slow Gates)")
plt.show()




project_health = (
    df_phases
    .withColumn(
        "LATE",
        F.when(F.col("DELAY_DAYS") > 0, 1).otherwise(0)
    )
    .groupBy("PROJECTOBJECTID")
    .agg(
        F.countDistinct("OBJECTID").alias("n_activities"),
        F.expr("avg(LATE)").alias("share_late"),
        F.expr("percentile_approx(PHASE_DURATION, 0.5)").alias("median_duration")
    )
)

pdf_proj = project_health.toPandas()

plt.figure(figsize=(8,6))
plt.scatter(
    pdf_proj["share_late"],
    pdf_proj["median_duration"],
    s=pdf_proj["n_activities"] * 2,
    alpha=0.6
)
plt.xlabel("Share of Late Activities")
plt.ylabel("Median Phase Duration (days)")
plt.title("Project Health Map")
plt.show()




pdf_dd = (
    df_phases
    .select("DELAY_DAYS", "PHASE_DURATION")
    .toPandas()
)

plt.figure(figsize=(8,6))
plt.scatter(
    pdf_dd["DELAY_DAYS"],
    pdf_dd["PHASE_DURATION"],
    alpha=0.3
)
plt.axvline(0, linestyle="--", color="grey")
plt.xlabel("Start Delay (days)")
plt.ylabel("Phase Duration (days)")
plt.title("Delay vs Phase Duration")
plt.show()



total_acts = df_activity_full.select("OBJECTID").distinct().count()

gate_reach = (
    df_activity_full
    .select("ID", "OBJECTID")
    .distinct()
    .groupBy("ID")
    .count()
    .withColumn("reach_pct", F.col("count") / F.lit(total_acts))
    .orderBy("ID")
)
pdf_reach = gate_reach.toPandas()

plt.figure(figsize=(12,5))
plt.plot(pdf_reach["ID"].astype(str), pdf_reach["reach_pct"], marker="o")
plt.xlabel("Gate (ID)")
plt.ylabel("Reach %")
plt.title("Lifecycle Completion by Gate")
plt.xticks(rotation=90)
plt.show()
