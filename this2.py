from pyspark.sql import functions as F

PROJECT_COL = "Project ID"

d = (df
     .withColumn("region_clean", F.coalesce(F.trim(F.col("region")), F.lit("Unknown")))
     .withColumn("category_clean", F.coalesce(F.trim(F.col("category_clean")), F.trim(F.col("Category")), F.lit("Unknown")))
)

# Gate decision date cols
a2 = F.col("Gate A2 Decision Date")
b  = F.col("Gate B Decision Date")
c  = F.col("Gate C Decision Date")
d_ = F.col("Gate D Decision Date")
e  = F.col("Gate E Decision Date")

# Transition durations: only compute when both dates exist AND are in correct order
d = (d
     .withColumn("ct_a2_to_b_days", F.when(a2.isNotNull() & b.isNotNull() & (b >= a2), F.datediff(b, a2)).otherwise(F.lit(None)).cast("double"))
     .withColumn("ct_b_to_c_days",  F.when(b.isNotNull()  & c.isNotNull() & (c >= b),  F.datediff(c, b)).otherwise(F.lit(None)).cast("double"))
     .withColumn("ct_c_to_d_days",  F.when(c.isNotNull()  & d_.isNotNull() & (d_ >= c), F.datediff(d_, c)).otherwise(F.lit(None)).cast("double"))
     .withColumn("ct_d_to_e_days",  F.when(d_.isNotNull() & e.isNotNull() & (e >= d_), F.datediff(e, d_)).otherwise(F.lit(None)).cast("double"))
)



GROUP_KEYS = ["region_clean", "category_clean"]  # use ["region_clean"] if you don't want split by category

bench_region = (
    d.groupBy(*GROUP_KEYS)
     .agg(
         F.countDistinct(F.col(PROJECT_COL)).alias("n_projects_total"),

         F.sum(F.col("ct_a2_to_b_days").isNotNull().cast("int")).alias("n_A2_B"),
         F.expr("percentile_approx(ct_a2_to_b_days, 0.5)").alias("good_A2_B_p50"),

         F.sum(F.col("ct_b_to_c_days").isNotNull().cast("int")).alias("n_B_C"),
         F.expr("percentile_approx(ct_b_to_c_days, 0.5)").alias("good_B_C_p50"),

         F.sum(F.col("ct_c_to_d_days").isNotNull().cast("int")).alias("n_C_D"),
         F.expr("percentile_approx(ct_c_to_d_days, 0.5)").alias("good_C_D_p50"),

         F.sum(F.col("ct_d_to_e_days").isNotNull().cast("int")).alias("n_D_E"),
         F.expr("percentile_approx(ct_d_to_e_days, 0.5)").alias("good_D_E_p50"),
     )
     .withColumnRenamed("region_clean", "region")
     .orderBy("region", "category_clean")
)

# Optional rounding for neat display
bench_region = (bench_region
    .withColumn("good_A2_B_p50", F.round(F.col("good_A2_B_p50"), 0))
    .withColumn("good_B_C_p50",  F.round(F.col("good_B_C_p50"),  0))
    .withColumn("good_C_D_p50",  F.round(F.col("good_C_D_p50"),  0))
    .withColumn("good_D_E_p50",  F.round(F.col("good_D_E_p50"),  0))
)

display(bench_region)




bench_for_join = (
    bench_region
    .withColumnRenamed("region", "region_clean")  # to match join key
    .select(
        "region_clean", "category_clean",
        F.col("good_A2_B_p50").alias("Good_A2_B"),
        F.col("good_B_C_p50").alias("Good_B_C"),
        F.col("good_C_D_p50").alias("Good_C_D"),
        F.col("good_D_E_p50").alias("Good_D_E"),
    )
)

df_with_good = d.join(bench_for_join, on=GROUP_KEYS, how="left")

display(df_with_good.select(
    PROJECT_COL, "region_clean", "category_clean",
    "ct_a2_to_b_days","Good_A2_B",
    "ct_b_to_c_days","Good_B_C",
    "ct_c_to_d_days","Good_C_D",
    "ct_d_to_e_days","Good_D_E"
).limit(20))
