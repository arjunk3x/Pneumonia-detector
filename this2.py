from pyspark.sql import functions as F

PROJECT_COL = "Project ID"

# ----------------------------
# 0) Base prep
# ----------------------------
base = (
    df
    .withColumn("region_clean", F.coalesce(F.trim(F.col("region")), F.lit("Unknown")))
)

# ct columns already exist: cast + set negatives to NULL (invalid)
ct_cols = ["ct_a2_to_b_days", "ct_b_to_c_days", "ct_c_to_d_days", "ct_d_to_e_days"]
for c in ct_cols:
    if c not in base.columns:
        raise Exception(f"Expected column missing in df: {c}")
    base = (base
            .withColumn(c, F.col(c).cast("double"))
            .withColumn(c, F.when(F.col(c) < 0, F.lit(None)).otherwise(F.col(c)))
           )

# ----------------------------
# 1) Count projects per region
# ----------------------------
projects_per_region = (
    base.groupBy("region_clean")
        .agg(F.countDistinct(F.col(PROJECT_COL)).alias("n_projects_total"))
)

# ----------------------------
# 2) Separate transition datasets (to avoid losing valid datapoints)
# ----------------------------
df_a2_b = base.filter(F.col("ct_a2_to_b_days").isNotNull())
df_b_c  = base.filter(F.col("ct_b_to_c_days").isNotNull())
df_c_d  = base.filter(F.col("ct_c_to_d_days").isNotNull())
df_d_e  = base.filter(F.col("ct_d_to_e_days").isNotNull())

# ----------------------------
# 3) Region benchmarks per transition:
#     - n_* = number of valid datapoints
#     - good_* = P50 (median) "what good looks like"
# ----------------------------
bench_a2_b = (
    df_a2_b.groupBy("region_clean")
           .agg(
               F.count("*").alias("n_A2_B"),
               F.expr("percentile_approx(ct_a2_to_b_days, 0.5)").alias("good_A2_B_p50")
           )
)

bench_b_c = (
    df_b_c.groupBy("region_clean")
          .agg(
              F.count("*").alias("n_B_C"),
              F.expr("percentile_approx(ct_b_to_c_days, 0.5)").alias("good_B_C_p50")
          )
)

bench_c_d = (
    df_c_d.groupBy("region_clean")
          .agg(
              F.count("*").alias("n_C_D"),
              F.expr("percentile_approx(ct_c_to_d_days, 0.5)").alias("good_C_D_p50")
          )
)

bench_d_e = (
    df_d_e.groupBy("region_clean")
          .agg(
              F.count("*").alias("n_D_E"),
              F.expr("percentile_approx(ct_d_to_e_days, 0.5)").alias("good_D_E_p50")
          )
)

# ----------------------------
# 4) Final Region benchmark table (matches your example structure)
# ----------------------------
region_benchmark_table = (
    projects_per_region
    .join(bench_a2_b, on="region_clean", how="left")
    .join(bench_b_c,  on="region_clean", how="left")
    .join(bench_c_d,  on="region_clean", how="left")
    .join(bench_d_e,  on="region_clean", how="left")
    .withColumnRenamed("region_clean", "region")
    .withColumn("good_A2_B_p50", F.round(F.col("good_A2_B_p50"), 0))
    .withColumn("good_B_C_p50",  F.round(F.col("good_B_C_p50"),  0))
    .withColumn("good_C_D_p50",  F.round(F.col("good_C_D_p50"),  0))
    .withColumn("good_D_E_p50",  F.round(F.col("good_D_E_p50"),  0))
    .orderBy("region")
)

display(region_benchmark_table)

# ----------------------------
# 5) Join regional "good" values back to each project row
# ----------------------------
df_with_good = (
    base.join(
        region_benchmark_table
          .withColumnRenamed("region", "region_clean")
          .select(
              "region_clean",
              F.col("good_A2_B_p50").alias("Good_A2_B"),
              F.col("good_B_C_p50").alias("Good_B_C"),
              F.col("good_C_D_p50").alias("Good_C_D"),
              F.col("good_D_E_p50").alias("Good_D_E"),
              "n_projects_total", "n_A2_B", "n_B_C", "n_C_D", "n_D_E"
          ),
        on="region_clean",
        how="left"
    )
)

display(
    df_with_good.select(
        PROJECT_COL, "region_clean",
        "ct_a2_to_b_days", "Good_A2_B",
        "ct_b_to_c_days",  "Good_B_C",
        "ct_c_to_d_days",  "Good_C_D",
        "ct_d_to_e_days",  "Good_D_E",
        "n_projects_total","n_A2_B","n_B_C","n_C_D","n_D_E"
    ).limit(25)
)

# df_with_good is the final output you described.
