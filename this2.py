# ============================================
# Regional "Good" benchmarks per transition
# (Separate transition DFs, no row loss)
# Base DF is: df
# ct_* columns already exist in df
# ============================================

from pyspark.sql import functions as F

PROJECT_COL = "Project ID"

# ----------------------------
# 0) Base prep (no row drops)
# ----------------------------
base = (
    df
    .withColumn("region_clean", F.coalesce(F.trim(F.col("region")), F.lit("Unknown")))
    .withColumn("category_clean", F.coalesce(F.trim(F.col("category_clean")), F.trim(F.col("Category")), F.lit("Unknown")))
)

# Ensure ct columns are usable: cast + invalidate negatives (set to NULL)
ct_map = {
    "ct_a2_to_b_days": "A2_B",
    "ct_b_to_c_days":  "B_C",
    "ct_c_to_d_days":  "C_D",
    "ct_d_to_e_days":  "D_E"
}

for ct_col in ct_map.keys():
    if ct_col not in base.columns:
        raise Exception(f"Expected column missing in df: {ct_col}")
    base = (base
            .withColumn(ct_col, F.col(ct_col).cast("double"))
            .withColumn(ct_col, F.when(F.col(ct_col) < 0, F.lit(None)).otherwise(F.col(ct_col)))
           )

# ----------------------------
# 1) Count projects per region (and category)
# ----------------------------
projects_per_region = (
    base.groupBy("region_clean", "category_clean")
        .agg(F.countDistinct(F.col(PROJECT_COL)).alias("n_projects_total"))
)

display(projects_per_region.orderBy("region_clean", "category_clean"))

# ----------------------------
# 2) Build per-transition filtered DFs
#    (each keeps valid datapoints only)
# ----------------------------
df_a2_b = base.filter(F.col("ct_a2_to_b_days").isNotNull())
df_b_c  = base.filter(F.col("ct_b_to_c_days").isNotNull())
df_c_d  = base.filter(F.col("ct_c_to_d_days").isNotNull())
df_d_e  = base.filter(F.col("ct_d_to_e_days").isNotNull())

# ----------------------------
# 3) Regional benchmarks (P50 + n points) per transition
# ----------------------------
bench_a2_b = (
    df_a2_b.groupBy("region_clean", "category_clean")
           .agg(
               F.count("*").alias("n_A2_B"),
               F.expr("percentile_approx(ct_a2_to_b_days, 0.5)").alias("Good_A2_B")
           )
)

bench_b_c = (
    df_b_c.groupBy("region_clean", "category_clean")
          .agg(
              F.count("*").alias("n_B_C"),
              F.expr("percentile_approx(ct_b_to_c_days, 0.5)").alias("Good_B_C")
          )
)

bench_c_d = (
    df_c_d.groupBy("region_clean", "category_clean")
          .agg(
              F.count("*").alias("n_C_D"),
              F.expr("percentile_approx(ct_c_to_d_days, 0.5)").alias("Good_C_D")
          )
)

bench_d_e = (
    df_d_e.groupBy("region_clean", "category_clean")
          .agg(
              F.count("*").alias("n_D_E"),
              F.expr("percentile_approx(ct_d_to_e_days, 0.5)").alias("Good_D_E")
          )
)

# Optional: round benchmark values for neat tables
for bname in ["Good_A2_B", "Good_B_C", "Good_C_D", "Good_D_E"]:
    # apply only if column exists in that bench df later at join time
    pass

# ----------------------------
# 4) Build the region benchmark table like your example
#    region | n_projects_total | n_A2_B | Good_A2_B | ...
# ----------------------------
region_benchmark_table = (
    projects_per_region
    .join(bench_a2_b, on=["region_clean", "category_clean"], how="left")
    .join(bench_b_c,  on=["region_clean", "category_clean"], how="left")
    .join(bench_c_d,  on=["region_clean", "category_clean"], how="left")
    .join(bench_d_e,  on=["region_clean", "category_clean"], how="left")
    .withColumnRenamed("region_clean", "region")
    .orderBy("region", "category_clean")
)

# Round good values (optional)
region_benchmark_table = (region_benchmark_table
    .withColumn("Good_A2_B", F.round(F.col("Good_A2_B"), 0))
    .withColumn("Good_B_C",  F.round(F.col("Good_B_C"),  0))
    .withColumn("Good_C_D",  F.round(F.col("Good_C_D"),  0))
    .withColumn("Good_D_E",  F.round(F.col("Good_D_E"),  0))
)

display(region_benchmark_table)

# ----------------------------
# 5) Join Good_* values back to EACH PROJECT ROW
#    (no row drops)
# ----------------------------
df_with_good = (
    base
    .join(region_benchmark_table.select(
            F.col("region").alias("region_clean"),
            "category_clean",
            "Good_A2_B", "Good_B_C", "Good_C_D", "Good_D_E",
            "n_A2_B", "n_B_C", "n_C_D", "n_D_E",
            "n_projects_total"
        ),
        on=["region_clean", "category_clean"], how="left"
    )
)

display(
    df_with_good.select(
        PROJECT_COL, "region_clean", "category_clean",
        "ct_a2_to_b_days", "Good_A2_B",
        "ct_b_to_c_days",  "Good_B_C",
        "ct_c_to_d_days",  "Good_C_D",
        "ct_d_to_e_days",  "Good_D_E",
        "n_projects_total", "n_A2_B", "n_B_C", "n_C_D", "n_D_E"
    ).limit(25)
)

# df_with_good is your final dataset with regional "Good" benchmarks attached.
