from pyspark.sql import functions as F

d3 = df.filter((F.col("benchmark_eligible")==1) & (F.col("is_sequence_full")==1))
gates = ["A2","B","C","D","E"]

# Distinct flag values per gate + null rate
for g in gates:
    col = f"{g}_timeliness_flag"
    print(g, "distinct flags:")
    display(
        d3.groupBy(F.upper(F.trim(F.col(col))).alias("flag"))
          .count()
          .orderBy(F.desc("count"))
    )
    display(
        d3.agg(F.round(F.mean(F.col(col).isNull().cast("int"))*100,2).alias(f"{g}_flag_null_pct"))
    )




cycle_cols = ["ct_a2_to_b_days","ct_b_to_c_days","ct_c_to_d_days","ct_d_to_e_days"]

proj_qc_by_mgr = (
    d3.groupBy("pm_clean")
      .agg(
          F.count("*").alias("n_projects"),
          *[F.round(F.mean(F.col(c).isNull().cast("int"))*100,2).alias(f"{c}_null_pct") for c in cycle_cols],
          *[F.round(F.mean((F.col(c)<0).cast("int"))*100,2).alias(f"{c}_neg_pct") for c in cycle_cols],
          *[F.round(F.mean((F.col(c)==0).cast("int"))*100,2).alias(f"{c}_zero_pct") for c in cycle_cols],
      )
      .orderBy(F.desc("n_projects"))
)

display(proj_qc_by_mgr)





mgr_cov = (
    d3.groupBy("pm_clean")
      .agg(
          F.count("*").alias("n_projects"),
          F.count("ct_a2_to_b_days").alias("n_a2_b"),
          F.count("ct_b_to_c_days").alias("n_b_c"),
          F.count("ct_c_to_d_days").alias("n_c_d"),
          F.count("ct_d_to_e_days").alias("n_d_e"),
          F.expr("percentile_approx(ct_a2_to_b_days, 0.5)").alias("med_a2_b"),
          F.expr("percentile_approx(ct_b_to_c_days, 0.5)").alias("med_b_c"),
          F.expr("percentile_approx(ct_c_to_d_days, 0.5)").alias("med_c_d"),
          F.expr("percentile_approx(ct_d_to_e_days, 0.5)").alias("med_d_e"),
      )
      .withColumn("cov_a2_b", F.round(F.col("n_a2_b")/F.col("n_projects"),3))
      .withColumn("cov_b_c",  F.round(F.col("n_b_c") /F.col("n_projects"),3))
      .withColumn("cov_c_d",  F.round(F.col("n_c_d") /F.col("n_projects"),3))
      .withColumn("cov_d_e",  F.round(F.col("n_d_e") /F.col("n_projects"),3))
      .orderBy(F.desc("n_projects"))
)

display(mgr_cov)





stage_by_mgr = (
    d3.groupBy("pm_clean","project_status_gate_based")
      .count()
)

# Share distribution per manager
stage_share = (
    stage_by_mgr
    .groupBy("pm_clean")
    .pivot("project_status_gate_based")
    .sum("count")
    .na.fill(0)
)

display(stage_share)





flag_cols = ["A2_timeliness_flag","B_timeliness_flag","C_timeliness_flag","D_timeliness_flag","E_timeliness_flag"]

flag_null_by_mgr = (
    d3.groupBy("pm_clean")
      .agg(
          F.count("*").alias("n_projects"),
          *[F.round(F.mean(F.col(c).isNull().cast("int"))*100,2).alias(f"{c}_null_pct") for c in flag_cols],
      )
      .orderBy(F.desc("n_projects"))
)

display(flag_null_by_mgr)






# Build your mgr table as you do, but stop before the imputer
mgr_raw = mgr  # assuming mgr is created up to feat_cols casting

feat_cols = ["med_a2_b","med_b_c","med_c_d","med_d_e","late_rate","overdue_rate","noplan_rate"]

mgr_feat_nulls = mgr_raw.agg(
    F.count("*").alias("n_managers"),
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"{c}_null_cnt") for c in feat_cols],
    *[F.round(F.mean(F.col(c).isNull().cast("int"))*100,2).alias(f"{c}_null_pct") for c in feat_cols],
)
display(mgr_feat_nulls)

















# =========================
# USE CASE 3: Manager Archetypes (pm_clean)
# =========================
d3 = df.filter((F.col("benchmark_eligible")==1) & (F.col("is_sequence_full")==1))

flag_cols = ["A2_timeliness_flag","B_timeliness_flag","C_timeliness_flag","D_timeliness_flag","E_timeliness_flag"]

def cnt_flag(col, flag):
    return F.avg((F.upper(F.col(col)) == flag).cast("int"))  # rate

mgr = (
    d3.groupBy("pm_clean")
      .agg(
          F.count("*").alias("n_projects"),
          # Cycle medians (robust)
          F.expr("percentile_approx(ct_a2_to_b_days, 0.5)").alias("med_a2_b"),
          F.expr("percentile_approx(ct_b_to_c_days, 0.5)").alias("med_b_c"),
          F.expr("percentile_approx(ct_c_to_d_days, 0.5)").alias("med_c_d"),
          F.expr("percentile_approx(ct_d_to_e_days, 0.5)").alias("med_d_e"),
          # Rates per gate then average them
          cnt_flag("A2_timeliness_flag","LATE").alias("late_rate_a2"),
          cnt_flag("B_timeliness_flag","LATE").alias("late_rate_b"),
          cnt_flag("C_timeliness_flag","LATE").alias("late_rate_c"),
          cnt_flag("D_timeliness_flag","LATE").alias("late_rate_d"),
          cnt_flag("E_timeliness_flag","LATE").alias("late_rate_e"),
          cnt_flag("A2_timeliness_flag","OVERDUE").alias("overdue_rate_a2"),
          cnt_flag("B_timeliness_flag","OVERDUE").alias("overdue_rate_b"),
          cnt_flag("C_timeliness_flag","OVERDUE").alias("overdue_rate_c"),
          cnt_flag("D_timeliness_flag","OVERDUE").alias("overdue_rate_d"),
          cnt_flag("E_timeliness_flag","OVERDUE").alias("overdue_rate_e"),
          cnt_flag("A2_timeliness_flag","NO_PLAN").alias("noplan_rate_a2"),
          cnt_flag("B_timeliness_flag","NO_PLAN").alias("noplan_rate_b"),
          cnt_flag("C_timeliness_flag","NO_PLAN").alias("noplan_rate_c"),
          cnt_flag("D_timeliness_flag","NO_PLAN").alias("noplan_rate_d"),
          cnt_flag("E_timeliness_flag","NO_PLAN").alias("noplan_rate_e"),
      )
)

# Average rates across gates
mgr = (mgr
       .withColumn("late_rate", (F.col("late_rate_a2")+F.col("late_rate_b")+F.col("late_rate_c")+F.col("late_rate_d")+F.col("late_rate_e"))/5.0)
       .withColumn("overdue_rate", (F.col("overdue_rate_a2")+F.col("overdue_rate_b")+F.col("overdue_rate_c")+F.col("overdue_rate_d")+F.col("overdue_rate_e"))/5.0)
       .withColumn("noplan_rate", (F.col("noplan_rate_a2")+F.col("noplan_rate_b")+F.col("noplan_rate_c")+F.col("noplan_rate_d")+F.col("noplan_rate_e"))/5.0)
)

# Filter tiny sample managers
MIN_N = 20
mgr = mgr.filter(F.col("n_projects") >= MIN_N)

feat_cols = ["med_a2_b","med_b_c","med_c_d","med_d_e","late_rate","overdue_rate","noplan_rate"]

# Cast numeric
for c in feat_cols:
    mgr = mgr.withColumn(c, F.col(c).cast("double"))

imputer = Imputer(strategy="median", inputCols=feat_cols, outputCols=[c+"_imp" for c in feat_cols])
assembler = VectorAssembler(inputCols=[c+"_imp" for c in feat_cols], outputCol="features_raw", handleInvalid="keep")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

K = 4
kmeans = KMeans(k=K, seed=42, featuresCol="features")
pca = PCA(k=2, inputCol="features", outputCol="pca2")

pipe = Pipeline(stages=[imputer, assembler, scaler, kmeans, pca])
m3 = pipe.fit(mgr)
out3 = m3.transform(mgr).withColumnRenamed("prediction","cluster_manager")

plot_cluster_sizes(out3, "cluster_manager", f"Use Case 3: Manager archetypes (n>={MIN_N}) — cluster sizes")
plot_pca_scatter(out3, "pca2", "cluster_manager", f"Use Case 3: Manager archetypes — PCA scatter", frac=1.0)

display(out3.select("pm_clean","n_projects","cluster_manager",*feat_cols).orderBy("cluster_manager", F.desc("med_d_e")))

