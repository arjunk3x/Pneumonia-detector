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
