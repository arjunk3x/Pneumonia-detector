from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, PCA
)
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array

import pandas as pd
import matplotlib.pyplot as plt


def plot_cluster_sizes(df_pred, cluster_col, title):
    counts = (df_pred.groupBy(cluster_col).count().orderBy(cluster_col))
    pdf = counts.toPandas()
    plt.figure(figsize=(8,3))
    plt.bar(pdf[cluster_col].astype(str), pdf["count"])
    plt.title(title)
    plt.xlabel("Cluster")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.show()
    display(counts)

def plot_pca_scatter(df_pred, pca_col, cluster_col, title, sample_n=5000):
    df_vis = (
        df_pred
        .select(cluster_col, vector_to_array(F.col(pca_col)).alias("p"))
        .select(cluster_col, F.col("p")[0].alias("x"), F.col("p")[1].alias("y"))
        .dropna()
        .limit(sample_n)
        .toPandas()
    )

    plt.figure(figsize=(7,4))
    # plot each cluster separately (no explicit colors)
    for cl in sorted(df_vis[cluster_col].unique()):
        sub = df_vis[df_vis[cluster_col] == cl]
        plt.scatter(sub["x"], sub["y"], s=10, label=f"cluster {cl}", alpha=0.6)
    plt.title(title)
    plt.xlabel("PCA 1")
    plt.ylabel("PCA 2")
    plt.legend()
    plt.tight_layout()
    plt.show()






# --------- 1) Filter clean rows ----------
df1 = (
    df
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
    .filter(F.col("project_status_gate_based") == "Completed")
)

cycle_cols = ["ct_a2_to_b_days", "ct_b_to_c_days", "ct_c_to_d_days", "ct_d_to_e_days"]

# cast + keep non-negative
for c in cycle_cols:
    df1 = df1.withColumn(c, F.when(F.col(c).cast("double") >= 0, F.col(c).cast("double")).otherwise(None))

# --------- 2) Build pipeline (impute -> scale -> PCA -> KMeans) ----------
imputer = Imputer(inputCols=cycle_cols, outputCols=[c+"_imp" for c in cycle_cols], strategy="median")

assembler = VectorAssembler(
    inputCols=[c+"_imp" for c in cycle_cols],
    outputCol="features_raw",
    handleInvalid="keep"
)

scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
pca = PCA(k=2, inputCol="features", outputCol="pca")

kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster_timeline")

pipe = Pipeline(stages=[imputer, assembler, scaler, pca, kmeans])
model1 = pipe.fit(df1)
df1_pred = model1.transform(df1)

# --------- 3) Visuals ----------
plot_cluster_sizes(df1_pred, "cluster_timeline", "Cluster sizes — Lifecycle timeline shape")
plot_pca_scatter(df1_pred, "pca", "cluster_timeline", "PCA view — Timeline clusters")

# --------- 4) Cluster profiles (means) ----------
profile1 = (
    df1_pred.groupBy("cluster_timeline")
    .agg(*[F.round(F.avg(c),1).alias(f"avg_{c}") for c in cycle_cols], F.count("*").alias("n"))
    .orderBy("cluster_timeline")
)
display(profile1)





# --------- 1) Start from clean df for analysis ----------
df2 = df.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))

gates = ["A2","B","C","D","E"]
decision_map = {
    "A2":"Gate A2 Decision Date", "B":"Gate B Decision Date", "C":"Gate C Decision Date",
    "D":"Gate D Decision Date", "E":"Gate E Decision Date"
}
plan_map = {
    "A2":"Gate A2 Approval Date", "B":"Gate B Approval Date", "C":"Gate C Approval Date",
    "D":"Gate D Approval Date", "E":"Gate E Approval Date"
}

today = F.current_date()

# --------- 2) Create numeric delay = Decision - Planned (positive => late) ----------
for g in gates:
    dcol = F.col(decision_map[g]).cast("date")
    pcol = F.col(plan_map[g]).cast("date")

    df2 = df2.withColumn(
        f"{g}_delay_actual_minus_planned_days",
        F.when(dcol.isNotNull() & pcol.isNotNull(), F.datediff(dcol, pcol)).otherwise(None).cast("double")
    )

    # per-gate status flag
    df2 = df2.withColumn(
        f"{g}_plan_status",
        F.when(pcol.isNull(), F.lit("NO_PLAN"))
         .when(dcol.isNull() & (today > pcol), F.lit("OVERDUE"))
         .when(dcol.isNull() & (today <= pcol), F.lit("UPCOMING"))
         .when(dcol.isNotNull() & (F.datediff(dcol, pcol) < 0), F.lit("EARLY"))
         .when(dcol.isNotNull() & (F.datediff(dcol, pcol) == 0), F.lit("ON_TIME"))
         .otherwise(F.lit("LATE"))
    )

# --------- 3) Behaviour counts across gates ----------
df2 = df2.withColumn("n_late", sum(F.col(f"{g}_plan_status").eqNullSafe("LATE").cast("int") for g in gates)) \
         .withColumn("n_overdue", sum(F.col(f"{g}_plan_status").eqNullSafe("OVERDUE").cast("int") for g in gates)) \
         .withColumn("n_no_plan", sum(F.col(f"{g}_plan_status").eqNullSafe("NO_PLAN").cast("int") for g in gates)) \
         .withColumn("n_upcoming", sum(F.col(f"{g}_plan_status").eqNullSafe("UPCOMING").cast("int") for g in gates)) \
         .withColumn("n_on_time", sum(F.col(f"{g}_plan_status").eqNullSafe("ON_TIME").cast("int") for g in gates)) \
         .withColumn("n_early", sum(F.col(f"{g}_plan_status").eqNullSafe("EARLY").cast("int") for g in gates))

delay_cols = [f"{g}_delay_actual_minus_planned_days" for g in gates]
feat_cols = delay_cols + ["n_late","n_overdue","n_no_plan","n_upcoming","n_on_time","n_early"]

# --------- 4) Pipeline (impute -> scale -> PCA -> GMM) ----------
imputer = Imputer(inputCols=delay_cols, outputCols=[c+"_imp" for c in delay_cols], strategy="median")
assembler = VectorAssembler(inputCols=[c+"_imp" for c in delay_cols] + ["n_late","n_overdue","n_no_plan","n_upcoming","n_on_time","n_early"],
                            outputCol="features_raw", handleInvalid="keep")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
pca = PCA(k=2, inputCol="features", outputCol="pca")
gmm = GaussianMixture(k=4, seed=42, featuresCol="features", predictionCol="cluster_timeliness")

pipe = Pipeline(stages=[imputer, assembler, scaler, pca, gmm])
model2 = pipe.fit(df2)
df2_pred = model2.transform(df2)

# --------- 5) Visuals ----------
plot_cluster_sizes(df2_pred, "cluster_timeliness", "Cluster sizes — Timeliness behaviour")
plot_pca_scatter(df2_pred, "pca", "cluster_timeliness", "PCA view — Timeliness clusters")

# --------- 6) Cluster profile ----------
profile2 = (
    df2_pred.groupBy("cluster_timeliness")
    .agg(
        F.count("*").alias("n"),
        F.round(F.avg("n_late"),2).alias("avg_n_late"),
        F.round(F.avg("n_overdue"),2).alias("avg_n_overdue"),
        F.round(F.avg("n_no_plan"),2).alias("avg_n_no_plan")
    ).orderBy("cluster_timeliness")
)
display(profile2)






df3 = df.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))

cat_cols = ["region", "Investment Type", "Project Type", "Delivery Unit", "category_clean"]
num_cols = [
    "ct_a2_to_b_days","ct_b_to_c_days","ct_c_to_d_days","ct_d_to_e_days",
    "gate_dates_present_cnt","consecutive_zero_cnt","all_gates_same_date"
]

# 1) Fill categoricals
df3 = df3.fillna("Unknown", subset=cat_cols)

# 2) Cast numerics
for c in num_cols:
    df3 = df3.withColumn(c, F.col(c).cast("double"))

# 3) Pipeline
imputer = Imputer(inputCols=num_cols, outputCols=[c+"_imp" for c in num_cols], strategy="median")

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in cat_cols], outputCols=[f"{c}_ohe" for c in cat_cols], handleInvalid="keep")

assembler = VectorAssembler(
    inputCols=[c+"_imp" for c in num_cols] + [f"{c}_ohe" for c in cat_cols],
    outputCol="features_raw",
    handleInvalid="keep"
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
pca = PCA(k=2, inputCol="features", outputCol="pca")

kmeans = KMeans(k=5, seed=42, featuresCol="features", predictionCol="cluster_profile")

pipe = Pipeline(stages=[imputer] + indexers + [ohe, assembler, scaler, pca, kmeans])
model3 = pipe.fit(df3)
df3_pred = model3.transform(df3)

# 4) Visuals
plot_cluster_sizes(df3_pred, "cluster_profile", "Cluster sizes — Portfolio profiles")
plot_pca_scatter(df3_pred, "pca", "cluster_profile", "PCA view — Portfolio profile clusters")

# 5) Profile table (what each cluster looks like)
profile3 = (
    df3_pred.groupBy("cluster_profile")
    .agg(
        F.count("*").alias("n"),
        F.round(F.avg("ct_a2_to_b_days"),1).alias("avg_a2_b"),
        F.round(F.avg("ct_b_to_c_days"),1).alias("avg_b_c"),
        F.round(F.avg("ct_c_to_d_days"),1).alias("avg_c_d"),
        F.round(F.avg("ct_d_to_e_days"),1).alias("avg_d_e"),
        F.expr("percentile_approx(ct_d_to_e_days, 0.8)").alias("p80_d_e")
    ).orderBy("cluster_profile")
)
display(profile3)






# Start from clean rows
df4 = df.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))

# Ensure pm_clean exists
df4 = df4.withColumn("pm_clean", F.coalesce(F.trim(F.col("pm").cast("string")), F.lit("Unknown")))

# Recompute per-gate plan_status quickly (same logic as earlier, but only need counts)
gates = ["A2","B","C","D","E"]
decision_map = {"A2":"Gate A2 Decision Date","B":"Gate B Decision Date","C":"Gate C Decision Date","D":"Gate D Decision Date","E":"Gate E Decision Date"}
plan_map = {"A2":"Gate A2 Approval Date","B":"Gate B Approval Date","C":"Gate C Approval Date","D":"Gate D Approval Date","E":"Gate E Approval Date"}
today = F.current_date()

for g in gates:
    dcol = F.col(decision_map[g]).cast("date")
    pcol = F.col(plan_map[g]).cast("date")
    df4 = df4.withColumn(
        f"{g}_plan_status",
        F.when(pcol.isNull(), F.lit("NO_PLAN"))
         .when(dcol.isNull() & (today > pcol), F.lit("OVERDUE"))
         .when(dcol.isNull() & (today <= pcol), F.lit("UPCOMING"))
         .when(dcol.isNotNull() & (F.datediff(dcol, pcol) <= 0), F.lit("ON_OR_EARLY"))
         .otherwise(F.lit("LATE"))
    )

# Aggregate per manager
mgr = (
    df4.groupBy("pm_clean")
    .agg(
        F.count("*").alias("n_projects"),
        F.expr("percentile_approx(ct_a2_to_b_days, 0.5)").alias("med_a2_b"),
        F.expr("percentile_approx(ct_b_to_c_days, 0.5)").alias("med_b_c"),
        F.expr("percentile_approx(ct_c_to_d_days, 0.5)").alias("med_c_d"),
        F.expr("percentile_approx(ct_d_to_e_days, 0.5)").alias("med_d_e"),
        # rates
        (F.avg(F.col("A2_plan_status").eqNullSafe("LATE").cast("int"))).alias("pct_late_A2"),
        (F.avg(F.col("B_plan_status").eqNullSafe("LATE").cast("int"))).alias("pct_late_B"),
        (F.avg(F.col("C_plan_status").eqNullSafe("LATE").cast("int"))).alias("pct_late_C"),
        (F.avg(F.col("D_plan_status").eqNullSafe("LATE").cast("int"))).alias("pct_late_D"),
        (F.avg(F.col("E_plan_status").eqNullSafe("LATE").cast("int"))).alias("pct_late_E"),
        (F.avg(F.col("A2_plan_status").eqNullSafe("NO_PLAN").cast("int"))).alias("pct_no_plan_A2"),
    )
    .filter(F.col("n_projects") >= 20)   # change threshold here
)

# Prepare features
mgr_num = [
    "med_a2_b","med_b_c","med_c_d","med_d_e",
    "pct_late_A2","pct_late_B","pct_late_C","pct_late_D","pct_late_E","pct_no_plan_A2"
]

for c in mgr_num:
    mgr = mgr.withColumn(c, F.col(c).cast("double"))

imputer = Imputer(inputCols=mgr_num, outputCols=[c+"_imp" for c in mgr_num], strategy="median")
assembler = VectorAssembler(inputCols=[c+"_imp" for c in mgr_num], outputCol="features_raw", handleInvalid="keep")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
pca = PCA(k=2, inputCol="features", outputCol="pca")
kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster_manager")

pipe = Pipeline(stages=[imputer, assembler, scaler, pca, kmeans])
model4 = pipe.fit(mgr)
mgr_pred = model4.transform(mgr)

# Visuals
plot_cluster_sizes(mgr_pred, "cluster_manager", "Cluster sizes — Manager archetypes")
plot_pca_scatter(mgr_pred, "pca", "cluster_manager", "PCA view — Manager clusters")

# Show managers by cluster (example)
display(mgr_pred.select("pm_clean","n_projects","cluster_manager","med_a2_b","med_d_e","pct_late_C").orderBy("cluster_manager", F.desc("med_d_e")))



























from pyspark.sql import functions as F

# Use your main df or df_analysis; you said use df
d = df

# Optional: keep only rows you use for benchmark analysis
d = d.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))

cycle_cols = [
    "ct_a2_to_b_days",
    "ct_b_to_c_days",
    "ct_c_to_d_days",
    "ct_d_to_e_days"
]

# Ensure numeric
for c in cycle_cols:
    d = d.withColumn(c, F.col(c).cast("double"))

# 1) Overall counts
overall = d.agg(
    F.count("*").alias("rows"),
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"{c}_null") for c in cycle_cols],
    *[F.sum((F.col(c) < 0).cast("int")).alias(f"{c}_neg") for c in cycle_cols],
    *[F.sum((F.col(c) == 0).cast("int")).alias(f"{c}_zero") for c in cycle_cols],
).withColumn(
    "rows_with_any_null",
    sum(F.col(f"{c}_null") for c in cycle_cols)  # not exact rows, but quick indicator
)

display(overall)

# 2) By category (OPPM vs P6)
by_cat = (
    d.groupBy("category_clean")
     .agg(
         F.count("*").alias("rows"),
         *[F.sum(F.col(c).isNull().cast("int")).alias(f"{c}_null") for c in cycle_cols],
         *[F.sum((F.col(c) < 0).cast("int")).alias(f"{c}_neg") for c in cycle_cols],
         *[F.sum((F.col(c) == 0).cast("int")).alias(f"{c}_zero") for c in cycle_cols],
     )
     .orderBy("category_clean")
)
display(by_cat)

# 3) Null/neg rates (percentages) by category
by_cat_rates = by_cat
for c in cycle_cols:
    by_cat_rates = (
        by_cat_rates
        .withColumn(f"{c}_null_pct", F.round(F.col(f"{c}_null") * 100.0 / F.col("rows"), 2))
        .withColumn(f"{c}_neg_pct",  F.round(F.col(f"{c}_neg")  * 100.0 / F.col("rows"), 2))
        .withColumn(f"{c}_zero_pct", F.round(F.col(f"{c}_zero") * 100.0 / F.col("rows"), 2))
    )
display(by_cat_rates.select("category_clean","rows", *[f"{c}_{m}_pct" for c in cycle_cols for m in ["null","neg","zero"]]))











# Percentiles help decide whether to cap/winsorize
percentiles = [0.01, 0.05, 0.5, 0.8, 0.9, 0.95, 0.99]

rows = []
for c in cycle_cols:
    stats = (
        d.filter(F.col(c).isNotNull() & (F.col(c) >= 0))
         .groupBy("category_clean")
         .agg(
             F.count("*").alias("n"),
             *[F.expr(f"percentile_approx({c}, {p})").alias(f"p{int(p*100)}") for p in percentiles],
             F.max(c).alias("max")
         )
         .withColumn("transition", F.lit(c))
    )
    rows.append(stats)

pct_df = rows[0]
for r in rows[1:]:
    pct_df = pct_df.unionByName(r)

display(pct_df.orderBy("transition","category_clean"))







# Where do negatives happen? Are they in OUT_OF_ORDER transitions?
qc = (
    d.select(
        "Project ID", "category_clean",
        "seq_a2_b_status","seq_b_c_status","seq_c_d_status","seq_d_e_status",
        "sequence_breaks",
        "all_gates_same_date","consecutive_zero_cnt",
        *cycle_cols
    )
)

# Count negatives by seq status
neg_by_seq = (
    qc.select(
        "category_clean",
        F.when(F.col("ct_a2_to_b_days") < 0, F.lit("A2→B")).alias("neg_transition"),
        "seq_a2_b_status"
    )
    .filter(F.col("neg_transition").isNotNull())
    .groupBy("category_clean","neg_transition","seq_a2_b_status")
    .count()
)

display(neg_by_seq.orderBy(F.desc("count")))

# Similar for other transitions (quick union)
def neg_table(ct_col, label, seq_col):
    return (
        qc.select("category_clean",
                  F.when(F.col(ct_col) < 0, F.lit(label)).alias("neg_transition"),
                  F.col(seq_col).alias("seq_status"))
        .filter(F.col("neg_transition").isNotNull())
        .groupBy("category_clean","neg_transition","seq_status")
        .count()
    )

neg_all = neg_table("ct_a2_to_b_days","A2→B","seq_a2_b_status") \
    .unionByName(neg_table("ct_b_to_c_days","B→C","seq_b_c_status")) \
    .unionByName(neg_table("ct_c_to_d_days","C→D","seq_c_d_status")) \
    .unionByName(neg_table("ct_d_to_e_days","D→E","seq_d_e_status"))

display(neg_all.orderBy("neg_transition", F.desc("count")))







pattern_df = (
    d.select(
        "Project ID", "category_clean",
        *[F.col(c).isNotNull().cast("int").alias(c+"_has") for c in cycle_cols]
    )
    .withColumn("has_pattern", F.concat_ws("",
        *[F.col(c+"_has").cast("string") for c in cycle_cols]
    ))
)

pattern_summary = (
    pattern_df.groupBy("category_clean","has_pattern")
              .count()
              .orderBy("category_clean", F.desc("count"))
)

display(pattern_summary)


