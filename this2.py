from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# -----------------------------
# 0) Base filter
# -----------------------------
d3 = df_analysed.filter((F.col("benchmark_eligible")==1) & (F.col("is_sequence_full")==1))

# Make sure pm_clean is not null
d3 = d3.withColumn("pm_clean", F.coalesce(F.trim(F.col("pm_clean").cast("string")), F.lit("Unknown")))

gates = ["A2","B","C","D","E"]
flag_cols = [f"{g}_timeliness_flag" for g in gates]

# -----------------------------
# 1) Wide -> Long: one row per (project, gate)
#    This makes rates fair and simple
# -----------------------------
stack_expr = "stack(5, " + ", ".join([f"'{g}', {g}_timeliness_flag" for g in gates]) + ") as (gate, status)"

df_long = (
    d3.select(
        F.col("Project ID").alias("project_id"),
        "pm_clean",
        F.expr(stack_expr)
    )
    .withColumn("status", F.upper(F.trim(F.coalesce(F.col("status").cast("string"), F.lit("UNKNOWN")))))
)

# Keep only a known set of statuses (everything else -> UNKNOWN)
valid_status = ["EARLY","ON_TIME","ON TIME","LATE","OVERDUE","UPCOMING","NO_PLAN","NO PLAN","UNKNOWN"]
df_long = df_long.withColumn(
    "status",
    F.when(F.col("status").isin(valid_status), F.col("status")).otherwise(F.lit("UNKNOWN"))
)

# Normalise label variants
df_long = df_long.withColumn(
    "status",
    F.when(F.col("status")=="ON TIME", F.lit("ON_TIME"))
     .when(F.col("status")=="NO PLAN", F.lit("NO_PLAN"))
     .otherwise(F.col("status"))
)

status_order = ["EARLY","ON_TIME","LATE","OVERDUE","UPCOMING","NO_PLAN","UNKNOWN"]

# -----------------------------
# 2) Manager features = % mix of statuses across all their gate-records
# -----------------------------
mgr_counts = (
    df_long.groupBy("pm_clean","status").count()
)

mgr_pivot = (
    mgr_counts.groupBy("pm_clean")
              .pivot("status", status_order)
              .sum("count")
              .na.fill(0)
)

# Total gate-records per manager
mgr_pivot = mgr_pivot.withColumn("gate_records_total", sum(F.col(s) for s in status_order))

# Rates (0..1)
for s in status_order:
    mgr_pivot = mgr_pivot.withColumn(f"{s}_rate", F.col(s) / F.when(F.col("gate_records_total")==0, F.lit(None)).otherwise(F.col("gate_records_total")))

# Add number of unique projects per manager (so you can filter tiny PMs)
mgr_projects = d3.groupBy("pm_clean").agg(F.countDistinct(F.col("Project ID")).alias("n_projects"))

mgr = mgr_pivot.join(mgr_projects, on="pm_clean", how="left")

# Filter tiny sample PMs
MIN_N = 20
mgr = mgr.filter(F.col("n_projects") >= MIN_N)

# -----------------------------
# 3) Build clustering pipeline (timeliness-only features)
# -----------------------------
feat_cols = [f"{s}_rate" for s in status_order]  # 7 features: EARLY..UNKNOWN rates

# Cast to double
for c in feat_cols:
    mgr = mgr.withColumn(c, F.col(c).cast("double"))

imputer = Imputer(strategy="median", inputCols=feat_cols, outputCols=[c+"_imp" for c in feat_cols])
assembler = VectorAssembler(inputCols=[c+"_imp" for c in feat_cols], outputCol="features_raw", handleInvalid="keep")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

K = 4
kmeans = KMeans(k=K, seed=42, featuresCol="features")
pca = PCA(k=2, inputCol="features", outputCol="pca2")

pipe = Pipeline(stages=[imputer, assembler, scaler, kmeans, pca])
m = pipe.fit(mgr)
out = m.transform(mgr).withColumnRenamed("prediction","cluster_manager")

# -----------------------------
# 4) VISUALS
# -----------------------------
def plot_cluster_sizes(df, cluster_col, title):
    pdf = df.groupBy(cluster_col).count().orderBy(cluster_col).toPandas()
    plt.figure(figsize=(8,4))
    plt.bar(pdf[cluster_col].astype(str), pdf["count"])
    plt.title(title)
    plt.xlabel("Cluster")
    plt.ylabel("Number of managers")
    plt.tight_layout()
    plt.show()

def plot_pca_scatter(df, pca_col, color_col, title, frac=1.0):
    samp = df.sample(withReplacement=False, fraction=frac, seed=42)
    pdf = samp.select(color_col, pca_col).toPandas()
    # Split PCA vector into x,y
    xy = np.vstack(pdf[pca_col].apply(lambda v: (float(v[0]), float(v[1]))).values)
    x, y = xy[:,0], xy[:,1]
    c = pdf[color_col].astype(int).values

    plt.figure(figsize=(8,5))
    plt.scatter(x, y, c=c, s=25)
    plt.title(title)
    plt.xlabel("PCA 1")
    plt.ylabel("PCA 2")
    plt.tight_layout()
    plt.show()

def plot_cluster_status_mix(df, cluster_col, title):
    # Average rate per cluster
    agg_exprs = [F.avg(F.col(f"{s}_rate")).alias(f"{s}_rate") for s in status_order]
    prof = df.groupBy(cluster_col).agg(*agg_exprs).orderBy(cluster_col)
    pdf = prof.toPandas().set_index(cluster_col)

    plt.figure(figsize=(10,4))
    bottom = np.zeros(len(pdf))
    x = np.arange(len(pdf))

    for s in status_order:
        vals = (pdf[f"{s}_rate"].values * 100.0)
        plt.bar(x, vals, bottom=bottom, label=s)
        bottom += vals

    plt.xticks(x, [str(i) for i in pdf.index])
    plt.ylabel("% of gate records")
    plt.title(title)
    plt.legend(ncol=4, bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.show()

# Plot 1: cluster sizes
plot_cluster_sizes(out, "cluster_manager", f"Manager archetypes (timeliness-only) — cluster sizes (n>={MIN_N})")

# Plot 2: PCA scatter (the “points coming together” plot)
plot_pca_scatter(out, "pca2", "cluster_manager", "Manager archetypes — PCA scatter (timeliness-only)", frac=1.0)

# Plot 3: status mix per cluster (what each cluster actually represents)
plot_cluster_status_mix(out, "cluster_manager", "What each manager cluster looks like (status mix)")

# Optional: table to inspect
display(
    out.select(
        "pm_clean","n_projects","cluster_manager",
        *[F.round(F.col(f"{s}_rate")*100,1).alias(f"{s}_pct") for s in status_order]
    ).orderBy("cluster_manager", F.desc("OVERDUE_rate"))
)
