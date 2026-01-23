from pyspark.sql import functions as F

# ----------------------------
# 0) Start from df (as you requested)
# ----------------------------
df_base = (
    df
    .filter(F.col("benchmark_eligible") == 1)
    .filter(F.col("is_sequence_full") == 1)  # full A2→B→C→D→E sequence
)

ct_cols = ["ct_a2_to_b_days", "ct_b_to_c_days", "ct_c_to_d_days", "ct_d_to_e_days"]

# ----------------------------
# 1) Clean cycle-time columns
#    - cast to double
#    - negative -> NULL (out-of-order)
# ----------------------------
for c in ct_cols:
    df_base = df_base.withColumn(c, F.col(c).cast("double"))
    df_base = df_base.withColumn(c, F.when(F.col(c) < 0, F.lit(None)).otherwise(F.col(c)))

# ----------------------------
# 2) Keep only rows where all 4 transitions exist (passed gates)
#    NOTE: this automatically removes projects where later gates are not reached yet
# ----------------------------
nonnull_cond = None
for c in ct_cols:
    cond = F.col(c).isNotNull()
    nonnull_cond = cond if nonnull_cond is None else (nonnull_cond & cond)

df_timeline = (
    df_base
    .filter(nonnull_cond)
    .withColumn("total_cycle_days", sum([F.col(c) for c in ct_cols]))
    .filter(F.col("total_cycle_days") > 0)   # avoid divide-by-zero "all zeros"
)

# ----------------------------
# 3) Shape features = share of total timeline
# ----------------------------
df_timeline = (
    df_timeline
    .withColumn("share_a2b", F.col("ct_a2_to_b_days") / F.col("total_cycle_days"))
    .withColumn("share_bc",  F.col("ct_b_to_c_days")  / F.col("total_cycle_days"))
    .withColumn("share_cd",  F.col("ct_c_to_d_days")  / F.col("total_cycle_days"))
    .withColumn("share_de",  F.col("ct_d_to_e_days")  / F.col("total_cycle_days"))
)

# Optional quick check
display(df_timeline.select("Project ID", *ct_cols, "total_cycle_days", "share_a2b","share_bc","share_cd","share_de").limit(10))




from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

feature_cols = ["share_a2b","share_bc","share_cd","share_de"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="skip"   # safety: if any unexpected null sneaks in, Spark will skip that row
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# --- find good K (2..8) ---
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")

scores = []
for k in range(2, 9):
    kmeans = KMeans(k=k, seed=42, featuresCol="features")
    pipe = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipe.fit(df_timeline)
    pred = model.transform(df_timeline)
    score = evaluator.evaluate(pred)
    scores.append((k, score))

scores_df = spark.createDataFrame(scores, ["k", "silhouette"])
display(scores_df.orderBy(F.desc("silhouette")))


K = 4

kmeans = KMeans(k=K, seed=42, featuresCol="features")
pipe = Pipeline(stages=[assembler, scaler, kmeans])

model = pipe.fit(df_timeline)
df_clustered = model.transform(df_timeline).withColumnRenamed("prediction", "cluster_timeline")

display(df_clustered.select("Project ID", "category_clean", "cluster_timeline", *ct_cols, "total_cycle_days").limit(20))







import matplotlib.pyplot as plt

pdf_sizes = (
    df_clustered.groupBy("cluster_timeline")
    .agg(F.count("*").alias("n"))
    .orderBy("cluster_timeline")
    .toPandas()
)

plt.figure(figsize=(8,4))
plt.bar(pdf_sizes["cluster_timeline"].astype(str), pdf_sizes["n"])
plt.xlabel("Cluster")
plt.ylabel("Number of projects")
plt.title("Timeline Shape Clusters — Size")
plt.tight_layout()
plt.show()






pdf_shape = (
    df_clustered.groupBy("cluster_timeline")
    .agg(
        F.avg("share_a2b").alias("share_a2b"),
        F.avg("share_bc").alias("share_bc"),
        F.avg("share_cd").alias("share_cd"),
        F.avg("share_de").alias("share_de"),
        F.avg("total_cycle_days").alias("avg_total_days"),
        F.count("*").alias("n")
    )
    .orderBy("cluster_timeline")
    .toPandas()
)

transitions = ["A2→B","B→C","C→D","D→E"]

plt.figure(figsize=(10,4))
for _, row in pdf_shape.iterrows():
    y = [row["share_a2b"], row["share_bc"], row["share_cd"], row["share_de"]]
    plt.plot(transitions, y, marker="o", label=f"Cluster {int(row['cluster_timeline'])} (n={int(row['n'])})")
plt.xlabel("Transition")
plt.ylabel("Avg share of total timeline")
plt.title("Timeline Shape Clusters — Where time is spent")
plt.legend()
plt.tight_layout()
plt.show()

display(spark.createDataFrame(pdf_shape))
