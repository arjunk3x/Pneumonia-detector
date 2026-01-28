from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA, Imputer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt

# ----------------------------
# 1) Prepare region-level feature table
# ----------------------------
r = region_benchmark_table

# Cast + clean
r = (r
     .withColumn("n_projects_total", F.col("n_projects_total").cast("double"))
     .withColumn("n_A2_B", F.coalesce(F.col("n_A2_B"), F.lit(0)).cast("double"))
     .withColumn("n_B_C",  F.coalesce(F.col("n_B_C"),  F.lit(0)).cast("double"))
     .withColumn("n_C_D",  F.coalesce(F.col("n_C_D"),  F.lit(0)).cast("double"))
     .withColumn("n_D_E",  F.coalesce(F.col("n_D_E"),  F.lit(0)).cast("double"))
     .withColumn("good_A2_B_p50", F.col("good_A2_B_p50").cast("double"))
     .withColumn("good_B_C_p50",  F.col("good_B_C_p50").cast("double"))
     .withColumn("good_C_D_p50",  F.col("good_C_D_p50").cast("double"))
     .withColumn("good_D_E_p50",  F.col("good_D_E_p50").cast("double"))
)

# Coverage rates (helps avoid size-only clustering)
# (still keeping the raw counts as you requested)
r = (r
     .withColumn("cov_A2_B", F.when(F.col("n_projects_total") > 0, F.col("n_A2_B")/F.col("n_projects_total")).otherwise(F.lit(0.0)))
     .withColumn("cov_B_C",  F.when(F.col("n_projects_total") > 0, F.col("n_B_C") /F.col("n_projects_total")).otherwise(F.lit(0.0)))
     .withColumn("cov_C_D",  F.when(F.col("n_projects_total") > 0, F.col("n_C_D") /F.col("n_projects_total")).otherwise(F.lit(0.0)))
     .withColumn("cov_D_E",  F.when(F.col("n_projects_total") > 0, F.col("n_D_E") /F.col("n_projects_total")).otherwise(F.lit(0.0)))
)

# Log counts (stops big regions dominating purely by size)
r = (r
     .withColumn("log_n_A2_B", F.log1p(F.col("n_A2_B")))
     .withColumn("log_n_B_C",  F.log1p(F.col("n_B_C")))
     .withColumn("log_n_C_D",  F.log1p(F.col("n_C_D")))
     .withColumn("log_n_D_E",  F.log1p(F.col("n_D_E")))
)

# Missing flags for benchmarks (these happen when n_* = 0)
for g in ["good_A2_B_p50","good_B_C_p50","good_C_D_p50","good_D_E_p50"]:
    r = r.withColumn(f"{g}_missing", F.when(F.col(g).isNull(), F.lit(1.0)).otherwise(F.lit(0.0)))

display(r.orderBy("region"))

# ----------------------------
# 2) Choose feature columns for clustering (Region-level)
# ----------------------------
feature_cols = [
    # speed profile
    "good_A2_B_p50","good_B_C_p50","good_C_D_p50","good_D_E_p50",
    # maturity / coverage (counts + proportions)
    "log_n_A2_B","log_n_B_C","log_n_C_D","log_n_D_E",
    "cov_A2_B","cov_B_C","cov_C_D","cov_D_E",
    # missingness flags so clusters can capture "sparse/unreliable"
    "good_A2_B_p50_missing","good_B_C_p50_missing","good_C_D_p50_missing","good_D_E_p50_missing",
]

# Median-impute numeric nulls so VectorAssembler won't crash
imputer = Imputer(strategy="median", inputCols=feature_cols, outputCols=[c+"_imp" for c in feature_cols])

assembler = VectorAssembler(inputCols=[c+"_imp" for c in feature_cols], outputCol="features_raw", handleInvalid="keep")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

# Optional: pick K via silhouette
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")

scores = []
for k in range(2, 8):
    km = KMeans(k=k, seed=42, featuresCol="features")
    pipe = Pipeline(stages=[imputer, assembler, scaler, km])
    m = pipe.fit(r)
    pred_tmp = m.transform(r)
    scores.append((k, evaluator.evaluate(pred_tmp)))

scores_df = spark.createDataFrame(scores, ["k","silhouette"])
display(scores_df.orderBy(F.desc("silhouette")))

# ----------------------------
# 3) Fit final clustering model (set K)
# ----------------------------
K = 4  # change based on silhouette + interpretability

kmeans = KMeans(k=K, seed=42, featuresCol="features", predictionCol="region_cluster")
pca = PCA(k=2, inputCol="features", outputCol="pca2")

pipe = Pipeline(stages=[imputer, assembler, scaler, kmeans, pca])
model = pipe.fit(r)
region_clustered = model.transform(r)

display(region_clustered.select("region","n_projects_total","n_A2_B","n_B_C","n_C_D","n_D_E",
                                "good_A2_B_p50","good_B_C_p50","good_C_D_p50","good_D_E_p50",
                                "region_cluster").orderBy("region_cluster","region"))

# ----------------------------
# 4) Cluster profile table (interpretation)
# ----------------------------
profile = (
    region_clustered.groupBy("region_cluster")
    .agg(
        F.count("*").alias("n_regions"),
        F.round(F.avg("n_projects_total"),0).alias("avg_projects"),
        F.round(F.avg("n_A2_B"),0).alias("avg_n_A2_B"),
        F.round(F.avg("n_B_C"),0).alias("avg_n_B_C"),
        F.round(F.avg("n_C_D"),0).alias("avg_n_C_D"),
        F.round(F.avg("n_D_E"),0).alias("avg_n_D_E"),
        F.round(F.avg("good_A2_B_p50"),0).alias("avg_good_A2_B"),
        F.round(F.avg("good_B_C_p50"),0).alias("avg_good_B_C"),
        F.round(F.avg("good_C_D_p50"),0).alias("avg_good_C_D"),
        F.round(F.avg("good_D_E_p50"),0).alias("avg_good_D_E"),
        F.round(F.avg("cov_A2_B"),2).alias("avg_cov_A2_B"),
        F.round(F.avg("cov_D_E"),2).alias("avg_cov_D_E")
    )
    .orderBy("region_cluster")
)
display(profile)

# ----------------------------
# 5) PCA scatter plot (regions)
# ----------------------------
pdf = (region_clustered
       .select("region","region_cluster",
               F.col("pca2")[0].alias("x"),
               F.col("pca2")[1].alias("y"))
       .toPandas())

plt.figure(figsize=(9,6))
plt.scatter(pdf["x"], pdf["y"], c=pdf["region_cluster"], s=60, alpha=0.8)
for _, row in pdf.iterrows():
    plt.text(row["x"], row["y"], str(row["region"]), fontsize=8, alpha=0.8)
plt.xlabel("PCA-1")
plt.ylabel("PCA-2")
plt.title("Region Clusters (speed + coverage maturity) — PCA scatter")
plt.tight_layout()
plt.show()

