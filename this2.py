# ============================================================
# 0) LIBS
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler,
    StringIndexer, OneHotEncoder
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ============================================================
# 1) START FROM df (your dataframe)
#    - df contains category (OPPM/P6) and all engineered columns
# ============================================================
df0 = df

# Helper: safe column check
def require_cols(df_in, cols):
    missing = [c for c in cols if c not in df_in.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

# Optional: standardize category
df0 = df0.withColumn("category_clean", F.upper(F.trim(F.col("category").cast("string"))))

# ============================================================
# 2) COMMON HELPERS
# ============================================================
def fit_kmeans_numeric(
    df_in,
    feature_cols,
    out_col,
    k=4,
    seed=42,
    filter_nulls=True,
    scale=True
):
    """
    Numeric-only KMeans with optional StandardScaler.
    Adds a cluster column out_col and returns (df_out, model, silhouette)
    """
    require_cols(df_in, feature_cols)

    df_use = df_in
    if filter_nulls:
        for c in feature_cols:
            df_use = df_use.filter(F.col(c).isNotNull())

    # Build feature vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol=f"__vec_{out_col}")
    stages = [assembler]

    if scale:
        scaler = StandardScaler(inputCol=f"__vec_{out_col}", outputCol=f"__scaled_{out_col}", withMean=True, withStd=True)
        stages.append(scaler)
        features_col = f"__scaled_{out_col}"
    else:
        features_col = f"__vec_{out_col}"

    kmeans = KMeans(featuresCol=features_col, predictionCol=out_col, k=k, seed=seed)
    stages.append(kmeans)

    pipe = Pipeline(stages=stages)
    model = pipe.fit(df_use)
    df_out = model.transform(df_use)

    evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol=out_col, metricName="silhouette")
    sil = evaluator.evaluate(df_out)

    return df_out, model, sil

def pick_k_by_silhouette(df_in, feature_cols, ks=(2,3,4,5,6), out_col_prefix="tmp_k"):
    """Quick helper to choose k by silhouette (higher is better)."""
    rows = []
    for k in ks:
        df_k, _, sil = fit_kmeans_numeric(df_in, feature_cols, out_col=f"{out_col_prefix}_{k}", k=k)
        rows.append((k, float(sil)))
    return spark.createDataFrame(rows, ["k", "silhouette"]).orderBy(F.desc("silhouette"))

# ============================================================
# 3) CLUSTER #1 — Timeline shape (cycle times)
#    Uses: ct_a2_to_b_days, ct_b_to_c_days, ct_c_to_d_days, ct_d_to_e_days
#    Best when sequence is clean.
# ============================================================
timeline_cols = ["ct_a2_to_b_days", "ct_b_to_c_days", "ct_c_to_d_days", "ct_d_to_e_days"]
require_cols(df0, ["benchmark_eligible", "is_sequence_full"] + timeline_cols)

df_timeline = (
    df0.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

# (Optional) choose k quickly
# display(pick_k_by_silhouette(df_timeline, timeline_cols, ks=(2,3,4,5,6), out_col_prefix="k_timeline"))

df_timeline_clustered, timeline_model, timeline_sil = fit_kmeans_numeric(
    df_timeline,
    feature_cols=timeline_cols,
    out_col="cluster_timeline",
    k=4
)

print("Timeline clustering silhouette:", timeline_sil)
display(df_timeline_clustered.groupBy("cluster_timeline").count().orderBy(F.desc("count")))
display(
    df_timeline_clustered.groupBy("cluster_timeline")
    .agg(*[F.round(F.avg(c), 1).alias(f"avg_{c}") for c in timeline_cols])
    .orderBy("cluster_timeline")
)

# ============================================================
# 4) CLUSTER #2 — Timeliness behaviour (planning vs execution)
#    Create per-project numeric features:
#      counts of flags + mean delay on available gates
# ============================================================
gates = ["A2","B","C","D","E"]
flag_cols  = [f"{g}_timeliness_flag" for g in gates]
delay_cols = [f"{g}_delay_days_actual_minus_planned" for g in gates]

require_cols(df0, ["benchmark_eligible", "is_sequence_full"] + flag_cols + delay_cols)

df_beh = (
    df0.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

# Build counts from flags
def count_flag(flag):
    return sum(F.when(F.upper(F.col(c).cast("string")) == flag, 1).otherwise(0) for c in flag_cols)

df_beh = (
    df_beh
    .withColumn("cnt_late",     count_flag("LATE"))
    .withColumn("cnt_overdue",  count_flag("OVERDUE"))
    .withColumn("cnt_on_time",  count_flag("ON_TIME"))
    .withColumn("cnt_early",    count_flag("EARLY"))
    .withColumn("cnt_upcoming", count_flag("UPCOMING"))
    .withColumn("cnt_no_plan",  count_flag("NO_PLAN"))
)

# Mean delay over available (non-null) delay values
delay_sum = sum(F.coalesce(F.col(c).cast("double"), F.lit(0.0)) for c in delay_cols)
delay_n   = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in delay_cols)

df_beh = df_beh.withColumn("avg_delay_days", F.when(delay_n > 0, delay_sum / delay_n).otherwise(F.lit(None).cast("double")))
df_beh = df_beh.withColumn("max_delay_days", F.greatest(*[F.coalesce(F.col(c).cast("double"), F.lit(float("-inf"))) for c in delay_cols]))

beh_cols = ["cnt_late","cnt_overdue","cnt_on_time","cnt_early","cnt_upcoming","cnt_no_plan","avg_delay_days","max_delay_days"]

# (Optional) choose k quickly
# display(pick_k_by_silhouette(df_beh, beh_cols, ks=(2,3,4,5,6), out_col_prefix="k_beh"))

df_beh_clustered, beh_model, beh_sil = fit_kmeans_numeric(
    df_beh,
    feature_cols=beh_cols,
    out_col="cluster_timeliness",
    k=4
)

print("Timeliness-behaviour clustering silhouette:", beh_sil)
display(df_beh_clustered.groupBy("cluster_timeliness").count().orderBy(F.desc("count")))
display(
    df_beh_clustered.groupBy("cluster_timeliness")
    .agg(*[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in beh_cols])
    .orderBy("cluster_timeliness")
)

# ============================================================
# 5) CLUSTER #3 — Data quality / anomaly clustering
#    Uses your quality flags & anomaly indicators
# ============================================================
quality_cols = [
    "gate_dates_present_cnt",
    "sequence_breaks",
    "consecutive_zero_cnt",
    "all_gates_same_date",
    "zero_a2_b","zero_b_c","zero_c_d","zero_d_e",
    "is_sequence_strict",
    "is_sequence_full"
]
require_cols(df0, quality_cols)

# For anomaly clustering, DON'T filter too aggressively — you want the weird ones too.
df_quality = df0

# Ensure everything numeric
df_quality = (
    df_quality
    .withColumn("all_gates_same_date", F.col("all_gates_same_date").cast("int"))
    .withColumn("is_sequence_strict",  F.col("is_sequence_strict").cast("int"))
    .withColumn("is_sequence_full",    F.col("is_sequence_full").cast("int"))
)

# (Optional) choose k quickly
# display(pick_k_by_silhouette(df_quality, quality_cols, ks=(2,3,4,5,6), out_col_prefix="k_qual"))

df_quality_clustered, qual_model, qual_sil = fit_kmeans_numeric(
    df_quality,
    feature_cols=quality_cols,
    out_col="cluster_quality",
    k=4,
    filter_nulls=True
)

print("Quality/anomaly clustering silhouette:", qual_sil)
display(df_quality_clustered.groupBy("cluster_quality").count().orderBy(F.desc("count")))
display(
    df_quality_clustered.groupBy("cluster_quality")
    .agg(*[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in quality_cols])
    .orderBy("cluster_quality")
)

# ============================================================
# 6) CLUSTER #4 — Portfolio segmentation (mixed categorical + numeric)
#    Categories: region, Investment Type, Project Type, Delivery Unit
#    Plus numeric behaviours (small set): avg_delay_days, cnt_overdue, cnt_no_plan
# ============================================================
cat_cols = ["region", "Investment Type", "Project Type", "Delivery Unit"]
num_cols = ["avg_delay_days", "cnt_overdue", "cnt_no_plan"]

# Build from df_beh (already has numeric behaviour columns)
df_port = df_beh  # already filtered to benchmark_eligible & is_sequence_full

require_cols(df_port, cat_cols + num_cols)

# Clean category columns
for c in cat_cols:
    df_port = df_port.withColumn(c, F.when(F.trim(F.col(c).cast("string")) == "", F.lit("Unknown"))
                                   .otherwise(F.coalesce(F.trim(F.col(c).cast("string")), F.lit("Unknown"))))

# Index + OneHotEncode categorical cols
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
encoder  = OneHotEncoder(inputCols=[f"{c}_idx" for c in cat_cols],
                         outputCols=[f"{c}_ohe" for c in cat_cols],
                         handleInvalid="keep")

# Assemble numeric + ohe
assembler = VectorAssembler(
    inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
    outputCol="__vec_port"
)

scaler = StandardScaler(inputCol="__vec_port", outputCol="__scaled_port", withMean=True, withStd=True)
kmeans = KMeans(featuresCol="__scaled_port", predictionCol="cluster_portfolio", k=5, seed=42)

pipe = Pipeline(stages=indexers + [encoder, assembler, scaler, kmeans])
port_model = pipe.fit(df_port)
df_port_clustered = port_model.transform(df_port)

evaluator = ClusteringEvaluator(featuresCol="__scaled_port", predictionCol="cluster_portfolio", metricName="silhouette")
port_sil = evaluator.evaluate(df_port_clustered)

print("Portfolio segmentation silhouette:", port_sil)
display(df_port_clustered.groupBy("cluster_portfolio").count().orderBy(F.desc("count")))

# Simple cluster profiling: dominant categories + numeric averages
display(
    df_port_clustered.groupBy("cluster_portfolio")
    .agg(
        F.round(F.avg("avg_delay_days"), 2).alias("avg_avg_delay_days"),
        F.round(F.avg("cnt_overdue"), 2).alias("avg_cnt_overdue"),
        F.round(F.avg("cnt_no_plan"), 2).alias("avg_cnt_no_plan"),
        F.expr("percentile_approx(avg_delay_days, 0.5)").alias("p50_avg_delay_days")
    )
    .orderBy("cluster_portfolio")
)

# For dominant category values per cluster (quick view), run a few top-value counts:
for c in cat_cols:
    print(f"Top {c} per cluster:")
    display(
        df_port_clustered.groupBy("cluster_portfolio", c).count()
        .orderBy("cluster_portfolio", F.desc("count"))
        .limit(50)
    )

# ============================================================
# 7) Optional: join cluster labels back

