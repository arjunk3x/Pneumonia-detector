# ============================================================
# 0) LIBS
# ============================================================
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler,
    StringIndexer, OneHotEncoder
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ============================================================
# 1) START FROM df (as you requested)
# ============================================================
df0 = df

# Make sure category is clean (OPPM / P6)
df0 = df0.withColumn("category_clean", F.upper(F.trim(F.col("category").cast("string"))))

PID = "Project ID"

# ============================================================
# 2) ENSURE delay_days_actual_minus_planned exists (for df)
#    delay = Decision Date - Approval Date
# ============================================================
gate_map = [
    ("A2", "Gate A2 Decision Date", "Gate A2 Approval Date"),
    ("B",  "Gate B Decision Date",  "Gate B Approval Date"),
    ("C",  "Gate C Decision Date",  "Gate C Approval Date"),
    ("D",  "Gate D Decision Date",  "Gate D Approval Date"),
    ("E",  "Gate E Decision Date",  "Gate E Approval Date"),
]

for g, dcol, acol in gate_map:
    new_col = f"{g}_delay_days_actual_minus_planned"
    if new_col not in df0.columns:
        df0 = df0.withColumn(
            new_col,
            F.when(F.col(dcol).isNotNull() & F.col(acol).isNotNull(),
                   F.datediff(F.col(dcol), F.col(acol)).cast("int")
            ).otherwise(F.lit(None).cast("int"))
        )

# ============================================================
# 3) ENSURE timeliness_flag exists (optional safety)
#    If missing, we build it using decision vs approval + current_date.
# ============================================================
analysis_date = F.current_date()

for g, dcol, acol in gate_map:
    flag_col = f"{g}_timeliness_flag"
    delay_col = f"{g}_delay_days_actual_minus_planned"
    if flag_col not in df0.columns:
        df0 = df0.withColumn(
            flag_col,
            F.when(F.col(acol).isNull(), F.lit("NO_PLAN"))
             .when(F.col(dcol).isNotNull() & (F.col(delay_col) < 0), F.lit("EARLY"))
             .when(F.col(dcol).isNotNull() & (F.col(delay_col) == 0), F.lit("ON_TIME"))
             .when(F.col(dcol).isNotNull() & (F.col(delay_col) > 0), F.lit("LATE"))
             .when(F.col(dcol).isNull() & (analysis_date > F.col(acol)), F.lit("OVERDUE"))
             .otherwise(F.lit("UPCOMING"))
        )

# ============================================================
# 4) Helper: numeric KMeans with scaling
# ============================================================
def kmeans_numeric(df_in, feature_cols, out_col, k=4, seed=42, filter_nulls=True):
    df_use = df_in
    if filter_nulls:
        for c in feature_cols:
            df_use = df_use.filter(F.col(c).isNotNull())

    assembler = VectorAssembler(inputCols=feature_cols, outputCol=f"__vec_{out_col}")
    scaler = StandardScaler(inputCol=f"__vec_{out_col}", outputCol=f"__scaled_{out_col}", withMean=True, withStd=True)
    kmeans = KMeans(featuresCol=f"__scaled_{out_col}", predictionCol=out_col, k=k, seed=seed)

    pipe = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipe.fit(df_use)
    out = model.transform(df_use)

    sil = ClusteringEvaluator(featuresCol=f"__scaled_{out_col}", predictionCol=out_col, metricName="silhouette").evaluate(out)
    return out, model, sil

# ============================================================
# 5) CLUSTER #1 — Timeline shape (cycle times)
#    Uses: ct_a2_to_b_days, ct_b_to_c_days, ct_c_to_d_days, ct_d_to_e_days
# ============================================================
timeline_cols = ["ct_a2_to_b_days", "ct_b_to_c_days", "ct_c_to_d_days", "ct_d_to_e_days"]

df_timeline = (
    df0.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

df_timeline_clustered, model_timeline, sil_timeline = kmeans_numeric(
    df_timeline, timeline_cols, out_col="cluster_timeline", k=4
)

print("Timeline clustering silhouette:", sil_timeline)
display(df_timeline_clustered.groupBy("cluster_timeline").count().orderBy(F.desc("count")))
display(
    df_timeline_clustered.groupBy("cluster_timeline")
    .agg(*[F.round(F.avg(c), 1).alias(f"avg_{c}") for c in timeline_cols])
    .orderBy("cluster_timeline")
)

# ============================================================
# 6) CLUSTER #2 — Timeliness behaviour
#    Features per project:
#      counts of flags + avg/max delay over gates
# ============================================================
flag_cols  = [f"{g}_timeliness_flag" for g,_,_ in gate_map]
delay_cols = [f"{g}_delay_days_actual_minus_planned" for g,_,_ in gate_map]

df_beh = (
    df0.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

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

delay_sum = sum(F.coalesce(F.col(c).cast("double"), F.lit(0.0)) for c in delay_cols)
delay_n   = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in delay_cols)

df_beh = (
    df_beh
    .withColumn("avg_delay_days", F.when(delay_n > 0, delay_sum / delay_n).otherwise(F.lit(None).cast("double")))
    .withColumn("max_delay_days", F.greatest(*[F.coalesce(F.col(c).cast("double"), F.lit(float("-inf"))) for c in delay_cols]))
)

beh_cols = ["cnt_late","cnt_overdue","cnt_on_time","cnt_early","cnt_upcoming","cnt_no_plan","avg_delay_days","max_delay_days"]

df_beh_clustered, model_beh, sil_beh = kmeans_numeric(
    df_beh, beh_cols, out_col="cluster_timeliness", k=4
)

print("Timeliness clustering silhouette:", sil_beh)
display(df_beh_clustered.groupBy("cluster_timeliness").count().orderBy(F.desc("count")))
display(
    df_beh_clustered.groupBy("cluster_timeliness")
    .agg(*[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in beh_cols])
    .orderBy("cluster_timeliness")
)

# ============================================================
# 7) CLUSTER #3 — Data quality / anomaly clustering
#    IMPORTANT: sequence_breaks is STRING -> we don't use it directly.
#    Instead we create numeric flags for out-of-order transitions.
# ============================================================
df_q = df0

df_q = (
    df_q
    .withColumn("oo_a2_b", F.when(F.col("seq_a2_b_status") == "OUT_OF_ORDER", 1).otherwise(0))
    .withColumn("oo_b_c",  F.when(F.col("seq_b_c_status")  == "OUT_OF_ORDER", 1).otherwise(0))
    .withColumn("oo_c_d",  F.when(F.col("seq_c_d_status")  == "OUT_OF_ORDER", 1).otherwise(0))
    .withColumn("oo_d_e",  F.when(F.col("seq_d_e_status")  == "OUT_OF_ORDER", 1).otherwise(0))
    .withColumn("all_gates_same_date", F.col("all_gates_same_date").cast("int"))
    .withColumn("is_sequence_strict",  F.col("is_sequence_strict").cast("int"))
    .withColumn("is_sequence_full",    F.col("is_sequence_full").cast("int"))
)

quality_cols = [
    "gate_dates_present_cnt",
    "is_sequence_strict",
    "is_sequence_full",
    "oo_a2_b","oo_b_c","oo_c_d","oo_d_e",
    "zero_a2_b","zero_b_c","zero_c_d","zero_d_e",
    "consecutive_zero_cnt",
    "all_gates_same_date"
]

df_quality_clustered, model_quality, sil_quality = kmeans_numeric(
    df_q, quality_cols, out_col="cluster_quality", k=4
)

print("Quality clustering silhouette:", sil_quality)
display(df_quality_clustered.groupBy("cluster_quality").count().orderBy(F.desc("count")))
display(
    df_quality_clustered.groupBy("cluster_quality")
    .agg(*[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in quality_cols])
    .orderBy("cluster_quality")
)

# ============================================================
# 8) CLUSTER #4 — Portfolio segmentation (mixed categorical + numeric)
#    Categories: region, Investment Type, Project Type, Delivery Unit
#    Numeric: avg_delay_days, cnt_overdue, cnt_no_plan
# ============================================================
cat_cols = ["region", "Investment Type", "Project Type", "Delivery Unit"]
num_cols = ["avg_delay_days", "cnt_overdue", "cnt_no_plan"]

df_port = df_beh  # already has numeric fields

# Clean categorical
for c in cat_cols:
    df_port = df_port.withColumn(
        c,
        F.when(F.trim(F.col(c).cast("string")) == "", F.lit("Unknown"))
         .otherwise(F.coalesce(F.trim(F.col(c).cast("string")), F.lit("Unknown")))
    )

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
encoder = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in cat_cols],
    outputCols=[f"{c}_ohe" for c in cat_cols],
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
    outputCol="__vec_port"
)
scaler = StandardScaler(inputCol="__vec_port", outputCol="__scaled_port", withMean=True, withStd=True)

kmeans = KMeans(featuresCol="__scaled_port", predictionCol="cluster_portfolio", k=5, seed=42)

pipe = Pipeline(stages=indexers + [encoder, assembler, scaler, kmeans])
model_port = pipe.fit(df_port)
df_port_clustered = model_port.transform(df_port)

sil_port = ClusteringEvaluator(featuresCol="__scaled_port", predictionCol="cluster_portfolio", metricName="silhouette").evaluate(df_port_clustered)

print("Portfolio clustering silhouette:", sil_port)
display(df_port_clustered.groupBy("cluster_portfolio").count().orderBy(F.desc("count")))

# ============================================================
# 9) Join all cluster labels back into one table (per project)
# ============================================================
df_clusters = (
    df0.select(PID, "category_clean")
    .join(df_timeline_clustered.select(PID, "cluster_timeline"), on=PID, how="left")
    .join(df_beh_clustered.select(PID, "cluster_timeliness"), on=PID, how="left")
    .join(df_quality_clustered.select(PID, "cluster_quality"), on=PID, how="left")
    .join(df_port_clustered.select(PID, "cluster_portfolio"), on=PID, how="left")
)

display(df_clusters.limit(50))

# Optional: see cluster mix by OPPM vs P6
display(df_clusters.groupBy("category_clean", "cluster_timeline").count().orderBy("category_clean", "cluster_timeline"))
display(df_clusters.groupBy("category_clean", "cluster_timeliness").count().orderBy("category_clean", "cluster_timeliness"))
display(df_clusters.groupBy("category_clean", "cluster_quality").count().orderBy("category_clean", "cluster_quality"))
display(df_clusters.groupBy("category_clean", "cluster_portfolio").count().orderBy("category_clean", "cluster_portfolio"))
