from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---- 1) Load CSV (your existing code) ----
path = "file:/Workspace/Users/arjun.krishna@nationalgrid.com/Drafts/OPPM.csv"

df_OPPM = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
)

print("Displaying the full DataFrame. All rows are now loaded correctly.")
display(df_OPPM)

# ---- 2) Standardise Gate Decision Date columns (Spark-native first, dateutil fallback) ----
gate_cols = [
    "Gate A2 Decision Date",
    "Gate B Decision Date",
    "Gate C Decision Date",
    "Gate D Decision Date",
    "Gate E Decision Date"
]

# 2a) Spark-native parsing across multiple common formats (fast + scalable)
def parse_date_spark(colname: str):
    c = F.trim(F.col(colname).cast("string"))
    c = F.when((c == "") | c.isNull(), F.lit(None)).otherwise(c)

    parsed = F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "yyyy/MM/dd"),
        F.to_date(c, "dd/MM/yyyy"),
        F.to_date(c, "MM/dd/yyyy"),
        F.to_date(c, "dd-MM-yyyy"),
        F.to_date(c, "MM-dd-yyyy"),
        F.to_date(c, "dd-MMM-yyyy"),   # e.g., 05-Jan-2024
        F.to_date(c, "dd-MMM-yy"),     # e.g., 01-Jan-90 (problem case)
        F.to_date(c, "MMM dd, yyyy"),  # e.g., Jan 05, 2024
        F.to_date(F.to_timestamp(c))
    )
    return parsed

# 2b) Fallback parser using python-dateutil (handles messy / inconsistent formats)
from dateutil import parser
import re

@F.udf(returnType=T.DateType())
def parse_date_dateutil(s: str):
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        dt = parser.parse(s, fuzzy=True, dayfirst=True)

        # Fix ONLY the "90" year mapping: 2090 -> 1990
        # This keeps your rule: "all other years being converted to 2000+ is fine"
        if re.search(r"$", s) and dt.year == 2090:
            dt = dt.replace(year=1990)

        return dt.date()
    except Exception:
        return None

# Helper: fix the specific Spark parsing issue for yy=90 interpreted as 2090
# Apply only to Gate D and Gate E
def fix_90_to_1990_for_specific_gates(colname: str, raw_col, parsed_col):
    return F.when(
        (F.lit(colname).isin("Gate D Decision Date", "Gate E Decision Date")) &
        (raw_col.rlike(r"(?i).*(?:-|/)(90)$")) &
        (F.year(parsed_col) == 2090),
        F.add_months(parsed_col, -1200)  # subtract 100 years
    ).otherwise(parsed_col)

# Apply standardisation:
# - First parse with Spark
# - Fix 2090->1990 ONLY for Gate D/E when raw ends with -90 or /90
# - If Spark returns NULL but raw value is present, try dateutil UDF (includes same fix)
for gc in gate_cols:
    raw = F.trim(F.col(gc).cast("string"))
    raw = F.when((raw == "") | raw.isNull(), F.lit(None)).otherwise(raw)

    spark_parsed = parse_date_spark(gc)
    spark_parsed = fix_90_to_1990_for_specific_gates(gc, raw, spark_parsed)

    df_OPPM = df_OPPM.withColumn(
        gc,
        F.when(spark_parsed.isNotNull(), spark_parsed)
         .when((raw.isNotNull()) & (raw != ""), parse_date_dateutil(raw))
         .otherwise(F.lit(None).cast("date"))
    )

# Quick check (optional)
display(df_OPPM.select("Project ID", *gate_cols).limit(25))

# ---- 3) Categorise projects: Completed / In Progress / Not Started ----
non_null_count = sum(
    F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))
    for c in gate_cols
)
total_gates = len(gate_cols)

df_OPPM = (
    df_OPPM
    .withColumn("gate_dates_present_cnt", non_null_count)
    .withColumn(
        "project_status_gate_based",
        F.when(F.col("gate_dates_present_cnt") == total_gates, F.lit("Completed"))
         .when(F.col("gate_dates_present_cnt") == 0,          F.lit("Not Started"))
         .otherwise(F.lit("In Progress"))
    )
)

# Distribution (optional)
display(
    df_OPPM.groupBy("project_status_gate_based")
           .count()
           .orderBy(F.desc("count"))
)

# Inspect sample rows (optional)
display(
    df_OPPM.select(
        "Project ID",
        "project_status_gate_based",
        "gate_dates_present_cnt",
        *gate_cols
    ).limit(50)
)




from pyspark.sql import functions as F

# Gate date cols (already standardised to date)
gA2 = F.col("Gate A2 Decision Date")
gB  = F.col("Gate B Decision Date")
gC  = F.col("Gate C Decision Date")
gD  = F.col("Gate D Decision Date")
gE  = F.col("Gate E Decision Date")

# 1) Per-transition "zero-day" flags (same date across consecutive gates)
df_OPPM = (
    df_OPPM
    .withColumn("zero_a2_b", F.when(gA2.isNotNull() & gB.isNotNull() & (gA2 == gB), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("zero_b_c",  F.when(gB.isNotNull()  & gC.isNotNull() & (gB  == gC), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("zero_c_d",  F.when(gC.isNotNull()  & gD.isNotNull() & (gC  == gD), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("zero_d_e",  F.when(gD.isNotNull()  & gE.isNotNull() & (gD  == gE), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("consecutive_zero_cnt", F.col("zero_a2_b") + F.col("zero_b_c") + F.col("zero_c_d") + F.col("zero_d_e"))
)

# 2) Flag where ALL gates have the exact same date
df_OPPM = df_OPPM.withColumn(
    "all_gates_same_date",
    F.when(
        gA2.isNotNull() & gB.isNotNull() & gC.isNotNull() & gD.isNotNull() & gE.isNotNull() &
        (gA2 == gB) & (gB == gC) & (gC == gD) & (gD == gE),
        F.lit(1)
    ).otherwise(F.lit(0))
)

# 3) Benchmark eligibility (recommended default)
# Exclude: all gates same date OR 2+ consecutive zero transitions
df_OPPM = df_OPPM.withColumn(
    "benchmark_eligible",
    F.when((F.col("all_gates_same_date") == 1) | (F.col("consecutive_zero_cnt") >= 2), F.lit(0)).otherwise(F.lit(1))
)

# Optional quick view
display(df_OPPM.select(
    "Project ID",
    "all_gates_same_date",
    "consecutive_zero_cnt",
    "benchmark_eligible",
    "zero_a2_b", "zero_b_c", "zero_c_d", "zero_d_e",
    "Gate A2 Decision Date", "Gate B Decision Date", "Gate C Decision Date", "Gate D Decision Date", "Gate E Decision Date"
).limit(50))









base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)


df_a2_b = base_df.filter(F.col("seq_a2_b_status") == "OK") \
    .select("Project ID", "Project Type", "Delivery Unit", "region", F.col("ct_a2_to_b_days").alias("cycle_days"))

df_b_c = base_df.filter(F.col("seq_b_c_status") == "OK") \
    .select("Project ID", "Project Type", "Delivery Unit", "region", F.col("ct_b_to_c_days").alias("cycle_days"))

df_c_d = base_df.filter(F.col("seq_c_d_status") == "OK") \
    .select("Project ID", "Project Type", "Delivery Unit", "region", F.col("ct_c_to_d_days").alias("cycle_days"))

df_d_e = base_df.filter(F.col("seq_d_e_status") == "OK") \
    .select("Project ID", "Project Type", "Delivery Unit", "region", F.col("ct_d_to_e_days").alias("cycle_days"))


from pyspark.sql import functions as F

mean_a2_b = df_a2_b.agg(F.mean("cycle_days").alias("mean_days")).withColumn("transition", F.lit("A2→B"))
mean_b_c  = df_b_c.agg(F.mean("cycle_days").alias("mean_days")).withColumn("transition", F.lit("B→C"))
mean_c_d  = df_c_d.agg(F.mean("cycle_days").alias("mean_days")).withColumn("transition", F.lit("C→D"))
mean_d_e  = df_d_e.agg(F.mean("cycle_days").alias("mean_days")).withColumn("transition", F.lit("D→E"))

bench_means = mean_a2_b.unionByName(mean_b_c).unionByName(mean_c_d).unionByName(mean_d_e)
display(bench_means)


bench_means_by_unit = (
    df_b_c.groupBy("Delivery Unit")  # change to region/project type/etc.
          .agg(F.mean("cycle_days").alias("mean_days"),
               F.count("*").alias("n"))
          .orderBy(F.desc("n"))
)
display(bench_means_by_unit)












from pyspark.sql import functions as F

# Base filter for benchmarking (removes bulk same-day patterns etc.)
base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)

def bench_stats(df, transition_name):
    return (df.agg(
                F.count("*").alias("n_datapoints"),
                F.expr("percentile_approx(cycle_days, 0.2)").alias("P20_days"),
                F.expr("percentile_approx(cycle_days, 0.5)").alias("P50_days"),
                F.expr("percentile_approx(cycle_days, 0.8)").alias("P80_days"),
                F.round(F.avg("cycle_days"), 1).alias("Mean_days")
            )
            .withColumn("transition", F.lit(transition_name))
           )

# 1) A2 → B
df_a2_b = (
    base_df
    .filter(F.col("seq_a2_b_status") == "OK")
    .select("Project ID", F.col("ct_a2_to_b_days").alias("cycle_days"))
    .filter(F.col("cycle_days").isNotNull())
)

# 2) B → C
df_b_c = (
    base_df
    .filter(F.col("seq_b_c_status") == "OK")
    .select("Project ID", F.col("ct_b_to_c_days").alias("cycle_days"))
    .filter(F.col("cycle_days").isNotNull())
)

# 3) C → D
df_c_d = (
    base_df
    .filter(F.col("seq_c_d_status") == "OK")
    .select("Project ID", F.col("ct_c_to_d_days").alias("cycle_days"))
    .filter(F.col("cycle_days").isNotNull())
)

# 4) D → E
df_d_e = (
    base_df
    .filter(F.col("seq_d_e_status") == "OK")
    .select("Project ID", F.col("ct_d_to_e_days").alias("cycle_days"))
    .filter(F.col("cycle_days").isNotNull())
)

# Benchmarks (P20/P50/P80 + Mean) per transition
bench_a2_b = bench_stats(df_a2_b, "A2→B")
bench_b_c  = bench_stats(df_b_c,  "B→C")
bench_c_d  = bench_stats(df_c_d,  "C→D")
bench_d_e  = bench_stats(df_d_e,  "D→E")

benchmarks = (
    bench_a2_b
    .unionByName(bench_b_c)
    .unionByName(bench_c_d)
    .unionByName(bench_d_e)
    .select("transition", "n_datapoints", "P20_days", "P50_days", "P80_days", "Mean_days")
    .orderBy("transition")
)

display(benchmarks)























from pyspark.sql import functions as F

base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)

def bench_stats(df, transition_name):
    return (df.agg(
                F.count("*").alias("n_datapoints"),
                F.expr("percentile_approx(cycle_days, 0.2)").alias("P20_days"),
                F.expr("percentile_approx(cycle_days, 0.5)").alias("P50_days"),
                F.expr("percentile_approx(cycle_days, 0.8)").alias("P80_days"),
                F.round(F.avg("cycle_days"), 1).alias("Mean_days")
            )
            .withColumn("transition", F.lit(transition_name))
           )

df_a2_b = base_df.filter(F.col("seq_a2_b_status")=="OK").select(F.col("ct_a2_to_b_days").alias("cycle_days")).filter(F.col("cycle_days").isNotNull())
df_b_c  = base_df.filter(F.col("seq_b_c_status")=="OK").select(F.col("ct_b_to_c_days").alias("cycle_days")).filter(F.col("cycle_days").isNotNull())
df_c_d  = base_df.filter(F.col("seq_c_d_status")=="OK").select(F.col("ct_c_to_d_days").alias("cycle_days")).filter(F.col("cycle_days").isNotNull())
df_d_e  = base_df.filter(F.col("seq_d_e_status")=="OK").select(F.col("ct_d_to_e_days").alias("cycle_days")).filter(F.col("cycle_days").isNotNull())

benchmarks = (
    bench_stats(df_a2_b, "A2→B")
    .unionByName(bench_stats(df_b_c, "B→C"))
    .unionByName(bench_stats(df_c_d, "C→D"))
    .unionByName(bench_stats(df_d_e, "D→E"))
    .select("transition","n_datapoints","P20_days","P50_days","P80_days","Mean_days")
    .orderBy("transition")
)

display(benchmarks)






from pyspark.sql import functions as F

base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)

# Pick bucket edges (days). Adjust as needed.
# This makes your distribution interpretable and avoids huge tails dominating.
bins = [0, 7, 30, 90, 180, 365, 730, 1460, 3650]  # up to 10 years
labels = [
    "0-7", "8-30", "31-90", "91-180", "181-365",
    "366-730", "731-1460", "1461-3650", "3650+"
]

def bucketize(col):
    return (
        F.when(col.isNull(), None)
         .when(col <= bins[1], labels[0])
         .when((col > bins[1]) & (col <= bins[2]), labels[1])
         .when((col > bins[2]) & (col <= bins[3]), labels[2])
         .when((col > bins[3]) & (col <= bins[4]), labels[3])
         .when((col > bins[4]) & (col <= bins[5]), labels[4])
         .when((col > bins[5]) & (col <= bins[6]), labels[5])
         .when((col > bins[6]) & (col <= bins[7]), labels[6])
         .when((col > bins[7]) & (col <= bins[8]), labels[7])
         .otherwise(labels[8])
    )

df_dist = (
    base_df.select(
        F.when(F.col("seq_a2_b_status")=="OK", F.col("ct_a2_to_b_days")).alias("A2→B"),
        F.when(F.col("seq_b_c_status")=="OK",  F.col("ct_b_to_c_days")).alias("B→C"),
        F.when(F.col("seq_c_d_status")=="OK",  F.col("ct_c_to_d_days")).alias("C→D"),
        F.when(F.col("seq_d_e_status")=="OK",  F.col("ct_d_to_e_days")).alias("D→E")
    )
    .selectExpr("stack(4, 'A2→B', `A2→B`, 'B→C', `B→C`, 'C→D', `C→D`, 'D→E', `D→E`) as (transition, cycle_days)")
    .filter(F.col("cycle_days").isNotNull())
    .withColumn("bucket", bucketize(F.col("cycle_days")))
    .groupBy("transition","bucket")
    .count()
    .orderBy("transition","bucket")
)

display(df_dist)








from pyspark.sql import functions as F

dq_seq = df_OPPM.select(
    F.sum(F.when(F.col("seq_a2_b_status")=="OUT_OF_ORDER", 1).otherwise(0)).alias("A2→B_breaks"),
    F.sum(F.when(F.col("seq_b_c_status")=="OUT_OF_ORDER", 1).otherwise(0)).alias("B→C_breaks"),
    F.sum(F.when(F.col("seq_c_d_status")=="OUT_OF_ORDER", 1).otherwise(0)).alias("C→D_breaks"),
    F.sum(F.when(F.col("seq_d_e_status")=="OUT_OF_ORDER", 1).otherwise(0)).alias("D→E_breaks")
)

# Convert wide -> long for easy bar charting
dq_seq_long = dq_seq.selectExpr(
    "stack(4, 'A2→B', A2→B_breaks, 'B→C', B→C_breaks, 'C→D', C→D_breaks, 'D→E', D→E_breaks) as (transition, out_of_order_count)"
)

display(dq_seq_long)









from pyspark.sql import functions as F

same_day_dist = (
    df_OPPM.groupBy("consecutive_zero_cnt")
           .count()
           .orderBy("consecutive_zero_cnt")
)

display(same_day_dist)





total = df_OPPM.count()

same_day_kpis = df_OPPM.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("all_gates_same_date")==1, 1).otherwise(0)).alias("all_gates_same_date_cnt"),
    F.sum(F.when(F.col("consecutive_zero_cnt")>=2, 1).otherwise(0)).alias("two_or_more_zero_transitions_cnt"),
    F.sum(F.when(F.col("benchmark_eligible")==0, 1).otherwise(0)).alias("benchmark_excluded_cnt")
).withColumn(
    "all_gates_same_date_pct",
    F.round(F.col("all_gates_same_date_cnt") / F.col("total_rows") * 100, 2)
).withColumn(
    "two_or_more_zero_transitions_pct",
    F.round(F.col("two_or_more_zero_transitions_cnt") / F.col("total_rows") * 100, 2)
).withColumn(
    "benchmark_excluded_pct",
    F.round(F.col("benchmark_excluded_cnt") / F.col("total_rows") * 100, 2)
)

display(same_day_kpis)



