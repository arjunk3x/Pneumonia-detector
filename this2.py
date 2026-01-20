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
import matplotlib.pyplot as plt

base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)

transitions = [
    ("A2→B", "seq_a2_b_status", "ct_a2_to_b_days"),
    ("B→C",  "seq_b_c_status",  "ct_b_to_c_days"),
    ("C→D",  "seq_c_d_status",  "ct_c_to_d_days"),
    ("D→E",  "seq_d_e_status",  "ct_d_to_e_days"),
]

bench_rows = []
for name, status_col, ct_col in transitions:
    stats = (base_df
        .filter(F.col(status_col) == "OK")
        .select(F.col(ct_col).alias("cycle_days"))
        .filter(F.col("cycle_days").isNotNull())
        .agg(
            F.count("*").alias("n_datapoints"),
            F.expr("percentile_approx(cycle_days, 0.2)").alias("P20_days"),
            F.expr("percentile_approx(cycle_days, 0.5)").alias("P50_days"),
            F.expr("percentile_approx(cycle_days, 0.8)").alias("P80_days"),
            F.round(F.avg("cycle_days"), 1).alias("Mean_days"),
        )
        .withColumn("transition", F.lit(name))
    )
    bench_rows.append(stats)

benchmarks = bench_rows[0]
for b in bench_rows[1:]:
    benchmarks = benchmarks.unionByName(b)

benchmarks_pd = benchmarks.select("transition","n_datapoints","P20_days","P50_days","P80_days","Mean_days") \
                          .orderBy("transition") \
                          .toPandas()

# Plot: P20–P80 range with P50 dot
plt.figure(figsize=(9, 3.5))
y = list(range(len(benchmarks_pd)))

x = benchmarks_pd["P50_days"].values
left_err  = (benchmarks_pd["P50_days"] - benchmarks_pd["P20_days"]).values
right_err = (benchmarks_pd["P80_days"] - benchmarks_pd["P50_days"]).values

plt.errorbar(x=x, y=y, xerr=[left_err, right_err], fmt='o')
plt.yticks(y, benchmarks_pd["transition"])
plt.xlabel("Days")
plt.title("Gate-to-Gate Benchmarks (P20–P80 range, dot = P50)")

# add n labels
for i, n in enumerate(benchmarks_pd["n_datapoints"].values):
    plt.text(benchmarks_pd["P80_days"].values[i], i, f"  n={n}", va="center")

plt.tight_layout()
plt.show()

display(benchmarks)










from pyspark.sql import functions as F
import matplotlib.pyplot as plt

base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)

same_day_rows = []
for name, status_col, ct_col in transitions:
    tmp = (base_df
        .filter(F.col(status_col) == "OK")
        .select(F.col(ct_col).alias("cycle_days"))
        .filter(F.col("cycle_days").isNotNull())
        .withColumn("is_same_day", F.when(F.col("cycle_days") == 0, F.lit(1)).otherwise(F.lit(0)))
        .agg(
            F.count("*").alias("n_datapoints"),
            (F.avg("is_same_day") * 100).alias("pct_same_day")
        )
        .withColumn("transition", F.lit(name))
    )
    same_day_rows.append(tmp)

same_day_df = same_day_rows[0]
for s in same_day_rows[1:]:
    same_day_df = same_day_df.unionByName(s)

same_day_pd = same_day_df.select("transition","n_datapoints","pct_same_day") \
                         .orderBy("transition") \
                         .toPandas()

plt.figure(figsize=(9, 3.2))
plt.barh(same_day_pd["transition"], same_day_pd["pct_same_day"])
plt.xlabel("% of datapoints that are same-day (0 days)")
plt.title("Same-day Transitions (0-day cycle time)")

plt.tight_layout()
plt.show()

display(same_day_df)














from pyspark.sql import functions as F
import matplotlib.pyplot as plt

base_df = df_OPPM.filter(F.col("benchmark_eligible") == 1)

dist_rows = []
for name, status_col, ct_col in transitions:
    dist = (base_df
        .filter(F.col(status_col) == "OK")
        .select(F.col(ct_col).alias("cycle_days"))
        .filter(F.col("cycle_days").isNotNull() & (F.col("cycle_days") > 0))
        .agg(
            F.count("*").alias("n_nonzero"),
            F.expr("percentile_approx(cycle_days, 0.05)").alias("p05"),
            F.expr("percentile_approx(cycle_days, 0.25)").alias("p25"),
            F.expr("percentile_approx(cycle_days, 0.50)").alias("p50"),
            F.expr("percentile_approx(cycle_days, 0.75)").alias("p75"),
            F.expr("percentile_approx(cycle_days, 0.95)").alias("p95"),
        )
        .withColumn("transition", F.lit(name))
    )
    dist_rows.append(dist)

dist_df = dist_rows[0]
for d in dist_rows[1:]:
    dist_df = dist_df.unionByName(d)

dist_pd = dist_df.select("transition","n_nonzero","p05","p25","p50","p75","p95") \
                 .orderBy("transition") \
                 .toPandas()

# Build bxp stats for matplotlib (no raw points required)
bxp_stats = []
for _, r in dist_pd.iterrows():
    bxp_stats.append({
        "label": r["transition"],
        "whislo": r["p05"],
        "q1": r["p25"],
        "med": r["p50"],
        "q3": r["p75"],
        "whishi": r["p95"],
        "fliers": []
    })

plt.figure(figsize=(9, 3.6))
plt.bxp(bxp_stats, vert=False, showfliers=False)
plt.xlabel("Days (non-zero only)")
plt.title("Non-zero Cycle Time Spread (P05–P95 whiskers, box=P25–P75, line=P50)")
plt.tight_layout()
plt.show()

display(dist_df)












from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd

status_rows = []
status_map = [
    ("A2→B", "seq_a2_b_status"),
    ("B→C",  "seq_b_c_status"),
    ("C→D",  "seq_c_d_status"),
    ("D→E",  "seq_d_e_status"),
]

for tname, scol in status_map:
    tmp = (df_OPPM
        .groupBy(F.col(scol).alias("status"))
        .count()
        .withColumn("transition", F.lit(tname))
    )
    status_rows.append(tmp)

status_df = status_rows[0]
for s in status_rows[1:]:
    status_df = status_df.unionByName(s)

status_pd = status_df.toPandas()
pivot = status_pd.pivot(index="transition", columns="status", values="count").fillna(0)

# Keep consistent order if columns exist
for col in ["OK", "MISSING", "OUT_OF_ORDER"]:
    if col not in pivot.columns:
        pivot[col] = 0
pivot = pivot[["OK", "MISSING", "OUT_OF_ORDER"]]

ax = pivot.plot(kind="bar", stacked=True, figsize=(9, 3.8))
ax.set_xlabel("Transition")
ax.set_ylabel("Count")
ax.set_title("Sequence Data Quality by Transition (OK vs Missing vs Out-of-order)")
plt.tight_layout()
plt.show()

display(status_df.orderBy("transition","status"))















from pyspark.sql import functions as F
import matplotlib.pyplot as plt

dist_zero = (df_OPPM
    .groupBy("consecutive_zero_cnt")
    .count()
    .orderBy("consecutive_zero_cnt")
)

dist_zero_pd = dist_zero.toPandas()

plt.figure(figsize=(8.5, 3.2))
plt.bar(dist_zero_pd["consecutive_zero_cnt"].astype(str), dist_zero_pd["count"])
plt.xlabel("consecutive_zero_cnt (0..4)")
plt.ylabel("Number of projects")
plt.title("Distribution of Same-day Consecutive Transitions")
plt.tight_layout()
plt.show()

display(dist_zero)




from pyspark.sql import functions as F

kpis = (df_OPPM.agg(
    F.round(F.avg(F.when(F.col("all_gates_same_date")==1, 1).otherwise(0))*100, 2).alias("pct_all_gates_same_date"),
    F.round(F.avg(F.when(F.col("consecutive_zero_cnt")>=2, 1).otherwise(0))*100, 2).alias("pct_2plus_consecutive_zero"),
    F.round(F.avg(F.when(F.col("benchmark_eligible")==1, 1).otherwise(0))*100, 2).alias("pct_benchmark_eligible"),
    F.count("*").alias("n_projects")
))

display(kpis)




