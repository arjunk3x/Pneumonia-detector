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


