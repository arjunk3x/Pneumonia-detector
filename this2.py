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
        F.to_date(c, "dd-MMM-yy"),     # e.g., 01-Jan-90 (Spark may interpret as 2090)
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

        # ---- FIX (Gate D issue): map "90" -> 1990 (not 2090) ----
        # Applies only when raw string ends with -90 or /90 and parse produced year 2090
        if re.search(r"$", s) and dt.year == 2090:
            dt = dt.replace(year=1990)

        return dt.date()
    except Exception:
        return None

# Apply standardisation:
# - First parse with Spark
# - If Spark returns NULL but raw value is present, try dateutil UDF
# - Special fix ONLY for Gate D: if "90" got parsed as 2090, shift to 1990
for gc in gate_cols:
    raw = F.trim(F.col(gc).cast("string"))
    spark_parsed = parse_date_spark(gc)

    # ---- FIX (Spark side) for Gate D only ----
    if gc == "Gate D Decision Date":
        # If raw ends with -90 or /90 AND Spark parsed year is 2090 -> subtract 100 years
        spark_parsed = F.when(
            (raw.rlike(r".*$")) & (F.year(spark_parsed) == 2090),
            F.add_months(spark_parsed, -1200)  # 100 years = 1200 months
        ).otherwise(spark_parsed)

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



