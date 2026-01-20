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











from pyspark.sql import functions as F

# Gate columns (already standardised to date)
a2 = F.col("Gate A2 Decision Date")
b  = F.col("Gate B Decision Date")
c  = F.col("Gate C Decision Date")
d  = F.col("Gate D Decision Date")
e  = F.col("Gate E Decision Date")

# Rule:
# - If a later gate exists, all previous gates must exist (no skipping)
# - And dates must be non-decreasing: A2 <= B <= C <= D <= E (only when both exist)
df_OPPM = df_OPPM.withColumn(
    "is_sequence",
    F.when(
        # ---- no skipping gates ----
        (e.isNotNull() & (d.isNull() | c.isNull() | b.isNull() | a2.isNull())) |
        (d.isNotNull() & (c.isNull() | b.isNull() | a2.isNull())) |
        (c.isNotNull() & (b.isNull() | a2.isNull())) |
        (b.isNotNull() & a2.isNull()) |
        # ---- out-of-order dates ----
        (a2.isNotNull() & b.isNotNull() & (b < a2)) |
        (b.isNotNull()  & c.isNotNull() & (c < b))  |
        (c.isNotNull()  & d.isNotNull() & (d < c))  |
        (d.isNotNull()  & e.isNotNull() & (e < d)),
        F.lit(False)
    ).otherwise(F.lit(True))
)

# Optional quick check
display(df_OPPM.select("Project ID", "is_sequence",
                       "Gate A2 Decision Date","Gate B Decision Date","Gate C Decision Date",
                       "Gate D Decision Date","Gate E Decision Date").limit(50))










from pyspark.sql import functions as F

# Gate date columns
gA2 = F.col("Gate A2 Decision Date")
gB  = F.col("Gate B Decision Date")
gC  = F.col("Gate C Decision Date")
gD  = F.col("Gate D Decision Date")
gE  = F.col("Gate E Decision Date")

def transition_status(prev_col, next_col, label):
    """
    Returns a string status for the transition:
    - MISSING: one or both dates missing
    - OUT_OF_ORDER: both present but next < prev
    - OK: both present and next >= prev
    """
    return (
        F.when(prev_col.isNull() | next_col.isNull(), F.lit("MISSING"))
         .when(next_col < prev_col, F.lit("OUT_OF_ORDER"))
         .otherwise(F.lit("OK"))
         .alias(f"seq_{label}_status")
    )

df_OPPM = (
    df_OPPM
    # 1) Status per transition
    .withColumn("seq_a2_b_status", transition_status(gA2, gB, "a2_b"))
    .withColumn("seq_b_c_status",  transition_status(gB,  gC, "b_c"))
    .withColumn("seq_c_d_status",  transition_status(gC,  gD, "c_d"))
    .withColumn("seq_d_e_status",  transition_status(gD,  gE, "d_e"))
)

# 2) Which transition(s) break the sequence (only OUT_OF_ORDER)
df_OPPM = df_OPPM.withColumn(
    "sequence_breaks",
    F.concat_ws(
        ",",
        F.array_remove(
            F.array(
                F.when(F.col("seq_a2_b_status") == "OUT_OF_ORDER", F.lit("A2->B")),
                F.when(F.col("seq_b_c_status")  == "OUT_OF_ORDER", F.lit("B->C")),
                F.when(F.col("seq_c_d_status")  == "OUT_OF_ORDER", F.lit("C->D")),
                F.when(F.col("seq_d_e_status")  == "OUT_OF_ORDER", F.lit("D->E"))
            ),
            F.lit(None)
        )
    )
)

# 3) Transition cycle times ONLY where status == OK, else NULL
df_OPPM = (
    df_OPPM
    .withColumn(
        "ct_a2_to_b_days",
        F.when(F.col("seq_a2_b_status") == "OK", F.datediff(gB, gA2))
    )
    .withColumn(
        "ct_b_to_c_days",
        F.when(F.col("seq_b_c_status") == "OK", F.datediff(gC, gB))
    )
    .withColumn(
        "ct_c_to_d_days",
        F.when(F.col("seq_c_d_status") == "OK", F.datediff(gD, gC))
    )
    .withColumn(
        "ct_d_to_e_days",
        F.when(F.col("seq_d_e_status") == "OK", F.datediff(gE, gD))
    )
)

# Optional: a clean "is_sequence_strict" that also checks for skipping
# (i.e., if D exists then C and B and A2 must exist, etc.)
df_OPPM = df_OPPM.withColumn(
    "is_sequence_strict",
    F.when(
        (gE.isNotNull() & (gD.isNull() | gC.isNull() | gB.isNull() | gA2.isNull())) |
        (gD.isNotNull() & (gC.isNull() | gB.isNull() | gA2.isNull())) |
        (gC.isNotNull() & (gB.isNull() | gA2.isNull())) |
        (gB.isNotNull() & gA2.isNull()),
        F.lit(False)
    ).otherwise(
        # if not skipping, then strict = no OUT_OF_ORDER anywhere
        (F.col("sequence_breaks") == "") | F.col("sequence_breaks").isNull()
    )
)

# Quick view
display(
    df_OPPM.select(
        "Project ID",
        "sequence_breaks",
        "seq_a2_b_status","seq_b_c_status","seq_c_d_status","seq_d_e_status",
        "ct_a2_to_b_days","ct_b_to_c_days","ct_c_to_d_days","ct_d_to_e_days",
        "Gate A2 Decision Date","Gate B Decision Date","Gate C Decision Date","Gate D Decision Date","Gate E Decision Date"
    ).limit(50)
)



