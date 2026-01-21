from pyspark.sql import functions as F, types as T
import matplotlib.pyplot as plt
import pandas as pd
from dateutil import parser

# ------------------------------------------------------------
# 0) Start from df_analysis and apply your required filters
# ------------------------------------------------------------
df = (
    df_analysis
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

analysis_date = F.current_date()  # replace with extract_date if you have one

# ------------------------------------------------------------
# 1) Map: gate -> (Decision Date col, "Approval Value" col treated as planned date)
# ------------------------------------------------------------
gate_map = [
    ("A2", "Gate A2 Decision Date", "Gate A2 Approval Value"),
    ("B",  "Gate B Decision Date",  "Gate B Approval Value"),
    ("C",  "Gate C Decision Date",  "Gate C Approval Value"),
    ("D",  "Gate D Decision Date",  "Gate D Approval Value"),
    ("E",  "Gate E Decision Date",  "Gate E Approval Value"),
]

# ------------------------------------------------------------
# 2) Date parsing helpers (Spark first, then dateutil fallback)
# ------------------------------------------------------------
def parse_date_spark(colname: str):
    c = F.trim(F.col(colname).cast("string"))
    c = F.when((c == "") | c.isNull(), F.lit(None)).otherwise(c)

    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "yyyy/MM/dd"),
        F.to_date(c, "dd/MM/yyyy"),
        F.to_date(c, "MM/dd/yyyy"),
        F.to_date(c, "dd-MM-yyyy"),
        F.to_date(c, "MM-dd-yyyy"),
        F.to_date(c, "dd-MMM-yyyy"),  # 05-Jan-2024
        F.to_date(c, "dd-MMM-yy"),    # 05-Jan-24 / 05-Jan-90 (Spark may treat 90 as 2090)
        F.to_date(c, "MMM dd, yyyy"), # Jan 05, 2024
        F.to_date(F.to_timestamp(c))  # last resort
    )

@F.udf(returnType=T.DateType())
def parse_date_dateutil(s: str):
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        dt = parser.parse(s, fuzzy=True, dayfirst=True)  # UK-style day first
        return dt.date()
    except Exception:
        return None

def fix_century_209x(parsed_date_col, raw_str_col):
    """
    Fix Spark 'yy' pivot problem: 01-Jan-90 becoming 2090.
    Rule: if parsed year is 2090-2099 AND raw ends with 90-99 -> subtract 100 years.
    """
    raw = F.trim(raw_str_col)
    return F.when(
        parsed_date_col.isNotNull()
        & F.year(parsed_date_col).between(2090, 2099)
        & raw.rlike(r".*[-/]\s*9[0-9]\s*$"),
        F.add_months(parsed_date_col, -1200)  # -100 years
    ).otherwise(parsed_date_col)

# ------------------------------------------------------------
# 3) Standardise ALL Approval Value columns -> Gate <X> Approval Date
# ------------------------------------------------------------
for gate, decision_col, approval_val_col in gate_map:
    raw = F.trim(F.col(approval_val_col).cast("string"))
    spark_parsed = parse_date_spark(approval_val_col)

    parsed = (
        F.when(spark_parsed.isNotNull(), spark_parsed)
         .when((raw.isNotNull()) & (raw != ""), parse_date_dateutil(raw))
         .otherwise(F.lit(None).cast("date"))
    )

    parsed = fix_century_209x(parsed, raw)

    df = df.withColumn(f"Gate {gate} Approval Date", parsed)

# ------------------------------------------------------------
# 4) Timeliness metrics per gate
#    delay_days = Decision - Approval  ( +ve late, -ve early )
#    timeliness_flag logic:
#      - NO_PLAN (planned missing)
#      - if decision exists: EARLY / ON_TIME / LATE
#      - else: OVERDUE if today>planned else UPCOMING
# ------------------------------------------------------------
for gate, decision_col, _ in gate_map:
    approval_date = F.col(f"Gate {gate} Approval Date")
    decision_date  = F.col(decision_col)

    df = df.withColumn(
        f"{gate}_delay_days",
        F.when(approval_date.isNotNull() & decision_date.isNotNull(),
               F.datediff(decision_date, approval_date)
        ).otherwise(F.lit(None).cast("int"))
    )

    df = df.withColumn(
        f"{gate}_timeliness_flag",
        F.when(approval_date.isNull(), F.lit("NO_PLAN"))
         .when(decision_date.isNotNull() & (decision_date < approval_date), F.lit("EARLY"))
         .when(decision_date.isNotNull() & (decision_date == approval_date), F.lit("ON_TIME"))
         .when(decision_date.isNotNull() & (decision_date > approval_date), F.lit("LATE"))
         .when(decision_date.isNull() & (analysis_date > approval_date), F.lit("OVERDUE"))
         .otherwise(F.lit("UPCOMING"))
    )

# ------------------------------------------------------------
# PLOT 1) Planned-date parsing success (counts + %)
# ------------------------------------------------------------
parse_counts_exprs = []
total_rows = df.count()

for gate, _, _ in gate_map:
    parse_counts_exprs.append(
        F.sum(F.when(F.col(f"Gate {gate} Approval Date").isNotNull(), 1).otherwise(0)).alias(f"{gate}_parsed_cnt")
    )

parse_counts = df.select(*parse_counts_exprs).toPandas().iloc[0].to_dict()

parse_pdf = pd.DataFrame({
    "gate": list(parse_counts.keys()),
    "parsed_cnt": list(parse_counts.values()),
})
parse_pdf["gate"] = parse_pdf["gate"].str.replace("_parsed_cnt", "", regex=False)
parse_pdf["parsed_pct"] = (parse_pdf["parsed_cnt"] / total_rows * 100).round(1)

plt.figure(figsize=(8,4))
plt.bar(parse_pdf["gate"], parse_pdf["parsed_cnt"])
plt.title("Planned (Approval) Date Parsing Success — Count")
plt.xlabel("Gate")
plt.ylabel("Parsed planned dates (count)")
for i, (cnt, pct) in enumerate(zip(parse_pdf["parsed_cnt"], parse_pdf["parsed_pct"])):
    plt.text(i, cnt, f"{pct}%", ha="center", va="bottom")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# PLOT 2) Timeliness mix per gate (stacked bar)
# ------------------------------------------------------------
flags = ["EARLY", "ON_TIME", "LATE", "OVERDUE", "UPCOMING", "NO_PLAN"]

timeliness_rows = []
for gate, _, _ in gate_map:
    grp = (
        df.groupBy(F.col(f"{gate}_timeliness_flag").alias("flag"))
          .count()
          .withColumn("gate", F.lit(gate))
    )
    timeliness_rows.append(grp)

timeliness_df = timeliness_rows[0]
for t in timeliness_rows[1:]:
    timeliness_df = timeliness_df.unionByName(t)

timeliness_pdf = (
    timeliness_df.toPandas()
    .pivot_table(index="gate", columns="flag", values="count", fill_value=0)
    .reindex(columns=flags, fill_value=0)
)

plt.figure(figsize=(10,5))
bottom = None
x = timeliness_pdf.index.tolist()
for f in flags:
    vals = timeliness_pdf[f].values
    if bottom is None:
        plt.bar(x, vals, label=f)
        bottom = vals
    else:
        plt.bar(x, vals, bottom=bottom, label=f)
        bottom = bottom + vals

plt.title("Timeliness Mix per Gate (Planned vs Actual / Overdue vs Upcoming)")
plt.xlabel("Gate")
plt.ylabel("Number of projects (rows)")
plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# PLOT 3) Median delay days per gate (only where decision+planned exist)
# ------------------------------------------------------------
delay_stats_rows = []
for gate, decision_col, _ in gate_map:
    delay_stats_rows.append(
        df.filter(F.col(f"{gate}_delay_days").isNotNull())
          .agg(
              F.count("*").alias("n"),
              F.expr(f"percentile_approx({gate}_delay_days, 0.5)").alias("median_delay_days"),
              F.round(F.avg(F.col(f"{gate}_delay_days")), 1).alias("mean_delay_days"),
          )
          .withColumn("gate", F.lit(gate))
    )

delay_stats = delay_stats_rows[0]
for d in delay_stats_rows[1:]:
    delay_stats = delay_stats.unionByName(d)

delay_pdf = delay_stats.select("gate","n","median_delay_days","mean_delay_days").toPandas()
delay_pdf = delay_pdf.sort_values("gate")

plt.figure(figsize=(8,4))
plt.bar(delay_pdf["gate"], delay_pdf["median_delay_days"])
plt.title("Median Delay Days per Gate (Decision − Planned) — Completed Gates Only")
plt.xlabel("Gate")
plt.ylabel("Median delay (days)  (+ late, − early)")
for i, (v, n) in enumerate(zip(delay_pdf["median_delay_days"], delay_pdf["n"])):
    plt.text(i, v, f"n={n}", ha="center", va="bottom")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# PLOT 4) Overdue backlog per gate (decision missing but planned date passed)
# ------------------------------------------------------------
overdue_rows = []
for gate, decision_col, _ in gate_map:
    overdue_rows.append(
        df.filter(
            F.col(decision_col).isNull()
            & F.col(f"Gate {gate} Approval Date").isNotNull()
            & (analysis_date > F.col(f"Gate {gate} Approval Date"))
        ).agg(F.count("*").alias("overdue_cnt"))
         .withColumn("gate", F.lit(gate))
    )

overdue_df = overdue_rows[0]
for o in overdue_rows[1:]:
    overdue_df = overdue_df.unionByName(o)

overdue_pdf = overdue_df.toPandas().sort_values("gate")

plt.figure(figsize=(8,4))
plt.bar(overdue_pdf["gate"], overdue_pdf["overdue_cnt"])
plt.title("Overdue Backlog per Gate (No Decision Date, Planned Date Passed)")
plt.xlabel("Gate")
plt.ylabel("Overdue count")
plt.tight_layout()
plt.show()

# Optional: keep this enriched DF for later joins/analysis
df_timeliness = df
