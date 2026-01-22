from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import numpy as np

# -----------------------------
# 0) Start from your filtered dataset (as you asked)
# -----------------------------
df_analysis = (
    df_analysed
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

# OPTIONAL: if you only want completed projects for this root-cause view, uncomment:
# df_analysis = df_analysis.filter(F.col("project_status_gate_based") == "Completed")

# -----------------------------
# 1) Build an ordered gate/status array from your timeliness flags
# -----------------------------
gate_order = ["A2", "B", "C", "D", "E"]
flag_cols = {g: f"{g}_timeliness_flag" for g in gate_order}

def status_col(cname):
    # Normalize status to uppercase + fill nulls
    return F.upper(F.coalesce(F.col(cname).cast("string"), F.lit("UNKNOWN")))

gate_status_arr = F.array(*[
    F.struct(
        F.lit(g).alias("gate"),
        status_col(flag_cols[g]).alias("status")
    )
    for g in gate_order
])

df_rc = (
    df_analysis
    .select(
        F.col("Project ID").alias("project_id"),
        gate_status_arr.alias("gate_status_arr")
    )
)

# -----------------------------
# 2) Find delayed gates (LATE or OVERDUE), first delay, second delay
# -----------------------------
df_rc = (
    df_rc
    .withColumn("delayed_arr", F.expr("filter(gate_status_arr, x -> x.status IN ('LATE','OVERDUE'))"))
    .withColumn("delay_count", F.size("delayed_arr"))
    .withColumn("first_delay_gate",
                F.when(F.col("delay_count") > 0, F.element_at("delayed_arr", 1).getField("gate"))
                 .otherwise(F.lit("NO_DELAY")))
    .withColumn("first_delay_status",
                F.when(F.col("delay_count") > 0, F.element_at("delayed_arr", 1).getField("status"))
                 .otherwise(F.lit(None)))
    .withColumn("second_delay_gate",
                F.when(F.col("delay_count") > 1, F.element_at("delayed_arr", 2).getField("gate"))
                 .otherwise(F.lit(None)))
    .withColumn("delayed_gates_path",
                F.concat_ws("→", F.transform("delayed_arr", lambda x: x["gate"])))
)

# -----------------------------
# 3) Domino vs Second-hit logic
#    Domino = delayed gates are consecutive in the process (no "gap" gate in between)
#    Second hit = delays happen, then things are OK, then delay again later (gap exists)
# -----------------------------
gate_idx_map = F.create_map(*sum([[F.lit(g), F.lit(i+1)] for i, g in enumerate(gate_order)], []))

df_rc = (
    df_rc
    .withColumn(
        "delayed_idx_arr",
        F.transform("delayed_arr", lambda x: F.element_at(gate_idx_map, x["gate"]).cast("int"))
    )
    .withColumn("min_delay_idx", F.array_min("delayed_idx_arr"))
    .withColumn("max_delay_idx", F.array_max("delayed_idx_arr"))
    .withColumn(
        "is_consecutive",
        F.when(
            F.col("delay_count") > 1,
            (F.col("max_delay_idx") - F.col("min_delay_idx") + F.lit(1)) == F.col("delay_count")
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "delay_pattern",
        F.when(F.col("delay_count") == 0, F.lit("No late/overdue"))
         .when(F.col("delay_count") == 1, F.lit("Single hit"))
         .when(F.col("is_consecutive") == True, F.lit("Domino (continuous)"))
         .otherwise(F.lit("Second hit (gap)"))
    )
)

# Quick inspect (optional)
display(
    df_rc.select("project_id", "first_delay_gate", "first_delay_status", "second_delay_gate",
                 "delay_pattern", "delayed_gates_path")
         .orderBy(F.desc("delay_count"))
         .limit(30)
)

# -----------------------------
# 4) VISUAL 1: "Where do delays start?" (first gate that goes late/overdue)
# -----------------------------
first_gate_counts = (
    df_rc.groupBy("first_delay_gate")
         .count()
         .orderBy(F.desc("count"))
)

pdf1 = first_gate_counts.toPandas()

plt.figure(figsize=(10,4))
plt.bar(pdf1["first_delay_gate"], pdf1["count"])
plt.title("Where do delays start? (First gate marked LATE/OVERDUE)")
plt.xlabel("First gate that goes late/overdue")
plt.ylabel("Number of projects")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

display(first_gate_counts)

# -----------------------------
# 5) VISUAL 2: Domino vs Second-hit (stacked by first delay gate)
# -----------------------------
pattern_counts = (
    df_rc.groupBy("first_delay_gate", "delay_pattern")
         .count()
)

pivoted = (
    pattern_counts
    .groupBy("first_delay_gate")
    .pivot("delay_pattern", ["No late/overdue", "Single hit", "Domino (continuous)", "Second hit (gap)"])
    .sum("count")
    .na.fill(0)
)

pdf2 = pivoted.toPandas().set_index("first_delay_gate")

# Sort by total volume
pdf2["TOTAL"] = pdf2.sum(axis=1)
pdf2 = pdf2.sort_values("TOTAL", ascending=False).drop(columns=["TOTAL"])

plt.figure(figsize=(12,5))
bottom = np.zeros(len(pdf2))
x = np.arange(len(pdf2))

for col in pdf2.columns:
    vals = pdf2[col].values
    plt.bar(x, vals, bottom=bottom, label=col)
    bottom += vals

plt.xticks(x, pdf2.index, rotation=0)
plt.title("Domino vs Second-hit (grouped by first delay gate)")
plt.xlabel("First delay gate")
plt.ylabel("Number of projects")
plt.legend(ncol=2, bbox_to_anchor=(1.02, 1), loc="upper left")
plt.tight_layout()
plt.show()

display(pivoted)
