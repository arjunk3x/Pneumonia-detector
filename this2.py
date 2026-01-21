from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# =========================================================
# Assumes you already built these columns:
# - benchmark_eligible, is_sequence_full, project_status_gate_based
# - Gate {X} Approval Date (planned)  [DATE]
# - Gate {X} Decision Date (actual)   [DATE]
# - {X}_timeliness_flag  (EARLY/ON_TIME/LATE/OVERDUE/UPCOMING/NO_PLAN)
# - {X}_delay_days_actual_minus_planned (int, actual - planned)
# And df_analysed exists.
# =========================================================

# ---------- 0) Base dataset ----------
df_analysis = (
    df_analysed
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
    .withColumn(
        "pm_clean",
        F.when(F.trim(F.col("pm").cast("string")).isNull() | (F.trim(F.col("pm").cast("string")) == ""), F.lit("Unknown"))
         .otherwise(F.trim(F.col("pm").cast("string")))
    )
)

gates = ["A2", "B", "C", "D", "E"]

# Helper: build a long-format DF with (gate, timeliness_flag, delay_days, pm_clean, status)
stack_expr = "stack(5, " + ", ".join([
    f"'{g}', `{g}_timeliness_flag`, `{g}_delay_days_actual_minus_planned`"
    for g in gates
]) + ") as (gate, timeliness_flag, delay_days)"

df_long = (
    df_analysis
    .selectExpr("`Project ID`", "project_status_gate_based", "pm_clean", stack_expr)
)

# =========================================================
# 1) Stacked bar: Timeliness breakdown by gate
# =========================================================
timeliness_counts = (
    df_long
    .groupBy("gate", "timeliness_flag")
    .count()
)

# Convert to pandas pivot for stacked bar
tc_pd = timeliness_counts.toPandas()
pivot = (tc_pd.pivot(index="gate", columns="timeliness_flag", values="count")
              .fillna(0)
              .reindex(gates))

# Make sure a consistent column order (only keep those that exist)
flag_order = ["EARLY","ON_TIME","LATE","OVERDUE","UPCOMING","NO_PLAN"]
flag_cols = [c for c in flag_order if c in pivot.columns]
pivot = pivot[flag_cols]

ax = pivot.plot(kind="bar", stacked=True, figsize=(10, 4))
ax.set_xlabel("Gate")
ax.set_ylabel("Number of projects")
ax.set_title("Timeliness by Gate (Planned vs Actual / Due Status)")
plt.tight_layout()
plt.show()

display(timeliness_counts.orderBy("gate", "timeliness_flag"))

# =========================================================
# 2) Late-days distribution per gate (bar chart of percentiles)
#    (Only where timeliness_flag == LATE and delay_days > 0)
# =========================================================
late_dist = (
    df_long
    .filter((F.col("timeliness_flag") == "LATE") & (F.col("delay_days").isNotNull()) & (F.col("delay_days") > 0))
    .groupBy("gate")
    .agg(
        F.count("*").alias("n_late"),
        F.expr("percentile_approx(delay_days, 0.5)").alias("P50_late_days"),
        F.expr("percentile_approx(delay_days, 0.8)").alias("P80_late_days"),
        F.expr("percentile_approx(delay_days, 0.9)").alias("P90_late_days"),
    )
    .orderBy("gate")
)

ld_pd = late_dist.toPandas().set_index("gate").reindex(gates).fillna(0)

x = np.arange(len(gates))
w = 0.25

plt.figure(figsize=(10, 4))
plt.bar(x - w, ld_pd["P50_late_days"], width=w, label="P50 late days")
plt.bar(x,      ld_pd["P80_late_days"], width=w, label="P80 late days")
plt.bar(x + w,  ld_pd["P90_late_days"], width=w, label="P90 late days")
plt.xticks(x, gates)
plt.xlabel("Gate")
plt.ylabel("Days late (Actual - Planned)")
plt.title("Late Duration Distribution by Gate (Percentiles)")
for i, n in enumerate(ld_pd["n_late"].values):
    plt.text(i, max(ld_pd["P90_late_days"].values[i], 0), f" n={int(n)}", ha="center", va="bottom", fontsize=9)
plt.legend()
plt.tight_layout()
plt.show()

display(late_dist)

# =========================================================
# 3) Overdue pipeline view (In Progress + Not Started): count + avg overdue days by gate
#    Overdue means: actual missing AND today > planned (approval) date
# =========================================================
# Build a long DF that includes planned/actual dates too (for overdue days calculation)
stack_expr_dates = "stack(5, " + ", ".join([
    f"'{g}', `Gate {g} Approval Date`, `Gate {g} Decision Date`"
    for g in gates
]) + ") as (gate, planned_date, actual_date)"

df_long_dates = (
    df_analysis
    .selectExpr("`Project ID`", "project_status_gate_based", stack_expr_dates)
)

df_overdue = (
    df_long_dates
    .filter(F.col("project_status_gate_based").isin("In Progress", "Not Started"))
    .filter(F.col("planned_date").isNotNull() & F.col("actual_date").isNull())
    .withColumn("overdue_days", F.datediff(F.current_date(), F.col("planned_date")))
    .filter(F.col("overdue_days") > 0)
)

overdue_summary = (
    df_overdue
    .groupBy("gate")
    .agg(
        F.count("*").alias("overdue_count"),
        F.round(F.avg("overdue_days"), 1).alias("avg_overdue_days")
    )
    .orderBy("gate")
)

os_pd = overdue_summary.toPandas().set_index("gate").reindex(gates).fillna(0)

# Bar chart: overdue count
plt.figure(figsize=(10, 3.6))
plt.bar(os_pd.index, os_pd["overdue_count"])
plt.xlabel("Gate")
plt.ylabel("Number of overdue items")
plt.title("Overdue Planned Gates (In Progress + Not Started)")
plt.tight_layout()
plt.show()

# Bar chart: average overdue days
plt.figure(figsize=(10, 3.6))
plt.bar(os_pd.index, os_pd["avg_overdue_days"])
plt.xlabel("Gate")
plt.ylabel("Average overdue days")
plt.title("Average Overdue Days by Gate (In Progress + Not Started)")
plt.tight_layout()
plt.show()

display(overdue_summary)

# =========================================================
# 4) Manager accountability: Top 10 PMs with highest median lateness per gate (bar)
#    (Only LATE with delay_days > 0)
#    Uses n>=MIN_N to avoid tiny-sample rankings
# =========================================================
MIN_N = 20
TOP_K = 10

def pm_late_rank_for_gate(gate):
    return (
        df_long
        .filter((F.col("gate") == gate) &
                (F.col("timeliness_flag") == "LATE") &
                (F.col("delay_days").isNotNull()) & (F.col("delay_days") > 0) &
                (F.col("pm_clean") != "Unknown"))
        .groupBy("pm_clean")
        .agg(
            F.count("*").alias("n_late_projects"),
            F.expr("percentile_approx(delay_days, 0.5)").alias("median_late_days"),
            F.round(F.avg("delay_days"), 1).alias("mean_late_days")
        )
        .filter(F.col("n_late_projects") >= MIN_N)
        .orderBy(F.desc("median_late_days"))
        .limit(TOP_K)
        .withColumn("gate", F.lit(gate))
    )

def plot_pm_bar(df_top, title):
    pdf = df_top.select("pm_clean", "median_late_days", "n_late_projects").toPandas()
    if pdf.empty:
        print(f"{title}: No PMs meet n_late_projects >= {MIN_N}. Reduce MIN_N.")
        return
    pdf = pdf.sort_values("median_late_days", ascending=True)
    plt.figure(figsize=(10, 4))
    plt.barh(pdf["pm_clean"], pdf["median_late_days"])
    plt.xlabel("Median late days (Actual - Planned)")
    plt.ylabel("PM")
    plt.title(title)
    for i, (val, n) in enumerate(zip(pdf["median_late_days"], pdf["n_late_projects"])):
        plt.text(val, i, f"  n={int(n)}", va="center")
    plt.tight_layout()
    plt.show()

for g in gates:
    df_top_pm = pm_late_rank_for_gate(g)
    display(df_top_pm)
    plot_pm_bar(df_top_pm, f"Top {TOP_K} PMs by Median Lateness — Gate {g} (n≥{MIN_N})")


