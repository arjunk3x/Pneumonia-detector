from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# -----------------------------
# 0) Start from your analysis DF (already has benchmark_eligible + is_sequence_full + timeliness columns)
# -----------------------------
df_timing = (
    df_analysed
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

gates = ["A2", "B", "C", "D", "E"]

# -----------------------------
# 1) FIRST MISSED GATE (Late or Overdue) — works for Completed/In-Progress/Not Started
# -----------------------------
# "Missed" = LATE or OVERDUE (you can add more statuses if you want)
missed_flags = ["LATE", "OVERDUE"]

missed_arr = F.array(*[
    F.when(F.col(f"{g}_timeliness_flag").isin(missed_flags), F.lit(g)).otherwise(F.lit(None))
    for g in gates
])

df_first_missed = (
    df_timing
    .withColumn("missed_gates_arr", F.expr("filter(missed_gates_arr, x -> x is not null)"))
    .withColumn("first_missed_gate",
                F.when(F.size(F.col("missed_gates_arr")) > 0, F.element_at(F.col("missed_gates_arr"), 1))
                 .otherwise(F.lit("NO_MISS")))
)

first_missed_counts = (
    df_first_missed
    .groupBy("first_missed_gate")
    .count()
    .orderBy(F.desc("count"))
)

# Plot 1: Bar chart — where do misses start?
pdf1 = first_missed_counts.toPandas()
plt.figure(figsize=(10,4))
plt.bar(pdf1["first_missed_gate"], pdf1["count"])
plt.title("Where do misses start? (First gate that is LATE or OVERDUE)")
plt.xlabel("First missed gate")
plt.ylabel("Number of projects")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

display(first_missed_counts)


# -----------------------------
# 2) DOMINO vs SECOND HIT — Completed projects only (needs actual decision dates for all gates)
# -----------------------------
decision_cols = [f"Gate {g} Decision Date" for g in gates]
df_completed = df_timing
for c in decision_cols:
    df_completed = df_completed.filter(F.col(c).isNotNull())

# Build an ordered array of gate structs {idx, gate, delay}
gate_structs = F.array(*[
    F.struct(
        F.lit(i).alias("idx"),
        F.lit(g).alias("gate"),
        F.col(f"{g}_delay_days_actual_minus_planned").cast("double").alias("delay")
    )
    for i, g in enumerate(gates)
])

# Gate-to-gate increments (how much *more* delay was added at the next gate)
# inc_struct idx_next refers to the "next gate" (B, C, D, E)
inc_structs = F.array(*[
    F.struct(
        F.lit(i+1).alias("idx_next"),
        F.lit(gates[i+1]).alias("gate_next"),
        (F.col(f"{gates[i+1]}_delay_days_actual_minus_planned").cast("double")
         - F.col(f"{gates[i]}_delay_days_actual_minus_planned").cast("double")).alias("inc")
    )
    for i in range(len(gates)-1)
])

extra_slip_days = 0.0  # set to 0 = any additional slip counts as a "second hit" (try 7 or 14 if you want stricter)

df_patterns = (
    df_completed
    .withColumn("gate_structs", gate_structs)
    .withColumn("late_gates", F.expr("filter(gate_structs, x -> x.delay is not null and x.delay > 0)"))
    .withColumn("first_late_struct",
                F.when(F.size(F.col("late_gates")) > 0, F.element_at(F.col("late_gates"), 1)))
    .withColumn("first_late_gate", F.col("first_late_struct.gate"))
    .withColumn("first_late_idx", F.col("first_late_struct.idx"))
    .withColumn("inc_structs", inc_structs)
    .withColumn(
        "second_hit_structs",
        F.expr(f"filter(inc_structs, x -> x.inc is not null and x.inc > {extra_slip_days})")
    )
    # Keep only second hits that happen AFTER the first late gate
    .withColumn(
        "second_hit_after_first",
        F.expr("filter(second_hit_structs, x -> first_late_idx is not null and x.idx_next > first_late_idx)")
    )
    .withColumn(
        "second_hit_gate",
        F.when(F.size(F.col("second_hit_after_first")) > 0, F.element_at(F.col("second_hit_after_first"), 1).gate_next)
         .otherwise(F.lit(None))
    )
    .withColumn(
        "delay_pattern",
        F.when(F.col("first_late_gate").isNull(), F.lit("NEVER_LATE"))
         .when(F.col("second_hit_gate").isNull(), F.lit("DOMINO (slip carried forward)"))
         .otherwise(F.concat(F.lit("SECOND_HIT at "), F.col("second_hit_gate")))
    )
)

# Summary counts
pattern_counts = (
    df_patterns
    .groupBy("delay_pattern")
    .count()
    .orderBy(F.desc("count"))
)

# Plot 2: Bar chart — domino vs second hit
pdf2 = pattern_counts.toPandas()
plt.figure(figsize=(10,4))
plt.bar(pdf2["delay_pattern"], pdf2["count"])
plt.title("Domino vs Second Hit (Completed projects only)")
plt.xlabel("Pattern")
plt.ylabel("Number of projects")
plt.xticks(rotation=25, ha="right")
plt.tight_layout()
plt.show()

display(pattern_counts)

# Plot 3: Stacked bar — for each first_late_gate, how many are domino vs second hit?
first_late_split = (
    df_patterns
    .withColumn("first_late_gate", F.coalesce(F.col("first_late_gate"), F.lit("NEVER_LATE")))
    .withColumn(
        "pattern_group",
        F.when(F.col("delay_pattern").startswith("DOMINO"), F.lit("DOMINO"))
         .when(F.col("delay_pattern").startswith("SECOND_HIT"), F.lit("SECOND_HIT"))
         .otherwise(F.lit("NEVER_LATE"))
    )
    .groupBy("first_late_gate", "pattern_group")
    .count()
)

pdf3 = first_late_split.toPandas().pivot(index="first_late_gate", columns="pattern_group", values="count").fillna(0)
pdf3 = pdf3.reindex(["A2","B","C","D","E","NEVER_LATE"], axis=0).dropna(how="all")  # keep gate order if present

plt.figure(figsize=(10,4))
bottom = None
for col in pdf3.columns:
    if bottom is None:
        plt.bar(pdf3.index, pdf3[col], label=col)
        bottom = pdf3[col].values
    else:
        plt.bar(pdf3.index, pdf3[col], bottom=bottom, label=col)
        bottom = bottom + pdf3[col].values

plt.title("Where does delay start, and is it a Domino vs Second Hit?")
plt.xlabel("First late gate")
plt.ylabel("Number of completed projects")
plt.legend()
plt.tight_layout()
plt.show()

display(first_late_split)










from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import numpy as np

# -----------------------------
# 0) Base DF for visuals (your required filters)
# -----------------------------
df_viz = (
    df_analysed
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

# Optional but recommended: clean group columns so nulls don't collapse everything
df_viz = (
    df_viz
    .withColumn("pm_clean", F.coalesce(F.trim(F.col("pm").cast("string")), F.lit("Unknown")))
    .withColumn("region_clean", F.coalesce(F.trim(F.col("region").cast("string")), F.lit("Unknown")))
)

# Gates + timeliness flag columns you already created earlier
gates = ["A2", "B", "C", "D", "E"]
flag_cols = [f"{g}_timeliness_flag" for g in gates]

# -----------------------------
# 1) Convert wide -> long (one row per project x gate)
# -----------------------------
# This makes it easy to count statuses by manager/region
stack_expr = "stack(5, " + ", ".join([f"'{g}', {g}_timeliness_flag" for g in gates]) + ") as (gate, status)"

df_long = (
    df_viz
    .select(
        F.col("Project ID").alias("project_id"),
        "pm_clean",
        "region_clean",
        F.expr(stack_expr)
    )
    .withColumn("status", F.coalesce(F.col("status"), F.lit("UNKNOWN")))
)

# Common status order (adjust to match your exact labels if needed)
status_order = ["EARLY", "ON TIME", "LATE", "OVERDUE", "UPCOMING", "NO_PLAN", "UNKNOWN"]


# ============================================================
# 1) ON-TIME SCORECARD (STACKED BARS) — Manager or Region
# ============================================================

def plot_scorecard_stacked(df_long, group_col="pm_clean", top_n=15, as_percent=False):
    """
    Stacked bar chart: for each group (manager/region), how many gate records are
    EARLY / ON TIME / LATE / OVERDUE / UPCOMING / NO_PLAN / UNKNOWN.
    """
    # Count statuses by group
    counts = (
        df_long
        .groupBy(group_col, "status")
        .count()
    )

    # Pivot statuses into columns
    pivoted = (
        counts
        .groupBy(group_col)
        .pivot("status")
        .sum("count")
        .na.fill(0)
    )

    # Ensure all status columns exist
    for s in status_order:
        if s not in pivoted.columns:
            pivoted = pivoted.withColumn(s, F.lit(0))

    # Add total gate-records per group
    pivoted = pivoted.withColumn("total", sum(F.col(s) for s in status_order))

    # Pick top N groups by total volume (helps readability)
    top_groups = pivoted.orderBy(F.desc("total")).limit(top_n)

    pdf = top_groups.toPandas().set_index(group_col)

    # Optional: convert to %
    if as_percent:
        denom = pdf["total"].replace(0, np.nan)
        for s in status_order:
            pdf[s] = (pdf[s] / denom) * 100
        ylabel = "% of gate records"
        title_suffix = "(Percent split)"
    else:
        ylabel = "Count of gate records"
        title_suffix = "(Counts)"

    # Plot stacked bars
    plt.figure(figsize=(12, 5))
    bottom = np.zeros(len(pdf))
    x = np.arange(len(pdf))

    for s in status_order:
        vals = pdf[s].values
        plt.bar(x, vals, bottom=bottom, label=s)
        bottom += vals

    plt.xticks(x, pdf.index, rotation=45, ha="right")
    plt.ylabel(ylabel)
    plt.title(f"On-time scorecard by {group_col} {title_suffix} — Top {top_n}")
    plt.legend(ncol=3, bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.show()

    display(top_groups)


# Run scorecard for managers
plot_scorecard_stacked(df_long, group_col="pm_clean", top_n=15, as_percent=False)

# Run scorecard for regions
plot_scorecard_stacked(df_long, group_col="region_clean", top_n=15, as_percent=False)


# ============================================================
# 2) OVERDUE BACKLOG (BAR CHART) — Gate × Manager/Region
# ============================================================

def plot_overdue_backlog(df_long, group_col="pm_clean", top_n=15):
    """
    Grouped bar chart: For top N groups (by total overdue),
    show overdue counts per gate (A2, B, C, D, E).
    """
    overdue = (
        df_long
        .filter(F.col("status") == "OVERDUE")
        .groupBy(group_col, "gate")
        .count()
    )

    # Total overdue per group to pick top N
    overdue_totals = (
        overdue
        .groupBy(group_col)
        .agg(F.sum("count").alias("overdue_total"))
        .orderBy(F.desc("overdue_total"))
        .limit(top_n)
    )

    overdue_top = overdue.join(overdue_totals.select(group_col), on=group_col, how="inner")

    # Pivot gates into columns
    pivoted = (
        overdue_top
        .groupBy(group_col)
        .pivot("gate", gates)
        .sum("count")
        .na.fill(0)
    )

    pdf = pivoted.toPandas().set_index(group_col)

    # Grouped bar chart (one bar per gate per group)
    plt.figure(figsize=(12, 5))
    x = np.arange(len(pdf))
    width = 0.15

    for i, g in enumerate(gates):
        vals = pdf[g].values if g in pdf.columns else np.zeros(len(pdf))
        plt.bar(x + (i - 2) * width, vals, width=width, label=g)

    plt.xticks(x, pdf.index, rotation=45, ha="right")
    plt.ylabel("Overdue count")
    plt.title(f"Overdue backlog by gate × {group_col} — Top {top_n} by overdue volume")
    plt.legend(title="Gate", ncol=5, bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.show()

    display(overdue_totals)


# Run overdue backlog for managers
plot_overdue_backlog(df_long, group_col="pm_clean", top_n=15)

# Run overdue backlog for regions
plot_overdue_backlog(df_long, group_col="region_clean", top_n=15)
