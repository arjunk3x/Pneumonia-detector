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
