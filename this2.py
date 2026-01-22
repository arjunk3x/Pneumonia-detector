# =========================
# Rate-based + time-based metrics for OPPM vs P6 (size-agnostic)
# Uses: df (your main dataframe) with "category" (OPPM/P6) + gate decision dates
# Assumes you already have: benchmark_eligible, is_sequence_full
# =========================

from pyspark.sql import functions as F

# -------------------------
# 0) Column name helpers
# -------------------------
def pick_existing_col(df, *candidates):
    """Return the first column name that exists in df.columns."""
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"None of these columns exist: {candidates}")

project_id_col = pick_existing_col(df, "Project ID", "ProjectID", "project_id")
category_col   = pick_existing_col(df, "category", "Category")

# Clean category -> force to "OPPM"/"P6" for stable grouping
df = df.withColumn(
    "category_clean",
    F.when(F.lower(F.col(category_col)).like("%p6%"),   F.lit("P6"))
     .when(F.lower(F.col(category_col)).like("%oppm%"), F.lit("OPPM"))
     .otherwise(F.col(category_col))
)

# Your strict filters to avoid noise/bad rows
df_analysis = (
    df.filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
)

analysis_date = F.current_date()  # or replace with F.to_date(F.lit("2026-01-22"))

# -------------------------
# 1) Gate plan/decision mapping (planned = "Approval Value" -> parsed date)
#    If you already created "Gate X Approval Date" columns, we will use them.
# -------------------------
gate_map = [
    # gate, decision date column, preferred planned date col, fallback raw planned col in your dataset
    ("A2", "Gate A2 Decision Date", "Gate A2 Approval Date", "Gate A2 Approval Value"),
    ("B",  "Gate B Decision Date",  "Gate B Approval Date",  "Gate B Approval Value Sanction"),
    ("C",  "Gate C Decision Date",  "Gate C Approval Date",  "Gate C Approval Value FSA ACL"),
    ("D",  "Gate D Decision Date",  "Gate D Approval Date",  "Gate D Approval Value"),
    ("E",  "Gate E Decision Date",  "Gate E Approval Date",  "Gate E Approval Value"),
]

# If any "Gate X Approval Date" is missing, parse from the raw Approval Value into that date column
def parse_plan_date(df_in, gate, preferred_plan_col, fallback_raw_col):
    if preferred_plan_col in df_in.columns:
        return df_in
    if fallback_raw_col not in df_in.columns:
        raise ValueError(f"Missing both planned date columns for Gate {gate}: {preferred_plan_col}, {fallback_raw_col}")

    raw = F.trim(F.col(fallback_raw_col).cast("string"))
    raw = F.when((raw == "") | raw.isNull(), F.lit(None)).otherwise(raw)

    # Try common formats (extend if needed)
    plan = F.coalesce(
        F.to_date(raw, "yyyy-MM-dd"),
        F.to_date(raw, "yyyy/MM/dd"),
        F.to_date(raw, "dd/MM/yyyy"),
        F.to_date(raw, "MM/dd/yyyy"),
        F.to_date(raw, "dd-MM-yyyy"),
        F.to_date(raw, "MM-dd-yyyy"),
        F.to_date(raw, "dd-MMM-yyyy"),
        F.to_date(raw, "dd-MMM-yy"),
        F.to_date(F.to_timestamp(raw))
    )

    return df_in.withColumn(preferred_plan_col, plan)

for gate, dcol, pcol, rawp in gate_map:
    df_analysis = parse_plan_date(df_analysis, gate, pcol, rawp)

# -------------------------
# 2) Build per-gate timeliness flag + delay days (Actual - Planned)
#    EARLY/ON_TIME/LATE: plan+decision present
#    OVERDUE/UPCOMING: plan present, decision missing
#    NO_PLAN: plan missing
# -------------------------
for gate, dcol, pcol, _ in gate_map:
    decision = F.col(dcol)
    plan     = F.col(pcol)

    df_analysis = (
        df_analysis
        .withColumn(
            f"{gate}_delay_days_actual_minus_planned",
            F.when(plan.isNotNull() & decision.isNotNull(), F.datediff(decision, plan)).otherwise(F.lit(None).cast("int"))
        )
        .withColumn(
            f"{gate}_timeliness_flag",
            F.when(plan.isNull(), F.lit("NO_PLAN"))
             .when(decision.isNotNull() & (decision < plan),  F.lit("EARLY"))
             .when(decision.isNotNull() & (decision == plan), F.lit("ON_TIME"))
             .when(decision.isNotNull() & (decision > plan),  F.lit("LATE"))
             .when(decision.isNull() & (analysis_date > plan), F.lit("OVERDUE"))
             .otherwise(F.lit("UPCOMING"))
        )
    )

# -------------------------
# 3) (1) Percentages/rates, not counts
#    a) Scorecard split: % of total projects by status (per gate x group)
#    b) % Late out of "decided + planned" only (fair execution metric)
# -------------------------
# Total projects per group (denominator for % of total)
df_totals = df_analysis.groupBy("category_clean").agg(F.countDistinct(project_id_col).alias("total_projects"))

# Long-form timeliness records: (category, gate, flag)
timeliness_long = None
for gate, _, _, _ in gate_map:
    tmp = df_analysis.select(
        "category_clean",
        F.lit(gate).alias("gate"),
        F.col(f"{gate}_timeliness_flag").alias("flag"),
    )
    timeliness_long = tmp if timeliness_long is None else timeliness_long.unionByName(tmp)

# a) Scorecard: % of total projects by timeliness state
gate_scorecard = (
    timeliness_long
    .groupBy("category_clean", "gate", "flag")
    .agg(F.count("*").alias("n"))
    .join(df_totals, on="category_clean", how="left")
    .withColumn("pct_of_total", F.round(F.col("n") * 100.0 / F.col("total_projects"), 2))
    .orderBy("gate", "category_clean", "flag")
)

display(gate_scorecard)

# b) % Late among decided+planned only (EARLY/ON_TIME/LATE denom)
late_rate = (
    timeliness_long
    .filter(F.col("flag").isin(["EARLY", "ON_TIME", "LATE"]))
    .groupBy("category_clean", "gate")
    .agg(
        F.count("*").alias("n_decided_planned"),
        F.sum(F.when(F.col("flag") == "LATE", 1).otherwise(0)).alias("n_late")
    )
    .withColumn("pct_late", F.round(F.col("n_late") * 100.0 / F.col("n_decided_planned"), 2))
    .orderBy("gate", "category_clean")
)

display(late_rate)

# -------------------------
# 4) (2) Cycle-time percentiles per transition (P50/P80) – not averages
# -------------------------
transitions = [
    ("A2→B", "seq_a2_b_status", "ct_a2_to_b_days"),
    ("B→C",  "seq_b_c_status",  "ct_b_to_c_days"),
    ("C→D",  "seq_c_d_status",  "ct_c_to_d_days"),
    ("D→E",  "seq_d_e_status",  "ct_d_to_e_days"),
]

# Long-form transitions (category, transition, cycle_days)
cycle_long = None
for tname, status_col, ct_col in transitions:
    tmp = (
        df_analysis
        .filter(F.col(status_col) == "OK")
        .select(
            "category_clean",
            F.lit(tname).alias("transition"),
            F.col(ct_col).cast("double").alias("cycle_days")
        )
        .filter(F.col("cycle_days").isNotNull() & (F.col("cycle_days") >= 0))
    )
    cycle_long = tmp if cycle_long is None else cycle_long.unionByName(tmp)

cycle_stats = (
    cycle_long
    .groupBy("category_clean", "transition")
    .agg(
        F.count("*").alias("n"),
        F.expr("percentile_approx(cycle_days, 0.5)").alias("p50_days"),
        F.expr("percentile_approx(cycle_days, 0.8)").alias("p80_days"),
        F.round(F.avg("cycle_days"), 1).alias("mean_days")
    )
    .orderBy("transition", "category_clean")
)

display(cycle_stats)

# -------------------------
# 5) (3) Risk score: % of P6 worse than OPPM P80 for each transition
# -------------------------
oppm_p80 = (
    cycle_stats
    .filter(F.col("category_clean") == "OPPM")
    .select("transition", F.col("p80_days").alias("oppm_p80_days"))
)

p6_risk = (
    cycle_long
    .filter(F.col("category_clean") == "P6")
    .join(oppm_p80, on="transition", how="inner")
    .withColumn("is_worse_than_oppm_p80", (F.col("cycle_days") > F.col("oppm_p80_days")).cast("int"))
    .groupBy("transition")
    .agg(
        F.count("*").alias("p6_n"),
        F.round(F.avg("is_worse_than_oppm_p80") * 100.0, 2).alias("p6_pct_worse_than_oppm_p80"),
        F.first("oppm_p80_days").alias("oppm_p80_days")
    )
    .orderBy("transition")
)

display(p6_risk)

# -------------------------
# 6) (4) Overdue burden: total overdue-days, avg overdue-days, overdue-days per 100 projects
# -------------------------
overdue_long = None
for gate, dcol, pcol, _ in gate_map:
    decision = F.col(dcol)
    plan     = F.col(pcol)

    tmp = (
        df_analysis
        .select(
            "category_clean",
            F.lit(gate).alias("gate"),
            F.when(decision.isNull() & plan.isNotNull() & (analysis_date > plan),
                   F.datediff(analysis_date, plan)
            ).otherwise(F.lit(None).cast("int")).alias("overdue_days")
        )
    )

    overdue_long = tmp if overdue_long is None else overdue_long.unionByName(tmp)

overdue_burden = (
    overdue_long
    .withColumn("is_overdue", F.col("overdue_days").isNotNull().cast("int"))
    .groupBy("category_clean", "gate")
    .agg(
        F.sum("is_overdue").alias("overdue_projects"),
        F.sum(F.coalesce(F.col("overdue_days"), F.lit(0))).alias("total_overdue_days"),
        F.round(F.avg("overdue_days"), 1).alias("avg_overdue_days")  # avg across overdue-only rows (nulls ignored)
    )
    .join(df_totals, on="category_clean", how="left")
    .withColumn("overdue_days_per_100_projects", F.round(F.col("total_overdue_days") * 100.0 / F.col("total_projects"), 2))
    .withColumn("pct_projects_overdue", F.round(F.col("overdue_projects") * 100.0 / F.col("total_projects"), 2))
    .orderBy("gate", "category_clean")
)

display(overdue_burden)

# ============================================================
# BAR CHARTS (matplotlib) — one per metric (no seaborn)
# ============================================================
import matplotlib.pyplot as plt

def grouped_bar_from_spark(sdf, index_col, series_col, value_col, title, xlabel, ylabel):
    pdf = sdf.select(index_col, series_col, value_col).toPandas()
    pivot = pdf.pivot(index=index_col, columns=series_col, values=value_col).fillna(0)
    pivot.plot(kind="bar", figsize=(10,4))
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()

# A) % Late (among decided+planned) per gate, split by group
grouped_bar_from_spark(
    late_rate,
    index_col="gate",
    series_col="category_clean",
    value_col="pct_late",
    title="% Late (out of Planned+Decided) by Gate — OPPM vs P6",
    xlabel="Gate",
    ylabel="% Late"
)

# B) Median (P50) cycle days per transition, split by group
grouped_bar_from_spark(
    cycle_stats.select("transition","category_clean",F.col("p50_days").cast("double").alias("p50_days")),
    index_col="transition",
    series_col="category_clean",
    value_col="p50_days",
    title="Median Cycle Time (P50 days) by Transition — OPPM vs P6",
    xlabel="Transition",
    ylabel="Days"
)

# C) Risk score: % of P6 worse than OPPM P80 (per transition)
pdf = p6_risk.select("transition","p6_pct_worse_than_oppm_p80").toPandas()
plt.figure(figsize=(10,4))
plt.bar(pdf["transition"], pdf["p6_pct_worse_than_oppm_p80"])
plt.title("% of P6 Projects Slower Than OPPM P80 (Risk Score)")
plt.xlabel("Transition")
plt.ylabel("% of P6 > OPPM P80")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

# D) Overdue burden: overdue-days per 100 projects, split by group
grouped_bar_from_spark(
    overdue_burden.select("gate","category_clean","overdue_days_per_100_projects"),
    index_col="gate",
    series_col="category_clean",
    value_col="overdue_days_per_100_projects",
    title="Overdue Burden (Total Overdue Days per 100 Projects) by Gate — OPPM vs P6",
    xlabel="Gate",
    ylabel="Overdue days per 100 projects"
)
