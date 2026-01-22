from pyspark.sql import functions as F, types as T
import matplotlib.pyplot as plt
import numpy as np

# =========================
# 0) Base filters (as you requested)
# =========================
df_analysis = (
    df
    .filter((F.col("benchmark_eligible") == 1) & (F.col("is_sequence_full") == 1))
    .withColumn("category_clean", F.upper(F.trim(F.col("category").cast("string"))))
)

# Keep only OPPM and P6 if needed
df_analysis = df_analysis.filter(F.col("category_clean").isin("OPPM", "P6"))

analysis_date = F.current_date()

# =========================
# Helper: ensure planned "Approval Date" exists (parse Approval Value -> Approval Date if needed)
# =========================
gate_map = [
    ("A2", "Gate A2 Decision Date", "Gate A2 Approval Value", "Gate A2 Approval Date"),
    ("B",  "Gate B Decision Date",  "Gate B Approval Value",  "Gate B Approval Date"),
    ("C",  "Gate C Decision Date",  "Gate C Approval Value",  "Gate C Approval Date"),
    ("D",  "Gate D Decision Date",  "Gate D Approval Value",  "Gate D Approval Date"),
    ("E",  "Gate E Decision Date",  "Gate E Approval Value",  "Gate E Approval Date"),
]

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
        F.to_date(c, "dd-MMM-yyyy"),
        F.to_date(c, "dd-MMM-yy"),
        F.to_date(c, "MMM dd, yyyy"),
        F.to_date(F.to_timestamp(c))
    )

# Create Approval Date columns if they don't already exist
existing_cols = set(df_analysis.columns)
for g, dcol, aval_col, adate_col in gate_map:
    if adate_col not in existing_cols and aval_col in existing_cols:
        df_analysis = df_analysis.withColumn(adate_col, parse_date_spark(aval_col))

# =========================
# Helper: build timeliness flag + delay days (Actual - Planned)
# =========================
# Flag values: NO_PLAN, EARLY, ON_TIME, LATE, OVERDUE, UPCOMING
for g, dcol, aval_col, adate_col in gate_map:
    planned = F.col(adate_col)
    actual  = F.col(dcol)

    # Delay days: Actual - Planned (only when both exist)
    df_analysis = df_analysis.withColumn(
        f"{g}_delay_days_actual_minus_planned",
        F.when(planned.isNotNull() & actual.isNotNull(),
               F.datediff(actual, planned).cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # Timeliness flag
    df_analysis = df_analysis.withColumn(
        f"{g}_timeliness_flag",
        F.when(planned.isNull(), F.lit("NO_PLAN"))
         .when(actual.isNotNull() & (actual < planned), F.lit("EARLY"))
         .when(actual.isNotNull() & (actual == planned), F.lit("ON_TIME"))
         .when(actual.isNotNull() & (actual > planned), F.lit("LATE"))
         .when(actual.isNull() & (analysis_date > planned), F.lit("OVERDUE"))
         .otherwise(F.lit("UPCOMING"))
    )

# =========================
# 2) BOTTLENECK ANALYSIS
# =========================
# Needs cycle time columns (computed earlier). If your names differ, change here.
transitions = [
    ("A2→B", "ct_a2_to_b_days"),
    ("B→C",  "ct_b_to_c_days"),
    ("C→D",  "ct_c_to_d_days"),
    ("D→E",  "ct_d_to_e_days"),
]

# Slowest transition per project = max of available cycle times
arr = F.array(*[
    F.struct(F.lit(tname).alias("transition"), F.col(ctcol).cast("double").alias("days"))
    for tname, ctcol in transitions
])

df_bottleneck = (
    df_analysis
    .withColumn("transition_days_arr", arr)
    .withColumn("transition_days_arr", F.expr("filter(transition_days_arr, x -> x.days is not null and x.days >= 0)"))
    .withColumn(
        "slowest_struct",
        F.expr("""
          aggregate(
            transition_days_arr,
            cast(null as struct<transition:string,days:double>),
            (acc, x) -> case
                          when acc is null then x
                          when x.days > acc.days then x
                          else acc
                        end
          )
        """)
    )
    .withColumn("slowest_transition", F.col("slowest_struct.transition"))
    .withColumn("slowest_days", F.col("slowest_struct.days"))
)

bottleneck_counts = (
    df_bottleneck
    .filter(F.col("slowest_transition").isNotNull())
    .groupBy("category_clean", "slowest_transition")
    .agg(F.count("*").alias("n_projects"))
)

display(bottleneck_counts.orderBy("category_clean", F.desc("n_projects")))

# --- Heatmap: Category × Slowest transition (counts)
pdf_heat = (
    bottleneck_counts
    .groupBy("category_clean")
    .pivot("slowest_transition", [t[0] for t in transitions])
    .agg(F.first("n_projects"))
    .fillna(0)
    .toPandas()
    .set_index("category_clean")
)

mat = pdf_heat.values.astype(float)
fig, ax = plt.subplots(figsize=(8, 3))
im = ax.imshow(mat, aspect="auto")  # default colormap
ax.set_xticks(np.arange(len(pdf_heat.columns)))
ax.set_xticklabels(pdf_heat.columns)
ax.set_yticks(np.arange(len(pdf_heat.index)))
ax.set_yticklabels(pdf_heat.index)
ax.set_title("Bottleneck heatmap: Category × Slowest transition (count)")
for i in range(mat.shape[0]):
    for j in range(mat.shape[1]):
        ax.text(j, i, f"{int(mat[i,j])}", ha="center", va="center", fontsize=9)
fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="Projects")
plt.tight_layout()
plt.show()

# --- Stacked bar: Most common bottleneck transition (share within each category)
pdf_stack = pdf_heat.copy()
pdf_stack_pct = pdf_stack.div(pdf_stack.sum(axis=1), axis=0).fillna(0)

ax = pdf_stack_pct.plot(kind="bar", stacked=True, figsize=(9, 4))
ax.set_title("Most common bottleneck transition (share within category)")
ax.set_xlabel("Category")
ax.set_ylabel("Share of projects")
plt.tight_layout()
plt.show()

# =========================
# 3) TIMELINESS VS PLAN (Actual vs Planned)
# =========================
# Long-format of gate flags
gate_order = ["A2", "B", "C", "D", "E"]
flag_cols = {g: f"{g}_timeliness_flag" for g in gate_order}
delay_cols = {g: f"{g}_delay_days_actual_minus_planned" for g in gate_order}

stack_flags = "stack(5, " + ", ".join([f"'{g}', `{flag_cols[g]}`" for g in gate_order]) + ") as (gate, flag)"
stack_delays = "stack(5, " + ", ".join([f"'{g}', `{delay_cols[g]}`" for g in gate_order]) + ") as (gate, delay_days)"

df_flags_long = (
    df_analysis
    .select("category_clean", F.expr(stack_flags))
    .withColumn("flag", F.coalesce(F.col("flag"), F.lit("NO_PLAN")))
)

flag_counts = (
    df_flags_long
    .groupBy("category_clean", "gate", "flag")
    .agg(F.count("*").alias("n"))
)

display(flag_counts.orderBy("gate", "category_clean", F.desc("n")))

# --- Stacked bar per gate (2 bars: OPPM vs P6, stacked by flags)
flag_order = ["EARLY", "ON_TIME", "LATE", "OVERDUE", "UPCOMING", "NO_PLAN"]

for g in gate_order:
    pdf = (
        flag_counts.filter(F.col("gate") == g)
        .groupBy("category_clean")
        .pivot("flag", flag_order)
        .agg(F.first("n"))
        .fillna(0)
        .toPandas()
        .set_index("category_clean")
    )
    ax = pdf.plot(kind="bar", stacked=True, figsize=(9, 4))
    ax.set_title(f"Timeliness split for Gate {g} (OPPM vs P6)")
    ax.set_xlabel("Category")
    ax.set_ylabel("Count of projects")
    plt.tight_layout()
    plt.show()

# --- Median + P80 delay bars per gate split by category (using delay_days where both exist)
df_delays_long = (
    df_analysis
    .select("category_clean", F.expr(stack_delays))
    .filter(F.col("delay_days").isNotNull())
    .withColumn("delay_days", F.col("delay_days").cast("double"))
)

delay_stats = (
    df_delays_long
    .groupBy("category_clean", "gate")
    .agg(
        F.count("*").alias("n"),
        F.expr("percentile_approx(delay_days, 0.5)").alias("median_delay"),
        F.expr("percentile_approx(delay_days, 0.8)").alias("p80_delay"),
    )
)

display(delay_stats.orderBy("gate", "category_clean"))

pdf_med = (
    delay_stats
    .select("category_clean", "gate", "median_delay")
    .groupBy("gate")
    .pivot("category_clean", ["OPPM", "P6"])
    .agg(F.first("median_delay"))
    .toPandas()
    .set_index("gate")
    .reindex(gate_order)
)

ax = pdf_med.plot(kind="bar", figsize=(9, 4))
ax.set_title("Median delay days (Actual − Planned) per gate, split by category")
ax.set_xlabel("Gate")
ax.set_ylabel("Median delay (days)")
plt.tight_layout()
plt.show()

pdf_p80 = (
    delay_stats
    .select("category_clean", "gate", "p80_delay")
    .groupBy("gate")
    .pivot("category_clean", ["OPPM", "P6"])
    .agg(F.first("p80_delay"))
    .toPandas()
    .set_index("gate")
    .reindex(gate_order)
)

ax = pdf_p80.plot(kind="bar", figsize=(9, 4))
ax.set_title("P80 delay days (Actual − Planned) per gate, split by category")
ax.set_xlabel("Gate")
ax.set_ylabel("P80 delay (days)")
plt.tight_layout()
plt.show()

# --- Overdue backlog bars per gate split by category
overdue_counts = (
    df_flags_long
    .filter(F.col("flag") == "OVERDUE")
    .groupBy("category_clean", "gate")
    .agg(F.count("*").alias("overdue_count"))
)

display(overdue_counts.orderBy("gate", "category_clean"))

pdf_overdue = (
    overdue_counts
    .groupBy("gate")
    .pivot("category_clean", ["OPPM", "P6"])
    .agg(F.first("overdue_count"))
    .fillna(0)
    .toPandas()
    .set_index("gate")
    .reindex(gate_order)
)

ax = pdf_overdue.plot(kind="bar", figsize=(9, 4))
ax.set_title("Overdue backlog count per gate (planned passed, decision missing)")
ax.set_xlabel("Gate")
ax.set_ylabel("Overdue count")
plt.tight_layout()
plt.show()

# =========================
# 4) BACKLOG ANALYSIS (definition-based, not flag-based)
# planned date passed AND decision missing
# =========================
backlog_rows = []
for g, dcol, aval_col, adate_col in gate_map:
    backlog_rows.append(
        df_analysis.select(
            "category_clean",
            F.lit(g).alias("gate"),
            F.col(adate_col).alias("planned_date"),
            F.col(dcol).alias("decision_date")
        )
    )

df_backlog_long = backlog_rows[0]
for tmp in backlog_rows[1:]:
    df_backlog_long = df_backlog_long.unionByName(tmp)

df_backlog_overdue = (
    df_backlog_long
    .filter(F.col("planned_date").isNotNull())
    .filter(F.col("decision_date").isNull())
    .filter(F.col("planned_date") < analysis_date)
    .groupBy("category_clean", "gate")
    .agg(F.count("*").alias("overdue_backlog_count"))
)

display(df_backlog_overdue.orderBy("gate", "category_clean"))

pdf_backlog = (
    df_backlog_overdue
    .groupBy("gate")
    .pivot("category_clean", ["OPPM", "P6"])
    .agg(F.first("overdue_backlog_count"))
    .fillna(0)
    .toPandas()
    .set_index("gate")
    .reindex(gate_order)
)

ax = pdf_backlog.plot(kind="bar", figsize=(9, 4))
ax.set_title("Overdue backlog (planned passed & decision missing): Gate × Category")
ax.set_xlabel("Gate")
ax.set_ylabel("Overdue backlog count")
plt.tight_layout()
plt.show()

