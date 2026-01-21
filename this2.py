from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import numpy as np

# =========================================================
# 0) QUICK CHECK: ensure required derived columns exist
# =========================================================
needed = [
    "benchmark_eligible",
    "seq_a2_b_status","seq_b_c_status","seq_c_d_status","seq_d_e_status",
    "ct_a2_to_b_days","ct_b_to_c_days","ct_c_to_d_days","ct_d_to_e_days",
    "Gate A2 Decision Date","Gate B Decision Date","Gate C Decision Date","Gate D Decision Date","Gate E Decision Date"
]
missing = [c for c in needed if c not in df_OPPM.columns]
if missing:
    raise Exception(
        f"Missing columns: {missing}\n"
        f"Run your calculated-fields block first (benchmark_eligible, seq_*, ct_*)."
    )

# =========================================================
# 1) STRICT SEQUENCE FILTER FOR A2→E TIMELINE ANALYSIS
#    Drop ENTIRE ROW if ANY transition is OUT_OF_ORDER
# =========================================================
df_OPPM = df_OPPM.withColumn(
    "is_sequence_full",
    F.when(
        (F.col("seq_a2_b_status") != "OUT_OF_ORDER") &
        (F.col("seq_b_c_status")  != "OUT_OF_ORDER") &
        (F.col("seq_c_d_status")  != "OUT_OF_ORDER") &
        (F.col("seq_d_e_status")  != "OUT_OF_ORDER"),
        F.lit(1)
    ).otherwise(F.lit(0))
)

# Analysis dataset (your rule)
df_analysis = df_OPPM.filter(
    (F.col("benchmark_eligible") == 1) &
    (F.col("is_sequence_full") == 1)
)

print("Rows kept for A2→E analysis:", df_analysis.count())
display(df_analysis.select(
    "Project ID","benchmark_eligible","is_sequence_full",
    "seq_a2_b_status","seq_b_c_status","seq_c_d_status","seq_d_e_status",
    "Gate A2 Decision Date","Gate B Decision Date","Gate C Decision Date","Gate D Decision Date","Gate E Decision Date"
).limit(20))

# =========================================================
# 2) BENCHMARKS PER TRANSITION: P20 / P50 / P80 + Mean
#    (computed only on OK datapoints per transition)
# =========================================================
transitions = [
    ("A2→B", "seq_a2_b_status", "ct_a2_to_b_days"),
    ("B→C",  "seq_b_c_status",  "ct_b_to_c_days"),
    ("C→D",  "seq_c_d_status",  "ct_c_to_d_days"),
    ("D→E",  "seq_d_e_status",  "ct_d_to_e_days"),
]

bench_list = []
for tname, status_col, ct_col in transitions:
    b = (
        df_analysis
        .filter(F.col(status_col) == "OK")
        .select(F.col(ct_col).cast("double").alias("cycle_days"))
        .filter(F.col("cycle_days").isNotNull() & (F.col("cycle_days") >= 0))
        .agg(
            F.count("*").alias("n_datapoints"),
            F.expr("percentile_approx(cycle_days, 0.2)").alias("P20_days"),
            F.expr("percentile_approx(cycle_days, 0.5)").alias("P50_days"),
            F.expr("percentile_approx(cycle_days, 0.8)").alias("P80_days"),
            F.round(F.avg("cycle_days"), 1).alias("Mean_days")
        )
        .withColumn("transition", F.lit(tname))
    )
    bench_list.append(b)

benchmarks = bench_list[0]
for b in bench_list[1:]:
    benchmarks = benchmarks.unionByName(b)

benchmarks = benchmarks.select("transition","n_datapoints","P20_days","P50_days","P80_days","Mean_days") \
                       .orderBy("transition")

display(benchmarks)

# =========================================================
# 3) BAR CHART: P20 vs P50 vs P80 (distribution per transition)
# =========================================================
bp = benchmarks.toPandas().sort_values("transition")

x = np.arange(len(bp["transition"]))
w = 0.25

plt.figure(figsize=(10, 4))
plt.bar(x - w, bp["P20_days"], width=w, label="P20")
plt.bar(x,      bp["P50_days"], width=w, label="P50 (Median)")
plt.bar(x + w,  bp["P80_days"], width=w, label="P80")

plt.xticks(x, bp["transition"])
plt.xlabel("Transition")
plt.ylabel("Days")
plt.title("Transition Time Distribution (P20 / P50 / P80) — Sequence-safe + Benchmark eligible")

# annotate sample size
for i, n in enumerate(bp["n_datapoints"].values):
    plt.text(i, bp["P80_days"].values[i], f" n={int(n)}", ha="center", va="bottom", fontsize=9)

plt.legend()
plt.tight_layout()
plt.show()

# =========================================================
# 4) BAR CHART: Which transition takes the most time overall (Mean)
# =========================================================
plt.figure(figsize=(8.5, 3.8))
plt.bar(bp["transition"], bp["Mean_days"])
plt.xlabel("Transition")
plt.ylabel("Mean days")
plt.title("Which Gate Transition Takes the Most Time Overall (Mean) — Sequence-safe + Benchmark eligible")
plt.tight_layout()
plt.show()

# =========================================================
# 5) TOP 10 PROJECTS PER TRANSITION (4 BAR CHARTS)
# =========================================================
def top10_projects_for_transition(tname, status_col, ct_col):
    return (
        df_analysis
        .filter(F.col(status_col) == "OK")
        .select(
            F.col("Project ID"),
            F.col("Project Title"),
            F.col("region"),
            F.col("Investment Type"),
            F.col("Delivery Unit"),
            F.col(ct_col).cast("int").alias("duration_days")
        )
        .filter(F.col("duration_days").isNotNull() & (F.col("duration_days") >= 0))
        .orderBy(F.desc("duration_days"))
        .limit(10)
        .withColumn("transition", F.lit(tname))
    )

def plot_top10_bar(df_top10, title):
    pd_top = df_top10.select("Project ID", "duration_days").toPandas()
    plt.figure(figsize=(10, 4))
    plt.barh(pd_top["Project ID"][::-1], pd_top["duration_days"][::-1])
    plt.xlabel("Days")
    plt.ylabel("Project ID")
    plt.title(title)
    plt.tight_layout()
    plt.show()

top10_a2b = top10_projects_for_transition("A2→B", "seq_a2_b_status", "ct_a2_to_b_days")
top10_bc  = top10_projects_for_transition("B→C",  "seq_b_c_status",  "ct_b_to_c_days")
top10_cd  = top10_projects_for_transition("C→D",  "seq_c_d_status",  "ct_c_to_d_days")
top10_de  = top10_projects_for_transition("D→E",  "seq_d_e_status",  "ct_d_to_e_days")

print("Top 10 A2→B")
display(top10_a2b); plot_top10_bar(top10_a2b, "Top 10 Longest Projects — A2→B (Sequence-safe)")

print("Top 10 B→C")
display(top10_bc);  plot_top10_bar(top10_bc,  "Top 10 Longest Projects — B→C (Sequence-safe)")

print("Top 10 C→D")
display(top10_cd);  plot_top10_bar(top10_cd,  "Top 10 Longest Projects — C→D (Sequence-safe)")

print("Top 10 D→E")
display(top10_de);  plot_top10_bar(top10_de,  "Top 10 Longest Projects — D→E (Sequence-safe)")

# =========================================================
# 6) TOP 10 PROJECTS FOR TOTAL A2→E (BAR CHART)
#    Requires A2 + E present (and sequence-safe dataset already applied)
# =========================================================
df_top10_a2e = (
    df_analysis
    .filter(F.col("Gate A2 Decision Date").isNotNull() & F.col("Gate E Decision Date").isNotNull())
    .withColumn("total_A2_to_E_days", F.datediff(F.col("Gate E Decision Date"), F.col("Gate A2 Decision Date")))
    .filter(F.col("total_A2_to_E_days").isNotNull() & (F.col("total_A2_to_E_days") >= 0))
    .select(
        "Project ID","Project Title","region","Investment Type","Delivery Unit",
        "total_A2_to_E_days",
        "Gate A2 Decision Date","Gate E Decision Date"
    )
    .orderBy(F.desc("total_A2_to_E_days"))
    .limit(10)
)

print("Top 10 Total A2→E")
display(df_top10_a2e)

pd_a2e = df_top10_a2e.select("Project ID","total_A2_tosA2_to_E_days".replace("sA2","A2")).toPandas()  # harmless safety if copy glitches
# If the above line ever errors, just use:
# pd_a2e = df_top10_a2e.select("Project ID","total_A2_to_E_days").toPandas()

plt.figure(figsize=(10, 4))
plt.barh(pd_a2e["Project ID"][::-1], pd_a2e["total_A2_to_E_days"][::-1])
plt.xlabel("Total Days (A2→E)")
plt.ylabel("Project ID")
plt.title("Top 10 Longest Projects — Total A2→E (Sequence-safe + Benchmark eligible)")
plt.tight_layout()
plt.show()
