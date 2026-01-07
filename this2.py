import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

plt.rcParams["figure.figsize"] = (12, 6)




DATA_DIR = Path(r"./data")  # change this

activity_full = pd.read_csv(DATA_DIR / "Activity_Full.csv", low_memory=False)
aca = pd.read_csv(DATA_DIR / "activitycodeassignment.csv", low_memory=False)

# Your phase sequence table exported as CSV (recommended)
# It must have columns exactly: "Activity Code", "Milestone Description"
phase_seq = pd.read_csv(DATA_DIR / "phase_sequence.csv", low_memory=False)

# Optional (only if you want project names)
project_full = pd.read_csv(DATA_DIR / "project_full.csv", low_memory=False)



date_cols = [
    "DATADATE", "CREATEDATE", "LASTUPDATEDATE",
    "BASELINESTARTDATE", "PLANNEDSTARTDATE", "ACTUALSTARTDATE",
    "STARTDATE", "EARLYSTARTDATE", "LATESTARTDATE"
]

cols_present = [c for c in date_cols if c in activity_full.columns]

def parse_mixed_excel_dates(s: pd.Series) -> pd.Series:
    # Make everything string first (preserves things like '01-Sep-25')
    s_str = s.astype(str).str.strip()

    # Replace common non-date garbage with NA
    s_str = s_str.replace(
        {"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA, "########": pd.NA, "#####": pd.NA},
        regex=False
    )

    # Try parse as normal date strings first
    dt1 = pd.to_datetime(s_str, errors="coerce", dayfirst=True)

    # For anything still NaT, try interpreting as Excel serial numbers
    # (only where the original looks numeric)
    is_num = s_str.str.fullmatch(r"\d+(\.\d+)?", na=False)
    serial = pd.to_numeric(s_str.where(is_num), errors="coerce")

    dt2 = pd.to_datetime(serial, errors="coerce", unit="D", origin="1899-12-30")

    # Combine: prefer dt1, fallback to dt2
    return dt1.fillna(dt2)

for c in cols_present:
    activity_full[c] = parse_mixed_excel_dates(activity_full[c])








phase_seq = phase_seq.copy()
phase_seq["phase_order"] = np.arange(1, len(phase_seq) + 1)

# sanity check
phase_seq.head()








from pyspark.sql import functions as F

# Assume these are Spark DataFrames:
# activity_full, phase_seq

# Ensure string matching is consistent (cast to string + trim)
act = (
    activity_full
    .withColumn("ID", F.trim(F.col("ID").cast("string")))
)

phase_seq_clean = (
    phase_seq
    .withColumn("Activity Code", F.trim(F.col("Activity Code").cast("string")))
    .select("Activity Code")
    .dropDuplicates()
)

# Keep only gate activities (ID is in the gate code list)
act_gate = (
    act.join(
        phase_seq_clean,
        act["ID"] == phase_seq_clean["Activity Code"],
        how="left_semi"   # filters act only; doesn't add columns
    )
)






def compute_gate_date(df: pd.DataFrame) -> pd.Series:
    if "FINISHDATE" in df.columns:
        return df["FINISHDATE"].fillna(df["ACTUALSTARTDATE"]).fillna(df["STARTDATE"])
    return df["ACTUALSTARTDATE"].fillna(df["STARTDATE"])

act_gate["gate_date"] = compute_gate_date(act_gate)





latest_datadate = act_gate.groupby("PROJECTOBJECTID")["DATADATE"].transform("max")
act_gate = act_gate[act_gate["DATADATE"] == latest_datadate].copy()




gate_events = (
    act_gate.dropna(subset=["gate_date"])
    .groupby(["PROJECTOBJECTID", "ACTIVITYCODEVALUE"], as_index=False)
    .agg(gate_date=("gate_date", "min"))
)

# Add phase order + description from your sequence table
gate_events = gate_events.merge(
    phase_seq.rename(columns={"Activity Code": "ACTIVITYCODEVALUE", "Milestone Description": "phase_name"}),
    on="ACTIVITYCODEVALUE",
    how="left"
)

gate_events = gate_events.sort_values(["PROJECTOBJECTID", "phase_order"])
gate_events.head()






gate_events["next_gate_date"] = gate_events.groupby("PROJECTOBJECTID")["gate_date"].shift(-1)
gate_events["phase_duration_days"] = (gate_events["next_gate_date"] - gate_events["gate_date"]).dt.days




project_timeline = (
    gate_events.groupby("PROJECTOBJECTID", as_index=False)
    .agg(
        project_start=("gate_date", "min"),
        project_end=("gate_date", "max"),
        gates_present=("ACTIVITYCODEVALUE", "count")
    )
)
project_timeline["project_total_days"] = (project_timeline["project_end"] - project_timeline["project_start"]).dt.days





if "OBJECTID" in project_full.columns and "NAME" in project_full.columns:
    project_timeline = project_timeline.merge(
        project_full[["OBJECTID", "NAME"]],
        left_on="PROJECTOBJECTID",
        right_on="OBJECTID",
        how="left"
    )




def add_iqr_outlier_flags(df: pd.DataFrame, group_col: str, value_col: str) -> pd.DataFrame:
    df = df.copy()
    q1 = df.groupby(group_col)[value_col].transform(lambda s: s.quantile(0.25))
    q3 = df.groupby(group_col)[value_col].transform(lambda s: s.quantile(0.75))
    iqr = q3 - q1
    df["outlier_threshold"] = q3 + 1.5 * iqr
    df["is_outlier"] = df[value_col] > df["outlier_threshold"]
    return df

phase_perf = gate_events.dropna(subset=["phase_duration_days"]).copy()
phase_perf = add_iqr_outlier_flags(phase_perf, "phase_order", "phase_duration_days")





# Build arrays in phase order
phase_labels = (phase_seq.sort_values("phase_order")["Milestone Description"]).tolist()

data = []
labels = []
for _, row in phase_seq.sort_values("phase_order").iterrows():
    po = row["phase_order"]
    s = phase_perf.loc[phase_perf["phase_order"] == po, "phase_duration_days"].dropna()
    if len(s) > 0:
        data.append(s.values)
        labels.append(row["Milestone Description"])

plt.figure(figsize=(14, 6))
plt.boxplot(data, labels=labels, showfliers=True)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Phase duration (days)")
plt.title("Phase duration distribution (gate-to-gate)")
plt.tight_layout()
plt.show()





# Use project name if available, else PROJECTOBJECTID
row_label = "NAME" if "NAME" in project_timeline.columns else "PROJECTOBJECTID"

# Merge labels onto phase_perf
phase_perf_labeled = phase_perf.merge(
    project_timeline[[ "PROJECTOBJECTID", row_label ]],
    on="PROJECTOBJECTID",
    how="left"
)

pivot = phase_perf_labeled.pivot_table(
    index=row_label,
    columns="phase_order",
    values="phase_duration_days",
    aggfunc="first"
).sort_index()

# Convert columns (phase_order) to readable names
col_map = dict(zip(phase_seq["phase_order"], phase_seq["Milestone Description"]))
pivot = pivot.rename(columns=col_map)

plt.figure(figsize=(16, 8))
plt.imshow(pivot.fillna(0).values, aspect="auto")
plt.colorbar(label="Phase duration (days)")
plt.yticks(range(len(pivot.index)), pivot.index)
plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=45, ha="right")
plt.title("Projects × Phases heatmap (duration in days; 0 = missing)")
plt.tight_layout()
plt.show()





outliers = phase_perf[phase_perf["is_outlier"]].copy()
outliers = outliers.sort_values("phase_duration_days", ascending=False).head(20)

# Label bars
outliers["label"] = outliers["PROJECTOBJECTID"].astype(str) + " | " + outliers["phase_name"].astype(str)

plt.figure(figsize=(14, 6))
plt.barh(outliers["label"], outliers["phase_duration_days"])
plt.gca().invert_yaxis()
plt.xlabel("Days")
plt.title("Top 20 phase-duration outliers")
plt.tight_layout()
plt.show()





top_projects = project_timeline.sort_values("project_total_days", ascending=False).head(30)

plt.figure(figsize=(14, 6))
plt.barh(top_projects[row_label].astype(str), top_projects["project_total_days"])
plt.gca().invert_yaxis()
plt.xlabel("Total duration (days)")
plt.title("Top 30 longest projects (first gate → last gate)")
plt.tight_layout()
plt.show()




# pick a project (longest)
target_project = project_timeline.sort_values("project_total_days", ascending=False).iloc[0]["PROJECTOBJECTID"]

p = gate_events[gate_events["PROJECTOBJECTID"] == target_project].sort_values("phase_order").copy()
p = p.dropna(subset=["next_gate_date"])

plt.figure(figsize=(12, 6))
y = np.arange(len(p))

dur = (p["next_gate_date"] - p["gate_date"]).dt.days
plt.barh(y, dur, left=p["gate_date"].map(pd.Timestamp.toordinal))
plt.yticks(y, p["phase_name"])
plt.xlabel("Time")
plt.title(f"Phase durations for project {target_project}")

# Convert ordinal axis ticks back to dates
ax = plt.gca()
ticks = ax.get_xticks()
ax.set_xticklabels([pd.Timestamp.fromordinal(int(t)).strftime("%Y-%m-%d") if t > 0 else "" for t in ticks], rotation=30, ha="right")

plt.tight_layout()
plt.show()




