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



import pandas as pd
import numpy as np

# 2) Parse dates (using Activity_Full.csv columns exactly)
date_cols_activity = [
    "DATADATE", "CREATEDATE", "LASTUPDATEDATE",
    "BASELINESTARTDATE", "PLANNEDSTARTDATE", "ACTUALSTARTDATE",
    "STARTDATE", "EARLYSTARTDATE", "LATESTARTDATE"
]

for c in date_cols_activity:
    # only parse if the column exists (safe)
    if c in activity_full.columns:
        activity_full[c] = pd.to_datetime(activity_full[c], errors="coerce")


# 2b) Gate event date (best available "gate achieved/forecast" date)
# Rule:
# - if ACTUALSTARTDATE exists => treat as achieved date
# - else use STARTDATE as forecast
activity_full["gate_event_date"] = (
    activity_full["ACTUALSTARTDATE"]
    .fillna(activity_full["STARTDATE"])
)

# planned slip (current/actual vs planned)
activity_full["slip_vs_planned_days"] = (
    activity_full["gate_event_date"] - activity_full["PLANNEDSTARTDATE"]
).dt.days

# baseline slip (current/actual vs baseline)
activity_full["slip_vs_baseline_days"] = (
    activity_full["gate_event_date"] - activity_full["BASELINESTARTDATE"]
).dt.days







phase_seq = phase_seq.copy()
phase_seq["phase_order"] = np.arange(1, len(phase_seq) + 1)

# sanity check
phase_seq.head()








# Merge activity with code assignments
act = activity_full.merge(
    aca[["ACTIVITYID", "ACTIVITYCODETYPENAME", "ACTIVITYCODEVALUE", "ACTIVITYCODEDESCRIPTION", "PROJECTOBJECTID"]],
    left_on="OBJECTID",
    right_on="ACTIVITYID",
    how="left"
)

# Keep only gate codes in the sequence
gate_codes = set(phase_seq["Activity Code"].dropna().astype(str))
act["ACTIVITYCODEVALUE"] = act["ACTIVITYCODEVALUE"].astype(str)

act_gate = act[act["ACTIVITYCODEVALUE"].isin(gate_codes)].copy()






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

