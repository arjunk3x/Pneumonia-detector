# Cell 2 - Imports and Databricks helpers
import os
import re
import uuid
import yaml
import math
from pathlib import Path
from datetime import date, datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T

print(f"Python: {os.sys.version.split()[0]}")
print(f"Spark: {spark.version}")
print(f"Working dir: {os.getcwd()}")


def widget_text(name: str, default_value: str, label=None) -> None:
    """Create a Databricks widget if it does not already exist."""
    try:
        dbutils.widgets.text(name, default_value, label or name)
    except Exception:
        pass


def get_widget(name: str, default_value: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)
        if value is None or str(value).strip() == "":
            return default_value
        return value
    except Exception:
        return default_value


widget_text("rules_path", "/Volumes/<catalog>/<schema>/<volume>/rules.yaml", "rules_path")
widget_text("csv_path", "/Volumes/<catalog>/<schema>/<volume>/investment_projects.csv", "csv_path")
widget_text("output_path", "dbfs:/tmp/dq_rules_engine_output", "output_path")
widget_text("run_date", "", "run_date YYYY-MM-DD optional")
widget_text("run_id", "", "run_id optional")
widget_text("run_timestamp_utc", "", "run_timestamp_utc optional")
widget_text("spark_timezone", "UTC", "spark_timezone")
widget_text("azure_endpoint", "https://et-dev-uks-ai-aif-01.services.ai.azure.com/", "azure_endpoint")
widget_text("azure_deployment", "gpt-4.1", "azure_deployment")
widget_text("azure_api_version", "2024-10-21", "azure_api_version")
widget_text("azure_openai_secret_scope", "", "azure_openai_secret_scope optional")
widget_text("azure_openai_secret_key", "AZURE_OPENAI_API_KEY", "azure_openai_secret_key")

spark.conf.set("spark.sql.session.timeZone", get_widget("spark_timezone", "UTC"))


def to_driver_path(path: str) -> str:
    """Convert dbfs:/ paths to the driver-mounted /dbfs path for Python open()."""
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/"):].lstrip("/")
    return path


def to_dbutils_path(path: str) -> str:
    """Convert /dbfs paths back to dbfs:/ for dbutils.fs operations."""
    if path.startswith("/dbfs/"):
        return "dbfs:/" + path[len("/dbfs/"):].lstrip("/")
    return path


def path_join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])


def read_text_file(path: str) -> str:
    """Read a small text file from DBFS, Volumes, Workspace files, or cloud storage."""
    driver_path = to_driver_path(path)
    try:
        with open(driver_path, "r", encoding="utf-8") as f:
            return f.read()
    except OSError:
        return "\n".join(row.value for row in spark.read.text(path).collect())


def write_text_file(path: str, text: str, overwrite: bool = True) -> None:
    dbutils.fs.put(to_dbutils_path(path), text, overwrite=overwrite)


def quote_spark_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def spark_col(name: str):
    return F.col(quote_spark_identifier(name))


def get_run_date() -> date:
    run_date = get_widget("run_date", "").strip()
    if run_date:
        return datetime.strptime(run_date, "%Y-%m-%d").date()
    return datetime.now().date()


















# Cell 3 - GPT-4.1 connection
from langchain_openai import AzureChatOpenAI

AZURE_ENDPOINT = get_widget("azure_endpoint", "https://et-dev-uks-ai-aif-01.services.ai.azure.com/")
AZURE_DEPLOYMENT = get_widget("azure_deployment", "gpt-4.1")
AZURE_API_VERSION = get_widget("azure_api_version", "2024-10-21")

AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
if not AZURE_API_KEY:
    secret_scope = get_widget("azure_openai_secret_scope", "")
    secret_key = get_widget("azure_openai_secret_key", "AZURE_OPENAI_API_KEY")
    if secret_scope:
        AZURE_API_KEY = dbutils.secrets.get(scope=secret_scope, key=secret_key)

if not AZURE_API_KEY:
    raise ValueError(
        "Set AZURE_OPENAI_API_KEY, or set the azure_openai_secret_scope and "
        "azure_openai_secret_key Databricks widgets."
    )

llm = AzureChatOpenAI(
    api_version=AZURE_API_VERSION,
    azure_endpoint=AZURE_ENDPOINT,
    azure_deployment=AZURE_DEPLOYMENT,
    api_key=AZURE_API_KEY,
    temperature=0.0,
)

resp = llm.invoke("Say OK")
print(f"GPT-4.1 connected - {resp.content[:30]}")
























# Cell 4 - Load rules.yaml
RULES_PATH = get_widget("rules_path")

RULES_CONFIG = yaml.safe_load(read_text_file(RULES_PATH))
RULES = RULES_CONFIG["rules"]

print(f"Loaded {len(RULES)} rules from {RULES_PATH}\n")
for r in RULES:
    print(f"  {r['id']:6} {r['name']}")













# Cell 5 - Load CSV with Spark
CSV = get_widget("csv_path")

investment_projects = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .option("nullValue", "")
    .option("emptyValue", "")
    .load(CSV)
)

# Match pandas read_csv(..., encoding="utf-8-sig") by removing a BOM from the first header.
for col_name in investment_projects.columns:
    cleaned = col_name.lstrip("\ufeff")
    if cleaned != col_name:
        investment_projects = investment_projects.withColumnRenamed(col_name, cleaned)

investment_projects = investment_projects.cache()
investment_projects.createOrReplaceTempView("investment_projects")

row_count = investment_projects.count()
print(f"Loaded: {row_count:,} rows x {len(investment_projects.columns)} cols")
















# Cell 6 - Schema extraction for the LLM
def get_table_schema(df, table_name: str) -> str:
    lines = [f"Table: {table_name}", f"Row count: {df.count()}", "Columns:"]
    for col in df.columns:
        sample_values = [
            row[0]
            for row in (
                df
                .select(spark_col(col).cast("string"))
                .where(spark_col(col).isNotNull())
                .limit(20)
                .collect()
            )
        ]

        if "date" in col.lower():
            dtype = "DATE (DD/MM/YYYY text, sentinel '01/01/1990' = NULL)"
        elif len(sample_values) > 0 and all(str(v).isnumeric() for v in sample_values):
            dtype = "NUMERIC"
        else:
            dtype = "STRING"

        lines.append(f"- {col} ({dtype})")

    return "\n".join(lines)


TABLE_SCHEMA = get_table_schema(investment_projects, "investment_projects")
print(TABLE_SCHEMA[:500])
print(f"... ({len(investment_projects.columns)} columns total)")





















# Cell 7 - Spark SQL normalisation for deterministic Databricks execution
def split_top_level_sql_args(arg_text: str) -> list[str]:
    args = []
    start = 0
    depth = 0
    quote = None
    i = 0

    while i < len(arg_text):
        ch = arg_text[i]

        if quote:
            if ch == quote:
                if i + 1 < len(arg_text) and arg_text[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            args.append(arg_text[start:i].strip())
            start = i + 1

        i += 1

    args.append(arg_text[start:].strip())
    return args


def find_matching_paren(sql: str, open_paren_index: int) -> int:
    depth = 1
    quote = None
    i = open_paren_index + 1

    while i < len(sql):
        ch = sql[i]

        if quote:
            if ch == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i

        i += 1

    return -1


def tolerant_date_expr(expr: str) -> str:
    return (
        f"coalesce("
        f"try_to_date({expr}, 'dd/MM/yyyy'), "
        f"try_to_date({expr}, 'yyyy-MM-dd'), "
        f"try_to_date({expr}, 'dd-MM-yyyy'), "
        f"try_to_date({expr}, 'yyyy/MM/dd')"
        f")"
    )


def replace_to_date_calls(sql: str) -> str:
    """Replace strict to_date(expr, fmt) calls with tolerant multi-format parsing."""
    pieces = []
    pos = 0
    pattern = re.compile(r"\bto_date\s*\(", flags=re.IGNORECASE)

    while True:
        match = pattern.search(sql, pos)
        if not match:
            pieces.append(sql[pos:])
            break

        open_paren = match.end() - 1
        close_paren = find_matching_paren(sql, open_paren)
        if close_paren == -1:
            pieces.append(sql[pos:])
            break

        inner = sql[open_paren + 1:close_paren]
        args = split_top_level_sql_args(inner)
        original = sql[match.start():close_paren + 1]

        pieces.append(sql[pos:match.start()])
        if len(args) >= 2:
            pieces.append(tolerant_date_expr(args[0]))
        else:
            pieces.append(original)

        pos = close_paren + 1

    return "".join(pieces)


def normalize_spark_sql(sql: str) -> str:
    """Keep the LLM SQL Spark-native and pin current_date() to this run's date."""
    s = sql.strip().rstrip(";")
    s = replace_to_date_calls(s)
    run_date_sql = f"DATE '{get_run_date().strftime('%Y-%m-%d')}'"
    s = re.sub(r"\bcurrent_date\s*\(\s*\)", run_date_sql, s, flags=re.IGNORECASE)
    s = re.sub(r"\bcurrent_date\b", run_date_sql, s, flags=re.IGNORECASE)
    return s


def dedupe_spark_columns(df):
    """Keep the first instance of duplicate selected columns, matching pandas behaviour."""
    seen = {}
    renamed_columns = []
    keep_columns = []

    for col_name in df.columns:
        count = seen.get(col_name, 0)
        if count == 0:
            renamed_columns.append(col_name)
            keep_columns.append(col_name)
        else:
            renamed_columns.append(f"{col_name}__duplicate_{count}")
        seen[col_name] = count + 1

    return df.toDF(*renamed_columns).select(*[spark_col(c) for c in keep_columns])


print("normalize_spark_sql() ready")




















# Cell 8 — Pydantic output schema
from pydantic import BaseModel, Field

class GeneratedSQL(BaseModel):
    """Structured output from the LLM for a single rule's SQL query."""
    rule_id: str = Field(description="The rule ID (e.g. DQ001)")
    sql: str = Field(description="SQL query returning rows VIOLATING the rule")
    explanation: str = Field(description="Brief explanation of the SQL logic")
    failed_field: str = Field(description="The primary column(s) being checked")

print("Pydantic schema defined: GeneratedSQL")


















# Cell 9 - INTENT-BASED prompt (no SQL templates - LLM must reason independently)
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser

RULE_TO_SQL_TEMPLATE = """You are a Data Quality Agent for National Grid's Infrastructure Delivery team.
You will be given a data quality rule definition and a database schema. Your task is to
generate a Spark SQL query that returns EXACTLY the rows that VIOLATE the rule.

Your SQL is the FINAL determination - there is no post-processing layer. Every row your
query returns will be flagged as a violation. Every row it does not return will be
considered compliant. Both precision and recall matter equally.

------------------------------------------------------------
TECHNICAL ENVIRONMENT
------------------------------------------------------------

SQL dialect: Databricks Spark SQL.
Table name: investment_projects.
All columns are stored as STRING - cast explicitly when needed.

Date storage: Date values may appear as DD/MM/YYYY text (e.g. '30/04/2018') or
ISO text (e.g. '2015-09-10'). Convert dates with to_date(column, 'dd/MM/yyyy')
before comparing them; the notebook normalizes those calls to tolerant Spark
parsing that also accepts ISO dates. Use current_date() for today's date; the
notebook pins it to the run_date widget during execution so report values are
reproducible.

Null sentinel: The value '01/01/1990' in any date column means NULL / not populated.
Treat it identically to NULL or empty string for all rules that care about missing data.

Mixed types: The CSV is loaded as STRING in Spark. Always use TRIM(CAST(col AS STRING))
before text comparisons. Use TRY_CAST(col AS DOUBLE) for numeric checks so non-numeric
values are handled explicitly rather than crashing the query.

Output columns: Always SELECT project_number as the first column and
project_current_phase as the second, followed by the column(s) being checked.
SELECT only - never generate DDL or DML.

------------------------------------------------------------
DATA QUALITY CHECK TYPES - BUSINESS INTENT & EXPECTED BEHAVIOUR
------------------------------------------------------------

The rule definition below will include a `check_type` field. Here is what each
check type means from a business perspective:

not_null
Business intent: Certain fields are mandatory for governance, reporting, and
lifecycle tracking. A project record missing any of these fields is incomplete.
Expected behaviour: A row violates this rule if ANY of the specified columns is
missing. "Missing" means the column is SQL NULL, contains only whitespace, or
holds the date sentinel '01/01/1990'. If the rule specifies `skip_phases`, exclude
those phases entirely - they are not subject to this check.

positive_value
Business intent: Financial values like sanction amounts must be meaningful positive
numbers. Zero or negative values indicate data entry errors or placeholder data that
distorts financial reporting.
Expected behaviour: A row violates this rule if the specified column holds a numeric
value that is zero or negative. If `skip_null: true`, rows where the column is missing
(NULL, empty, or sentinel) should be EXCLUDED - a separate completeness rule (DQ001)
owns null detection. Non-numeric values that cannot be parsed should also be counted
as violations.

unique
Business intent: Key identifiers must be unique to prevent double-counting in reports,
conflicting project data, and inaccurate KPI calculations.
Expected behaviour: Flag ALL rows that share a duplicated value - not just the second
occurrence. If `skip_null` is true, exclude null/blank values from the duplicate check
entirely (a null key is not a "duplicate").

value_in_list
Business intent: Categorical fields must use approved values to ensure consistent
classification across regions, consistent aggregation, and reliable governance reporting.
Expected behaviour: A row violates this rule if the column's value is NOT in the
allowed_values list. If `flag_null: true`, also flag rows where the column is NULL,
blank, or sentinel - these represent unmapped or legacy values that also violate the
standard.

active_gate_overdue
Business intent: National Grid projects follow a gated lifecycle (Gates A2 -> B -> C -> D -> E).
Each project phase has one "active" gate - the milestone the project is currently working
toward. If that gate's planned date is far in the past, the project data is stale and
may indicate an unreported delay or a failure to update milestones.
Expected behaviour: The rule provides a mapping from phase name to the active gate column.
For each project, look up its current phase, find the corresponding gate column, and check
whether that gate's date is more than `overdue_days` before today. Only flag a row if the
gate date is actually populated (not null/blank/sentinel) - missing dates are handled by
other rules. If `skip_phases` is specified, exclude those phases entirely.

progressive_gate_completeness
Business intent: As a project advances through phases, earlier gate milestones should
already have been recorded. A project in phase 4.4 (Execute) should have gate dates for
A2, B, C, and D. Missing earlier gates indicate incomplete data entry or process gaps.
Expected behaviour: The rule provides a mapping from phase name to the list of gate
columns that MUST be populated for that phase. A row violates this rule if the project
is in a given phase and ANY of the required gate columns is missing (NULL, blank, or
sentinel). If `skip_phases` is specified, exclude those phases.

completed_gate_not_future
Business intent: Gates that a project has already passed (completed gates) should have
milestone dates on or before today. A completed gate showing a future date is a data
quality error - it suggests the date was entered incorrectly or is a placeholder.
Expected behaviour: The rule provides a mapping from phase name to the list of gate
columns considered "completed" for that phase. A row violates this rule if ANY completed
gate has a populated date that falls AFTER today. Null/sentinel dates should be ignored
(not flagged) - missing dates are handled by other rules. If `skip_phases` is specified,
exclude those phases.

date_sequence
Business intent: Project gate milestones must follow a logical chronological order
(A2 before B, B before C, etc.). Out-of-order dates undermine schedule reporting,
governance tracking, and performance analysis.
Expected behaviour: The rule provides an ordered sequence of date columns. Check every
adjacent pair. A row violates this rule if ANY pair is out of order (earlier date > later
date, meaning equal dates are also violations). Only compare pairs where BOTH dates are
populated (not null/blank/sentinel). If even one adjacent pair is out of order, the row
fails. Do NOT filter by phase unless the rule explicitly specifies `skip_phases`.
CRITICAL: Each date column must be converted with to_date(column, 'dd/MM/yyyy') before
comparison. Apply the EXACT same date conversion to EVERY column in EVERY pair.
The notebook normalizes these calls to tolerant Spark parsing at execution time.

------------------------------------------------------------

{format_instructions}

DATABASE SCHEMA (dynamically extracted from the actual dataset):
{schema}

RULE TO CONVERT (from rules.yaml):
{rule}
"""

parser = PydanticOutputParser(pydantic_object=GeneratedSQL)

prompt = PromptTemplate(
    template=RULE_TO_SQL_TEMPLATE,
    input_variables=["rule", "schema"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

chain = prompt | llm | parser

print("Chain ready: PromptTemplate -> GPT-4.1 -> PydanticOutputParser")
print("Intent-based prompt - no SQL templates")


















# Cell 10 — Generate SQL for each rule
rule_lookup = {r["id"]: r for r in RULES}
generated_queries = {}

print("Generating SQL for each rule...\n")

for rule in RULES:
    try:
        rule_str = yaml.dump(rule, sort_keys=False, default_flow_style=False)
        result = chain.invoke({"rule": rule_str, "schema": TABLE_SCHEMA})
        generated_queries[rule["id"]] = result
        print(f"  ✓ {rule['id']:6} — {result.explanation[:70]}")
    except Exception as e:
        print(f"  ✗ {rule['id']:6} — ERROR: {type(e).__name__}: {str(e)[:80]}")

print(f"\nGenerated: {len(generated_queries)}/{len(RULES)}")


















# Cell 11 - Validate Spark SQL (test on a small Spark DataFrame)
validated_queries = {}

print("Validating SQL...\n")

investment_projects.limit(10).createOrReplaceTempView("investment_projects")

for rule_id, gen in generated_queries.items():
    try:
        sql_spark = normalize_spark_sql(gen.sql)
        spark.sql(sql_spark).limit(1).collect()
        validated_queries[rule_id] = gen
        print(f"  OK {rule_id:6}")
    except Exception as e:
        print(f"  ERR {rule_id:6} - {str(e)[:100]}")

investment_projects.createOrReplaceTempView("investment_projects")

print(f"\nValidated: {len(validated_queries)}/{len(generated_queries)}")















# Cell 12 - Execute Spark SQL on full dataset
execution_results = {}

print("Executing SQL on full dataset...\n")

for rule_id, gen in validated_queries.items():
    try:
        sql_spark = normalize_spark_sql(gen.sql)
        result_df = dedupe_spark_columns(spark.sql(sql_spark)).cache()
        row_count = result_df.count()
        execution_results[rule_id] = result_df
        print(f"  OK {rule_id:6} - {row_count:,} rows")
    except Exception as e:
        print(f"  ERR {rule_id:6} - {type(e).__name__}: {e}")

print(f"\nExecuted: {len(execution_results)}/{len(validated_queries)}")


















# Cell 13 - Show generated SQL
print("=" * 90)
print("  GENERATED SPARK SQL")
print("=" * 90)

for rule_id, gen in validated_queries.items():
    print(f"\n{'-' * 90}")
    print(f"{rule_id} - {gen.explanation}")
    print(f"failed_field: {gen.failed_field}")
    print(f"{'-' * 90}")
    print(normalize_spark_sql(gen.sql))













# Cell 14 - Assemble fail report (specific explanations per check_type)
OUT_DIR = get_widget("output_path", "dbfs:/tmp/dq_rules_engine_output").rstrip("/")
dbutils.fs.mkdirs(to_dbutils_path(OUT_DIR))

run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
run_id = get_widget("run_id", "") or str(uuid.uuid4())
timestamp = get_widget("run_timestamp_utc", "") or datetime.utcnow().isoformat()
RUN_DATE = get_run_date()
today_str = RUN_DATE.strftime("%Y-%m-%d")
SENTINEL = "01/01/1990"
IDENTIFIER_COLS = {"project_number", "project_current_phase"}


def is_missing_value(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def display_value(value) -> str:
    return "NULL" if is_missing_value(value) else str(value)


def parse_dq_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Could not parse date: {value}")


def gate_label(gate_col: str) -> str:
    return (
        gate_col
        .replace("project_gate_", "GATE ")
        .replace("_milestone_date", "")
        .upper()
    )


def write_single_csv(df, target_path: str) -> None:
    target = to_dbutils_path(target_path)
    tmp = f"{target}.tmp_{uuid.uuid4().hex}"
    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(tmp)
    )

    part_files = [
        f.path for f in dbutils.fs.ls(tmp)
        if f.name.startswith("part-") and f.name.endswith(".csv")
    ]
    if not part_files:
        raise FileNotFoundError(f"No CSV part file was written under {tmp}")

    try:
        dbutils.fs.rm(target, True)
    except Exception:
        pass

    dbutils.fs.mv(part_files[0], target)
    dbutils.fs.rm(tmp, True)


# Build SQL metadata dict
sql_output = {}
for rule_id, gen in validated_queries.items():
    sql_output[rule_id] = {
        "sql": gen.sql,
        "spark_sql": normalize_spark_sql(gen.sql),
        "explanation": gen.explanation,
        "failed_field": gen.failed_field,
    }

rule_lookup = {r["id"]: r for r in RULES}
check_types = {r["id"]: r.get("check_type", "") for r in RULES}
fail_records = []

for rule_id, result_df in execution_results.items():
    rule_meta = rule_lookup[rule_id]
    sql_meta = sql_output.get(rule_id, {})
    failed_field_str = sql_meta.get("failed_field", "unknown")
    candidate_fields = [f.strip() for f in failed_field_str.split(",")]
    check_cols = [c for c in result_df.columns if c not in IDENTIFIER_COLS]
    ct = check_types.get(rule_id, "")
    desc = rule_meta.get("description", "").strip()

    for row in result_df.toLocalIterator():
        row_dict = row.asDict(recursive=True)
        record_id = str(row_dict.get("project_number", "UNKNOWN"))
        phase = str(row_dict.get("project_current_phase", "UNKNOWN"))

        base = {
            "run_id": run_id,
            "timestamp": timestamp,
            "record_id": record_id,
            "phase": phase,
            "rule_id": rule_id,
            "rule_name": rule_meta["name"],
            "severity": rule_meta.get("severity", "medium"),
            "dimension": rule_meta.get("dimension", "unknown"),
        }

        # not_null: one record per null field
        if ct == "not_null":
            for field in check_cols:
                val = row_dict.get(field, None)
                val_str = display_value(val)
                is_null = is_missing_value(val)
                is_empty = isinstance(val, str) and val.strip() == ""
                is_sent = val_str == SENTINEL

                if is_null or is_empty or is_sent:
                    found = "NULL" if is_null else (
                        "blank/empty" if is_empty else f"sentinel date ({SENTINEL})"
                    )

                    fail_records.append({
                        **base,
                        "failed_field": field,
                        "failed_value": "NULL" if is_null else val_str,
                        "explanation": f"Column: {field} | Found: {found} | Expected: {desc}",
                    })

            continue

        # positive_value
        if ct == "positive_value":
            field = candidate_fields[0] if candidate_fields else check_cols[0]
            val = row_dict.get(field, None)
            val_str = display_value(val)

            fail_records.append({
                **base,
                "failed_field": field,
                "failed_value": val_str,
                "explanation": f"Column: {field} | Found: '{val_str}' | Expected: {desc}",
            })

            continue

        # unique
        if ct == "unique":
            field = candidate_fields[0] if candidate_fields else check_cols[0]
            val = row_dict.get(field, None)
            val_str = display_value(val)

            fail_records.append({
                **base,
                "failed_field": field,
                "failed_value": val_str,
                "explanation": f"Column: {field} | Found: '{val_str}' | Expected: {desc}",
            })

            continue

        # value_in_list
        if ct == "value_in_list":
            field = candidate_fields[0] if candidate_fields else check_cols[0]
            val = row_dict.get(field, None)
            val_str = display_value(val)
            found = "NULL" if is_missing_value(val) else f"'{val_str}'"

            fail_records.append({
                **base,
                "failed_field": field,
                "failed_value": val_str,
                "explanation": f"Column: {field} | Found: {found} | Expected: {desc}",
            })

            continue

        # active_gate_overdue: compute actual days overdue
        if ct == "active_gate_overdue":
            phase_gate_map = rule_meta.get("parameters", {}).get("phase_to_gate_map", {})
            overdue_days = rule_meta.get("parameters", {}).get("overdue_days", 180)
            active_gate_col = phase_gate_map.get(phase, None)

            if active_gate_col:
                val = row_dict.get(active_gate_col, None)

                if is_missing_value(val):
                    fallback_col = check_cols[0] if check_cols else None
                    val = row_dict.get(fallback_col, None) if fallback_col else None

                if not is_missing_value(val) and str(val).strip() not in ("", SENTINEL):
                    try:
                        gate_date = parse_dq_date(val)
                        days_diff = (RUN_DATE - gate_date).days
                        label = gate_label(active_gate_col)

                        fail_records.append({
                            **base,
                            "failed_field": active_gate_col,
                            "failed_value": str(val),
                            "explanation": (
                                f"Project is in phase '{phase}'. {label} milestone date ({val}) "
                                f"is {days_diff} days overdue, which exceeds the "
                                f"{overdue_days}-day tolerance."
                            ),
                        })

                    except (ValueError, TypeError):
                        fail_records.append({
                            **base,
                            "failed_field": active_gate_col,
                            "failed_value": str(val),
                            "explanation": (
                                f"Project is in phase '{phase}'. Could not parse gate date '{val}'."
                            ),
                        })

            continue

        # progressive_gate_completeness: list missing gates
        if ct == "progressive_gate_completeness":
            phase_map = rule_meta.get("parameters", {}).get("phase_to_required_gates", {})
            required_gates = phase_map.get(phase, candidate_fields)
            missing_gates = []

            for gate_col in required_gates:
                val = row_dict.get(gate_col, None)

                if is_missing_value(val) or str(val).strip() in ("", SENTINEL):
                    missing_gates.append(gate_label(gate_col))

            if missing_gates:
                fail_records.append({
                    **base,
                    "failed_field": ", ".join(missing_gates),
                    "failed_value": "NULL/missing",
                    "explanation": (
                        f"Missing required gates for phase '{phase}': "
                        f"{', '.join(missing_gates)}"
                    ),
                })

            continue

        # completed_gate_not_future: list future-dated gates
        if ct == "completed_gate_not_future":
            phase_map = rule_meta.get("parameters", {}).get("phase_to_completed_gates", {})
            completed_gates = phase_map.get(phase, candidate_fields)
            violations = []

            for gate_col in completed_gates:
                val = row_dict.get(gate_col, None)

                if is_missing_value(val) or str(val).strip() in ("", SENTINEL):
                    continue

                try:
                    gate_date = parse_dq_date(val)

                    if gate_date > RUN_DATE:
                        label = gate_label(gate_col)

                        violations.append(
                            f"{label} date ({gate_date}) is in the future but should be "
                            f"<= today ({today_str}) for phase '{phase}'"
                        )

                except (ValueError, TypeError):
                    pass

            if violations:
                fail_records.append({
                    **base,
                    "failed_field": "gate_dates",
                    "failed_value": "See explanation",
                    "explanation": "; ".join(violations),
                })

            continue

        # date_sequence: list out-of-order pairs
        if ct == "date_sequence":
            sequence = rule_meta.get("parameters", {}).get("sequence", candidate_fields)
            violations = []

            for i in range(len(sequence) - 1):
                col_a, col_b = sequence[i], sequence[i + 1]
                val_a, val_b = row_dict.get(col_a, None), row_dict.get(col_b, None)

                if is_missing_value(val_a) or is_missing_value(val_b):
                    continue

                sa, sb = str(val_a).strip(), str(val_b).strip()

                if sa in ("", SENTINEL) or sb in ("", SENTINEL):
                    continue

                try:
                    da = parse_dq_date(sa)
                    db = parse_dq_date(sb)

                    if da >= db:
                        label_a = gate_label(col_a)
                        label_b = gate_label(col_b)

                        violations.append(
                            f"{label_b} date ({db}) should be later than "
                            f"{label_a} date ({da})"
                        )

                except (ValueError, TypeError):
                    pass

            if violations:
                fail_records.append({
                    **base,
                    "failed_field": "date_sequence",
                    "failed_value": "See explanation",
                    "explanation": "; ".join(violations),
                })

            continue

        # Fallback
        primary_col = check_cols[0] if check_cols else failed_field_str
        val = row_dict.get(primary_col, None)
        val_str = display_value(val)

        fail_records.append({
            **base,
            "failed_field": failed_field_str,
            "failed_value": val_str,
            "explanation": sql_meta.get("explanation", ""),
        })


FAIL_REPORT_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("record_id", T.StringType(), True),
    T.StructField("phase", T.StringType(), True),
    T.StructField("rule_id", T.StringType(), True),
    T.StructField("rule_name", T.StringType(), True),
    T.StructField("severity", T.StringType(), True),
    T.StructField("dimension", T.StringType(), True),
    T.StructField("failed_field", T.StringType(), True),
    T.StructField("failed_value", T.StringType(), True),
    T.StructField("explanation", T.StringType(), True),
])

fail_report = spark.createDataFrame(fail_records, schema=FAIL_REPORT_SCHEMA)
fail_report = fail_report.orderBy(
    "rule_id",
    "record_id",
    "phase",
    "failed_field",
    "failed_value",
    "explanation",
)

# Rule summary
rule_summary = (
    fail_report
    .groupBy("rule_id", "rule_name", "severity", "dimension")
    .agg(F.count("record_id").alias("violation_count"))
    .orderBy("rule_id")
)

print(f"{'Rule':<8} {'Name':<50} {'Sev':<8} {'Count':>8}")
print("-" * 80)

summary_rows = rule_summary.collect()
for r in summary_rows:
    print(f"{r['rule_id']:<8} {r['rule_name']:<50} {r['severity']:<8} {r['violation_count']:>8,}")

print("-" * 80)
print(f"{'TOTAL':<66} {sum(int(r['violation_count']) for r in summary_rows):>8,}")

# Save everything
fail_report_path = path_join(OUT_DIR, f"fail_report_{run_ts}.csv")
rule_summary_path = path_join(OUT_DIR, f"rule_summary_{run_ts}.csv")
generated_sql_path = path_join(OUT_DIR, f"generated_sql_{run_ts}.yaml")
rules_path = path_join(OUT_DIR, f"rules_{run_ts}.yaml")
execution_results_dir = path_join(OUT_DIR, f"execution_results_{run_ts}")
latest_run_path = path_join(OUT_DIR, "latest_run.yaml")

write_single_csv(fail_report, fail_report_path)
write_single_csv(rule_summary, rule_summary_path)

write_text_file(generated_sql_path, yaml.dump(sql_output, sort_keys=False))
write_text_file(rules_path, yaml.dump({"rules": RULES}, sort_keys=False))

for rule_id, result_df in execution_results.items():
    (
        result_df
        .write
        .mode("overwrite")
        .parquet(path_join(execution_results_dir, rule_id))
    )

manifest = {
    "run_ts": run_ts,
    "run_id": run_id,
    "timestamp": timestamp,
    "run_date": today_str,
    "model": AZURE_DEPLOYMENT,
    "output_path": OUT_DIR,
}

write_text_file(latest_run_path, yaml.dump(manifest, sort_keys=False))

print(f"\nSaved to {OUT_DIR}/")
print(f"   fail_report_{run_ts}.csv         ({fail_report.count():,} violations)")
print(f"   rule_summary_{run_ts}.csv        ({rule_summary.count()} rules)")
print(f"   generated_sql_{run_ts}.yaml")
print(f"   execution_results_{run_ts}/      (Parquet by rule)")
print(f"   rules_{run_ts}.yaml")

















