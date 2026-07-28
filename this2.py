import yaml

RULES_PATH = "file:/Workspace/POC DQ Agent/dq-agent-poc/rules.yaml"

def read_text_file_any(path):
    if path.startswith("file:/Workspace/"):
        local_path = path.replace("file:", "")
        with open(local_path, "r", encoding="utf-8") as f:
            return f.read()
    return "\n".join(row.value for row in spark.read.text(path).collect())

RULES_CONFIG = yaml.safe_load(read_text_file_any(RULES_PATH))
RULES = RULES_CONFIG["rules"]

print(f"Loaded {len(RULES)} rules from {RULES_PATH}")








from pyspark.sql import functions as F
from functools import reduce

SENTINEL = "01/01/1990"

def q(col_name):
    return "`" + col_name.replace("`", "``") + "`"

date_cols = [
    c for c in investment_projects.columns
    if "date" in c.lower() or "milestone" in c.lower()
]

print(f"Found {len(date_cols)} possible date columns:")
for c in date_cols:
    print(" -", c)

profiles = []

for c in date_cols:
    s = F.trim(F.col(q(c)).cast("string"))

    profile = (
        investment_projects
        .select(s.alias("v"))
        .agg(
            F.lit(c).alias("column_name"),
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("v").isNull(), 1).otherwise(0)).alias("null_count"),
            F.sum(F.when(F.col("v") == "", 1).otherwise(0)).alias("blank_count"),
            F.sum(F.when(F.col("v") == SENTINEL, 1).otherwise(0)).alias("sentinel_count"),
            F.sum(F.when(F.expr("try_to_date(v, 'dd/MM/yyyy') is not null"), 1).otherwise(0)).alias("dd_MM_yyyy_count"),
            F.sum(F.when(F.expr("try_to_date(v, 'yyyy-MM-dd') is not null"), 1).otherwise(0)).alias("yyyy_MM_dd_count"),
            F.sum(F.when(F.expr("try_to_date(v, 'dd-MM-yyyy') is not null"), 1).otherwise(0)).alias("dd_MM_yyyy_dash_count"),
            F.sum(F.when(F.expr("try_to_date(v, 'yyyy/MM/dd') is not null"), 1).otherwise(0)).alias("yyyy_MM_dd_slash_count"),
            F.countDistinct("v").alias("distinct_values"),
        )
        .withColumn(
            "unparsed_nonblank_count",
            F.col("total_rows")
            - F.col("null_count")
            - F.col("blank_count")
            - F.col("dd_MM_yyyy_count")
            - F.col("yyyy_MM_dd_count")
            - F.col("dd_MM_yyyy_dash_count")
            - F.col("yyyy_MM_dd_slash_count")
        )
    )

    profiles.append(profile)

date_profile = reduce(lambda a, b: a.unionByName(b), profiles)
display(date_profile.orderBy("column_name"))



for c in date_cols:
    print(f"\n{c}")
    (
        investment_projects
        .select(F.trim(F.col(q(c)).cast("string")).alias("v"))
        .where("v is not null and v <> ''")
        .groupBy(
            F.when(F.col("v").rlike(r"^\d{2}/\d{2}/\d{4}$"), "dd/MM/yyyy")
             .when(F.col("v").rlike(r"^\d{4}-\d{2}-\d{2}$"), "yyyy-MM-dd")
             .when(F.col("v").rlike(r"^\d{2}-\d{2}-\d{4}$"), "dd-MM-yyyy")
             .when(F.col("v").rlike(r"^\d{4}/\d{2}/\d{2}$"), "yyyy/MM/dd")
             .otherwise("other")
             .alias("detected_format")
        )
        .count()
        .orderBy("detected_format")
        .show(truncate=False)
    )










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

Date storage: Profiling of the Databricks dataset shows date values are stored
primarily as ISO text in yyyy-MM-dd format (e.g. '2015-09-10'), with a small
number of other nonblank values. Convert dates with to_date(column, 'yyyy-MM-dd')
before comparing them; the notebook normalizes strict date calls to tolerant
Spark parsing with fallbacks. Use current_date() for today's date; the notebook
pins it to the run_date widget during execution so report values are reproducible.

Null sentinel: The value '01/01/1990' in any date column means NULL / not populated.
Treat it identically to NULL or empty string for all rules that care about missing data.

Mixed types: The CSV is loaded as STRING in Spark. Always use TRIM(CAST(col AS STRING))
before text comparisons. Use TRY_CAST(col AS DOUBLE) for numeric checks so non-numeric
values are handled explicitly rather than crashing the query.

Output columns: Always SELECT project_number as the first column and
project_current_phase as the second, followed by the column(s) being checked.
SELECT only - never generate DDL or DML.

CRITICAL SQL FORMATTING RULES:
- The sql field must contain executable Databricks Spark SQL only.
- Do not include SQL comments such as -- or /* */ anywhere in the SQL.
- Do not include explanatory prose inside the SQL.
- Do not include markdown fences.
- Do not leave any parenthesis, CASE expression, subquery, or WHERE clause incomplete.
- For complex OR conditions, wrap each OR branch in parentheses.

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
CRITICAL: Each date column must be converted with to_date(column, 'yyyy-MM-dd') before
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




















# Cell 10 - Generate SQL for each rule
import json

rule_lookup = {r["id"]: r for r in RULES}
generated_queries = {}
raw_generated_outputs = {}


def extract_json_object(text: str) -> str:
    """Extract the first complete JSON object from an LLM response."""
    text = str(text).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)

    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in model response")

    depth = 0
    quote = None
    escape = False

    for i in range(start, len(text)):
        ch = text[i]

        if quote:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                quote = None
            continue

        if ch in ("'", '"'):
            quote = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]

    raise ValueError("JSON object was not closed in model response")


def parse_generated_sql_response(text: str) -> GeneratedSQL:
    try:
        return parser.parse(text)
    except Exception:
        json_text = extract_json_object(text)
        data = json.loads(json_text)
        return GeneratedSQL(**data)


def repair_generated_sql_response(rule_id: str, bad_text: str, parse_error: Exception) -> GeneratedSQL:
    repair_prompt = f"""
You returned a malformed response for rule {rule_id}.

Convert the response below into one valid JSON object matching exactly this schema:
{{
  "rule_id": "{rule_id}",
  "sql": "Spark SQL SELECT query returning violating rows",
  "explanation": "brief explanation",
  "failed_field": "primary checked field or comma-separated fields"
}}

Rules:
- Return JSON only.
- Keep the SQL logic the same.
- Escape all quotes and newlines inside JSON strings.
- Do not wrap the JSON in markdown.

Parse error:
{type(parse_error).__name__}: {str(parse_error)[:1000]}

Malformed response:
{bad_text[:8000]}
"""
    repaired = llm.invoke(repair_prompt)
    repaired_text = repaired.content if hasattr(repaired, "content") else str(repaired)
    return parse_generated_sql_response(repaired_text)


raw_chain = prompt | llm

print("Generating SQL for each rule...\n")

for rule in RULES:
    try:
        rule_str = yaml.dump(rule, sort_keys=False, default_flow_style=False)
        raw_result = raw_chain.invoke({"rule": rule_str, "schema": TABLE_SCHEMA})
        raw_text = raw_result.content if hasattr(raw_result, "content") else str(raw_result)
        raw_generated_outputs[rule["id"]] = raw_text

        try:
            result = parse_generated_sql_response(raw_text)
        except Exception as parse_error:
            print(f"  retry {rule['id']:6} - repairing malformed structured output")
            result = repair_generated_sql_response(rule["id"], raw_text, parse_error)

        generated_queries[rule["id"]] = result
        print(f"  OK {rule['id']:6} - {result.explanation[:70]}")
    except Exception as e:
        print(f"  ERR {rule['id']:6} - ERROR: {type(e).__name__}: {str(e)[:120]}")

print(f"\nGenerated: {len(generated_queries)}/{len(RULES)}")




















# Cell 11 - Validate Spark SQL (test on a small Spark DataFrame)
validated_queries = {}
validation_errors = {}


def print_sql_error_context(rule_id: str, sql_spark: str, error: Exception) -> None:
    msg = str(error)
    print(f"\n--- SQL debug for {rule_id} ---")
    print(f"SQL length: {len(sql_spark)}")
    pos_match = re.search(r"pos\s+(\d+)", msg, flags=re.IGNORECASE)
    if pos_match:
        pos = int(pos_match.group(1))
        start = max(0, pos - 500)
        end = min(len(sql_spark), pos + 500)
        print(f"Spark error position: {pos}")
        print(sql_spark[start:end])
    else:
        print(sql_spark[:4000])
    print("--- end SQL debug ---\n")


def validate_spark_sql(sql: str) -> str:
    sql_spark = normalize_spark_sql(sql)
    spark.sql(sql_spark).limit(1).collect()
    return sql_spark


def repair_generated_sql_for_spark(rule_id: str, gen: GeneratedSQL, spark_error: Exception) -> GeneratedSQL:
    rule_meta = rule_lookup.get(rule_id, {"id": rule_id})
    rule_str = yaml.dump(rule_meta, sort_keys=False, default_flow_style=False)

    repair_prompt = f"""
The Spark SQL generated for rule {rule_id} failed Databricks validation.

Return one valid JSON object matching exactly this schema:
{{
  "rule_id": "{rule_id}",
  "sql": "corrected Databricks Spark SQL SELECT query returning violating rows",
  "explanation": "brief explanation",
  "failed_field": "primary checked field or comma-separated fields"
}}

Rules:
- Return JSON only.
- SELECT only. No DDL, no DML, no markdown.
- Keep the same data-quality business logic.
- Always select project_number first and project_current_phase second.
- Use table name investment_projects.
- Dates are primarily yyyy-MM-dd strings. Use to_date(column, 'yyyy-MM-dd') for date comparisons.
- The sql value must be executable SQL only.
- Do not include SQL comments such as -- or /* */ anywhere in the SQL.
- Do not include explanatory prose inside the SQL.
- If the previous SQL contained comments, prose, or was incomplete, regenerate the SQL from scratch.
- For complex OR conditions, wrap each OR branch in parentheses.
- Ensure all parentheses, CASE expressions, subqueries, and WHERE predicates are complete.

Rule YAML:
{rule_str}

Original explanation:
{gen.explanation}

Original failed_field:
{gen.failed_field}

Databricks Spark parser/analyzer error:
{type(spark_error).__name__}: {str(spark_error)[:4000]}

Invalid SQL:
{gen.sql[:12000]}
"""
    repaired = llm.invoke(repair_prompt)
    repaired_text = repaired.content if hasattr(repaired, "content") else str(repaired)
    repaired_gen = parse_generated_sql_response(repaired_text)

    if repaired_gen.rule_id != rule_id:
        repaired_gen.rule_id = rule_id

    return repaired_gen

print("Validating SQL...\n")

investment_projects.limit(10).createOrReplaceTempView("investment_projects")

for rule_id, gen in generated_queries.items():
    try:
        validate_spark_sql(gen.sql)
        validated_queries[rule_id] = gen
        print(f"  OK {rule_id:6}")
    except Exception as e:
        current_gen = gen
        current_error = e
        repaired = False

        failed_sql = normalize_spark_sql(current_gen.sql)
        print_sql_error_context(rule_id, failed_sql, current_error)

        for attempt in range(1, 3):
            print(f"  retry {rule_id:6} - LLM Spark SQL repair attempt {attempt}")
            repaired_gen = None
            try:
                repaired_gen = repair_generated_sql_for_spark(rule_id, current_gen, current_error)
                validate_spark_sql(repaired_gen.sql)
                generated_queries[rule_id] = repaired_gen
                validated_queries[rule_id] = repaired_gen
                repaired = True
                print(f"  OK {rule_id:6} - repaired by LLM")
                break
            except Exception as retry_error:
                current_gen = repaired_gen if repaired_gen is not None else current_gen
                current_error = retry_error
                try:
                    failed_sql = normalize_spark_sql(current_gen.sql)
                    print_sql_error_context(rule_id, failed_sql, current_error)
                except Exception:
                    pass

        if not repaired:
            validation_errors[rule_id] = {
                "original_error": str(e),
                "repair_error": str(current_error),
                "sql": current_gen.sql,
            }
            print(f"  ERR {rule_id:6} - {str(current_error)[:160]}")

investment_projects.createOrReplaceTempView("investment_projects")

print(f"\nValidated: {len(validated_queries)}/{len(generated_queries)}")

if validation_errors:
    print("\nRules still failing validation:")
    for rule_id, info in validation_errors.items():
        print(f"  {rule_id}: {info['repair_error'][:300]}")


















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
































# =========================
# CONFIG
# =========================

OUTPUT_PATH = "file:/Workspace/POC DQ Agent/dq-agent-poc/output"
FAIL_REPORT_FILE = "fail_report_20260728_092523.csv"
RULE_SUMMARY_FILE = "rule_summary_20260728_092523.csv"

FAIL_REPORT_PATH = f"{OUTPUT_PATH.rstrip('/')}/{FAIL_REPORT_FILE}"
RULE_SUMMARY_PATH = f"{OUTPUT_PATH.rstrip('/')}/{RULE_SUMMARY_FILE}"








%pip install pyyaml
import yaml
from functools import reduce
from datetime import datetime
from pyspark.sql import functions as F, types as T

# =========================
# PATHS
# =========================

RULES_PATH = "file:/Workspace/POC DQ Agent/dq-agent-poc/config/rules.yaml"
INVESTMENT_PROJECTS_PATH = "file:/Workspace/POC DQ Agent/dq-agent-poc/data/oppm/investment_projects 1.csv"
OUTPUT_PATH = "file:/Workspace/POC DQ Agent/dq-agent-poc/output"

FAIL_REPORT_FILE = "fail_report_20260728_092523.csv"
RULE_SUMMARY_FILE = "rule_summary_20260728_092523.csv"

FAIL_REPORT_PATH = f"{OUTPUT_PATH.rstrip('/')}/{FAIL_REPORT_FILE}"
RULE_SUMMARY_PATH = f"{OUTPUT_PATH.rstrip('/')}/{RULE_SUMMARY_FILE}"

# =========================
# LOAD INPUTS
# =========================

def localize_path(path):
    if path.startswith("file:"):
        return path.replace("file:", "", 1)
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path.replace("dbfs:/", "", 1).lstrip("/")
    return path

def read_text_file_any(path):
    try:
        with open(localize_path(path), "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "\n".join(row.value for row in spark.read.text(path).collect())

RULES_CONFIG = yaml.safe_load(read_text_file_any(RULES_PATH))
RULES = RULES_CONFIG["rules"]
print(f"Loaded {len(RULES)} rules from {RULES_PATH}")

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
    .load(INVESTMENT_PROJECTS_PATH)
)

for c in investment_projects.columns:
    cleaned = c.lstrip("\ufeff")
    if cleaned != c:
        investment_projects = investment_projects.withColumnRenamed(c, cleaned)

investment_projects = investment_projects.cache()
print(f"Loaded investment_projects: {investment_projects.count():,} rows x {len(investment_projects.columns)} cols")

actual_report_raw = (
    spark.read
    .option("header", "true")
    .csv(FAIL_REPORT_PATH)
)
print(f"Loaded generated fail report: {actual_report_raw.count():,} rows")

try:
    actual_rule_summary_raw = (
        spark.read
        .option("header", "true")
        .csv(RULE_SUMMARY_PATH)
    )
    print(f"Loaded rule summary: {actual_rule_summary_raw.count():,} rows")
except Exception as e:
    actual_rule_summary_raw = None
    print("Rule summary not loaded:", str(e)[:200])

# =========================
# HELPERS
# =========================

SENTINELS = ["01/01/1990", "1990-01-01"]
ID_COL = "project_number"
PHASE_COL = "project_current_phase"
AUDIT_RUN_DATE_STR = "2026-07-28"
AUDIT_RUN_DATE_COL = F.to_date(F.lit(AUDIT_RUN_DATE_STR))

def q(c):
    return "`" + c.replace("`", "``") + "`"

def col(c):
    return F.col(q(c))

def clean(c):
    return F.trim(col(c).cast("string"))

def as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    return [x.strip() for x in str(v).split(",") if x.strip()]

def params(rule):
    return rule.get("parameters", {}) or {}

def get_rule_cols(rule):
    p = params(rule)
    for source in [rule, p]:
        for k in ["columns", "fields", "field", "column", "target_columns", "target_column", "mandatory_fields"]:
            if source.get(k) is not None:
                return as_list(source.get(k))
    return []

def get_allowed_values(rule):
    p = params(rule)
    for source in [rule, p]:
        for k in ["allowed_values", "values", "valid_values", "list"]:
            if source.get(k) is not None:
                return as_list(source.get(k))
    return []

def get_skip_phases(rule):
    p = params(rule)
    return as_list(p.get("skip_phases") or rule.get("skip_phases"))

def apply_skip(df, rule):
    skip = get_skip_phases(rule)
    if skip:
        return df.where(~clean(PHASE_COL).isin(skip))
    return df

def is_missing(c):
    s = clean(c)
    return col(c).isNull() | (s == "") | s.isin(SENTINELS)

def value_str(c):
    return F.when(col(c).isNull(), F.lit("NULL")).otherwise(clean(c))

def try_double(c):
    return F.expr(f"try_cast({q(c)} as double)")

def dq_date(c):
    x = f"substring(trim(cast({q(c)} as string)), 1, 10)"
    return F.expr(
        f"coalesce("
        f"try_to_date({x}, 'yyyy-MM-dd'), "
        f"try_to_date({x}, 'dd/MM/yyyy'), "
        f"try_to_date({x}, 'dd-MM-yyyy'), "
        f"try_to_date({x}, 'yyyy/MM/dd'))"
    )

def is_populated_date(c):
    return (~is_missing(c)) & dq_date(c).isNotNull()

def or_all(exprs):
    return reduce(lambda a, b: a | b, exprs) if exprs else F.lit(False)

def gate_label(c):
    return c.replace("project_gate_", "GATE ").replace("_milestone_date", "").upper()

GT_SCHEMA = T.StructType([
    T.StructField("rule_id", T.StringType()),
    T.StructField("record_id", T.StringType()),
    T.StructField("phase", T.StringType()),
    T.StructField("failed_field", T.StringType()),
    T.StructField("failed_value", T.StringType()),
])

def mk(df, rule, failed_field, failed_value):
    return df.select(
        F.lit(rule["id"]).alias("rule_id"),
        F.coalesce(col(ID_COL).cast("string"), F.lit("UNKNOWN")).alias("record_id"),
        F.coalesce(col(PHASE_COL).cast("string"), F.lit("UNKNOWN")).alias("phase"),
        failed_field.cast("string").alias("failed_field"),
        failed_value.cast("string").alias("failed_value"),
    )

# =========================
# GROUND TRUTH PER RULE
# =========================

def gt_for_rule(rule):
    ct = rule.get("check_type", "")
    df = apply_skip(investment_projects, rule)
    p = params(rule)

    if ct == "not_null":
        return [
            mk(df.where(is_missing(c)), rule, F.lit(c), value_str(c))
            for c in get_rule_cols(rule)
        ]

    if ct == "positive_value":
        c = get_rule_cols(rule)[0]
        skip_null = bool(p.get("skip_null", rule.get("skip_null", False)))
        cond = try_double(c).isNull() | (try_double(c) <= 0)
        cond = ((~is_missing(c)) & cond) if skip_null else (is_missing(c) | cond)
        return [mk(df.where(cond), rule, F.lit(c), value_str(c))]

    if ct == "unique":
        c = get_rule_cols(rule)[0]
        skip_null = bool(p.get("skip_null", rule.get("skip_null", False)))
        base = df.withColumn("_key", clean(c))
        if skip_null:
            base = base.where(~is_missing(c))
        dups = base.groupBy("_key").count().where(F.col("count") > 1).select("_key")
        return [mk(base.join(dups, "_key", "inner"), rule, F.lit(c), F.col("_key"))]

    if ct == "value_in_list":
        c = get_rule_cols(rule)[0]
        allowed = get_allowed_values(rule)
        flag_null = bool(p.get("flag_null", rule.get("flag_null", False)))
        cond = ~clean(c).isin(allowed)
        cond = (cond | is_missing(c)) if flag_null else (cond & (~is_missing(c)))
        return [mk(df.where(cond), rule, F.lit(c), value_str(c))]

    if ct == "active_gate_overdue":
        parts = []
        phase_map = p.get("phase_to_gate_map", {})
        overdue_days = int(p.get("overdue_days", 180))
        for phase, gate_col in phase_map.items():
            cond = (
                (clean(PHASE_COL) == phase)
                & is_populated_date(gate_col)
                & (F.datediff(AUDIT_RUN_DATE_COL, dq_date(gate_col)) > overdue_days)
            )
            parts.append(mk(df.where(cond), rule, F.lit(gate_col), value_str(gate_col)))
        return parts

    if ct == "progressive_gate_completeness":
        parts = []
        phase_map = p.get("phase_to_required_gates", {})
        for phase, gates in phase_map.items():
            gates = as_list(gates)
            cond = (clean(PHASE_COL) == phase) & or_all([is_missing(g) for g in gates])
            missing_labels = [F.when(is_missing(g), F.lit(gate_label(g))) for g in gates]
            parts.append(mk(df.where(cond), rule, F.concat_ws(", ", *missing_labels), F.lit("NULL/missing")))
        return parts

    if ct == "completed_gate_not_future":
        parts = []
        phase_map = p.get("phase_to_completed_gates", {})
        for phase, gates in phase_map.items():
            gates = as_list(gates)
            cond = (clean(PHASE_COL) == phase) & or_all([
                is_populated_date(g) & (dq_date(g) > AUDIT_RUN_DATE_COL)
                for g in gates
            ])
            parts.append(mk(df.where(cond), rule, F.lit("gate_dates"), F.lit("See explanation")))
        return parts

    if ct == "date_sequence":
        seq = as_list(p.get("sequence") or rule.get("sequence") or get_rule_cols(rule))
        pair_conds = [
            is_populated_date(a) & is_populated_date(b) & (dq_date(a) >= dq_date(b))
            for a, b in zip(seq, seq[1:])
        ]
        return [mk(df.where(or_all(pair_conds)), rule, F.lit("date_sequence"), F.lit("See explanation"))]

    print(f"WARNING: no ground-truth logic for {rule['id']} check_type={ct}")
    return []

# =========================
# BUILD REPORTS
# =========================

gt_parts = []
for rule in RULES:
    gt_parts.extend(gt_for_rule(rule))

ground_truth_report = reduce(lambda a, b: a.unionByName(b), gt_parts) if gt_parts else spark.createDataFrame([], GT_SCHEMA)

actual_report = actual_report_raw.select(
    F.col("rule_id").cast("string").alias("rule_id"),
    F.col("record_id").cast("string").alias("record_id"),
    F.col("phase").cast("string").alias("phase"),
    F.col("failed_field").cast("string").alias("failed_field"),
    F.col("failed_value").cast("string").alias("failed_value"),
)

rules_df = spark.createDataFrame(
    [(r["id"], r.get("name", ""), r.get("check_type", "")) for r in RULES],
    ["rule_id", "rule_name", "check_type"]
)

# =========================
# COMPARISON
# =========================

gt_summary = ground_truth_report.groupBy("rule_id").agg(
    F.count("*").alias("gt_fail_report_count"),
    F.countDistinct("record_id").alias("gt_distinct_records")
)

actual_summary = actual_report.groupBy("rule_id").agg(
    F.count("*").alias("actual_fail_report_count"),
    F.countDistinct("record_id").alias("actual_distinct_records")
)

comparison = (
    rules_df
    .join(gt_summary, "rule_id", "left")
    .join(actual_summary, "rule_id", "left")
    .fillna(0)
    .withColumn("count_delta", F.col("actual_fail_report_count") - F.col("gt_fail_report_count"))
    .withColumn("record_delta", F.col("actual_distinct_records") - F.col("gt_distinct_records"))
    .withColumn("count_match", F.col("count_delta") == 0)
    .orderBy("rule_id")
)

print("GROUND TRUTH VS GENERATED FAIL REPORT")
display(comparison)

gt_record_keys = ground_truth_report.select("rule_id", "record_id").distinct()
actual_record_keys = actual_report.select("rule_id", "record_id").distinct()

false_negative_records = gt_record_keys.join(actual_record_keys, ["rule_id", "record_id"], "left_anti")
false_positive_records = actual_record_keys.join(gt_record_keys, ["rule_id", "record_id"], "left_anti")

mismatch_summary = (
    rules_df
    .join(false_negative_records.groupBy("rule_id").count().withColumnRenamed("count", "false_negative_records"), "rule_id", "left")
    .join(false_positive_records.groupBy("rule_id").count().withColumnRenamed("count", "false_positive_records"), "rule_id", "left")
    .fillna(0)
    .orderBy("rule_id")
)

print("FALSE POSITIVE / FALSE NEGATIVE RECORD IDS")
display(mismatch_summary)

if actual_rule_summary_raw is not None:
    dq_summary_check = (
        comparison
        .join(
            actual_rule_summary_raw.select(
                F.col("rule_id").cast("string"),
                F.col("violation_count").cast("long").alias("dq_summary_count")
            ),
            "rule_id",
            "left"
        )
        .withColumn("dq_summary_matches_ground_truth", F.col("dq_summary_count") == F.col("gt_fail_report_count"))
        .withColumn("dq_summary_matches_generated_report", F.col("dq_summary_count") == F.col("actual_fail_report_count"))
        .orderBy("rule_id")
    )
    print("DQ SUMMARY CHECK")
    display(dq_summary_check)

print("SAMPLE FALSE NEGATIVES")
display(false_negative_records.orderBy("rule_id", "record_id").limit(100))

print("SAMPLE FALSE POSITIVES")
display(false_positive_records.orderBy("rule_id", "record_id").limit(100))








