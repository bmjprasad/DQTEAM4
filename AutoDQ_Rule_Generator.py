# Databricks notebook source
# MAGIC %md
# MAGIC ## Auto DQ Rule Generator (Metadata- and Profile-driven)
# MAGIC 
# MAGIC Purpose:
# MAGIC - Generate candidate Data Quality (DQ) rules automatically using table metadata and profile statistics.
# MAGIC - Persist rule suggestions into a Delta table `dq_rule_generator` with confidence, support, and provenance stats.
# MAGIC 
# MAGIC Inputs (via widgets):
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema/Database name
# MAGIC - `table`: Table name
# MAGIC - `sampleFraction`: Fraction of data to profile (0.0-1.0)
# MAGIC - `minConfidence`: Minimum confidence to persist a rule suggestion (not enforced, informational)
# MAGIC - `coverageThreshold`: Minimum data coverage for a rule to be considered
# MAGIC 
# MAGIC Output:
# MAGIC - Delta table `<catalog>.dq.dq_rule_generator` containing rule suggestions for the specified table/columns

# COMMAND ----------
# Widgets and imports
from pyspark.sql import functions as F
from pyspark.sql import types as T
import hashlib, json, datetime

try:
  dbutils.widgets.text("catalog", "main")
  dbutils.widgets.text("schema", "default")
  dbutils.widgets.text("table", "")
  dbutils.widgets.text("sampleFraction", "0.2")
  dbutils.widgets.text("minConfidence", "0.8")
  dbutils.widgets.text("coverageThreshold", "0.95")
except NameError:
  pass

# COMMAND ----------
# Parameters and DDL
catalog = dbutils.widgets.get("catalog") if 'dbutils' in globals() else "main"
schema = dbutils.widgets.get("schema") if 'dbutils' in globals() else "default"
table = dbutils.widgets.get("table") if 'dbutils' in globals() else ""
sample_fraction = float(dbutils.widgets.get("sampleFraction")) if 'dbutils' in globals() else 0.2
min_confidence = float(dbutils.widgets.get("minConfidence")) if 'dbutils' in globals() else 0.8
coverage_threshold = float(dbutils.widgets.get("coverageThreshold")) if 'dbutils' in globals() else 0.95

full_name = f"{catalog}.{schema}.{table}" if table else None

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dq")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.dq.dq_rule_generator (
  catalog_name STRING,
  schema_name STRING,
  table_name STRING,
  column_name STRING,
  rule_type STRING,
  rule_expression STRING,
  confidence DOUBLE,
  coverage DOUBLE,
  support LONG,
  row_count LONG,
  profile_json STRING,
  provenance STRING,
  created_at TIMESTAMP,
  rule_id STRING
) USING DELTA
PARTITIONED BY (table_name)
TBLPROPERTIES (delta.logRetentionDuration='interval 30 days', delta.minReaderVersion=2)
""")
print(f"Params set for {full_name}")

# COMMAND ----------
# Helper functions

def compute_quantiles_numeric(dfin, col, probs=(0.01, 0.5, 0.99)):
  try:
    q = dfin.approxQuantile(col, list(probs), 0.01)
    return {f"q{int(p*100)}": v for p, v in zip(probs, q)}
  except Exception:
    return {}


def compute_quantiles_length(dfin, col, probs=(0.01, 0.5, 0.99)):
  try:
    tmp = dfin.select(F.length(F.col(col)).alias("_len")).na.drop()
    q = tmp.approxQuantile("_len", list(probs), 0.01)
    return {f"len_q{int(p*100)}": v for p, v in zip(probs, q)}
  except Exception:
    return {}


def safe_float(x):
  try:
    return float(x)
  except Exception:
    return None


def sha256_hex(s: str) -> str:
  return hashlib.sha256(s.encode("utf-8")).hexdigest()

# COMMAND ----------
# Load table and sample
assert full_name, "Please set the 'table' widget."
df = spark.table(full_name)
row_count = df.count()
if sample_fraction < 1.0:
  dfp = df.sample(False, sample_fraction, seed=42)
  provenance = f"sampled={sample_fraction}"
else:
  dfp = df
  provenance = "full"
profile_ts = datetime.datetime.utcnow()

cols = [f.name for f in df.schema.fields]
name_to_type = {f.name: f.dataType for f in df.schema.fields}

print(f"Profiling {full_name} with {row_count} rows, columns={len(cols)} ({provenance})")

# COMMAND ----------
# Profile and generate rules
rule_rows = []

for col in cols:
  dtype = name_to_type[col]
  non_null = dfp.filter(F.col(col).isNotNull()).count()
  approx_distinct = dfp.agg(F.approx_count_distinct(F.col(col))).first()[0]
  support = non_null
  coverage = (non_null / row_count) if row_count > 0 else 0.0

  profile = {
    "non_null": non_null,
    "approx_distinct": approx_distinct,
    "coverage": coverage,
  }

  # NOT NULL
  if coverage >= coverage_threshold:
    rule_expr = f"{col} IS NOT NULL"
    rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|NOT_NULL|{rule_expr}")
    rule_rows.append((catalog, schema, table, col, "NOT_NULL", rule_expr, coverage, coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))

  # UNIQUE (approx)
  if non_null > 0:
    uniqueness = (approx_distinct / non_null) if non_null else 0.0
    profile["approx_uniqueness"] = uniqueness
    if uniqueness >= 0.999 and coverage >= coverage_threshold:
      rule_expr = f"IS_UNIQUE({col})"
      rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|UNIQUE|{rule_expr}")
      rule_rows.append((catalog, schema, table, col, "UNIQUE", rule_expr, uniqueness, coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))

  # Numeric/date ranges and non-negative
  from pyspark.sql.types import NumericType, DateType, TimestampType
  if isinstance(dtype, NumericType) or isinstance(dtype, DateType) or isinstance(dtype, TimestampType):
    qs = compute_quantiles_numeric(dfp.na.drop(subset=[col]), col)
    profile.update(qs)
    q01 = safe_float(qs.get("q1"))
    q99 = safe_float(qs.get("q99"))
    if q01 is not None and q99 is not None:
      within = dfp.filter((F.col(col) >= F.lit(q01)) & (F.col(col) <= F.lit(q99))).count()
      within_cov = (within / non_null) if non_null else 0.0
      if within_cov >= coverage_threshold:
        rule_expr = f"{col} BETWEEN {q01} AND {q99}"
        rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|RANGE|{rule_expr}")
        rule_rows.append((catalog, schema, table, col, "RANGE", rule_expr, within_cov, coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))

    # Non-negative
    try:
      min_val = dfp.agg(F.min(F.col(col))).first()[0]
      if min_val is not None and safe_float(min_val) is not None and min_val >= 0:
        rule_expr = f"{col} >= 0"
        rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|NON_NEGATIVE|{rule_expr}")
        rule_rows.append((catalog, schema, table, col, "NON_NEGATIVE", rule_expr, 1.0, coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))
    except Exception:
      pass

  # String patterns and lengths
  from pyspark.sql.types import StringType
  if isinstance(dtype, StringType):
    lqs = compute_quantiles_length(dfp, col)
    profile.update(lqs)
    len_q01 = safe_float(lqs.get("len_q1"))
    len_q99 = safe_float(lqs.get("len_q99"))
    if len_q01 is not None and len_q99 is not None:
      within_len = dfp.filter((F.length(F.col(col)) >= F.lit(int(len_q01))) & (F.length(F.col(col)) <= F.lit(int(len_q99)))).count()
      within_len_cov = (within_len / non_null) if non_null else 0.0
      if within_len_cov >= coverage_threshold:
        rule_expr = f"length({col}) BETWEEN {int(len_q01)} AND {int(len_q99)}"
        rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|LENGTH_RANGE|{rule_expr}")
        rule_rows.append((catalog, schema, table, col, "LENGTH_RANGE", rule_expr, within_len_cov, coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))

    # Domain inference (small cardinality)
    try:
      top_vals = dfp.groupBy(col).count().orderBy(F.desc("count")).limit(50)
      tv = [(r[0], r[1]) for r in top_vals.collect() if r[0] is not None]
      if tv:
        cumulative = 0
        domain = []
        for v, c in tv:
          domain.append(v)
          cumulative += c
          if (cumulative / non_null) >= coverage_threshold or len(domain) >= 20:
            break
        if len(domain) <= 20 and non_null > 0 and (cumulative / non_null) >= coverage_threshold:
          expr_vals = ", ".join([json.dumps(v) for v in domain])
          rule_expr = f"{col} IN ({expr_vals})"
          rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|DOMAIN|{rule_expr}")
          rule_rows.append((catalog, schema, table, col, "DOMAIN", rule_expr, (cumulative/non_null), coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))
    except Exception:
      pass

    # Regex patterns
    patterns = {
      "DIGITS": r"^[0-9]+$",
      "ALPHA": r"^[A-Za-z]+$",
      "ALNUM": r"^[A-Za-z0-9_-]+$",
      "UUID": r"^[0-9a-fA-F-]{36}$",
      "EMAIL": r"^[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}$",
    }
    best = None
    for ptype, pregex in patterns.items():
      matches = dfp.filter(F.col(col).rlike(pregex)).count()
      cov = (matches / non_null) if non_null else 0.0
      if best is None or cov > best[2]:
        best = (ptype, pregex, cov)
    if best and best[2] >= coverage_threshold:
      rule_expr = f"{col} RLIKE '{best[1]}'"
      rule_id = sha256_hex(f"{catalog}|{schema}|{table}|{col}|PATTERN_{best[0]}|{rule_expr}")
      rule_rows.append((catalog, schema, table, col, f"PATTERN_{best[0]}", rule_expr, best[2], coverage, support, row_count, json.dumps(profile), provenance, profile_ts, rule_id))

# COMMAND ----------
# Persist rules
if rule_rows:
  rules_df = spark.createDataFrame(rule_rows, schema=T.StructType([
    T.StructField("catalog_name", T.StringType(), False),
    T.StructField("schema_name", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("column_name", T.StringType(), True),
    T.StructField("rule_type", T.StringType(), False),
    T.StructField("rule_expression", T.StringType(), False),
    T.StructField("confidence", T.DoubleType(), True),
    T.StructField("coverage", T.DoubleType(), True),
    T.StructField("support", T.LongType(), True),
    T.StructField("row_count", T.LongType(), True),
    T.StructField("profile_json", T.StringType(), True),
    T.StructField("provenance", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("rule_id", T.StringType(), False),
  ]))
else:
  rules_df = spark.createDataFrame([], schema=T.StructType([
    T.StructField("catalog_name", T.StringType(), False),
    T.StructField("schema_name", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("column_name", T.StringType(), True),
    T.StructField("rule_type", T.StringType(), False),
    T.StructField("rule_expression", T.StringType(), False),
    T.StructField("confidence", T.DoubleType(), True),
    T.StructField("coverage", T.DoubleType(), True),
    T.StructField("support", T.LongType(), True),
    T.StructField("row_count", T.LongType(), True),
    T.StructField("profile_json", T.StringType(), True),
    T.StructField("provenance", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("rule_id", T.StringType(), False),
  ]))

spark.sql(f"DELETE FROM {catalog}.dq.dq_rule_generator WHERE catalog_name='{catalog}' AND schema_name='{schema}' AND table_name='{table}'")
rules_df.write.mode("append").format("delta").saveAsTable(f"{catalog}.dq.dq_rule_generator")

print(f"Generated {rules_df.count()} rules for {full_name}")

display(rules_df.orderBy(F.desc("confidence")))