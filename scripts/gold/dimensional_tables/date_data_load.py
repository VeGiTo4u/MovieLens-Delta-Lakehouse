# Databricks notebook source
# MAGIC %run /MovieLens-Delta-Lakehouse/gold/gold_utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets
# ------------------------------------------------------------
dbutils.widgets.text("target_table_name",   "",          "Target Table Name")
dbutils.widgets.text("s3_target_path",      "",          "Target S3 URI (Delta Location)")
dbutils.widgets.text("target_catalog_name", "movielens", "Target Catalog")
dbutils.widgets.text("target_schema_name",  "gold",      "Target Schema")
dbutils.widgets.text("model_version",       "1.0",       "Model Version")

target_table_name   = dbutils.widgets.get("target_table_name")
s3_target_path      = dbutils.widgets.get("s3_target_path")
target_catalog_name = dbutils.widgets.get("target_catalog_name")
target_schema_name  = dbutils.widgets.get("target_schema_name")
model_version       = dbutils.widgets.get("model_version")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context
#
# dim_date has no silver source table — it is generated
# programmatically from the date range found in silver.ratings.
# source_table_name is not a widget here.
# ------------------------------------------------------------
s3_target_path = validate_inputs(s3_target_path, target_table_name)
etl_meta       = resolve_etl_metadata()

target_full, _ = build_table_names(
    target_catalog_name, target_schema_name, target_table_name
)

# COMMAND ----------

# ------------------------------------------------------------
# Determine date range from silver.ratings
#
# dim_date is driven by the actual span of rating events —
# not a hardcoded range. This means dim_date automatically
# extends if new years of ratings are loaded into silver.
#
# Dependency enforcement: if silver.ratings does not exist,
# the pipeline fails here with a clear message rather than
# silently generating an empty or wrong date range.
# ------------------------------------------------------------
from pyspark.sql import functions as F

silver_ratings_table = f"{target_catalog_name}.silver.ratings"

print(f"[START] Determining date range from {silver_ratings_table}")

try:
    df_ratings  = spark.table(silver_ratings_table)
    date_range  = df_ratings.select(
        F.min("interaction_timestamp").alias("min_ts"),
        F.max("interaction_timestamp").alias("max_ts"),
    ).collect()[0]

    if date_range["min_ts"] is None or date_range["max_ts"] is None:
        raise ValueError("silver.ratings exists but contains no data — timestamps are NULL")

    START_DATE = date_range["min_ts"].date().strftime("%Y-%m-%d")
    END_DATE   = date_range["max_ts"].date().strftime("%Y-%m-%d")

    print(f"[INFO] Date range : {START_DATE} → {END_DATE}")

except Exception as e:
    raise RuntimeError(
        f"FAILED: Cannot determine date range from '{silver_ratings_table}'. "
        f"silver.ratings must exist before dim_date can be generated. Error: {e}"
    )

# COMMAND ----------

# ------------------------------------------------------------
# Generate date dimension with pandas
#
# Pandas is used for date generation because it handles
# calendar arithmetic (leap years, day-of-week, ISO weeks)
# cleanly in driver memory. The result is small (a few thousand
# rows) so driver-side generation is appropriate and avoids
# the complexity of distributed date sequence generation.
#
# Day-of-week convention: US Standard (1=Sunday, 7=Saturday)
# Pandas uses Monday=0, Sunday=6 — we convert accordingly.
# ------------------------------------------------------------
import pandas as pd
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DateType, StringType, BooleanType
)

print("[START] Generating date dimension")

date_range_pd    = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
expected_count   = len(date_range_pd)
print(f"[INFO] Generating {expected_count:,} dates")

date_data = []
for date in date_range_pd:
    # Pandas dayofweek: Monday=0 ... Sunday=6
    # US Standard:      Sunday=1 ... Saturday=7
    dow_us = (date.dayofweek + 2) % 7
    if dow_us == 0:
        dow_us = 7  # Sunday wraps from 0 → 7

    date_data.append({
        "date_key":          int(date.strftime("%Y%m%d")),
        "full_date":         date.date(),
        "year":              date.year,
        "quarter":           (date.month - 1) // 3 + 1,
        "month":             date.month,
        "month_name":        date.strftime("%B"),
        "day_of_month":      date.day,
        "day_of_week":       dow_us,
        "day_of_week_name":  date.strftime("%A"),
        "is_weekend":        dow_us in (1, 7),
        "week_of_year":      int(date.isocalendar()[1]),
    })

pdf = pd.DataFrame(date_data)

schema = StructType([
    StructField("date_key",          IntegerType(), False),
    StructField("full_date",         DateType(),    False),
    StructField("year",              IntegerType(), False),
    StructField("quarter",           IntegerType(), False),
    StructField("month",             IntegerType(), False),
    StructField("month_name",        StringType(),  False),
    StructField("day_of_month",      IntegerType(), False),
    StructField("day_of_week",       IntegerType(), False),
    StructField("day_of_week_name",  StringType(),  False),
    StructField("is_weekend",        BooleanType(), False),
    StructField("week_of_year",      IntegerType(), False),
])

df_gold = spark.createDataFrame(pdf, schema=schema)
print(f"[SUCCESS] Date dimension generated : {expected_count:,} rows")

# COMMAND ----------

# ------------------------------------------------------------
# Quality validation
#
# Three critical checks for a date dimension:
#
# 1. Record count — must match expected days in range
# 2. PK uniqueness — date_key must be unique (YYYYMMDD)
# 3. Date continuity — no gaps in the sequence
#
# Gap detection uses Spark lag() instead of a Python loop.
# The original approach collected all date_keys to the driver
# and iterated in Python — O(n) on the driver and not
# distributed. The lag() approach stays in Spark:
#   - Compute previous date_key per row using lag()
#   - Convert both date_keys to actual DATE values
#   - A gap exists where current_date != prev_date + 1 day
#   - Count those rows — zero gaps means a clean sequence
# ------------------------------------------------------------
from pyspark.sql import Window as W

print("[START] Quality validation")

actual_count = df_gold.count()

# Check 1: Record count
if actual_count != expected_count:
    raise ValueError(
        f"FAILED: Record count mismatch. "
        f"Expected {expected_count:,}, got {actual_count:,}"
    )
print(f"[PASS] Record count : {actual_count:,}")

# Check 2: PK uniqueness
pk_dupes = df_gold.groupBy("date_key").count().filter(F.col("count") > 1).count()
if pk_dupes > 0:
    raise ValueError(f"FAILED: {pk_dupes:,} duplicate date_key values found")
print(f"[PASS] PK uniqueness : no duplicates")

# Check 3: Date continuity via lag() — fully distributed, no collect()
window_ordered = W.orderBy("date_key")

gap_count = (
    df_gold
    .withColumn("prev_date_key", F.lag("date_key").over(window_ordered))
    .filter(F.col("prev_date_key").isNotNull())
    .withColumn("expected_prev",
                F.date_format(
                    F.date_sub(F.to_date(F.col("date_key").cast("string"), "yyyyMMdd"), 1),
                    "yyyyMMdd"
                ).cast(IntegerType()))
    .filter(F.col("prev_date_key") != F.col("expected_prev"))
    .count()
)

if gap_count > 0:
    raise ValueError(f"FAILED: {gap_count:,} date gaps detected in sequence")
print(f"[PASS] Date continuity : no gaps")

print("[SUCCESS] Quality validation passed")

# COMMAND ----------

# ------------------------------------------------------------
# Append ETL Metadata + Write + Register + Validate
#
# _source_table      = "GENERATED" — no silver source
# _source_silver_version = None    — no silver source
#
# Note: OPTIMIZE, ANALYZE TABLE, and VACUUM are handled by the
# dedicated maintenance notebook (scripts/maintenance/jobs/table_maintenance.py)
# scheduled during off-peak hours — decoupled from ETL.
# ------------------------------------------------------------
df_gold = append_gold_metadata(
    df            = df_gold,
    etl_meta      = etl_meta,
    source_table  = "GENERATED",
    model_version = model_version,
    silver_version = None,
)

final_count = write_gold(df_gold, s3_target_path, target_table_name)

register_table(target_full, s3_target_path)

post_write_validation_gold(target_full, final_count, pk_columns=["date_key"])

# COMMAND ----------

stats = spark.table(target_full).agg(
    F.min("year").alias("min_year"),
    F.max("year").alias("max_year"),
    F.countDistinct("year").alias("unique_years"),
    F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_days"),
    F.sum(F.when(~F.col("is_weekend"), 1).otherwise(0)).alias("weekday_days"),
).collect()[0]

print_summary(
    label            = "dim_date",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    extra_info       = {
        "Date range"             : f"{START_DATE} → {END_DATE}",
        "Total days"             : f"{final_count:,}",
        "Years covered"          : f"{stats['unique_years']} ({stats['min_year']}–{stats['max_year']})",
        "Weekend days"           : f"{stats['weekend_days']:,}",
        "Weekday days"           : f"{stats['weekday_days']:,}",
        "Source"                 : "GENERATED — driven by silver.ratings date range",
        "Day-of-week convention" : "US Standard (1=Sunday, 7=Saturday)",
        "Gap detection method"   : "Spark lag() — fully distributed",
        "Write strategy"         : "Full overwrite + mergeSchema",
    }
)