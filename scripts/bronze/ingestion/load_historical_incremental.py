# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/bronze/utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets — Parameterized for Job Orchestration
#
# All path/table/schema values are injected via widgets so this
# notebook can be driven by Databricks Jobs or parent notebooks
# without hardcoding environment-specific values.
# ------------------------------------------------------------
dbutils.widgets.text("s3_source_path", "", "S3 Source URI (CSV)")
dbutils.widgets.text("s3_target_path", "", "S3 Target URI (Delta Location)")
dbutils.widgets.text("table_name",     "", "Target Table Name")
dbutils.widgets.text("catalog_name",   "movielens", "Catalog Name")
dbutils.widgets.text("schema_name",    "bronze",    "Schema Name")

s3_source_path = dbutils.widgets.get("s3_source_path")
s3_target_path = dbutils.widgets.get("s3_target_path")
table_name     = dbutils.widgets.get("table_name")
catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context — Fail Fast Before Any Spark Work
#
# Validates inputs and resolves ETL metadata upfront so failures
# surface immediately, not after a costly S3 scan or write.
# ------------------------------------------------------------
s3_source_path, s3_target_path = validate_inputs(
    s3_source_path, s3_target_path, table_name
)
etl_meta        = resolve_etl_metadata()
full_table_name = build_table_name(catalog_name, schema_name, table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Schemas
# Schema inference is intentionally avoided.
# Incremental tables: ratings, tags.
# ------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType, StringType
)
from pyspark.sql import functions as F

schemas = {
    "ratings": StructType([
        StructField("userId",    IntegerType(), True),
        StructField("movieId",   IntegerType(), True),
        StructField("rating",    DoubleType(),  True),
        StructField("timestamp", LongType(),    True),
    ]),
    "tags": StructType([
        StructField("userId",    IntegerType(), True),
        StructField("movieId",   IntegerType(), True),
        StructField("tag",       StringType(),  True),
        StructField("timestamp", LongType(),    True),
    ]),
}

if table_name not in schemas:
    raise ValueError(
        f"Unsupported table_name '{table_name}'. "
        f"Supported: {list(schemas.keys())}"
    )

print(f"[START] Bronze incremental ingestion for table: {full_table_name}")
print(f"        Source : {s3_source_path}")
print(f"        Target : {s3_target_path}")

# COMMAND ----------

# ------------------------------------------------------------
# S3 File Discovery + Incrementality Resolution
#
# STEP 1 — discover_s3_years():
#   List all files at s3_source_path and extract years from
#   filenames matching: <table_name>_<YYYY>.csv
#
# STEP 2 — get_already_processed_years_bronze():
#   Read distinct _batch_id values already committed in the
#   Bronze Delta table. The table is the source of truth —
#   no external state or control files needed.
#
# STEP 3 — Set subtraction:
#   years_to_process = available_on_s3 - already_in_delta
# ------------------------------------------------------------
available_years         = discover_s3_years(s3_source_path, table_name)
already_processed_years = get_already_processed_years_bronze(full_table_name)

years_to_process = sorted(available_years - already_processed_years)
years_to_skip    = sorted(available_years & already_processed_years)

print(f"\n[INFO] Years to process : {years_to_process}")
print(f"[INFO] Years to skip    : {years_to_skip} (already in Delta)")
print(f"[INFO] Total to process : {len(years_to_process)}")

if not years_to_process:
    print("[INFO] All available years already processed — nothing to do. Exiting.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# ------------------------------------------------------------
# Process years loop
#
# Write strategy: replaceWhere(_batch_year = year)
#   Despite the word 'overwrite' this is NOT a full table overwrite.
#   Delta scopes the operation to only _batch_year = year partition.
#   All other year partitions are never touched.
#
# Failure handling:
#   File not found → file disappeared between discovery and read
#                    (race condition) — skip + warn, continue loop
#   Any other error → hard fail immediately, job exits non-zero
#                     Already-committed partitions are ACID safe.
# ------------------------------------------------------------
processed_years = []
skipped_years   = []
total_written   = 0
extra_cross_year_info = {}  # year → cross-year summary string (populated if anomalies found)

for year in years_to_process:
    source_file = f"{s3_source_path}{table_name}_{year}.csv"

    print(f"\n[YEAR {year}] Processing | Source: {source_file}")

    try:
        df_raw = (
            spark.read
                 .format("csv")
                 .option("header", "true")
                 .schema(schemas[table_name])
                 .load(source_file)
        )

        df_bronze    = append_incremental_metadata(df_raw, etl_meta, table_name, year)
        year_written = write_incremental_bronze(df_bronze, s3_target_path, table_name, year)

        # Register UC table after the FIRST successful write so that
        # SHOW PARTITIONS works on retry if the loop fails mid-way.
        # CREATE TABLE IF NOT EXISTS is a no-op on subsequent iterations.
        if len(processed_years) == 0:
            register_table(full_table_name, s3_target_path)

        processed_years.append(year)
        total_written += year_written
        print(f"[YEAR {year}] SUCCESS — {year_written:,} records written")

        # ---------------------------------------------------
        # Cross-year observability — WARNING only, never fails
        # Detects late arrivals at the earliest possible point.
        # Silver will handle them correctly via is_late_arrival
        # flag + Gold MERGE. This log gives operators an early
        # signal to investigate if counts look unexpected.
        # ---------------------------------------------------
        cross_year_result = detect_cross_year_records(
            full_table_name = full_table_name,
            batch_year      = year,
            timestamp_col   = "timestamp",
        )
        if cross_year_result["cross_year_count"] > 0:
            extra_cross_year_info[year] = (
                f"{cross_year_result['cross_year_count']:,} cross-year records "
                f"({cross_year_result['cross_year_pct']}%) — "
                f"dist: {cross_year_result['year_distribution']}"
            )

    except Exception as e:
        error_msg = str(e)

        is_file_not_found = (
            "AnalysisException" in type(e).__name__ or
            "Path does not exist" in error_msg or
            "FileNotFoundException" in error_msg
        )

        if is_file_not_found:
            # File was listed in S3 discovery but vanished before read.
            # Skip + warn. Do not fail the job.
            skipped_years.append(year)
            print(f"[YEAR {year}] SKIPPED — file disappeared after discovery: {source_file}")
        else:
            # Unexpected error — fail immediately.
            # Job exits non-zero, Databricks marks run FAILED.
            print(f"[YEAR {year}] FAILED — unexpected error: {error_msg}")
            raise RuntimeError(
                f"Pipeline failed on year {year} for table '{table_name}'. "
                f"Error: {error_msg}"
            )

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(full_table_name, s3_target_path)

post_write_validation_bronze(full_table_name, total_written, processed_years)

print_summary(
    label           = "Incremental",
    full_table_name = full_table_name,
    s3_source_path  = s3_source_path,
    s3_target_path  = s3_target_path,
    etl_meta        = etl_meta,
    extra_info      = {
        "Available years on S3"  : sorted(available_years),
        "Years processed"        : processed_years,
        "Years skipped (Delta)"  : years_to_skip,
        "Years skipped (missing)": skipped_years,
        "Total records written"  : f"{total_written:,}",
        "Write strategy"         : "replaceWhere(_batch_year) + partitionBy",
        "Idempotency"            : "per-year partition isolation",
        **({f"Cross-year [year {y}]": v for y, v in extra_cross_year_info.items()}
           if extra_cross_year_info else {"Cross-year anomalies": "None detected"}),
    }
)
