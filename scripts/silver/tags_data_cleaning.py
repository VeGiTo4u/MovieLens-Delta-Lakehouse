# Databricks notebook source
# MAGIC %run ./silver_utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets — Parameterized for Job Orchestration
# ------------------------------------------------------------
dbutils.widgets.text("source_table_name",  "",          "Source Table Name")
dbutils.widgets.text("target_table_name",  "",          "Target Table Name")
dbutils.widgets.text("s3_target_path",     "",          "Target S3 URI (Delta Location)")
dbutils.widgets.text("source_catalog_name","movielens", "Source Catalog")
dbutils.widgets.text("source_schema_name", "bronze",    "Source Schema")
dbutils.widgets.text("target_catalog_name","movielens", "Target Catalog")
dbutils.widgets.text("target_schema_name", "silver",    "Target Schema")

source_table_name   = dbutils.widgets.get("source_table_name")
target_table_name   = dbutils.widgets.get("target_table_name")
s3_target_path      = dbutils.widgets.get("s3_target_path")
source_catalog_name = dbutils.widgets.get("source_catalog_name")
source_schema_name  = dbutils.widgets.get("source_schema_name")
target_catalog_name = dbutils.widgets.get("target_catalog_name")
target_schema_name  = dbutils.widgets.get("target_schema_name")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context — Fail Fast Before Any Spark Work
# ------------------------------------------------------------
s3_target_path = validate_inputs(s3_target_path, source_table_name, target_table_name)
etl_meta       = resolve_etl_metadata()

source_full, target_full = build_table_names(
    source_catalog_name, source_schema_name, source_table_name,
    target_catalog_name, target_schema_name, target_table_name
)

# COMMAND ----------

# ------------------------------------------------------------
# Imports — Deferred Until After %run and Validation
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType

# COMMAND ----------

# ------------------------------------------------------------
# Incrementality — which years still need processing
#
# Both available_years (from Bronze) and already_processed_years
# (from Silver) now use get_partition_years() — SHOW PARTITIONS
# against the Delta transaction log. Zero data files opened.
#
# See ratings_data_cleaning.py for full reasoning on why
# distinct().collect() was replaced with get_partition_years().
# ------------------------------------------------------------
# get_available_years_from_source() — Bronze source side
#   Tries SHOW PARTITIONS first (zero scan). Falls back to
#   distinct().collect() if Unity Catalog metadata hasn't synced
#   yet after the first Bronze write. Fails hard if Bronze is absent.
#
# get_already_processed_years() — Silver target side
#   SHOW PARTITIONS only. Empty set = first run = correct.
#   No fallback needed: an absent Silver table is expected on run 1.
available_years         = get_available_years_from_source(source_full)
already_processed_years = get_already_processed_years(target_full)

years_to_process = sorted(available_years - already_processed_years)
years_to_skip    = sorted(available_years & already_processed_years)

print(f"[INFO] Available in Bronze : {sorted(available_years)}")
print(f"[INFO] Years to process    : {years_to_process}")
print(f"[INFO] Years to skip       : {years_to_skip}")

if not years_to_process:
    print("[INFO] All years already processed — nothing to do. Exiting.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# ------------------------------------------------------------
# Transformation — Business Logic Isolation
# ------------------------------------------------------------
def transform_tags(df):
    """
    Cleans and conforms Bronze tags data to Silver standards.

    Steps:
      1. Column rename + type cast
      2. Tag normalization:
           - Trim whitespace / newlines / tabs
           - Remove special characters (keep alphanumeric, spaces, hyphens)
           - Collapse multiple spaces to single space
           - Title Case formatting
      3. Timestamp conversion (Unix epoch → TIMESTAMP)
      4. Date key derivation (YYYYMMDD as INT)

    No deduplication — all tag events are preserved.
    A user tagging the same movie multiple times is a valid
    event history. Deduplication is a Gold-layer decision.
    """
    return (
        df
        # Step 1: Rename + cast
        .withColumn("user_id",  F.col("userId").cast(IntegerType()))
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("tag_raw",  F.col("tag").cast(StringType()))

        # Step 2a: Trim + remove control characters
        .withColumn("tag",
                    F.trim(F.regexp_replace(F.col("tag_raw"), r"[\n\t\r]+", " ")))

        # Step 2b: Remove special characters (keep letters, numbers, spaces, hyphens)
        .withColumn("tag",
                    F.regexp_replace(F.col("tag"), r"[^a-zA-Z0-9\s\-]", ""))

        # Step 2c: Collapse multiple spaces to single space
        .withColumn("tag",
                    F.regexp_replace(F.col("tag"), r"\s+", " "))

        # Step 2d: Final trim + Title Case
        .withColumn("tag",
                    F.initcap(F.trim(F.col("tag"))))

        # Step 3: Unix epoch → TIMESTAMP
        .withColumn("tag_timestamp",
                    F.from_unixtime(F.col("timestamp")).cast(TimestampType()))

        # Step 4: Date key YYYYMMDD as INT
        .withColumn("date_key",
                    F.date_format(F.col("tag_timestamp"), "yyyyMMdd")
                     .cast(IntegerType()))

        # Final selection — drop raw bronze columns
        .select(
            "user_id",
            "movie_id",
            "tag",
            "tag_timestamp",
            "date_key",
            "_ingestion_timestamp",
        )
    )

# COMMAND ----------

# ------------------------------------------------------------
# DQ Rules for tags
# ------------------------------------------------------------
def get_dq_rules():
    return [
        ("NULL_USER_ID",
         F.col("user_id").isNull()),

        ("NULL_MOVIE_ID",
         F.col("movie_id").isNull()),

        ("NULL_TAG",
         F.col("tag").isNull()),

        ("EMPTY_TAG",
         F.trim(F.col("tag")) == ""),

        ("NULL_TIMESTAMP",
         F.col("tag_timestamp").isNull()),

        # Guard against epoch-zero and sentinel timestamps — same reasoning
        # as ratings INVALID_TIMESTAMP_FLOOR. A tag with timestamp=0 produces
        # tag_timestamp=1970-01-01 which is not a real user action.
        # isNotNull() guard ensures we don't compare against a null value —
        # NULL_TIMESTAMP quarantines those rows before this rule fires.
        ("INVALID_TIMESTAMP_FLOOR",
         F.col("tag_timestamp").isNotNull() &
         (F.col("tag_timestamp") < F.lit("1995-01-01").cast(TimestampType()))),

        # Tags must be at least 3 characters to be meaningful
        ("SHORT_TAG",
         F.length(F.col("tag")) < 3),

        # date_key derivation consistency check
        ("DATE_KEY_MISMATCH",
         F.col("date_key") != F.date_format(
             F.col("tag_timestamp"), "yyyyMMdd"
         ).cast(IntegerType())),
    ]

# COMMAND ----------

# ------------------------------------------------------------
# Process years loop
# ------------------------------------------------------------
total_processed  = 0
total_quarantine = 0

for year in years_to_process:
    print(f"\n{'='*60}")
    print(f"[YEAR {year}] Starting Silver transformation")
    print(f"{'='*60}")

    # Read this year's Bronze partition only
    df_bronze_year = (
        spark.table(source_full)
             .filter(F.col("_batch_year") == year)
    )

    # Transform
    df_transformed = transform_tags(df_bronze_year)

    # DQ flagging
    df_flagged = apply_dq_flags(df_transformed, get_dq_rules())

    # ETL metadata
    df_silver_year = append_incremental_metadata(df_flagged, etl_meta, year)

    # ------------------------------------------------------------
    # Single-pass metrics — replaces 2 separate count() calls
    #
    # Old pattern (2 Spark jobs):
    #   year_final_count      = df_silver_year.count()
    #   year_quarantine_count = df_silver_year.filter(_dq_status==QUARANTINE).count()
    #
    # New pattern (1 Spark job):
    #   Both metrics computed in a single agg() action.
    #   tags does not have is_late_arrival — track_late_arrivals=False.
    # ------------------------------------------------------------
    year_metrics = compute_year_metrics(df_silver_year, track_late_arrivals=False)

    year_final_count      = year_metrics["total"]
    year_quarantine_count = year_metrics["quarantine"]

    print(f"[YEAR {year}] Records to write : {year_final_count:,}")
    print(f"[YEAR {year}] Quarantined      : {year_quarantine_count:,}")

    # Write — idempotent via replaceWhere on _batch_year
    write_incremental(df_silver_year, s3_target_path, target_table_name, year)

    # Register UC table after the FIRST successful write so that
    # SHOW PARTITIONS works on retry if the loop fails mid-way.
    # CREATE TABLE IF NOT EXISTS is a no-op on subsequent iterations.
    if year == years_to_process[0]:
        register_table(target_full, s3_target_path)

    total_processed  += year_final_count
    total_quarantine += year_quarantine_count
    print(f"[YEAR {year}] DONE")

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(target_full, s3_target_path)

post_write_validation(target_full, total_processed)

print_summary(
    source_full_table_name = source_full,
    target_full_table_name = target_full,
    s3_target_path         = s3_target_path,
    initial_count          = total_processed,
    final_count            = total_processed,
    etl_meta               = etl_meta,
    extra_info             = {
        "Years processed"     : years_to_process,
        "Years skipped"       : years_to_skip,
        "Quarantined records" : f"{total_quarantine:,}",
        "Write strategy"      : "replaceWhere(_batch_year) + partitionBy",
        "Deduplication"       : "None — all events preserved (Gold responsibility)",
    }
)