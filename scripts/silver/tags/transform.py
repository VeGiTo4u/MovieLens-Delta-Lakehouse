# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/common

# COMMAND ----------

# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/silver/utils

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
s3_target_path = validate_s3_path(s3_target_path, "target path")
validate_table_name(source_table_name, "source_table_name")
validate_table_name(target_table_name, "target_table_name")
etl_meta       = resolve_etl_metadata(include_source_system=True)

source_full = build_table_name(source_catalog_name, source_schema_name, source_table_name)
target_full = build_table_name(target_catalog_name, target_schema_name, target_table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Imports — Deferred Until After %run and Validation
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType
import sys

REPO_ROOT = "/Workspace/MovieLens-Delta-Lakehouse"
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

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
# Import production transform functions (single source of truth
# in scripts/silver/transforms/tags — tested by pytest).
# ------------------------------------------------------------
from scripts.silver.transforms.tags import get_dq_rules, transform_tags

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
        register_table(spark, target_full, s3_target_path)

    total_processed  += year_final_count
    total_quarantine += year_quarantine_count
    print(f"[YEAR {year}] DONE")

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(spark, target_full, s3_target_path)


    extra_info = {
        "Years processed"     : years_to_process,
        "Years skipped"       : years_to_skip,
        "Quarantined records" : f"{total_quarantine:,}",
        "Write strategy"      : "replaceWhere(_batch_year) + partitionBy",
        "Deduplication"       : "None — all events preserved (Gold responsibility)",
    }
    extra_info.update({
        "Initial count": f"{total_processed:,}",
        "Final count": f"{total_processed:,}",
    })

    print_pipeline_summary(
        "SILVER", "TRANSFORMATION", 
        {
            "": {
                "Source Table": source_full,
                "Target Table": target_full,
                "Target S3": s3_target_path,
            },
            "ETL Metadata": {
                "_job_run_id": etl_meta["job_run_id"],
                "_notebook_path": etl_meta["notebook_path"],
                "_source_system": etl_meta.get("source_system", "UNKNOWN"),
            },
            "Run Details": extra_info,
        }
    )
