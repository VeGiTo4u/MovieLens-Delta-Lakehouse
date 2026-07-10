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
etl_meta = resolve_etl_metadata(include_source_system=True)

source_full = build_table_name(source_catalog_name, source_schema_name, source_table_name)
target_full = build_table_name(target_catalog_name, target_schema_name, target_table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Imports — Deferred Until After %run and Validation
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
import sys

REPO_ROOT = "/Workspace/MovieLens-Delta-Lakehouse"
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

# COMMAND ----------

# ------------------------------------------------------------
# Incrementality — which years still need processing
#
# Incrementality is still tracked by _batch_year (which source
# files have been processed), NOT by rating_year. _batch_year
# is the stable unit of work: one file, one batch. "Has this
# batch been ingested into Silver?" is still the right question.
#
# What changed (Architecture fix):
#   Silver is now partitioned by rating_year (event year from
#   timestamp), not _batch_year. This means Silver owns the
#   clean, event-time-based partitioning scheme. Late arrivals
#   (e.g. 2019 events in ratings_2022.csv) are routed to the
#   correct rating_year=2019 partition by MERGE at the Silver
#   layer — no longer pushed down to Gold.
#
#   _batch_year is retained as a non-partition column for:
#     1. Incrementality tracking — which batches were processed
#     2. Audit trail — which file each record came from
#     3. Late arrival detection — comparing rating_year vs _batch_year
# ------------------------------------------------------------
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
# in scripts/silver/transforms/ratings — tested by pytest).
# ------------------------------------------------------------
from scripts.silver.transforms.ratings import get_dq_rules, transform_ratings

# COMMAND ----------

# ------------------------------------------------------------
# Process years — accumulate all new batches then MERGE once
#
# Architecture change:
#   Previously, Silver wrote per-year using replaceWhere on
#   _batch_year. Now Silver is partitioned by rating_year
#   (event year from timestamp). Since late arrivals cross
#   year boundaries, we accumulate ALL new batch data into
#   a single DataFrame and MERGE once.
#
#   This is cleaner and more efficient:
#     - One MERGE instead of N replaceWhere calls
#     - Late arrivals routed to correct rating_year automatically
#     - No risk of overwriting existing data in other partitions
#
# MERGE key: (user_id, movie_id, interaction_timestamp)
#   Uniquely identifies one rating event.
#   On rerun: MATCHED → UPDATE SET * (same values, idempotent)
#   On new data: NOT MATCHED → INSERT
#   Late arrivals: INSERT into correct rating_year partition
# ------------------------------------------------------------
total_processed     = 0
total_quarantine    = 0
total_late_arrivals = 0

# Accumulate all new-batch DataFrames
df_all_new = None

for year in years_to_process:
    print(f"\n{'='*60}")
    print(f"[YEAR {year}] Starting Silver transformation")
    print(f"{'='*60}")

    # Read this year's Bronze partition only
    df_bronze_year = (
        spark.table(source_full)
             .filter(F.col("_batch_year") == year)
    )

    # Transform — is_late_arrival + rating_year added here
    df_transformed = transform_ratings(df_bronze_year)

    # DQ flagging
    df_flagged = apply_dq_flags(df_transformed, get_dq_rules())

    # ETL metadata
    df_silver_year = append_incremental_metadata(df_flagged, etl_meta, year)

    # ------------------------------------------------------------
    # Single-pass metrics — replaces 3 separate count() calls
    # ------------------------------------------------------------
    year_metrics = compute_year_metrics(df_silver_year, track_late_arrivals=True)

    year_final_count        = year_metrics["total"]
    year_quarantine_count   = year_metrics["quarantine"]
    year_late_arrival_count = year_metrics["late_arrivals"]

    print(f"[YEAR {year}] Records to write : {year_final_count:,}")
    print(f"[YEAR {year}] Quarantined      : {year_quarantine_count:,}")
    if year_late_arrival_count > 0:
        print(f"[YEAR {year}] Late arrivals    : {year_late_arrival_count:,} "
              f"(is_late_arrival=True — Silver MERGE will route to correct rating_year partition)")

    # Accumulate into single DataFrame for one MERGE
    if df_all_new is None:
        df_all_new = df_silver_year
    else:
        df_all_new = df_all_new.unionByName(df_silver_year)

    total_processed     += year_final_count
    total_quarantine    += year_quarantine_count
    total_late_arrivals += year_late_arrival_count
    print(f"[YEAR {year}] DONE (accumulated for MERGE)")

# COMMAND ----------

# ------------------------------------------------------------
# Write via MERGE — one operation for all new batches
#
# Silver ratings are partitioned by rating_year (event year),
# not _batch_year. MERGE routes each record to the correct
# rating_year partition automatically.
#
# MERGE key: (user_id, movie_id, interaction_timestamp)
#   Uniquely identifies one rating event.
#   On rerun: MATCHED → UPDATE SET * (same values, idempotent)
#   On new data: NOT MATCHED → INSERT
#   Late arrivals: INSERT into correct rating_year partition
#
# SCD2 natural key: (user_id, movie_id)
#   Identifies the entity being versioned. When a user re-rates
#   the same movie with a newer timestamp, the old row is expired
#   and the new row becomes the current version.
# ------------------------------------------------------------
MERGE_KEY_COLS      = ["user_id", "movie_id", "interaction_timestamp"]
SCD2_NATURAL_KEY    = ["user_id", "movie_id"]

merge_result = write_incremental_merge(
    df               = df_all_new,
    full_table_name  = target_full,
    s3_target_path   = s3_target_path,
    merge_key_cols   = MERGE_KEY_COLS,
    partition_by     = ["rating_year"],
    scd2_natural_key = SCD2_NATURAL_KEY,
)

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(spark, target_full, s3_target_path)


    extra_info = {
        "Years processed"      : years_to_process,
        "Years skipped"        : years_to_skip,
        "Quarantined records"  : f"{total_quarantine:,}",
        "Late arrival records" : f"{total_late_arrivals:,} (flagged — Silver MERGE routes to correct rating_year)",
        "Rows inserted (MERGE)": f"{merge_result['rows_inserted']:,}",
        "Rows updated (MERGE)" : f"{merge_result['rows_updated']:,}",
        "SCD2 expirations"     : f"{merge_result['scd2_expirations']:,}",
        "Duplicates dropped"   : f"{merge_result['duplicates_dropped']:,}",
        "Partition column"     : "rating_year (event year from timestamp, NOT _batch_year)",
        "Write strategy"       : "MERGE upsert with SCD Type-2 — routes late arrivals + versions re-ratings",
        "Merge key"            : "(user_id, movie_id, interaction_timestamp)",
        "SCD2 natural key"     : "(user_id, movie_id)",
        "Deduplication"        : "row_number() ORDER BY _processing_timestamp DESC (deterministic)",
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
