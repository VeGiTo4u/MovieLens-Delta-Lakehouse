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
etl_meta = resolve_etl_metadata()

source_full, target_full = build_table_names(
    source_catalog_name, source_schema_name, source_table_name,
    target_catalog_name, target_schema_name, target_table_name
)

# COMMAND ----------

# ------------------------------------------------------------
# Imports — Deferred Until After %run and Validation
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

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
# Transformation — Business Logic Isolation
# ------------------------------------------------------------
def transform_ratings(df):
    """
    Cleans and conforms Bronze ratings data to Silver standards.

    Steps:
      1. Column rename + type cast
      2. Timestamp conversion (Unix epoch → TIMESTAMP)
      3. Date key derivation (YYYYMMDD as INT)
      4. Rating rounding to 1 decimal place
      5. Late arrival flag derivation
      6. rating_year derivation (event year — partition column)
      7. SCD2 columns initialization

    No deduplication — all rating events are preserved.
    A user rating the same movie multiple times over the years
    is a valid event history. SCD2 versioning is handled by
    write_incremental_merge() during the MERGE step.

    DQ flagging (not dropping) is handled by apply_dq_flags()
    so quarantined rows remain visible for audit.
    """
    from pyspark.sql.types import IntegerType, DoubleType, TimestampType

    return (
        df
        # Step 1: Rename + cast
        .withColumn("user_id",  F.col("userId").cast(IntegerType()))
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("rating",   F.col("rating").cast(DoubleType()))

        # Step 2: Unix epoch → TIMESTAMP
        .withColumn("interaction_timestamp",
                    F.from_unixtime(F.col("timestamp")).cast(TimestampType()))

        # Step 3: Date key YYYYMMDD as INT — used as FK to dim_date in Gold
        .withColumn("date_key",
                    F.date_format(F.col("interaction_timestamp"), "yyyyMMdd")
                     .cast(IntegerType()))

        # Step 4: Round rating to 1 decimal place
        .withColumn("rating", F.round(F.col("rating"), 1))

        # Step 5: Late arrival flag
        # Compares the actual event year (from timestamp) to the batch year
        # (from the source filename via _batch_year).
        #
        # True  → record arrived in a different year's batch than its event year.
        #         e.g. a 2019 rating appearing in ratings_2022.csv
        # False → record arrived in the correct batch year (normal case)
        .withColumn("is_late_arrival",
                    F.year(F.col("interaction_timestamp")) != F.col("_batch_year"))

        # Step 6: rating_year — event year from timestamp
        # THIS IS THE PARTITION COLUMN. Not _batch_year.
        # Late arrivals land in the correct partition via MERGE.
        .withColumn("rating_year",
                    F.year(F.col("interaction_timestamp")))

        # Step 7: SCD2 columns
        # is_current: defaults to True for incoming rows
        #   write_incremental_merge() will set this correctly during MERGE:
        #   - On first run: computed via window function across all rows
        #   - On incremental: new rows insert as True, old versions expire to False
        # effective_start_date: when this version of the rating became active
        #   Always equals interaction_timestamp (the moment the user rated)
        # effective_end_date: when this version was superseded by a newer rating
        #   NULL for current versions, set during MERGE when a re-rating arrives
        .withColumn("is_current", F.lit(True))
        .withColumn("effective_start_date", F.col("interaction_timestamp"))
        .withColumn("effective_end_date", F.lit(None).cast(TimestampType()))

        # Final column selection — drop raw bronze columns
        .select(
            "user_id",
            "movie_id",
            "rating",
            "interaction_timestamp",
            "date_key",
            "is_late_arrival",
            "rating_year",
            "is_current",
            "effective_start_date",
            "effective_end_date",
            "_ingestion_timestamp",
            "_batch_year",
        )
    )

# COMMAND ----------

# ------------------------------------------------------------
# DQ Rules for ratings
#
# Rules define what makes a row QUARANTINE.
# Each rule is (rule_name, fail_condition_as_Column).
# apply_dq_flags() evaluates all rules per row and attaches
# _dq_status + _dq_failed_rules without dropping any rows.
# Gold filters to _dq_status = 'PASS' rows only.
# ------------------------------------------------------------
def get_dq_rules():
    return [
        ("NULL_USER_ID",
         F.col("user_id").isNull()),

        ("NULL_MOVIE_ID",
         F.col("movie_id").isNull()),

        ("NULL_RATING",
         F.col("rating").isNull()),

        ("NULL_TIMESTAMP",
         F.col("interaction_timestamp").isNull()),

        # Guard against epoch-zero (timestamp=0 → 1970-01-01) and other
        # sentinel values that pass NULL checks but are not real events.
        # MovieLens rating activity begins ~1995. Any timestamp before
        # this floor is a source system default or corruption signal —
        # not a real user rating. Without this rule, a single bad record
        # silently expands dim_date back to 1970 and pollutes year-based
        # analytics. Evaluated after NULL_TIMESTAMP so we never call
        # the comparison on a null — NULL_TIMESTAMP quarantines those first.
        ("INVALID_TIMESTAMP_FLOOR",
         F.col("interaction_timestamp").isNotNull() &
         (F.col("interaction_timestamp") < F.lit("1995-01-01").cast(TimestampType()))),

        # Rating must be within valid MovieLens range
        ("INVALID_RATING_RANGE",
         ~F.col("rating").between(0.0, 5.0)),

        # date_key must match interaction_timestamp — detects derivation bugs
        ("DATE_KEY_MISMATCH",
         F.col("date_key") != F.date_format(
             F.col("interaction_timestamp"), "yyyyMMdd"
         ).cast(IntegerType())),
    ]

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
register_table(target_full, s3_target_path)

post_write_validation(target_full, merge_result["rows_affected"])

print_summary(
    source_full_table_name = source_full,
    target_full_table_name = target_full,
    s3_target_path         = s3_target_path,
    initial_count          = total_processed,
    final_count            = total_processed,
    etl_meta               = etl_meta,
    extra_info             = {
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
)