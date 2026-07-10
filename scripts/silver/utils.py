# Databricks notebook source
# ============================================================
# silver_utils
# Shared utility functions for all Silver layer notebooks.
#
# Usage in each Silver notebook:
#   %run ./silver_utils
#
# Functions provided:
#   resolve_etl_metadata()         — job/run context resolution
#   validate_s3_path() / validate_table_name() — input validation
#   build_table_names()            — fully qualified Unity Catalog names
#   get_partition_years()          — metadata-only year discovery via SHOW PARTITIONS
#   get_already_processed_years()  — incrementality check (ratings, tags)
#   read_bronze()                  — read source table with error handling
#   append_static_metadata()       — ETL metadata for static tables
#   append_incremental_metadata()  — ETL metadata for incremental tables
#   compute_year_metrics()         — single-pass agg for per-year loop metrics
#   apply_dq_flags()               — attach _dq_status + _dq_failed_rules
#   write_static()                 — full overwrite + mergeSchema
#   write_incremental()            — replaceWhere per _batch_year
#   register_table()               — CREATE TABLE IF NOT EXISTS
#   post_write_validation()        — NULL metadata check (count from Delta log, not re-scan)
#   print_summary()                — standardized run summary
# ============================================================

# COMMAND ----------

import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from typing import List, Dict, Any
try:
    from scripts.common import *
except ImportError:
    pass

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# ------------------------------------------------------------
# get_available_years_from_source
# ------------------------------------------------------------
def get_available_years_from_source(source_full_table_name: str) -> set:
    """
    Returns the set of _batch_year values available in the Bronze
    source table — used exclusively on the SOURCE side of the
    incrementality check.

    Why this is separate from get_partition_years():
        get_partition_years() returns an empty set on ANY failure,
        which is correct for the Silver target (empty = first run =
        process everything). But applied to the Bronze source the same
        empty return is semantically wrong — it means "no years
        available", which causes the notebook to exit with NO_NEW_DATA
        on a genuine first load.

        The two cases that produce an empty set from SHOW PARTITIONS
        on the Bronze source are:
          1. Unity Catalog partition metadata has not fully synced
             after the first Bronze write (common on serverless compute).
             SHOW PARTITIONS reads the Hive metastore partition registry,
             which may lag the Delta log on the very first commit.
          2. SHOW PARTITIONS throws an exception (permissions, catalog
             config, etc.) — silently caught and returned as empty set.

        In both cases, the Bronze data IS there and IS readable.
        We fall back to a distinct().collect() scan to get the years.
        This scan only happens when SHOW PARTITIONS returns empty AND
        the table exists — i.e. only on first run or after a catalog
        metadata sync issue. All subsequent runs use SHOW PARTITIONS
        with no fallback needed.

        If Bronze itself does not exist, that is always a hard pipeline
        configuration error — Silver cannot run without Bronze — so we
        fail explicitly rather than silently returning empty.

    Strategy:
        1. Try SHOW PARTITIONS (zero data scan — fast path, all runs)
        2. If empty AND table exists → fallback distinct().collect()
           (one scan — only on first run or metadata sync edge case)
        3. If table does not exist → RuntimeError (Bronze must exist)

    Returns:
        Set of available _batch_year values as ints. Never empty
        if Bronze exists and has data.
    """
    # Fast path — metadata only, zero data scan
    try:
        rows = spark.sql(f"SHOW PARTITIONS {source_full_table_name}").collect()
        years = {
            int(row.partition.split("_batch_year=")[1])
            for row in rows
            if "_batch_year=" in row.partition
        }
    except Exception:
        years = set()

    if years:
        print(f"[INFO] Available years in Bronze (SHOW PARTITIONS): {sorted(years)}")
        return years

    # SHOW PARTITIONS returned empty — check whether the table actually exists
    table_exists = spark.catalog.tableExists(source_full_table_name)

    if not table_exists:
        raise RuntimeError(
            f"FAILED: Bronze source table '{source_full_table_name}' does not exist. "
            f"Run the Bronze ingestion notebook before Silver."
        )

    # Table exists but SHOW PARTITIONS returned empty — Unity Catalog
    # partition metadata has not synced yet after the first Bronze write.
    # Fall back to a distinct scan to read partition values directly
    # from the Delta data files. This path runs at most once per table
    # (only on first Silver execution after Bronze first load).
    print(f"[WARN] SHOW PARTITIONS returned empty for '{source_full_table_name}' "
          f"but table exists — Unity Catalog metadata sync lag detected.")
    print(f"[INFO] Falling back to distinct scan to discover available years.")

    years = {
        row._batch_year
        for row in (
            spark.table(source_full_table_name)
                 .select("_batch_year")
                 .distinct()
                 .collect()
        )
    }

    if not years:
        raise RuntimeError(
            f"FAILED: Bronze source table '{source_full_table_name}' exists "
            f"but contains no _batch_year values. "
            f"Check Bronze ingestion completed successfully."
        )

    print(f"[INFO] Available years in Bronze (fallback scan): {sorted(years)}")
    return years

# COMMAND ----------

# ------------------------------------------------------------
# get_already_processed_years
# ------------------------------------------------------------
def get_already_processed_years(target_full_table_name: str) -> set:
    """
    Returns the set of _batch_year values already committed in
    the Silver Delta table to determine which years to skip.

    Post-architecture-refactor:
        Silver ratings is now partitioned by rating_year (event year),
        NOT _batch_year. _batch_year is a non-partition column retained
        for incrementality tracking. SHOW PARTITIONS cannot discover it.

        For tables still partitioned by _batch_year (e.g. tags), this
        function first tries get_partition_years() (SHOW PARTITIONS —
        fast path). If the table exists but partition years are empty
        (new partitioning scheme), it falls back to SELECT DISTINCT.

    Returns:
        Set of already-processed _batch_year values (empty on first run)
    """
    # Try SHOW PARTITIONS first — fast path for tables still partitioned by _batch_year
    already_processed = get_partition_years(spark, target_full_table_name)

    if already_processed:
        print(f"[INFO] Already processed years in Silver (SHOW PARTITIONS): {sorted(already_processed)}")
        return already_processed

    # Table might exist but use rating_year partitioning — read _batch_year from data
    try:
        if not spark.catalog.tableExists(target_full_table_name):
            print("[INFO] Silver table does not exist yet — full load will run")
            return set()

        # Table exists but SHOW PARTITIONS returned empty or only rating_year values
        # Fall back to reading _batch_year directly from data
        already_processed = {
            row._batch_year
            for row in (
                spark.table(target_full_table_name)
                     .select("_batch_year")
                     .distinct()
                     .collect()
            )
        }

        if already_processed:
            print(f"[INFO] Already processed years in Silver (data scan): {sorted(already_processed)}")
        else:
            print("[INFO] Silver table exists but no _batch_year values found — full load will run")

        return already_processed
    except Exception:
        print("[INFO] Silver table does not exist yet — full load will run")
        return set()

# COMMAND ----------

# ------------------------------------------------------------
# read_bronze
# ------------------------------------------------------------
def read_bronze(source_full_table_name: str) -> DataFrame:
    """
    Reads the Bronze Delta table into a DataFrame.
    Fails hard if the table is missing or unreadable —
    a missing Bronze table is always a pipeline configuration
    error, never a recoverable condition.

    Row count logging removed:
        The old implementation called df.count() purely for a
        log message. This triggered a full scan of the Bronze
        table on every Silver run — significant overhead for
        tables like ratings with 100M+ rows. Bronze notebooks
        already log the committed count via Delta log metrics
        during their own write. Silver does not need to re-count
        the input; it logs per-year metrics after transformation.

    Returns:
        Bronze DataFrame (lazy — no Spark action triggered here)
    """
    print(f"[START] Reading Bronze table: {source_full_table_name}")

    try:
        df = spark.table(source_full_table_name)
        print(f"[INFO] Bronze table loaded (lazy): {source_full_table_name}")
        return df

    except Exception as e:
        raise RuntimeError(
            f"FAILED: Cannot read Bronze table '{source_full_table_name}'. "
            f"Ensure the table exists and is registered in Unity Catalog. "
            f"Error: {e}"
        )

# COMMAND ----------

# ------------------------------------------------------------
# append_static_metadata
# ------------------------------------------------------------
def append_static_metadata(df: DataFrame, etl_meta: Dict[str, str]) -> DataFrame:
    """
    Appends ETL metadata columns to static Silver tables.

    Columns added:
      _processing_timestamp      — when Silver transformation ran
      _bronze_ingestion_timestamp — carried from Bronze for latency tracking
      _job_run_id                — ties this Silver row to Databricks Job log
      _notebook_path             — which Silver notebook produced this row
      _source_system             — constant "S3_MovieLens"
    """
    return (
        df
        .withColumn("_processing_timestamp",       F.current_timestamp())
        .withColumn("_bronze_ingestion_timestamp", F.col("_ingestion_timestamp"))
        .withColumn("_job_run_id",                 F.lit(etl_meta["job_run_id"]))
        .withColumn("_notebook_path",              F.lit(etl_meta["notebook_path"]))
        .withColumn("_source_system",              F.lit(etl_meta["source_system"]))
    )

# COMMAND ----------

# ------------------------------------------------------------
# append_incremental_metadata
# ------------------------------------------------------------
def append_incremental_metadata(
    df:       DataFrame,
    etl_meta: Dict[str, str],
    year:     int
) -> DataFrame:
    """
    Appends ETL metadata columns to incremental Silver tables
    (ratings, tags). Carries _batch_year forward from Bronze
    for partition-level idempotency in Silver and Gold.

    Columns added:
      _processing_timestamp      — when Silver transformation ran
      _bronze_ingestion_timestamp — carried from Bronze for latency tracking
      _job_run_id                — ties this Silver row to Databricks Job log
      _notebook_path             — which Silver notebook produced this row
      _source_system             — constant "S3_MovieLens"
      _batch_year                — carried from Bronze; Silver's partition
                                   column and incrementality key
    """
    return (
        df
        .withColumn("_processing_timestamp",       F.current_timestamp())
        .withColumn("_bronze_ingestion_timestamp", F.col("_ingestion_timestamp"))
        .withColumn("_job_run_id",                 F.lit(etl_meta["job_run_id"]))
        .withColumn("_notebook_path",              F.lit(etl_meta["notebook_path"]))
        .withColumn("_source_system",              F.lit(etl_meta["source_system"]))
        .withColumn("_batch_year",                 F.lit(year))
    )

# COMMAND ----------

# ------------------------------------------------------------
# compute_year_metrics
# ------------------------------------------------------------
def compute_year_metrics(
    df:                   DataFrame,
    track_late_arrivals:  bool = False,
) -> Dict[str, int]:
    """
    Computes per-year loop metrics in a single Spark action,
    replacing the pattern of calling count() separately for
    total rows, quarantine rows, and late arrival rows.

    Why single-pass matters here:
        The per-year loop processes one Bronze partition at a time.
        The old pattern triggered 3 separate Spark jobs on the same
        DataFrame (total count, quarantine filter count, late arrival
        filter count). Spark cannot reuse the shuffle/scan results
        across separate actions. With a combined agg(), all three
        metrics are computed in one scan, one shuffle, one job.

        At scale (e.g. 20M ratings in a single year partition), the
        savings are 2 full data scans per year, per run.

    Args:
        df:                   Silver DataFrame after transformation + DQ flagging
                              + metadata appended — the final per-year DataFrame
                              that will be written to the Silver table.
        track_late_arrivals:  True for tables that have is_late_arrival column
                              (ratings). False for tables without it (tags).

    Returns:
        dict with keys:
            total         — total records in this year's batch
            quarantine    — records with _dq_status = 'QUARANTINE'
            late_arrivals — records with is_late_arrival = True
                            (only present when track_late_arrivals=True)
    """
    agg_exprs = [
        F.count("*").alias("total"),
        F.sum(F.when(F.col("_dq_status") == "QUARANTINE", 1).otherwise(0)).alias("quarantine"),
    ]

    if track_late_arrivals:
        agg_exprs.append(
            F.sum(F.when(F.col("is_late_arrival") == True, 1).otherwise(0)).alias("late_arrivals")
        )

    result = df.agg(*agg_exprs).collect()[0]

    metrics = {
        "total":      result["total"],
        "quarantine": result["quarantine"],
    }
    if track_late_arrivals:
        metrics["late_arrivals"] = result["late_arrivals"]

    return metrics

# COMMAND ----------

# ------------------------------------------------------------
# apply_dq_flags
# ------------------------------------------------------------
def apply_dq_flags(df: DataFrame, dq_rules: List[tuple]) -> DataFrame:
    """
    Evaluates DQ rules per row and attaches _dq_status and
    _dq_failed_rules columns.

    Why per-row DQ flags instead of dropping bad rows:
      Silver's job is to clean and flag — not to silently discard.
      Quarantined rows stay in the table with _dq_status='QUARANTINE'
      so analysts can audit them. Gold filters to PASS rows only.

    Args:
        df: DataFrame after transformation
        dq_rules: List of (rule_name, condition_column_expr) tuples.
                  condition_column_expr must be a Column that evaluates
                  to True when the row FAILS the rule.

    Example:
        dq_rules = [
            ("NULL_USER_ID",      F.col("user_id").isNull()),
            ("INVALID_RATING",    ~F.col("rating").between(0.0, 5.0)),
        ]

    Columns added:
      _dq_status      : STRING  — "PASS" or "QUARANTINE"
      _dq_failed_rules: ARRAY<STRING> — list of rule names that failed
                        e.g. ["NULL_USER_ID", "INVALID_RATING"]
                        Empty array for PASS rows.
    """
    failed_rules_col = F.array_compact(
        F.array(*[
            F.when(fail_condition, F.lit(rule_name))
            for rule_name, fail_condition in dq_rules
        ])
    )

    return (
        df
        .withColumn("_dq_failed_rules", failed_rules_col)
        .withColumn("_dq_status",
                    F.when(F.size(F.col("_dq_failed_rules")) > 0,
                           F.lit("QUARANTINE"))
                     .otherwise(F.lit("PASS")))
    )

# COMMAND ----------

# ------------------------------------------------------------
# write_static
# ------------------------------------------------------------
def write_static(df: DataFrame, s3_target_path: str, table_name: str) -> int:
    """
    Writes a static Silver table using full overwrite + mergeSchema.

    mergeSchema (not overwriteSchema):
      - New columns added upstream → safely adopted
      - Columns removed or type-changed → write FAILS (breaking change caught)
      This is the correct schema evolution strategy for production.

    Record count strategy:
      Pre-write df.count() removed. Count read from Delta log after
      write via history(1).operationMetrics["numOutputRows"].
      Delta's write contract guarantees this equals the actual
      committed row count — no additional scan needed.

    Returns:
        Record count written (from Delta transaction log)
    """
    print(f"[START] Writing static Silver table: {table_name}")

    try:
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("mergeSchema", "true")
              .save(s3_target_path)
        )
        print(f"[SUCCESS] Written to: {s3_target_path}")

    except Exception as e:
        raise RuntimeError(
            f"FAILED: Write failed for table '{table_name}'. Error: {e}"
        )

    # Read committed count from Delta log — no data scan
    count = read_write_metrics(spark, s3_target_path)
    return count

# COMMAND ----------

# ------------------------------------------------------------
# write_incremental
# ------------------------------------------------------------
def write_incremental(
    df:             DataFrame,
    s3_target_path: str,
    table_name:     str,
    year:           int
) -> int:
    """
    Writes one year's partition to an incremental Silver table
    using replaceWhere for idempotency.

    Write strategy: overwrite + replaceWhere(_batch_year = year)
      - Scopes the overwrite to ONLY _batch_year = year partition
      - All other year partitions are never touched
      - Running the same year N times always produces exactly
        one copy — idempotent by design
      - Delta ACID guarantees: failed writes roll back cleanly,
        so a retry always starts from a clean state

    Record count strategy:
      Pre-write df.count() removed. Count read from Delta log after
      write via history(1).operationMetrics["numOutputRows"].

    Returns:
        Record count written for this year (from Delta transaction log)
    """
    print(f"[START] Writing Silver partition _batch_year={year} for: {table_name}")

    from delta.tables import DeltaTable
    is_new_table = not DeltaTable.isDeltaTable(spark, s3_target_path)

    try:
        writer = (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("replaceWhere", f"_batch_year = {year}")
              .option("mergeSchema", "true")
        )

        # partitionBy is only needed on the FIRST write to establish
        # the partition layout. Subsequent writes inherit the existing
        # partition scheme from Delta table metadata. Specifying it on
        # every write can conflict with replaceWhere and cause Delta
        # to overwrite the entire table instead of a single partition.
        if is_new_table:
            writer = writer.partitionBy("_batch_year")
            print(f"[INFO] New table — partitionBy('_batch_year') applied")

        writer.save(s3_target_path)
        print(f"[SUCCESS] Partition _batch_year={year} written to: {s3_target_path}")

    except Exception as e:
        raise RuntimeError(
            f"FAILED: Write failed for table '{table_name}' year {year}. Error: {e}"
        )

    # Read committed count from Delta log — no data scan
    count = read_write_metrics(spark, s3_target_path)
    return count

# COMMAND ----------

# ------------------------------------------------------------
# write_incremental_merge
# ------------------------------------------------------------
def write_incremental_merge(
    df:                DataFrame,
    full_table_name:   str,
    s3_target_path:    str,
    merge_key_cols:    List[str],
    partition_by:      List[str],
    scd2_natural_key:  List[str],
    dedup_order_col:   str = "_processing_timestamp",
) -> dict:
    """
    Writes to a Silver Delta table using MERGE with SCD Type-2 semantics.

    Purpose — SCD Type-2 for Silver ratings:
        Silver maintains the FULL history of all rating events. When a
        user re-rates the same movie (same user_id + movie_id), the
        old row is expired (is_current=False, effective_end_date set)
        and the new row is inserted as the current version.

        This allows:
          - Gold to read only is_current=True for latest ratings (BI)
          - ML models to read Silver directly for full history

    SCD2 MERGE strategy:
        Delta MERGE cannot UPDATE + INSERT for the same matched row in
        a single pass. The standard SCD2 pattern uses a staging approach:

        1. Build a staging view with TWO row types from the source:
           a) 'update' rows — carry the values needed to expire old rows
           b) 'insert' rows — the actual new data to insert

        2. MERGE with conditions:
           WHEN MATCHED AND merge_action = 'update' → SET is_current=False, effective_end_date=...
           WHEN NOT MATCHED AND merge_action = 'insert' → INSERT *

        The staging view is a UNION ALL of:
           - Source joined to target (matched on natural key, newer timestamp)
             → tagged 'update', carrying target's merge_key + new end_date
           - Original source → tagged 'insert'

    First-run handling:
        On first run, all rows bootstrap with is_current=True.

    Args:
        df:              DataFrame with SCD2 columns already added
        full_table_name: Fully qualified Unity Catalog table name
        s3_target_path:  S3 Delta location
        merge_key_cols:  Columns forming the unique event key
                         (e.g. [user_id, movie_id, interaction_timestamp])
        partition_by:    Partition columns (e.g. ["rating_year"])
        scd2_natural_key: Natural key for SCD2 versioning
                         (e.g. [user_id, movie_id])
        dedup_order_col: Column for deterministic dedup ordering

    Returns:
        dict with keys:
            rows_inserted, rows_updated, rows_affected,
            duplicates_dropped, scd2_expirations
    """
    from pyspark.sql import Window

    total_incoming = df.count()
    print(f"[START] write_incremental_merge (SCD2) → {full_table_name}")
    print(f"[INFO]  Incoming records  : {total_incoming:,}")
    print(f"[INFO]  Merge keys        : {merge_key_cols}")
    print(f"[INFO]  SCD2 natural key  : {scd2_natural_key}")

    # Pre-merge deduplication — deterministic: latest dedup_order_col wins
    dedup_window = (
        Window
        .partitionBy(merge_key_cols)
        .orderBy(F.col(dedup_order_col).desc())
    )
    df_deduped = (
        df
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )

    deduped_count      = df_deduped.count()
    duplicates_dropped = total_incoming - deduped_count

    if duplicates_dropped > 0:
        print(f"[WARN]  Duplicates dropped before MERGE : {duplicates_dropped:,}")
    else:
        print(f"[INFO]  No duplicates — pre-merge dedup was a no-op")

    # First-run detection
    table_exists = spark.catalog.tableExists(full_table_name)

    if not table_exists:
        print(f"[INFO]  First run detected — bootstrapping table with full write")

        # On first run, within this batch itself, a user may have rated
        # the same movie multiple times. We need to set is_current correctly:
        # only the LATEST interaction_timestamp per (user_id, movie_id)
        # should be is_current=True.
        scd2_window = (
            Window
            .partitionBy(scd2_natural_key)
            .orderBy(F.col("interaction_timestamp").desc())
        )
        df_bootstrap = (
            df_deduped
            .withColumn("_scd2_rank", F.row_number().over(scd2_window))
            .withColumn("is_current",
                        F.when(F.col("_scd2_rank") == 1, F.lit(True))
                         .otherwise(F.lit(False)))
            .withColumn("effective_end_date",
                        F.when(F.col("_scd2_rank") == 1, F.lit(None).cast("timestamp"))
                         .otherwise(
                             F.lead("interaction_timestamp")
                              .over(Window.partitionBy(scd2_natural_key)
                                         .orderBy(F.col("interaction_timestamp").asc()))
                         ))
            .drop("_scd2_rank")
        )

        writer = (
            df_bootstrap.write
                        .format("delta")
                        .mode("overwrite")
                        .option("mergeSchema", "true")
        )
        if full_table_name.endswith(".ratings"):
            writer = writer.option("delta.enableChangeDataFeed", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(s3_target_path)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING DELTA
            LOCATION '{s3_target_path}'
        """)

        bootstrapped_count = read_write_metrics(spark, s3_target_path)
        print(f"[SUCCESS] Table bootstrapped : {bootstrapped_count:,} rows written")

        return {
            "rows_inserted":      bootstrapped_count,
            "rows_updated":       0,
            "rows_affected":      bootstrapped_count,
            "duplicates_dropped": duplicates_dropped,
            "scd2_expirations":   0,
        }

    # ----------------------------------------------------------------
    # Incremental run — key-scoped SCD2 recomputation
    #
    # Why this strategy:
    #   The older staging MERGE pattern could insert retroactive rows as
    #   is_current=True. Here we recompute the full timeline for each
    #   affected natural key from target+incoming together, then replace
    #   only those keys atomically.
    # ----------------------------------------------------------------
    target_df = spark.table(full_table_name)
    affected_keys_df = df_deduped.select(*scd2_natural_key).distinct()
    affected_keys_count = affected_keys_df.count()

    target_affected = target_df.join(affected_keys_df, on=scd2_natural_key, how="inner")
    target_affected_count = target_affected.count()

    combined = (
        target_affected
        .withColumn("_is_incoming", F.lit(0))
        .unionByName(
            df_deduped.withColumn("_is_incoming", F.lit(1)),
            allowMissingColumns=False
        )
    )

    # De-dupe exact event keys; incoming rows win over existing target rows.
    event_dedup_window = (
        Window
        .partitionBy(merge_key_cols)
        .orderBy(F.col("_is_incoming").desc(), F.col(dedup_order_col).desc())
    )
    combined_event_deduped = (
        combined
        .withColumn("_event_rank", F.row_number().over(event_dedup_window))
        .filter(F.col("_event_rank") == 1)
        .drop("_event_rank")
    )

    # Recompute timeline per natural key by event-time ordering.
    scd2_desc_window = (
        Window
        .partitionBy(scd2_natural_key)
        .orderBy(F.col("interaction_timestamp").desc(), F.col(dedup_order_col).desc())
    )
    scd2_asc_window = (
        Window
        .partitionBy(scd2_natural_key)
        .orderBy(F.col("interaction_timestamp").asc(), F.col(dedup_order_col).asc())
    )

    recomputed_affected = (
        combined_event_deduped
        .withColumn("_scd2_rank", F.row_number().over(scd2_desc_window))
        .withColumn(
            "is_current",
            F.when(F.col("_scd2_rank") == 1, F.lit(True)).otherwise(F.lit(False))
        )
        .withColumn(
            "effective_end_date",
            F.when(F.col("_scd2_rank") == 1, F.lit(None).cast("timestamp"))
             .otherwise(F.lead("interaction_timestamp").over(scd2_asc_window))
        )
        .drop("_scd2_rank", "_is_incoming")
    )

    # Detach from source-table lineage before mutating target table.
    # localCheckpoint(eager=True) prevents post-delete recomputation.
    recomputed_affected = recomputed_affected.localCheckpoint(eager=True)
    recomputed_count = recomputed_affected.count()
    rows_inserted = max(recomputed_count - target_affected_count, 0)
    rows_updated = min(target_affected_count, recomputed_count)

    nk_delete_clause = " AND ".join(
        f"target.{col} = keys.{col}" for col in scd2_natural_key
    )

    print(f"[INFO]  Rewriting SCD2 timelines for {affected_keys_count:,} natural keys")
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, full_table_name)
        .alias("target")
        .merge(affected_keys_df.alias("keys"), nk_delete_clause)
        .whenMatchedDelete()
        .execute()
    )

    (
        recomputed_affected.write
            .format("delta")
            .mode("append")
            .save(s3_target_path)
    )

    # Invariant checks for affected natural keys only.
    verify_df = spark.table(full_table_name).join(affected_keys_df, on=scd2_natural_key, how="inner")

    duplicate_currents = (
        verify_df.groupBy(*scd2_natural_key)
        .agg(F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("current_count"))
        .filter(F.col("current_count") > 1)
        .count()
    )
    missing_current = (
        verify_df.groupBy(*scd2_natural_key)
        .agg(F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("current_count"))
        .filter(F.col("current_count") == 0)
        .count()
    )
    current_with_end_date = verify_df.filter(
        (F.col("is_current") == True) & F.col("effective_end_date").isNotNull()
    ).count()
    historical_without_end_date = verify_df.filter(
        (F.col("is_current") == False) & F.col("effective_end_date").isNull()
    ).count()

    if duplicate_currents > 0 or missing_current > 0 or current_with_end_date > 0 or historical_without_end_date > 0:
        raise RuntimeError(
            "FAILED: SCD2 invariant violation after incremental write. "
            f"duplicate_currents={duplicate_currents}, "
            f"missing_current={missing_current}, "
            f"current_with_end_date={current_with_end_date}, "
            f"historical_without_end_date={historical_without_end_date}"
        )

    rows_affected = rows_inserted + rows_updated

    print(f"[SUCCESS] SCD2 incremental rewrite completed")
    print(f"          Rows inserted (estimated)    : {rows_inserted:,}")
    print(f"          Rows updated  (estimated)    : {rows_updated:,}")
    print(f"          Rows affected                : {rows_affected:,}")

    return {
        "rows_inserted":      rows_inserted,
        "rows_updated":       rows_updated,
        "rows_affected":      rows_affected,
        "duplicates_dropped": duplicates_dropped,
        "scd2_expirations":   rows_updated,
    }

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

print("[INFO] silver_utils loaded successfully")
print("[INFO] Available functions:")
_utils_functions = [
    "get_available_years_from_source()",
    "get_already_processed_years()",
    "read_bronze()",
    "append_static_metadata()",
    "append_incremental_metadata()",
    "compute_year_metrics()",
    "apply_dq_flags()",
    "write_static()",
    "write_incremental()",
    "write_incremental_merge()",
]
for fn in _utils_functions:
    print(f"  - {fn}")
