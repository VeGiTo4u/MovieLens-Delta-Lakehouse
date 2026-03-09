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
#   validate_inputs()              — s3 path + table name checks
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

# COMMAND ----------

# ------------------------------------------------------------
# resolve_etl_metadata
# ------------------------------------------------------------
def resolve_etl_metadata() -> Dict[str, str]:
    """
    Resolves job-level ETL metadata from Databricks notebook context.

    Uses dbruntime.databricks_repl_context — the stable Python-native
    API (DBR 14.1+). Works on all cluster modes including Unity Catalog
    Shared Access Mode where dbutils.notebook.entry_point raises
    Py4JSecurityException.

    Returns:
        dict with keys: job_run_id, notebook_path, source_system
    """
    from dbruntime.databricks_repl_context import get_context

    _ctx = get_context()

    job_id       = _ctx.jobId       or os.environ.get("DATABRICKS_JOB_ID", "INTERACTIVE")
    run_id       = _ctx.currentRunId or "INTERACTIVE"
    job_run_id   = f"{job_id}_{run_id}"
    notebook_path = _ctx.notebookPath or "UNKNOWN"
    source_system = "S3_MovieLens"

    print(f"[INFO] ETL Metadata resolved:")
    print(f"  _job_run_id    : {job_run_id}")
    print(f"  _notebook_path : {notebook_path}")
    print(f"  _source_system : {source_system}")

    return {
        "job_run_id":    job_run_id,
        "notebook_path": notebook_path,
        "source_system": source_system,
    }

# COMMAND ----------

# ------------------------------------------------------------
# validate_inputs
# ------------------------------------------------------------
def validate_inputs(
    s3_target_path:    str,
    source_table_name: str,
    target_table_name: str
) -> str:
    """
    Validates and normalizes widget inputs. Fails fast on
    misconfigured jobs before any Spark work is done.

    Returns:
        Normalized s3_target_path (guaranteed trailing slash)
    """
    if not s3_target_path or not s3_target_path.startswith("s3://"):
        raise ValueError(f"CONFIGURATION ERROR: Invalid target path '{s3_target_path}'")

    if not source_table_name:
        raise ValueError("CONFIGURATION ERROR: source_table_name not provided")

    if not target_table_name:
        raise ValueError("CONFIGURATION ERROR: target_table_name not provided")

    if not s3_target_path.endswith("/"):
        s3_target_path += "/"

    print("[INFO] Input validation passed")
    return s3_target_path

# COMMAND ----------

# ------------------------------------------------------------
# build_table_names
# ------------------------------------------------------------
def build_table_names(
    source_catalog: str,
    source_schema:  str,
    source_table:   str,
    target_catalog: str,
    target_schema:  str,
    target_table:   str
) -> tuple:
    """
    Builds fully qualified Unity Catalog table names.

    Returns:
        (source_full_name, target_full_name)
    """
    source_full = f"{source_catalog}.{source_schema}.{source_table}"
    target_full = f"{target_catalog}.{target_schema}.{target_table}"

    print(f"[INFO] Source table : {source_full}")
    print(f"[INFO] Target table : {target_full}")

    return source_full, target_full

# COMMAND ----------

# ------------------------------------------------------------
# get_partition_years
# ------------------------------------------------------------
def get_partition_years(full_table_name: str) -> set:
    """
    Returns the set of _batch_year partition values already committed
    in a Silver TARGET Delta table — used exclusively for the
    incrementality check on the Silver table being written to.

    Uses SHOW PARTITIONS — a pure metadata read against the Delta
    transaction log. Zero data files are opened.

    Semantics of empty set:
        An empty return always means the Silver target does not exist
        yet (first run) — which is the correct signal to process ALL
        available Bronze years. This function must NOT be called on
        the Bronze source table because the two failure modes are
        semantically different:

          Silver target missing → empty set → process everything  ✓
          Bronze source missing → empty set → process nothing     ✗ (wrong)

        For the Bronze source, use get_available_years_from_source()
        which has an explicit fallback and fails hard if Bronze is absent.

    Partition format returned by Databricks SHOW PARTITIONS:
        Each row has a "partition" column with value like:
        "_batch_year=2022"

    Returns:
        Set of _batch_year values as ints.
        Empty set if table does not exist (first run).
    """
    try:
        rows = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()
        years = set()
        for row in rows:
            val = str(row[0]).strip()
            # Handle both DBR formats:
            #   "_batch_year=2022"  (key=value from some DBR versions)
            #   "2022"             (plain value from other DBR versions)
            if "_batch_year=" in val:
                years.add(int(val.split("_batch_year=")[1]))
            else:
                years.add(int(val))
        return years
    except AnalysisException as e:
        error_msg = str(e)
        # Table does not exist yet (first run) — expected, return empty set.
        if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "table or view" in error_msg.lower():
            print(f"[INFO] Table not found (expected on first run): {full_table_name}")
            return set()
        # Any other AnalysisException is unexpected — propagate so the
        # operator sees the real error instead of silently reprocessing.
        raise

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
    already_processed = get_partition_years(target_full_table_name)

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
    count = _read_write_metrics(s3_target_path)
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
    count = _read_write_metrics(s3_target_path)
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
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(s3_target_path)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING DELTA
            LOCATION '{s3_target_path}'
        """)

        bootstrapped_count = _read_write_metrics(s3_target_path)
        print(f"[SUCCESS] Table bootstrapped : {bootstrapped_count:,} rows written")

        return {
            "rows_inserted":      bootstrapped_count,
            "rows_updated":       0,
            "rows_affected":      bootstrapped_count,
            "duplicates_dropped": duplicates_dropped,
            "scd2_expirations":   0,
        }

    # ----------------------------------------------------------------
    # Incremental run — SCD2 MERGE
    #
    # Step 1: Register incoming data as temp view
    # Step 2: Build staging view with 'update' + 'insert' rows
    # Step 3: Run MERGE with SCD2 conditions
    # ----------------------------------------------------------------
    source_view = f"_merge_source_{full_table_name.replace('.', '_')}"
    df_deduped.createOrReplaceTempView(source_view)

    # Build ON clause for SCD2 natural key
    nk_on_clause = " AND ".join(
        f"target.{col} = staging.{col}" for col in scd2_natural_key
    )

    # Build ON clause for exact match (merge key = full event key)
    mk_on_clause = " AND ".join(
        f"target.{col} = staging.{col}" for col in merge_key_cols
    )

    # Source columns for the staging view — exclude SCD2 management columns
    # (they'll be set by the MERGE logic)
    source_cols = [c for c in df_deduped.columns
                   if c not in ("is_current", "effective_end_date")]
    source_select = ", ".join(f"source.{c}" for c in source_cols)

    print(f"[INFO]  Building SCD2 staging view")

    # STAGING VIEW:
    # Part 1 (update rows): For each incoming row that matches an existing
    #   CURRENT row on natural key with a NEWER timestamp → create an
    #   'update' row that carries the target's merge_key values so the
    #   MERGE can find and expire the old row.
    # Part 2 (insert rows): All incoming rows → tag as 'insert'
    staging_view = f"_scd2_staging_{full_table_name.replace('.', '_')}"

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {staging_view} AS

        -- Part 1: Rows to EXPIRE existing current versions
        SELECT
            {', '.join(f'target.{c}' for c in source_cols)},
            CAST(NULL AS BOOLEAN) AS is_current,
            CAST(NULL AS TIMESTAMP) AS effective_end_date,
            'update' AS _merge_action,
            source.interaction_timestamp AS _new_effective_end_date
        FROM {full_table_name} AS target
        INNER JOIN {source_view} AS source
            ON  {nk_on_clause}
        WHERE target.is_current = TRUE
          AND source.interaction_timestamp > target.interaction_timestamp

        UNION ALL

        -- Part 2: All incoming rows to INSERT as new versions
        SELECT
            {source_select},
            TRUE AS is_current,
            CAST(NULL AS TIMESTAMP) AS effective_end_date,
            'insert' AS _merge_action,
            CAST(NULL AS TIMESTAMP) AS _new_effective_end_date
        FROM {source_view} AS source
    """)

    print(f"[INFO]  Running SCD2 MERGE INTO {full_table_name}")

    # The MERGE:
    #   MATCHED + _merge_action='update' → expire old row
    #   NOT MATCHED + _merge_action='insert' → insert new row as current
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING {staging_view} AS staging
        ON  {mk_on_clause}
        WHEN MATCHED AND staging._merge_action = 'update' THEN
            UPDATE SET
                target.is_current = FALSE,
                target.effective_end_date = staging._new_effective_end_date
        WHEN NOT MATCHED AND staging._merge_action = 'insert' THEN
            INSERT ({', '.join(df_deduped.columns)})
            VALUES ({', '.join(f'staging.{c}' for c in df_deduped.columns)})
    """)

    # MERGE metrics from Delta commit log
    try:
        merge_metrics = (
            spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1")
                 .select("operationMetrics")
                 .collect()[0]["operationMetrics"]
        )
        rows_inserted = int(merge_metrics.get("numTargetRowsInserted", 0))
        rows_updated  = int(merge_metrics.get("numTargetRowsUpdated",  0))
    except Exception:
        rows_inserted = -1
        rows_updated  = -1

    rows_affected = (
        rows_inserted + rows_updated
        if rows_inserted >= 0
        else deduped_count
    )

    print(f"[SUCCESS] SCD2 MERGE completed")
    print(f"          Rows inserted (new versions) : {rows_inserted:,}")
    print(f"          Rows updated  (expirations)  : {rows_updated:,}")
    print(f"          Rows affected                : {rows_affected:,}")

    return {
        "rows_inserted":      rows_inserted,
        "rows_updated":       rows_updated,
        "rows_affected":      rows_affected,
        "duplicates_dropped": duplicates_dropped,
        "scd2_expirations":   rows_updated,
    }

# COMMAND ----------

# ------------------------------------------------------------
# _read_write_metrics  (internal helper)
# ------------------------------------------------------------
def _read_write_metrics(s3_target_path: str) -> int:
    """
    Reads numOutputRows from the Delta transaction log entry
    produced by the most recent write operation.

    Uses DeltaTable.forPath so it works before Unity Catalog
    registration. Reads a single _delta_log JSON — no data scan.

    Returns:
        numOutputRows as int, or -1 if metrics cannot be read.
    """
    from delta.tables import DeltaTable
    try:
        metrics = (
            DeltaTable.forPath(spark, s3_target_path)
                      .history(1)
                      .select("operationMetrics")
                      .collect()[0]["operationMetrics"]
        )
        count = int(metrics.get("numOutputRows", -1))
        print(f"[INFO] Records committed (Delta log): {count:,}")
        return count
    except Exception as e:
        print(f"[WARN] Could not read write metrics from Delta log: {e}")
        return -1

# COMMAND ----------

# ------------------------------------------------------------
# register_table
# ------------------------------------------------------------
def register_table(target_full_table_name: str, s3_target_path: str) -> None:
    """
    Registers the Delta table in Unity Catalog.
    CREATE TABLE IF NOT EXISTS is a no-op on subsequent runs —
    safe to call on every execution.

    Storage and metadata are intentionally decoupled:
    the Delta files exist on S3 independently of the catalog
    entry. Dropping the catalog entry does not delete data.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_full_table_name}
        USING DELTA
        LOCATION '{s3_target_path}'
    """)
    print(f"[SUCCESS] Table registered: {target_full_table_name}")

# COMMAND ----------

# ------------------------------------------------------------
# post_write_validation
# ------------------------------------------------------------
def post_write_validation(
    target_full_table_name: str,
    expected_count:         int
) -> None:
    """
    Validates the registered Silver table after write.

    What this checks:
      1. Logs the expected record count (from Delta transaction log
         via write_static/write_incremental return values — no re-scan).
      2. All ETL metadata columns are present and non-NULL.
         This is the real correctness check — NULL metadata means
         context resolution or the write itself silently failed.

    Why the count re-scan was removed:
        The old implementation called df_val.count() from the
        registered table and compared it to expected_count. Since
        expected_count was already derived from the write operation,
        this was comparing a value to itself via a full data scan —
        circular logic with real cost. Delta ACID guarantees that
        if write() returned without exception, the committed count
        equals what the log reports. The NULL metadata check below
        is where the actual validation value lies.

    The NULL check still scans the table — this is intentional.
    Metadata correctness cannot be inferred from the Delta log alone.
    """
    print("[START] Post-write validation")
    print(f"[INFO]  Expected records (from Delta log): {expected_count:,}")

    try:
        df_val = spark.table(target_full_table_name)

        meta_cols = [
            "_processing_timestamp",
            "_bronze_ingestion_timestamp",
            "_job_run_id",
            "_notebook_path",
            "_dq_status",
            "_dq_failed_rules",
        ]

        # Only check _batch_year if it exists (incremental tables only)
        if "_batch_year" in df_val.columns:
            meta_cols.append("_batch_year")

        # Single-pass aggregation — all NULL checks in one Spark action
        null_counts = df_val.select([
            F.count(F.when(F.col(c).isNull(), 1)).alias(c)
            for c in meta_cols
        ]).collect()[0]

        all_passed = True
        for col_name in meta_cols:
            null_count = null_counts[col_name]
            status     = "[PASS]" if null_count == 0 else "[FAIL]"
            if null_count > 0:
                all_passed = False
            print(f"  {status} {col_name}: {null_count:,} NULLs")

        if not all_passed:
            raise ValueError(
                "FAILED: NULL values found in ETL metadata columns. "
                "Check context resolution and write logic."
            )

        print("[SUCCESS] Post-write validation passed")

    except Exception as e:
        raise RuntimeError(f"Post-write validation failed: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# print_summary
# ------------------------------------------------------------
def print_summary(
    source_full_table_name: str,
    target_full_table_name: str,
    s3_target_path:         str,
    initial_count:          int,
    final_count:            int,
    etl_meta:               Dict[str, str],
    extra_info:             Dict[str, Any] = None
) -> None:
    """
    Prints a standardized run summary for every Silver notebook.
    extra_info accepts table-specific metrics to append at the end.
    """
    records_removed = initial_count - final_count
    removal_pct     = (records_removed / initial_count * 100) if initial_count > 0 else 0

    print("\n" + "=" * 70)
    print("SILVER LAYER TRANSFORMATION SUMMARY")
    print("=" * 70)
    print(f"Source Table         : {source_full_table_name}")
    print(f"Target Table         : {target_full_table_name}")
    print(f"Target Location      : {s3_target_path}")
    print(f"\nRecord Counts")
    print(f"  Bronze (input)     : {initial_count:,}")
    print(f"  Silver (output)    : {final_count:,}")
    print(f"  Removed (DQ/clean) : {records_removed:,} ({removal_pct:.2f}%)")
    print(f"\nETL Metadata")
    print(f"  _job_run_id        : {etl_meta['job_run_id']}")
    print(f"  _notebook_path     : {etl_meta['notebook_path']}")

    if extra_info:
        print(f"\nTable-Specific Metrics")
        for key, val in extra_info.items():
            print(f"  {key:<22}: {val}")

    print("=" * 70)
    print("[END] Silver transformation completed successfully")
    print("=" * 70)

# COMMAND ----------

print("[INFO] silver_utils loaded successfully")
print("[INFO] Available functions:")
_utils_functions = [
    "resolve_etl_metadata()",
    "validate_inputs()",
    "build_table_names()",
    "get_partition_years()",
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
    "register_table()",
    "post_write_validation()",
    "print_summary()",
]
for fn in _utils_functions:
    print(f"  - {fn}")