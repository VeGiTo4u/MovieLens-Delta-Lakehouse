# Databricks notebook source
# ============================================================
# gold_utils
# Shared utility functions for all Gold layer notebooks.
#
# Usage in each Gold notebook:
#   %run ./gold_utils
#
# Functions provided:
#   resolve_etl_metadata()         — job/run context resolution
#   validate_inputs()              — s3 path + table name checks
#   build_table_names()            — fully qualified Unity Catalog names
#   get_partition_years()          — metadata-only year discovery via SHOW PARTITIONS
#   read_silver_pass_only()        — read silver filtered to _dq_status=PASS (single-pass agg)
#   get_silver_version()           — Delta table version at read time
#   generate_surrogate_key()       — SHA2-256 surrogate key from natural key cols
#   append_gold_metadata()         — 6 ETL metadata columns
#   write_gold()                   — full overwrite + mergeSchema (optional partitionBy)
#   write_gold_merge()             — MERGE upsert for late-arrival-safe incremental writes
#   (OPTIMIZE/ANALYZE/VACUUM are now in scripts/maintenance/utils.py)
#   register_table()               — CREATE TABLE IF NOT EXISTS
#   post_write_validation_gold()   — Gold contracts (PK, non-null, FK, metadata)
#   print_summary()                — standardized run summary
# ============================================================

# COMMAND ----------

import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from typing import Dict, List, Any, Optional

# COMMAND ----------

# ------------------------------------------------------------
# resolve_etl_metadata
# ------------------------------------------------------------
def resolve_etl_metadata() -> Dict[str, str]:
    """
    Resolves job-level ETL metadata from Databricks notebook context.

    Uses dbruntime.databricks_repl_context — stable Python-native API
    (DBR 14.1+). Works on all cluster modes including Unity Catalog
    Shared Access Mode.

    Returns:
        dict with keys: job_run_id, notebook_path
    """
    from dbruntime.databricks_repl_context import get_context

    _ctx = get_context()

    job_id        = _ctx.jobId        or os.environ.get("DATABRICKS_JOB_ID", "INTERACTIVE")
    run_id        = _ctx.currentRunId or "INTERACTIVE"
    job_run_id    = f"{job_id}_{run_id}"
    notebook_path = _ctx.notebookPath or "UNKNOWN"

    print("[INFO] ETL Metadata resolved:")
    print(f"  _job_run_id    : {job_run_id}")
    print(f"  _notebook_path : {notebook_path}")

    return {
        "job_run_id":    job_run_id,
        "notebook_path": notebook_path,
    }

# COMMAND ----------

# ------------------------------------------------------------
# validate_inputs
# ------------------------------------------------------------
def validate_inputs(
    s3_target_path:    str,
    target_table_name: str,
    source_table_name: str = None
) -> str:
    """
    Validates and normalizes widget inputs.
    source_table_name is optional — dim_date has no silver source.

    Returns:
        Normalized s3_target_path (guaranteed trailing slash)
    """
    if not s3_target_path or not s3_target_path.startswith("s3://"):
        raise ValueError(f"CONFIGURATION ERROR: Invalid target path '{s3_target_path}'")

    if not target_table_name:
        raise ValueError("CONFIGURATION ERROR: target_table_name not provided")

    if source_table_name is not None and not source_table_name:
        raise ValueError("CONFIGURATION ERROR: source_table_name not provided")

    if not s3_target_path.endswith("/"):
        s3_target_path += "/"

    print("[INFO] Input validation passed")
    return s3_target_path

# COMMAND ----------

# ------------------------------------------------------------
# build_table_names
# ------------------------------------------------------------
def build_table_names(
    target_catalog: str,
    target_schema:  str,
    target_table:   str,
    source_catalog: str = None,
    source_schema:  str = None,
    source_table:   str = None,
) -> tuple:
    """
    Builds fully qualified Unity Catalog table names.
    Source params are optional for generated tables (dim_date).

    Returns:
        (target_full_name, source_full_name)
        source_full_name is None if source params not provided.
    """
    target_full = f"{target_catalog}.{target_schema}.{target_table}"
    source_full = (
        f"{source_catalog}.{source_schema}.{source_table}"
        if all([source_catalog, source_schema, source_table])
        else None
    )

    print(f"[INFO] Target table : {target_full}")
    if source_full:
        print(f"[INFO] Source table : {source_full}")

    return target_full, source_full

# COMMAND ----------

# ------------------------------------------------------------
# get_partition_years
# ------------------------------------------------------------
def get_partition_years(full_table_name: str) -> set:
    """
    Returns the set of partition year values for a Delta table
    using SHOW PARTITIONS — a pure metadata read against the
    Delta transaction log. Zero data files are opened.

    Supports both partition column formats:
      - _batch_year=2022  (Bronze/Silver tags, Gold tables with _batch_year)
      - rating_year=2022  (Silver ratings, Gold fact_ratings)
      - plain "2022"      (some DBR versions)

    Returns:
        Set of year values as ints.
        Empty set if table does not exist (first run).
    """
    try:
        rows = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()
        years = set()
        for row in rows:
            val = str(row[0]).strip()
            # Handle all DBR formats:
            #   "_batch_year=2022"  (key=value from some DBR versions)
            #   "rating_year=2022"  (Silver/Gold ratings post-refactor)
            #   "2022"             (plain value from other DBR versions)
            if "_batch_year=" in val:
                years.add(int(val.split("_batch_year=")[1]))
            elif "rating_year=" in val:
                years.add(int(val.split("rating_year=")[1]))
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
    Returns the set of _batch_year values available in the Silver
    source table — used for Gold incrementality to determine which
    source batches have been ingested.

    Post-architecture-refactor:
        Silver ratings is now partitioned by rating_year (event year),
        NOT _batch_year. _batch_year is a non-partition column retained
        for audit and incrementality tracking. Since _batch_year is no
        longer a partition column, SHOW PARTITIONS cannot discover it.
        We read distinct _batch_year values directly from the table.

        This is a SELECT DISTINCT on a non-partition column, but
        _batch_year has very low cardinality (~10 values for 10 years
        of data). Spark only needs to read the column from the Parquet
        column index, which is fast even on large tables.

    If Silver does not exist, that is always a hard pipeline
    configuration error — Gold cannot run without Silver — so we
    fail explicitly.

    Returns:
        Set of available _batch_year values as ints. Never empty
        if Silver exists and has data.
    """
    table_exists = spark.catalog.tableExists(source_full_table_name)

    if not table_exists:
        raise RuntimeError(
            f"FAILED: Silver source table '{source_full_table_name}' does not exist. "
            f"Run the Silver processing notebook before Gold."
        )

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
            f"FAILED: Silver source table '{source_full_table_name}' exists "
            f"but contains no _batch_year values. "
            f"Check Silver processing completed successfully."
        )

    print(f"[INFO] Available _batch_year values in Silver: {sorted(years)}")
    return years

# COMMAND ----------

# ------------------------------------------------------------
# get_processed_batch_years
# ------------------------------------------------------------
def get_processed_batch_years(full_table_name: str) -> set:
    """
    Returns the set of _batch_year values already processed in a
    Gold target table. _batch_year is a non-partition column (tracked
    for incrementality, not used as partition key).

    Post-architecture-refactor:
        Gold fact_ratings is partitioned by rating_year (event year),
        NOT _batch_year. We read distinct _batch_year values from
        the Gold table data directly.

        On first run (table does not exist), returns empty set.

    Returns:
        Set of _batch_year values as ints.
        Empty set if table does not exist (first run).
    """
    try:
        if not spark.catalog.tableExists(full_table_name):
            print(f"[INFO] Table not found (expected on first run): {full_table_name}")
            return set()

        years = {
            row._batch_year
            for row in (
                spark.table(full_table_name)
                     .select("_batch_year")
                     .distinct()
                     .collect()
            )
        }
        print(f"[INFO] Processed _batch_year values in Gold: {sorted(years)}")
        return years
    except AnalysisException as e:
        error_msg = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "table or view" in error_msg.lower():
            print(f"[INFO] Table not found (expected on first run): {full_table_name}")
            return set()
        raise

# COMMAND ----------

# ------------------------------------------------------------
# get_silver_version
# ------------------------------------------------------------
def get_silver_version(source_full_table_name: str) -> Optional[int]:
    """
    Reads the current Delta table version of the silver source.
    This version number is stored in _source_silver_version on
    every gold row — it ties the Gold output to the exact Silver
    snapshot that was read, enabling point-in-time lineage tracing.

    If the version cannot be determined (e.g. non-Delta table),
    returns None rather than failing the pipeline.
    """
    try:
        history = spark.sql(
            f"DESCRIBE HISTORY {source_full_table_name} LIMIT 1"
        ).collect()
        version = history[0]["version"] if history else None
        print(f"[INFO] Silver table version : {version} ({source_full_table_name})")
        return version
    except Exception as e:
        print(f"[WARN] Could not determine silver version: {e}")
        return None

# COMMAND ----------

# ------------------------------------------------------------
# read_silver_pass_only
# ------------------------------------------------------------
def read_silver_pass_only(source_full_table_name: str) -> tuple:
    """
    Reads a Silver Delta table and filters to _dq_status = 'PASS' rows only.

    Why filter here and not repeat DQ checks:
      Silver already evaluated every DQ rule per row and attached
      _dq_status = 'QUARANTINE' to bad rows. Re-running NULL checks,
      duplicate checks, and range checks in Gold is wasted compute
      and a maintenance problem. Gold trusts Silver's DQ work and
      simply excludes quarantined rows.

    Count strategy — single-pass aggregation:
      The old implementation called df_silver.count() for total rows
      then df_pass.count() for PASS rows — two full table scans on
      the same data. We now compute both counts in a single Spark
      action using a conditional sum:
        total      = COUNT(*)
        pass_count = SUM(CASE WHEN _dq_status = 'PASS' THEN 1 ELSE 0)
      One scan, one job, identical results.

    Returns:
        (df_pass, initial_count, quarantine_count)
        df_pass          — DataFrame filtered to PASS rows only (lazy)
        initial_count    — total rows in silver before filtering
        quarantine_count — rows excluded (QUARANTINE)
    """
    print(f"[START] Reading Silver table: {source_full_table_name}")

    try:
        df_silver = spark.table(source_full_table_name)

        # Single-pass aggregation: total + PASS count in one Spark action
        agg_result = df_silver.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("_dq_status") == "PASS", 1).otherwise(0)).alias("pass_count"),
        ).collect()[0]

        initial_count    = agg_result["total"]
        pass_count       = agg_result["pass_count"]
        quarantine_count = initial_count - pass_count

        print(f"[INFO] Total rows in Silver      : {initial_count:,}")
        print(f"[INFO] PASS rows (used in Gold)  : {pass_count:,}")
        print(f"[INFO] QUARANTINE rows (excluded): {quarantine_count:,}")

        if pass_count == 0:
            raise RuntimeError(
                f"FAILED: Zero PASS rows in '{source_full_table_name}'. "
                f"All {initial_count:,} rows are QUARANTINE. "
                f"Check Silver DQ rules before proceeding."
            )

        # Lazy filter — no additional Spark action triggered here
        df_pass = df_silver.filter(F.col("_dq_status") == "PASS")
        return df_pass, initial_count, quarantine_count

    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(
            f"FAILED: Cannot read Silver table '{source_full_table_name}'. "
            f"Ensure the table exists and is registered in Unity Catalog. "
            f"Error: {e}"
        )

# COMMAND ----------

# ------------------------------------------------------------
# append_gold_metadata
# ------------------------------------------------------------
def append_gold_metadata(
    df:                   DataFrame,
    etl_meta:             Dict[str, str],
    source_table:         str,
    model_version:        str,
    silver_version:       Optional[int] = None,
) -> DataFrame:
    """
    Appends 6 ETL metadata columns to every Gold table.

    Columns added:
      _source_table          — fully qualified silver table this row came
                               from. "GENERATED" for programmatic tables
                               like dim_date that have no silver source.
      _job_run_id            — <jobId>_<runId>. Consistent with bronze
                               and silver. Ties this gold row to the
                               exact Databricks Job run log.
      _notebook_path         — workspace path of the gold notebook that
                               produced this row.
      _model_version         — version of the Gold transformation logic.
      _aggregation_timestamp — when the Gold transformation ran.
      _source_silver_version — Delta table version of the silver source
                               at read time. NULL for generated tables.
    """
    return (
        df
        .withColumn("_source_table",
                    F.lit(source_table))
        .withColumn("_job_run_id",
                    F.lit(etl_meta["job_run_id"]))
        .withColumn("_notebook_path",
                    F.lit(etl_meta["notebook_path"]))
        .withColumn("_model_version",
                    F.lit(model_version))
        .withColumn("_aggregation_timestamp",
                    F.current_timestamp())
        .withColumn("_source_silver_version",
                    F.lit(silver_version).cast("int"))
    )

# COMMAND ----------

# ------------------------------------------------------------
# generate_surrogate_key
# ------------------------------------------------------------
def generate_surrogate_key(
    df:               DataFrame,
    sk_col_name:      str,
    *natural_key_cols: str,
) -> DataFrame:
    """
    Adds a SHA2-256 surrogate key column to a DataFrame by hashing
    one or more natural key columns.

    Why SHA2-256 over monotonically_increasing_id():
      - Deterministic: same natural key always produces the same SK.
        Reruns are idempotent — no risk of ID drift between runs.
      - Partition-safe: SHA2 works identically across all Spark
        partitions with no driver-side coordination needed.
      - SCD2-ready: when SCD2 is implemented, the SK becomes
        SHA2(natural_key || effective_start_date).

    Why NOT MD5:
      MD5 has known collision vulnerabilities. SHA2-256 is the
      industry standard for non-cryptographic deterministic hashing.

    Args:
        df:               Input DataFrame
        sk_col_name:      Name of the new surrogate key column
        *natural_key_cols: One or more column names forming the
                          natural key. Multiple columns are
                          pipe-delimited before hashing to avoid
                          collisions between (1,"23") and (12,"3").

    Returns:
        DataFrame with sk_col_name prepended as the first new column.
    """
    if not natural_key_cols:
        raise ValueError("At least one natural_key_col must be provided.")

    if len(natural_key_cols) == 1:
        hash_input = F.col(natural_key_cols[0]).cast("string")
    else:
        hash_input = F.concat_ws(
            "|",
            *[F.col(c).cast("string") for c in natural_key_cols]
        )

    return df.withColumn(sk_col_name, F.sha2(hash_input, 256))

# COMMAND ----------

# ------------------------------------------------------------
# write_gold
# ------------------------------------------------------------
def write_gold(
    df:              DataFrame,
    s3_target_path:  str,
    table_name:      str,
    partition_by:    Optional[List[str]] = None,
) -> int:
    """
    Writes a Gold table using full overwrite + mergeSchema.

    All Gold tables are fully rebuilt on each run — they are
    derived from Silver which is the system of record.

    mergeSchema (not overwriteSchema):
      - New upstream columns → safely adopted
      - Destructive changes (column removal, type change) → write FAILS

    Record count strategy:
      Pre-write df.count() removed. Count read from Delta log after
      write via history(1).operationMetrics["numOutputRows"].
      See bronze_utils docstring for full reasoning.

    Args:
        df:             DataFrame to write
        s3_target_path: Target Delta location on S3
        table_name:     Table name for logging
        partition_by:   Optional list of columns to partition by.

    Returns:
        Record count written (from Delta transaction log)
    """
    print(f"[START] Writing Gold table: {table_name}")
    if partition_by:
        print(f"[INFO]  Partition by    : {partition_by}")

    try:
        writer = (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("mergeSchema", "true")
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(s3_target_path)
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
# write_gold_ratings_replacewhere_partitions
# ------------------------------------------------------------
def write_gold_ratings_replacewhere_partitions(
    df:               DataFrame,
    full_table_name:  str,
    s3_target_path:   str,
    partition_col:    str = "rating_year",
) -> int:
    """
    Writes fact_ratings by replacing only touched event-year partitions.

    This keeps reruns idempotent and avoids rewriting untouched years.
    On first write only, partitionBy(partition_col) is applied to define
    table layout; subsequent writes rely on existing Delta metadata.

    Returns:
        Total rows reported committed across the partition writes.
    """
    from delta.tables import DeltaTable

    year_rows = df.select(partition_col).distinct().collect()
    writing_years = sorted({row[partition_col] for row in year_rows if row[partition_col] is not None})
    if not writing_years:
        print(f"[WARN] No {partition_col} values found — skipping write")
        return 0

    is_new_table = not DeltaTable.isDeltaTable(spark, s3_target_path)
    print(f"[INFO] Writing to {partition_col} partitions: {writing_years}")

    total_written = 0
    for year_val in writing_years:
        df_year = df.filter(F.col(partition_col) == year_val)
        writer = (
            df_year.write
                   .format("delta")
                   .mode("overwrite")
                   .option("replaceWhere", f"{partition_col} = {year_val}")
                   .option("mergeSchema", "true")
        )
        if is_new_table:
            writer = writer.partitionBy(partition_col)

        writer.save(s3_target_path)

        if is_new_table:
            register_table(full_table_name, s3_target_path)
            is_new_table = False

        year_count = _read_write_metrics(s3_target_path)
        total_written += year_count if year_count > 0 else 0
        print(f"[SUCCESS] {partition_col}={year_val}: {year_count:,} rows written")

    return total_written

# COMMAND ----------

# ------------------------------------------------------------
# write_gold_merge
# ------------------------------------------------------------
def write_gold_merge(
    df:               DataFrame,
    full_table_name:  str,
    s3_target_path:   str,
    merge_key_cols:   List[str],
    partition_by:     Optional[List[str]] = None,
) -> dict:
    """
    Writes to a Gold Delta table using MERGE (upsert) semantics.

    Purpose — why MERGE instead of replaceWhere for fact_ratings:
        replaceWhere overwrites an entire year partition atomically.
        With late arrivals (is_late_arrival=True), a 2022 batch can
        contain 2019-timestamped records. A naive replaceWhere on
        rating_year=2019 would erase 5M existing 2019 records and
        replace them with only 150 late arrivals. MERGE solves both
        data loss and idempotency problems simultaneously.

    First-run handling:
        On first run the target table does not exist. We fall back
        to a full write to bootstrap the table, then all subsequent
        runs use MERGE.

    Bootstrap count strategy:
        The old implementation called spark.table(full_table_name).count()
        after the bootstrap write — a full scan of data we just wrote.
        We now read numOutputRows from the Delta log via history(1)
        — same value, no data scan.

    Pre-merge deduplication:
        MERGE fails if the source DataFrame contains duplicate rows on
        the merge key. We deduplicate defensively before merging.

        Why row_number() over dropDuplicates():
            dropDuplicates(merge_key_cols) makes no ordering guarantee.
            Which duplicate is kept depends on Spark partition assignment,
            which depends on shuffle — non-deterministic across runs.
            On a rerun with identical input, a different row could be kept,
            causing a MERGE UPDATE to silently overwrite a Gold row with
            different metadata values than the first run. That is a
            correctness bug, not just a style issue.

            row_number() over an explicit ORDER BY _processing_timestamp DESC
            window guarantees the most recently processed Silver row wins —
            deterministically and repeatably, regardless of shuffle.

    Returns:
        dict with keys:
            rows_inserted      — new rows added
            rows_updated       — existing rows updated
            rows_affected      — total (inserted + updated)
            duplicates_dropped — rows removed in pre-merge dedup
    """
    from pyspark.sql import Window

    total_incoming = df.count()
    print(f"[START] write_gold_merge → {full_table_name}")
    print(f"[INFO]  Incoming records : {total_incoming:,}")
    print(f"[INFO]  Merge keys       : {merge_key_cols}")

    # Pre-merge deduplication — deterministic: latest _processing_timestamp wins
    # row_number() over merge_key_cols partitioned window, ordered by most recent
    # Silver processing time. Rank 1 = the row Silver produced last for that key.
    dedup_window = (
        Window
        .partitionBy(merge_key_cols)
        .orderBy(F.col("_processing_timestamp").desc())
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
        print(f"[WARN]  Silver DQ should prevent this — investigate upstream")
        print(f"[WARN]  Kept row: latest _processing_timestamp per merge key")
    else:
        print(f"[INFO]  No duplicates — pre-merge dedup was a no-op")

    # First-run detection
    table_exists = spark.catalog.tableExists(full_table_name)

    if not table_exists:
        print(f"[INFO]  First run detected — bootstrapping table with full write")

        writer = (
            df_deduped.write
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

        # Read bootstrap count from Delta log — replaces spark.table().count()
        bootstrapped_count = _read_write_metrics(s3_target_path)
        print(f"[SUCCESS] Table bootstrapped : {bootstrapped_count:,} rows written")

        return {
            "rows_inserted":      bootstrapped_count,
            "rows_updated":       0,
            "rows_affected":      bootstrapped_count,
            "duplicates_dropped": duplicates_dropped,
        }

    # Incremental run — register incoming batch as temp view for MERGE SQL
    temp_view = f"_merge_source_{full_table_name.replace('.', '_')}"
    df_deduped.createOrReplaceTempView(temp_view)

    on_clause = "\n    AND ".join(
        f"target.{col} = source.{col}" for col in merge_key_cols
    )

    print(f"[INFO]  Running MERGE INTO {full_table_name}")

    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING {temp_view} AS source
        ON  {on_clause}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    # MERGE metrics from Delta commit log — already a metadata read, no data scan
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

    print(f"[SUCCESS] MERGE completed")
    print(f"          Rows inserted : {rows_inserted:,}")
    print(f"          Rows updated  : {rows_updated:,}")
    print(f"          Rows affected : {rows_affected:,}")

    return {
        "rows_inserted":      rows_inserted,
        "rows_updated":       rows_updated,
        "rows_affected":      rows_affected,
        "duplicates_dropped": duplicates_dropped,
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

# COMMAND ----------
# NOTE: optimize_table() has been removed.
# OPTIMIZE, ANALYZE TABLE, and VACUUM are now handled by the
# dedicated maintenance notebook: scripts/maintenance/jobs/table_maintenance.py
# This decouples maintenance from ETL and allows scheduling
# during off-peak hours.

# COMMAND ----------

# ------------------------------------------------------------
# register_table
# ------------------------------------------------------------
def register_table(full_table_name: str, s3_target_path: str) -> None:
    """
    Registers the Delta table in Unity Catalog.
    CREATE TABLE IF NOT EXISTS is a no-op on subsequent runs.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING DELTA
        LOCATION '{s3_target_path}'
    """)
    print(f"[SUCCESS] Table registered: {full_table_name}")

# COMMAND ----------

# ------------------------------------------------------------
# post_write_validation_gold
# ------------------------------------------------------------
def post_write_validation_gold(
    full_table_name: str,
    expected_count:  int,
    pk_columns:      List[str],
    required_non_null_cols: Optional[List[str]] = None,
    fk_checks: Optional[List[Dict[str, str]]] = None,
) -> None:
    """
    Validates the registered Gold table after write.

    What this checks:
      1. Logs the expected record count (from Delta transaction log
         or MERGE metrics — no re-scan of the full table).
      2. Primary key uniqueness — no duplicate PK combinations.
         This is the first-class correctness check for Gold tables
         that feed BI and ML. A PK violation here means the pipeline
         produced incorrect data.
      3. Required non-null columns are populated (for fact-level contracts).
      4. Foreign keys resolve to their referenced dimension keys.
      5. Gold metadata completeness rules.

    Why the count re-scan was removed:
        The old implementation called df_val.count() and compared it
        to expected_count — both derived from the same table, making
        the check circular. Delta ACID ensures write() committed
        exactly what was written. Re-scanning to confirm that is
        redundant cost with no safety benefit.

    PK check optimization — single Spark action:
        The old implementation ran groupBy().count().filter().count()
        — two separate Spark actions. We now chain the outer .count()
        into an .agg(sum(when > 1)) on the grouped result, reducing
        two actions to one. The groupBy shuffle is the expensive part;
        the final aggregation is a narrow transformation on a small
        grouped result and adds negligible overhead.

    pk_columns:
        list of column names forming the primary key.
        For composite PKs pass multiple columns e.g. ["movie_id", "tag_id"].
    required_non_null_cols:
        optional list of columns that must be non-NULL.
    fk_checks:
        optional list of FK check dicts:
          {
            "fk_column": "movie_sk",
            "reference_table": "movielens.gold.dim_movies",
            "reference_column": "movie_sk",
          }

    Fails hard on any violation — Gold tables feed BI and ML.
    A corrupted Gold table is worse than a failed pipeline.
    """
    print("[START] Post-write validation")
    print(f"[INFO]  Expected records (from write operation): {expected_count:,}")

    required_non_null_cols = required_non_null_cols or []
    fk_checks = fk_checks or []

    df_val = spark.table(full_table_name)

    # Check 1: PK uniqueness — single Spark action
    # groupBy + count gives per-key row counts. The outer agg(sum(when > 1))
    # counts how many keys have more than one row — all in one job.
    pk_dupes = int(
        df_val
        .groupBy(pk_columns)
        .count()
        .agg(F.sum(F.when(F.col("count") > 1, 1).otherwise(0)).alias("dupes"))
        .collect()[0]["dupes"] or 0
    )

    if pk_dupes > 0:
        raise ValueError(
            f"FAILED: PK violation — {pk_dupes:,} duplicate "
            f"({', '.join(pk_columns)}) combinations found."
        )
    print(f"[PASS] PK uniqueness ({', '.join(pk_columns)}): no duplicates")

    # Check 2 + Check 5: required non-null + metadata contracts in one Spark action
    source_table_trim = F.trim(F.col("_source_table"))
    source_is_generated = F.coalesce(source_table_trim == F.lit("GENERATED"), F.lit(False))

    null_agg_exprs = [
        F.count(F.when(F.col(c).isNull(), 1)).alias(c)
        for c in required_non_null_cols
    ] + [
        # _source_table and _job_run_id must be non-null and non-blank
        F.count(
            F.when(
                F.col("_source_table").isNull() | (source_table_trim == ""),
                1,
            )
        ).alias("_source_table_invalid"),
        F.count(
            F.when(
                F.col("_job_run_id").isNull() | (F.trim(F.col("_job_run_id")) == ""),
                1,
            )
        ).alias("_job_run_id_invalid"),
        # Keep existing non-null checks for shared metadata columns
        F.count(F.when(F.col("_notebook_path").isNull(), 1)).alias("_notebook_path"),
        F.count(F.when(F.col("_model_version").isNull(), 1)).alias("_model_version"),
        F.count(F.when(F.col("_aggregation_timestamp").isNull(), 1)).alias("_aggregation_timestamp"),
        # _source_silver_version is nullable only for GENERATED rows (dim_date)
        F.count(
            F.when((~source_is_generated) & F.col("_source_silver_version").isNull(), 1)
        ).alias("_source_silver_version_invalid"),
    ]

    null_counts = df_val.select(null_agg_exprs).collect()[0]

    required_failures = []
    for col_name in required_non_null_cols:
        null_count = int(null_counts[col_name] or 0)
        status = "[PASS]" if null_count == 0 else "[FAIL]"
        print(f"  {status} required non-null {col_name}: {null_count:,} NULLs")
        if null_count > 0:
            required_failures.append(f"{col_name}={null_count:,}")

    metadata_checks = [
        ("_source_table", "_source_table_invalid", "invalid (NULL/blank)"),
        ("_job_run_id", "_job_run_id_invalid", "invalid (NULL/blank)"),
        ("_notebook_path", "_notebook_path", "NULLs"),
        ("_model_version", "_model_version", "NULLs"),
        ("_aggregation_timestamp", "_aggregation_timestamp", "NULLs"),
        ("_source_silver_version", "_source_silver_version_invalid", "invalid for sourced rows"),
    ]

    metadata_failures = []
    for display_name, agg_col, label in metadata_checks:
        invalid_count = int(null_counts[agg_col] or 0)
        status = "[PASS]" if invalid_count == 0 else "[FAIL]"
        print(f"  {status} {display_name}: {invalid_count:,} {label}")
        if invalid_count > 0:
            metadata_failures.append(f"{display_name}={invalid_count:,}")

    if required_failures:
        raise ValueError(
            "FAILED: required non-null column violations: "
            + ", ".join(required_failures)
        )

    if metadata_failures:
        raise ValueError(
            "FAILED: Gold metadata validation failed: "
            + ", ".join(metadata_failures)
        )

    # Check 4: FK referential integrity via distinct left_anti check(s)
    for fk_check in fk_checks:
        fk_column = fk_check.get("fk_column")
        reference_table = fk_check.get("reference_table")
        reference_column = fk_check.get("reference_column")

        if not all([fk_column, reference_table, reference_column]):
            raise ValueError(
                f"FAILED: Invalid fk_checks config: {fk_check}. "
                "Expected keys: fk_column, reference_table, reference_column."
            )

        fk_distinct = (
            df_val
            .select(F.col(fk_column).alias("__fk_value"))
            .where(F.col("__fk_value").isNotNull())
            .distinct()
        )
        ref_distinct = (
            spark.table(reference_table)
                 .select(F.col(reference_column).alias("__ref_value"))
                 .distinct()
        )

        orphan_count = (
            fk_distinct
            .join(ref_distinct, fk_distinct["__fk_value"] == ref_distinct["__ref_value"], "left_anti")
            .count()
        )

        if orphan_count > 0:
            raise ValueError(
                f"FAILED: FK violation — {orphan_count:,} orphan values in "
                f"'{fk_column}' not found in '{reference_table}.{reference_column}'."
            )
        print(
            f"[PASS] FK {fk_column} -> {reference_table}.{reference_column}: "
            "no orphan values"
        )

    print("[SUCCESS] Post-write validation passed")

# COMMAND ----------

# ------------------------------------------------------------
# ensure_gold_batch_audit_table
# ------------------------------------------------------------
def ensure_gold_batch_audit_table(
    catalog_name: str,
    schema_name: str = "gold",
    table_name: str = "gold_pipeline_audit",
) -> str:
    """
    Ensures a Gold batch-audit table exists for replay-safe incrementality.

    Returns:
        Fully qualified audit table name.
    """
    audit_full_name = f"{catalog_name}.{schema_name}.{table_name}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {audit_full_name} (
            target_table STRING,
            source_table STRING,
            batch_year INT,
            source_silver_version INT,
            model_version STRING,
            status STRING,
            processed_at TIMESTAMP,
            job_run_id STRING,
            notebook_path STRING
        )
        USING DELTA
    """)
    print(f"[INFO] Audit table ready: {audit_full_name}")
    return audit_full_name

# COMMAND ----------

# ------------------------------------------------------------
# get_successfully_processed_batches
# ------------------------------------------------------------
def get_successfully_processed_batches(
    audit_full_name: str,
    target_full_name: str,
    source_full_name: str,
    source_silver_version: Optional[int],
    model_version: str,
) -> set:
    """
    Returns successful batch years already processed for the same
    target/source/version/model combination.
    """
    df_audit = spark.table(audit_full_name).filter(
        (F.col("target_table") == target_full_name) &
        (F.col("source_table") == source_full_name) &
        (F.col("status") == "SUCCESS") &
        (F.col("model_version") == model_version)
    )

    if source_silver_version is None:
        df_audit = df_audit.filter(F.col("source_silver_version").isNull())
    else:
        df_audit = df_audit.filter(F.col("source_silver_version") == source_silver_version)

    processed = {
        row.batch_year
        for row in df_audit.select("batch_year").distinct().collect()
    }
    return processed

# COMMAND ----------

# ------------------------------------------------------------
# log_gold_batch_audit
# ------------------------------------------------------------
def log_gold_batch_audit(
    audit_full_name: str,
    target_full_name: str,
    source_full_name: str,
    batch_years: List[int],
    source_silver_version: Optional[int],
    model_version: str,
    status: str,
    etl_meta: Dict[str, str],
) -> None:
    """
    Appends batch-level processing audit records.
    """
    from datetime import datetime

    if not batch_years:
        return

    rows = [
        (
            target_full_name,
            source_full_name,
            int(year),
            source_silver_version,
            model_version,
            status,
            datetime.utcnow(),
            etl_meta["job_run_id"],
            etl_meta["notebook_path"],
        )
        for year in sorted(set(batch_years))
    ]
    schema = (
        "target_table STRING, source_table STRING, batch_year INT, "
        "source_silver_version INT, model_version STRING, status STRING, "
        "processed_at TIMESTAMP, job_run_id STRING, notebook_path STRING"
    )
    spark.createDataFrame(rows, schema=schema).write.format("delta").mode("append").saveAsTable(audit_full_name)
    print(f"[INFO] Logged {len(rows)} batch audit rows to {audit_full_name}")

# COMMAND ----------

# ------------------------------------------------------------
# print_summary
# ------------------------------------------------------------
def print_summary(
    label:              str,
    target_full_name:   str,
    s3_target_path:     str,
    etl_meta:           Dict[str, str],
    model_version:      str,
    extra_info:         Dict[str, Any] = None,
    source_full_name:   str = None,
) -> None:
    """
    Prints a standardized run summary for Gold notebooks.
    """
    print("\n" + "=" * 70)
    print(f"GOLD {label.upper()} CREATION SUMMARY")
    print("=" * 70)
    if source_full_name:
        print(f"Source         : {source_full_name}")
    print(f"Target         : {target_full_name}")
    print(f"Location       : {s3_target_path}")
    print(f"\nETL Metadata")
    print(f"  _job_run_id    : {etl_meta['job_run_id']}")
    print(f"  _notebook_path : {etl_meta['notebook_path']}")
    print(f"  _model_version : {model_version}")

    if extra_info:
        print(f"\nRun Details")
        for key, val in extra_info.items():
            print(f"  {key:<28}: {val}")

    print("=" * 70)
    print(f"[END] Gold {label} creation completed successfully")
    print("=" * 70)

# COMMAND ----------

print("[INFO] gold_utils loaded successfully")
print("[INFO] Available functions:")
for fn in [
    "resolve_etl_metadata()",
    "validate_inputs()",
    "build_table_names()",
    "get_partition_years()",
    "get_available_years_from_source()",
    "get_processed_batch_years()",
    "get_silver_version()",
    "read_silver_pass_only()",
    "generate_surrogate_key()",
    "append_gold_metadata()",
    "write_gold()",
    "write_gold_ratings_replacewhere_partitions()",
    "write_gold_merge()",
    "ensure_gold_batch_audit_table()",
    "get_successfully_processed_batches()",
    "log_gold_batch_audit()",
    "register_table()",
    "post_write_validation_gold()",
    "print_summary()",
]:
    print(f"  - {fn}")
