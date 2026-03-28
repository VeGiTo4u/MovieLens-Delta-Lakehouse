# Databricks notebook source
# ============================================================
# bronze_utils
# Shared utility functions for all Bronze layer notebooks.
#
# Usage in each Bronze notebook:
#   %run ./bronze_utils
#
# Functions provided:
#   resolve_etl_metadata()              — job/run context resolution
#   validate_inputs()                   — s3 path + table name checks
#   build_table_name()                  — fully qualified Unity Catalog name
#   get_partition_years()               — metadata-only year discovery via SHOW PARTITIONS
#   append_static_metadata()            — 5 ETL cols (static loader)
#   append_incremental_metadata()       — 7 ETL cols (historical loader)
#   write_static_bronze()               — full overwrite + mergeSchema
#   write_incremental_bronze()          — replaceWhere per _batch_year
#   discover_s3_years()                 — S3 file discovery (historical only)
#   get_already_processed_years_bronze()— incrementality check via SHOW PARTITIONS (historical only)
#   register_table()                    — CREATE TABLE IF NOT EXISTS
#   post_write_validation_bronze()      — NULL metadata check (count from Delta log, not re-scan)
#   detect_cross_year_records()         — late arrival observability (incremental only)
#   print_summary()                     — standardized run summary
# ============================================================

# COMMAND ----------

import os
import re
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from typing import Dict, List, Any

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

    Resolved once at notebook startup — not inside loops — so all
    rows in a given batch share identical job-level metadata.

    Returns:
        dict with keys: job_run_id, notebook_path, source_system
    """
    from dbruntime.databricks_repl_context import get_context

    _ctx = get_context()

    job_id        = _ctx.jobId        or os.environ.get("DATABRICKS_JOB_ID", "INTERACTIVE")
    run_id        = _ctx.currentRunId or "INTERACTIVE"
    job_run_id    = f"{job_id}_{run_id}"
    notebook_path = _ctx.notebookPath or "UNKNOWN"
    source_system = "S3_MovieLens"

    print("[INFO] ETL Metadata resolved:")
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
    s3_source_path: str,
    s3_target_path: str,
    table_name:     str
) -> tuple:
    """
    Validates and normalizes widget inputs.
    Fails fast on misconfigured jobs before any Spark work is done.

    Returns:
        (normalized_s3_source_path, normalized_s3_target_path)
        Both guaranteed to have a trailing slash.
    """
    if not s3_source_path or not s3_source_path.startswith("s3://"):
        raise ValueError(f"CONFIGURATION ERROR: Invalid source path '{s3_source_path}'")

    if not s3_target_path or not s3_target_path.startswith("s3://"):
        raise ValueError(f"CONFIGURATION ERROR: Invalid target path '{s3_target_path}'")

    if not table_name:
        raise ValueError("CONFIGURATION ERROR: table_name not provided")

    if not s3_source_path.endswith("/"):
        s3_source_path += "/"

    if not s3_target_path.endswith("/"):
        s3_target_path += "/"

    print("[INFO] Input validation passed")
    return s3_source_path, s3_target_path

# COMMAND ----------

# ------------------------------------------------------------
# build_table_name
# ------------------------------------------------------------
def build_table_name(catalog: str, schema: str, table: str) -> str:
    """
    Builds a fully qualified Unity Catalog table name.

    Returns:
        "<catalog>.<schema>.<table>"
    """
    full_name = f"{catalog}.{schema}.{table}"
    print(f"[INFO] Target table : {full_name}")
    return full_name

# COMMAND ----------

# ------------------------------------------------------------
# get_partition_years
# ------------------------------------------------------------
def get_partition_years(full_table_name: str) -> set:
    """
    Returns the set of _batch_year partition values for a Delta
    table using SHOW PARTITIONS — a pure metadata read against
    the Delta transaction log. Zero data files are opened.

    Why SHOW PARTITIONS instead of distinct().collect():
        distinct().collect() triggers a full table scan to find
        unique values. On a 100M-row table with 10 partition years,
        Spark still reads every row. SHOW PARTITIONS reads only the
        _delta_log partition metadata — O(partitions), not O(rows).
        The performance difference is astronomical at scale, and the
        result is identical.

    Partition format returned by Databricks SHOW PARTITIONS:
        Each row has a single column "partition" with value like:
        "_batch_year=2022"
        We extract the integer after "=" to get the year.

    Returns:
        Set of available _batch_year values as ints.
        Empty set if the table does not exist (first run).
    """
    try:
        rows = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()
        years = set()
        for row in rows:
            val = str(row[0]).strip()
            # Handle both formats:
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
# _read_write_metrics  (internal helper)
# ------------------------------------------------------------
def _read_write_metrics(s3_target_path: str) -> int:
    """
    Reads numOutputRows from the Delta transaction log entry
    produced by the most recent write operation.

    Uses DeltaTable.forPath (not forName) so it works immediately
    after a write, before Unity Catalog registration has happened.
    Reads a single JSON file from _delta_log — no data files opened.

    Why this is safe to trust:
        Delta's write contract guarantees that if the write call
        returns without exception, exactly numOutputRows rows were
        committed atomically. The log entry is the write receipt.

    Returns:
        numOutputRows as int, or -1 if metrics cannot be read.
        -1 is non-blocking — callers log a warning and continue.
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
# append_static_metadata
# ------------------------------------------------------------
def append_static_metadata(df: DataFrame, etl_meta: Dict[str, str]) -> DataFrame:
    """
    Appends 5 ETL metadata columns to static Bronze tables
    (genome_scores, genome_tags, links, movies).

    Static tables are loaded from a single file — no year
    partitioning, no _batch_id or _batch_year needed.

    Columns added:
      _input_file_name     — exact S3 path per row via _metadata.file_path
                             Unity Catalog compatible (not input_file_name())
      _ingestion_timestamp — when Spark read this record
      _job_run_id          — <jobId>_<runId> ties batch to Databricks Job log
      _notebook_path       — which bronze notebook produced this row
      _source_system       — constant "S3_MovieLens"
    """
    return (
        df
        .withColumn("_input_file_name",     F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_job_run_id",          F.lit(etl_meta["job_run_id"]))
        .withColumn("_notebook_path",       F.lit(etl_meta["notebook_path"]))
        .withColumn("_source_system",       F.lit(etl_meta["source_system"]))
    )

# COMMAND ----------

# ------------------------------------------------------------
# append_incremental_metadata
# ------------------------------------------------------------
def append_incremental_metadata(
    df:       DataFrame,
    etl_meta: Dict[str, str],
    table_name: str,
    year:     int
) -> DataFrame:
    """
    Appends 7 ETL metadata columns to incremental Bronze tables
    (ratings, tags). Adds _batch_id and _batch_year on top of
    the 5 static columns for partition-level idempotency.

    Columns added:
      _input_file_name     — exact S3 path per row
      _ingestion_timestamp — when Spark read this record
      _job_run_id          — ties batch to Databricks Job log
      _notebook_path       — which bronze notebook produced this row
      _source_system       — constant "S3_MovieLens"
      _batch_id            — "<table>_<year>" e.g. "ratings_2022"
                             Dual purpose: audit trail AND the key
                             that get_already_processed_years_bronze()
                             reads for incrementality decisions
      _batch_year          — year as INT — partition column and
                             replaceWhere predicate for idempotency
    """
    batch_id = f"{table_name}_{year}"

    return (
        df
        .withColumn("_input_file_name",     F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_job_run_id",          F.lit(etl_meta["job_run_id"]))
        .withColumn("_notebook_path",       F.lit(etl_meta["notebook_path"]))
        .withColumn("_source_system",       F.lit(etl_meta["source_system"]))
        .withColumn("_batch_id",            F.lit(batch_id))
        .withColumn("_batch_year",          F.lit(year))
    )

# COMMAND ----------

# ------------------------------------------------------------
# discover_s3_years
# ------------------------------------------------------------
def discover_s3_years(s3_source_path: str, table_name: str) -> set:
    """
    Lists all files at s3_source_path and extracts years from
    filenames matching pattern: <table_name>_<YYYY>.csv
    e.g. ratings_2022.csv → year 2022

    This replaces manual start_year/end_year widgets.
    The S3 listing is the source of truth for what's available.

    Fails hard if:
      - S3 path is inaccessible (permissions or wrong path)
      - No matching files found (likely a misconfiguration)

    Returns:
        Set of available years (ints)
    """
    print(f"[INFO] Scanning S3 for source files: {s3_source_path}")

    file_pattern = re.compile(rf"^{re.escape(table_name)}_(\d{{4}})\.csv$")

    try:
        s3_files = dbutils.fs.ls(s3_source_path)
    except Exception as e:
        raise RuntimeError(
            f"FAILED: Cannot list S3 source path '{s3_source_path}'. "
            f"Check bucket permissions and path. Error: {e}"
        )

    available_years = set()
    for f in s3_files:
        match = file_pattern.match(f.name)
        if match:
            available_years.add(int(match.group(1)))

    if not available_years:
        raise RuntimeError(
            f"FAILED: No files matching '{table_name}_<YYYY>.csv' "
            f"found at '{s3_source_path}'. "
            f"Files present: {[f.name for f in s3_files]}"
        )

    print(f"[INFO] Available years on S3 : {sorted(available_years)}")
    print(f"[INFO] Total files found     : {len(available_years)}")
    return available_years

# COMMAND ----------

# ------------------------------------------------------------
# get_already_processed_years_bronze
# ------------------------------------------------------------
def get_already_processed_years_bronze(full_table_name: str) -> set:
    """
    Returns the set of _batch_year values already committed in
    the Bronze Delta table to determine which years to skip.

    Delegates to get_partition_years() — a pure SHOW PARTITIONS
    metadata read against the Delta transaction log.

    Why SHOW PARTITIONS replaces the old distinct().collect():
        The old implementation read distinct _batch_id values from
        the table data and parsed out the year. This triggered a
        full table scan — every row read just to find a handful of
        unique year values. SHOW PARTITIONS reads only the _delta_log
        partition metadata. The result is identical; the cost is not.

    The Delta table is still the source of truth — no external state,
    no control file, no Autoloader checkpoint to manage. The only
    change is HOW we read that state: metadata path vs data path.

    Returns:
        Set of already-processed years (empty set on first run)
    """
    already_processed = get_partition_years(full_table_name)

    if already_processed:
        print(f"[INFO] Already processed years in Bronze: {sorted(already_processed)}")
    else:
        print("[INFO] Bronze table does not exist yet — full load will run")

    return already_processed

# COMMAND ----------

# ------------------------------------------------------------
# write_static_bronze
# ------------------------------------------------------------
def write_static_bronze(df: DataFrame, s3_target_path: str, table_name: str) -> int:
    """
    Writes a static Bronze table using full overwrite + mergeSchema.

    mergeSchema (not overwriteSchema):
      - New upstream columns → safely adopted into the Delta schema
      - Columns removed or type-changed → write FAILS (breaking change caught)
      overwriteSchema would silently accept any schema change including
      destructive ones — not appropriate for production pipelines.

    Record count strategy:
      The pre-write df.count() has been removed. Counting the DataFrame
      before writing triggers a full Spark job just to get a number we
      could read for free from the Delta log after the write. We now
      read numOutputRows from history(1).operationMetrics — a single
      JSON file in _delta_log, not a data scan. The count is identical
      (Delta commits exactly what Spark writes) at zero scan cost.

    Returns:
        Record count written (from Delta transaction log)
    """
    print(f"[START] Writing Bronze table: {table_name}")

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
# write_incremental_bronze
# ------------------------------------------------------------
def write_incremental_bronze(
    df:             DataFrame,
    s3_target_path: str,
    table_name:     str,
    year:           int
) -> int:
    """
    Writes one year's partition to an incremental Bronze table
    using replaceWhere for idempotency.

    Write strategy: overwrite + replaceWhere(_batch_year = year)
      - Scopes the overwrite to ONLY _batch_year = year partition
      - All other year partitions are never touched
      - Retry of same year → atomically replaces only that partition
      - Delta ACID guarantees failed writes roll back cleanly

    Record count strategy:
      Pre-write df.count() removed. Count read from Delta log after
      write via history(1).operationMetrics["numOutputRows"].
      See write_static_bronze docstring for full reasoning.

    Returns:
        Record count written for this year (from Delta transaction log)
    """
    print(f"[START] Writing Bronze partition _batch_year={year} for: {table_name}")

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
# register_table
# ------------------------------------------------------------
def register_table(full_table_name: str, s3_target_path: str) -> None:
    """
    Registers the Delta table in Unity Catalog.
    CREATE TABLE IF NOT EXISTS is a no-op on subsequent runs —
    safe to call on every execution.

    Storage and metadata are intentionally decoupled:
    Delta files exist on S3 independently of the catalog entry.
    Dropping the catalog entry does not delete data.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING DELTA
        LOCATION '{s3_target_path}'
    """)
    print(f"[SUCCESS] Table registered: {full_table_name}")

# COMMAND ----------

# ------------------------------------------------------------
# post_write_validation_bronze
# ------------------------------------------------------------
def post_write_validation_bronze(
    full_table_name: str,
    expected_count:  int,
    processed_years: list = None
) -> None:
    """
    Validates the registered Bronze table after write.

    What this checks:
      1. Logs the expected record count (from Delta transaction log —
         no re-scan). Delta ACID guarantees the committed count equals
         what write_static/incremental_bronze returned, so a redundant
         equality check adds no safety while doubling scan cost.
      2. All ETL metadata columns are present and non-NULL in the
         newly written data. This is the real correctness check —
         NULL metadata means context resolution or the write itself
         silently failed, which must be caught before Silver runs.

    Why the count re-scan was removed:
        The old implementation read df_val.count() from the registered
        table and compared it to expected_count. Both values were
        derived from the same data, making the check circular. More
        importantly, on a large table it triggered a full scan just
        to confirm what the Delta log already told us. Delta ACID
        means if write() returned without exception, the data is there
        and complete — no post-hoc scan needed for that guarantee.

    The NULL check still scans the table (scoped to processed_years
    for incremental tables) — this is intentional and retained because
    metadata correctness cannot be inferred from the log alone.
    """
    print("[START] Post-write validation")
    print(f"[INFO]  Expected records (from Delta log): {expected_count:,}")

    try:
        if expected_count == 0:
            raise ValueError(
                "FAILED: Zero records committed according to Delta log. "
                "Aborting to prevent silent empty-table propagation."
            )

        # Scope to processed years for incremental tables — avoids
        # scanning partitions from previous runs unnecessarily
        if processed_years:
            df_val = spark.table(full_table_name).filter(
                F.col("_batch_year").isin(processed_years)
            )
        else:
            df_val = spark.table(full_table_name)

        # Core metadata columns — present in all bronze tables
        meta_cols = [
            "_input_file_name",
            "_ingestion_timestamp",
            "_job_run_id",
            "_notebook_path",
            "_source_system",
        ]

        # _batch_id and _batch_year only exist on incremental tables
        if "_batch_id" in df_val.columns:
            meta_cols.append("_batch_id")
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
# detect_cross_year_records
# ------------------------------------------------------------
def detect_cross_year_records(
    full_table_name: str,
    batch_year:      int,
    timestamp_col:   str = "timestamp",
) -> dict:
    """
    Observability check — detects records in a Bronze batch whose
    raw Unix timestamp year does not match the expected _batch_year.

    Why this lives in Bronze and not Silver:
        Bronze is the first point where we can surface this signal.
        The earlier an anomaly is visible, the faster operators can
        investigate the source system. Silver will handle late arrivals
        correctly (is_late_arrival flag), but Bronze logging gives you
        an early warning before Silver even runs.

    Why this is WARNING-only and never fails the Bronze job:
        Bronze's contract is raw ingestion fidelity — store exactly
        what the source sent, with no filtering or transformation.
        Dropping or failing on cross-year records at Bronze would
        silently discard data that Silver is designed to handle.

    Count strategy:
        The old implementation called df_with_event_year.count() to
        get total_records, then groupBy().count().collect() for the
        year distribution — two separate Spark actions on the same
        data. We now derive total_records by summing the year
        distribution counts on the driver side. The groupBy is the
        only Spark action needed; the total is a free arithmetic sum.

    Returns:
        dict with keys:
            total_records      — total records in this batch partition
            cross_year_count   — records whose event year != batch_year
            cross_year_pct     — percentage of cross-year records
            year_distribution  — dict of {event_year: count} for all years found
    """
    print(f"\n[INFO] Cross-year detection for _batch_year={batch_year}")

    try:
        df_batch = (
            spark.table(full_table_name)
                 .filter(F.col("_batch_year") == batch_year)
        )

        # Convert raw Unix epoch → calendar year — stays fully distributed
        df_with_event_year = df_batch.withColumn(
            "_event_year",
            F.year(F.from_unixtime(F.col(timestamp_col).cast("long")))
        )

        # Single Spark action: year distribution groupBy.
        # total_records derived by summing counts on the driver —
        # no second count() action needed.
        year_dist_rows = (
            df_with_event_year
            .groupBy("_event_year")
            .count()
            .orderBy("_event_year")
            .collect()
        )
        year_distribution = {row["_event_year"]: row["count"] for row in year_dist_rows}

        # Driver-side arithmetic — no Spark action
        total_records    = sum(year_distribution.values())
        cross_year_count = sum(
            count for year, count in year_distribution.items()
            if year != batch_year
        )
        cross_year_pct = (cross_year_count / total_records * 100) if total_records > 0 else 0.0

        # Log results
        if cross_year_count == 0:
            print(f"[PASS] No cross-year records detected in _batch_year={batch_year}")
        else:
            print(f"[WARN] Cross-year records detected in _batch_year={batch_year}:")
            print(f"       Total records      : {total_records:,}")
            print(f"       Cross-year records : {cross_year_count:,} ({cross_year_pct:.2f}%)")
            print(f"       Year distribution  :")
            for yr, cnt in sorted(year_distribution.items()):
                marker = "← expected" if yr == batch_year else "← LATE ARRIVAL"
                print(f"         {yr} : {cnt:>10,}  {marker}")
            print(f"[WARN] These records will be flagged is_late_arrival=True in Silver")
            print(f"[WARN] Silver MERGE at Gold will route them to correct year partitions")

        return {
            "total_records":     total_records,
            "cross_year_count":  cross_year_count,
            "cross_year_pct":    round(cross_year_pct, 4),
            "year_distribution": year_distribution,
        }

    except Exception as e:
        # Non-blocking — detection failure must never kill Bronze ingestion
        print(f"[WARN] Cross-year detection failed (non-blocking): {e}")
        return {
            "total_records":     -1,
            "cross_year_count":  -1,
            "cross_year_pct":    -1.0,
            "year_distribution": {},
        }

# COMMAND ----------

# ------------------------------------------------------------
# print_summary
# ------------------------------------------------------------
def print_summary(
    label:           str,
    full_table_name: str,
    s3_source_path:  str,
    s3_target_path:  str,
    etl_meta:        Dict[str, str],
    extra_info:      Dict[str, Any] = None
) -> None:
    """
    Prints a standardized run summary for Bronze notebooks.
    extra_info accepts loader-specific metrics.
    """
    print("\n" + "=" * 70)
    print(f"BRONZE {label.upper()} INGESTION SUMMARY")
    print("=" * 70)
    print(f"Table          : {full_table_name}")
    print(f"Source         : {s3_source_path}")
    print(f"Target         : {s3_target_path}")
    print(f"\nETL Metadata")
    print(f"  _job_run_id    : {etl_meta['job_run_id']}")
    print(f"  _notebook_path : {etl_meta['notebook_path']}")
    print(f"  _source_system : {etl_meta['source_system']}")

    if extra_info:
        print(f"\nRun Details")
        for key, val in extra_info.items():
            print(f"  {key:<26}: {val}")

    print("=" * 70)
    print(f"[END] Bronze {label} ingestion completed successfully")
    print("=" * 70)

# COMMAND ----------

print("[INFO] bronze_utils loaded successfully")
print("[INFO] Available functions:")
_utils_functions = [
    "resolve_etl_metadata()",
    "validate_inputs()",
    "build_table_name()",
    "get_partition_years()",
    "append_static_metadata()",
    "append_incremental_metadata()",
    "write_static_bronze()",
    "write_incremental_bronze()",
    "discover_s3_years()",
    "get_already_processed_years_bronze()",
    "register_table()",
    "post_write_validation_bronze()",
    "detect_cross_year_records()",
    "print_summary()",
]
for fn in _utils_functions:
    print(f"  - {fn}")
