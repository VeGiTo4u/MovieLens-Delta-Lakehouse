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
#   validate_s3_path() / validate_table_name() — input validation
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

try:
    from scripts.common import *
except ImportError:
    pass

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

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
    already_processed = get_partition_years(spark, full_table_name)

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
    count = read_write_metrics(spark, s3_target_path)
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
    count = read_write_metrics(spark, s3_target_path)
    return count

# COMMAND ----------

# COMMAND ----------

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

# COMMAND ----------

print("[INFO] bronze_utils loaded successfully")
print("[INFO] Available functions:")
_utils_functions = [
    "append_static_metadata()",
    "append_incremental_metadata()",
    "write_static_bronze()",
    "write_incremental_bronze()",
    "discover_s3_years()",
    "get_already_processed_years_bronze()",
    "detect_cross_year_records()",
]
for fn in _utils_functions:
    print(f"  - {fn}")
