# Databricks notebook source
# ============================================================
# common — Shared utility functions used across all layers.
#
# This module consolidates functions that were previously
# copy-pasted across bronze_utils, silver_utils, gold_utils,
# and maintenance_utils. Each layer's utils.py re-exports
# these via thin wrappers so that %run ./utils still works.
#
# Functions provided:
#   resolve_etl_metadata()       — job/run context resolution
#   validate_s3_path()           — S3 path validation + normalization
#   build_table_name()           — fully qualified Unity Catalog name
#   get_partition_years()        — metadata-only year discovery
#   read_write_metrics()         — numOutputRows from Delta log
#   register_table()             — CREATE TABLE IF NOT EXISTS
#   post_write_validation()      — NULL metadata check
#   print_pipeline_summary()     — standardized run summary
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
def resolve_etl_metadata(include_source_system: bool = True) -> Dict[str, str]:
    """
    Resolves job-level ETL metadata from Databricks notebook context.

    Uses dbruntime.databricks_repl_context — the stable Python-native
    API (DBR 14.1+). Works on all cluster modes including Unity Catalog
    Shared Access Mode where dbutils.notebook.entry_point raises
    Py4JSecurityException.

    Resolved once at notebook startup — not inside loops — so all
    rows in a given batch share identical job-level metadata.

    Args:
        include_source_system: If True, include source_system in result.
            Bronze and Silver use this; Gold and Maintenance do not.

    Returns:
        dict with keys: job_run_id, notebook_path, and optionally source_system
    """
    try:
        from dbruntime.databricks_repl_context import get_context

        _ctx = get_context()

        job_id        = _ctx.jobId        or os.environ.get("DATABRICKS_JOB_ID", "INTERACTIVE")
        run_id        = _ctx.currentRunId or "INTERACTIVE"
        job_run_id    = f"{job_id}_{run_id}"
        notebook_path = _ctx.notebookPath or "UNKNOWN"
    except Exception:
        job_run_id    = "INTERACTIVE_INTERACTIVE"
        notebook_path = "UNKNOWN"

    result = {
        "job_run_id":    job_run_id,
        "notebook_path": notebook_path,
    }

    if include_source_system:
        result["source_system"] = "S3_MovieLens"

    print("[INFO] ETL Metadata resolved:")
    print(f"  _job_run_id    : {job_run_id}")
    print(f"  _notebook_path : {notebook_path}")
    if include_source_system:
        print(f"  _source_system : S3_MovieLens")

    return result

# COMMAND ----------

# ------------------------------------------------------------
# validate_s3_path
# ------------------------------------------------------------
def validate_s3_path(path: str, label: str = "path") -> str:
    """
    Validates a single S3 path and normalizes it with a trailing slash.

    Args:
        path:  The S3 path to validate
        label: Human-readable label for error messages (e.g. "source path")

    Returns:
        Normalized path (guaranteed trailing slash)

    Raises:
        ValueError: If path is empty or does not start with s3://
    """
    if not path or not path.startswith("s3://"):
        raise ValueError(f"CONFIGURATION ERROR: Invalid {label} '{path}'")
    if not path.endswith("/"):
        path += "/"
    return path

# COMMAND ----------

# ------------------------------------------------------------
# validate_table_name
# ------------------------------------------------------------
def validate_table_name(name: str, label: str = "table_name") -> None:
    """
    Validates that a table name is provided (non-empty).

    Args:
        name:  The table name to validate
        label: Human-readable label for error messages

    Raises:
        ValueError: If name is empty or None
    """
    if not name:
        raise ValueError(f"CONFIGURATION ERROR: {label} not provided")

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
    print(f"[INFO] Table : {full_name}")
    return full_name

# COMMAND ----------

# ------------------------------------------------------------
# get_partition_years
# ------------------------------------------------------------
def get_partition_years(
    spark,
    full_table_name: str,
    partition_cols: List[str] = None
) -> set:
    """
    Returns the set of partition year values for a Delta table
    using SHOW PARTITIONS — a pure metadata read against the
    Delta transaction log. Zero data files are opened.

    Why SHOW PARTITIONS instead of distinct().collect():
        distinct().collect() triggers a full table scan to find
        unique values. SHOW PARTITIONS reads only the _delta_log
        partition metadata — O(partitions), not O(rows).

    Args:
        spark: SparkSession
        full_table_name: Fully qualified table name
        partition_cols: List of partition column names to parse.
            Default: ["_batch_year"]. Gold ratings uses
            ["_batch_year", "rating_year"].

    Returns:
        Set of available partition year values as ints.
        Empty set if the table does not exist (first run).
    """
    if partition_cols is None:
        partition_cols = ["_batch_year"]

    try:
        rows = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()
        years = set()
        for row in rows:
            val = str(row[0]).strip()
            # Handle all DBR formats:
            #   "<col>=2022"  (key=value from some DBR versions)
            #   "2022"        (plain value from other DBR versions)
            for col_name in partition_cols:
                if f"{col_name}=" in val:
                    years.add(int(val.split(f"{col_name}=")[1]))
                    break
            else:
                # No partition column prefix found — try plain int
                try:
                    years.add(int(val))
                except ValueError:
                    pass
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
# read_write_metrics  (internal helper)
# ------------------------------------------------------------
def read_write_metrics(spark, s3_target_path: str) -> int:
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

    Args:
        spark: SparkSession
        s3_target_path: S3 path where the Delta table is stored

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
# register_table
# ------------------------------------------------------------
def register_table(
    spark,
    full_table_name: str,
    s3_target_path:  str,
    tbl_properties:  Optional[Dict[str, str]] = None
) -> None:
    """
    Registers the Delta table in Unity Catalog.
    CREATE TABLE IF NOT EXISTS is a no-op on subsequent runs —
    safe to call on every execution.

    Storage and metadata are intentionally decoupled:
    Delta files exist on S3 independently of the catalog entry.
    Dropping the catalog entry does not delete data.

    Args:
        spark: SparkSession
        full_table_name: Fully qualified table name
        s3_target_path: S3 location of Delta files
        tbl_properties: Optional dict of table properties to SET
            e.g. {"delta.enableChangeDataFeed": "true"}
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING DELTA
        LOCATION '{s3_target_path}'
    """)

    if tbl_properties:
        props_sql = ", ".join(
            f"{k} = {v}" for k, v in tbl_properties.items()
        )
        spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({props_sql})")
        print(f"[SUCCESS] Table properties set: {tbl_properties}")

    print(f"[SUCCESS] Table registered: {full_table_name}")

# COMMAND ----------

# ------------------------------------------------------------
# print_pipeline_summary
# ------------------------------------------------------------
def print_pipeline_summary(
    layer:     str,
    label:     str,
    sections:  Dict[str, Dict[str, Any]],
) -> None:
    """
    Prints a standardized run summary for any layer notebook.

    Args:
        layer:    Layer name for header (e.g. "BRONZE", "SILVER", "GOLD")
        label:    Table/action label (e.g. "STATIC", "RATINGS")
        sections: Ordered dict of section_name → {key: value} pairs.
            Each section gets its own header in the summary output.

    Example:
        print_pipeline_summary("BRONZE", "STATIC", {
            "": {
                "Table": full_table_name,
                "Source": s3_source_path,
                "Target": s3_target_path,
            },
            "ETL Metadata": {
                "_job_run_id": etl_meta["job_run_id"],
                "_notebook_path": etl_meta["notebook_path"],
            },
            "Run Details": extra_info,
        })
    """
    print("\n" + "=" * 70)
    print(f"{layer} {label.upper()} SUMMARY")
    print("=" * 70)

    for section_name, fields in sections.items():
        if not fields:
            continue
        if section_name:
            print(f"\n{section_name}")
        for key, val in fields.items():
            print(f"  {key:<28}: {val}")

    print("=" * 70)
    print(f"[END] {layer} {label} completed successfully")
    print("=" * 70)
