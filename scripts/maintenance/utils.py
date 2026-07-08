# Databricks notebook source
# ============================================================
# maintenance_utils
# Shared utility functions for the table maintenance notebook.
#
# Usage:
#   %run ./maintenance_utils
#
# Functions provided:
#   resolve_etl_metadata()         — job/run ID resolution
#   run_optimize()                 — OPTIMIZE with optional ZORDER BY
#   run_analyze()                  — ANALYZE TABLE COMPUTE STATISTICS
#   run_vacuum()                   — VACUUM with configurable retention
#   run_table_maintenance()        — orchestrates all commands for one table
#   run_layer_maintenance()        — runs maintenance for all tables in a layer
#   print_maintenance_summary()    — structured report of all operations
#
# Table Registry:
#   MAINTENANCE_CONFIG             — per-layer, per-table configuration
# ============================================================

# COMMAND ----------

from datetime import datetime
from typing import Dict, List, Any, Optional

# ------------------------------------------------------------
def resolve_etl_metadata() -> Dict[str, str]:
    """Resolves job-level metadata. Delegates to scripts.common.
    Maintenance does not include source_system."""
    return _resolve_etl_metadata(include_source_system=False)

# COMMAND ----------

# COMMAND ----------

# ------------------------------------------------------------
# run_optimize
# ------------------------------------------------------------
def run_optimize(full_table_name: str, zorder_cols: List[str] = None) -> Dict[str, Any]:
    """
    Runs OPTIMIZE on a Delta table with optional ZORDER BY.

    OPTIMIZE compacts small files into larger ones, improving
    read performance by reducing file-open overhead.

    ZORDER BY physically co-locates related data (by column values)
    within files, enabling data skipping on filtered queries.

    Args:
        full_table_name: catalog.schema.table
        zorder_cols: optional list of columns for Z-ORDER

    Returns:
        dict with status ('SUCCESS' or 'FAILED') and message
    """
    try:
        if zorder_cols:
            zorder_clause = ", ".join(zorder_cols)
            spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({zorder_clause})")
            msg = f"OPTIMIZE + ZORDER BY ({zorder_clause})"
        else:
            spark.sql(f"OPTIMIZE {full_table_name}")
            msg = "OPTIMIZE"

        print(f"[SUCCESS] {msg} completed: {full_table_name}")
        return {"status": "SUCCESS", "operation": msg}

    except Exception as e:
        print(f"[FAILED] OPTIMIZE failed on {full_table_name}: {e}")
        return {"status": "FAILED", "operation": "OPTIMIZE", "error": str(e)}

# COMMAND ----------

# ------------------------------------------------------------
# run_analyze
# ------------------------------------------------------------
def run_analyze(full_table_name: str) -> Dict[str, Any]:
    """
    Runs ANALYZE TABLE COMPUTE STATISTICS on a Delta table.

    Collects column-level statistics that the Spark query optimizer
    uses to choose better join strategies (broadcast vs shuffle)
    and filter plans. Lightweight operation — reads metadata, not
    full data files.

    Args:
        full_table_name: catalog.schema.table

    Returns:
        dict with status ('SUCCESS' or 'FAILED') and message
    """
    try:
        spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS")
        print(f"[SUCCESS] ANALYZE TABLE completed: {full_table_name}")
        return {"status": "SUCCESS", "operation": "ANALYZE TABLE"}

    except Exception as e:
        print(f"[FAILED] ANALYZE TABLE failed on {full_table_name}: {e}")
        return {"status": "FAILED", "operation": "ANALYZE TABLE", "error": str(e)}

# COMMAND ----------

# ------------------------------------------------------------
# run_vacuum
# ------------------------------------------------------------
def run_vacuum(
    full_table_name: str,
    retention_hours: int,
    break_glass: bool = False,
    break_glass_reason: str = "",
) -> Dict[str, Any]:
    """
    Runs VACUUM on a Delta table with the specified retention period.

    VACUUM removes data files that are no longer referenced by
    any version in the Delta log AND are older than the retention
    period. This reclaims storage on S3.

    Safety:
        - Delta's default retention is 168 hours (7 days).
        - For retention < 168 hours, Delta requires
          spark.databricks.delta.retentionDurationCheck.enabled = false.
          This notebook does NOT override that setting — if you
          set retention below 168 hours, the VACUUM will fail
          unless the check is explicitly disabled on the cluster.

    Args:
        full_table_name: catalog.schema.table
        retention_hours: minimum age (hours) of files to remove

    Returns:
        dict with status ('SUCCESS' or 'FAILED') and message
    """
    if retention_hours < MIN_VACUUM_RETENTION_HOURS:
        if not break_glass:
            raise ValueError(
                f"CONFIGURATION ERROR: retention_hours={retention_hours} is below "
                f"the safety floor of {MIN_VACUUM_RETENTION_HOURS}. "
                f"Set break_glass=true and provide a non-empty break_glass_reason."
            )
        if not str(break_glass_reason).strip():
            raise ValueError(
                "CONFIGURATION ERROR: break_glass_reason is required when break_glass=true "
                f"and retention_hours < {MIN_VACUUM_RETENTION_HOURS}."
            )

    try:
        spark.sql(f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS")
        msg = f"VACUUM RETAIN {retention_hours} HOURS"
        if break_glass and retention_hours < MIN_VACUUM_RETENTION_HOURS:
            msg = f"{msg} (BREAK_GLASS: {break_glass_reason.strip()})"
        print(f"[SUCCESS] {msg} completed: {full_table_name}")
        return {"status": "SUCCESS", "operation": msg}

    except Exception as e:
        print(f"[FAILED] VACUUM failed on {full_table_name}: {e}")
        return {"status": "FAILED", "operation": "VACUUM", "error": str(e)}

# COMMAND ----------

# ------------------------------------------------------------
# run_table_maintenance
# ------------------------------------------------------------
def run_table_maintenance(
    catalog: str,
    schema: str,
    table_name: str,
    config: Dict[str, Any],
    vacuum_retention_override: int = None,
    break_glass: bool = False,
    break_glass_reason: str = "",
) -> Dict[str, Any]:
    """
    Runs all configured maintenance commands for a single table.

    Executes commands in the correct order:
        1. OPTIMIZE (+ ZORDER if configured)  — compact first
        2. ANALYZE TABLE                      — refresh stats after compaction
        3. VACUUM                             — clean up after optimize

    Each command runs independently — a failure in one does NOT
    block the others. All results are captured and returned.

    Args:
        catalog: Unity Catalog name
        schema: schema/layer name (bronze, silver, gold)
        table_name: table name
        config: dict from MAINTENANCE_CONFIG with command flags
        vacuum_retention_override: optional override for vacuum_hours

    Returns:
        dict with table_name, full_table_name, and results list
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    results = []
    start_time = datetime.now()

    print(f"\n{'='*60}")
    print(f"[START] Maintenance: {full_table_name}")
    print(f"{'='*60}")

    # 1. OPTIMIZE (+ optional ZORDER)
    if config.get("optimize", False):
        zorder_cols = config.get("zorder_cols", None)
        results.append(run_optimize(full_table_name, zorder_cols))

    # 2. ANALYZE TABLE
    if config.get("analyze", False):
        results.append(run_analyze(full_table_name))

    # 3. VACUUM
    if config.get("vacuum", False):
        retention = vacuum_retention_override or config.get("vacuum_hours", 168)
        results.append(
            run_vacuum(
                full_table_name=full_table_name,
                retention_hours=retention,
                break_glass=break_glass,
                break_glass_reason=break_glass_reason,
            )
        )

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"[DONE] {full_table_name} — {elapsed:.1f}s")

    return {
        "table_name":      table_name,
        "full_table_name": full_table_name,
        "results":         results,
        "elapsed_seconds": elapsed,
    }

# COMMAND ----------

# ------------------------------------------------------------
# run_layer_maintenance
# ------------------------------------------------------------
def run_layer_maintenance(
    catalog: str,
    layer: str,
    vacuum_retention_override: int = None,
    break_glass: bool = False,
    break_glass_reason: str = "",
) -> List[Dict[str, Any]]:
    """
    Runs maintenance on all tables in a layer (bronze/silver/gold).
    Discovers tables dynamically using SHOW TABLES.
    """
    print(f"\\n{'#'*60}")
    print(f"# LAYER: {layer.upper()}")
    print(f"{'#'*60}")

    try:
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{layer}")
        table_names = [row.tableName for row in tables_df.collect() if not row.isTemporary]
    except Exception as e:
        print(f"[WARN] Could not discover tables in {catalog}.{layer}: {e}")
        return []

    print(f"[INFO] Discovered {len(table_names)} tables in {catalog}.{layer}")

    layer_results = []
    for table_name in table_names:
        # Determine rules based on layer
        config = {
            "optimize": True,
            "vacuum": True,
            "vacuum_hours": 8760 if layer == "bronze" else 168,
            "analyze": layer in ("silver", "gold"),
            "zorder_cols": []
        }
        
        # Determine specific Z-ORDER logic from legacy configs
        if layer == "silver" and table_name in ("ratings", "tags"):
            config["zorder_cols"] = ["user_id", "movie_id"]
        elif layer == "gold" and table_name == "dim_movies":
            config["zorder_cols"] = ["movie_id"]
        elif layer == "gold" and table_name == "dim_genres":
            config["zorder_cols"] = ["genre_name"]
        elif layer == "gold" and table_name == "bridge_movies_genres":
            config["zorder_cols"] = ["movie_sk", "genre_sk"]
        elif layer == "gold" and table_name == "fact_ratings":
            config["zorder_cols"] = ["movie_sk"]
        elif layer == "gold" and table_name == "fact_genome_scores":
            config["zorder_cols"] = ["movie_sk", "tag_sk"]

        try:
            result = run_table_maintenance(
                catalog=catalog,
                schema=layer,
                table_name=table_name,
                config=config,
                vacuum_retention_override=vacuum_retention_override,
                break_glass=break_glass,
                break_glass_reason=break_glass_reason,
            )
            layer_results.append(result)
        except Exception as e:
            print(f"[FAILED] Unexpected error on {catalog}.{layer}.{table_name}: {e}")
            layer_results.append({
                "table_name":      table_name,
                "full_table_name": f"{catalog}.{layer}.{table_name}",
                "results":         [{"status": "FAILED", "operation": "ALL", "error": str(e)}],
                "elapsed_seconds": 0,
            })

    return layer_results

# COMMAND ----------

# ------------------------------------------------------------
# print_maintenance_summary
# ------------------------------------------------------------
def print_maintenance_summary(
    all_results: Dict[str, List[Dict[str, Any]]],
    total_start: datetime,
    etl_meta: Dict[str, str],
) -> None:
    """
    Prints a structured summary of all maintenance operations.

    Args:
        all_results: dict keyed by layer name → list of table results
        total_start: datetime when maintenance began
        etl_meta: job metadata from resolve_etl_metadata()
    """
    total_elapsed = (datetime.now() - total_start).total_seconds()

    print(f"\n{'='*70}")
    print(f"  TABLE MAINTENANCE — SUMMARY REPORT")
    print(f"{'='*70}")
    print(f"  Job Run ID    : {etl_meta['job_run_id']}")
    print(f"  Notebook      : {etl_meta['notebook_path']}")
    print(f"  Total Duration: {total_elapsed:.1f}s")
    print(f"{'='*70}")

    total_success = 0
    total_failed  = 0

    for layer, layer_results in all_results.items():
        print(f"\n  --- {layer.upper()} ---")
        for table_result in layer_results:
            table_name = table_result["table_name"]
            elapsed    = table_result["elapsed_seconds"]
            results    = table_result["results"]

            statuses = []
            for r in results:
                op     = r["operation"]
                status = r["status"]
                if status == "SUCCESS":
                    statuses.append(f"✓ {op}")
                    total_success += 1
                else:
                    statuses.append(f"✗ {op}")
                    total_failed += 1

            status_str = " | ".join(statuses) if statuses else "NO OPS"
            print(f"    {table_name:<25s} [{elapsed:6.1f}s]  {status_str}")

    print(f"\n{'='*70}")
    print(f"  Operations: {total_success} succeeded, {total_failed} failed")
    print(f"{'='*70}")
