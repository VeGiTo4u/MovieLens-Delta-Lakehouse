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

# COMMAND ----------

MIN_VACUUM_RETENTION_HOURS = 168

# COMMAND ----------

# ------------------------------------------------------------
# resolve_etl_metadata
# ------------------------------------------------------------
def resolve_etl_metadata() -> Dict[str, str]:
    """
    Resolves job-level metadata from Databricks notebook context.

    Uses dbruntime.databricks_repl_context — the stable Python-native
    API (DBR 14.1+). Works on all cluster modes including Unity Catalog
    Shared Access Mode.

    Returns:
        dict with keys: job_run_id, notebook_path
    """
    try:
        from dbruntime.databricks_repl_context import get_context
        _ctx = get_context()
        job_run_id    = f"{_ctx.jobId or 'INTERACTIVE'}_{_ctx.currentRunId or 'INTERACTIVE'}"
        notebook_path = _ctx.notebookPath or "UNKNOWN"
    except Exception:
        job_run_id    = "INTERACTIVE_INTERACTIVE"
        notebook_path = "UNKNOWN"

    return {
        "job_run_id":    job_run_id,
        "notebook_path": notebook_path,
    }

# COMMAND ----------

# ============================================================
# MAINTENANCE_CONFIG — Table Registry
#
# Defines which maintenance commands apply to each table
# across all three layers. This is the single source of truth
# for the maintenance notebook.
#
# Keys per table:
#   optimize     — run OPTIMIZE (compacts small files)
#   analyze      — run ANALYZE TABLE COMPUTE STATISTICS
#   vacuum       — run VACUUM (removes old data files)
#   vacuum_hours — retention period in hours for VACUUM
#   zorder_cols  — list of columns for Z-ORDER (omit if none)
#
# Layer-level rules:
#   Bronze: OPTIMIZE + VACUUM only (no ANALYZE, no Z-ORDER)
#   Silver: OPTIMIZE + ANALYZE + VACUUM + Z-ORDER (where applicable)
#   Gold:   OPTIMIZE + ANALYZE + VACUUM + Z-ORDER (where applicable)
# ============================================================

MAINTENANCE_CONFIG = {
    # ----------------------------------------------------------
    # Bronze — OPTIMIZE + VACUUM only
    # Retention: >1 year (8760 hours)
    # No ANALYZE — Bronze is raw ingestion, not queried by
    # end users. Silver reads Bronze in full; optimizer stats
    # have minimal impact.
    # No Z-ORDER — Bronze preserves source fidelity. Reordering
    # data files would add cost with no downstream benefit.
    # ----------------------------------------------------------
    "bronze": {
        "movies":        {"optimize": True, "vacuum": True, "vacuum_hours": 8760},
        "genome_scores": {"optimize": True, "vacuum": True, "vacuum_hours": 8760},
        "genome_tags":   {"optimize": True, "vacuum": True, "vacuum_hours": 8760},
        "links":         {"optimize": True, "vacuum": True, "vacuum_hours": 8760},
        "ratings":       {"optimize": True, "vacuum": True, "vacuum_hours": 8760},
        "tags":          {"optimize": True, "vacuum": True, "vacuum_hours": 8760},
    },

    # ----------------------------------------------------------
    # Silver — OPTIMIZE + ANALYZE + VACUUM + Z-ORDER
    # Retention: 7 days (168 hours)
    # Z-ORDER: only on larger tables with common query predicates.
    # Small/static tables (movies, links, genome_*) — OPTIMIZE only.
    # ----------------------------------------------------------
    "silver": {
        "movies":        {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "genome_scores": {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "genome_tags":   {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "links":         {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "ratings":       {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["user_id", "movie_id"]},
        "tags":          {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["user_id", "movie_id"]},
    },

    # ----------------------------------------------------------
    # Gold — OPTIMIZE + ANALYZE + VACUUM + Z-ORDER
    # Retention: 7 days (168 hours)
    # Z-ORDER: on tables with frequent BI/ML query predicates.
    # Small dims (dim_date, dim_external_links, dim_genome_tags)
    # — OPTIMIZE only, too small to benefit from Z-ORDER.
    # ----------------------------------------------------------
    "gold": {
        "dim_movies":           {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["movie_id"]},
        "dim_genres":           {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["genre_name"]},
        "dim_genome_tags":      {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "dim_external_links":   {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "dim_date":             {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168},
        "bridge_movies_genres": {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["movie_sk", "genre_sk"]},
        "fact_ratings":         {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["movie_sk"]},
        "fact_genome_scores":   {"optimize": True, "analyze": True, "vacuum": True, "vacuum_hours": 168, "zorder_cols": ["movie_sk", "tag_sk"]},
    },
}

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

    Iterates through the MAINTENANCE_CONFIG registry for the
    given layer. Each table is processed independently — a failure
    on one table does not block subsequent tables.

    Args:
        catalog: Unity Catalog name
        layer: layer name key in MAINTENANCE_CONFIG
        vacuum_retention_override: optional override for vacuum_hours

    Returns:
        list of per-table result dicts
    """
    layer_config = MAINTENANCE_CONFIG.get(layer, {})
    if not layer_config:
        print(f"[WARN] No maintenance config found for layer: {layer}")
        return []

    print(f"\n{'#'*60}")
    print(f"# LAYER: {layer.upper()} — {len(layer_config)} tables")
    print(f"{'#'*60}")

    layer_results = []
    for table_name, config in layer_config.items():
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
