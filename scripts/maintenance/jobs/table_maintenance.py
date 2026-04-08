# Databricks notebook source
# MAGIC %run ./maintenance_utils

# COMMAND ----------

# ============================================================
# Table Maintenance Notebook
#
# Runs OPTIMIZE, ANALYZE TABLE, Z-ORDER BY, and VACUUM across
# all Bronze, Silver, and Gold Delta tables.
#
# Design:
#   This notebook is DECOUPLED from the ETL pipeline and should
#   be scheduled as a separate Databricks Job running during
#   off-peak hours. ETL notebooks focus on data correctness;
#   this notebook focuses on storage performance.
#
# Commands by layer:
#   Bronze: OPTIMIZE + VACUUM (retention >1 year)
#   Silver: OPTIMIZE + ANALYZE + ZORDER + VACUUM (retention 7 days)
#   Gold:   OPTIMIZE + ANALYZE + ZORDER + VACUUM (retention 7 days)
#
# Error handling:
#   Each table is maintained independently. A failure on one
#   table does NOT prevent maintenance on subsequent tables.
#   The summary report shows per-table success/failure status.
# ============================================================

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets
# ------------------------------------------------------------
dbutils.widgets.text("catalog_name",             "movielens", "Catalog Name")
dbutils.widgets.text("layers",                   "bronze,silver,gold", "Layers (comma-separated)")
dbutils.widgets.text("vacuum_retention_override", "",         "VACUUM Retention Override (hours, blank=use config)")
dbutils.widgets.dropdown("break_glass", "false", ["false", "true"], "Break Glass (allow retention < 168h)")
dbutils.widgets.text("break_glass_reason", "", "Break Glass Reason (required when break_glass=true)")

catalog_name             = dbutils.widgets.get("catalog_name")
layers_raw               = dbutils.widgets.get("layers")
vacuum_retention_raw     = dbutils.widgets.get("vacuum_retention_override").strip()
break_glass              = dbutils.widgets.get("break_glass").strip().lower() == "true"
break_glass_reason       = dbutils.widgets.get("break_glass_reason").strip()

# Parse layers
layers = [l.strip().lower() for l in layers_raw.split(",") if l.strip()]

# Parse vacuum override (empty string = use per-table config)
vacuum_retention_override = int(vacuum_retention_raw) if vacuum_retention_raw else None

# COMMAND ----------

# ------------------------------------------------------------
# Validation
# ------------------------------------------------------------
valid_layers = {"bronze", "silver", "gold"}
invalid = [l for l in layers if l not in valid_layers]
if invalid:
    raise ValueError(
        f"CONFIGURATION ERROR: Invalid layer(s): {invalid}. "
        f"Valid layers are: {sorted(valid_layers)}"
    )

if not layers:
    raise ValueError("CONFIGURATION ERROR: No layers specified.")

if vacuum_retention_override is not None and vacuum_retention_override <= 0:
    raise ValueError(
        f"CONFIGURATION ERROR: vacuum_retention_override must be positive, got {vacuum_retention_override}"
    )

if break_glass and not break_glass_reason:
    raise ValueError(
        "CONFIGURATION ERROR: break_glass_reason must be provided when break_glass=true."
    )

print(f"[CONFIG] Catalog                  : {catalog_name}")
print(f"[CONFIG] Layers                   : {layers}")
print(f"[CONFIG] VACUUM retention override : {vacuum_retention_override or '(per-table config)'}")
print(f"[CONFIG] Break glass              : {break_glass}")
if break_glass:
    print(f"[CONFIG] Break glass reason       : {break_glass_reason}")

# COMMAND ----------

# ------------------------------------------------------------
# Resolve ETL metadata for logging
# ------------------------------------------------------------
etl_meta = resolve_etl_metadata()
print(f"[INFO] Job Run ID : {etl_meta['job_run_id']}")
print(f"[INFO] Notebook   : {etl_meta['notebook_path']}")

# COMMAND ----------

# ------------------------------------------------------------
# Run maintenance for each requested layer
# ------------------------------------------------------------
from datetime import datetime

total_start = datetime.now()
all_results = {}

for layer in layers:
    layer_results = run_layer_maintenance(
        catalog=catalog_name,
        layer=layer,
        vacuum_retention_override=vacuum_retention_override,
        break_glass=break_glass,
        break_glass_reason=break_glass_reason,
    )
    all_results[layer] = layer_results

# COMMAND ----------

# ------------------------------------------------------------
# Summary Report
# ------------------------------------------------------------
print_maintenance_summary(all_results, total_start, etl_meta)

# COMMAND ----------

# Exit with status
total_ops    = sum(len(r["results"]) for lr in all_results.values() for r in lr)
failed_ops   = sum(
    1 for lr in all_results.values()
    for r in lr
    for op in r["results"]
    if op["status"] == "FAILED"
)

if failed_ops > 0:
    dbutils.notebook.exit(f"COMPLETED_WITH_FAILURES: {failed_ops}/{total_ops} operations failed")
else:
    dbutils.notebook.exit(f"SUCCESS: {total_ops} operations completed")
