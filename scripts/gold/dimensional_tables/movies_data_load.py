# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/common

# COMMAND ----------

# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/gold/utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets
# ------------------------------------------------------------
dbutils.widgets.text("source_table_name",   "",          "Source Table Name")
dbutils.widgets.text("target_table_name",   "",          "Target Table Name")
dbutils.widgets.text("s3_target_path",      "",          "Target S3 URI (Delta Location)")
dbutils.widgets.text("source_catalog_name", "movielens", "Source Catalog")
dbutils.widgets.text("source_schema_name",  "silver",    "Source Schema")
dbutils.widgets.text("target_catalog_name", "movielens", "Target Catalog")
dbutils.widgets.text("target_schema_name",  "gold",      "Target Schema")
dbutils.widgets.text("model_version",       "1.0",       "Model Version")

source_table_name   = dbutils.widgets.get("source_table_name")
target_table_name   = dbutils.widgets.get("target_table_name")
s3_target_path      = dbutils.widgets.get("s3_target_path")
source_catalog_name = dbutils.widgets.get("source_catalog_name")
source_schema_name  = dbutils.widgets.get("source_schema_name")
target_catalog_name = dbutils.widgets.get("target_catalog_name")
target_schema_name  = dbutils.widgets.get("target_schema_name")
model_version       = dbutils.widgets.get("model_version")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context
# ------------------------------------------------------------
s3_target_path = validate_s3_path(s3_target_path, "target path")
validate_table_name(target_table_name)
if source_table_name:
    validate_table_name(source_table_name)
etl_meta       = resolve_etl_metadata(include_source_system=False)

target_full = build_table_name(target_catalog_name, target_schema_name, target_table_name)
source_full = build_table_name(source_catalog_name, source_schema_name, source_table_name) if source_table_name else None

# COMMAND ----------

# ------------------------------------------------------------
# Read Silver — PASS rows only
# ------------------------------------------------------------
silver_version = get_silver_version(source_full)
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

# COMMAND ----------

# ------------------------------------------------------------
# Transform + Surrogate Key
#
# Surrogate key strategy: SHA2-256 of movie_id (natural key).
#
# Why SHA2(movie_id) not SHA2(title):
#   movie_id is the stable business identifier. Title can
#   change (e.g. re-releases, international titles). Basing
#   the SK on a mutable attribute would cause SK drift.
#
# Column order convention (applied to all dims):
#   1. Surrogate key  (_sk)     — PK for all FK references
#   2. Natural key    (_id)     — preserved for debugging & joins
#   3. Business attributes      — descriptive columns
#   4. ETL metadata columns     — appended by append_gold_metadata()
#
# SCD2 note: when SCD2 is implemented, movie_sk becomes
#   SHA2(CONCAT(CAST(movie_id AS STRING), '|', effective_start_date), 256)
#   so each version of the same movie gets a unique SK.
#   The generate_surrogate_key() call below only changes its
#   natural_key_cols argument — no structural change needed.
# ------------------------------------------------------------
from pyspark.sql import functions as F

df_gold = (
    df_pass
    .select("movie_id", "title", "release_year")
    .orderBy("movie_id")
)

# Generate surrogate key — becomes the PK referenced by all fact/bridge tables
df_gold = generate_surrogate_key(df_gold, "movie_sk", "movie_id")

# Reorder: SK first, then natural key, then attributes
df_gold = df_gold.select("movie_sk", "movie_id", "title", "release_year")

final_count = df_gold.count()
print(f"[INFO] Gold records : {final_count:,}")

# COMMAND ----------

# ------------------------------------------------------------
# Append ETL Metadata + Write + Register + Validate
#
# Note: OPTIMIZE, ANALYZE TABLE, and VACUUM are handled by the
# dedicated maintenance notebook (scripts/maintenance/jobs/table_maintenance.py)
# scheduled during off-peak hours — decoupled from ETL.
# ------------------------------------------------------------
df_gold = append_gold_metadata(
    df_gold, etl_meta, source_full, model_version, silver_version
)

final_count = write_gold(df_gold, s3_target_path, target_table_name)

register_table(spark, target_full, s3_target_path)

# PK is now movie_sk (surrogate), not movie_id (natural key)

# COMMAND ----------

main_fields = {"Target": target_full, "Location": s3_target_path}
if source_full: main_fields["Source"] = source_full
print_pipeline_summary("GOLD", "dim_movies".upper() + " CREATION", {"": main_fields, "ETL Metadata": {"_job_run_id": etl_meta["job_run_id"], "_notebook_path": etl_meta["notebook_path"], "_model_version": model_version}, "Run Details": {
        "Silver rows total"      : f"{initial_count:,}",
        "Quarantined (excluded)" : f"{quarantine_count:,}",
        "Gold rows written"      : f"{final_count:,}",
        "Silver version read"    : silver_version,
        "Columns"                : "movie_sk (PK), movie_id (NK), title, release_year",
        "SK generation"          : "SHA2(CAST(movie_id AS STRING), 256)",
        "genres excluded"        : "Lives in dim_genres (normalized)",
        "Write strategy"         : "Full overwrite + mergeSchema",
    }})
