# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/common

# COMMAND ----------

# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/silver/utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets — Parameterized for Job Orchestration
#
# Identical widget interface across all Silver notebooks ensures
# a single Databricks Job definition can drive any cleaning
# notebook by swapping widget values.
# ------------------------------------------------------------
dbutils.widgets.text("source_table_name",  "",          "Source Table Name")
dbutils.widgets.text("target_table_name",  "",          "Target Table Name")
dbutils.widgets.text("s3_target_path",     "",          "Target S3 URI (Delta Location)")
dbutils.widgets.text("source_catalog_name","movielens", "Source Catalog")
dbutils.widgets.text("source_schema_name", "bronze",    "Source Schema")
dbutils.widgets.text("target_catalog_name","movielens", "Target Catalog")
dbutils.widgets.text("target_schema_name", "silver",    "Target Schema")

source_table_name   = dbutils.widgets.get("source_table_name")
target_table_name   = dbutils.widgets.get("target_table_name")
s3_target_path      = dbutils.widgets.get("s3_target_path")
source_catalog_name = dbutils.widgets.get("source_catalog_name")
source_schema_name  = dbutils.widgets.get("source_schema_name")
target_catalog_name = dbutils.widgets.get("target_catalog_name")
target_schema_name  = dbutils.widgets.get("target_schema_name")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context — Fail Fast Before Any Spark Work
# ------------------------------------------------------------
s3_target_path = validate_s3_path(s3_target_path, "target path")
validate_table_name(source_table_name, "source_table_name")
validate_table_name(target_table_name, "target_table_name")
etl_meta       = resolve_etl_metadata(include_source_system=True)

source_full = build_table_name(source_catalog_name, source_schema_name, source_table_name)
target_full = build_table_name(target_catalog_name, target_schema_name, target_table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Imports — Deferred Until After %run and Validation
#
# Imports are placed after %run silver_utils so that the utils
# module is available, and after validation so that a bad input
# fails before Spark loads any libraries.
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
import sys

REPO_ROOT = "/Workspace/MovieLens-Delta-Lakehouse"
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

# COMMAND ----------

# ------------------------------------------------------------
# Read Bronze — Full Table Scan (Static Dataset)
#
# Static datasets (movies, links, genome_*) load the entire
# Bronze table. Unlike ratings/tags, there is no incremental
# partitioning — the full table is re-cleaned on every run.
# This is safe because the dataset is small and the write
# strategy is full overwrite.
# ------------------------------------------------------------
df_bronze      = read_bronze(source_full)
initial_count  = df_bronze.count()

# COMMAND ----------

# ------------------------------------------------------------
# Import production transform functions (single source of truth
# in scripts/silver/transforms/movies — tested by pytest).
# ------------------------------------------------------------
from scripts.silver.transforms.movies import get_dq_rules, transform_movies

# COMMAND ----------

# ------------------------------------------------------------
# Transform → DQ flag → Metadata → Write
# ------------------------------------------------------------
df_transformed = transform_movies(df_bronze)
df_flagged     = apply_dq_flags(df_transformed, get_dq_rules())
df_silver      = append_static_metadata(df_flagged, etl_meta)

final_count       = df_silver.count()
quarantine_count  = df_silver.filter(F.col("_dq_status") == "QUARANTINE").count()

print(f"[INFO] Records to write  : {final_count:,}")
print(f"[INFO] Quarantined       : {quarantine_count:,}")

write_static(df_silver, s3_target_path, target_table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(spark, target_full, s3_target_path)


# Table-specific metrics
null_year_count   = df_silver.filter(F.col("release_year").isNull()).count()
empty_genre_count = df_silver.filter(F.size(F.col("genres")) == 0).count()

extra_info = {
    "Quarantined records"   : f"{quarantine_count:,}",
    "Movies without year"   : f"{null_year_count:,}",
    "Movies without genres" : f"{empty_genre_count:,}",
    "Write strategy"        : "Full overwrite + mergeSchema",
}
extra_info.update({
    "Initial count": f"{initial_count:,}",
    "Final count": f"{final_count:,}",
})

print_pipeline_summary(
    "SILVER", "TRANSFORMATION", 
    {
        "": {
            "Source Table": source_full,
            "Target Table": target_full,
            "Target S3": s3_target_path,
        },
        "ETL Metadata": {
            "_job_run_id": etl_meta["job_run_id"],
            "_notebook_path": etl_meta["notebook_path"],
            "_source_system": etl_meta.get("source_system", "UNKNOWN"),
        },
        "Run Details": extra_info,
    }
)
