# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/common
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/silver/utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets — Parameterized for Job Orchestration
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
# genome_tags is a small lookup table (~1K rows) mapping tag
# IDs to human-readable labels. Full overwrite is the correct
# strategy as the dataset has no incremental key.
# ------------------------------------------------------------
df_bronze     = read_bronze(source_full)
initial_count = df_bronze.count()

# COMMAND ----------

# ------------------------------------------------------------
# Import production transform functions (single source of truth
# in scripts/silver/transforms/genome_tags — tested by pytest).
# ------------------------------------------------------------
from scripts.silver.transforms.genome_tags import get_dq_rules, transform_genome_tags

# COMMAND ----------

# ------------------------------------------------------------
# Transform → DQ flag → Metadata → Write
# ------------------------------------------------------------
df_transformed = transform_genome_tags(df_bronze)
df_flagged     = apply_dq_flags(df_transformed, get_dq_rules())
df_silver      = append_static_metadata(df_flagged, etl_meta)

final_count      = df_silver.count()
quarantine_count = df_silver.filter(F.col("_dq_status") == "QUARANTINE").count()

print(f"[INFO] Records to write : {final_count:,}")
print(f"[INFO] Quarantined      : {quarantine_count:,}")

write_static(df_silver, s3_target_path, target_table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(spark, target_full, s3_target_path)


tag_length_stats = df_silver.select(
    F.min(F.length(F.col("tag"))).alias("min_len"),
    F.max(F.length(F.col("tag"))).alias("max_len"),
    F.avg(F.length(F.col("tag"))).alias("avg_len"),
).collect()[0]

print_pipeline_summary("SILVER", "TRANSFORMATION", 
    source_full_table_name = source_full,
    target_full_table_name = target_full,
    s3_target_path         = s3_target_path,
    initial_count          = initial_count,
    final_count            = final_count,
    etl_meta               = etl_meta,
    extra_info             = {
        "Quarantined records" : f"{quarantine_count:,}",
        "Tag length min/max"  : f"{tag_length_stats['min_len']} / {tag_length_stats['max_len']}",
        "Tag length avg"      : f"{tag_length_stats['avg_len']:.2f}",
        "Write strategy"      : "Full overwrite + mergeSchema",
    }
)
