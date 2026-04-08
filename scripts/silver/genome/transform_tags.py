# Databricks notebook source
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
s3_target_path = validate_inputs(s3_target_path, source_table_name, target_table_name)
etl_meta       = resolve_etl_metadata()

source_full, target_full = build_table_names(
    source_catalog_name, source_schema_name, source_table_name,
    target_catalog_name, target_schema_name, target_table_name
)

# COMMAND ----------

# ------------------------------------------------------------
# Imports — Deferred Until After %run and Validation
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

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
# Transformation — Business Logic Isolation
# ------------------------------------------------------------
def transform_genome_tags(df):
    """
    Cleans and conforms Bronze genome_tags data to Silver standards.

    Steps:
      1. Column rename + type cast
      2. Whitespace normalization — trim + collapse internal whitespace
      3. Title Case formatting
    """
    return (
        df
        # Step 1: Cast
        .withColumn("tag_id",  F.col("tagId").cast(IntegerType()))
        .withColumn("tag_raw", F.col("tag").cast(StringType()))

        # Step 2: Trim + collapse internal whitespace (tabs, newlines, multiple spaces)
        .withColumn("tag",
                    F.trim(F.regexp_replace(
                        F.regexp_replace(F.col("tag_raw"), r"[\n\t\r]+", " "),
                        r"\s+", " "
                    )))

        # Step 3: Title Case
        .withColumn("tag", F.initcap(F.col("tag")))

        .select(
            "tag_id",
            "tag",
            "_ingestion_timestamp",
        )
    )

# COMMAND ----------

# ------------------------------------------------------------
# DQ Rules for genome_tags
# ------------------------------------------------------------
def get_dq_rules():
    return [
        ("NULL_TAG_ID",
         F.col("tag_id").isNull()),

        ("NULL_TAG",
         F.col("tag").isNull()),

        ("EMPTY_TAG",
         F.trim(F.col("tag")) == ""),
    ]

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
register_table(target_full, s3_target_path)

post_write_validation(target_full, final_count)

tag_length_stats = df_silver.select(
    F.min(F.length(F.col("tag"))).alias("min_len"),
    F.max(F.length(F.col("tag"))).alias("max_len"),
    F.avg(F.length(F.col("tag"))).alias("avg_len"),
).collect()[0]

print_summary(
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
