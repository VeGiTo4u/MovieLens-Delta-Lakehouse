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
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# ------------------------------------------------------------
# Read Bronze — Full Table Scan (Static Dataset)
#
# genome_scores is a static dataset (~14M rows). Even at this
# volume, full overwrite is the correct approach because the
# dataset has no time-based incremental key — all scores are
# point-in-time snapshots.
# ------------------------------------------------------------
df_bronze     = read_bronze(source_full)
initial_count = df_bronze.count()

# COMMAND ----------

# ------------------------------------------------------------
# Transformation — Business Logic Isolation
# ------------------------------------------------------------
def transform_genome_scores(df):
    """
    Cleans and conforms Bronze genome_scores data to Silver standards.

    Steps:
      1. Column rename + type cast
      2. Round relevance to 3 decimal places for storage efficiency
         (source precision beyond 3dp is noise for downstream ML use)
    """
    return (
        df
        # Step 1: Rename + cast
        .withColumn("movie_id",  F.col("movieId").cast(IntegerType()))
        .withColumn("tag_id",    F.col("tagId").cast(IntegerType()))
        .withColumn("relevance", F.col("relevance").cast(DoubleType()))

        # Step 2: Round to 3 decimal places
        .withColumn("relevance", F.round(F.col("relevance"), 3))

        .select(
            "movie_id",
            "tag_id",
            "relevance",
            "_ingestion_timestamp",
        )
    )

# COMMAND ----------

# ------------------------------------------------------------
# DQ Rules for genome_scores
# ------------------------------------------------------------
def get_dq_rules():
    return [
        ("NULL_MOVIE_ID",
         F.col("movie_id").isNull()),

        ("NULL_TAG_ID",
         F.col("tag_id").isNull()),

        ("NULL_RELEVANCE",
         F.col("relevance").isNull()),

        # Relevance is a probability score — must be 0.0 to 1.0
        ("INVALID_RELEVANCE_RANGE",
         ~F.col("relevance").between(0.0, 1.0)),
    ]

# COMMAND ----------

# ------------------------------------------------------------
# Transform → DQ flag → Metadata → Write
# ------------------------------------------------------------
df_transformed = transform_genome_scores(df_bronze)
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

relevance_stats = df_silver.filter(
    F.col("_dq_status") == "PASS"
).select(
    F.min("relevance").alias("min_r"),
    F.max("relevance").alias("max_r"),
    F.avg("relevance").alias("avg_r"),
).collect()[0]

print_summary(
    source_full_table_name = source_full,
    target_full_table_name = target_full,
    s3_target_path         = s3_target_path,
    initial_count          = initial_count,
    final_count            = final_count,
    etl_meta               = etl_meta,
    extra_info             = {
        "Quarantined records"  : f"{quarantine_count:,}",
        "Relevance min/max"    : f"{relevance_stats['min_r']:.3f} / {relevance_stats['max_r']:.3f}",
        "Relevance avg"        : f"{relevance_stats['avg_r']:.6f}",
        "Write strategy"       : "Full overwrite + mergeSchema",
    }
)
