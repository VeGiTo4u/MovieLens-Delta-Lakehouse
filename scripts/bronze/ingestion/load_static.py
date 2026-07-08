# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/common
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/bronze/utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets — Parameterized for Job Orchestration
#
# Widgets allow this notebook to be invoked by Databricks Jobs
# or parent notebooks with different table/path arguments,
# avoiding hardcoded values that break reuse across environments.
# ------------------------------------------------------------
dbutils.widgets.text("s3_source_path", "", "Source S3 URI (CSV)")
dbutils.widgets.text("s3_target_path", "", "Target S3 URI (Delta Location)")
dbutils.widgets.text("table_name",     "", "Target Table Name")
dbutils.widgets.text("catalog_name",   "movielens", "Catalog")
dbutils.widgets.text("schema_name",    "bronze",    "Schema")

s3_source_path = dbutils.widgets.get("s3_source_path")
s3_target_path = dbutils.widgets.get("s3_target_path")
table_name     = dbutils.widgets.get("table_name")
catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context — Fail Fast Before Any Spark Work
#
# Validates inputs and resolves ETL metadata (job ID, notebook
# path) upfront so failures surface immediately, not mid-write.
# ------------------------------------------------------------
s3_source_path = validate_s3_path(s3_source_path, "source path")
s3_target_path = validate_s3_path(s3_target_path, "target path")
validate_table_name(table_name)
etl_meta        = resolve_etl_metadata(include_source_system=True)
full_table_name = build_table_name(catalog_name, schema_name, table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Schemas
# Schema inference is intentionally avoided.
# Static tables: genome_scores, genome_tags, links, movies.
# ------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType
)

schemas = {
    "genome_scores": StructType([
        StructField("movieId",   IntegerType(), True),
        StructField("tagId",     IntegerType(), True),
        StructField("relevance", DoubleType(),  True),
    ]),
    "genome_tags": StructType([
        StructField("tagId", IntegerType(), True),
        StructField("tag",   StringType(),  True),
    ]),
    "links": StructType([
        StructField("movieId", IntegerType(), True),
        StructField("imdbId",  StringType(),  True),  # STRING preserves leading zeros
        StructField("tmdbId",  IntegerType(), True),
    ]),
    "movies": StructType([
        StructField("movieId", IntegerType(), True),
        StructField("title",   StringType(),  True),
        StructField("genres",  StringType(),  True),  # Pipe-separated, normalized in Silver
    ]),
}

if table_name not in schemas:
    raise ValueError(
        f"Unsupported table_name '{table_name}'. "
        f"Supported: {list(schemas.keys())}"
    )

print(f"[START] Bronze static ingestion for table: {full_table_name}")
print(f"Source : {s3_source_path}")
print(f"Target : {s3_target_path}")

# COMMAND ----------

# ------------------------------------------------------------
# Read → Metadata → Write
# ------------------------------------------------------------
from pyspark.sql import functions as F

df_raw = (
    spark.read
         .format("csv")
         .option("header", "true")
         .schema(schemas[table_name])
         .load(s3_source_path)
)

df_bronze       = append_static_metadata(df_raw, etl_meta)
records_written = write_static_bronze(df_bronze, s3_target_path, table_name)

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate + Summary
# ------------------------------------------------------------
register_table(spark, full_table_name, s3_target_path)


print_pipeline_summary(
    "BRONZE", "Static INGESTION",
    {
        "": {
            "Table": full_table_name,
            "Source": s3_source_path,
            "Target": s3_target_path,
        },
        "ETL Metadata": {
            "_job_run_id": etl_meta["job_run_id"],
            "_notebook_path": etl_meta["notebook_path"],
            "_source_system": etl_meta["source_system"],
        },
        "Run Details": {
        "Records written" : f"{records_written:,}",
        "Write strategy"  : "Full overwrite + mergeSchema",
    }
})
