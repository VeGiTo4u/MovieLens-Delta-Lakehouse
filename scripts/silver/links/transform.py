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
# Links is a small static dataset (one row per movie), so a
# full table scan + overwrite is the correct strategy.
# ------------------------------------------------------------
df_bronze     = read_bronze(source_full)
initial_count = df_bronze.count()

# COMMAND ----------

# ------------------------------------------------------------
# Transformation — Business Logic Isolation
# ------------------------------------------------------------
def transform_links(df):
    """
    Cleans and conforms Bronze links data to Silver standards.

    Steps:
      1. Column rename + type cast
      2. IMDB ID formatting — prepend 'tt' prefix (e.g. tt0114709)
         NULL imdb_id values are preserved as NULL
      3. has_external_ids flag — True if either imdb_id or tmdb_id
         is present. Useful for Gold FK validation completeness checks.

    Note: NULL imdb_id / tmdb_id are valid — not every movie has
    external IDs. Only NULL movie_id is a DQ failure (primary key).
    """
    return (
        df
        # Step 1: Rename + cast
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("imdb_id",  F.col("imdbId").cast(StringType()))
        .withColumn("tmdb_id",  F.col("tmdbId").cast(StringType()))

        # Step 2: Prepend 'tt' prefix to imdb_id where not null
        .withColumn("imdb_id",
                    F.when(
                        F.col("imdb_id").isNotNull(),
                        F.concat(F.lit("tt"), F.col("imdb_id"))
                    ).otherwise(None))

        # Step 3: Quality flag — does this movie have any external ID?
        .withColumn("has_external_ids",
                    F.col("imdb_id").isNotNull() | F.col("tmdb_id").isNotNull())

        .select(
            "movie_id",
            "imdb_id",
            "tmdb_id",
            "has_external_ids",
            "_ingestion_timestamp",
        )
    )

# COMMAND ----------

# ------------------------------------------------------------
# DQ Rules for links
# ------------------------------------------------------------
def get_dq_rules():
    return [
        ("NULL_MOVIE_ID",
         F.col("movie_id").isNull()),

        # Both external IDs missing is a soft quality concern —
        # the movie exists but has no cross-reference capability
        ("NO_EXTERNAL_IDS",
         ~F.col("has_external_ids")),
    ]

# COMMAND ----------

# ------------------------------------------------------------
# Transform → DQ flag → Metadata → Write
# ------------------------------------------------------------
df_transformed = transform_links(df_bronze)
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

no_external_ids_count = df_silver.filter(~F.col("has_external_ids")).count()
null_imdb_count       = df_silver.filter(F.col("imdb_id").isNull()).count()
null_tmdb_count       = df_silver.filter(F.col("tmdb_id").isNull()).count()

print_summary(
    source_full_table_name = source_full,
    target_full_table_name = target_full,
    s3_target_path         = s3_target_path,
    initial_count          = initial_count,
    final_count            = final_count,
    etl_meta               = etl_meta,
    extra_info             = {
        "Quarantined records"     : f"{quarantine_count:,}",
        "Movies with no ext IDs"  : f"{no_external_ids_count:,}",
        "NULL imdb_id"            : f"{null_imdb_count:,}",
        "NULL tmdb_id"            : f"{null_tmdb_count:,}",
        "Write strategy"          : "Full overwrite + mergeSchema",
    }
)
