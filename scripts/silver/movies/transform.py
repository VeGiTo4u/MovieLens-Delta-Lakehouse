# Databricks notebook source
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
s3_target_path = validate_inputs(s3_target_path, source_table_name, target_table_name)
etl_meta       = resolve_etl_metadata()

source_full, target_full = build_table_names(
    source_catalog_name, source_schema_name, source_table_name,
    target_catalog_name, target_schema_name, target_table_name
)

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
# Transformation — Business Logic Isolation
#
# Transform is a pure function: DataFrame in → DataFrame out.
# No side effects, no writes. This makes it testable in
# isolation and ensures the DQ → metadata → write pipeline
# always receives a deterministic input.
# ------------------------------------------------------------
def transform_movies(df):
    """
    Cleans and conforms Bronze movies data to Silver standards.

    Steps:
      1. Column rename + type cast
      2. Release year extraction from title — pattern (YYYY) at END of
         string only, validated to range 1800–2199
      3. Title cleaning — remove trailing year pattern, reorder trailing
         articles (e.g. "Dark Knight, The" → "The Dark Knight"),
         Title Case, trim
      4. Genres normalization — pipe-separated string → ARRAY<STRING>
         Title Case per element, empty/null/"no genres listed" → []
    """
    return (
        df
        # Step 1: Cast
        .withColumn("movie_id",   F.col("movieId").cast(IntegerType()))
        .withColumn("title_raw",  F.trim(F.col("title").cast(StringType())))
        .withColumn("genres_raw", F.col("genres").cast(StringType()))

        # Step 2: Extract release year from trailing (YYYY) at end of title
        .withColumn("year_str",
                    F.regexp_extract(F.col("title_raw"), r"\((\d{4})\)\s*$", 1))
        .withColumn("release_year",
                    F.when(
                        (F.col("year_str") != "") & F.col("year_str").isNotNull(),
                        F.col("year_str").cast(IntegerType())
                    ).otherwise(None))
        # Validate year is within realistic range
        .withColumn("release_year",
                    F.when(
                        F.col("release_year").between(1800, 2199),
                        F.col("release_year")
                    ).otherwise(None))

        # Step 3a: Remove trailing (YYYY) only
        .withColumn("title_no_year",
                    F.trim(
                        F.regexp_replace(F.col("title_raw"), r"\s*\(\d{4}\)\s*$", "")
                    ))
        # Step 3b: Move trailing article to front
        #   "Dark Knight, The" → "The Dark Knight"
        #   Supports: The, A, An, Le, La, Les, Das, Der, Die, El, Il, Los, Las
        .withColumn("title",
                    F.initcap(F.trim(
                        F.when(
                            F.col("title_no_year").rlike(
                                r",\s*(The|A|An|Le|La|Les|Das|Der|Die|El|Il|Los|Las)\s*$"
                            ),
                            F.concat(
                                F.regexp_extract(F.col("title_no_year"),
                                    r",\s*(The|A|An|Le|La|Les|Das|Der|Die|El|Il|Los|Las)\s*$", 1),
                                F.lit(" "),
                                F.regexp_replace(F.col("title_no_year"),
                                    r",\s*(The|A|An|Le|La|Les|Das|Der|Die|El|Il|Los|Las)\s*$", "")
                            )
                        ).otherwise(F.col("title_no_year"))
                    )))

        # Step 4: Genres — null/empty/"no genres listed" → empty array
        #         otherwise split by pipe and Title Case each element
        .withColumn("genres",
                    F.when(
                        F.col("genres_raw").isNull() |
                        (F.trim(F.col("genres_raw")) == "") |
                        F.lower(F.col("genres_raw")).contains("no genres listed"),
                        F.array()
                    ).otherwise(
                        F.transform(
                            F.split(F.col("genres_raw"), r"\|"),
                            lambda g: F.initcap(F.trim(g))
                        )
                    ))

        .select(
            "movie_id",
            "title",
            "release_year",
            "genres",
            "_ingestion_timestamp",
        )
    )

# COMMAND ----------

# ------------------------------------------------------------
# DQ Rules for movies
# ------------------------------------------------------------
def get_dq_rules():
    return [
        ("NULL_MOVIE_ID",
         F.col("movie_id").isNull()),

        ("NULL_TITLE",
         F.col("title").isNull() | (F.trim(F.col("title")) == "")),

        # release_year NULL is expected (not all movies have year in title)
        # but if present it must be in range — transformation already enforces
        # this so this rule catches any edge cases that slipped through
        ("INVALID_RELEASE_YEAR",
         F.col("release_year").isNotNull() &
         ~F.col("release_year").between(1800, 2199)),
    ]

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
register_table(target_full, s3_target_path)

post_write_validation(target_full, final_count)

# Table-specific metrics
null_year_count   = df_silver.filter(F.col("release_year").isNull()).count()
empty_genre_count = df_silver.filter(F.size(F.col("genres")) == 0).count()

print_summary(
    source_full_table_name = source_full,
    target_full_table_name = target_full,
    s3_target_path         = s3_target_path,
    initial_count          = initial_count,
    final_count            = final_count,
    etl_meta               = etl_meta,
    extra_info             = {
        "Quarantined records"   : f"{quarantine_count:,}",
        "Movies without year"   : f"{null_year_count:,}",
        "Movies without genres" : f"{empty_genre_count:,}",
        "Write strategy"        : "Full overwrite + mergeSchema",
    }
)
