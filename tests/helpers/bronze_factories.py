"""Bronze DataFrame factory functions for tests.

Provides schemas and factory functions to create Bronze-shaped
DataFrames for all six tables. No transformation or DQ rule
logic — tests import those from `scripts.silver.transforms.*`.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ────────────────────────────────────────────────────────────────
# Ratings
# ────────────────────────────────────────────────────────────────

BRONZE_RATINGS_SCHEMA = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_batch_year", IntegerType(), True),
])


def make_bronze_ratings_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema or BRONZE_RATINGS_SCHEMA)


# ────────────────────────────────────────────────────────────────
# Movies
# ────────────────────────────────────────────────────────────────

BRONZE_MOVIES_SCHEMA = StructType([
    StructField("movieId", StringType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def make_bronze_movies_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema or BRONZE_MOVIES_SCHEMA)


# ────────────────────────────────────────────────────────────────
# Tags
# ────────────────────────────────────────────────────────────────

BRONZE_TAGS_SCHEMA = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def make_bronze_tags_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema or BRONZE_TAGS_SCHEMA)


# ────────────────────────────────────────────────────────────────
# Links
# ────────────────────────────────────────────────────────────────

BRONZE_LINKS_SCHEMA = StructType([
    StructField("movieId", StringType(), True),
    StructField("imdbId",  StringType(), True),
    StructField("tmdbId",  StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def make_bronze_links_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema or BRONZE_LINKS_SCHEMA)


# ────────────────────────────────────────────────────────────────
# Genome Scores
# ────────────────────────────────────────────────────────────────

BRONZE_GENOME_SCORES_SCHEMA = StructType([
    StructField("movieId",   StringType(), True),
    StructField("tagId",     StringType(), True),
    StructField("relevance", StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def make_bronze_genome_scores_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema or BRONZE_GENOME_SCORES_SCHEMA)


# ────────────────────────────────────────────────────────────────
# Genome Tags
# ────────────────────────────────────────────────────────────────

BRONZE_GENOME_TAGS_SCHEMA = StructType([
    StructField("tagId", StringType(), True),
    StructField("tag",   StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def make_bronze_genome_tags_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema or BRONZE_GENOME_TAGS_SCHEMA)
