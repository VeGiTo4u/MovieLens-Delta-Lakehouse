"""Pure Silver transform and DQ rules for MovieLens external links."""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def transform_links(df):
    """Clean and conform Bronze links data to Silver standards."""
    return (
        df
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("imdb_id", F.col("imdbId").cast(StringType()))
        .withColumn("tmdb_id", F.col("tmdbId").cast(StringType()))
        .withColumn(
            "imdb_id",
            F.when(F.col("imdb_id").isNotNull(), F.concat(F.lit("tt"), F.col("imdb_id"))).otherwise(None),
        )
        .withColumn("has_external_ids", F.col("imdb_id").isNotNull() | F.col("tmdb_id").isNotNull())
        .select("movie_id", "imdb_id", "tmdb_id", "has_external_ids", "_ingestion_timestamp")
    )


def get_dq_rules():
    """Return DQ rules that mark malformed links rows for quarantine."""
    return [
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NO_EXTERNAL_IDS", ~F.col("has_external_ids")),
    ]
