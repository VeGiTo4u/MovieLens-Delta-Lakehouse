"""Pure Silver transform and DQ rules for MovieLens genome scores."""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def transform_genome_scores(df):
    """Clean and conform Bronze genome score data to Silver standards."""
    return (
        df
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("tag_id", F.col("tagId").cast(IntegerType()))
        .withColumn("relevance", F.col("relevance").cast(DoubleType()))
        .withColumn("relevance", F.round(F.col("relevance"), 3))
        .select("movie_id", "tag_id", "relevance", "_ingestion_timestamp")
    )


def get_dq_rules():
    """Return DQ rules that mark malformed genome score rows for quarantine."""
    return [
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NULL_TAG_ID", F.col("tag_id").isNull()),
        ("NULL_RELEVANCE", F.col("relevance").isNull()),
        ("INVALID_RELEVANCE_RANGE", ~F.col("relevance").between(0.0, 1.0)),
    ]
