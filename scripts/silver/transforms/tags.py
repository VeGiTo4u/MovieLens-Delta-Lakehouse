"""Pure Silver transform and DQ rules for MovieLens tags."""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType


def transform_tags(df):
    """Clean and conform Bronze tags data to Silver standards."""
    return (
        df
        .withColumn("user_id", F.col("userId").cast(IntegerType()))
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("tag_raw", F.col("tag").cast(StringType()))
        .withColumn("tag", F.trim(F.regexp_replace(F.col("tag_raw"), r"[\n\t\r]+", " ")))
        .withColumn("tag", F.regexp_replace(F.col("tag"), r"[^a-zA-Z0-9\s\-]", ""))
        .withColumn("tag", F.regexp_replace(F.col("tag"), r"\s+", " "))
        .withColumn("tag", F.initcap(F.trim(F.col("tag"))))
        .withColumn("tag_timestamp", F.from_unixtime(F.col("timestamp")).cast(TimestampType()))
        .withColumn(
            "date_key",
            F.date_format(F.col("tag_timestamp"), "yyyyMMdd").cast(IntegerType()),
        )
        .select(
            "user_id",
            "movie_id",
            "tag",
            "tag_timestamp",
            "date_key",
            "_ingestion_timestamp",
        )
    )


def get_dq_rules():
    """Return DQ rules that mark malformed tag rows for quarantine."""
    return [
        ("NULL_USER_ID", F.col("user_id").isNull()),
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NULL_TAG", F.col("tag").isNull()),
        ("EMPTY_TAG", F.trim(F.col("tag")) == ""),
        ("NULL_TIMESTAMP", F.col("tag_timestamp").isNull()),
        (
            "INVALID_TIMESTAMP_FLOOR",
            F.col("tag_timestamp").isNotNull()
            & (F.col("tag_timestamp") < F.lit("1995-01-01").cast(TimestampType())),
        ),
        ("SHORT_TAG", F.length(F.col("tag")) < 3),
        (
            "DATE_KEY_MISMATCH",
            F.col("date_key")
            != F.date_format(F.col("tag_timestamp"), "yyyyMMdd").cast(IntegerType()),
        ),
    ]
