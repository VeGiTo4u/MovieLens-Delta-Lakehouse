"""Pure Silver transform and DQ rules for MovieLens ratings."""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


def transform_ratings(df):
    """Clean and conform Bronze ratings data to Silver standards."""
    return (
        df
        .withColumn("user_id", F.col("userId").cast(IntegerType()))
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("rating", F.col("rating").cast(DoubleType()))
        .withColumn(
            "interaction_timestamp",
            F.from_unixtime(F.col("timestamp")).cast(TimestampType()),
        )
        .withColumn(
            "date_key",
            F.date_format(F.col("interaction_timestamp"), "yyyyMMdd").cast(IntegerType()),
        )
        .withColumn("rating", F.round(F.col("rating"), 1))
        .withColumn(
            "is_late_arrival",
            F.year(F.col("interaction_timestamp")) != F.col("_batch_year"),
        )
        .withColumn("rating_year", F.year(F.col("interaction_timestamp")))
        .withColumn("is_current", F.lit(True))
        .withColumn("effective_start_date", F.col("interaction_timestamp"))
        .withColumn("effective_end_date", F.lit(None).cast(TimestampType()))
        .select(
            "user_id",
            "movie_id",
            "rating",
            "interaction_timestamp",
            "date_key",
            "is_late_arrival",
            "rating_year",
            "is_current",
            "effective_start_date",
            "effective_end_date",
            "_ingestion_timestamp",
            "_batch_year",
        )
    )


def get_dq_rules():
    """Return DQ rules that mark malformed ratings rows for quarantine."""
    return [
        ("NULL_USER_ID", F.col("user_id").isNull()),
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NULL_RATING", F.col("rating").isNull()),
        ("NULL_TIMESTAMP", F.col("interaction_timestamp").isNull()),
        (
            "INVALID_TIMESTAMP_FLOOR",
            F.col("interaction_timestamp").isNotNull()
            & (F.col("interaction_timestamp") < F.lit("1995-01-01").cast(TimestampType())),
        ),
        ("INVALID_RATING_RANGE", ~F.col("rating").between(0.0, 5.0)),
        (
            "DATE_KEY_MISMATCH",
            F.col("date_key")
            != F.date_format(F.col("interaction_timestamp"), "yyyyMMdd").cast(IntegerType()),
        ),
    ]
