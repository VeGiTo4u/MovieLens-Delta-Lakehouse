"""Shared mirrors of Silver notebook logic used by tests only."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_RATINGS_SCHEMA = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_batch_year", IntegerType(), True),
])


BRONZE_MOVIES_SCHEMA = StructType([
    StructField("movieId", StringType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def make_bronze_ratings_df(spark, rows, schema=BRONZE_RATINGS_SCHEMA):
    return spark.createDataFrame(rows, schema)


def make_bronze_movies_df(spark, rows, schema=BRONZE_MOVIES_SCHEMA):
    return spark.createDataFrame(rows, schema)


def transform_ratings(df):
    """Mirror of src/silver/ratings_data_cleaning.py transform logic."""
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


def ratings_dq_rules():
    """Mirror of ratings DQ rules from src/silver/ratings_data_cleaning.py."""
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


def transform_movies(df):
    """Mirror of src/silver/movies_data_cleaning.py transform logic."""
    return (
        df
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("title_raw", F.trim(F.col("title").cast(StringType())))
        .withColumn("genres_raw", F.col("genres").cast(StringType()))
        .withColumn("year_str", F.regexp_extract(F.col("title_raw"), r"\((\d{4})\)\s*$", 1))
        .withColumn(
            "release_year",
            F.when(
                (F.col("year_str") != "") & F.col("year_str").isNotNull(),
                F.col("year_str").cast(IntegerType()),
            ).otherwise(None),
        )
        .withColumn(
            "release_year",
            F.when(F.col("release_year").between(1800, 2199), F.col("release_year")).otherwise(None),
        )
        .withColumn(
            "title_no_year",
            F.trim(F.regexp_replace(F.col("title_raw"), r"\s*\(\d{4}\)\s*$", "")),
        )
        .withColumn(
            "title",
            F.initcap(
                F.trim(
                    F.when(
                        F.col("title_no_year").rlike(
                            r",\s*(The|A|An|Le|La|Les|Das|Der|Die|El|Il|Los|Las)\s*$"
                        ),
                        F.concat(
                            F.regexp_extract(
                                F.col("title_no_year"),
                                r",\s*(The|A|An|Le|La|Les|Das|Der|Die|El|Il|Los|Las)\s*$",
                                1,
                            ),
                            F.lit(" "),
                            F.regexp_replace(
                                F.col("title_no_year"),
                                r",\s*(The|A|An|Le|La|Les|Das|Der|Die|El|Il|Los|Las)\s*$",
                                "",
                            ),
                        ),
                    ).otherwise(F.col("title_no_year"))
                )
            ),
        )
        .withColumn(
            "genres",
            F.when(
                F.col("genres_raw").isNull()
                | (F.trim(F.col("genres_raw")) == "")
                | F.lower(F.col("genres_raw")).contains("no genres listed"),
                F.array(),
            ).otherwise(
                F.transform(
                    F.split(F.col("genres_raw"), r"\|"),
                    lambda genre: F.initcap(F.trim(genre)),
                )
            ),
        )
        .select("movie_id", "title", "release_year", "genres", "_ingestion_timestamp")
    )


def movies_dq_rules():
    """Mirror of movies DQ rules from src/silver/movies_data_cleaning.py."""
    return [
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NULL_TITLE", F.col("title").isNull() | (F.trim(F.col("title")) == "")),
        (
            "INVALID_RELEASE_YEAR",
            F.col("release_year").isNotNull() & ~F.col("release_year").between(1800, 2199),
        ),
    ]
