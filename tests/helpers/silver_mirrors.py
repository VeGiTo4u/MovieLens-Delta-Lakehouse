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


def transform_tags(df):
    """Mirror of production transform_tags() from tags_data_cleaning.py."""
    return (
        df
        .withColumn("user_id",  F.col("userId").cast(IntegerType()))
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("tag_raw",  F.col("tag").cast(StringType()))
        .withColumn("tag", F.trim(F.regexp_replace(F.col("tag_raw"), r"[\n\t\r]+", " ")))
        .withColumn("tag", F.regexp_replace(F.col("tag"), r"[^a-zA-Z0-9\s\-]", ""))
        .withColumn("tag", F.regexp_replace(F.col("tag"), r"\s+", " "))
        .withColumn("tag", F.initcap(F.trim(F.col("tag"))))
        .withColumn("tag_timestamp",
                    F.from_unixtime(F.col("timestamp")).cast(TimestampType()))
        .withColumn("date_key",
                    F.date_format(F.col("tag_timestamp"), "yyyyMMdd").cast(IntegerType()))
        .select("user_id", "movie_id", "tag", "tag_timestamp", "date_key",
                "_ingestion_timestamp")
    )


def tags_dq_rules():
    """Mirror of tags DQ rules from tags_data_cleaning.py."""
    return [
        ("NULL_USER_ID",  F.col("user_id").isNull()),
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NULL_TAG",      F.col("tag").isNull()),
        ("EMPTY_TAG",     F.trim(F.col("tag")) == ""),
        ("NULL_TIMESTAMP", F.col("tag_timestamp").isNull()),
        ("INVALID_TIMESTAMP_FLOOR",
         F.col("tag_timestamp").isNotNull() &
         (F.col("tag_timestamp") < F.lit("1995-01-01").cast(TimestampType()))),
        ("SHORT_TAG",     F.length(F.col("tag")) < 3),
        ("DATE_KEY_MISMATCH",
         F.col("date_key") != F.date_format(F.col("tag_timestamp"), "yyyyMMdd").cast(IntegerType())),
    ]


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


def transform_links(df):
    """Mirror of production transform_links() from links_data_cleaning.py."""
    return (
        df
        .withColumn("movie_id", F.col("movieId").cast(IntegerType()))
        .withColumn("imdb_id",  F.col("imdbId").cast(StringType()))
        .withColumn("tmdb_id",  F.col("tmdbId").cast(StringType()))
        .withColumn("imdb_id",
                    F.when(F.col("imdb_id").isNotNull(),
                           F.concat(F.lit("tt"), F.col("imdb_id"))
                    ).otherwise(None))
        .withColumn("has_external_ids",
                    F.col("imdb_id").isNotNull() | F.col("tmdb_id").isNotNull())
        .select("movie_id", "imdb_id", "tmdb_id", "has_external_ids",
                "_ingestion_timestamp")
    )


def links_dq_rules():
    """Mirror of links DQ rules from links_data_cleaning.py."""
    return [
        ("NULL_MOVIE_ID",    F.col("movie_id").isNull()),
        ("NO_EXTERNAL_IDS",  ~F.col("has_external_ids")),
    ]


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


def transform_genome_scores(df):
    """Mirror of production transform_genome_scores() from genome_scores_data_cleaning.py."""
    return (
        df
        .withColumn("movie_id",  F.col("movieId").cast(IntegerType()))
        .withColumn("tag_id",    F.col("tagId").cast(IntegerType()))
        .withColumn("relevance", F.col("relevance").cast(DoubleType()))
        .withColumn("relevance", F.round(F.col("relevance"), 3))
        .select("movie_id", "tag_id", "relevance", "_ingestion_timestamp")
    )


def genome_scores_dq_rules():
    """Mirror of genome_scores DQ rules from genome_scores_data_cleaning.py."""
    return [
        ("NULL_MOVIE_ID",  F.col("movie_id").isNull()),
        ("NULL_TAG_ID",    F.col("tag_id").isNull()),
        ("NULL_RELEVANCE", F.col("relevance").isNull()),
        ("INVALID_RELEVANCE_RANGE", ~F.col("relevance").between(0.0, 1.0)),
    ]


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


def transform_genome_tags(df):
    """Mirror of production transform_genome_tags() from genome_tags_data_cleaning.py."""
    return (
        df
        .withColumn("tag_id",  F.col("tagId").cast(IntegerType()))
        .withColumn("tag_raw", F.col("tag").cast(StringType()))
        .withColumn("tag",
                    F.trim(F.regexp_replace(
                        F.regexp_replace(F.col("tag_raw"), r"[\n\t\r]+", " "),
                        r"\s+", " ")))
        .withColumn("tag", F.initcap(F.col("tag")))
        .select("tag_id", "tag", "_ingestion_timestamp")
    )


def genome_tags_dq_rules():
    """Mirror of genome_tags DQ rules from genome_tags_data_cleaning.py."""
    return [
        ("NULL_TAG_ID", F.col("tag_id").isNull()),
        ("NULL_TAG",    F.col("tag").isNull()),
        ("EMPTY_TAG",   F.trim(F.col("tag")) == ""),
    ]
