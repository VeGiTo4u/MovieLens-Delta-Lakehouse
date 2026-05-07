"""Pure Silver transform and DQ rules for MovieLens movies."""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def transform_movies(df):
    """Clean and conform Bronze movies data to Silver standards."""
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


def get_dq_rules():
    """Return DQ rules that mark malformed movie rows for quarantine."""
    return [
        ("NULL_MOVIE_ID", F.col("movie_id").isNull()),
        ("NULL_TITLE", F.col("title").isNull() | (F.trim(F.col("title")) == "")),
        (
            "INVALID_RELEASE_YEAR",
            F.col("release_year").isNotNull() & ~F.col("release_year").between(1800, 2199),
        ),
    ]
