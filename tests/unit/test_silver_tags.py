"""
test_silver_tags.py — Unit tests for silver.tags transformation logic.

Tests: special char removal, title-casing, timestamp conversion,
       date_key derivation, whitespace normalization, and all DQ rules.
"""
import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, LongType,
)


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


def get_dq_rules():
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


BRONZE_SCHEMA = StructType([
    StructField("userId",  StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("tag",     StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def _make(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


# ═══════════════════════════════════════════════════════════════
# Special Character Removal
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestSpecialCharRemoval:

    def test_removes_exclamation(self, spark):
        r = transform_tags(_make(spark, [("1","1","awesome!",1640995200,datetime(2024,1,1))])).collect()[0]
        assert "!" not in r["tag"]

    def test_removes_at_symbol(self, spark):
        r = transform_tags(_make(spark, [("1","1","tag@here",1640995200,datetime(2024,1,1))])).collect()[0]
        assert "@" not in r["tag"]

    def test_removes_hash(self, spark):
        r = transform_tags(_make(spark, [("1","1","#trending",1640995200,datetime(2024,1,1))])).collect()[0]
        assert "#" not in r["tag"]

    def test_preserves_hyphens(self, spark):
        r = transform_tags(_make(spark, [("1","1","sci-fi",1640995200,datetime(2024,1,1))])).collect()[0]
        assert "-" in r["tag"]

    def test_preserves_alphanumeric(self, spark):
        r = transform_tags(_make(spark, [("1","1","action2024",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Action2024"

    def test_multiple_specials_removed(self, spark):
        r = transform_tags(_make(spark, [("1","1","h@ck!ng$",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Hckng"


# ═══════════════════════════════════════════════════════════════
# Title Casing
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestTitleCasing:

    def test_lowercase_to_title(self, spark):
        r = transform_tags(_make(spark, [("1","1","dark comedy",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"

    def test_uppercase_to_title(self, spark):
        r = transform_tags(_make(spark, [("1","1","DARK COMEDY",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"

    def test_mixed_case_to_title(self, spark):
        r = transform_tags(_make(spark, [("1","1","dArK CoMeDy",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"


# ═══════════════════════════════════════════════════════════════
# Whitespace Normalization
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestWhitespaceNormalization:

    def test_tabs_replaced(self, spark):
        r = transform_tags(_make(spark, [("1","1","dark\tcomedy",1640995200,datetime(2024,1,1))])).collect()[0]
        assert "\t" not in r["tag"]

    def test_newlines_replaced(self, spark):
        r = transform_tags(_make(spark, [("1","1","dark\ncomedy",1640995200,datetime(2024,1,1))])).collect()[0]
        assert "\n" not in r["tag"]

    def test_multiple_spaces_collapsed(self, spark):
        r = transform_tags(_make(spark, [("1","1","dark   comedy",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"

    def test_leading_trailing_trimmed(self, spark):
        r = transform_tags(_make(spark, [("1","1","  dark comedy  ",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"


# ═══════════════════════════════════════════════════════════════
# Timestamp + Date Key
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestTagTimestamp:

    def test_epoch_to_timestamp(self, spark):
        r = transform_tags(_make(spark, [("1","1","tag",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["tag_timestamp"].year == 2022

    def test_date_key_format(self, spark):
        r = transform_tags(_make(spark, [("1","1","tag",1640995200,datetime(2024,1,1))])).collect()[0]
        assert r["date_key"] == 20220101

    def test_null_timestamp(self, spark):
        r = transform_tags(_make(spark, [("1","1","tag",None,datetime(2024,1,1))])).collect()[0]
        assert r["tag_timestamp"] is None
        assert r["date_key"] is None


# ═══════════════════════════════════════════════════════════════
# Output Schema
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestTagsOutputSchema:

    def test_output_columns(self, spark):
        result = transform_tags(_make(spark, [("1","1","tag",1640995200,datetime(2024,1,1))]))
        expected = {"user_id","movie_id","tag","tag_timestamp","date_key","_ingestion_timestamp"}
        assert set(result.columns) == expected

    def test_raw_columns_dropped(self, spark):
        result = transform_tags(_make(spark, [("1","1","tag",1640995200,datetime(2024,1,1))]))
        leaked = {"userId","movieId","timestamp","tag_raw"} & set(result.columns)
        assert leaked == set()


# ═══════════════════════════════════════════════════════════════
# DQ Rules (Contract)
# ═══════════════════════════════════════════════════════════════
@pytest.mark.contract
class TestTagsDQRules:

    def test_valid_passes(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","funny movie",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_null_user_id(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [(None,"1","tag",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_USER_ID" in r["_dq_failed_rules"]

    def test_null_movie_id(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [("1",None,"tag",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_MOVIE_ID" in r["_dq_failed_rules"]

    def test_null_tag(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1",None,1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TAG" in r["_dq_failed_rules"]

    def test_null_timestamp(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","tag",None,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TIMESTAMP" in r["_dq_failed_rules"]

    def test_epoch_zero_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","funny tag",0,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "INVALID_TIMESTAMP_FLOOR" in r["_dq_failed_rules"]

    def test_short_tag_quarantines(self, spark):
        """Tag shorter than 3 chars → SHORT_TAG fires."""
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","ab",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "SHORT_TAG" in r["_dq_failed_rules"]
