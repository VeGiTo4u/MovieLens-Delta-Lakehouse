"""
test_silver_genome_tags.py — Unit tests for silver.genome_tags transformation.

Tests: tag formatting (whitespace collapse, Title Case), column casting,
       NULL handling, and all DQ rules.
"""
import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType,
)


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


def get_dq_rules():
    return [
        ("NULL_TAG_ID", F.col("tag_id").isNull()),
        ("NULL_TAG",    F.col("tag").isNull()),
        ("EMPTY_TAG",   F.trim(F.col("tag")) == ""),
    ]


BRONZE_SCHEMA = StructType([
    StructField("tagId", StringType(), True),
    StructField("tag",   StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def _make(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


# ═══════════════════════════════════════════════════════════════
# Tag Formatting
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestGenomeTagFormatting:

    def test_title_case(self, spark):
        r = transform_genome_tags(_make(spark, [("1","action",datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Action"

    def test_multi_word_title_case(self, spark):
        r = transform_genome_tags(_make(spark, [("1","dark comedy",datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"

    def test_uppercase_to_title(self, spark):
        r = transform_genome_tags(_make(spark, [("1","DARK COMEDY",datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"

    def test_whitespace_collapsed(self, spark):
        r = transform_genome_tags(_make(spark, [("1","dark   comedy",datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"

    def test_tabs_replaced(self, spark):
        r = transform_genome_tags(_make(spark, [("1","dark\tcomedy",datetime(2024,1,1))])).collect()[0]
        assert "\t" not in r["tag"]
        assert r["tag"] == "Dark Comedy"

    def test_newlines_replaced(self, spark):
        r = transform_genome_tags(_make(spark, [("1","dark\ncomedy",datetime(2024,1,1))])).collect()[0]
        assert "\n" not in r["tag"]

    def test_leading_trailing_trimmed(self, spark):
        r = transform_genome_tags(_make(spark, [("1","  action  ",datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Action"

    def test_carriage_return_replaced(self, spark):
        r = transform_genome_tags(_make(spark, [("1","dark\rcomedy",datetime(2024,1,1))])).collect()[0]
        assert "\r" not in r["tag"]

    def test_mixed_whitespace(self, spark):
        """Tabs + newlines + multiple spaces all normalised to single space."""
        r = transform_genome_tags(_make(spark, [("1","  dark\t\n  comedy  ",datetime(2024,1,1))])).collect()[0]
        assert r["tag"] == "Dark Comedy"


# ═══════════════════════════════════════════════════════════════
# Column Casting
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestGenomeTagsCasting:

    def test_tag_id_int(self, spark):
        r = transform_genome_tags(_make(spark, [("42","action",datetime(2024,1,1))])).collect()[0]
        assert r["tag_id"] == 42

    def test_null_tag_id(self, spark):
        r = transform_genome_tags(_make(spark, [(None,"action",datetime(2024,1,1))])).collect()[0]
        assert r["tag_id"] is None

    def test_null_tag(self, spark):
        r = transform_genome_tags(_make(spark, [("1",None,datetime(2024,1,1))])).collect()[0]
        assert r["tag"] is None


# ═══════════════════════════════════════════════════════════════
# Output Schema
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestGenomeTagsSchema:

    def test_output_columns(self, spark):
        result = transform_genome_tags(_make(spark, [("1","action",datetime(2024,1,1))]))
        assert set(result.columns) == {"tag_id", "tag", "_ingestion_timestamp"}

    def test_raw_columns_dropped(self, spark):
        result = transform_genome_tags(_make(spark, [("1","action",datetime(2024,1,1))]))
        leaked = {"tagId", "tag_raw"} & set(result.columns)
        assert leaked == set()


# ═══════════════════════════════════════════════════════════════
# DQ Rules (Contract)
# ═══════════════════════════════════════════════════════════════
@pytest.mark.contract
class TestGenomeTagsDQRules:

    def test_valid_passes(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_tags(_make(spark, [("1","action",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_null_tag_id(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_tags(_make(spark, [(None,"action",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TAG_ID" in r["_dq_failed_rules"]

    def test_null_tag(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_tags(_make(spark, [("1",None,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TAG" in r["_dq_failed_rules"]

    def test_empty_tag(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_tags(_make(spark, [("1","",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "EMPTY_TAG" in r["_dq_failed_rules"]

    def test_whitespace_only_tag(self, spark):
        """Tag of only whitespace → after trim becomes empty → EMPTY_TAG fires."""
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_tags(_make(spark, [("1","   ",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "EMPTY_TAG" in r["_dq_failed_rules"]
