"""
test_silver_genome_scores.py — Unit tests for silver.genome_scores transformation.

Tests: relevance rounding to 3dp, column casting, NULL handling, and all DQ rules.
"""
import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType,
)


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


def get_dq_rules():
    return [
        ("NULL_MOVIE_ID",  F.col("movie_id").isNull()),
        ("NULL_TAG_ID",    F.col("tag_id").isNull()),
        ("NULL_RELEVANCE", F.col("relevance").isNull()),
        ("INVALID_RELEVANCE_RANGE", ~F.col("relevance").between(0.0, 1.0)),
    ]


BRONZE_SCHEMA = StructType([
    StructField("movieId",   StringType(), True),
    StructField("tagId",     StringType(), True),
    StructField("relevance", StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def _make(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


# ═══════════════════════════════════════════════════════════════
# Relevance Rounding
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestRelevanceRounding:

    def test_rounds_to_3dp(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1","0.12345",datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] == 0.123

    def test_exact_3dp_unchanged(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1","0.500",datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] == 0.5

    def test_rounds_up(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1","0.9999",datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] == 1.0

    def test_zero_relevance(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1","0.0",datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] == 0.0

    def test_full_relevance(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1","1.0",datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] == 1.0

    def test_many_decimal_places(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1","0.123456789",datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] == 0.123


# ═══════════════════════════════════════════════════════════════
# Column Casting
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestGenomeScoresCasting:

    def test_movie_id_int(self, spark):
        r = transform_genome_scores(_make(spark, [("42","1","0.5",datetime(2024,1,1))])).collect()[0]
        assert r["movie_id"] == 42

    def test_tag_id_int(self, spark):
        r = transform_genome_scores(_make(spark, [("1","99","0.5",datetime(2024,1,1))])).collect()[0]
        assert r["tag_id"] == 99

    def test_relevance_double(self, spark):
        result = transform_genome_scores(_make(spark, [("1","1","0.5",datetime(2024,1,1))]))
        t = [f for f in result.schema.fields if f.name == "relevance"][0].dataType
        assert isinstance(t, DoubleType)

    def test_null_movie_id(self, spark):
        r = transform_genome_scores(_make(spark, [(None,"1","0.5",datetime(2024,1,1))])).collect()[0]
        assert r["movie_id"] is None

    def test_null_relevance(self, spark):
        r = transform_genome_scores(_make(spark, [("1","1",None,datetime(2024,1,1))])).collect()[0]
        assert r["relevance"] is None


# ═══════════════════════════════════════════════════════════════
# Output Schema
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestGenomeScoresSchema:

    def test_output_columns(self, spark):
        result = transform_genome_scores(_make(spark, [("1","1","0.5",datetime(2024,1,1))]))
        assert set(result.columns) == {"movie_id","tag_id","relevance","_ingestion_timestamp"}

    def test_raw_columns_dropped(self, spark):
        result = transform_genome_scores(_make(spark, [("1","1","0.5",datetime(2024,1,1))]))
        leaked = {"movieId","tagId"} & set(result.columns)
        assert leaked == set()


# ═══════════════════════════════════════════════════════════════
# DQ Rules (Contract)
# ═══════════════════════════════════════════════════════════════
@pytest.mark.contract
class TestGenomeScoresDQRules:

    def test_valid_passes(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1","1","0.5",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_null_movie_id(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [(None,"1","0.5",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_MOVIE_ID" in r["_dq_failed_rules"]

    def test_null_tag_id(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1",None,"0.5",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TAG_ID" in r["_dq_failed_rules"]

    def test_null_relevance(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1","1",None,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_RELEVANCE" in r["_dq_failed_rules"]

    def test_relevance_above_1_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1","1","1.5",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "INVALID_RELEVANCE_RANGE" in r["_dq_failed_rules"]

    def test_negative_relevance_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1","1","-0.1",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "INVALID_RELEVANCE_RANGE" in r["_dq_failed_rules"]

    def test_boundary_0_passes(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1","1","0.0",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_boundary_1_passes(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_genome_scores(_make(spark, [("1","1","1.0",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"
