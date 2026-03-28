"""
test_silver_links.py — Unit tests for silver.links transformation logic.

Tests: IMDB 'tt' prefix, TMDB ID handling, has_external_ids flag,
       NULL preservation, column casting, and DQ rules.
"""
import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, BooleanType,
)


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


def get_dq_rules():
    return [
        ("NULL_MOVIE_ID",    F.col("movie_id").isNull()),
        ("NO_EXTERNAL_IDS",  ~F.col("has_external_ids")),
    ]


BRONZE_SCHEMA = StructType([
    StructField("movieId", StringType(), True),
    StructField("imdbId",  StringType(), True),
    StructField("tmdbId",  StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
])


def _make(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


# ═══════════════════════════════════════════════════════════════
# IMDB ID Handling
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestImdbIdFormatting:

    def test_tt_prefix_added(self, spark):
        """imdbId '0114709' → 'tt0114709'"""
        r = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))])).collect()[0]
        assert r["imdb_id"] == "tt0114709"

    def test_null_imdb_stays_null(self, spark):
        r = transform_links(_make(spark, [("1",None,"862",datetime(2024,1,1))])).collect()[0]
        assert r["imdb_id"] is None

    def test_imdb_id_type_string(self, spark):
        result = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))]))
        t = [f for f in result.schema.fields if f.name == "imdb_id"][0].dataType
        assert isinstance(t, StringType)

    def test_short_imdb_id(self, spark):
        """Short numeric ID still gets 'tt' prefix."""
        r = transform_links(_make(spark, [("1","123",None,datetime(2024,1,1))])).collect()[0]
        assert r["imdb_id"] == "tt123"


# ═══════════════════════════════════════════════════════════════
# TMDB ID Handling
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestTmdbIdHandling:

    def test_tmdb_preserved_as_string(self, spark):
        r = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))])).collect()[0]
        assert r["tmdb_id"] == "862"

    def test_null_tmdb_stays_null(self, spark):
        r = transform_links(_make(spark, [("1","0114709",None,datetime(2024,1,1))])).collect()[0]
        assert r["tmdb_id"] is None


# ═══════════════════════════════════════════════════════════════
# has_external_ids Flag
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestHasExternalIds:

    def test_both_ids_present(self, spark):
        r = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))])).collect()[0]
        assert r["has_external_ids"] is True

    def test_only_imdb(self, spark):
        r = transform_links(_make(spark, [("1","0114709",None,datetime(2024,1,1))])).collect()[0]
        assert r["has_external_ids"] is True

    def test_only_tmdb(self, spark):
        r = transform_links(_make(spark, [("1",None,"862",datetime(2024,1,1))])).collect()[0]
        assert r["has_external_ids"] is True

    def test_no_ids(self, spark):
        r = transform_links(_make(spark, [("1",None,None,datetime(2024,1,1))])).collect()[0]
        assert r["has_external_ids"] is False


# ═══════════════════════════════════════════════════════════════
# Output Schema
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestLinksOutputSchema:

    def test_output_columns(self, spark):
        result = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))]))
        expected = {"movie_id","imdb_id","tmdb_id","has_external_ids","_ingestion_timestamp"}
        assert set(result.columns) == expected

    def test_raw_columns_dropped(self, spark):
        result = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))]))
        leaked = {"movieId","imdbId","tmdbId"} & set(result.columns)
        assert leaked == set()

    def test_has_external_ids_boolean_type(self, spark):
        result = transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))]))
        t = [f for f in result.schema.fields if f.name == "has_external_ids"][0].dataType
        assert isinstance(t, BooleanType)


# ═══════════════════════════════════════════════════════════════
# DQ Rules (Contract)
# ═══════════════════════════════════════════════════════════════
@pytest.mark.contract
class TestLinksDQRules:

    def test_valid_passes(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_links(_make(spark, [("1","0114709","862",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_null_movie_id(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_links(_make(spark, [(None,"0114709","862",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_MOVIE_ID" in r["_dq_failed_rules"]

    def test_no_external_ids_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_links(_make(spark, [("1",None,None,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NO_EXTERNAL_IDS" in r["_dq_failed_rules"]

    def test_imdb_only_passes(self, spark):
        """Having just IMDB ID is enough — should PASS."""
        from tests.conftest import apply_dq_flags
        r = apply_dq_flags(transform_links(_make(spark, [("1","0114709",None,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"
