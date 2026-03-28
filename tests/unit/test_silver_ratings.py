"""
test_silver_ratings.py — Unit tests for silver.ratings transformation logic.

Tests validate:
  1. Unix epoch → TIMESTAMP conversion
  2. date_key derivation (YYYYMMDD INT)
  3. is_late_arrival flagging (event year vs batch year)
  4. Rating rounding to 1 decimal place
  5. Column rename + type casting (userId → user_id INT, etc.)
  6. rating_year derivation (event year from timestamp)
  7. SCD2 column initialization (is_current, effective_start_date, effective_end_date)
  8. DQ rules — all 7 rules from DQ_AND_LINEAGE.md

All tests use mock DataFrames — no S3, no Delta, no Unity Catalog.
"""
import pytest
from datetime import datetime
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    TimestampType,
)

from tests.helpers.silver_mirrors import (
    make_bronze_ratings_df as _make_bronze_df,
    ratings_dq_rules as get_dq_rules,
    transform_ratings,
)


# ════════════════════════════════════════════════════════════════
# UNIT TESTS — Transformation Logic
# ════════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestTimestampConversion:
    """Unix epoch → TIMESTAMP conversion."""

    def test_known_epoch_converts_correctly(self, spark):
        """1419472215 → 2014-12-25 01:30:15 UTC"""
        df = _make_bronze_df(spark, [
            ("1", "1", "4.0", 1419472215, datetime(2024, 1, 1), 2014),
        ])
        result = transform_ratings(df).collect()[0]
        ts = result["interaction_timestamp"]
        assert ts.year == 2014
        assert ts.month == 12
        assert ts.day == 25

    def test_epoch_zero_converts_to_1970(self, spark):
        """timestamp=0 → 1970-01-01 (will be quarantined by DQ)."""
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 0, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["interaction_timestamp"].year == 1970

    def test_null_timestamp_produces_null(self, spark):
        """NULL timestamp → NULL interaction_timestamp."""
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", None, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["interaction_timestamp"] is None

    def test_large_future_epoch(self, spark):
        """Very large timestamp (year 2040) still converts correctly."""
        # 2208988800 = 2040-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 2208988800, datetime(2024, 1, 1), 2040),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["interaction_timestamp"].year == 2040


@pytest.mark.unit
class TestDateKeyDerivation:
    """date_key = YYYYMMDD integer derived from interaction_timestamp."""

    def test_date_key_format(self, spark):
        """2014-12-25 → date_key = 20141225"""
        df = _make_bronze_df(spark, [
            ("1", "1", "4.0", 1419472215, datetime(2024, 1, 1), 2014),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["date_key"] == 20141225

    def test_date_key_null_on_null_timestamp(self, spark):
        """NULL timestamp → NULL date_key."""
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", None, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["date_key"] is None

    def test_date_key_january_first(self, spark):
        """2022-01-01 → date_key = 20220101"""
        # 1640995200 = 2022-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["date_key"] == 20220101

    def test_date_key_december_31(self, spark):
        """2021-12-31 → date_key = 20211231"""
        # 1640908800 = 2021-12-31 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 1640908800, datetime(2024, 1, 1), 2021),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["date_key"] == 20211231


@pytest.mark.unit
class TestLateArrivalFlagging:
    """is_late_arrival = (event_year != _batch_year)."""

    def test_normal_arrival_not_late(self, spark):
        """Event in 2022, batch 2022 → is_late_arrival = False"""
        # 1640995200 = 2022-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["is_late_arrival"] is False

    def test_late_arrival_flagged(self, spark):
        """Event in 2019, batch 2022 → is_late_arrival = True"""
        # 1546300800 = 2019-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 1546300800, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["is_late_arrival"] is True
        assert result["rating_year"] == 2019

    def test_future_arrival_flagged(self, spark):
        """Event in 2023, batch 2022 → is_late_arrival = True (early file delivery)."""
        # 1672531200 = 2023-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 1672531200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["is_late_arrival"] is True

    def test_late_arrival_does_not_quarantine(self, spark):
        """Late arrivals are valid — they must PASS DQ (not quarantined)."""
        from tests.conftest import apply_dq_flags

        # 2019 event in 2022 batch — valid rating data
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1546300800, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df)
        result = apply_dq_flags(result, get_dq_rules()).collect()[0]

        assert result["is_late_arrival"] is True
        assert result["_dq_status"] == "PASS"


@pytest.mark.unit
class TestRatingRounding:
    """Ratings are rounded to 1 decimal place."""

    def test_rating_rounds_to_1dp(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.55", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["rating"] == 3.6

    def test_exact_rating_unchanged(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "4.0", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["rating"] == 4.0

    def test_half_star_rating(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["rating"] == 3.5


@pytest.mark.unit
class TestColumnRenameAndCasting:
    """userId→user_id (INT), movieId→movie_id (INT), rating→DOUBLE."""

    def test_column_renames(self, spark):
        df = _make_bronze_df(spark, [
            ("42", "100", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df)
        row = result.collect()[0]
        assert row["user_id"] == 42
        assert row["movie_id"] == 100
        assert isinstance(row["rating"], float)

    def test_schema_types(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df)
        schema_dict = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema_dict["user_id"], IntegerType)
        assert isinstance(schema_dict["movie_id"], IntegerType)
        assert isinstance(schema_dict["rating"], DoubleType)
        assert isinstance(schema_dict["interaction_timestamp"], TimestampType)
        assert isinstance(schema_dict["date_key"], IntegerType)
        assert isinstance(schema_dict["is_late_arrival"], BooleanType)

    def test_null_userId_casts_to_null_int(self, spark):
        df = _make_bronze_df(spark, [
            (None, "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["user_id"] is None


@pytest.mark.unit
class TestRatingYearDerivation:
    """rating_year = YEAR(interaction_timestamp) — the partition column."""

    def test_rating_year_from_timestamp(self, spark):
        # 1546300800 = 2019-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", 1546300800, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["rating_year"] == 2019

    def test_rating_year_null_on_null_timestamp(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.0", None, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["rating_year"] is None


@pytest.mark.unit
class TestSCD2Initialization:
    """SCD2 columns are initialized correctly by transform."""

    def test_is_current_defaults_true(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["is_current"] is True

    def test_effective_start_date_equals_timestamp(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["effective_start_date"] == result["interaction_timestamp"]

    def test_effective_end_date_is_null(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df).collect()[0]
        assert result["effective_end_date"] is None


@pytest.mark.unit
class TestOutputColumnSelection:
    """Transform selects exactly the expected columns."""

    def test_output_columns(self, spark):
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df)
        expected_cols = {
            "user_id", "movie_id", "rating", "interaction_timestamp",
            "date_key", "is_late_arrival", "rating_year",
            "is_current", "effective_start_date", "effective_end_date",
            "_ingestion_timestamp", "_batch_year",
        }
        assert set(result.columns) == expected_cols

    def test_bronze_raw_columns_dropped(self, spark):
        """Raw Bronze columns (userId, movieId, timestamp) must not leak through."""
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = transform_ratings(df)
        leaked = {"userId", "movieId", "timestamp"} & set(result.columns)
        assert leaked == set(), f"Raw Bronze columns leaked: {leaked}"


# ════════════════════════════════════════════════════════════════
# CONTRACT TESTS — DQ Rules
# ════════════════════════════════════════════════════════════════
@pytest.mark.contract
class TestRatingsDQRules:
    """Validates all 7 DQ rules from DQ_AND_LINEAGE.md."""

    def test_valid_record_passes(self, spark):
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "PASS"
        assert result["_dq_failed_rules"] == []

    def test_null_user_id_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            (None, "1", "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "NULL_USER_ID" in result["_dq_failed_rules"]

    def test_null_movie_id_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", None, "3.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "NULL_MOVIE_ID" in result["_dq_failed_rules"]

    def test_null_rating_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", None, 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "NULL_RATING" in result["_dq_failed_rules"]

    def test_null_timestamp_quarantines(self, spark):
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", None, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "NULL_TIMESTAMP" in result["_dq_failed_rules"]

    def test_epoch_zero_quarantines_invalid_floor(self, spark):
        """timestamp=0 → 1970-01-01 → INVALID_TIMESTAMP_FLOOR fires."""
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 0, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "INVALID_TIMESTAMP_FLOOR" in result["_dq_failed_rules"]

    def test_rating_out_of_range_quarantines(self, spark):
        """Rating 5.5 > 5.0 → INVALID_RATING_RANGE fires."""
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "5.5", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "INVALID_RATING_RANGE" in result["_dq_failed_rules"]

    def test_negative_rating_quarantines(self, spark):
        """Rating -1.0 < 0.0 → INVALID_RATING_RANGE fires."""
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "-1.0", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        assert "INVALID_RATING_RANGE" in result["_dq_failed_rules"]

    def test_boundary_rating_0_passes(self, spark):
        """Rating 0.0 is within valid range [0.0, 5.0]."""
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "0.0", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "PASS"

    def test_boundary_rating_5_passes(self, spark):
        """Rating 5.0 is within valid range [0.0, 5.0]."""
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            ("1", "1", "5.0", 1640995200, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "PASS"

    def test_multiple_rules_fire_together(self, spark):
        """NULL user_id + NULL rating + epoch-zero → 3+ DQ failures."""
        from tests.conftest import apply_dq_flags
        df = _make_bronze_df(spark, [
            (None, "1", None, 0, datetime(2024, 1, 1), 2022),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "QUARANTINE"
        failed = set(result["_dq_failed_rules"])
        assert "NULL_USER_ID" in failed
        assert "NULL_RATING" in failed
        assert "INVALID_TIMESTAMP_FLOOR" in failed

    def test_1995_boundary_passes(self, spark):
        """Timestamp exactly 1995-01-01 → should PASS (not before 1995)."""
        from tests.conftest import apply_dq_flags
        # 788918400 = 1995-01-01 00:00:00 UTC
        df = _make_bronze_df(spark, [
            ("1", "1", "3.5", 788918400, datetime(2024, 1, 1), 1995),
        ])
        result = apply_dq_flags(transform_ratings(df), get_dq_rules()).collect()[0]
        assert result["_dq_status"] == "PASS"
