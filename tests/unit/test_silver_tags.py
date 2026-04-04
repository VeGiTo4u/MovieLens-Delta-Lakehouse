"""
test_silver_tags.py — Unit tests for silver.tags transformation logic.

Tests: special char removal, title-casing, timestamp conversion,
       date_key derivation, whitespace normalization, and all DQ rules.
"""
import pytest
from datetime import datetime

from tests.conftest import apply_dq_flags
from tests.helpers.silver_mirrors import (
    make_bronze_tags_df as _make,
    tags_dq_rules as get_dq_rules,
    transform_tags,
)


# ═══════════════════════════════════════════════════════════════
# Special Character Removal
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestSpecialCharRemoval:
    """Tests verify the removal of non-alphanumeric special characters from raw tags."""

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
    """Tests verify the capitalization of tag strings to Title Case."""

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
    """Tests verify the collapsing of multiple spaces, tabs, and newlines."""

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
    """Tests verify the conversion of Unix epochs to Timestamps and the derivation of date_key."""

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
    """Tests verify that the output DataFrame strictly matches the expected Silver schema."""

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
    """Tests validate the application of Data Quality (DQ) rules for tags."""

    def test_valid_passes(self, spark):
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","funny movie",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_null_user_id(self, spark):
        r = apply_dq_flags(transform_tags(_make(spark, [(None,"1","tag",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_USER_ID" in r["_dq_failed_rules"]

    def test_null_movie_id(self, spark):
        r = apply_dq_flags(transform_tags(_make(spark, [("1",None,"tag",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_MOVIE_ID" in r["_dq_failed_rules"]

    def test_null_tag(self, spark):
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1",None,1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TAG" in r["_dq_failed_rules"]

    def test_null_timestamp(self, spark):
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","tag",None,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TIMESTAMP" in r["_dq_failed_rules"]

    def test_epoch_zero_quarantines(self, spark):
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","funny tag",0,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "INVALID_TIMESTAMP_FLOOR" in r["_dq_failed_rules"]

    def test_short_tag_quarantines(self, spark):
        """Tag shorter than 3 chars → SHORT_TAG fires."""
        r = apply_dq_flags(transform_tags(_make(spark, [("1","1","ab",1640995200,datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "SHORT_TAG" in r["_dq_failed_rules"]
