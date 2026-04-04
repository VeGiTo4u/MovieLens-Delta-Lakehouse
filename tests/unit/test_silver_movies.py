"""
test_silver_movies.py — Unit tests for silver.movies transformation logic.

Tests: release_year extraction, article reordering, genre normalization,
       column casting, title cleaning, and all 3 DQ rules.
"""
import pytest
from datetime import datetime
from pyspark.sql.types import ArrayType

from tests.conftest import apply_dq_flags
from tests.helpers.silver_mirrors import (
    make_bronze_movies_df as _make,
    movies_dq_rules as get_dq_rules,
    transform_movies,
)


# ═══════════════════════════════════════════════════════════════
# Release Year Extraction
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestReleaseYearExtraction:
    """Tests verify the robust extraction of the release year from the movie title using regex."""

    def test_standard_year(self, spark):
        r = transform_movies(_make(spark, [("1","Toy Story (1995)","Animation",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] == 1995

    def test_no_year_returns_null(self, spark):
        r = transform_movies(_make(spark, [("1","Toy Story","Animation",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] is None

    def test_year_not_at_end_ignored(self, spark):
        r = transform_movies(_make(spark, [("1","(2001) A Space Odyssey","Sci-Fi",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] is None

    def test_out_of_range_year_null(self, spark):
        r = transform_movies(_make(spark, [("1","Future (9999)","Sci-Fi",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] is None

    def test_boundary_1800_valid(self, spark):
        r = transform_movies(_make(spark, [("1","Old (1800)","Drama",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] == 1800

    def test_boundary_2199_valid(self, spark):
        r = transform_movies(_make(spark, [("1","Future (2199)","Sci-Fi",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] == 2199

    def test_trailing_whitespace(self, spark):
        r = transform_movies(_make(spark, [("1","Toy Story (1995)  ","Animation",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] == 1995

    def test_parenthetical_non_year(self, spark):
        r = transform_movies(_make(spark, [("1","Se7en (Seven) (1995)","Crime",datetime(2024,1,1))])).collect()[0]
        assert r["release_year"] == 1995


# ═══════════════════════════════════════════════════════════════
# Article Reordering
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestArticleReordering:
    """Tests verify that definite articles at the end of titles are properly reordered to the front."""

    def test_the(self, spark):
        r = transform_movies(_make(spark, [("1","Dark Knight, The (2008)","Action",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "The Dark Knight"

    def test_a(self, spark):
        r = transform_movies(_make(spark, [("1","Beautiful Mind, A (2001)","Drama",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "A Beautiful Mind"

    def test_an(self, spark):
        r = transform_movies(_make(spark, [("1","American Werewolf In London, An (1981)","Horror",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "An American Werewolf In London"

    def test_la(self, spark):
        r = transform_movies(_make(spark, [("1","Dolce Vita, La (1960)","Drama",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "La Dolce Vita"

    def test_das(self, spark):
        r = transform_movies(_make(spark, [("1","Boot, Das (1981)","War",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "Das Boot"

    def test_el(self, spark):
        r = transform_movies(_make(spark, [("1","Laberinto, El (2006)","Fantasy",datetime(2024,1,1))])).collect()[0]
        assert r["title"].startswith("El ")

    def test_no_article_unchanged(self, spark):
        r = transform_movies(_make(spark, [("1","The Matrix (1999)","Action",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "The Matrix"


# ═══════════════════════════════════════════════════════════════
# Genre Normalization
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestGenreNormalization:
    """Tests verify the parsing and splitting of pipe-delimited genre strings into formatted Arrays."""

    def test_pipe_to_array(self, spark):
        r = transform_movies(_make(spark, [("1","M (1995)","Animation|Comedy|Children",datetime(2024,1,1))])).collect()[0]
        assert r["genres"] == ["Animation", "Comedy", "Children"]

    def test_single_genre(self, spark):
        r = transform_movies(_make(spark, [("1","M (2020)","Drama",datetime(2024,1,1))])).collect()[0]
        assert r["genres"] == ["Drama"]

    def test_no_genres_listed(self, spark):
        r = transform_movies(_make(spark, [("1","M (2020)","(no genres listed)",datetime(2024,1,1))])).collect()[0]
        assert r["genres"] == []

    def test_null_genres(self, spark):
        r = transform_movies(_make(spark, [("1","M (2020)",None,datetime(2024,1,1))])).collect()[0]
        assert r["genres"] == []

    def test_empty_genres(self, spark):
        r = transform_movies(_make(spark, [("1","M (2020)","",datetime(2024,1,1))])).collect()[0]
        assert r["genres"] == []

    def test_title_case(self, spark):
        """initcap treats hyphens as word separators: sci-fi → Sci-fi"""
        r = transform_movies(_make(spark, [("1","M (2020)","sci-fi|action",datetime(2024,1,1))])).collect()[0]
        assert r["genres"] == ["Sci-fi", "Action"]

    def test_array_type(self, spark):
        result = transform_movies(_make(spark, [("1","M (2020)","Drama",datetime(2024,1,1))]))
        t = [f for f in result.schema.fields if f.name == "genres"][0].dataType
        assert isinstance(t, ArrayType)


# ═══════════════════════════════════════════════════════════════
# Title Cleaning
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestTitleCleaning:
    """Tests verify the removal of the release year from the title string."""

    def test_year_removed(self, spark):
        r = transform_movies(_make(spark, [("1","Toy Story (1995)","Animation",datetime(2024,1,1))])).collect()[0]
        assert "(1995)" not in r["title"]
        assert r["title"] == "Toy Story"

    def test_title_case(self, spark):
        r = transform_movies(_make(spark, [("1","toy story (1995)","Animation",datetime(2024,1,1))])).collect()[0]
        assert r["title"] == "Toy Story"


# ═══════════════════════════════════════════════════════════════
# Output Schema
# ═══════════════════════════════════════════════════════════════
@pytest.mark.unit
class TestMoviesOutputSchema:
    """Tests verify that the output DataFrame strictly matches the expected Silver schema."""

    def test_output_columns(self, spark):
        result = transform_movies(_make(spark, [("1","M (1995)","Drama",datetime(2024,1,1))]))
        assert set(result.columns) == {"movie_id","title","release_year","genres","_ingestion_timestamp"}

    def test_raw_columns_dropped(self, spark):
        result = transform_movies(_make(spark, [("1","M (1995)","Drama",datetime(2024,1,1))]))
        leaked = {"movieId","genres_raw","title_raw","year_str","title_no_year"} & set(result.columns)
        assert leaked == set()


# ═══════════════════════════════════════════════════════════════
# DQ Rules (Contract)
# ═══════════════════════════════════════════════════════════════
@pytest.mark.contract
class TestMoviesDQRules:
    """Tests validate the application of Data Quality (DQ) rules for movies."""

    def test_valid_passes(self, spark):
        r = apply_dq_flags(transform_movies(_make(spark, [("1","Toy Story (1995)","Animation",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"

    def test_null_movie_id(self, spark):
        r = apply_dq_flags(transform_movies(_make(spark, [(None,"M (1995)","Drama",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_MOVIE_ID" in r["_dq_failed_rules"]

    def test_null_title(self, spark):
        r = apply_dq_flags(transform_movies(_make(spark, [("1",None,"Drama",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TITLE" in r["_dq_failed_rules"]

    def test_empty_title(self, spark):
        r = apply_dq_flags(transform_movies(_make(spark, [("1","   ","Drama",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert "NULL_TITLE" in r["_dq_failed_rules"]

    def test_null_year_passes(self, spark):
        r = apply_dq_flags(transform_movies(_make(spark, [("1","Toy Story","Drama",datetime(2024,1,1))])), get_dq_rules()).collect()[0]
        assert r["_dq_status"] == "PASS"
