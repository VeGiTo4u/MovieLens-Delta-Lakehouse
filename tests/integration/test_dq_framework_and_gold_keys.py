"""
test_dq_framework_and_gold_keys.py — cross-layer DQ wiring + Gold key behavior.

Scope owned by this suite:
  1. DQ framework wiring via apply_dq_flags using shared Silver mirror helpers
  2. Surrogate key determinism for Gold models
  3. Gold pass-through filtering behavior for PASS vs QUARANTINE records
"""

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType

from tests.conftest import apply_dq_flags
from tests.helpers.silver_mirrors import (
    make_bronze_movies_df,
    make_bronze_ratings_df,
    movies_dq_rules,
    ratings_dq_rules,
    transform_movies,
    transform_ratings,
)


_TS = datetime(2024, 1, 1)
_GOOD_EPOCH = 1640995200  # 2022-01-01 00:00:00 UTC
_EPOCH_ZERO = 0


# ────────────────────────────────────────────────────────────────
# generate_surrogate_key — mirror from gold_utils.py
# ────────────────────────────────────────────────────────────────
def generate_surrogate_key(df, sk_col_name, *natural_key_cols):
    """
    Mirror of scripts/gold/gold_utils.py generate_surrogate_key().
    """
    if not natural_key_cols:
        raise ValueError("At least one natural_key_col must be provided.")

    if len(natural_key_cols) == 1:
        hash_input = F.col(natural_key_cols[0]).cast("string")
    else:
        hash_input = F.concat_ws("|", *[F.col(c).cast("string") for c in natural_key_cols])

    return df.withColumn(sk_col_name, F.sha2(hash_input, 256))


@pytest.mark.contract
class TestDQFrameworkWiring:
    """Representative DQ framework checks across Silver ratings and movies."""

    def test_ratings_pass_smoke(self, spark):
        df = make_bronze_ratings_df(
            spark,
            [("1", "100", "3.5", _GOOD_EPOCH, _TS, 2022)],
        )

        row = apply_dq_flags(transform_ratings(df), ratings_dq_rules()).collect()[0]

        assert row["_dq_status"] == "PASS"
        assert row["_dq_failed_rules"] == []

    def test_ratings_quarantine_smoke(self, spark):
        df = make_bronze_ratings_df(
            spark,
            [(None, "100", "6.0", _EPOCH_ZERO, _TS, 2022)],
        )

        row = apply_dq_flags(transform_ratings(df), ratings_dq_rules()).collect()[0]

        assert row["_dq_status"] == "QUARANTINE"
        failed = set(row["_dq_failed_rules"])
        assert "NULL_USER_ID" in failed
        assert "INVALID_RATING_RANGE" in failed
        assert "INVALID_TIMESTAMP_FLOOR" in failed

    def test_movies_pass_smoke(self, spark):
        df = make_bronze_movies_df(
            spark,
            [("1", "Toy Story (1995)", "Animation|Comedy", _TS)],
        )

        row = apply_dq_flags(transform_movies(df), movies_dq_rules()).collect()[0]

        assert row["_dq_status"] == "PASS"
        assert row["_dq_failed_rules"] == []

    def test_movies_quarantine_smoke(self, spark):
        df = make_bronze_movies_df(
            spark,
            [(None, None, "Drama", _TS)],
        )

        row = apply_dq_flags(transform_movies(df), movies_dq_rules()).collect()[0]

        assert row["_dq_status"] == "QUARANTINE"
        failed = set(row["_dq_failed_rules"])
        assert "NULL_MOVIE_ID" in failed
        assert "NULL_TITLE" in failed


@pytest.mark.unit
class TestSurrogateKeyDeterminism:
    """Determinism and collision-safety checks for Gold surrogate keys."""

    def test_single_key_deterministic(self, spark):
        df = spark.createDataFrame([(1,), (2,), (1,)], ["movie_id"])
        rows = generate_surrogate_key(df, "movie_sk", "movie_id").orderBy("movie_id").collect()

        sk_for_1 = [r["movie_sk"] for r in rows if r["movie_id"] == 1]
        assert len(sk_for_1) == 2
        assert sk_for_1[0] == sk_for_1[1]

    def test_different_keys_produce_different_sks(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["movie_id"])
        rows = generate_surrogate_key(df, "movie_sk", "movie_id").collect()

        assert rows[0]["movie_sk"] != rows[1]["movie_sk"]

    def test_sk_is_64_char_hex(self, spark):
        df = spark.createDataFrame([(1,)], ["movie_id"])
        sk = generate_surrogate_key(df, "movie_sk", "movie_id").collect()[0]["movie_sk"]

        assert len(sk) == 64
        assert all(char in "0123456789abcdef" for char in sk)

    def test_deterministic_across_separate_runs(self, spark):
        df_run_1 = spark.createDataFrame([(42,)], ["movie_id"])
        df_run_2 = spark.createDataFrame([(42,)], ["movie_id"])

        sk_run_1 = generate_surrogate_key(df_run_1, "movie_sk", "movie_id").collect()[0]["movie_sk"]
        sk_run_2 = generate_surrogate_key(df_run_2, "movie_sk", "movie_id").collect()[0]["movie_sk"]

        assert sk_run_1 == sk_run_2

    def test_composite_key_pipe_separator(self, spark):
        df_1 = spark.createDataFrame([(1, 100)], ["user_id", "movie_id"])
        df_2 = spark.createDataFrame([(11, 0)], ["user_id", "movie_id"])

        sk_1 = generate_surrogate_key(df_1, "rating_sk", "user_id", "movie_id").collect()[0]["rating_sk"]
        sk_2 = generate_surrogate_key(df_2, "rating_sk", "user_id", "movie_id").collect()[0]["rating_sk"]

        assert sk_1 != sk_2

    def test_composite_key_three_columns(self, spark):
        ts = datetime(2022, 1, 1, 0, 0, 0)
        df = spark.createDataFrame(
            [(1, "abc123", ts), (1, "abc123", ts)],
            ["user_id", "movie_sk", "interaction_timestamp"],
        )

        rows = generate_surrogate_key(
            df,
            "fact_sk",
            "user_id",
            "movie_sk",
            "interaction_timestamp",
        ).collect()

        assert rows[0]["fact_sk"] == rows[1]["fact_sk"]

    def test_composite_key_different_timestamps_different_sks(self, spark):
        ts_1 = datetime(2022, 1, 1)
        ts_2 = datetime(2023, 6, 15)
        df = spark.createDataFrame(
            [(1, "abc", ts_1), (1, "abc", ts_2)],
            ["user_id", "movie_sk", "interaction_timestamp"],
        )

        rows = generate_surrogate_key(
            df,
            "fact_sk",
            "user_id",
            "movie_sk",
            "interaction_timestamp",
        ).collect()

        assert rows[0]["fact_sk"] != rows[1]["fact_sk"]

    def test_no_natural_key_raises_error(self, spark):
        df = spark.createDataFrame([(1,)], ["movie_id"])
        with pytest.raises(ValueError, match="At least one natural_key_col"):
            generate_surrogate_key(df, "sk")

    def test_null_input_produces_null_sk(self, spark):
        schema = StructType([StructField("movie_id", IntegerType(), True)])
        df = spark.createDataFrame([(None,)], schema)

        row = generate_surrogate_key(df, "movie_sk", "movie_id").collect()[0]
        assert row["movie_sk"] is None


@pytest.mark.contract
class TestGoldPassThroughLogic:
    """Cross-layer pass-through behavior used by Gold read patterns."""

    def test_quarantine_records_excluded_from_gold_ratings(self, spark):
        rows = [
            ("1", "100", "3.5", _GOOD_EPOCH, _TS, 2022),
            ("2", "200", "4.0", _GOOD_EPOCH, _TS, 2022),
            (None, "100", "3.5", _GOOD_EPOCH, _TS, 2022),
            ("3", "300", "7.0", _GOOD_EPOCH, _TS, 2022),
        ]
        df = make_bronze_ratings_df(spark, rows)
        flagged = apply_dq_flags(transform_ratings(df), ratings_dq_rules())

        passed = flagged.filter(F.col("_dq_status") == "PASS")

        assert passed.count() == 2
        assert passed.filter(F.col("_dq_status") == "QUARANTINE").count() == 0

    def test_counts_match_single_pass_aggregation(self, spark):
        rows = [
            ("1", "100", "3.5", _GOOD_EPOCH, _TS, 2022),
            ("2", "200", "4.0", _GOOD_EPOCH, _TS, 2022),
            (None, "100", "3.5", _GOOD_EPOCH, _TS, 2022),
            ("3", "300", "7.0", _GOOD_EPOCH, _TS, 2022),
        ]
        df = make_bronze_ratings_df(spark, rows)
        flagged = apply_dq_flags(transform_ratings(df), ratings_dq_rules())

        agg = flagged.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("_dq_status") == "PASS", 1).otherwise(0)).alias("pass_count"),
        ).collect()[0]

        assert agg["total"] == 4
        assert agg["pass_count"] == 2
        assert agg["total"] - agg["pass_count"] == 2

    def test_all_quarantine_zero_pass_scenario(self, spark):
        rows = [
            (None, "100", "3.5", _GOOD_EPOCH, _TS, 2022),
            ("1", None, "3.5", _GOOD_EPOCH, _TS, 2022),
            ("2", "200", None, _GOOD_EPOCH, _TS, 2022),
        ]
        df = make_bronze_ratings_df(spark, rows)
        flagged = apply_dq_flags(transform_ratings(df), ratings_dq_rules())

        passed = flagged.filter(F.col("_dq_status") == "PASS")
        assert passed.count() == 0

    def test_quarantine_records_excluded_from_gold_movies(self, spark):
        rows = [
            ("1", "Toy Story (1995)", "Animation|Comedy", _TS),
            ("2", None, "Drama", _TS),
            (None, "Unknown (2020)", "Action", _TS),
        ]
        df = make_bronze_movies_df(spark, rows)
        flagged = apply_dq_flags(transform_movies(df), movies_dq_rules())

        passed = flagged.filter(F.col("_dq_status") == "PASS")

        assert passed.count() == 1
        assert passed.collect()[0]["movie_id"] == 1

    def test_scd2_current_filter_with_dq_pass(self, spark):
        rows = [
            ("1", "100", "3.5", _GOOD_EPOCH, _TS, 2022),
            (None, "100", "3.5", _GOOD_EPOCH, _TS, 2022),
        ]
        df = make_bronze_ratings_df(spark, rows)
        flagged = apply_dq_flags(transform_ratings(df), ratings_dq_rules())

        gold_df = flagged.filter(F.col("_dq_status") == "PASS").filter(F.col("is_current") == True)

        assert gold_df.count() == 1
        row = gold_df.collect()[0]
        assert row["user_id"] == 1
        assert row["is_current"] is True
        assert row["_dq_status"] == "PASS"
