from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tests.conftest import apply_dq_flags
from tests.helpers.delta_support import patch_module_spark
from tests.helpers.silver_mirrors import ratings_dq_rules, transform_ratings


BRONZE_RATINGS_SCHEMA = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_batch_year", IntegerType(), True),
])

SILVER_RATINGS_FINAL_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("interaction_timestamp", TimestampType(), True),
    StructField("date_key", IntegerType(), True),
    StructField("is_late_arrival", BooleanType(), True),
    StructField("rating_year", IntegerType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("effective_start_date", TimestampType(), True),
    StructField("effective_end_date", TimestampType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_batch_year", IntegerType(), True),
    StructField("_dq_failed_rules", ArrayType(StringType(), True), True),
    StructField("_dq_status", StringType(), True),
    StructField("_processing_timestamp", TimestampType(), True),
    StructField("_bronze_ingestion_timestamp", TimestampType(), True),
    StructField("_job_run_id", StringType(), True),
    StructField("_notebook_path", StringType(), True),
    StructField("_source_system", StringType(), True),
])

@pytest.mark.integration
def test_delta_fixture_smoke(delta_spark, tmp_delta_path):
    df = delta_spark.createDataFrame([(1,)], ["id"])
    smoke_path = tmp_delta_path("smoke")
    df.write.format("delta").mode("overwrite").save(smoke_path)
    assert delta_spark.read.format("delta").load(smoke_path).count() == 1


@pytest.mark.integration
def test_silver_merge_expires_old_rating_and_inserts_new_current_version(delta_spark, tmp_delta_path, registered_db):
    from scripts.silver import utils as silver_utils

    get_dq_rules = ratings_dq_rules
    original_spark = patch_module_spark(silver_utils, delta_spark)
    table_name = f"{registered_db}.ratings_scd2"
    path = tmp_delta_path("silver_scd2")

    try:
        seed_df = delta_spark.createDataFrame(
            [(
                1, 10, 3.5, datetime(2022, 1, 1, 0, 0, 0), 20220101, False, 2022, True,
                datetime(2022, 1, 1, 0, 0, 0), None, datetime(2024, 1, 1, 0, 0, 0), 2022,
                [], "PASS", datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 0, 0),
                "job1", "/silver/ratings", "S3_MovieLens",
            )],
            SILVER_RATINGS_FINAL_SCHEMA,
        )
        seed_df.write.format("delta").mode("overwrite").partitionBy("rating_year").save(path)
        delta_spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")

        bronze_df = delta_spark.createDataFrame(
            [("1", "10", "4.5", 1672531200, datetime(2024, 1, 1, 0, 0, 0), 2023)],
            BRONZE_RATINGS_SCHEMA,
        )
        incoming = transform_ratings(bronze_df)
        incoming = apply_dq_flags(incoming, get_dq_rules())
        incoming = (
            incoming
            .withColumn("_processing_timestamp", F.lit(datetime(2024, 1, 2, 0, 0, 0)))
            .withColumn("_bronze_ingestion_timestamp", F.lit(datetime(2024, 1, 1, 0, 0, 0)))
            .withColumn("_job_run_id", F.lit("job2"))
            .withColumn("_notebook_path", F.lit("/silver/ratings"))
            .withColumn("_source_system", F.lit("S3_MovieLens"))
        )

        result = silver_utils.write_incremental_merge(
            df=incoming,
            full_table_name=table_name,
            s3_target_path=path,
            merge_key_cols=["user_id", "movie_id", "interaction_timestamp"],
            partition_by=["rating_year"],
            scd2_natural_key=["user_id", "movie_id"],
        )

        rows = (
            delta_spark.read.format("delta").load(path)
            .filter("user_id = 1 AND movie_id = 10")
            .collect()
        )
        assert len(rows) == 2
        assert result["rows_updated"] == 1
        assert result["rows_inserted"] == 1

        old_row = [r for r in rows if r["interaction_timestamp"].year == 2022][0]
        new_row = [r for r in rows if r["interaction_timestamp"].year == 2023][0]
        assert old_row["is_current"] is False
        assert old_row["effective_end_date"] == new_row["interaction_timestamp"]
        assert new_row["is_current"] is True
        assert new_row["effective_end_date"] is None
    finally:
        silver_utils.spark = original_spark


@pytest.mark.integration
def test_silver_merge_retroactive_older_rating_stays_historical(delta_spark, tmp_delta_path, registered_db):
    from scripts.silver import utils as silver_utils

    get_dq_rules = ratings_dq_rules
    original_spark = patch_module_spark(silver_utils, delta_spark)
    table_name = f"{registered_db}.ratings_scd2_retro"
    path = tmp_delta_path("silver_scd2_retro")

    try:
        seed_df = delta_spark.createDataFrame(
            [(
                1, 10, 4.5, datetime(2023, 1, 1, 0, 0, 0), 20230101, False, 2023, True,
                datetime(2023, 1, 1, 0, 0, 0), None, datetime(2024, 1, 1, 0, 0, 0), 2023,
                [], "PASS", datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 0, 0),
                "job1", "/silver/ratings", "S3_MovieLens",
            )],
            SILVER_RATINGS_FINAL_SCHEMA,
        )
        seed_df.write.format("delta").mode("overwrite").partitionBy("rating_year").save(path)
        delta_spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")

        bronze_df = delta_spark.createDataFrame(
            [("1", "10", "3.0", 1640995200, datetime(2024, 1, 2, 0, 0, 0), 2024)],
            BRONZE_RATINGS_SCHEMA,
        )
        incoming = transform_ratings(bronze_df)
        incoming = apply_dq_flags(incoming, get_dq_rules())
        incoming = (
            incoming
            .withColumn("_processing_timestamp", F.lit(datetime(2024, 1, 2, 0, 0, 0)))
            .withColumn("_bronze_ingestion_timestamp", F.lit(datetime(2024, 1, 2, 0, 0, 0)))
            .withColumn("_job_run_id", F.lit("job-retro"))
            .withColumn("_notebook_path", F.lit("/silver/ratings"))
            .withColumn("_source_system", F.lit("S3_MovieLens"))
        )

        silver_utils.write_incremental_merge(
            df=incoming,
            full_table_name=table_name,
            s3_target_path=path,
            merge_key_cols=["user_id", "movie_id", "interaction_timestamp"],
            partition_by=["rating_year"],
            scd2_natural_key=["user_id", "movie_id"],
        )

        rows = (
            delta_spark.read.format("delta").load(path)
            .filter("user_id = 1 AND movie_id = 10")
            .collect()
        )
        assert len(rows) == 2
        current_rows = [r for r in rows if r["is_current"] is True]
        old_row = [r for r in rows if r["interaction_timestamp"].year == 2022][0]
        new_row = [r for r in rows if r["interaction_timestamp"].year == 2023][0]

        assert len(current_rows) == 1
        assert current_rows[0]["interaction_timestamp"].year == 2023
        assert old_row["is_current"] is False
        assert old_row["effective_end_date"] == new_row["interaction_timestamp"]
    finally:
        silver_utils.spark = original_spark


@pytest.mark.integration
def test_late_arrival_warning_and_merge_routes_to_event_year_partition(delta_spark, tmp_delta_path, registered_db):
    from scripts.bronze import utils as bronze_utils
    from scripts.silver import utils as silver_utils

    get_dq_rules = ratings_dq_rules
    original_bronze_spark = patch_module_spark(bronze_utils, delta_spark)
    original_silver_spark = patch_module_spark(silver_utils, delta_spark)

    bronze_table = f"{registered_db}.bronze_ratings"
    silver_table = f"{registered_db}.silver_ratings"
    bronze_path = tmp_delta_path("bronze_ratings")
    silver_path = tmp_delta_path("silver_ratings")

    try:
        bronze_df = delta_spark.createDataFrame(
            [("7", "99", "4.0", 1546300800, datetime(2024, 1, 1, 0, 0, 0), 2022)],
            BRONZE_RATINGS_SCHEMA,
        )
        bronze_df.write.format("delta").mode("overwrite").partitionBy("_batch_year").save(bronze_path)
        delta_spark.sql(f"CREATE TABLE {bronze_table} USING DELTA LOCATION '{bronze_path}'")

        buf = StringIO()
        with redirect_stdout(buf):
            result = bronze_utils.detect_cross_year_records(bronze_table, 2022)

        log = buf.getvalue()
        assert result["cross_year_count"] == 1
        assert "_batch_year=2022" in log
        assert "2019" in log
        assert "LATE ARRIVAL" in log
        assert "Silver MERGE" in log

        incoming = transform_ratings(bronze_df)
        incoming = apply_dq_flags(incoming, get_dq_rules())
        incoming = (
            incoming
            .withColumn("_processing_timestamp", F.lit(datetime(2024, 1, 2, 0, 0, 0)))
            .withColumn("_bronze_ingestion_timestamp", F.lit(datetime(2024, 1, 1, 0, 0, 0)))
            .withColumn("_job_run_id", F.lit("job-late"))
            .withColumn("_notebook_path", F.lit("/silver/ratings"))
            .withColumn("_source_system", F.lit("S3_MovieLens"))
        )

        silver_utils.write_incremental_merge(
            df=incoming,
            full_table_name=silver_table,
            s3_target_path=silver_path,
            merge_key_cols=["user_id", "movie_id", "interaction_timestamp"],
            partition_by=["rating_year"],
            scd2_natural_key=["user_id", "movie_id"],
        )

        written = delta_spark.read.format("delta").load(silver_path).collect()
        assert len(written) == 1
        assert written[0]["_batch_year"] == 2022
        assert written[0]["is_late_arrival"] is True
        assert written[0]["rating_year"] == 2019
        assert delta_spark.read.format("delta").load(silver_path).filter("rating_year = 2022").count() == 0
    finally:
        bronze_utils.spark = original_bronze_spark
        silver_utils.spark = original_silver_spark


@pytest.mark.integration
def test_write_gold_ratings_partitions_replacewhere_is_idempotent(delta_spark, tmp_delta_path, registered_db):
    from scripts.gold import utils as gold_utils

    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings"
    path = tmp_delta_path("fact_ratings")

    try:
        df = delta_spark.createDataFrame(
            [(
                1, "movie_sk_10", 20190101, 4.0, datetime(2019, 1, 1, 0, 0, 0), 2019,
                datetime(2024, 1, 2, 0, 0, 0), "movielens.silver.ratings", "job1", "/gold/fact_ratings",
                "v1", datetime(2024, 1, 2, 0, 0, 0), 7,
            )],
            [
                "user_id", "movie_sk", "date_key", "rating", "interaction_timestamp", "rating_year",
                "_processing_timestamp", "_source_table", "_job_run_id", "_notebook_path",
                "_model_version", "_aggregation_timestamp", "_source_silver_version",
            ],
        )

        gold_utils.write_gold_ratings_replacewhere_partitions(df, table_name, path)
        first = (
            delta_spark.read.format("delta").load(path)
            .orderBy("user_id", "movie_sk", "interaction_timestamp")
            .collect()
        )

        gold_utils.write_gold_ratings_replacewhere_partitions(df, table_name, path)
        second = (
            delta_spark.read.format("delta").load(path)
            .orderBy("user_id", "movie_sk", "interaction_timestamp")
            .collect()
        )

        assert first == second
        assert len(second) == 1
        assert (
            delta_spark.read.format("delta").load(path)
            .groupBy("user_id", "movie_sk", "interaction_timestamp")
            .count()
            .filter("count > 1")
            .count()
        ) == 0
    finally:
        gold_utils.spark = original_spark


@pytest.mark.integration
def test_write_gold_merge_preserves_existing_rows_when_late_arrival_touches_old_year(delta_spark, tmp_delta_path, registered_db):
    from scripts.gold import utils as gold_utils

    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings_merge_safe"
    path = tmp_delta_path("fact_ratings_merge_safe")

    try:
        base_rows = [
            (
                "movie_sk_10", 1, 4.0, datetime(2019, 1, 1, 0, 0, 0), 20190101, 2019, False, 2019,
                datetime(2024, 1, 1, 0, 0, 0), "movielens.silver.ratings", "job1", "/gold/fact_ratings",
                "v1", datetime(2024, 1, 1, 0, 0, 0), 7,
            ),
            (
                "movie_sk_11", 2, 3.5, datetime(2019, 2, 1, 0, 0, 0), 20190201, 2019, False, 2019,
                datetime(2024, 1, 1, 0, 0, 0), "movielens.silver.ratings", "job1", "/gold/fact_ratings",
                "v1", datetime(2024, 1, 1, 0, 0, 0), 7,
            ),
        ]
        schema = [
            "movie_sk", "user_id", "rating", "interaction_timestamp", "date_key", "rating_year",
            "is_late_arrival", "_batch_year", "_processing_timestamp", "_source_table", "_job_run_id",
            "_notebook_path", "_model_version", "_aggregation_timestamp", "_source_silver_version",
        ]
        seed_df = delta_spark.createDataFrame(base_rows, schema)
        seed_df.write.format("delta").mode("overwrite").partitionBy("rating_year").save(path)
        delta_spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")

        # New batch with a late-arrival record for an old rating_year=2019.
        incoming = delta_spark.createDataFrame(
            [(
                "movie_sk_12", 3, 4.5, datetime(2019, 3, 1, 0, 0, 0), 20190301, 2019, True, 2022,
                datetime(2024, 1, 2, 0, 0, 0), "movielens.silver.ratings", "job2", "/gold/fact_ratings",
                "v1", datetime(2024, 1, 2, 0, 0, 0), 8,
            )],
            schema,
        )

        gold_utils.write_gold_merge(
            df=incoming,
            full_table_name=table_name,
            s3_target_path=path,
            merge_key_cols=["user_id", "movie_sk", "interaction_timestamp"],
            partition_by=["rating_year"],
        )

        out = delta_spark.read.format("delta").load(path)
        assert out.count() == 3
        assert out.filter("rating_year = 2019").count() == 3
        assert out.filter("user_id = 1").count() == 1
        assert out.filter("user_id = 2").count() == 1
        assert out.filter("user_id = 3").count() == 1
    finally:
        gold_utils.spark = original_spark
