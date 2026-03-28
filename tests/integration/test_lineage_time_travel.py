from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


pytestmark = [pytest.mark.integration, pytest.mark.contract]


SILVER_RATINGS_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("interaction_timestamp", TimestampType(), True),
    StructField("rating", DoubleType(), True),
    StructField("_dq_status", StringType(), True),
])

GOLD_FACT_RATINGS_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_sk", StringType(), True),
    StructField("interaction_timestamp", TimestampType(), True),
    StructField("rating", DoubleType(), True),
    StructField("_source_table", StringType(), True),
    StructField("_source_silver_version", IntegerType(), True),
])


def test_gold_fact_row_can_trace_back_to_exact_silver_version(delta_spark, tmp_delta_path, registered_db):
    silver_table = f"{registered_db}.silver_ratings_lineage"
    gold_table = f"{registered_db}.gold_fact_ratings_lineage"
    silver_path = tmp_delta_path("silver_ratings_lineage")
    gold_path = tmp_delta_path("gold_fact_ratings_lineage")
    event_ts = datetime(2024, 1, 1, 0, 0, 0)

    silver_v0 = [(1, 10, event_ts, 3.5, "PASS")]
    delta_spark.createDataFrame(silver_v0, SILVER_RATINGS_SCHEMA).write.format("delta").mode("overwrite").save(silver_path)
    delta_spark.sql(f"CREATE TABLE {silver_table} USING DELTA LOCATION '{silver_path}'")

    source_version = delta_spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").collect()[0]["version"]
    assert source_version == 0

    silver_v1 = [(1, 10, event_ts, 4.5, "PASS")]
    delta_spark.createDataFrame(silver_v1, SILVER_RATINGS_SCHEMA).write.format("delta").mode("overwrite").save(silver_path)

    latest_version = delta_spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").collect()[0]["version"]
    assert latest_version > source_version

    gold_rows = [(1, "movie_sk_10", event_ts, 3.5, silver_table, int(source_version))]
    delta_spark.createDataFrame(gold_rows, GOLD_FACT_RATINGS_SCHEMA).write.format("delta").mode("overwrite").save(gold_path)
    delta_spark.sql(f"CREATE TABLE {gold_table} USING DELTA LOCATION '{gold_path}'")

    gold_row = delta_spark.table(gold_table).collect()[0]
    assert gold_row["_source_silver_version"] == source_version

    silver_snapshot = (
        delta_spark.read.format("delta")
        .option("versionAsOf", gold_row["_source_silver_version"])
        .table(gold_row["_source_table"])
    )
    silver_snapshot_row = (
        silver_snapshot
        .filter(
            (F.col("user_id") == gold_row["user_id"])
            & (F.col("interaction_timestamp") == F.lit(gold_row["interaction_timestamp"]))
        )
        .select("rating")
        .collect()[0]
    )

    latest_silver_rating = (
        delta_spark.table(silver_table)
        .filter((F.col("user_id") == 1) & (F.col("movie_id") == 10))
        .select("rating")
        .collect()[0]["rating"]
    )

    assert silver_snapshot_row["rating"] == gold_row["rating"]
    assert latest_silver_rating != gold_row["rating"]
