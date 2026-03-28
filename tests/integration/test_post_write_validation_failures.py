from datetime import datetime

import pytest
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from scripts.bronze import utils as bronze_utils
from scripts.silver import utils as silver_utils
from tests.helpers.delta_support import patch_module_spark, write_delta_table


pytestmark = [pytest.mark.integration, pytest.mark.contract]


BRONZE_VALIDATION_SCHEMA = StructType([
    StructField("_input_file_name", StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_job_run_id", StringType(), True),
    StructField("_notebook_path", StringType(), True),
    StructField("_source_system", StringType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_batch_year", IntegerType(), True),
])

SILVER_VALIDATION_SCHEMA = StructType([
    StructField("_processing_timestamp", TimestampType(), True),
    StructField("_bronze_ingestion_timestamp", TimestampType(), True),
    StructField("_job_run_id", StringType(), True),
    StructField("_notebook_path", StringType(), True),
    StructField("_dq_status", StringType(), True),
    StructField("_dq_failed_rules", ArrayType(StringType(), True), True),
    StructField("_batch_year", IntegerType(), True),
])


def test_post_write_validation_bronze_hard_fails_when_metadata_is_null(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(bronze_utils, delta_spark)
    table_name = f"{registered_db}.bronze_validation_null_meta"
    path = tmp_delta_path("bronze_validation_null_meta")

    try:
        rows = [(
            "s3://bucket/raw/ratings_2022.csv",
            datetime(2024, 1, 1, 0, 0, 0),
            None,
            "/bronze/historical",
            "S3_MovieLens",
            "ratings_2022",
            2022,
        )]
        write_delta_table(delta_spark, rows, BRONZE_VALIDATION_SCHEMA, path, table_name)

        with pytest.raises(RuntimeError, match="NULL values found in ETL metadata columns"):
            bronze_utils.post_write_validation_bronze(table_name, expected_count=1, processed_years=[2022])
    finally:
        bronze_utils.spark = original_spark


def test_post_write_validation_silver_hard_fails_when_metadata_is_null(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(silver_utils, delta_spark)
    table_name = f"{registered_db}.silver_validation_null_meta"
    path = tmp_delta_path("silver_validation_null_meta")

    try:
        rows = [(
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            None,
            "/silver/ratings",
            "PASS",
            [],
            2022,
        )]
        write_delta_table(delta_spark, rows, SILVER_VALIDATION_SCHEMA, path, table_name)

        with pytest.raises(RuntimeError, match="NULL values found in ETL metadata columns"):
            silver_utils.post_write_validation(table_name, expected_count=1)
    finally:
        silver_utils.spark = original_spark


def test_post_write_validation_bronze_hard_fails_when_delta_log_count_is_zero(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(bronze_utils, delta_spark)
    table_name = f"{registered_db}.bronze_validation_zero_count"
    path = tmp_delta_path("bronze_validation_zero_count")

    try:
        rows = [(
            "s3://bucket/raw/ratings_2022.csv",
            datetime(2024, 1, 1, 0, 0, 0),
            "job_1",
            "/bronze/historical",
            "S3_MovieLens",
            "ratings_2022",
            2022,
        )]
        write_delta_table(delta_spark, rows, BRONZE_VALIDATION_SCHEMA, path, table_name)

        with pytest.raises(RuntimeError, match="Zero records committed"):
            bronze_utils.post_write_validation_bronze(table_name, expected_count=0, processed_years=[2022])
    finally:
        bronze_utils.spark = original_spark


def test_post_write_validation_silver_hard_fails_when_delta_log_count_is_zero(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(silver_utils, delta_spark)
    table_name = f"{registered_db}.silver_validation_zero_count"
    path = tmp_delta_path("silver_validation_zero_count")

    try:
        rows = [(
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            "job_1",
            "/silver/ratings",
            "PASS",
            [],
            2022,
        )]
        write_delta_table(delta_spark, rows, SILVER_VALIDATION_SCHEMA, path, table_name)

        with pytest.raises(RuntimeError, match="Zero records committed"):
            silver_utils.post_write_validation(table_name, expected_count=0)
    finally:
        silver_utils.spark = original_spark
