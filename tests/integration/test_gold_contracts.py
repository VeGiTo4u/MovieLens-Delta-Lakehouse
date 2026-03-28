from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from scripts.gold import utils as gold_utils
from tests.helpers.delta_support import patch_module_spark, write_delta_table


pytestmark = [pytest.mark.contract, pytest.mark.integration]


FACT_COLUMNS = [
    "user_id",
    "movie_sk",
    "interaction_timestamp",
    "rating",
    "_source_table",
    "_job_run_id",
    "_notebook_path",
    "_model_version",
    "_aggregation_timestamp",
    "_source_silver_version",
]

DIM_COLUMNS = [
    "movie_sk",
    "_source_table",
    "_job_run_id",
    "_notebook_path",
    "_model_version",
    "_aggregation_timestamp",
    "_source_silver_version",
]

GENOME_TAG_COLUMNS = [
    "tag_sk",
    "_source_table",
    "_job_run_id",
    "_notebook_path",
    "_model_version",
    "_aggregation_timestamp",
    "_source_silver_version",
]


FACT_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_sk", StringType(), True),
    StructField("interaction_timestamp", TimestampType(), True),
    StructField("rating", DoubleType(), True),
    StructField("_source_table", StringType(), True),
    StructField("_job_run_id", StringType(), True),
    StructField("_notebook_path", StringType(), True),
    StructField("_model_version", StringType(), True),
    StructField("_aggregation_timestamp", TimestampType(), True),
    StructField("_source_silver_version", IntegerType(), True),
])

DIM_SCHEMA = StructType([
    StructField("movie_sk", StringType(), True),
    StructField("_source_table", StringType(), True),
    StructField("_job_run_id", StringType(), True),
    StructField("_notebook_path", StringType(), True),
    StructField("_model_version", StringType(), True),
    StructField("_aggregation_timestamp", TimestampType(), True),
    StructField("_source_silver_version", IntegerType(), True),
])

GENOME_TAG_SCHEMA = StructType([
    StructField("tag_sk", StringType(), True),
    StructField("_source_table", StringType(), True),
    StructField("_job_run_id", StringType(), True),
    StructField("_notebook_path", StringType(), True),
    StructField("_model_version", StringType(), True),
    StructField("_aggregation_timestamp", TimestampType(), True),
    StructField("_source_silver_version", IntegerType(), True),
])


def valid_fact_row(
    user_id=1,
    movie_sk="movie_sk_1",
    source_table="movielens.silver.ratings",
    job_run_id="job_1",
    source_silver_version=7,
):
    return (
        user_id,
        movie_sk,
        datetime(2024, 1, 1, 0, 0, 0),
        4.0,
        source_table,
        job_run_id,
        "/gold/fact_ratings",
        "1.0",
        datetime(2024, 1, 1, 0, 0, 1),
        source_silver_version,
    )


def valid_dim_row(movie_sk):
    return (
        movie_sk,
        "movielens.silver.movies",
        "job_1",
        "/gold/dim_movies",
        "1.0",
        datetime(2024, 1, 1, 0, 0, 1),
        12,
    )


def valid_genome_tag_row(tag_sk):
    return (
        tag_sk,
        "movielens.silver.genome_tags",
        "job_1",
        "/gold/dim_genome_tags",
        "1.0",
        datetime(2024, 1, 1, 0, 0, 1),
        13,
    )


def test_dim_movies_duplicate_movie_sk_fails_validation(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.dim_movies_contract"
    path = tmp_delta_path("dim_movies_contract")

    try:
        rows = [valid_dim_row("movie_sk_dup"), valid_dim_row("movie_sk_dup")]
        write_delta_table(delta_spark, rows, DIM_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="PK violation"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["movie_sk"],
            )
    finally:
        gold_utils.spark = original_spark


def test_dim_genome_tags_duplicate_tag_sk_fails_validation(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.dim_genome_tags_contract"
    path = tmp_delta_path("dim_genome_tags_contract")

    try:
        rows = [valid_genome_tag_row("tag_sk_dup"), valid_genome_tag_row("tag_sk_dup")]
        write_delta_table(delta_spark, rows, GENOME_TAG_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="PK violation"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["tag_sk"],
            )
    finally:
        gold_utils.spark = original_spark


def test_fact_ratings_null_movie_sk_fails_required_non_null(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings_null_movie_sk"
    path = tmp_delta_path("fact_ratings_null_movie_sk")

    try:
        rows = [valid_fact_row(movie_sk=None)]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="required non-null"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
                required_non_null_cols=["movie_sk", "user_id"],
            )
    finally:
        gold_utils.spark = original_spark


def test_fact_ratings_null_user_id_fails_required_non_null(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings_null_user_id"
    path = tmp_delta_path("fact_ratings_null_user_id")

    try:
        rows = [valid_fact_row(user_id=None)]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="required non-null"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
                required_non_null_cols=["movie_sk", "user_id"],
            )
    finally:
        gold_utils.spark = original_spark


def test_fact_ratings_orphan_movie_sk_fails_fk_check(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    fact_table = f"{registered_db}.fact_ratings_fk_contract"
    dim_table = f"{registered_db}.dim_movies_fk_contract"
    fact_path = tmp_delta_path("fact_ratings_fk_contract")
    dim_path = tmp_delta_path("dim_movies_fk_contract")

    try:
        write_delta_table(delta_spark, [valid_dim_row("movie_sk_1")], DIM_SCHEMA, dim_path, dim_table)
        rows = [valid_fact_row(movie_sk="movie_sk_orphan")]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, fact_path, fact_table)

        with pytest.raises(ValueError, match="FK violation"):
            gold_utils.post_write_validation_gold(
                fact_table,
                expected_count=len(rows),
                pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
                required_non_null_cols=["movie_sk", "user_id"],
                fk_checks=[
                    {
                        "fk_column": "movie_sk",
                        "reference_table": dim_table,
                        "reference_column": "movie_sk",
                    }
                ],
            )
    finally:
        gold_utils.spark = original_spark


@pytest.mark.parametrize("source_table", [None, "", "   "])
def test_sourced_rows_null_or_blank_source_table_fail(delta_spark, tmp_delta_path, registered_db, source_table):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings_bad_source_table"
    path = tmp_delta_path("fact_ratings_bad_source_table")

    try:
        rows = [valid_fact_row(source_table=source_table)]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="_source_table"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
            )
    finally:
        gold_utils.spark = original_spark


@pytest.mark.parametrize("job_run_id", [None, "", "   "])
def test_sourced_rows_null_or_blank_job_run_id_fail(delta_spark, tmp_delta_path, registered_db, job_run_id):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings_bad_job_run_id"
    path = tmp_delta_path("fact_ratings_bad_job_run_id")

    try:
        rows = [valid_fact_row(job_run_id=job_run_id)]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="_job_run_id"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
            )
    finally:
        gold_utils.spark = original_spark


def test_sourced_rows_null_source_silver_version_fail(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.fact_ratings_bad_source_version"
    path = tmp_delta_path("fact_ratings_bad_source_version")

    try:
        rows = [valid_fact_row(source_silver_version=None)]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, path, table_name)

        with pytest.raises(ValueError, match="_source_silver_version"):
            gold_utils.post_write_validation_gold(
                table_name,
                expected_count=len(rows),
                pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
            )
    finally:
        gold_utils.spark = original_spark


def test_generated_rows_allow_null_source_silver_version(delta_spark, tmp_delta_path, registered_db):
    original_spark = patch_module_spark(gold_utils, delta_spark)
    table_name = f"{registered_db}.dim_date_generated_contract"
    path = tmp_delta_path("dim_date_generated_contract")

    try:
        rows = [valid_fact_row(source_table="GENERATED", source_silver_version=None)]
        write_delta_table(delta_spark, rows, FACT_SCHEMA, path, table_name)

        gold_utils.post_write_validation_gold(
            table_name,
            expected_count=len(rows),
            pk_columns=["user_id", "movie_sk", "interaction_timestamp"],
        )
    finally:
        gold_utils.spark = original_spark
