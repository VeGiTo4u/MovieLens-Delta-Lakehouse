"""
conftest.py — Shared test fixtures for MovieLens Delta Lakehouse tests.

Provides:
  - spark: Module-scoped local SparkSession for non-Delta tests
  - apply_dq_flags: Re-implementation of silver_utils.apply_dq_flags() for tests
    (avoids importing from Databricks notebook source files with %run dependencies)
"""
import os
import uuid
import sys
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext
from pyspark.sql import functions as F
from typing import List


# ────────────────────────────────────────────────────────────────
# Spark cleanup helper
# ────────────────────────────────────────────────────────────────
def _reset_spark_state():
    """
    Ensures there is no active/default Spark session/context
    before creating a fixture-specific session.
    """
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
    except Exception:
        pass

    try:
        default = SparkSession.getDefaultSession()
        if default is not None:
            default.stop()
    except Exception:
        pass

    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    except Exception:
        pass

    # Clear global singleton handles so getOrCreate() cannot reuse
    # a stale non-Delta session/context from previous fixtures.
    try:
        SparkContext._active_spark_context = None
    except Exception:
        pass

    try:
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
    except Exception:
        pass


# ────────────────────────────────────────────────────────────────
# Test ordering hook
# ────────────────────────────────────────────────────────────────
def pytest_collection_modifyitems(items):
    """
    Run Delta integration tests first.

    PySpark launches a JVM gateway once per pytest process, and the
    gateway classpath is fixed by the first Spark startup. Running
    integration tests first guarantees Delta classes are available
    before non-Delta fixtures create Spark sessions.
    """
    items.sort(
        key=lambda item: (
            0 if item.get_closest_marker("integration") is not None else 1,
            item.nodeid,
        )
    )


# ────────────────────────────────────────────────────────────────
# Spark fixture (module scope)
# ────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def spark():
    """
    Creates a lightweight local SparkSession for unit testing.

    Configuration choices:
      - local[*]    : Uses all available cores for parallelism
      - No Hive     : Avoids metastore overhead — tests don't need it
      - No Delta    : Unit tests operate on plain DataFrames, not Delta tables
      - shuffle=1   : Minimises shuffle partitions for small test data
      - driver 512m : Keeps memory footprint low for CI runners
      - UTC timezone: Matches Databricks default — timestamps behave identically
    """
    # Suppress verbose Spark logs during test runs
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    _reset_spark_state()

    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("movielens-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")         # No Spark UI needed
        .config("spark.driver.host", "localhost")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    # Suppress noisy Spark/Hadoop logs
    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()
    _reset_spark_state()


@pytest.fixture(scope="module")
def delta_spark(tmp_path_factory):
    """
    Creates a Delta-enabled SparkSession for integration tests.
    Keeps Delta config isolated from the lightweight unit-test session.
    """
    from delta import configure_spark_with_delta_pip

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    _reset_spark_state()

    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    ivy_dir = tmp_path_factory.mktemp("spark-ivy")
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("movielens-delta-integration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.jars.ivy", str(ivy_dir))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    _reset_spark_state()


@pytest.fixture
def tmp_delta_path(tmp_path):
    def _make(name: str) -> str:
        path = tmp_path / name
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    return _make


@pytest.fixture
def temp_db_name():
    return f"test_movielens_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def registered_db(delta_spark, temp_db_name):
    delta_spark.sql(f"CREATE DATABASE {temp_db_name}")
    try:
        yield temp_db_name
    finally:
        delta_spark.sql(f"DROP DATABASE IF EXISTS {temp_db_name} CASCADE")


# ────────────────────────────────────────────────────────────────
# DQ flag application (mirrors silver_utils.apply_dq_flags)
# ────────────────────────────────────────────────────────────────
def apply_dq_flags(df: DataFrame, dq_rules: List[tuple]) -> DataFrame:
    """
    Evaluates DQ rules per row and attaches _dq_status and
    _dq_failed_rules columns.

    Identical logic to silver_utils.apply_dq_flags() — reproduced
    here to avoid importing from Databricks notebook source files
    that depend on %run and dbutils.

    Args:
        df: DataFrame after transformation
        dq_rules: List of (rule_name, condition_column_expr) tuples.
                  condition_column_expr must be a Column that evaluates
                  to True when the row FAILS the rule.

    Columns added:
      _dq_status       : STRING — "PASS" or "QUARANTINE"
      _dq_failed_rules : ARRAY<STRING> — list of failed rule names
    """
    failed_rules_col = F.array_compact(
        F.array(*[
            F.when(fail_condition, F.lit(rule_name))
            for rule_name, fail_condition in dq_rules
        ])
    )

    return (
        df
        .withColumn("_dq_failed_rules", failed_rules_col)
        .withColumn("_dq_status",
                    F.when(F.size(F.col("_dq_failed_rules")) > 0,
                           F.lit("QUARANTINE"))
                     .otherwise(F.lit("PASS")))
    )
