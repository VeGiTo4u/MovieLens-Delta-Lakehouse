"""Reusable Delta/Spark infrastructure helpers for integration tests."""


def patch_module_spark(module, spark_session):
    original = getattr(module, "spark", None)
    module.spark = spark_session
    return original


def write_delta_table(spark, rows, schema, path, full_table_name):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)
    spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{path}'")
