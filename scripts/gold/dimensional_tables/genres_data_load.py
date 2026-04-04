# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/gold/utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets
# ------------------------------------------------------------
dbutils.widgets.text("source_table_name",   "",          "Source Table Name")
dbutils.widgets.text("target_table_name",   "",          "Target Table Name")
dbutils.widgets.text("s3_target_path",      "",          "Target S3 URI (Delta Location)")
dbutils.widgets.text("source_catalog_name", "movielens", "Source Catalog")
dbutils.widgets.text("source_schema_name",  "silver",    "Source Schema")
dbutils.widgets.text("target_catalog_name", "movielens", "Target Catalog")
dbutils.widgets.text("target_schema_name",  "gold",      "Target Schema")
dbutils.widgets.text("model_version",       "1.0",       "Model Version")

source_table_name   = dbutils.widgets.get("source_table_name")
target_table_name   = dbutils.widgets.get("target_table_name")
s3_target_path      = dbutils.widgets.get("s3_target_path")
source_catalog_name = dbutils.widgets.get("source_catalog_name")
source_schema_name  = dbutils.widgets.get("source_schema_name")
target_catalog_name = dbutils.widgets.get("target_catalog_name")
target_schema_name  = dbutils.widgets.get("target_schema_name")
model_version       = dbutils.widgets.get("model_version")

# COMMAND ----------

# ------------------------------------------------------------
# Validation + Context
# ------------------------------------------------------------
s3_target_path = validate_inputs(s3_target_path, target_table_name, source_table_name)
etl_meta       = resolve_etl_metadata()

target_full, source_full = build_table_names(
    target_catalog_name, target_schema_name, target_table_name,
    source_catalog_name, source_schema_name, source_table_name,
)

# COMMAND ----------

# ------------------------------------------------------------
# Detect run mode — first run vs incremental
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql import Window

print("[START] Detecting run mode")

table_exists = spark.catalog.tableExists(target_full)

if table_exists:
    df_existing     = spark.table(target_full)
    existing_count  = df_existing.count()
    max_existing_id = df_existing.agg(F.max("genre_id")).collect()[0][0] or 0
    is_first_run    = False
    print(f"[INFO] Incremental run — existing genres: {existing_count:,}, max genre_id: {max_existing_id}")
else:
    df_existing     = None
    existing_count  = 0
    max_existing_id = 0
    is_first_run    = True
    print("[INFO] First run — full initial load")

# COMMAND ----------

# ------------------------------------------------------------
# Read Silver — PASS rows only
# ------------------------------------------------------------
silver_version                           = get_silver_version(source_full)
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

# COMMAND ----------

# ------------------------------------------------------------
# Extract unique genres from silver.movies
# ------------------------------------------------------------
df_genres_unique = (
    df_pass
    .select(F.explode("genres").alias("genre_name"))
    .filter(
        F.col("genre_name").isNotNull() &
        (F.trim(F.col("genre_name")) != "")
    )
    .select("genre_name")
    .distinct()
)

unique_genre_count = df_genres_unique.count()
print(f"[INFO] Unique genres in Silver : {unique_genre_count:,}")

# COMMAND ----------

# ------------------------------------------------------------
# Identify new genres
# ------------------------------------------------------------
if is_first_run:
    df_new_genres   = df_genres_unique
    new_genre_count = unique_genre_count
    print(f"[INFO] First run — all {new_genre_count:,} genres are new")
else:
    df_new_genres = df_genres_unique.join(
        df_existing.select("genre_name"),
        on="genre_name",
        how="left_anti"
    )
    new_genre_count = df_new_genres.count()
    print(f"[INFO] New genres to add : {new_genre_count:,}")

    if new_genre_count == 0:
        print("[INFO] No new genres — dim_genres is up to date")

# COMMAND ----------

# ------------------------------------------------------------
# Assign genre_id + generate genre_sk
#
# genre_id: sequential integer via row_number() — same stable
# assignment strategy as the original script.
#
# genre_sk: SHA2(genre_name, 256)
#   Why hash genre_name (not genre_id):
#     genre_name IS the natural key for genres — it is the
#     meaningful business identifier ("Action", "Comedy" etc.).
#     genre_id is itself already a surrogate-like integer, so
#     SHA2(genre_name) gives a true natural-key-derived SK that
#     will survive if genre_id assignment ever changes.
#     The bridge table uses genre_sk as its FK, so
#     bridge_movies_genres can generate the same hash from
#     genre_name without any join back to this table.
# ------------------------------------------------------------
if new_genre_count > 0:
    window_spec = Window.orderBy(F.monotonically_increasing_id())

    if is_first_run:
        df_new_with_ids = (
            df_new_genres
            .withColumn("genre_id", F.row_number().over(window_spec))
            .select("genre_id", "genre_name")
        )
    else:
        df_new_with_ids = (
            df_new_genres
            .withColumn("genre_id", F.row_number().over(window_spec) + max_existing_id)
            .select("genre_id", "genre_name")
        )

    # Generate surrogate key from genre_name (the true natural key)
    df_new_with_ids = generate_surrogate_key(df_new_with_ids, "genre_sk", "genre_name")

    # Reorder: SK first, then assigned ID, then name
    df_new_with_ids = df_new_with_ids.select("genre_sk", "genre_id", "genre_name")

    print(f"[INFO] genre_id + genre_sk assigned to {new_genre_count:,} new genres")
else:
    df_new_with_ids = None

# COMMAND ----------

# ------------------------------------------------------------
# Write or MERGE into dim_genres
# ------------------------------------------------------------
if new_genre_count > 0:
    df_new_with_ids = append_gold_metadata(
        df_new_with_ids, etl_meta, source_full, model_version, silver_version
    )

    if is_first_run:
        write_gold(df_new_with_ids, s3_target_path, target_table_name)
    else:
        df_new_with_ids.createOrReplaceTempView("new_genres_temp")

        spark.sql(f"""
            MERGE INTO {target_full} AS target
            USING new_genres_temp AS source
            ON LOWER(target.genre_name) = LOWER(source.genre_name)
            WHEN NOT MATCHED THEN
                INSERT (genre_sk, genre_id, genre_name,
                        _source_table, _job_run_id, _notebook_path,
                        _model_version, _aggregation_timestamp, _source_silver_version)
                VALUES (source.genre_sk, source.genre_id, source.genre_name,
                        source._source_table, source._job_run_id, source._notebook_path,
                        source._model_version, source._aggregation_timestamp,
                        source._source_silver_version)
        """)
        print(f"[SUCCESS] MERGE completed — {new_genre_count:,} new genres inserted")
else:
    print("[INFO] No new genres to write")

# COMMAND ----------

# ------------------------------------------------------------
# Register + Validate
#
# Note: OPTIMIZE, ANALYZE TABLE, and VACUUM are handled by the
# dedicated maintenance notebook (maintenance/table_maintenance.py)
# scheduled during off-peak hours — decoupled from ETL.
# ------------------------------------------------------------
register_table(target_full, s3_target_path)

expected_count = existing_count + new_genre_count
post_write_validation_gold(target_full, expected_count, pk_columns=["genre_sk"])

# COMMAND ----------

id_stats = spark.table(target_full).select(
    F.min("genre_id").alias("min_id"),
    F.max("genre_id").alias("max_id"),
).collect()[0]

print_summary(
    label            = "dim_genres",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    source_full_name = source_full,
    extra_info       = {
        "Run mode"               : "Initial Load" if is_first_run else "Incremental Update",
        "Silver rows total"      : f"{initial_count:,}",
        "Quarantined (excluded)" : f"{quarantine_count:,}",
        "Unique genres in Silver": f"{unique_genre_count:,}",
        "New genres added"       : f"{new_genre_count:,}",
        "Total genres in Gold"   : f"{expected_count:,}",
        "genre_id range"         : f"{id_stats['min_id']} → {id_stats['max_id']}",
        "Silver version read"    : silver_version,
        "Columns"                : "genre_sk (PK), genre_id, genre_name",
        "SK generation"          : "SHA2(genre_name, 256) — natural key is genre_name",
        "Write strategy"         : "OVERWRITE (first run) / MERGE LOWER(genre_name) (incremental)",
    }
)
