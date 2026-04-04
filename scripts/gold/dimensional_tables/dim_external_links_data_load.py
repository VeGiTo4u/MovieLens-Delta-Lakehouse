# Databricks notebook source
# MAGIC %run /Workspace/MovieLens-Delta-Lakehouse/scripts/gold/gold_utils

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

# FK reference — dim_movies must be created before this notebook runs
dim_movies_full = f"{target_catalog_name}.{target_schema_name}.dim_movies"
print(f"[INFO] FK reference : {dim_movies_full}")

# COMMAND ----------

# ------------------------------------------------------------
# Read Silver — PASS rows only
# ------------------------------------------------------------
silver_version                           = get_silver_version(source_full)
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

# COMMAND ----------

# ------------------------------------------------------------
# FK Validation vs dim_movies
#
# FK validation still uses movie_id (natural key) for the join
# predicate — this is correct because silver.links only contains
# movie_id, not movie_sk. The natural key join resolves which
# links rows have a valid counterpart in dim_movies.
#
# After validation, movie_sk is generated locally via SHA2(movie_id).
# Because dim_movies.movie_sk is also SHA2(movie_id), the values
# will match exactly — no additional join to dim_movies is needed
# just to look up the SK.
# ------------------------------------------------------------
from pyspark.sql import functions as F

print("[START] FK validation vs dim_movies")

try:
    valid_movie_ids  = spark.table(dim_movies_full).select("movie_id").distinct()
    dim_movies_count = valid_movie_ids.count()
    print(f"[INFO] Valid movie_ids in dim_movies : {dim_movies_count:,}")
except Exception as e:
    raise RuntimeError(
        f"FAILED: Cannot read dim_movies '{dim_movies_full}'. "
        f"dim_movies must be created before dim_external_links. Error: {e}"
    )

orphan_count = df_pass.join(valid_movie_ids, on="movie_id", how="left_anti").count()
df_fk_valid  = df_pass.join(valid_movie_ids, on="movie_id", how="inner")

print(f"[INFO] Orphan movie_ids (removed) : {orphan_count:,}")
print(f"[INFO] Records after FK filter    : {df_fk_valid.count():,}")

if orphan_count > 0:
    print(f"[WARN] {orphan_count:,} links rows removed — movie_id not in dim_movies")

# COMMAND ----------

# ------------------------------------------------------------
# Transform + Surrogate Key
#
# movie_sk is generated from movie_id using the same SHA2(movie_id)
# formula as dim_movies. The values match by construction —
# no join needed to look them up.
#
# dim_external_links is a 1:1 extension of dim_movies (one row
# per movie). Its SK is therefore identical to movie_sk in
# dim_movies, making it a simple equi-join in BI queries.
# ------------------------------------------------------------
df_gold = (
    df_fk_valid
    .select("movie_id", "imdb_id", "tmdb_id")
    .withColumn(
        "has_external_ids",
        F.col("imdb_id").isNotNull() | F.col("tmdb_id").isNotNull()
    )
    .orderBy("movie_id")
)

# Generate surrogate key — same input as dim_movies → same hash values
df_gold = generate_surrogate_key(df_gold, "movie_sk", "movie_id")

# Reorder: SK first, then natural key, then attributes
df_gold = df_gold.select("movie_sk", "movie_id", "imdb_id", "tmdb_id", "has_external_ids")

final_count = df_gold.count()
print(f"[INFO] Gold records : {final_count:,}")

# COMMAND ----------

# ------------------------------------------------------------
# Append ETL Metadata + Write + Register + Validate
#
# Note: OPTIMIZE, ANALYZE TABLE, and VACUUM are handled by the
# dedicated maintenance notebook (maintenance/table_maintenance.py)
# scheduled during off-peak hours — decoupled from ETL.
# ------------------------------------------------------------
df_gold = append_gold_metadata(
    df_gold, etl_meta, source_full, model_version, silver_version
)

final_count = write_gold(df_gold, s3_target_path, target_table_name)

register_table(target_full, s3_target_path)

post_write_validation_gold(target_full, final_count, pk_columns=["movie_sk"])

# COMMAND ----------

no_ext_ids_count = spark.table(target_full).filter(~F.col("has_external_ids")).count()

print_summary(
    label            = "dim_external_links",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    source_full_name = source_full,
    extra_info       = {
        "Silver rows total"        : f"{initial_count:,}",
        "Quarantined (excluded)"   : f"{quarantine_count:,}",
        "Orphans removed (FK)"     : f"{orphan_count:,}",
        "Gold rows written"        : f"{final_count:,}",
        "Movies with no ext IDs"   : f"{no_ext_ids_count:,}",
        "Silver version read"      : silver_version,
        "FK reference"             : dim_movies_full,
        "Columns"                  : "movie_sk (PK), movie_id (NK), imdb_id, tmdb_id, has_external_ids",
        "SK generation"            : "SHA2(CAST(movie_id AS STRING), 256) — matches dim_movies.movie_sk",
        "Write strategy"           : "Full overwrite + mergeSchema",
    }
)
