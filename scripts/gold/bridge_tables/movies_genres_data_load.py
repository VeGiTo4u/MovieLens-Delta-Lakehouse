# Databricks notebook source
# MAGIC %run /MovieLens-Delta-Lakehouse/gold/gold_utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets
# ------------------------------------------------------------
dbutils.widgets.text("source_table_name",   "movies",    "Source Table Name (Silver)")
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

# Reference Gold Dimensions
dim_movies_full = f"{target_catalog_name}.{target_schema_name}.dim_movies"
dim_genres_full = f"{target_catalog_name}.{target_schema_name}.dim_genres"

# COMMAND ----------

# ------------------------------------------------------------
# Read Silver (PASS only) & Gold Dimensions
#
# We read (movie_sk) from dim_movies and (genre_sk, genre_name)
# from dim_genres. The bridge stores surrogate key FKs only —
# natural keys (movie_id, genre_id) are excluded because they
# add no value in the bridge; fact tables never join through
# the bridge to get natural keys.
# ------------------------------------------------------------
silver_version                           = get_silver_version(source_full)
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

print("[START] Loading Gold Dimensions for SK lookup")
try:
    # Load movie_sk lookup: movie_id → movie_sk
    df_dim_movies = spark.table(dim_movies_full).select("movie_id", "movie_sk")
    # Load genre_sk lookup: genre_name → genre_sk (genre_name is the join key)
    df_dim_genres = spark.table(dim_genres_full).select("genre_name", "genre_sk")
    print(f"[INFO] dim_movies loaded : {df_dim_movies.count():,} rows")
    print(f"[INFO] dim_genres loaded : {df_dim_genres.count():,} rows")
except Exception as e:
    raise RuntimeError(f"FAILED: dim_movies and dim_genres must exist before bridge. Error: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Transform — build bridge with surrogate key FKs
#
# Pipeline:
#   1. Explode genres ARRAY<STRING> → one row per (movie_id, genre_name)
#   2. Join → dim_movies on movie_id    → resolves movie_sk FK
#   3. Join → dim_genres on genre_name  → resolves genre_sk FK
#      (Broadcast join: dim_genres is small, ~20 rows)
#   4. Select (movie_sk, genre_sk) and deduplicate
#
# Why join to get SKs instead of generating them locally:
#   movie_sk  = SHA2(movie_id) — could be generated here,
#   genre_sk  = SHA2(genre_name) — could also be generated here.
#   However, joining dim_movies also performs implicit FK validation:
#   any silver movie_id not present in dim_movies (e.g. quarantined
#   movies) is automatically excluded by the INNER JOIN, with no
#   separate orphan-removal step needed.
#   The same applies to genre_sk via dim_genres.
#   The joins therefore serve dual purpose: SK resolution + FK enforcement.
# ------------------------------------------------------------
from pyspark.sql import functions as F

print("[START] Building bridge table with surrogate key FKs")

# Step 1: Explode genres
df_exploded = df_pass.select(
    "movie_id",
    F.explode("genres").alias("genre_name")
)

# Step 2: Resolve movie_sk — inner join excludes orphan movie_ids
df_with_movie_sk = df_exploded.join(
    df_dim_movies,
    on="movie_id",
    how="inner"
)

# Step 3: Resolve genre_sk — broadcast join (small dim), inner join
# excludes genre strings not present in dim_genres
df_with_both_sk = df_with_movie_sk.join(
    F.broadcast(df_dim_genres),
    on="genre_name",
    how="inner"
)

# Step 4: Final bridge — surrogate keys only, deduplicated
df_gold = (
    df_with_both_sk
    .select("movie_sk", "genre_sk")
    .distinct()
    .orderBy("movie_sk", "genre_sk")
)

final_count = df_gold.count()
print(f"[INFO] Bridge records : {final_count:,}")

# COMMAND ----------

# ------------------------------------------------------------
# Append ETL Metadata + Write + Register + Validate
#
# Composite PK is now (movie_sk, genre_sk) — both are STRING
# surrogate keys. Natural keys (movie_id, genre_id) are not
# stored in the bridge — BI tools join to dims for those.
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

post_write_validation_gold(target_full, final_count, pk_columns=["movie_sk", "genre_sk"])

# COMMAND ----------

unique_movies = spark.table(target_full).select("movie_sk").distinct().count()
unique_genres = spark.table(target_full).select("genre_sk").distinct().count()

print_summary(
    label            = "bridge_movies_genres",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    source_full_name = source_full,
    extra_info       = {
        "Silver movies total"    : f"{initial_count:,}",
        "Quarantined movies"     : f"{quarantine_count:,}",
        "Bridge rows written"    : f"{final_count:,}",
        "Unique movies in bridge": f"{unique_movies:,}",
        "Unique genres in bridge": f"{unique_genres:,}",
        "Avg genres per movie"   : f"{(final_count / unique_movies):.2f}" if unique_movies > 0 else "0",
        "Columns"                : "movie_sk (FK→dim_movies), genre_sk (FK→dim_genres)",
        "FK resolution strategy" : "INNER JOIN to dims — dual purpose: SK lookup + orphan exclusion",
        "Join strategy"          : "dim_genres via broadcast join (small dim, ~20 rows)",
        "Write strategy"         : "Full overwrite + mergeSchema",
    }
)