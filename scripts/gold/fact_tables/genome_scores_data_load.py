# Databricks notebook source
# MAGIC %run /MovieLens-Delta-Lakehouse/gold/gold_utils

# COMMAND ----------

# ------------------------------------------------------------
# Databricks Widgets
# ------------------------------------------------------------
dbutils.widgets.text("source_table_name",   "",              "Source Table Name")
dbutils.widgets.text("target_table_name",   "",              "Target Table Name")
dbutils.widgets.text("s3_target_path",      "",              "Target S3 URI (Delta Location)")
dbutils.widgets.text("source_catalog_name", "movielens",     "Source Catalog")
dbutils.widgets.text("source_schema_name",  "silver",        "Source Schema")
dbutils.widgets.text("target_catalog_name", "movielens",     "Target Catalog")
dbutils.widgets.text("target_schema_name",  "gold",          "Target Schema")
dbutils.widgets.text("model_version",       "1.0",           "Model Version")

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

dim_movies_full      = f"{target_catalog_name}.{target_schema_name}.dim_movies"
dim_genome_tags_full = f"{target_catalog_name}.{target_schema_name}.dim_genome_tags"

# COMMAND ----------

# ------------------------------------------------------------
# Read Silver — PASS rows only
# ------------------------------------------------------------
silver_version = get_silver_version(source_full)
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

# COMMAND ----------

# ------------------------------------------------------------
# Load dimension tables for SK lookup + FK validation
#
# Both joins serve dual purpose:
#   1. Resolve the surrogate key FK stored in the fact table
#   2. Exclude orphan natural keys not present in the Gold dims
# ------------------------------------------------------------
from pyspark.sql import functions as F

print("[START] Loading dimension tables")
try:
    df_dim_movies = spark.table(dim_movies_full).select("movie_id", "movie_sk")
    df_dim_tags   = spark.table(dim_genome_tags_full).select("tag_id", "tag_sk")
    print(f"[INFO] dim_movies      : {df_dim_movies.count():,} rows")
    print(f"[INFO] dim_genome_tags : {df_dim_tags.count():,} rows")
except Exception as e:
    raise RuntimeError(
        f"FAILED: dim_movies and dim_genome_tags must exist before fact_genome_scores. Error: {e}"
    )

# COMMAND ----------

# ------------------------------------------------------------
# Transform
#
# Steps:
#   1. Join → dim_movies on movie_id → resolves movie_sk + excludes orphans
#   2. Join → dim_genome_tags on tag_id → resolves tag_sk + excludes orphans
#   3. Select (movie_sk, tag_sk, relevance) — natural keys dropped
#
# fact_genome_scores has no time dimension so no partitioning.
# Composite PK is (movie_sk, tag_sk).
# ------------------------------------------------------------
print("[START] Transforming fact_genome_scores")

# Step 1: Resolve movie_sk + exclude orphan movie_ids
df_with_movie_sk = df_pass.join(
    df_dim_movies,
    on="movie_id",
    how="inner"
)

records_after_movie_join = df_with_movie_sk.count()
movie_orphans_removed    = initial_count - records_after_movie_join
print(f"[INFO] After dim_movies join      : {records_after_movie_join:,} rows ({movie_orphans_removed:,} orphans removed)")

# Step 2: Resolve tag_sk + exclude orphan tag_ids
df_gold_raw = df_with_movie_sk.join(
    df_dim_tags,
    on="tag_id",
    how="inner"
)

records_after_tag_join = df_gold_raw.count()
tag_orphans_removed    = records_after_movie_join - records_after_tag_join
print(f"[INFO] After dim_genome_tags join : {records_after_tag_join:,} rows ({tag_orphans_removed:,} orphans removed)")

# Step 3: Final selection — natural keys excluded, SKs only
df_gold = (
    df_gold_raw
    .select("movie_sk", "tag_sk", "relevance")
    .orderBy("movie_sk", "tag_sk")
)

final_count = df_gold.count()

if final_count == 0:
    raise RuntimeError("FAILED: Zero records after FK joins. Pipeline stopped.")

total_removed = initial_count - final_count
print(f"[INFO] Final fact records         : {final_count:,}")
print(f"[INFO] Total removed              : {total_removed:,}")

# COMMAND ----------

# ------------------------------------------------------------
# Append ETL Metadata + Write + Register + Optimize + Validate
#
# No partition_by — genome_scores has no time dimension.
# Z-ORDER by (movie_sk, tag_sk) — the composite PK, which
# is also the most common query predicate.
# ------------------------------------------------------------
df_gold = append_gold_metadata(
    df_gold, etl_meta, source_full, model_version, silver_version
)

final_count = write_gold(df_gold, s3_target_path, target_table_name)

# Note: OPTIMIZE, ANALYZE TABLE, and VACUUM are handled by the
# dedicated maintenance notebook (scripts/maintenance/jobs/table_maintenance.py)
# scheduled during off-peak hours — decoupled from ETL.
register_table(target_full, s3_target_path)

# Composite PK: (movie_sk, tag_sk)
post_write_validation_gold(
    target_full, final_count,
    pk_columns=["movie_sk", "tag_sk"]
)

# COMMAND ----------

coverage = spark.table(target_full).agg(
    F.countDistinct("movie_sk").alias("unique_movies"),
    F.countDistinct("tag_sk").alias("unique_tags"),
    F.min("relevance").alias("min_relevance"),
    F.max("relevance").alias("max_relevance"),
    F.avg("relevance").alias("avg_relevance"),
).collect()[0]

print_summary(
    label            = "fact_genome_scores",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    source_full_name = source_full,
    extra_info       = {
        "Silver rows (PASS)"     : f"{initial_count:,}",
        "Silver quarantined"     : f"{quarantine_count:,}",
        "Movie orphans removed"  : f"{movie_orphans_removed:,}",
        "Tag orphans removed"    : f"{tag_orphans_removed:,}",
        "Gold rows written"      : f"{final_count:,}",
        "Silver version read"    : silver_version,
        "Unique movies"          : f"{coverage['unique_movies']:,}",
        "Unique tags"            : f"{coverage['unique_tags']:,}",
        "Relevance range"        : f"{coverage['min_relevance']:.4f} → {coverage['max_relevance']:.4f}",
        "Avg relevance"          : f"{coverage['avg_relevance']:.4f}",
        "Columns"                : "movie_sk (FK), tag_sk (FK), relevance",
        "FK changes"             : "movie_id → movie_sk, tag_id → tag_sk (both surrogate)",
        "Composite PK"           : "(movie_sk, tag_sk)",
        "Partitioning"           : "None — no time dimension",
        "Optimization"           : "Z-ORDER BY (movie_sk, tag_sk)",
        "Write strategy"         : "Full overwrite + mergeSchema",
    }
)