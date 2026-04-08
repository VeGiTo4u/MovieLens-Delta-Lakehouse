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
dbutils.widgets.text("force_reprocess_batches", "",      "Force Reprocess Batches: '' | 'YYYY,YYYY' | 'ALL'")

source_table_name   = dbutils.widgets.get("source_table_name")
target_table_name   = dbutils.widgets.get("target_table_name")
s3_target_path      = dbutils.widgets.get("s3_target_path")
source_catalog_name = dbutils.widgets.get("source_catalog_name")
source_schema_name  = dbutils.widgets.get("source_schema_name")
target_catalog_name = dbutils.widgets.get("target_catalog_name")
target_schema_name  = dbutils.widgets.get("target_schema_name")
model_version       = dbutils.widgets.get("model_version")
force_reprocess_batches = dbutils.widgets.get("force_reprocess_batches").strip()

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

dim_movies_full = f"{target_catalog_name}.{target_schema_name}.dim_movies"
dim_date_full   = f"{target_catalog_name}.{target_schema_name}.dim_date"

# COMMAND ----------

# ------------------------------------------------------------
# Incrementality — replay-safe batch gating with audit table
#
# Batch replay semantics:
#   A batch is considered "already processed" only for the same
#   source_silver_version + model_version combination. This allows
#   safe recomputation after Silver fixes or Gold model changes.
#
# force_reprocess_batches:
#   ""            -> normal incrementality
#   "YYYY,YYYY"   -> process only listed years (if available)
#   "ALL"         -> process all available years
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

print("[START] Incrementality check")

silver_version = get_silver_version(source_full)
silver_batch_years = sorted(get_available_years_from_source(source_full))
audit_full = ensure_gold_batch_audit_table(target_catalog_name, target_schema_name)
processed_batch_years = sorted(
    get_successfully_processed_batches(
        audit_full_name=audit_full,
        target_full_name=target_full,
        source_full_name=source_full,
        source_silver_version=silver_version,
        model_version=model_version,
    )
)

if force_reprocess_batches.upper() == "ALL":
    batches_to_process = silver_batch_years
    replay_mode = "ALL"
elif force_reprocess_batches:
    requested = sorted({
        int(part.strip())
        for part in force_reprocess_batches.split(",")
        if part.strip()
    })
    missing = sorted(set(requested) - set(silver_batch_years))
    if missing:
        raise ValueError(
            f"CONFIGURATION ERROR: force_reprocess_batches contains unavailable years: {missing}. "
            f"Available: {silver_batch_years}"
        )
    batches_to_process = requested
    replay_mode = "SELECTIVE"
else:
    batches_to_process = sorted(set(silver_batch_years) - set(processed_batch_years))
    replay_mode = "INCREMENTAL"

batches_to_skip    = sorted(set(silver_batch_years) & set(processed_batch_years))

print(f"[INFO] Silver batch years available : {silver_batch_years}")
print(f"[INFO] Already in Gold              : {processed_batch_years}")
print(f"[INFO] Batches to process           : {batches_to_process}")
print(f"[INFO] Batches to skip              : {batches_to_skip}")
print(f"[INFO] Replay mode                  : {replay_mode}")
print(f"[INFO] Silver version               : {silver_version}")

if not batches_to_process:
    print("[INFO] All batches already in Gold — nothing to do. Exiting.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# ------------------------------------------------------------
# Read Silver — PASS + is_current rows for new batches only
#
# Silver ratings now has SCD Type-2 versioning. Gold only
# wants the LATEST rating per (user_id, movie_id) — filter
# is_current = True. ML models that need full history read
# Silver directly (all versions available there).
# ------------------------------------------------------------
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

df_new_batches = (
    df_pass
    .filter(F.col("_batch_year").isin(batches_to_process))
    .filter(F.col("is_current") == True)
)

# Batch metrics — single-pass aggregation
batch_agg = df_new_batches.agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("is_late_arrival") == True, 1).otherwise(0)).alias("late_arrivals"),
).collect()[0]

batch_count        = batch_agg["total"]
late_arrival_count = batch_agg["late_arrivals"]

print(f"[INFO] Silver PASS records in new batches : {batch_count:,}")

if late_arrival_count > 0:
    print(f"[INFO] Late arrivals in batch : {late_arrival_count:,}")
    print(f"[INFO] Silver already placed these in correct rating_year partitions")
    # Late arrival distribution — informational only
    df_new_batches.filter(F.col("is_late_arrival") == True) \
        .groupBy("_batch_year", "rating_year").count() \
        .orderBy("_batch_year", "rating_year").show(truncate=False)
else:
    print(f"[INFO] No late arrivals in this batch")

# COMMAND ----------

# ------------------------------------------------------------
# Load dimension tables for SK lookup + FK validation
# ------------------------------------------------------------
print("[START] Loading dimension tables")
try:
    df_dim_movies = spark.table(dim_movies_full).select("movie_id", "movie_sk")
    df_dim_date   = spark.table(dim_date_full).select("date_key")
    print(f"[INFO] dim_movies loaded : {dim_movies_full}")
    print(f"[INFO] dim_date loaded   : {dim_date_full}")
except Exception as e:
    raise RuntimeError(
        f"FAILED: dim_movies and dim_date must exist before fact_ratings. Error: {e}"
    )

# COMMAND ----------

# ------------------------------------------------------------
# Transform
#
# Architecture simplification (post-refactor):
#   Silver now provides rating_year directly — no need to
#   derive it here. Late arrivals are already in the correct
#   Silver partition. Gold simply reads, joins dims, and writes.
#
#   Gold uses MERGE upsert semantics to avoid year-partition
#   replacement risks when late-arrival batches touch old years.
# ------------------------------------------------------------
print("[START] Transforming")

df_with_sk   = df_new_batches.join(F.broadcast(df_dim_movies), on="movie_id", how="inner")
df_with_date = df_with_sk.join(F.broadcast(df_dim_date), on="date_key", how="inner")

df_gold = (
    df_with_date
    .select(
        "movie_sk",               # FK → dim_movies (surrogate)
        "user_id",                # Identifier — no dim_users
        "rating",                 # Measure
        "interaction_timestamp",  # Event time
        "date_key",               # FK → dim_date
        "rating_year",            # Partition column — from Silver
        "is_late_arrival",        # Observability flag from Silver
        "_batch_year",            # Retained for incrementality tracking
        "_processing_timestamp"
    )
)

# Final count + rating_year distribution in a single Spark action
year_dist_rows = (
    df_gold
    .groupBy("rating_year", "is_late_arrival")
    .count()
    .orderBy("rating_year", "is_late_arrival")
    .collect()
)

# Driver-side sum — no Spark action
final_count = sum(row["count"] for row in year_dist_rows)

if final_count == 0:
    raise RuntimeError("FAILED: Zero records after FK joins. Pipeline stopped.")

orphans_removed = batch_count - final_count
print(f"[INFO] Records to write  : {final_count:,}")
print(f"[INFO] Orphans removed   : {orphans_removed:,} (no matching dim_movies or dim_date key)")
print("[INFO] rating_year distribution:")
for row in year_dist_rows:
    print(f"  rating_year={row['rating_year']}  is_late_arrival={row['is_late_arrival']}  count={row['count']:,}")

# COMMAND ----------

# ------------------------------------------------------------
# Append ETL Metadata
# ------------------------------------------------------------
df_gold = append_gold_metadata(
    df_gold, etl_meta, source_full, model_version, silver_version
)

# COMMAND ----------

# ------------------------------------------------------------
# Write via MERGE upsert
# ------------------------------------------------------------
merge_result = write_gold_merge(
    df=df_gold,
    full_table_name=target_full,
    s3_target_path=s3_target_path,
    merge_key_cols=["user_id", "movie_sk", "interaction_timestamp"],
    partition_by=["rating_year"],
)
total_written = merge_result["rows_affected"]

# COMMAND ----------

# ------------------------------------------------------------
# Register Table
#
# Note: OPTIMIZE, ANALYZE TABLE, and VACUUM are handled by the
# dedicated maintenance notebook (scripts/maintenance/jobs/table_maintenance.py)
# scheduled during off-peak hours — decoupled from ETL.
# ------------------------------------------------------------
register_table(target_full, s3_target_path)

# COMMAND ----------

# ------------------------------------------------------------
# Coverage + Post-write Validation
# ------------------------------------------------------------
coverage = spark.table(target_full).agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("movie_sk").alias("unique_movies"),
    F.countDistinct("user_id").alias("unique_users"),
    F.min("rating_year").alias("min_year"),
    F.max("rating_year").alias("max_year"),
    F.avg("rating").alias("avg_rating"),
).collect()[0]

total_gold_count = coverage["total_rows"]

post_write_validation_gold(
    target_full,
    expected_count = total_written,
    pk_columns     = ["user_id", "movie_sk", "interaction_timestamp"],
    required_non_null_cols = ["movie_sk", "user_id"],
    fk_checks = [
        {
            "fk_column": "movie_sk",
            "reference_table": dim_movies_full,
            "reference_column": "movie_sk",
        }
    ],
)

log_gold_batch_audit(
    audit_full_name=audit_full,
    target_full_name=target_full,
    source_full_name=source_full,
    batch_years=batches_to_process,
    source_silver_version=silver_version,
    model_version=model_version,
    status="SUCCESS",
    etl_meta=etl_meta,
)

# COMMAND ----------

print_summary(
    label            = "fact_ratings",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    source_full_name = source_full,
    extra_info       = {
        "Batches processed"      : batches_to_process,
        "Batches skipped"        : batches_to_skip,
        "Silver PASS records"    : f"{batch_count:,}",
        "Late arrivals in batch" : f"{late_arrival_count:,}",
        "Orphans removed"        : f"{orphans_removed:,}",
        "Rows written"           : f"{total_written:,}",
        "Rows inserted"          : f"{merge_result['rows_inserted']:,}",
        "Rows updated"           : f"{merge_result['rows_updated']:,}",
        "Total rows in Gold"     : f"{total_gold_count:,}",
        "Unique movies"          : f"{coverage['unique_movies']:,}",
        "Unique users"           : f"{coverage['unique_users']:,}",
        "Year range"             : f"{coverage['min_year']} → {coverage['max_year']}",
        "Avg rating"             : f"{coverage['avg_rating']:.4f}",
        "Partition column"       : "rating_year — from Silver (event year, already routed)",
        "Late arrival handling"  : "Handled by Silver MERGE — Gold reads clean partitions",
        "Replay mode"            : replay_mode,
        "Idempotency"            : "MERGE on (user_id, movie_sk, interaction_timestamp)",
        "Write strategy"         : "MERGE upsert (no partition replacement)",
        "Optimization"           : "Z-ORDER BY (movie_sk, user_id)",
    }
)
