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
# Read Silver — PASS rows only
# ------------------------------------------------------------
silver_version                           = get_silver_version(source_full)
df_pass, initial_count, quarantine_count = read_silver_pass_only(source_full)

# COMMAND ----------

# ------------------------------------------------------------
# Transform + Surrogate Key
#
# tag_id is the natural key for genome tags — it is assigned
# by MovieLens and is stable across dataset versions.
# tag_sk = SHA2(CAST(tag_id AS STRING), 256).
#
# fact_genome_scores will reference tag_sk (not tag_id) as its
# FK to this dimension.
# ------------------------------------------------------------
from pyspark.sql import functions as F

df_gold = df_pass.select("tag_id", "tag").orderBy("tag_id")

# Generate surrogate key
df_gold = generate_surrogate_key(df_gold, "tag_sk", "tag_id")

# Reorder: SK first, then natural key, then attributes
df_gold = df_gold.select("tag_sk", "tag_id", "tag")

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

post_write_validation_gold(target_full, final_count, pk_columns=["tag_sk"])

# COMMAND ----------

print_summary(
    label            = "dim_genome_tags",
    target_full_name = target_full,
    s3_target_path   = s3_target_path,
    etl_meta         = etl_meta,
    model_version    = model_version,
    source_full_name = source_full,
    extra_info       = {
        "Silver rows total"      : f"{initial_count:,}",
        "Quarantined (excluded)" : f"{quarantine_count:,}",
        "Gold rows written"      : f"{final_count:,}",
        "Silver version read"    : silver_version,
        "Columns"                : "tag_sk (PK), tag_id (NK), tag",
        "SK generation"          : "SHA2(CAST(tag_id AS STRING), 256)",
        "Write strategy"         : "Full overwrite + mergeSchema",
    }
)
