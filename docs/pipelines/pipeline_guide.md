# PIPELINE_GUIDE.md — Running the Pipeline

## Prerequisites

1. Databricks workspace with **Unity Catalog enabled** (Free/Trial edition works)
2. AWS account with S3 bucket and IAM role configured
3. DBR 14.1+ (required for `dbruntime.databricks_repl_context`)

---

## Setup (One Time)

### Step 1: S3 Bucket Structure
Create one S3 bucket with four folders:
```
s3://<your-bucket>/raw/
s3://<your-bucket>/bronze/
s3://<your-bucket>/silver/
s3://<your-bucket>/gold/
```

### Step 2: AWS IAM Role
Create an IAM role with S3 read/write permissions for the bucket. Follow the detailed walkthrough in `environment_setup/external_location_setup_guide.md`.

### Step 3: Databricks External Locations
Create four External Locations in Databricks (Catalog → External Data → External Locations):
- `ext_raw_data` → `s3://<bucket>/raw/`
- `ext_bronze_data` → `s3://<bucket>/bronze/`
- `ext_silver_data` → `s3://<bucket>/silver/`
- `ext_gold_data` → `s3://<bucket>/gold/`

Apply the Databricks-generated trust policy to the IAM role's Trust Relationship in AWS.

### Step 4: Unity Catalog Bootstrap
Run `environment_setup/environmentSetup.sql` in Databricks SQL:
```sql
CREATE CATALOG IF NOT EXISTS movieLens;
USE CATALOG movieLens;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ext_raw_data TO `account users`;
-- (etc. for bronze, silver, gold locations)
```

### Step 5: Upload Source Data
Upload MovieLens CSV files to `s3://<bucket>/raw/`:
- Static files: `movies.csv`, `genome_scores.csv`, `genome_tags.csv`, `links.csv`
- Year-partitioned: `ratings_2015.csv`, `ratings_2016.csv`, ... `ratings_YYYY.csv`
- Year-partitioned: `tags_2015.csv`, `tags_2016.csv`, ... `tags_YYYY.csv`

---

## Notebook Execution Order

Strict dependency order must be followed. Each layer depends on the previous layer completing successfully.

### Phase 1: Bronze Ingestion

Run **static tables** first (no dependencies between them, can run in parallel):
```
bronze/static_data_load.py   → table_name=genome_scores
bronze/static_data_load.py   → table_name=genome_tags
bronze/static_data_load.py   → table_name=links
bronze/static_data_load.py   → table_name=movies
```

Run **incremental tables** (can run in parallel with each other):
```
bronze/historical_and_incremental_data_load.py  → table_name=ratings
bronze/historical_and_incremental_data_load.py  → table_name=tags
```

### Phase 2: Silver Cleaning

Run after Bronze completes (can run all in parallel):
```
silver/genome_scores_data_cleaning.py
silver/genome_tags_data_cleaning.py
silver/links_data_cleaning.py
silver/movies_data_cleaning.py
silver/ratings_data_cleaning.py
silver/tags_data_cleaning.py
```

### Phase 3: Gold — Dimensions (must run before facts/bridge)

These must complete before any facts or bridge:
```
gold/dimensional_tables/movies_data_load.py            ← must run first (others depend on movie_sk)
gold/dimensional_tables/genres_data_load.py            ← must run before bridge
gold/dimensional_tables/genome_tags_data_load.py
gold/dimensional_tables/external_links_data_load.py
gold/dimensional_tables/date_data_load.py              ← requires silver.ratings to exist
```

### Phase 4: Gold — Facts and Bridge (after dimensions)

```
gold/bridge_tables/movies_genres_data_load.py          ← requires dim_movies + dim_genres
gold/fact_tables/ratings_data_load.py                  ← requires dim_movies + dim_date
gold/fact_tables/genome_scores_data_load.py            ← requires dim_movies + dim_genome_tags
```

### Phase 5: Maintenance (scheduled separately — off-peak hours)

```
maintenance/jobs/table_maintenance.py                  ← runs OPTIMIZE, ANALYZE, ZORDER, VACUUM
```

> **Note:** This notebook is NOT part of the ETL pipeline. Schedule it as a separate Databricks Job to run during off-peak hours (e.g., nightly or weekly). It processes all Bronze, Silver, and Gold tables.

| Widget | Default | Description |
|--------|---------|-------------|
| `catalog_name` | `movielens` | Unity Catalog name |
| `layers` | `bronze,silver,gold` | Comma-separated layers to maintain |
| `vacuum_retention_override` | *(blank)* | Override VACUUM retention (hours). Blank = use per-table config (Bronze: 8760h / 1yr, Silver/Gold: 168h / 7d) |

---

## Widget Parameters

Every notebook is parameterized via **Databricks Widgets**. All parameters must be set before running.

### Bronze — `static_data_load.py`

| Widget | Default | Description |
|--------|---------|-------------|
| `s3_source_path` | *(required)* | `s3://<bucket>/raw/movies.csv` (full path to CSV) |
| `s3_target_path` | *(required)* | `s3://<bucket>/bronze/movies/` |
| `table_name` | *(required)* | `movies` (or `genome_scores`, `genome_tags`, `links`) |
| `catalog_name` | `movielens` | Unity Catalog name |
| `schema_name` | `bronze` | Schema name |

**Supported `table_name` values:** `genome_scores`, `genome_tags`, `links`, `movies`

### Bronze — `historical_and_incremental_data_load.py`

| Widget | Default | Description |
|--------|---------|-------------|
| `s3_source_path` | *(required)* | `s3://<bucket>/raw/` (directory, not file — notebook discovers years) |
| `s3_target_path` | *(required)* | `s3://<bucket>/bronze/ratings/` |
| `table_name` | *(required)* | `ratings` or `tags` |
| `catalog_name` | `movielens` | Unity Catalog name |
| `schema_name` | `bronze` | Schema name |

**Note:** `s3_source_path` points to the **directory** containing `ratings_YYYY.csv` files, not a specific file. The notebook discovers all available years automatically.

### Silver — All cleaning notebooks

| Widget | Default | Description |
|--------|---------|-------------|
| `source_table_name` | *(required)* | e.g., `movies` (Bronze table name) |
| `target_table_name` | *(required)* | e.g., `movies` (Silver table name) |
| `s3_target_path` | *(required)* | `s3://<bucket>/silver/movies/` |
| `source_catalog_name` | `movielens` | |
| `source_schema_name` | `bronze` | |
| `target_catalog_name` | `movielens` | |
| `target_schema_name` | `silver` | |

### Gold — All dimension/fact/bridge notebooks (except dim_date)

| Widget | Default | Description |
|--------|---------|-------------|
| `source_table_name` | *(required)* | Silver table name |
| `target_table_name` | *(required)* | Gold table name |
| `s3_target_path` | *(required)* | `s3://<bucket>/gold/dim_movies/` |
| `source_catalog_name` | `movielens` | |
| `source_schema_name` | `silver` | |
| `target_catalog_name` | `movielens` | |
| `target_schema_name` | `gold` | |
| `model_version` | `1.0` | Version tag stored in `_model_version` metadata column |

### Gold — `date_data_load.py` (no source table widget)

| Widget | Default | Description |
|--------|---------|-------------|
| `target_table_name` | *(required)* | `dim_date` |
| `s3_target_path` | *(required)* | `s3://<bucket>/gold/dim_date/` |
| `target_catalog_name` | `movielens` | |
| `target_schema_name` | `gold` | |
| `model_version` | `1.0` | |

`dim_date` automatically reads date range from `movielens.silver.ratings`.

---

## First Run vs Incremental Runs

### First Run

Bronze static tables: full write to an empty target path. Table created and registered.

Bronze incremental tables: discovers all year files on S3 → processes all years in order → each year written as a separate partition.

Silver: reads all Bronze partitions → processes all years → writes all partitions.

Gold dims: full Silver read → full overwrite of Gold table.

Gold fact_ratings: reads Silver `is_current=True` + `_dq_status=PASS` rows → writes via MERGE upsert keyed on `(user_id, movie_sk, interaction_timestamp)`.

### Incremental Runs (New Year Added to S3)

When a new year file (e.g., `ratings_2024.csv`) is uploaded to S3:

1. **Bronze:** `discover_s3_years()` finds 2024 in S3. `get_already_processed_years_bronze()` shows 2024 not in Delta. Processes only 2024, writes `_batch_year=2024` partition.

2. **Silver:** `get_available_years_from_source()` shows Bronze has 2024. `get_already_processed_years()` shows Silver doesn't. Processes only 2024.

3. **Gold fact_ratings:** `get_available_years_from_source()` shows Silver has 2024. Audit gating checks unseen batches / source version / model version / forced replay. Reads Silver 2024 `is_current=True` PASS rows and MERGEs into fact_ratings.

4. **Gold dims:** Full overwrite (dimensions are small; no incremental needed).

### Exit Codes

All incremental notebooks call `dbutils.notebook.exit("NO_NEW_DATA")` when there's nothing to process. This is a clean exit, not an error.

---

## What Happens on Rerun of Same Year

**Bronze incremental:** `replaceWhere(_batch_year=year)` atomically replaces the partition. Delta ACID rolls back if the write fails. Result: exactly one copy of the year's data.

**Silver ratings:** MERGE with SCD2 semantics. Same records matched → no changes (idempotent). Re-ratings handled via SCD2 versioning (expire old, insert new).

**Gold fact_ratings:** MERGE upsert on `(user_id, movie_sk, interaction_timestamp)`. Reads Silver `is_current=True` rows only. Re-runs are idempotent and replay-safe.

**Gold dims:** Full overwrite. Idempotent by definition.

---

## Troubleshooting

**`CONFIGURATION ERROR: Invalid source path ''`**
→ Widget not set. Set all required widgets before running.

**`FAILED: Bronze source table '...' does not exist`**
→ Bronze hasn't run yet, or ran with a different catalog/schema. Check execution order.

**`FAILED: Cannot list S3 source path '...'`**
→ External Location not configured, or S3 path doesn't exist. Check `external_location_setup_guide.md`.

**`FAILED: Zero PASS rows in silver...`**
→ All Silver rows are QUARANTINE. Check `silver.<table>` for `_dq_failed_rules` breakdown. Fix upstream data or DQ rules before running Gold.

**`FAILED: PK violation — N duplicate ... combinations found`**
→ Gold post-write validation caught duplicate primary keys. Check Silver for unexpected duplicates. Review MERGE key configuration.

**`[WARN] SHOW PARTITIONS returned empty ... but table exists`**
→ Unity Catalog metadata sync lag (expected on serverless, first run). The fallback `distinct().collect()` ran automatically. Not an error.

**`[WARN] Duplicates dropped before MERGE`**
→ Silver produced duplicate records on the MERGE key. Should not happen with correct Silver DQ. Investigate Silver processing for the affected year.

**Notebook exits with `NO_NEW_DATA`**
→ All available years are already processed. Not an error — expected behavior on rerun with no new source files.
