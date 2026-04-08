# ARCHITECTURE.md — Medallion Architecture & Data Flow

## Overview

The pipeline implements the **Medallion Architecture** across three distinct layers, each with a strict contract about what it stores and what transformations it performs.

```
S3 (raw/)          S3 (bronze/)        S3 (silver/)        S3 (gold/)
CSV files    →     Delta (raw)   →     Delta (clean)  →    Delta (star schema)
             Bronze             Silver              Gold
             [bronze_utils]     [silver_utils]      [gold_utils]
```

---

## Layer Contracts

### Bronze — Raw Ingestion Fidelity

**Principle:** Store exactly what the source sent. No business logic. No filtering. No transformation beyond schema enforcement.

**What Bronze does:**
- Reads CSV from S3 with an **explicit, enforced schema** (no schema inference ever)
- Attaches **ETL metadata columns** (`_input_file_name`, `_ingestion_timestamp`, `_job_run_id`, `_notebook_path`, `_source_system`)
- Incremental tables additionally attach `_batch_id` and `_batch_year`
- Writes to Delta on S3 and registers in Unity Catalog
- Runs `post_write_validation_bronze()` — NULL-checks all metadata columns

**What Bronze does NOT do:**
- Type coercion beyond the declared schema
- Deduplication
- Filtering of any rows
- Business rule enforcement

**Late arrival detection:** Bronze runs `detect_cross_year_records()` as a WARNING-only observability check. It never drops or modifies late arrival records — Bronze's contract is fidelity.

---

### Silver — Cleaning, Typing & Quality Flagging

**Principle:** Clean and conform the data. Flag bad rows. Never drop them.

**What Silver does:**
- Reads Bronze table (always the full registered Unity Catalog table)
- Applies **transformations**: column renames, type casts, timestamp conversions, string normalization, array expansion (genres pipe → `ARRAY<STRING>`)
- Attaches **DQ flags**: `_dq_status` (`PASS`/`QUARANTINE`) and `_dq_failed_rules` (array of rule names)
- For `ratings`: derives `date_key` (YYYYMMDD INT), `is_late_arrival`, casts Unix epoch → `TIMESTAMP`
- For `movies`: extracts `release_year` from trailing `(YYYY)` in title only, reorders trailing articles (e.g. "Dark Knight, The" → "The Dark Knight"), normalizes genres to `ARRAY<STRING>`
- Attaches Silver ETL metadata columns
- Preserves all rows — quarantined rows stay in Silver for audit

**What Silver does NOT do:**
- Deduplication of rating events (multiple ratings of same movie by same user are valid events)
- Business aggregation
- Surrogate key generation
- Joins between tables

**Incrementality:** For `ratings`, Silver processes batches using `_batch_year` incrementality tracking but **partitions by `rating_year`** (event year from timestamp). Late arrivals (e.g. 2019 events in `ratings_2022.csv`) are routed to the correct `rating_year` partition via MERGE. `_batch_year` is retained as a non-partition column for incrementality and audit.

**SCD Type-2 on ratings:** Silver implements SCD2 versioning on `(user_id, movie_id)`. When a user re-rates the same movie, the old row is expired (`is_current=False`, `effective_end_date` set) and the new row is inserted as current. Gold reads only `is_current=True` for BI; ML models read Silver directly for full rating history.

For `tags`, Silver continues to use `_batch_year` partitioning with `SHOW PARTITIONS` for incrementality.

---

### Gold — Star Schema Materialization

**Principle:** Produce clean, analytics-ready tables with surrogate keys, validated FK relationships, and full ETL lineage.

**What Gold does:**
- Reads Silver via `read_silver_pass_only()` — filters `_dq_status = 'PASS'` only
- Generates **SHA2-256 surrogate keys** for dimension primary keys
- Resolves **foreign key surrogate keys** via joins to dimension tables (INNER JOIN = implicit FK validation + orphan removal)
- Appends Gold ETL metadata: `_source_table`, `_job_run_id`, `_notebook_path`, `_model_version`, `_aggregation_timestamp`, `_source_silver_version`
- Runs `post_write_validation_gold()` — PK uniqueness check + NULL metadata check

**Maintenance (OPTIMIZE, ANALYZE, VACUUM, Z-ORDER)** is decoupled from ETL. These commands run in the dedicated `scripts/maintenance/jobs/table_maintenance.py` notebook, scheduled during off-peak hours. See `scripts/maintenance/utils.py` for the per-table configuration registry.

**Write strategies:**
- **Dimensions & bridge:** Full overwrite + `mergeSchema`
- **`fact_ratings`:** MERGE upsert on `(user_id, movie_sk, interaction_timestamp)` with `rating_year` partitioning (Silver handles late arrival routing + SCD2; Gold reads only `is_current=True` rows)
- **`fact_genome_scores`:** Full overwrite (no late arrival scenario)

**`dim_date` is special:** Generated programmatically from `silver.ratings` date range using pandas. Has no Silver source table. `_source_table = "GENERATED"`, `_source_silver_version = NULL`.

---

## Data Flow Diagram

```
Source Files (S3 raw/)
│
├─ genome_scores.csv ──→ bronze.genome_scores ──→ silver.genome_scores ──→ gold.fact_genome_scores
├─ genome_tags.csv   ──→ bronze.genome_tags   ──→ silver.genome_tags   ──→ gold.dim_genome_tags
├─ links.csv         ──→ bronze.links         ──→ silver.links         ──→ gold.dim_external_links
├─ movies.csv        ──→ bronze.movies        ──→ silver.movies        ──→ gold.dim_movies
│                                                                       ──→ gold.dim_genres
│                                                                       ──→ gold.bridge_movies_genres
│
├─ ratings_YYYY.csv  ──→ bronze.ratings       ──→ silver.ratings       ──→ gold.fact_ratings
│   (per year)            (partitioned by          (partitioned by          (partitioned by
│                          _batch_year)              rating_year — MERGE      rating_year — MERGE
│                                                    + SCD2 versioning)       reads is_current=True only)
└─ tags_YYYY.csv     ──→ bronze.tags          ──→ silver.tags
    (per year)
                                                silver.ratings ──→ gold.dim_date (generated)
```

---

## S3 Storage Layout

```
s3://<bucket>/
├── raw/                    # Source CSV files — immutable, never modified
│   ├── movies.csv
│   ├── genome_scores.csv
│   ├── genome_tags.csv
│   ├── links.csv
│   ├── ratings_2015.csv
│   ├── ratings_2016.csv
│   └── ... (one file per year for ratings and tags)
│
├── bronze/                 # Delta tables (raw ingestion)
│   ├── movies/
│   ├── genome_scores/
│   ├── genome_tags/
│   ├── links/
│   ├── ratings/            # Partitioned by _batch_year
│   └── tags/               # Partitioned by _batch_year
│
├── silver/                 # Delta tables (cleaned)
│   ├── movies/
│   ├── genome_scores/
│   ├── genome_tags/
│   ├── links/
│   ├── ratings/            # Partitioned by rating_year (SCD2 versioned)
│   └── tags/               # Partitioned by _batch_year
│
└── gold/                   # Delta tables (star schema)
    ├── dim_movies/
    ├── dim_genres/
    ├── dim_genome_tags/
    ├── dim_external_links/
    ├── dim_date/
    ├── bridge_movies_genres/
    ├── fact_ratings/       # Partitioned by rating_year (event year)
    └── fact_genome_scores/
```

---

## Unity Catalog Structure

```
movielens (catalog)
├── bronze (schema)
│   ├── movies
│   ├── genome_scores
│   ├── genome_tags
│   ├── links
│   ├── ratings          -- partitioned by _batch_year
│   └── tags             -- partitioned by _batch_year
│
├── silver (schema)
│   ├── movies
│   ├── genome_scores
│   ├── genome_tags
│   ├── links
│   ├── ratings          -- partitioned by rating_year, SCD2 (is_current, effective_start/end_date)
│   └── tags             -- partitioned by _batch_year
│
└── gold (schema)
    ├── dim_movies
    ├── dim_genres
    ├── dim_genome_tags
    ├── dim_external_links
    ├── dim_date
    ├── bridge_movies_genres
    ├── fact_ratings       -- partitioned by rating_year
    └── fact_genome_scores
```

**Key Unity Catalog behaviors this project accounts for:**
- Storage and metadata are **decoupled**: Delta files exist on S3 independently of the catalog entry. Dropping the catalog table entry does NOT delete data.
- `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION '...'` is always a no-op on subsequent runs.
- `SHOW PARTITIONS` on serverless compute may lag after the first write (Unity Catalog metadata sync). Both `silver_utils` and `gold_utils` have a fallback to `distinct().collect()` that fires exactly once when this edge case is hit. All three layers (`bronze_utils`, `silver_utils`, `gold_utils`) use targeted `AnalysisException` handling in `get_partition_years()` — only `TABLE_OR_VIEW_NOT_FOUND` returns an empty set; all other errors propagate immediately to prevent silent full-reprocessing. Partition values use positional `row[0]` access with multi-format parsing (`_batch_year=2022`, `rating_year=2022`, and plain `2022`) for cross-DBR compatibility. Bronze and Silver call `register_table()` early (inside the write loop, after the first successful year write) to ensure `SHOW PARTITIONS` works on pipeline retry. Gold `fact_ratings` uses `get_processed_batch_years()` to read `_batch_year` as a non-partition column for incrementality, since Gold is partitioned by `rating_year`.

---

## Execution Runtime Requirements

- **Databricks Runtime:** DBR 14.1+ (required for `dbruntime.databricks_repl_context`)
- **Cluster mode:** Works on all modes including Unity Catalog Shared Access Mode
- **Note:** `dbutils.notebook.entry_point` raises `Py4JSecurityException` on Shared Access Mode. This project uses `dbruntime.databricks_repl_context.get_context()` exclusively for job/run ID resolution.
