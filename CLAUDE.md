# CLAUDE.md — MovieLens Delta Lakehouse

> **AI Context File** — Read this first. All sub-documents are linked below.
> This file gives you full working context for the project without reading any source code.

---

## What This Project Is

A production-grade **Delta Lake Lakehouse pipeline** built on **Databricks** and **AWS S3**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) over the **MovieLens dataset**. The Gold layer materializes a full **star schema** for BI and ML consumption.

**Tech stack:** PySpark · Delta Lake · Databricks Unity Catalog · AWS S3 · pandas (dim_date generation only)

**Dataset:** [MovieLens](https://grouplens.org/datasets/movielens/) — movies, ratings, tags, genome scores/tags, external links

---

## Repository Layout

```
MovieLens-Delta-Lakehouse/
├── environment_setup/
│   ├── environmentSetup.sql              # Unity Catalog bootstrap (catalog, schemas, grants)
│   └── external_location_setup_guide.md  # AWS IAM + S3 External Location setup walkthrough
│
├── bronze/
│   ├── bronze_utils.py                   # Shared utility library for all Bronze notebooks
│   ├── static_data_load.py               # Loader for static tables (movies, links, genome_*)
│   └── historical_and_incremental_data_load.py  # Year-partitioned loader (ratings, tags)
│
├── silver/
│   ├── silver_utils.py                   # Shared utility library for all Silver notebooks
│   ├── movies_data_cleaning.py
│   ├── ratings_data_cleaning.py
│   ├── tags_data_cleaning.py
│   ├── links_data_cleaning.py
│   ├── genome_scores_data_cleaning.py
│   └── genome_tags_data_cleaning.py
│
└── gold/
    ├── gold_utils.py                     # Shared utility library for all Gold notebooks
    ├── dimensional_tables/
    │   ├── dim_movies_data_load.py
    │   ├── dim_genres_data_load.py
    │   ├── dim_genome_tags_data_load.py
    │   ├── dim_external_links_data_load.py
    │   └── dim_date_data_load.py         # Generated from silver.ratings date range
    ├── fact_tables/
    │   ├── fact_ratings_data_load.py     # replaceWhere — reads is_current=True from Silver
    │   └── fact_genome_scores_data_load.py
    └── bridge_tables/
        └── bridge_movies_genres_data_load.py  # Resolves many-to-many movies↔genres

maintenance/
├── maintenance_utils.py                  # Table registry + OPTIMIZE/ANALYZE/VACUUM/ZORDER utilities
└── table_maintenance.py                  # Scheduled notebook — runs maintenance for all layers
```

---

## Sub-Documents (Read These for Deep Dives)

| File | What It Covers |
|------|---------------|
| [`README's/ARCHITECTURE.md`](README's/ARCHITECTURE.md) | Medallion layers, data flow, layer contracts, S3 layout, Unity Catalog structure |
| [`README's/DATA_MODEL.md`](README's/DATA_MODEL.md) | Full Gold star schema — all tables, columns, PKs, FKs, surrogate keys, relationships |
| [`README's/ENGINEERING_PATTERNS.md`](README's/ENGINEERING_PATTERNS.md) | Every significant design decision with rationale (SHOW PARTITIONS, MERGE vs replaceWhere, SHA2 SK, single-pass agg, mergeSchema, etc.) |
| [`README's/PIPELINE_GUIDE.md`](README's/PIPELINE_GUIDE.md) | How to run the pipeline — setup, execution order, widget parameters, first-run vs incremental |
| [`README's/DQ_AND_LINEAGE.md`](README's/DQ_AND_LINEAGE.md) | Data quality framework, ETL metadata columns at each layer, lineage tracing, late arrival handling |

---

## Critical Concepts to Know Immediately

### 1. Every notebook uses a shared `*_utils.py` via `%run`
There is no framework import — each layer has its own `bronze_utils.py`, `silver_utils.py`, `gold_utils.py`. Notebooks `%run` their layer's utils file at the top. All shared functions live there.

### 2. Two types of Bronze tables
- **Static** (`genome_scores`, `genome_tags`, `links`, `movies`): single CSV file → full overwrite on each load.
- **Incremental** (`ratings`, `tags`): year-partitioned CSVs (`ratings_2022.csv`). Loaded year-by-year with `replaceWhere(_batch_year)`. Idempotent per year.

### 3. Incrementality is self-contained in Delta
No external control files, no Autoloader checkpoints, no state databases. Processed years are tracked via `SHOW PARTITIONS` on the Delta table itself — a metadata-only read that never touches data files. Only `AnalysisException` ("table not found") is caught; all other errors propagate immediately to prevent silent full-reprocessing. Partition values are parsed with dual-format support (`_batch_year=2022` and plain `2022`) for cross-DBR compatibility.

### 4. Silver: flag, never drop
Silver attaches `_dq_status` (`PASS`/`QUARANTINE`) and `_dq_failed_rules` to every row. Quarantined rows stay in the Silver table for audit. Gold reads only `PASS` rows via `read_silver_pass_only()`.

### 5. Late arrivals + SCD2 are handled in Silver
A rating event timestamped 2019 may arrive in `ratings_2022.csv`. Bronze detects this (`detect_cross_year_records()`). Silver flags it (`is_late_arrival=True`) and routes it to the correct `rating_year` partition via `MERGE`. Silver also implements **SCD Type-2**: when a user re-rates the same movie, the old row is expired (`is_current=False`, `effective_end_date` set) and the new row is inserted as current. Gold reads only `is_current=True` rows — latest ratings for BI. ML models read Silver directly for full history.

### 6. Surrogate keys are SHA2-256, not monotonically_increasing_id
`generate_surrogate_key()` hashes the natural key with SHA2-256. Deterministic across reruns, partition-safe, SCD2-ready. MD5 was deliberately rejected (collision risk).

### 7. Record counts come from the Delta log, not re-scans
`_read_write_metrics()` reads `numOutputRows` from `DeltaTable.history(1).operationMetrics` — a single JSON file in `_delta_log`. Pre-write `df.count()` calls have been systematically removed everywhere.

### 8. Maintenance is decoupled from ETL
OPTIMIZE, ANALYZE TABLE, Z-ORDER BY, and VACUUM are **not** run inline in data load notebooks. They are handled by a dedicated `maintenance/table_maintenance.py` notebook, scheduled as a separate Databricks Job during off-peak hours. A centralized table registry in `maintenance/maintenance_utils.py` defines which commands apply to each table. Bronze gets OPTIMIZE + VACUUM (1-year retention). Silver and Gold get all four commands (7-day retention).

---

## Unity Catalog Namespace

All tables follow the three-part naming convention:

```
movielens.<layer>.<table>
          bronze   ratings
          silver   movies
          gold     dim_movies
                   fact_ratings
```

---

## Gold Layer — Star Schema at a Glance

```
dim_date ─────────────────────────────┐
dim_movies ───────────────────────────┤
          └─ bridge_movies_genres ────┤
                  │                   │
             dim_genres               │
                                      ▼
                               fact_ratings
                          fact_genome_scores
dim_genome_tags ──────────────────────┘
dim_external_links (standalone lookup)
```

See [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) for the full schema.

---

## Environment Setup (Quick Reference)

1. Run `environment_setup/environmentSetup.sql` in Databricks SQL to create the `movielens` catalog and `bronze`/`silver`/`gold` schemas.
2. Follow `environment_setup/external_location_setup_guide.md` to set up AWS IAM role + S3 External Locations (`ext_raw_data`, `ext_bronze_data`, `ext_silver_data`, `ext_gold_data`).
3. Upload MovieLens CSV files to `s3://<bucket>/raw/`.
4. Execute notebooks in dependency order: Bronze → Silver → Gold dims → Gold facts/bridge.

Full execution details: [`docs/PIPELINE_GUIDE.md`](docs/PIPELINE_GUIDE.md).

---

## Key Anti-Patterns This Project Explicitly Avoids

| Anti-Pattern | What We Do Instead |
|---|---|
| `df.count()` before write for record count | Read `numOutputRows` from Delta log after write |
| `distinct().collect()` to find partition years | `SHOW PARTITIONS` — metadata-only |
| Multiple `count()` calls on same DF | Single `agg()` with multiple expressions |
| `overwriteSchema` | `mergeSchema` — catches destructive schema changes |
| `monotonically_increasing_id()` for surrogate keys | SHA2-256 of natural key — deterministic & idempotent |
| Dropping bad rows in Silver | Flag with `_dq_status=QUARANTINE`, retain for audit |
| External state for incrementality | Delta table itself is the source of truth |
| `replaceWhere` for late arrivals in fact tables | Silver MERGE routes late arrivals to correct `rating_year` partition; Gold uses `replaceWhere` |
| `dropDuplicates()` before MERGE | `row_number()` with explicit ORDER BY — deterministic |
| No version history in Silver | SCD Type-2 on ratings — full history preserved, Gold reads only `is_current=True` |
| Blanket `except Exception` in metadata queries | Targeted `AnalysisException` catch — other errors propagate |
| `partitionBy()` on every incremental write | `partitionBy` only on first write — subsequent writes inherit layout |
| OPTIMIZE/ANALYZE inline after every write | Decoupled to `maintenance/table_maintenance.py` — runs on schedule during off-peak hours |
