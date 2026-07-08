<p align="center">
  <h1 align="center">MovieLens Delta Lakehouse</h1>
  <p align="center">
    A production-grade Delta Lake pipeline implementing the Medallion Architecture on Databricks,<br>
    transforming raw MovieLens data into an analytics-ready star schema.
  </p>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/AWS_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="S3"/>
  <img src="https://img.shields.io/badge/Unity_Catalog-003366?style=for-the-badge&logoColor=white" alt="Unity Catalog"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit"/>
</p>

---

## What This Project Does

This pipeline takes the [MovieLens](https://grouplens.org/datasets/movielens/) dataset — millions of movie ratings, tags, and genome scores — and processes it through a **three-layer Medallion Architecture** into a fully normalized **star schema** optimized for BI dashboards and ML feature extraction.

```
S3 (raw CSV)  →  Bronze (raw Delta)  →  Silver (clean + DQ-flagged)  →  Gold (star schema)
                                                                          ↓
                                                            Streamlit Dashboard (DuckDB + Plotly)
```

**It is not a toy ETL script.** Every design decision — from how incrementality works without external state, to why surrogate keys use SHA2-256 instead of `monotonically_increasing_id()` — is deliberately chosen and documented with rationale.

---

## Architecture

```
                    ┌──────────────────────────────────────────────────┐
                    │              S3 (raw/)                           │
                    │  movies.csv · ratings_YYYY.csv · tags_YYYY.csv   │
                    └──────────────────────┬───────────────────────────┘
                                           │
                    ┌──────────────────────▼───────────────────────────┐
                    │  BRONZE — Raw Ingestion Fidelity                 │
                    │  Explicit schemas · ETL metadata · Zero filters  │
                    │  Partitioned by _batch_year (source file year)   │
                    └──────────────────────┬───────────────────────────┘
                                           │
                    ┌──────────────────────▼───────────────────────────┐
                    │  SILVER — Cleaning & Quality Control             │
                    │  Type casts · DQ flags (never drops rows)        │
                    │  SCD Type-2 on ratings · MERGE for late arrivals │
                    │  Partitioned by rating_year (event year)         │
                    └──────────────────────┬───────────────────────────┘
                                           │
                    ┌──────────────────────▼───────────────────────────┐
                    │  GOLD — Star Schema                              │
                    │  SHA2-256 surrogate keys · FK validation via     │
                    │  INNER JOIN · MERGE upsert for fact_ratings      │
                    └──────────────────────────────────────────────────┘
```

<details>
<summary><b>Gold Star Schema</b></summary>

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

| Table | Type | Primary Key | Write Strategy |
|-------|------|-------------|----------------|
| `dim_movies` | Dimension | `movie_sk` (SHA2-256) | Full overwrite |
| `dim_genres` | Dimension | `genre_sk` (SHA2-256) | Incremental anti-join |
| `dim_genome_tags` | Dimension | `tag_sk` (SHA2-256) | Full overwrite |
| `dim_external_links` | Dimension | `link_sk` (SHA2-256) | Full overwrite |
| `dim_date` | Dimension | `date_key` (YYYYMMDD) | Generated from Silver |
| `bridge_movies_genres` | Bridge | `(movie_sk, genre_sk)` | Full overwrite |
| `fact_ratings` | Fact | `(user_id, movie_sk, timestamp)` | MERGE upsert on `(user_id, movie_sk, interaction_timestamp)` |
| `fact_genome_scores` | Fact | `(movie_sk, tag_sk)` | Full overwrite |

</details>

---

## Key Engineering Decisions

These aren't just implementation choices — each one solves a specific problem. Full rationale in [`engineering_patterns.md`](docs/standards/engineering_patterns.md).

| Decision | Why It Matters |
|----------|---------------|
| **`SHOW PARTITIONS` for incrementality** | The Delta table IS the state. No external control files, no checkpoint databases. `SHOW PARTITIONS` reads only transaction log metadata — O(partitions), not O(rows). |
| **Record counts from Delta log** | `DeltaTable.history(1).operationMetrics["numOutputRows"]` reads a single JSON file. Zero data scans. Every `df.count()` call was systematically removed. |
| **SHA2-256 surrogate keys** | Deterministic (same input → same key across reruns), partition-safe, SCD2-ready. `monotonically_increasing_id()` drifts across runs and has no ordering guarantee. |
| **Silver flags, never drops** | Every row gets `_dq_status` + `_dq_failed_rules`. Quarantined rows stay for audit. Gold reads only `PASS` rows — no Silver reprocess if DQ rules change. |
| **SCD Type-2 on ratings** | When a user re-rates a movie, the old row is expired and the new row becomes current. Gold reads `is_current=True` for BI; ML reads Silver directly for full history. |
| **MERGE for late arrivals** | A 2019 rating in `ratings_2022.csv` lands in `rating_year=2019` via Silver MERGE. Gold also uses MERGE to avoid historical partition rewrites dropping valid rows. |
| **`mergeSchema`, never `overwriteSchema`** | New columns are safely adopted. Removed or type-changed columns fail the write — breaking changes caught early, not silently applied. |
| **Maintenance decoupled from ETL** | OPTIMIZE, VACUUM, Z-ORDER run in a separate scheduled notebook during off-peak hours — not inline after every write. |

---

## Pipeline Impact & Scale

> **`41M+ records` · `24 year-partitions` · `22 DQ rules` · `8 Gold tables` · `16 engineering patterns` · `~55K rows/sec`**

<details>
<summary><b>Data Scale & Throughput</b></summary>

| Metric | Value |
|--------|-------|
| **Total records processed (all layers)** | **41M+** rows across Bronze → Silver → Gold |
| Core fact table (`fact_ratings`) | **23,799,461** ratings from **156,843** users across **52,057** movies |
| Genome relevance scores | **15,000,000** rows (dense matrix: movies × 1,129 genome tags) |
| Tag applications | **1,093,360** user-generated tags |
| Year-partitions managed | **24** partitions (1995–2018), largest single partition: **1.76M** ratings (2016) |
| Pipeline execution per partition | **~7–8 min** end-to-end (Bronze → Silver → Gold) on Databricks Serverless |
| Estimated sustained throughput | **~55K rows/sec** (1.76M-row partition / ~30s Spark processing per layer) |
| Gold star schema | **8 tables** — 5 dimensions, 2 fact tables, 1 bridge table |
| Cross-layer row-transitions | **~123M+** (each source row passes through 3 layers with transformation) |

</details>

<details>
<summary><b>Data Quality & Governance</b></summary>

| Metric | Value |
|--------|-------|
| **DQ rules enforced** | **22 rules** across 6 Silver tables — evaluated in a single pass per table |
| Row retention policy | **100%** — zero data loss. Quarantined rows retained with `_dq_status` + `_dq_failed_rules` for audit |
| SCD Type-2 versioning | Full history on **23.8M ratings** — re-ratings tracked with `is_current`, `effective_start_date`, `effective_end_date` |
| Late arrival handling | **3-layer observability**: Bronze detects → Silver flags + MERGE-routes to correct partition → Gold handles via MERGE upsert |
| Cross-layer lineage | **Row-level** tracing: Gold `_source_silver_version` → Silver Delta time-travel → Bronze `_input_file_name` → exact S3 source file |
| Data Integrity constraints | Native Delta Lake constraints via `ALTER TABLE ... ADD CONSTRAINT` for NOT NULL and explicit CHECK constraints on every table |
| Surrogate key strategy | **SHA2-256** deterministic hashing — idempotent across reruns, partition-safe, SCD2-ready |

**DQ coverage by table:**

| Table | Rules | Key Checks |
|-------|-------|------------|
| `ratings` | 7 | NULL checks, rating range [0–5], timestamp floor (1995+), date_key derivation |
| `tags` | 6 | NULL checks, empty/short tag detection, timestamp validation |
| `genome_scores` | 4 | NULL checks, relevance range [0–1] |
| `genome_tags` | 3 | NULL checks, empty tag detection |
| `movies` | 2 | NULL movie_id, empty title detection |
| `links` | 2 | NULL movie_id, missing external IDs |

</details>

<details>
<summary><b>Performance Engineering</b></summary>

| Optimization | Impact |
|-------------|--------|
| **Eliminated redundant `df.count()` scans** | 12+ full-table scans replaced with Delta log reads (`history(1).operationMetrics`) — zero data files opened for record counts |
| **Single-pass aggregations** | Replaced 3-action patterns (`count()` + `filter().count()` + `filter().count()`) with fused `agg()` — saves ~2 full data scans per year-partition, per table, per run |
| **O(partitions) incrementality** | `SHOW PARTITIONS` reads Delta transaction log metadata only, not data files. On a 23.8M-row table, this replaces a `distinct().collect()` full scan |
| **Zero-scan record counts** | `_read_write_metrics()` reads a single `_delta_log` JSON file — Delta ACID guarantees the committed count matches what Spark wrote |
| **Deterministic deduplication** | `row_number()` over explicit `ORDER BY _processing_timestamp DESC` — guarantees identical results across reruns regardless of Spark shuffle. `dropDuplicates()` is non-deterministic |
| **Broadcast joins** | Small dimensions (`dim_genres` ~20 rows, `dim_date`) broadcast-joined to avoid shuffle on multi-million-row fact tables |
| **Estimated compute savings** | ~40–60% reduction in redundant Spark actions per pipeline run from scan elimination patterns |

</details>

<details>
<summary><b>Infrastructure & Engineering Scope</b></summary>

| Component | Scale |
|-----------|-------|
| Pipeline code (`scripts/`) | **~7,900 lines** of PySpark across Bronze, Silver, Gold, maintenance, analytics |
| Test suite | **~3,100 lines** — 9 unit tests (Silver transforms + maintenance registry) + 5 integration tests (DQ framework, Gold contracts, incremental pipeline) |
| Dashboard | **~2,000 lines** — 5-page Streamlit app with DuckDB + Plotly (Trends, Movies, Genres, Users, Content DNA) |
| Documentation | **~1,400 lines** across 5 deep-dive docs (architecture, data model, engineering patterns, pipeline guide, DQ & lineage) |
| Total codebase | **~15,000 lines** (Python + Markdown + SQL) |
| Unity Catalog tables | **20 tables** managed across 3 schemas (6 Bronze + 6 Silver + 8 Gold) |
| Design patterns documented | **16** — each with problem statement, decision, and rationale in [`engineering_patterns.md`](docs/standards/engineering_patterns.md) |
| Anti-patterns avoided | **13** explicitly documented with alternatives |
| Maintenance operations | **4** automated commands (OPTIMIZE, ANALYZE, VACUUM, Z-ORDER) via centralized registry — decoupled from ETL |
| Schema evolution | `mergeSchema` on every write — safely adopts new columns, blocks destructive changes |

</details>

---

## Repository Structure

```
MovieLens-Delta-Lakehouse/
│
├── .github/workflows/              # CI/CD pipelines (tests + dashboard sync)
├── scripts/
│   ├── common.py                   # Global configuration and cross-layer helpers
│   ├── environment_setup/          # Unity Catalog bootstrap + AWS IAM setup guide
│   ├── bronze/                     # Raw ingestion
│   │   ├── utils.py                #   Shared utilities for all Bronze notebooks
│   │   └── ingestion/              #   Static and historical/incremental loaders
│   ├── silver/                     # Cleaning & DQ
│   │   ├── utils.py                #   Shared utilities (MERGE, SCD2, DQ framework)
│   │   ├── transforms/             #   Importable pure transform + DQ functions
│   │   └── */transform.py          #   Per-table Databricks wrappers
│   ├── gold/                       # Star schema
│   │   ├── utils.py                #   Shared utilities (SK generation, FK joins, CDF audit)
│   │   ├── dimensional_tables/     #   5 dimension loaders
│   │   ├── fact_tables/            #   2 fact loaders
│   │   └── bridge_tables/          #   1 bridge loader
│   ├── maintenance/                # OPTIMIZE, VACUUM, Z-ORDER (separate from ETL)
│   └── analytics/                  # KPI export to Parquet for Streamlit
│
├── tests/                          # Pytest suite
│   ├── unit/                       #   PySpark transform logic tests
│   └── integration/                #   Delta Lake MERGE and DQ integration tests
│
├── dashboard/                      # Multi-page dashboard (DuckDB + Plotly)
│   ├── app.py                      #   Entry point
│   ├── pages/                      #   5 pages: Trends, Movies, Genres, Users, Content DNA
│   ├── data/                       #   Parquet KPIs fetched by AWS CLI
│   ├── services/                   #   Dashboard business logic
│   │   └── data_loader.py          #   DuckDB execution engine
│
├── docs/                           # Deep-dive documentation
│   ├── architecture/               #   Architecture, data model, DQ and lineage
│   ├── pipelines/                  #   Execution order, parameters, troubleshooting
│   └── standards/                  #   Engineering patterns and rationale
│
└── CLAUDE.md                       # AI context file (full project summary)
```

---

## How to Run

### Prerequisites
- **Databricks** workspace with Unity Catalog enabled (Free/Trial works)
- **AWS** S3 bucket with IAM role configured
- **DBR 14.1+** (required for `dbruntime.databricks_repl_context`)

### Quick Start

1. **Set up infrastructure** — Run [`environmentSetup.sql`](scripts/environment_setup/sql/environmentSetup.sql) to create the `movielens` catalog and schemas. Follow the [External Location guide](scripts/environment_setup/docs/external_location_setup_guide.md) for AWS IAM.

2. **Upload data** — Place MovieLens CSVs in `s3://<bucket>/raw/`.

3. **Execute in order** — Bronze → Silver → Gold dimensions → Gold facts/bridge. Each notebook is parameterized via Databricks Widgets.

4. **Schedule maintenance** — Run `scripts/maintenance/jobs/table_maintenance.py` as a separate job during off-peak hours.

Full execution details, widget parameters, and troubleshooting: [`pipeline_guide.md`](docs/pipelines/pipeline_guide.md).

### Dashboard

```bash
pip install -r dashboard/requirements.txt
aws s3 sync s3://movielens-data-store/analytics/ dashboard/data/ --exclude "*" --include "*.parquet" --delete
streamlit run dashboard/app.py
```

### Verification

```bash
python -m pip install -e ".[test]"
pytest tests/unit/ -q
pytest tests/integration/ -q
```

Full Spark/Delta integration tests require Maven access, or cached Delta Lake jars:

```bash
pytest tests/integration -q
```

---

## Detailed Documentation

| Document | What You'll Find |
|----------|-----------------|
| [`architecture.md`](docs/architecture/architecture.md) | Layer contracts, data flow diagrams, S3 layout, Unity Catalog structure |
| [`data_model.md`](docs/architecture/data_model.md) | Complete star schema — every table, column, PK, FK, and surrogate key |
| [`engineering_patterns.md`](docs/standards/engineering_patterns.md) | 16 design patterns, each with the problem, decision, and rationale |
| [`pipeline_guide.md`](docs/pipelines/pipeline_guide.md) | Step-by-step execution, widget parameters, first-run vs incremental |
| [`dq_and_lineage.md`](docs/architecture/dq_and_lineage.md) | DQ rules by table, ETL metadata columns, cross-layer lineage tracing |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Processing | Apache Spark (PySpark) |
| Storage Format | Delta Lake |
| Orchestration | Databricks Notebooks + Jobs |
| Object Storage | AWS S3 |
| Catalog | Databricks Unity Catalog |
| Surrogate Keys | SHA2-256 |
| Dashboard | Streamlit + DuckDB + Plotly |
| Language | Python |

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
