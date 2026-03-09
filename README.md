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
                    │              S3 (raw/)                          │
                    │  movies.csv · ratings_YYYY.csv · tags_YYYY.csv  │
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
                    │  INNER JOIN · replaceWhere per partition          │
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
| `fact_ratings` | Fact | `(user_id, movie_sk, timestamp)` | `replaceWhere` per partition |
| `fact_genome_scores` | Fact | `(movie_sk, tag_sk)` | Full overwrite |

</details>

---

## Key Engineering Decisions

These aren't just implementation choices — each one solves a specific problem. Full rationale in [`ENGINEERING_PATTERNS.md`](README's/ENGINEERING_PATTERNS.md).

| Decision | Why It Matters |
|----------|---------------|
| **`SHOW PARTITIONS` for incrementality** | The Delta table IS the state. No external control files, no checkpoint databases. `SHOW PARTITIONS` reads only transaction log metadata — O(partitions), not O(rows). |
| **Record counts from Delta log** | `DeltaTable.history(1).operationMetrics["numOutputRows"]` reads a single JSON file. Zero data scans. Every `df.count()` call was systematically removed. |
| **SHA2-256 surrogate keys** | Deterministic (same input → same key across reruns), partition-safe, SCD2-ready. `monotonically_increasing_id()` drifts across runs and has no ordering guarantee. |
| **Silver flags, never drops** | Every row gets `_dq_status` + `_dq_failed_rules`. Quarantined rows stay for audit. Gold reads only `PASS` rows — no Silver reprocess if DQ rules change. |
| **SCD Type-2 on ratings** | When a user re-rates a movie, the old row is expired and the new row becomes current. Gold reads `is_current=True` for BI; ML reads Silver directly for full history. |
| **MERGE for late arrivals** | A 2019 rating in `ratings_2022.csv` lands in `rating_year=2019` via Silver MERGE. `replaceWhere` would wipe all existing 2019 data. |
| **`mergeSchema`, never `overwriteSchema`** | New columns are safely adopted. Removed or type-changed columns fail the write — breaking changes caught early, not silently applied. |
| **Maintenance decoupled from ETL** | OPTIMIZE, VACUUM, Z-ORDER run in a separate scheduled notebook during off-peak hours — not inline after every write. |

---

## Repository Structure

```
MovieLens-Delta-Lakehouse/
│
├── scripts/
│   ├── environment_setup/          # Unity Catalog bootstrap + AWS IAM setup guide
│   ├── bronze/                     # Raw ingestion (3 files)
│   │   ├── bronze_utils.py         #   Shared utilities for all Bronze notebooks
│   │   ├── static_data_load.py     #   movies, links, genome_scores, genome_tags
│   │   └── historical_and_incremental_data_load.py   # ratings, tags (year-partitioned)
│   ├── silver/                     # Cleaning & DQ (7 files)
│   │   ├── silver_utils.py         #   Shared utilities (MERGE, SCD2, DQ framework)
│   │   └── *_data_cleaning.py      #   Per-table cleaning notebooks
│   ├── gold/                       # Star schema (9 files)
│   │   ├── gold_utils.py           #   Shared utilities (SK generation, FK joins)
│   │   ├── dimensional_tables/     #   5 dimension loaders
│   │   ├── fact_tables/            #   2 fact loaders
│   │   └── bridge_tables/          #   1 bridge loader
│   ├── maintenance/                # OPTIMIZE, VACUUM, Z-ORDER (separate from ETL)
│   └── analytics/                  # KPI export to Parquet for Streamlit
│
├── streamlit_app/                  # Multi-page dashboard (DuckDB + Plotly)
│   ├── app.py                      #   Entry point
│   ├── pages/                      #   5 pages: Trends, Movies, Genres, Users, Content DNA
│   ├── data/                       #   Local Parquet cache (synced from S3)
│   └── sync_data.py                #   S3 → local sync utility
│
├── README's/                       # Deep-dive documentation
│   ├── ARCHITECTURE.md             #   Layer contracts, data flow, S3 layout
│   ├── DATA_MODEL.md               #   Full star schema specification
│   ├── ENGINEERING_PATTERNS.md     #   16 design patterns with rationale
│   ├── PIPELINE_GUIDE.md           #   Execution order, parameters, troubleshooting
│   └── DQ_AND_LINEAGE.md           #   DQ rules, ETL metadata, lineage tracing
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

1. **Set up infrastructure** — Run [`environmentSetup.sql`](scripts/environment_setup/environmentSetup.sql) to create the `movielens` catalog and schemas. Follow the [External Location guide](scripts/environment_setup/external_location_setup_guide.md) for AWS IAM.

2. **Upload data** — Place MovieLens CSVs in `s3://<bucket>/raw/`.

3. **Execute in order** — Bronze → Silver → Gold dimensions → Gold facts/bridge. Each notebook is parameterized via Databricks Widgets.

4. **Schedule maintenance** — Run `maintenance/table_maintenance.py` as a separate job during off-peak hours.

Full execution details, widget parameters, and troubleshooting: [`PIPELINE_GUIDE.md`](README's/PIPELINE_GUIDE.md).

### Streamlit Dashboard

```bash
cd streamlit_app
pip install -r requirements.txt
python sync_data.py          # Sync latest Parquet from S3
streamlit run app.py
```

---

## Detailed Documentation

| Document | What You'll Find |
|----------|-----------------|
| [`ARCHITECTURE.md`](README's/ARCHITECTURE.md) | Layer contracts, data flow diagrams, S3 layout, Unity Catalog structure |
| [`DATA_MODEL.md`](README's/DATA_MODEL.md) | Complete star schema — every table, column, PK, FK, and surrogate key |
| [`ENGINEERING_PATTERNS.md`](README's/ENGINEERING_PATTERNS.md) | 16 design patterns, each with the problem, decision, and rationale |
| [`PIPELINE_GUIDE.md`](README's/PIPELINE_GUIDE.md) | Step-by-step execution, widget parameters, first-run vs incremental |
| [`DQ_AND_LINEAGE.md`](README's/DQ_AND_LINEAGE.md) | DQ rules by table, ETL metadata columns, cross-layer lineage tracing |

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
