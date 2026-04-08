# DATA_MODEL.md — Gold Layer Star Schema

## Schema Overview

The Gold layer is a **star schema** optimized for BI tools and ML feature extraction. All dimension tables use **SHA2-256 surrogate keys** as primary keys. Fact tables reference dimension SKs as foreign keys.

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (date_key) │
                    └──────┬──────┘
                           │ date_key (FK)
┌───────────────────────────────────────────────────┐
│                   fact_ratings                     │
│  PK: (user_id, movie_sk, interaction_timestamp)   │
│  Partitioned by: rating_year                       │
└───────────────┬────────────────────────────────────┘
                │ movie_sk (FK)
         ┌──────┴───────┐
         │  dim_movies   │
         │  (movie_sk)   │
         └──────┬────────┘
                │ movie_sk (FK)
    ┌───────────┴────────────┐
    │  bridge_movies_genres  │
    │  PK: (movie_sk, genre_sk) │
    └───────────┬────────────┘
                │ genre_sk (FK)
         ┌──────┴───────┐
         │  dim_genres   │
         │  (genre_sk)   │
         └──────────────┘

┌──────────────────────────────────────────────────────────┐
│              fact_genome_scores                           │
│  PK: (movie_sk, tag_sk)                                  │
└──────────┬─────────────────────────┬─────────────────────┘
           │ movie_sk (FK)           │ tag_sk (FK)
    ┌──────┴───────┐          ┌──────┴────────┐
    │  dim_movies   │          │ dim_genome_tags│
    └──────────────┘          └───────────────┘

dim_external_links — standalone lookup (no fact FK)
```

---

## Dimension Tables

### `gold.dim_movies`

| Column | Type | Notes |
|--------|------|-------|
| `movie_sk` | STRING | **PK** — SHA2-256 of `movie_id` |
| `movie_id` | INT | Natural key (from MovieLens) |
| `title` | STRING | Cleaned: trailing year removed, articles reordered (e.g. "Dark Knight, The" → "The Dark Knight"), Title Case |
| `release_year` | INT | Extracted from trailing `(YYYY)` at end of title only, NULL if not found |
| `_source_table` | STRING | ETL: `movielens.silver.movies` |
| `_job_run_id` | STRING | ETL: Databricks job+run ID |
| `_notebook_path` | STRING | ETL: Gold notebook path |
| `_model_version` | STRING | ETL: model version string |
| `_aggregation_timestamp` | TIMESTAMP | ETL: when Gold ran |
| `_source_silver_version` | INT | ETL: Delta version of silver.movies read |

**SK generation:** `SHA2(CAST(movie_id AS STRING), 256)`

**Note:** `genres` is NOT stored here — it lives in `dim_genres` + `bridge_movies_genres`. This is a deliberate normalization choice.

**SCD2 note:** When SCD2 is implemented, `movie_sk = SHA2(CONCAT(CAST(movie_id AS STRING), '|', effective_start_date), 256)`. The `generate_surrogate_key()` function call only needs its `natural_key_cols` argument changed.

---

### `gold.dim_genres`

| Column | Type | Notes |
|--------|------|-------|
| `genre_sk` | STRING | **PK** — SHA2-256 of `genre_name` |
| `genre_id` | INT | Auto-assigned sequential integer (stable: max_existing_id + row_number) |
| `genre_name` | STRING | Title Case (e.g., `"Action"`, `"Sci-Fi"`) |
| *(+ standard ETL metadata columns)* | | |

**Source:** Exploded from `silver.movies.genres ARRAY<STRING>`

**Incremental behavior:** First run generates all genres. Subsequent runs detect new genres only (`LEFT ANTI JOIN` to existing), assign IDs continuing from `max(genre_id)`, and `UNION ALL` to existing table.

---

### `gold.dim_genome_tags`

| Column | Type | Notes |
|--------|------|-------|
| `tag_sk` | STRING | **PK** — SHA2-256 of `tag_id` |
| `tag_id` | INT | Natural key |
| `tag` | STRING | Tag label text |
| *(+ standard ETL metadata columns)* | | |

---

### `gold.dim_external_links`

| Column | Type | Notes |
|--------|------|-------|
| `link_sk` | STRING | **PK** — SHA2-256 of `movie_id` |
| `movie_id` | INT | Natural key |
| `imdb_id` | STRING | Stored as STRING (preserves leading zeros) |
| `tmdb_id` | INT | |
| *(+ standard ETL metadata columns)* | | |

**Design note:** `imdbId` is stored as STRING in Bronze schema intentionally — numeric coercion would silently drop leading zeros in IMDB IDs.

---

### `gold.dim_date`

| Column | Type | Notes |
|--------|------|-------|
| `date_key` | INT | **PK** — YYYYMMDD format (e.g., `20220315`) |
| `full_date` | DATE | Actual date value |
| `year` | INT | Calendar year |
| `quarter` | INT | 1–4 |
| `month` | INT | 1–12 |
| `month_name` | STRING | `"January"` etc. |
| `day_of_month` | INT | 1–31 |
| `day_of_week` | INT | US Standard: 1=Sunday, 7=Saturday |
| `day_of_week_name` | STRING | `"Monday"` etc. |
| `is_weekend` | BOOLEAN | True for Saturday/Sunday |
| `week_of_year` | INT | ISO week number |
| *(+ standard ETL metadata columns)* | | |

**Generated from:** `MIN` and `MAX` of `silver.ratings.interaction_timestamp` — automatically extends range as new years of ratings are loaded.

**Day-of-week convention:** Pandas uses Monday=0, Sunday=6. This table converts to US Standard (Sunday=1, Saturday=7).

**Quality checks:** Record count vs expected count, PK uniqueness, date continuity (no gaps detected via Spark `lag()` — not a driver-side Python loop).

**`_source_table`:** `"GENERATED"` — no Silver source. `_source_silver_version = NULL`.

---

## Fact Tables

### `gold.fact_ratings`

| Column | Type | Notes |
|--------|------|-------|
| `movie_sk` | STRING | FK → `dim_movies.movie_sk` |
| `user_id` | INT | No `dim_users` — denormalized identifier |
| `rating` | DOUBLE | 0.0–5.0, rounded to 1 decimal |
| `interaction_timestamp` | TIMESTAMP | Event time (from Unix epoch) |
| `date_key` | INT | FK → `dim_date.date_key` (YYYYMMDD) |
| `rating_year` | INT | **Partition column** — derived from `interaction_timestamp` year |
| `is_late_arrival` | BOOLEAN | True when event year ≠ _batch_year |
| `_batch_year` | INT | Retained for Gold incrementality tracking |
| `_processing_timestamp` | TIMESTAMP | From Silver (used in pre-MERGE dedup window) |
| *(+ standard Gold ETL metadata columns)* | | |

**Primary Key:** `(user_id, movie_sk, interaction_timestamp)` — composite natural key

**Partition column:** `rating_year` (event year from timestamp, NOT `_batch_year`)

**Write strategy:** MERGE upsert on `(user_id, movie_sk, interaction_timestamp)`, partitioned by `rating_year`. Silver handles late arrival routing and SCD2 versioning; Gold reads only `is_current=True` rows from Silver.

**Source filtering:** Gold reads Silver with `.filter(is_current == True)` — only the latest rating per `(user_id, movie_id)` pair is materialized in Gold. Full rating history (all versions) is preserved in Silver for ML models.

**Pre-write deduplication:** `row_number() OVER (PARTITION BY merge_key ORDER BY _processing_timestamp DESC)` — deterministic (latest Silver processing time wins).

**Optimization:** `OPTIMIZE ... ZORDER BY (movie_sk, user_id)` is handled by the decoupled maintenance job.

---

### `gold.fact_genome_scores`

| Column | Type | Notes |
|--------|------|-------|
| `movie_sk` | STRING | FK → `dim_movies.movie_sk` |
| `tag_sk` | STRING | FK → `dim_genome_tags.tag_sk` |
| `relevance` | DOUBLE | Genome relevance score (0.0–1.0) |
| *(+ standard Gold ETL metadata columns)* | | |

**Primary Key:** `(movie_sk, tag_sk)`

**Write strategy:** Full overwrite + `mergeSchema` (no late arrival scenario, static source)

---

## Bridge Tables

### `gold.bridge_movies_genres`

Resolves the many-to-many relationship between movies and genres.

| Column | Type | Notes |
|--------|------|-------|
| `movie_sk` | STRING | FK → `dim_movies.movie_sk` |
| `genre_sk` | STRING | FK → `dim_genres.genre_sk` |
| *(+ standard Gold ETL metadata columns)* | | |

**Primary Key:** `(movie_sk, genre_sk)`

**Source:** `silver.movies.genres ARRAY<STRING>` — exploded to one row per movie/genre pair.

**Join strategy:** INNER JOIN to `dim_movies` and `dim_genres` (broadcast on genres — ~20 rows). The INNER JOIN serves dual purpose: surrogate key resolution + implicit FK validation (orphan movie_ids excluded automatically).

**Natural keys excluded:** `movie_id` and `genre_id` are NOT stored in the bridge. BI tools join through the dimension tables to get natural keys.

---

## ETL Metadata Columns by Layer

### Bronze — Static tables
`_input_file_name`, `_ingestion_timestamp`, `_job_run_id`, `_notebook_path`, `_source_system`

### Bronze — Incremental tables (ratings, tags)
All above plus: `_batch_id` (e.g. `"ratings_2022"`), `_batch_year` (INT partition column)

### Silver — All tables
`_processing_timestamp`, `_bronze_ingestion_timestamp`, `_job_run_id`, `_notebook_path`, `_source_system`
Incremental also carry: `_batch_year`, `_dq_status`, `_dq_failed_rules`

### Silver — Ratings SCD2 columns
`is_current` (BOOLEAN), `effective_start_date` (TIMESTAMP), `effective_end_date` (TIMESTAMP)
`is_current=True` for the latest rating per `(user_id, movie_id)`. `effective_end_date=NULL` for current versions.

### Gold — All tables
`_source_table`, `_job_run_id`, `_notebook_path`, `_model_version`, `_aggregation_timestamp`, `_source_silver_version`

---

## Column Naming Conventions

| Pattern | Example | Meaning |
|---------|---------|---------|
| `*_sk` | `movie_sk` | Surrogate key (SHA2-256 STRING) |
| `*_id` | `movie_id` | Natural key from source system |
| `*_key` | `date_key` | Lookup key (not a surrogate, YYYYMMDD INT) |
| `_batch_year` | `_batch_year` | Source year partition (from filename) |
| `rating_year` | `rating_year` | Event year (from timestamp) |
| `_dq_*` | `_dq_status` | Data quality control columns |
| `_*` prefix | `_job_run_id` | System/ETL metadata — not business data |
