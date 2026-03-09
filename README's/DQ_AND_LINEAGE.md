# DQ_AND_LINEAGE.md — Data Quality Framework & Lineage

## Data Quality Philosophy

**Silver flags; Gold filters; Bronze observes.**

| Layer | DQ Role | Action on Bad Data |
|-------|---------|-------------------|
| Bronze | Observability only | Logs cross-year records as WARNING; never modifies or drops |
| Silver | Full DQ evaluation | Flags each row with `_dq_status` and `_dq_failed_rules`; retains all rows |
| Gold | Enforcement | Reads only `_dq_status = 'PASS'` rows via `read_silver_pass_only()` |

---

## Silver DQ Rules by Table

### `silver.ratings`

| Rule Name | Condition (row is QUARANTINE if...) | Rationale |
|-----------|-------------------------------------|-----------|
| `NULL_USER_ID` | `user_id IS NULL` | Every rating must have an actor |
| `NULL_MOVIE_ID` | `movie_id IS NULL` | Every rating must have a subject |
| `NULL_RATING` | `rating IS NULL` | Rating value is the core measure |
| `NULL_TIMESTAMP` | `interaction_timestamp IS NULL` | Cannot derive date_key or late arrival flag |
| `INVALID_TIMESTAMP_FLOOR` | `interaction_timestamp < 1995-01-01` | Guards against Unix epoch-zero (1970-01-01) sentinel values. MovieLens activity begins ~1995. Evaluated only when `interaction_timestamp IS NOT NULL`. |
| `INVALID_RATING_RANGE` | `rating NOT BETWEEN 0.0 AND 5.0` | MovieLens valid range |
| `DATE_KEY_MISMATCH` | `date_key != date_format(interaction_timestamp, 'yyyyMMdd')` | Detects derivation bugs — ensures FK to dim_date is correct |

**Late arrivals are NOT quarantined:** `is_late_arrival = (event_year != _batch_year)` flags the record but does not affect `_dq_status`. Late arrivals are valid business events — dropping them would silently lose real user activity.

### `silver.movies`

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `NULL_MOVIE_ID` | `movie_id IS NULL` | Primary identifier required |
| `NULL_TITLE` | `title IS NULL OR TRIM(title) = ''` | Movie must have a name |
| `INVALID_RELEASE_YEAR` | `release_year IS NOT NULL AND release_year NOT BETWEEN 1800 AND 2199` | Transformation already validates; rule catches edge cases |

**Note:** `release_year = NULL` is valid and expected (not all movies have a trailing `(YYYY)` in their title string). Year is extracted from the end of the title only.

### `silver.genome_scores`

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `NULL_MOVIE_ID` | `movie_id IS NULL` | |
| `NULL_TAG_ID` | `tag_id IS NULL` | |
| `NULL_RELEVANCE` | `relevance IS NULL` | Core measure |
| `INVALID_RELEVANCE_RANGE` | `relevance NOT BETWEEN 0.0 AND 1.0` | Valid genome score range |

### `silver.genome_tags`

| Rule Name | Condition |
|-----------|-----------|
| `NULL_TAG_ID` | `tag_id IS NULL` |
| `NULL_TAG` | `tag IS NULL OR TRIM(tag) = ''` |

### `silver.links`

| Rule Name | Condition |
|-----------|-----------|
| `NULL_MOVIE_ID` | `movie_id IS NULL` |
| `NULL_IMDB_ID` | `imdb_id IS NULL OR TRIM(imdb_id) = ''` |

### `silver.tags`

| Rule Name | Condition |
|-----------|-----------|
| `NULL_USER_ID` | `user_id IS NULL` |
| `NULL_MOVIE_ID` | `movie_id IS NULL` |
| `NULL_TAG` | `tag IS NULL OR TRIM(tag) = ''` |
| `NULL_TIMESTAMP` | `interaction_timestamp IS NULL` |

---

## DQ Implementation: `apply_dq_flags()`

```python
def apply_dq_flags(df, dq_rules):
    """
    dq_rules: list of (rule_name: str, fail_condition: Column)
    fail_condition evaluates True when the row FAILS the rule.
    """
    failed_rules_col = array_compact(array(*[
        when(fail_condition, lit(rule_name))
        for rule_name, fail_condition in dq_rules
    ]))
    return (
        df
        .withColumn("_dq_failed_rules", failed_rules_col)  # ARRAY<STRING>
        .withColumn("_dq_status",
                    when(size(col("_dq_failed_rules")) > 0, lit("QUARANTINE"))
                    .otherwise(lit("PASS")))                # STRING
    )
```

All rules are evaluated in a single pass — no multiple filter scans. `array_compact` removes `NULL` entries from the array (rules that didn't fire produce `NULL`, not a name).

**Querying quarantined records:**
```sql
SELECT _dq_failed_rules, COUNT(*) as count
FROM movielens.silver.ratings
WHERE _dq_status = 'QUARANTINE'
GROUP BY _dq_failed_rules
ORDER BY count DESC
```

---

## ETL Metadata Columns — Full Reference

### Bronze — Static Tables

| Column | Type | Description |
|--------|------|-------------|
| `_input_file_name` | STRING | Exact S3 path per row via `_metadata.file_path` (Unity Catalog compatible) |
| `_ingestion_timestamp` | TIMESTAMP | When Spark read this record |
| `_job_run_id` | STRING | `{jobId}_{runId}` — ties row to Databricks Job run log |
| `_notebook_path` | STRING | Workspace path of Bronze notebook |
| `_source_system` | STRING | Constant `"S3_MovieLens"` |

### Bronze — Incremental Tables (ratings, tags)

All static columns plus:

| Column | Type | Description |
|--------|------|-------------|
| `_batch_id` | STRING | `"{table_name}_{year}"` e.g. `"ratings_2022"` — audit trail key |
| `_batch_year` | INT | Year as integer — **partition column**, used for `replaceWhere` and incrementality |

### Silver — All Tables

| Column | Type | Description |
|--------|------|-------------|
| `_processing_timestamp` | TIMESTAMP | When Silver transformation ran |
| `_bronze_ingestion_timestamp` | TIMESTAMP | Carried from Bronze `_ingestion_timestamp` — enables latency tracking |
| `_job_run_id` | STRING | Silver job+run ID |
| `_notebook_path` | STRING | Silver notebook path |
| `_source_system` | STRING | Constant `"S3_MovieLens"` |
| `_dq_status` | STRING | `"PASS"` or `"QUARANTINE"` |
| `_dq_failed_rules` | ARRAY\<STRING\> | Names of failed DQ rules; empty array for PASS rows |
| `_batch_year` | INT | Incremental tables only — partition column carried from Bronze |

### Silver — Ratings SCD2 Columns

| Column | Type | Description |
|--------|------|-------------|
| `is_current` | BOOLEAN | `True` for the latest rating per `(user_id, movie_id)`. Set to `False` when superseded by a re-rating |
| `effective_start_date` | TIMESTAMP | When this version became active (equals `interaction_timestamp`) |
| `effective_end_date` | TIMESTAMP | When this version was superseded. `NULL` for current versions |

### Gold — All Tables

| Column | Type | Description |
|--------|------|-------------|
| `_source_table` | STRING | Fully qualified Silver table name (e.g. `movielens.silver.movies`) or `"GENERATED"` for dim_date |
| `_job_run_id` | STRING | Gold job+run ID |
| `_notebook_path` | STRING | Gold notebook path |
| `_model_version` | STRING | Version string from widget (e.g. `"1.0"`) |
| `_aggregation_timestamp` | TIMESTAMP | When Gold transformation ran |
| `_source_silver_version` | INT | Delta table version of Silver source at read time. NULL for generated tables. |

---

## Lineage Tracing

Every Gold row can be traced back to its exact Silver snapshot:

1. `_source_table` → which Silver table it came from
2. `_source_silver_version` → exact Delta version of that table (point-in-time)
3. `_job_run_id` → which Databricks Job run processed it in Gold

To reconstruct the Silver data that produced a Gold row:
```python
# Time-travel read to the exact Silver version
df = spark.read.format("delta").option("versionAsOf", silver_version).table("movielens.silver.ratings")
```

Cross-layer lineage chain:
```
Gold._source_silver_version  →  Silver Delta log version
Silver._bronze_ingestion_timestamp  →  when Bronze loaded the row
Bronze._input_file_name  →  exact S3 source file path
Bronze._job_run_id  →  which Bronze Databricks Job run
```

---

## Post-Write Validation by Layer

### `post_write_validation_bronze()`
- Logs expected record count (from Delta log — no re-scan)
- Single-pass NULL check on all `_*` metadata columns
- Scoped to `processed_years` for incremental tables (avoids scanning old partitions)
- Fails hard on any NULL — NULL metadata means context resolution silently failed

### `post_write_validation()` (Silver)
- Logs expected record count
- Single-pass NULL check on: `_processing_timestamp`, `_bronze_ingestion_timestamp`, `_job_run_id`, `_notebook_path`, `_dq_status`, `_dq_failed_rules`
- Checks `_batch_year` for incremental tables only
- Fails hard on any NULL

### `post_write_validation_gold()`
- Logs expected record count (from write metrics or MERGE operation metrics)
- **PK uniqueness check:** `groupBy(pk_columns).count()` → `agg(sum(when(count > 1, 1)))` — single Spark action
- Single-pass NULL check on Gold metadata columns
- Fails hard on PK violations or NULL metadata
- Note: `_source_silver_version` is exempt from NULL check (intentionally NULL for dim_date)

---

## Bronze Observability: Cross-Year Detection

`detect_cross_year_records()` in `bronze_utils` provides early-warning observability for late arrivals.

**How it works:**
1. Filters Bronze table to `_batch_year = year`
2. Converts raw Unix timestamp to calendar year (`_event_year`)
3. Single `groupBy(_event_year).count()` action — driver-side arithmetic derives totals (no second Spark action)
4. Logs cross-year records as WARNING with distribution table

**Example log output:**
```
[WARN] Cross-year records detected in _batch_year=2022:
       Total records      : 5,000,000
       Cross-year records : 152 (0.003%)
       Year distribution  :
         2019 :        152  ← LATE ARRIVAL
         2022 : 4,999,848  ← expected
[WARN] These records will be flagged is_late_arrival=True in Silver
[WARN] Silver MERGE will route them to correct rating_year partitions
```

**Never fails Bronze:** Bronze's contract is raw fidelity — dropping or failing on cross-year records would silently discard data that Silver is designed to handle. Detection failure is also non-blocking (returns `{"total_records": -1, ...}` and continues).

---

## Querying the DQ System

**Find all quarantined ratings and why:**
```sql
SELECT
    _dq_failed_rules,
    COUNT(*) as records,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
FROM movielens.silver.ratings
WHERE _dq_status = 'QUARANTINE'
GROUP BY _dq_failed_rules
ORDER BY records DESC;
```

**Late arrival analysis:**
```sql
SELECT
    _batch_year,
    YEAR(interaction_timestamp) as event_year,
    COUNT(*) as count
FROM movielens.silver.ratings
WHERE is_late_arrival = TRUE
GROUP BY _batch_year, event_year
ORDER BY _batch_year, event_year;
```

**SCD2 versioning analysis (Silver ratings):**
```sql
-- Users who re-rated movies (multiple versions exist)
SELECT user_id, movie_id, COUNT(*) AS versions
FROM movielens.silver.ratings
WHERE _dq_status = 'PASS'
GROUP BY user_id, movie_id
HAVING COUNT(*) > 1
ORDER BY versions DESC
LIMIT 20;

-- View full version history for a specific user-movie pair
SELECT user_id, movie_id, rating, interaction_timestamp,
       is_current, effective_start_date, effective_end_date
FROM movielens.silver.ratings
WHERE user_id = 123 AND movie_id = 456
ORDER BY interaction_timestamp;
```

**Cross-layer latency:**
```sql
-- Bronze → Silver processing lag
SELECT
    _batch_year,
    AVG(UNIX_TIMESTAMP(_processing_timestamp) - UNIX_TIMESTAMP(_bronze_ingestion_timestamp)) / 60 AS avg_lag_minutes
FROM movielens.silver.ratings
GROUP BY _batch_year;
```

**Full lineage for a specific Gold fact row:**
```sql
-- Step 1: Find Gold row
SELECT movie_sk, user_id, interaction_timestamp, _source_silver_version, _job_run_id
FROM movielens.gold.fact_ratings
WHERE user_id = 123 AND movie_sk = 'abc...';

-- Step 2: Read exact Silver version (time travel)
SELECT * FROM movielens.silver.ratings VERSION AS OF <_source_silver_version>
WHERE user_id = 123;
```
