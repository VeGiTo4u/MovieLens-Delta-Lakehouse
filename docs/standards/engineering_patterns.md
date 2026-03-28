# ENGINEERING_PATTERNS.md — Design Decisions & Recurring Patterns

This document captures every significant technical decision in the codebase with its rationale. These patterns repeat consistently across all layers.

---

## 1. Incrementality Without External State

**Problem:** How to know which years have already been processed without a control table, checkpoint file, or external database?

**Decision:** Use `SHOW PARTITIONS` on the Delta table itself.

```python
def get_partition_years(full_table_name: str) -> set:
    rows = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()
    years = set()
    for row in rows:
        val = str(row[0]).strip()
        # Handle all DBR formats: "_batch_year=2022", "rating_year=2022", or plain "2022"
        if "_batch_year=" in val:
            years.add(int(val.split("_batch_year=")[1]))
        elif "rating_year=" in val:
            years.add(int(val.split("rating_year=")[1]))
        else:
            years.add(int(val))
    return years
```

**Why positional access (`row[0]`) instead of `row.partition`:** The column name returned by `SHOW PARTITIONS` varies across Databricks Runtime versions. Some versions return a column named `partition`, others do not. Positional access is universally safe.

**Why multi-format parsing:** Some DBR versions return key=value format (`_batch_year=2022` or `rating_year=2022`), others return plain values (`2022`). All formats are handled.

**Why SHOW PARTITIONS over `distinct().collect()`:**
- `distinct().collect()` triggers a full table scan — every row read to find ~10 unique year values. On a 200M-row ratings table this is catastrophic.
- `SHOW PARTITIONS` reads only the `_delta_log` partition metadata — O(partitions), not O(rows). Zero data files opened.

**Why not external state:**
- The Delta table IS the state. No synchronization needed. No orphaned control tables. Retry from failure is automatic — whatever's in the table is what was successfully committed.

**Exception handling:** Only `AnalysisException` with `TABLE_OR_VIEW_NOT_FOUND` is caught (returns empty set = first run). All other exceptions propagate — a blanket `except Exception` previously masked parsing errors and permissions issues, causing silent full reprocessing on every run.

**Early table registration:** `register_table()` is called inside the write loop after the first successful year write (not after the entire loop). This ensures `SHOW PARTITIONS` works correctly on retry if the loop fails mid-way.

**Unity Catalog metadata sync edge case:** On serverless compute, `SHOW PARTITIONS` may lag after the very first write (Hive metastore partition registry vs Delta log). Both `silver_utils` and `gold_utils` detect this: if `SHOW PARTITIONS` returns empty AND the table exists, they fall back to `distinct().collect()`. This fallback fires at most once per table lifetime.

**Non-partition incrementality:** For tables where the tracking column (`_batch_year`) is not the partition column (e.g., Silver ratings partitioned by `rating_year`), `get_already_processed_years()` and `get_processed_batch_years()` read `_batch_year` via `SELECT DISTINCT` instead of `SHOW PARTITIONS`.

---

## 2. Record Counts from Delta Log (Not Re-Scans)

**Problem:** Every pipeline needs to log "N records written." The naive approach is `df.count()` before write, or `spark.table(...).count()` after write. Both trigger full data scans.

**Decision:** Read `numOutputRows` from `DeltaTable.history(1).operationMetrics` after write.

```python
def _read_write_metrics(s3_target_path: str) -> int:
    metrics = (
        DeltaTable.forPath(spark, s3_target_path)
                  .history(1)
                  .select("operationMetrics")
                  .collect()[0]["operationMetrics"]
    )
    return int(metrics.get("numOutputRows", -1))
```

**Why this is safe:** Delta's write contract guarantees that if `write()` returns without exception, exactly `numOutputRows` rows were atomically committed. The log entry is the write receipt — no additional verification scan needed.

**Uses `forPath` not `forName`:** Works immediately after write, before Unity Catalog registration. `forName` requires the table to be registered first.

**Returns -1 on failure:** Non-blocking. Callers log a warning and continue — a missing count is not a pipeline correctness issue.

---

## 3. Single-Pass Aggregations

**Problem:** Per-year loops needed multiple metrics (total count, quarantine count, late arrival count). The naive pattern calls `df.count()` three times on the same DataFrame = three separate Spark jobs with no result reuse.

**Decision:** Compute all metrics in one `agg()` call.

```python
# OLD: 3 Spark jobs
total      = df.count()
quarantine = df.filter(col("_dq_status") == "QUARANTINE").count()
late       = df.filter(col("is_late_arrival") == True).count()

# NEW: 1 Spark job
result = df.agg(
    count("*").alias("total"),
    sum(when(col("_dq_status") == "QUARANTINE", 1).otherwise(0)).alias("quarantine"),
    sum(when(col("is_late_arrival") == True, 1).otherwise(0)).alias("late_arrivals"),
).collect()[0]
```

Spark's DAG optimizer evaluates all three aggregations in one scan + one shuffle. The savings are 2 full data scans per year, per run, per table.

This pattern also applies to PK uniqueness checks in `post_write_validation_gold()`:

```python
# Fused: groupBy + count + sum(when > 1) in one job
pk_dupes = (
    df_val
    .groupBy(pk_columns)
    .count()
    .agg(sum(when(col("count") > 1, 1).otherwise(0)).alias("dupes"))
    .collect()[0]["dupes"] or 0
)
```

And to computing total/pass counts in `read_silver_pass_only()` — one `agg()` for both, instead of two separate counts.

---

## 4. Write Idempotency — replaceWhere vs Full Overwrite

**Static tables** (`movies`, `genome_scores`, etc.): Full overwrite on every run. Safe because the source is a single immutable file.

**Incremental tables** (`ratings`, `tags`): Per-year partitioned write using `replaceWhere`.

```python
from delta.tables import DeltaTable
is_new_table = not DeltaTable.isDeltaTable(spark, s3_target_path)

writer = (
    df.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", f"_batch_year = {year}")
      .option("mergeSchema", "true")
)

# partitionBy only on first write — subsequent writes inherit the layout
if is_new_table:
    writer = writer.partitionBy("_batch_year")

writer.save(s3_target_path)
```

**Why `partitionBy` is conditional:** Once the Delta table is created with `partitionBy("_batch_year")` on the first write, the partition scheme is permanently stored in the Delta metadata. Subsequent writes inherit it automatically. Specifying `partitionBy` on every write can conflict with `replaceWhere` and cause Delta to overwrite the entire table instead of a single partition.

**`replaceWhere` semantics:** Despite the word "overwrite", this is NOT a full table overwrite. Delta scopes the operation to exactly the `_batch_year = year` partition. All other year partitions are never touched. ACID guarantees failed writes roll back cleanly.

**Why this achieves idempotency:** Running the same year N times produces exactly one copy of that year's data — the last run atomically replaces the partition.

---

## 5. MERGE for Late-Arrival + SCD2 in Silver

**Problem:** `silver.ratings` is partitioned by `rating_year` (the event year from the timestamp). A batch file `ratings_2022.csv` may contain records timestamped 2019. Using `replaceWhere(rating_year=2019)` would wipe all existing 2019 data and replace it with ~150 late arrivals. Additionally, users may re-rate the same movie over time — Silver must preserve full history.

**Decision:** Use Delta MERGE with SCD Type-2 semantics in Silver (`write_incremental_merge()`). Gold uses simple `replaceWhere`.

**SCD2 MERGE strategy (staging view approach):**
Delta MERGE cannot UPDATE + INSERT for the same matched row. The standard SCD2 pattern uses a staging view:

```python
# Staging view: UNION ALL of 'update' rows (to expire) + 'insert' rows (new data)
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW {staging_view} AS
    -- Part 1: Expire existing current versions
    SELECT target.user_id, target.movie_id, target.interaction_timestamp,
           'update' AS _merge_action,
           source.interaction_timestamp AS _new_effective_end_date
    FROM {target} AS target
    INNER JOIN {source_view} AS source
        ON target.user_id = source.user_id AND target.movie_id = source.movie_id
    WHERE target.is_current = TRUE
      AND source.interaction_timestamp > target.interaction_timestamp
    UNION ALL
    -- Part 2: All incoming rows as new inserts
    SELECT source.*, 'insert' AS _merge_action, NULL AS _new_effective_end_date
    FROM {source_view} AS source
""")

# MERGE with SCD2 conditions
spark.sql(f"""
    MERGE INTO {target} AS target
    USING {staging_view} AS staging
    ON target.user_id = staging.user_id
    AND target.movie_id = staging.movie_id
    AND target.interaction_timestamp = staging.interaction_timestamp
    WHEN MATCHED AND staging._merge_action = 'update' THEN
        UPDATE SET target.is_current = FALSE,
                   target.effective_end_date = staging._new_effective_end_date
    WHEN NOT MATCHED AND staging._merge_action = 'insert' THEN
        INSERT *
""")
```

**Merge key:** `(user_id, movie_id, interaction_timestamp)` — uniquely identifies a rating event.

**SCD2 natural key:** `(user_id, movie_id)` — identifies the entity being versioned.

**Late arrival routing:** A 2019 record in a 2022 batch gets `rating_year=2019`. MERGE inserts it into the 2019 partition. Existing 2019 records are completely untouched.

**Re-rating handling:** When user 42 rates movie 100 again with a newer timestamp, the old row is expired (`is_current=False`, `effective_end_date` set) and the new row is inserted as `is_current=True`.

**Rerun safety:** Same record matched again → no changes (idempotent).

**Gold reads only current:** `fact_ratings_data_load.py` filters `.filter(is_current == True)` when reading Silver. No MERGE in Gold — simple `replaceWhere`.

**First-run handling:** `write_incremental_merge()` detects `tableExists() == False` and falls back to a full write with SCD2 columns computed via window functions to bootstrap the table.

---

## 6. Pre-MERGE Deduplication — row_number() not dropDuplicates()

**Problem:** MERGE fails if the source DataFrame has duplicate rows on the merge key. Deduplication is needed before MERGE.

**Why NOT `dropDuplicates(merge_key_cols)`:**
- Makes no ordering guarantee. Which duplicate is kept depends on Spark partition assignment, which depends on shuffle — non-deterministic across runs.
- On a rerun with identical input, a different row could be kept, causing MERGE to UPDATE a Gold row with different metadata values than the first run. Silent correctness bug.

**Decision:** `row_number()` over an explicit ordering window.

```python
dedup_window = (
    Window
    .partitionBy(merge_key_cols)
    .orderBy(col("_processing_timestamp").desc())
)
df_deduped = (
    df
    .withColumn("_dedup_rank", row_number().over(dedup_window))
    .filter(col("_dedup_rank") == 1)
    .drop("_dedup_rank")
)
```

**Ordering:** Latest `_processing_timestamp` wins. Deterministic and repeatable across reruns regardless of shuffle.

---

## 7. SHA2-256 Surrogate Keys

**Decision:** All dimension surrogate keys are generated as `SHA2(natural_key_as_string, 256)`.

```python
def generate_surrogate_key(df, sk_col_name, *natural_key_cols):
    if len(natural_key_cols) == 1:
        hash_input = col(natural_key_cols[0]).cast("string")
    else:
        hash_input = concat_ws("|", *[col(c).cast("string") for c in natural_key_cols])
    return df.withColumn(sk_col_name, sha2(hash_input, 256))
```

**Why SHA2 over `monotonically_increasing_id()`:**
- **Deterministic:** Same natural key always produces the same SK. Reruns are idempotent — no SK drift between pipeline runs.
- **Partition-safe:** SHA2 works identically across all Spark partitions with zero driver coordination.
- **SCD2-ready:** SHA2(natural_key || effective_start_date) gives unique SKs per version.

**Why SHA2 over MD5:** MD5 has known collision vulnerabilities. SHA2-256 is the industry standard for non-cryptographic deterministic hashing.

**Multi-column keys:** Concatenated with `|` separator before hashing to prevent collisions between `(1, "23")` and `(12, "3")`.

---

## 8. mergeSchema, Never overwriteSchema

Every write uses `mergeSchema`:

```python
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(...)
```

**`mergeSchema` behavior:**
- New columns added upstream → safely adopted into the Delta schema
- Columns removed or type-changed → write FAILS (breaking change caught early)

**Why NOT `overwriteSchema`:** Would silently accept destructive schema changes — a column rename or type change would wipe metadata across the table. Appropriate only for intentional schema resets, never for routine pipeline runs.

---

## 9. Silver DQ: Per-Row Flagging, Never Dropping

**Decision:** Silver never drops rows with data quality issues. Every row gets `_dq_status` (`PASS`/`QUARANTINE`) and `_dq_failed_rules` (array of failed rule names).

```python
def apply_dq_flags(df, dq_rules):
    failed_rules_col = array_compact(array(*[
        when(fail_condition, lit(rule_name))
        for rule_name, fail_condition in dq_rules
    ]))
    return (
        df
        .withColumn("_dq_failed_rules", failed_rules_col)
        .withColumn("_dq_status",
                    when(size(col("_dq_failed_rules")) > 0, lit("QUARANTINE"))
                    .otherwise(lit("PASS")))
    )
```

**Why flag instead of drop:**
- Quarantined rows are auditable — analysts can query `WHERE _dq_status = 'QUARANTINE'` to understand data quality issues.
- Gold simply ignores them via `WHERE _dq_status = 'PASS'` — no Silver reprocess needed if Gold DQ rules change.
- Late arrivals are flagged with `is_late_arrival=True` (not quarantined) — they are valid events that simply arrived in the wrong batch.

---

## 10. ETL Metadata — `dbruntime.databricks_repl_context`

**Problem:** How to resolve job_id, run_id, and notebook_path reliably across all cluster modes?

**Decision:** Use `dbruntime.databricks_repl_context.get_context()` — the Python-native API (DBR 14.1+).

```python
from dbruntime.databricks_repl_context import get_context
_ctx = get_context()
job_run_id = f"{_ctx.jobId or 'INTERACTIVE'}_{_ctx.currentRunId or 'INTERACTIVE'}"
```

**Why NOT `dbutils.notebook.entry_point`:** Raises `Py4JSecurityException` on Unity Catalog Shared Access Mode clusters. The `dbruntime` API works on all cluster modes.

**Resolved once at notebook startup** (not inside loops) — all rows in a batch share identical job-level metadata.

---

## 11. INNER JOINs as Implicit FK Validation

**Pattern used in:** `bridge_movies_genres`, `fact_ratings`, `fact_genome_scores`

When Gold joins Silver data to dimension tables to resolve surrogate keys, INNER JOIN is used (not LEFT JOIN).

**Effect:** Any Silver record whose natural key has no matching dimension record is automatically excluded. This is intentional — it means dimension tables were built from quarantined Silver data. The orphan is the anomaly, not the JOIN behavior.

**Example in bridge_movies_genres:**
```python
# Step 1: Resolve movie_sk — orphan movie_ids silently excluded
df_with_movie_sk = df_exploded.join(df_dim_movies, on="movie_id", how="inner")

# Step 2: Resolve genre_sk — unknown genre strings silently excluded
df_with_genre_sk = df_with_movie_sk.join(broadcast(df_dim_genres), on="genre_name", how="inner")
```

This eliminates separate orphan-removal steps and makes FK validation a natural consequence of the transformation.

---

## 12. Late Arrival — Three-Layer Handling

| Layer | What It Does | Why |
|-------|-------------|-----|
| **Bronze** | `detect_cross_year_records()` — logs WARNING | Earliest observability point. Never fails the job — Bronze's contract is raw fidelity. |
| **Silver** | `is_late_arrival = (event_year != _batch_year)` flag + MERGE to correct `rating_year` partition | Marks and routes the record. MERGE ensures 2019 events land in `rating_year=2019` without disturbing existing data. |
| **Gold** | `replaceWhere` per `rating_year` — reads Silver as-is | Silver already routed late arrivals correctly. Gold simply reads and writes per partition. |

---

## 13. Broadcast Joins for Small Dimensions

`dim_genres` (~20 rows) and `dim_date` when referenced from fact tables are always broadcast-joined:

```python
df.join(broadcast(df_dim_genres), on="genre_name", how="inner")
```

`dim_movies` is also broadcast in fact_ratings loading (smaller than Silver ratings, fits in memory).

---

## 14. Schema Inference is Never Used

All Bronze reads use explicit `StructType` schemas:

```python
schemas = {
    "ratings": StructType([
        StructField("userId",    IntegerType(), True),
        StructField("movieId",   IntegerType(), True),
        StructField("rating",    DoubleType(),  True),
        StructField("timestamp", LongType(),    True),
    ]),
    ...
}
df = spark.read.format("csv").schema(schemas[table_name]).load(source_file)
```

**Why:** Schema inference reads the file twice (once to infer, once to read) and can mistype columns (e.g., treating a ZIP code as INT). Explicit schemas also serve as documentation and catch upstream schema drift immediately.

**`imdbId` as STRING:** A deliberate choice to preserve leading zeros in IMDB IDs. Numeric cast would silently corrupt them.

---

## 15. S3 File Discovery for Incremental Tables

Year discovery for incremental tables (`ratings`, `tags`) is driven by listing S3 files, not hardcoded year ranges:

```python
def discover_s3_years(s3_source_path, table_name):
    file_pattern = re.compile(rf"^{re.escape(table_name)}_(\d{{4}})\.csv$")
    s3_files = dbutils.fs.ls(s3_source_path)
    available_years = {int(m.group(1)) for f in s3_files if (m := file_pattern.match(f.name))}
```

**S3 listing is the source of truth** for what's available. No manual year range widgets. New yearly files are automatically picked up on next run.

**Fail hard if no files found:** Listing succeeds but no files match → `RuntimeError`. This catches misconfigured paths early, before any Spark work.

---

## 16. Maintenance Decoupled from ETL

**Problem:** Running OPTIMIZE, ANALYZE TABLE, Z-ORDER BY, and VACUUM inline after every write (a) slows down ETL pipelines, (b) wastes compute when small files accumulate between compactions, and (c) prevents scheduling maintenance during off-peak hours.

**Solution:** All maintenance commands are handled by `maintenance/table_maintenance.py`, a dedicated Databricks notebook scheduled separately from ETL.

**Architecture:**
- `maintenance/maintenance_utils.py` contains a **table registry** (`MAINTENANCE_CONFIG`) defining per-table maintenance commands, Z-ORDER columns, and VACUUM retention periods.
- `maintenance/table_maintenance.py` iterates across layers (Bronze, Silver, Gold) and runs the configured commands for each table.

**Per-layer maintenance rules:**

| Layer | OPTIMIZE | ANALYZE | Z-ORDER | VACUUM | Retention |
|-------|----------|---------|---------|--------|-----------|
| Bronze | ✓ | — | — | ✓ | 8760h (1 year) |
| Silver | ✓ | ✓ | Selected tables | ✓ | 168h (7 days) |
| Gold | ✓ | ✓ | Selected tables | ✓ | 168h (7 days) |

**Error isolation:** Each table is processed independently — a failure on one table does not block maintenance on others. The summary report shows per-table success/failure status.

**Why no ANALYZE on Bronze:** Bronze is raw ingestion, not queried by end users. Silver reads Bronze in full; optimizer statistics have minimal impact. Adding ANALYZE would add cost with no downstream benefit.

**Why no Z-ORDER on small tables:** Dimension tables like `dim_date`, `dim_genome_tags`, and `dim_external_links` are small enough that Z-ORDER provides no measurable data-skipping benefit. OPTIMIZE alone handles file compaction.
