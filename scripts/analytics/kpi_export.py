# Databricks notebook source
# ============================================================
# kpi_export.py
# Standalone analytics notebook — computes 10 KPIs from Gold
# layer tables and saves each as a Parquet file for downstream
# Streamlit dashboards powered by DuckDB.
#
# Output path: s3://movielens-data-store/analytics/
#
# This notebook has NO dependency on gold_utils.py.
# It reads directly from the movielens.gold.* Unity Catalog tables.
#
# Usage:
#   Set the "catalog_name" widget (default: movielens) and run.
# ============================================================

# COMMAND ----------

# ------------------------------------------------------------
# Widget
# ------------------------------------------------------------
dbutils.widgets.text("catalog_name", "movielens", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# ------------------------------------------------------------
# Imports + Output Path Setup
# ------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

OUTPUT_DIR = "s3://movielens-data-store/analytics"

print(f"[INFO] Catalog    : {catalog_name}")
print(f"[INFO] Output dir : {OUTPUT_DIR}")

# COMMAND ----------

# ------------------------------------------------------------
# Load Gold Tables
# ------------------------------------------------------------
print("[START] Loading Gold layer tables")

fact_ratings     = spark.table(f"{catalog_name}.gold.fact_ratings")
dim_movies       = spark.table(f"{catalog_name}.gold.dim_movies")
dim_genres       = spark.table(f"{catalog_name}.gold.dim_genres")
bridge_mg        = spark.table(f"{catalog_name}.gold.bridge_movies_genres")
dim_date         = spark.table(f"{catalog_name}.gold.dim_date")
fact_gs          = spark.table(f"{catalog_name}.gold.fact_genome_scores")
dim_genome_tags  = spark.table(f"{catalog_name}.gold.dim_genome_tags")

print("[DONE] All Gold tables loaded")

# COMMAND ----------

# ============================================================
# KPI 1 — Monthly Rating Trends
# Line chart: avg rating + volume over time
# ============================================================
print("[KPI 1] Monthly Rating Trends")

kpi_1 = (
    fact_ratings
    .join(dim_date, on="date_key", how="inner")
    .groupBy("year", "month", "month_name")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("rating_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("movie_sk").alias("unique_movies"),
    )
    .orderBy("year", "month")
)

kpi_1_count = kpi_1.count()
kpi_1.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/rating_trends_monthly.parquet")
print(f"[DONE] rating_trends_monthly.parquet — {kpi_1_count} rows")

# COMMAND ----------

# ============================================================
# KPI 2 — Genre Performance Summary
# Bar/radar chart: avg rating, volume, breadth per genre
# ============================================================
print("[KPI 2] Genre Performance Summary")

kpi_2 = (
    fact_ratings
    .join(bridge_mg, on="movie_sk", how="inner")
    .join(dim_genres, on="genre_sk", how="inner")
    .groupBy("genre_name")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("rating_count"),
        F.countDistinct("movie_sk").alias("unique_movies"),
        F.countDistinct("user_id").alias("unique_users"),
    )
    .orderBy(F.desc("rating_count"))
)

kpi_2_count = kpi_2.count()
kpi_2.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/genre_performance.parquet")
print(f"[DONE] genre_performance.parquet — {kpi_2_count} rows")

# COMMAND ----------

# ============================================================
# KPI 3 — Top 30 Highest Rated Movies (min 100 ratings)
# Table / bar chart: quality ranking with threshold
# ============================================================
print("[KPI 3] Top 30 Highest Rated Movies")

movie_stats = (
    fact_ratings
    .groupBy("movie_sk")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("rating_count"),
    )
)

kpi_3 = (
    movie_stats
    .filter(F.col("rating_count") >= 100)
    .join(dim_movies.select("movie_sk", "movie_id", "title", "release_year"),
          on="movie_sk", how="inner")
    .select("movie_id", "title", "release_year", "avg_rating", "rating_count")
    .orderBy(F.desc("avg_rating"))
    .limit(30)
)

kpi_3_count = kpi_3.count()
kpi_3.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/top_rated_movies.parquet")
print(f"[DONE] top_rated_movies.parquet — {kpi_3_count} rows")

# COMMAND ----------

# ============================================================
# KPI 4 — Top 30 Most Popular (Most Rated) Movies
# Bar chart: engagement / popularity ranking
# ============================================================
print("[KPI 4] Top 30 Most Popular Movies")

kpi_4 = (
    movie_stats
    .join(dim_movies.select("movie_sk", "movie_id", "title", "release_year"),
          on="movie_sk", how="inner")
    .select("movie_id", "title", "release_year", "avg_rating", "rating_count")
    .orderBy(F.desc("rating_count"))
    .limit(30)
)

kpi_4_count = kpi_4.count()
kpi_4.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/most_popular_movies.parquet")
print(f"[DONE] most_popular_movies.parquet — {kpi_4_count} rows")

# COMMAND ----------

# ============================================================
# KPI 5 — Genre Popularity Over Time (Yearly)
# Stacked area / heatmap: genre evolution year by year
# ============================================================
print("[KPI 5] Genre Trends Yearly")

kpi_5 = (
    fact_ratings
    .join(dim_date.select("date_key", "year"), on="date_key", how="inner")
    .join(bridge_mg, on="movie_sk", how="inner")
    .join(dim_genres, on="genre_sk", how="inner")
    .groupBy("year", "genre_name")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("rating_count"),
    )
    .orderBy("year", "genre_name")
)

kpi_5_count = kpi_5.count()
kpi_5.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/genre_trends_yearly.parquet")
print(f"[DONE] genre_trends_yearly.parquet — {kpi_5_count} rows")

# COMMAND ----------

# ============================================================
# KPI 6 — User Activity Distribution
# Bar/pie chart: how engaged are users? (bucketed)
# ============================================================
print("[KPI 6] User Activity Distribution")

user_counts = (
    fact_ratings
    .groupBy("user_id")
    .agg(F.count("*").alias("rating_count"))
)

kpi_6 = (
    user_counts
    .withColumn(
        "activity_bucket",
        F.when(F.col("rating_count") <= 10,   F.lit("01 — 1-10"))
         .when(F.col("rating_count") <= 50,   F.lit("02 — 11-50"))
         .when(F.col("rating_count") <= 200,  F.lit("03 — 51-200"))
         .when(F.col("rating_count") <= 500,  F.lit("04 — 201-500"))
         .when(F.col("rating_count") <= 1000, F.lit("05 — 501-1000"))
         .otherwise(                          F.lit("06 — 1000+"))
    )
    .groupBy("activity_bucket")
    .agg(F.count("*").alias("user_count"))
    .withColumn(
        "pct_of_users",
        F.round(F.col("user_count") * 100.0 / user_counts.count(), 2)
    )
    .orderBy("activity_bucket")
)

kpi_6_count = kpi_6.count()
kpi_6.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/user_activity_distribution.parquet")
print(f"[DONE] user_activity_distribution.parquet — {kpi_6_count} rows")

# COMMAND ----------

# ============================================================
# KPI 7 — Rating Value Distribution
# Bar chart: classic 0.5–5.0 histogram
# ============================================================
print("[KPI 7] Rating Value Distribution")

total_ratings = fact_ratings.count()

kpi_7 = (
    fact_ratings
    .groupBy("rating")
    .agg(F.count("*").alias("rating_count"))
    .withColumn(
        "pct_of_total",
        F.round(F.col("rating_count") * 100.0 / total_ratings, 2)
    )
    .orderBy("rating")
)

kpi_7_count = kpi_7.count()
kpi_7.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/rating_distribution.parquet")
print(f"[DONE] rating_distribution.parquet — {kpi_7_count} rows")

# COMMAND ----------

# ============================================================
# KPI 8 — Year-over-Year Summary Statistics
# KPI cards / summary table: executive-level overview
# ============================================================
print("[KPI 8] Yearly Summary Statistics")

kpi_8 = (
    fact_ratings
    .withColumn("year", F.year("interaction_timestamp"))
    .groupBy("year")
    .agg(
        F.count("*").alias("total_ratings"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("movie_sk").alias("unique_movies"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("is_late_arrival") == True, 1).otherwise(0)).alias("late_arrival_count"),
    )
    .withColumn(
        "late_arrival_pct",
        F.round(F.col("late_arrival_count") * 100.0 / F.col("total_ratings"), 3)
    )
    .orderBy("year")
)

kpi_8_count = kpi_8.count()
kpi_8.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/yearly_summary.parquet")
print(f"[DONE] yearly_summary.parquet — {kpi_8_count} rows")

# COMMAND ----------

# ============================================================
# KPI 9 — Movie Release Decade Analysis
# Bar chart: how does movie era affect ratings?
# ============================================================
print("[KPI 9] Release Decade Analysis")

kpi_9 = (
    fact_ratings
    .join(dim_movies.select("movie_sk", "release_year"),
          on="movie_sk", how="inner")
    .filter(F.col("release_year").isNotNull())
    .withColumn(
        "decade",
        F.concat(
            (F.floor(F.col("release_year") / 10) * 10).cast("string"),
            F.lit("s")
        )
    )
    .groupBy("decade")
    .agg(
        F.countDistinct("movie_sk").alias("movie_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("total_ratings"),
        F.countDistinct("user_id").alias("unique_users"),
    )
    .orderBy("decade")
)

kpi_9_count = kpi_9.count()
kpi_9.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/release_decade_analysis.parquet")
print(f"[DONE] release_decade_analysis.parquet — {kpi_9_count} rows")

# COMMAND ----------

# ============================================================
# KPI 10 — Top 20 Genome Tags by Average Relevance
# Horizontal bar: content DNA — what tags define movies?
# ============================================================
print("[KPI 10] Top 20 Genome Tags")

kpi_10 = (
    fact_gs
    .join(dim_genome_tags, on="tag_sk", how="inner")
    .groupBy("tag")
    .agg(
        F.round(F.avg("relevance"), 4).alias("avg_relevance"),
        F.countDistinct("movie_sk").alias("movie_count"),
    )
    .orderBy(F.desc("avg_relevance"))
    .limit(20)
)

kpi_10_count = kpi_10.count()
kpi_10.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/top_genome_tags.parquet")
print(f"[DONE] top_genome_tags.parquet — {kpi_10_count} rows")

# COMMAND ----------

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("  KPI EXPORT COMPLETE")
print("=" * 60)
print(f"  Output directory : {OUTPUT_DIR}")
print(f"  Catalog used     : {catalog_name}")
print("  Files written    :")
print(f"    1. rating_trends_monthly.parquet    — {kpi_1_count:,} rows")
print(f"    2. genre_performance.parquet        — {kpi_2_count:,} rows")
print(f"    3. top_rated_movies.parquet         — {kpi_3_count:,} rows")
print(f"    4. most_popular_movies.parquet      — {kpi_4_count:,} rows")
print(f"    5. genre_trends_yearly.parquet      — {kpi_5_count:,} rows")
print(f"    6. user_activity_distribution.parquet — {kpi_6_count:,} rows")
print(f"    7. rating_distribution.parquet      — {kpi_7_count:,} rows")
print(f"    8. yearly_summary.parquet           — {kpi_8_count:,} rows")
print(f"    9. release_decade_analysis.parquet  — {kpi_9_count:,} rows")
print(f"   10. top_genome_tags.parquet          — {kpi_10_count:,} rows")
print("=" * 60)
print("\n[INFO] Output stored at S3:")
print(f"  {OUTPUT_DIR}/<filename>.parquet")
print("[INFO] Query locally with DuckDB:")
print("  SELECT * FROM read_parquet('rating_trends_monthly.parquet');")