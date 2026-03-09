# KPI Reference — MovieLens Analytics Export

> Output path: `/dbfs/FileStore/dashboard/movielens_analytics/`
> Script: `analytics/kpi_export.py`

---

## Parquet Files

| # | File | Description | Key Columns | Suggested Chart |
|---|------|-------------|-------------|-----------------|
| 1 | `rating_trends_monthly.parquet` | Avg rating + volume per month | `year`, `month`, `month_name`, `avg_rating`, `rating_count`, `unique_users`, `unique_movies` | Line chart |
| 2 | `genre_performance.parquet` | Rating stats per genre | `genre_name`, `avg_rating`, `rating_count`, `unique_movies`, `unique_users` | Bar / radar |
| 3 | `top_rated_movies.parquet` | Top 30 by avg rating (≥100 ratings) | `movie_id`, `title`, `release_year`, `avg_rating`, `rating_count` | Table / bar |
| 4 | `most_popular_movies.parquet` | Top 30 by rating count | `movie_id`, `title`, `release_year`, `avg_rating`, `rating_count` | Bar chart |
| 5 | `genre_trends_yearly.parquet` | Genre popularity by year | `year`, `genre_name`, `avg_rating`, `rating_count` | Stacked area / heatmap |
| 6 | `user_activity_distribution.parquet` | Users bucketed by activity level | `activity_bucket`, `user_count`, `pct_of_users` | Bar / pie |
| 7 | `rating_distribution.parquet` | Count per rating value (0.5–5.0) | `rating`, `rating_count`, `pct_of_total` | Bar chart |
| 8 | `yearly_summary.parquet` | Year-over-year key metrics | `year`, `total_ratings`, `unique_users`, `unique_movies`, `avg_rating`, `late_arrival_count`, `late_arrival_pct` | KPI cards / table |
| 9 | `release_decade_analysis.parquet` | Ratings grouped by movie release decade | `decade`, `movie_count`, `avg_rating`, `total_ratings`, `unique_users` | Grouped bar |
| 10 | `top_genome_tags.parquet` | Top 20 tags by avg relevance | `tag`, `avg_relevance`, `movie_count` | Horizontal bar |

---

## Running

```
# On Databricks — set widget and run:
catalog_name = movielens   (default)
```

## Downloading Parquet Files

```
https://<workspace>.databricks.com/files/dashboard/movielens_analytics/<filename>.parquet
```

## Querying with DuckDB (Streamlit)

```sql
SELECT * FROM read_parquet('rating_trends_monthly.parquet');
```
