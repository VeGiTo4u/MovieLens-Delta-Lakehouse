"""
data_loader.py — DuckDB-Powered Data Layer

All Parquet reads go through this module. Each function is cached
with @st.cache_data so data is loaded once per session regardless
of how many times pages re-render.

Spark writes each KPI as a directory with part-*.parquet files.
DuckDB reads them via glob: read_parquet('dir/*.parquet')
"""

import os
import streamlit as st
import duckdb

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def _query(kpi_dir: str) -> "pd.DataFrame":
    """
    Run a DuckDB query on a Spark-style Parquet directory.
    Spark writes: kpi_dir/part-00000-....snappy.parquet
    DuckDB reads: kpi_dir/*.parquet (glob)
    """
    dir_path = os.path.join(DATA_DIR, kpi_dir)

    if not os.path.exists(dir_path):
        st.error(f"Data not found: `{kpi_dir}`. Run `python sync_data.py` first.")
        st.stop()

    # Check for Parquet files inside the directory
    parquet_files = [f for f in os.listdir(dir_path) if f.endswith(".parquet")]
    if not parquet_files:
        st.error(f"No Parquet files in `{kpi_dir}/`. Re-run `python sync_data.py`.")
        st.stop()

    glob_path = os.path.join(dir_path, "*.parquet")
    return duckdb.query(f"SELECT * FROM read_parquet('{glob_path}')").to_df()


@st.cache_data(ttl=300)
def load_rating_trends_monthly():
    return _query("rating_trends_monthly.parquet")


@st.cache_data(ttl=300)
def load_genre_performance():
    return _query("genre_performance.parquet")


@st.cache_data(ttl=300)
def load_top_rated_movies():
    return _query("top_rated_movies.parquet")


@st.cache_data(ttl=300)
def load_most_popular_movies():
    return _query("most_popular_movies.parquet")


@st.cache_data(ttl=300)
def load_genre_trends_yearly():
    return _query("genre_trends_yearly.parquet")


@st.cache_data(ttl=300)
def load_user_activity_distribution():
    return _query("user_activity_distribution.parquet")


@st.cache_data(ttl=300)
def load_rating_distribution():
    return _query("rating_distribution.parquet")


@st.cache_data(ttl=300)
def load_yearly_summary():
    return _query("yearly_summary.parquet")


@st.cache_data(ttl=300)
def load_release_decade_analysis():
    return _query("release_decade_analysis.parquet")


@st.cache_data(ttl=300)
def load_top_genome_tags():
    return _query("top_genome_tags.parquet")


@st.cache_data(ttl=300)
def load_all_time_summary():
    return _query("all_time_summary.parquet")
