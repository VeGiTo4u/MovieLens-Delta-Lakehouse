"""
sync_data.py — S3 → Local Parquet Sync

Downloads the latest Parquet files from S3 to the local data/ folder.
Handles Spark's directory-style Parquet output (each KPI is a folder
containing part-*.parquet files).

Run this before launching the Streamlit app to ensure fresh data.

Usage:
    python sync_data.py

Requires:
    - boto3 installed
    - AWS credentials configured (via env vars, ~/.aws/credentials, or IAM role)

Environment variables (optional overrides):
    S3_BUCKET    — default: movielens-data-store
    S3_PREFIX    — default: analytics/
    LOCAL_DIR    — default: data/
"""

import os
import json
import boto3
from datetime import datetime, timezone

S3_BUCKET = os.environ.get("S3_BUCKET", "movielens-data-store")
S3_PREFIX = os.environ.get("S3_PREFIX", "analytics/")
LOCAL_DIR = os.environ.get("LOCAL_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
MANIFEST  = os.path.join(LOCAL_DIR, ".sync_manifest.json")

# Expected KPI directories (Spark writes each as a folder)
KPI_DIRS = [
    "all_time_summary.parquet",
    "rating_trends_monthly.parquet",
    "genre_performance.parquet",
    "top_rated_movies.parquet",
    "most_popular_movies.parquet",
    "genre_trends_yearly.parquet",
    "user_activity_distribution.parquet",
    "rating_distribution.parquet",
    "yearly_summary.parquet",
    "release_decade_analysis.parquet",
    "top_genome_tags.parquet",
]


def load_manifest() -> dict:
    """Load the sync manifest — tracks ETag of each downloaded file."""
    if os.path.exists(MANIFEST):
        with open(MANIFEST, "r") as f:
            return json.load(f)
    return {}


def save_manifest(manifest: dict):
    """Persist the sync manifest."""
    with open(MANIFEST, "w") as f:
        json.dump(manifest, f, indent=2)


def sync():
    """
    Download new/changed Parquet files from S3.

    Spark writes each KPI as a directory:
        s3://bucket/analytics/rating_trends_monthly.parquet/
            ├── part-00000-....snappy.parquet
            ├── _SUCCESS
            └── _committed_...

    This script downloads the part-*.parquet files into:
        data/rating_trends_monthly.parquet/part-00000-....snappy.parquet

    DuckDB can read these directories natively via:
        SELECT * FROM read_parquet('data/rating_trends_monthly.parquet/*.parquet')
    """
    os.makedirs(LOCAL_DIR, exist_ok=True)
    manifest = load_manifest()

    s3 = boto3.client("s3")
    print(f"[SYNC] Bucket : s3://{S3_BUCKET}/{S3_PREFIX}")
    print(f"[SYNC] Local  : {LOCAL_DIR}")
    print()

    downloaded = 0
    skipped = 0
    errors = 0

    for kpi_dir in KPI_DIRS:
        s3_prefix = f"{S3_PREFIX}{kpi_dir}/"
        local_kpi_dir = os.path.join(LOCAL_DIR, kpi_dir)
        os.makedirs(local_kpi_dir, exist_ok=True)

        # List objects in this KPI directory
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        except Exception as e:
            print(f"  ✗ {kpi_dir} — S3 error: {e}")
            errors += 1
            continue

        if "Contents" not in response:
            print(f"  ✗ {kpi_dir} — not found on S3")
            errors += 1
            continue

        # Download only actual parquet data files (skip _SUCCESS, _committed_, etc.)
        parquet_files = [
            obj for obj in response["Contents"]
            if obj["Key"].endswith(".parquet") and "/part-" in obj["Key"]
        ]

        if not parquet_files:
            print(f"  ✗ {kpi_dir} — no part files found")
            errors += 1
            continue

        kpi_downloaded = 0
        for obj in parquet_files:
            key = obj["Key"]
            filename = os.path.basename(key)
            local_path = os.path.join(local_kpi_dir, filename)
            etag = obj["ETag"]
            manifest_key = f"{kpi_dir}/{filename}"

            # Skip if ETag matches — file hasn't changed on S3
            if manifest.get(manifest_key) == etag and os.path.exists(local_path):
                skipped += 1
                continue

            s3.download_file(S3_BUCKET, key, local_path)
            manifest[manifest_key] = etag
            downloaded += 1
            kpi_downloaded += 1

        status = f"↓ {kpi_downloaded} file(s)" if kpi_downloaded > 0 else "✓ up to date"
        print(f"  {status} — {kpi_dir}")

    save_manifest(manifest)

    print(f"\n[SYNC] Done — {downloaded} downloaded, {skipped} unchanged, {errors} errors")
    print(f"[SYNC] Timestamp: {datetime.now(timezone.utc).isoformat()}")

    if errors > 0:
        print(f"\n[WARN] {errors} KPI(s) missing on S3. Have you run kpi_export.py on Databricks?")


if __name__ == "__main__":
    sync()
