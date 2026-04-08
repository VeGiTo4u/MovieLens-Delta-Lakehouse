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
LOCAL_DIR = os.environ.get("LOCAL_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"))
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
    """
    Load the sync manifest.

    Manifest contract:
      last_sync, status, errors, synced_kpis, bucket, prefix, files
    """
    if os.path.exists(MANIFEST):
        with open(MANIFEST, "r") as f:
            payload = json.load(f)

        # Backward compatibility: older manifests were file->etag maps.
        if "files" not in payload:
            return {
                "last_sync": payload.get("last_sync"),
                "status": payload.get("status", "unknown"),
                "errors": payload.get("errors", []),
                "synced_kpis": payload.get("synced_kpis", []),
                "bucket": payload.get("bucket", S3_BUCKET),
                "prefix": payload.get("prefix", S3_PREFIX),
                "files": {k: v for k, v in payload.items() if k.endswith(".parquet")},
            }
        return payload

    return {
        "last_sync": None,
        "status": "never_run",
        "errors": [],
        "synced_kpis": [],
        "bucket": S3_BUCKET,
        "prefix": S3_PREFIX,
        "files": {},
    }


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
    file_etags = dict(manifest.get("files", {}))

    s3 = boto3.client("s3")
    print(f"[SYNC] Bucket : s3://{S3_BUCKET}/{S3_PREFIX}")
    print(f"[SYNC] Local  : {LOCAL_DIR}")
    print()

    downloaded = 0
    skipped = 0
    errors = []
    synced_kpis = []

    for kpi_dir in KPI_DIRS:
        s3_prefix = f"{S3_PREFIX}{kpi_dir}/"
        local_kpi_dir = os.path.join(LOCAL_DIR, kpi_dir)
        os.makedirs(local_kpi_dir, exist_ok=True)

        # List objects in this KPI directory
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        except Exception as e:
            print(f"  ✗ {kpi_dir} — S3 error: {e}")
            errors.append(f"{kpi_dir}: S3 error: {e}")
            continue

        if "Contents" not in response:
            print(f"  ✗ {kpi_dir} — not found on S3")
            errors.append(f"{kpi_dir}: not found on S3")
            continue

        # Download only actual parquet data files (skip _SUCCESS, _committed_, etc.)
        parquet_files = [
            obj for obj in response["Contents"]
            if obj["Key"].endswith(".parquet") and "/part-" in obj["Key"]
        ]

        if not parquet_files:
            print(f"  ✗ {kpi_dir} — no part files found")
            errors.append(f"{kpi_dir}: no part files found")
            continue

        kpi_downloaded = 0
        valid_filenames = set()
        
        for obj in parquet_files:
            key = obj["Key"]
            filename = os.path.basename(key)
            valid_filenames.add(filename)
            local_path = os.path.join(local_kpi_dir, filename)
            etag = obj["ETag"]
            manifest_key = f"{kpi_dir}/{filename}"

            # Skip if ETag matches — file hasn't changed on S3
            if file_etags.get(manifest_key) == etag and os.path.exists(local_path):
                skipped += 1
                continue

            s3.download_file(S3_BUCKET, key, local_path)
            file_etags[manifest_key] = etag
            downloaded += 1
            kpi_downloaded += 1

        # Clean up stale local parquet files (from older Spark runs)
        for local_file in os.listdir(local_kpi_dir):
            if local_file.endswith(".parquet") and local_file not in valid_filenames:
                os.remove(os.path.join(local_kpi_dir, local_file))
                stale_manifest_key = f"{kpi_dir}/{local_file}"
                file_etags.pop(stale_manifest_key, None)

        status = f"↓ {kpi_downloaded} file(s)" if kpi_downloaded > 0 else "✓ up to date"
        print(f"  {status} — {kpi_dir}")
        synced_kpis.append(kpi_dir)

    sync_status = "success" if len(errors) == 0 else "failed"
    manifest_payload = {
        "last_sync": datetime.now(timezone.utc).isoformat(),
        "status": sync_status,
        "errors": errors,
        "synced_kpis": synced_kpis,
        "bucket": S3_BUCKET,
        "prefix": S3_PREFIX,
        "files": file_etags,
    }
    save_manifest(manifest_payload)

    print(f"\n[SYNC] Done — {downloaded} downloaded, {skipped} unchanged, {len(errors)} errors")
    print(f"[SYNC] Timestamp: {manifest_payload['last_sync']}")

    if errors:
        print("\n[FAIL] Sync completed with errors:")
        for err in errors:
            print(f"  - {err}")
        print("\n[WARN] Required KPI(s) missing or unreadable. Have you run kpi_export.py on Databricks?")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(sync())
