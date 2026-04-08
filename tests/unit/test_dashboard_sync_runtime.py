import json
from pathlib import Path

import pytest

from dashboard.services.health import evaluate_sync_manifest
from dashboard.services import sync_data


pytestmark = [pytest.mark.unit]


class _FakeS3Client:
    def list_objects_v2(self, Bucket, Prefix):
        if "missing_kpi.parquet" in Prefix:
            return {}
        return {
            "Contents": [
                {
                    "Key": f"{Prefix}part-00000.snappy.parquet",
                    "ETag": '"etag-123"',
                }
            ]
        }

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).parent.mkdir(parents=True, exist_ok=True)
        Path(Filename).write_bytes(b"parquet-bytes")


def test_sync_writes_failed_manifest_and_nonzero_on_missing_required_kpi(monkeypatch, tmp_path):
    manifest_path = tmp_path / ".sync_manifest.json"

    monkeypatch.setattr(sync_data, "LOCAL_DIR", str(tmp_path))
    monkeypatch.setattr(sync_data, "MANIFEST", str(manifest_path))
    monkeypatch.setattr(sync_data, "KPI_DIRS", ["ok_kpi.parquet", "missing_kpi.parquet"])
    monkeypatch.setattr(sync_data.boto3, "client", lambda _: _FakeS3Client())

    exit_code = sync_data.sync()

    assert exit_code == 1
    payload = json.loads(manifest_path.read_text())
    assert payload["status"] == "failed"
    assert payload["last_sync"]
    assert payload["bucket"] == sync_data.S3_BUCKET
    assert payload["prefix"] == sync_data.S3_PREFIX
    assert "ok_kpi.parquet" in payload["synced_kpis"]
    assert any("missing_kpi.parquet" in err for err in payload["errors"])


def test_sync_health_evaluator_success_and_failure_messages():
    success_kind, success_message = evaluate_sync_manifest(
        {"status": "success", "last_sync": "2026-04-08T10:00:00+00:00"}
    )
    assert success_kind == "success"
    assert "Live" in success_message

    failed_kind, failed_message = evaluate_sync_manifest(
        {
            "status": "failed",
            "last_sync": "2026-04-08T10:00:00+00:00",
            "errors": ["rating_distribution.parquet: not found on S3"],
        }
    )
    assert failed_kind == "warning"
    assert "degraded" in failed_message
    assert "rating_distribution.parquet" in failed_message
