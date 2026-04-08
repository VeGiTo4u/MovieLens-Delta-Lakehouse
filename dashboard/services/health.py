from __future__ import annotations


def evaluate_sync_manifest(manifest: dict) -> tuple[str, str]:
    """
    Map sync manifest payload to a dashboard callout kind/message.

    Returns:
        (kind, message) where kind is one of: success, warning
    """
    last_sync = manifest.get("last_sync", "unknown")
    status = manifest.get("status", "unknown")

    if status == "success":
        return (
            "success",
            f"Pipeline last synced: <strong>{last_sync}</strong> — "
            f"<span class='status-pill'>Live</span>",
        )

    errors = manifest.get("errors", [])
    error_preview = "<br>".join(errors[:2]) if errors else "Unknown sync failure"
    return (
        "warning",
        f"Pipeline sync degraded (status: <strong>{status}</strong>). "
        f"Last attempt: <strong>{last_sync}</strong>.<br>{error_preview}",
    )
