import pytest

from scripts.maintenance import utils as maintenance_utils


pytestmark = [pytest.mark.unit]


def test_layer_maintenance_dispatches_only_registered_tables(monkeypatch):
    """Commands must run only for tables present in the maintenance registry."""
    custom_registry = {
        "silver": {
            "ratings": {
                "optimize": True,
                "analyze": True,
                "vacuum": True,
                "vacuum_hours": 168,
                "zorder_cols": ["user_id", "movie_id"],
            },
            "tags": {
                "optimize": True,
                "analyze": False,
                "vacuum": True,
                "vacuum_hours": 168,
            },
        }
    }
    dispatched_ops = []

    def _record_optimize(full_table_name, zorder_cols=None):
        dispatched_ops.append(("OPTIMIZE", full_table_name))
        return {"status": "SUCCESS", "operation": "OPTIMIZE"}

    def _record_analyze(full_table_name):
        dispatched_ops.append(("ANALYZE TABLE", full_table_name))
        return {"status": "SUCCESS", "operation": "ANALYZE TABLE"}

    def _record_vacuum(full_table_name, retention_hours, break_glass=False, break_glass_reason=""):
        dispatched_ops.append(("VACUUM", full_table_name))
        return {"status": "SUCCESS", "operation": "VACUUM"}

    monkeypatch.setattr(maintenance_utils, "MAINTENANCE_CONFIG", custom_registry)
    monkeypatch.setattr(maintenance_utils, "run_optimize", _record_optimize)
    monkeypatch.setattr(maintenance_utils, "run_analyze", _record_analyze)
    monkeypatch.setattr(maintenance_utils, "run_vacuum", _record_vacuum)

    layer_results = maintenance_utils.run_layer_maintenance(catalog="movielens", layer="silver")

    assert [result["table_name"] for result in layer_results] == ["ratings", "tags"]
    assert dispatched_ops == [
        ("OPTIMIZE", "movielens.silver.ratings"),
        ("ANALYZE TABLE", "movielens.silver.ratings"),
        ("VACUUM", "movielens.silver.ratings"),
        ("OPTIMIZE", "movielens.silver.tags"),
        ("VACUUM", "movielens.silver.tags"),
    ]
    assert all(table in {"movielens.silver.ratings", "movielens.silver.tags"} for _, table in dispatched_ops)


def test_vacuum_guardrail_rejects_unsafe_retention_without_break_glass():
    with pytest.raises(ValueError, match="safety floor"):
        maintenance_utils.run_vacuum(
            full_table_name="movielens.silver.ratings",
            retention_hours=24,
            break_glass=False,
            break_glass_reason="",
        )


def test_vacuum_guardrail_requires_reason_when_break_glass():
    with pytest.raises(ValueError, match="break_glass_reason is required"):
        maintenance_utils.run_vacuum(
            full_table_name="movielens.silver.ratings",
            retention_hours=24,
            break_glass=True,
            break_glass_reason="   ",
        )
