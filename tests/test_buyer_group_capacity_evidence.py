"""The execution GO is bounded by actual complete, ordered source probes."""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "artifacts/audits/buyer_group_execution_v1"


@pytest.mark.parametrize("path", sorted(REPORTS.glob("*-*.json")), ids=lambda p: p.stem)
def test_group_probe_proves_its_declared_scope(path):
    report = json.loads(path.read_text())
    assert report["status"] == "PASS"
    assert report["counts_before"] == report["counts_after"]
    assert report["materialized_physical_rows"] == report["batch_physical_rows"]
    assert report["materialized_physical_rows"] == int(report["counts_before"]["physical_rows"])
    assert report["materialized_unique_ids"] == int(report["counts_before"]["unique_ids"])
    assert report["unique_physical_ids"] == report["materialized_physical_rows"]
    assert report["page_boundary_equal"] and not report["wrong_names"]
    assert not report["unstable_rows"]
    assert len([c for c in report["calls"] if c["tag"].startswith(("all_time_", "2026_"))]) == 12
    for call in report["calls"]:
        assert call["status"] == "PASS" and call["latency_seconds"] < report["timeout_seconds"]
        if "ORDER BY" in call["soql"]:
            assert ":id DESC" in call["soql"] and call["ordering"] == "total"
        assert "nit_entidad" not in call["soql"]


def test_capacity_evidence_covers_all_eight_cases():
    summary = json.loads((REPORTS / "summary.json").read_text())
    assert summary["status"] == "PASS" and len(summary["cases"]) == 8
    assert summary["full_materialization_scope"] == ["2026-08-01", "2026-08-08"]
