"""Tests for B3 — plan_query node inserted as no-op pass-through."""

import json
from pathlib import Path
import re
import subprocess
import sys
import uuid

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def _wf():
    wf = SecopalWorkflow(Settings())
    # Prevent SECOP calls — we only care about state through plan_query
    wf.secop_client.query = lambda ds, soql: [{
        "precio_base": "1000000",
        "fecha_de_publicacion_del": "2026-01-01",
    }]
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    return wf


def test_plan_query_attaches_plan_to_state():
    wf = _wf()
    result = wf.run_query(
        "contratos de mantenimiento en bolívar", chat_id="test_b3"
    )
    assert "query_plan" in result
    assert isinstance(result["query_plan"], dict)
    assert result["query_plan"]["should_execute"] is True


def test_query_plan_is_json_serializable():
    """Risk 5 of review: conversation_store persists state as JSONL.
    If query_plan were a raw dataclass, json.dumps() would break persistence."""
    wf = _wf()
    result = wf.run_query(
        "contratos de mantenimiento en bolívar", chat_id="test_b3_json"
    )
    serialized = json.dumps(result["query_plan"])
    assert "should_execute" in serialized


def test_b3_does_not_modify_parsed_params():
    """B3 was no-op. B4.1 activates P3 → 'ipiales' should now be extracted.
    This test validates the activation worked E2E."""
    wf = _wf()
    result = wf.run_query("diseño ipiales", chat_id="test_b41_active")
    plan = result.get("query_plan", {})
    # P3 is now active → ciudad must be extracted
    assert plan["params"].get("ciudad") == "Ipiales", (
        "B4.1 should extract Ipiales from objeto into ciudad"
    )


def test_state_only_builds_where_but_execution_requests_scope_or_topic():
    """Estado evita WHERE 1=1, pero no es base suficiente para ejecutar."""
    wf = _wf()
    wf.secop_client.query = lambda ds, soql: (_ for _ in ()).throw(
        AssertionError("SECOP query should not run without scope/topic")
    )
    wf.secop_client.count = lambda ds, soql: (_ for _ in ()).throw(
        AssertionError("SECOP count should not run without scope/topic")
    )

    result = wf.run_query(
        "contratos en ejecución",
        chat_id=f"test_state_only_guard_{uuid.uuid4().hex}",
    )

    # INV-STATE-FAMILY-PROJECTS-COMPLETE (HCA-STATE-007): la aserción original
    # fijaba el escalar truncado ``estado_contrato = 'En ejecución'``, que era
    # el defecto — ADR-003 define contrato_activo como los tres estados. Lo que
    # este test comprueba (hay WHERE de estado, pero no basta para ejecutar)
    # queda intacto.
    assert (
        "estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')"
        in result.get("soql_query", "")
    )
    assert result.get("needs_clarification") is True
    assert "filtro de entidad, lugar, tema o contratista" in result.get(
        "clarification_reason", ""
    )
    assert result.get("results") == []
    assert result.get("query_plan", {}).get("risk_flags") == ["unsafe_global_candidate"]


def test_corpus_v1_unaffected():
    """Smoke: corpus v1 must stay green after inserting the plan_query node."""
    repo_root = Path(__file__).resolve().parents[1]
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import app; print(app.__file__)",
        ],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    assert probe.returncode == 0, probe.stderr
    imported_app = Path(probe.stdout.strip()).resolve()
    assert imported_app.is_relative_to(repo_root)

    proc = subprocess.run(
        [
            sys.executable,
            "scripts/run_query_corpus.py",
            "--mode",
            "all",
        ],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    summary = re.search(
        r"Summary: (\d+)/(\d+) PASS, 0 FAIL",
        proc.stdout,
    )
    assert summary is not None, proc.stdout
    passed, total = map(int, summary.groups())
    assert passed == total
    assert total > 0
