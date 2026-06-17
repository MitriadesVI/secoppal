"""Golden contract tests for SecopalWorkflow.run_query.

PURPOSE
-------
These tests are the safety net for the upcoming refactor that splits
`app/core/orchestrator.py` (1549 LOC) into `app/core/workflow/`.

They assert the **contract** of each branch of run_query, not the full
result dict. Each test pins:

- route_reason, intent_type, needs_clarification, followup, dataset_id,
  total_count, len(results) — invariants the refactor must preserve.
- Key substrings in `response` — proves rendering didn't drift.
- Side effects: feedback.jsonl line count and conv_store turn count.

Volatile keys deliberately ignored: trace_id, timings_ms,
universe_insights (non-deterministic under mocks), narrated.

NINE BRANCHES COVERED
---------------------
1. refinement bar bypass
2. /reset
3. pagination_more (with history)
4. pagination_no_context (without history)
5. direct_lookup id — single hit
6. direct_lookup id — not found
7. normal flow — first turn (mock 15 results)
8. normal flow — follow-up
9. timeout / risk_flag_timeout

NOT covered here (will be unit-tested in handler extraction step):
- suggestion_selection (requires conv_store internal seeding)
- entity_ref_disambiguation (multi-dataset mocking)
- analytical_response (separate code path with own tests)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


# ─────────────────────────────────────────────────────────────────────────────
# Fixture builders
# ─────────────────────────────────────────────────────────────────────────────

def _mk_proceso(i: int, entidad: str, modality: str = "Licitacion Publica") -> dict:
    """Mimic a SECOP procesos row (dataset p6dx-8zbt)."""
    return {
        "id_del_proceso": f"CO1.BDOS.{100000 + i}",
        "nombre_del_procedimiento": f"Obra {i+1}",
        "entidad": entidad,
        "precio_base": str(100_000_000 * (i + 1)),
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": f"2026-0{(i % 3) + 1}-15T00:00:00.000",
        "modalidad_de_contratacion": modality,
        "urlproceso": f"https://example.com/{i}",
        "departamento_entidad": "ATLANTICO",
        "ciudad_entidad": "BARRANQUILLA",
        "referencia_del_proceso": f"LP-2026-{i:03d}",
    }


def _mk_results(n: int, entidad: str = "ALCALDIA DE BARRANQUILLA") -> list[dict]:
    return [_mk_proceso(i, entidad) for i in range(n)]


@pytest.fixture
def wf(tmp_path: Path):
    """SecopalWorkflow with deterministic SECOP mocks and isolated stores."""
    workflow = SecopalWorkflow(Settings())
    # Default mock: 15 results, count=15
    workflow.secop_client.query = lambda dataset_id, soql_query: _mk_results(15)  # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql_query: 15  # type: ignore[method-assign]
    # No-op narrator (LLM calls would be non-deterministic and slow)
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    # Isolated conversation store per test
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return workflow


# ─────────────────────────────────────────────────────────────────────────────
# Side-effect helpers
# ─────────────────────────────────────────────────────────────────────────────

def _feedback_path() -> Path:
    """Read SECOP_FEEDBACK_PATH set by conftest.isolate_feedback_jsonl."""
    import os
    return Path(os.environ["SECOP_FEEDBACK_PATH"])


def _feedback_lines() -> int:
    p = _feedback_path()
    if not p.exists():
        return 0
    return sum(1 for line in p.read_text().splitlines() if line.strip())


def _conv_turns(wf: SecopalWorkflow, chat_id: str) -> int:
    return len(wf.conv_store.get_history(chat_id, last_n=100))


# ─────────────────────────────────────────────────────────────────────────────
# Contract assertion helper
# ─────────────────────────────────────────────────────────────────────────────

# Keys explicitly excluded from any equality comparison.
VOLATILE_KEYS = frozenset({
    "trace_id", "timings_ms", "universe_insights", "narrated",
})


def assert_contract(
    result: dict[str, Any],
    *,
    route_reason: str,
    intent_type: str | None = None,
    needs_clarification: bool = False,
    followup: bool = False,
    dataset_id: str | None = None,
    total_count: Any = ...,  # sentinel: don't check unless provided
    results_len: int | None = None,
    response_contains: list[str] | None = None,
    response_excludes: list[str] | None = None,
) -> None:
    """Assert the public contract of a run_query result."""
    assert result["route_reason"] == route_reason, (
        f"route_reason mismatch: {result['route_reason']!r} != {route_reason!r}"
    )
    if intent_type is not None:
        assert result["intent_type"] == intent_type, (
            f"intent_type mismatch: {result['intent_type']!r} != {intent_type!r}"
        )
    assert result["needs_clarification"] is needs_clarification
    assert result["followup"] is followup
    if dataset_id is not None:
        assert result["dataset_id"] == dataset_id
    if total_count is not ...:
        assert result["total_count"] == total_count, (
            f"total_count mismatch: {result['total_count']!r} != {total_count!r}"
        )
    if results_len is not None:
        assert len(result.get("results", [])) == results_len
    if response_contains:
        for needle in response_contains:
            assert needle in result["response"], (
                f"response missing {needle!r}; got: {result['response'][:200]!r}"
            )
    if response_excludes:
        for needle in response_excludes:
            assert needle not in result["response"], (
                f"response unexpectedly contains {needle!r}"
            )


# ═════════════════════════════════════════════════════════════════════════════
# SCENARIOS
# ═════════════════════════════════════════════════════════════════════════════


# ─── 1. Refinement bar bypass ────────────────────────────────────────────────

def test_golden_refinement_bar(wf, tmp_path):
    """seed_resolved_params skips parser, executes SoQL directly."""
    seed = {
        "objeto": ["mantenimiento"],
        "ciudad": "BARRANQUILLA",
        "dataset": "procesos",
    }
    result = wf.run_query(
        "filtrando por mantenimiento",
        channel="streamlit",
        chat_id="golden_refinement",
        seed_resolved_params=seed,
    )
    assert_contract(
        result,
        route_reason="refinement",
        intent_type="refinement",
        needs_clarification=False,
        followup=True,
        total_count=15,
        results_len=15,
        response_contains=["Refinamiento aplicado"],
    )
    # Side effect: 1 trace logged, 1 turn appended.
    assert _feedback_lines() == 1
    assert _conv_turns(wf, "golden_refinement") == 1


# ─── 2. /reset ───────────────────────────────────────────────────────────────

def test_golden_reset(wf, tmp_path):
    # Seed a turn so reset has something to clear.
    wf.run_query("procesos abiertos en Barranquilla", channel="streamlit", chat_id="golden_reset")
    assert _conv_turns(wf, "golden_reset") == 1

    result = wf.run_query("/reset", channel="streamlit", chat_id="golden_reset")
    assert_contract(
        result,
        route_reason="reset",
        intent_type="",
        needs_clarification=False,
        followup=False,
        dataset_id="",
        total_count=0,
        results_len=0,
    )
    # Conv store cleared.
    assert _conv_turns(wf, "golden_reset") == 0


# ─── 3. Pagination — with history ────────────────────────────────────────────

def test_golden_pagination_more(wf, tmp_path):
    wf.run_query("procesos abiertos en Barranquilla", channel="streamlit", chat_id="golden_pag")
    result = wf.run_query("muestrame mas", channel="streamlit", chat_id="golden_pag")
    assert_contract(
        result,
        route_reason="pagination_more",
        intent_type="pagination_more",
        needs_clarification=False,
        followup=True,
        results_len=15,
        response_contains=["Resultados"],
    )
    # Two turns persisted (first + pagination).
    assert _conv_turns(wf, "golden_pag") == 2


# ─── 4. Pagination — no history ──────────────────────────────────────────────

def test_golden_pagination_no_context(wf, tmp_path):
    result = wf.run_query("muestrame mas", channel="streamlit", chat_id="golden_pag_empty")
    assert_contract(
        result,
        route_reason="pagination_no_context",
        intent_type="pagination_more",
        needs_clarification=True,
        followup=False,
        dataset_id="",
        total_count=0,
        results_len=0,
    )
    # No turn appended (clarification, not a real result).
    assert _conv_turns(wf, "golden_pag_empty") == 0


# ─── 5. Direct lookup — id hit ───────────────────────────────────────────────

def test_golden_direct_lookup_id_hit(wf, tmp_path):
    # Replace mock: id lookup returns 1 row matching the queried id.
    target_id = "CO1.BDOS.123456"

    def fake_query(dataset_id, soql_query):
        # direct_lookup uses equality on id_del_proceso; honor it.
        if target_id in soql_query:
            return [_mk_proceso(0, "ALCALDIA DE BARRANQUILLA") | {"id_del_proceso": target_id}]
        return []

    wf.secop_client.query = fake_query  # type: ignore[method-assign]
    wf.secop_client.count = lambda *a, **kw: 1  # type: ignore[method-assign]

    result = wf.run_query(target_id, channel="streamlit", chat_id="golden_lookup_hit")
    assert_contract(
        result,
        route_reason="direct_lookup_id",
        intent_type="direct_lookup",
        needs_clarification=False,
        followup=False,
        total_count=1,
        results_len=1,
    )
    assert result["dataset_id"]  # non-empty (procesos dataset)
    assert _feedback_lines() >= 1


# ─── 6. Direct lookup — id not found ─────────────────────────────────────────

def test_golden_direct_lookup_id_not_found(wf, tmp_path):
    wf.secop_client.query = lambda *a, **kw: []  # type: ignore[method-assign]
    wf.secop_client.count = lambda *a, **kw: 0  # type: ignore[method-assign]

    result = wf.run_query("CO1.BDOS.999999", channel="streamlit", chat_id="golden_lookup_miss")
    # On miss, run_query falls through different paths depending on dataset.
    # The contract that must hold: not a real result, no entries persisted.
    assert result["needs_clarification"] is False
    assert result.get("total_count") in (0, None)
    assert len(result.get("results", [])) == 0
    # response should reference the looked-up id or explain the miss
    assert "CO1" in result["response"] or "no" in result["response"].lower()


# ─── 7. Normal flow — first turn ─────────────────────────────────────────────

def test_golden_normal_flow_first_turn(wf, tmp_path):
    # Lleva rubro ("mantenimiento") para ser una búsqueda ejecutable: un
    # browse territorial sin rubro lo bloquea P15 (ver test_demo_alcalde).
    result = wf.run_query(
        "procesos abiertos de mantenimiento en Barranquilla",
        channel="streamlit",
        chat_id="golden_normal",
    )
    # route_reason is set by the router; we don't pin its exact string
    # (router can evolve). We DO pin the structural contract.
    assert result["needs_clarification"] is False
    assert result["followup"] is False
    assert result["dataset_id"]  # non-empty
    assert result["total_count"] == 15
    assert len(result["results"]) == 15
    assert "soql_query" in result and result["soql_query"]
    # SoQL was actually built (not a clarification path)
    assert "SELECT" in result["soql_query"].upper()
    # Side effects
    assert _feedback_lines() == 1
    assert _conv_turns(wf, "golden_normal") == 1


# ─── 8. Normal flow — follow-up ──────────────────────────────────────────────

def test_golden_normal_flow_followup(wf, tmp_path):
    wf.run_query("procesos abiertos en Barranquilla", channel="streamlit", chat_id="golden_fu")
    result = wf.run_query(
        "solo los de infraestructura vial",
        channel="streamlit",
        chat_id="golden_fu",
    )
    assert result["followup"] is True
    assert result["needs_clarification"] is False
    assert result["dataset_id"]
    assert len(result["results"]) == 15
    # Two turns persisted.
    assert _conv_turns(wf, "golden_fu") == 2


# ─── 9. Timeout / degraded ───────────────────────────────────────────────────

def test_golden_timeout_transparent(wf, tmp_path):
    def raise_timeout(*a, **kw):
        raise TimeoutError("SECOP timed out")

    wf.secop_client.query = raise_timeout  # type: ignore[method-assign]
    wf.secop_client.count = lambda *a, **kw: 1  # type: ignore[method-assign]

    result = wf.run_query(
        "contratos de mantenimiento en antioquia",
        channel="streamlit",
        chat_id="golden_timeout",
    )
    # Timeout contract — copied from test_timeout_transparency.py
    assert result["risk_flag_timeout"] is True
    assert result["total_count"] is None
    response_lower = result["response"].lower()
    assert "no puedo confirmar si hay o no resultados" in response_lower
    assert "0 resultados" not in response_lower
    assert "no encontre resultados" not in response_lower
