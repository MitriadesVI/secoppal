"""Dogfood hardening V2 — four correctness defects found in real Actor Profile use.

They are four surfaces of one causal chain: parser → span ownership →
actor/buyer/geography arbitration → conversational frame → query plan.

* **A — identity split brain.** A frame ended up carrying ``contratista`` from
  one person and ``actor_scope`` from another. The SoQL filtered by one
  document while every consumer that reads the scope believed the other.
* **B — geography stolen from the actor span.** A word *inside* an accredited
  actor name ("… Llanos") was read as a region alias and became a territorial
  filter, producing a false zero.
* **C — asserted name leaking to topic.** A name typed next to an explicit
  document survived as ``objeto``, so the query demanded that the contract
  *object* mention the contractor.
* **D — conversational punctuation as buyer identity.** ``"… con alcaldía de
  barranquilla?"`` captured the ``?`` into the buyer name and failed to
  resolve.

Every fixture here is synthetic. Real dogfood documents live only in the audit
artifact, masked; the suite never treats a real person as an oracle.
"""
import socket

import pytest

from app.config import Settings
from app.core.entity_types import rewrite_entity
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from tests.actor_prefix_fixtures import resolver_for


# ── Synthetic directory ───────────────────────────────────────────────────
# Names are invented; the *shapes* are what matters:
#   SOLANO/SOLANO_PEREZ — an exact-prefix family that produces a pending list;
#   LLANOS              — a surname that is also a colloquial region alias;
#   CARIBE              — an organization whose tail is a region alias;
#   SANTANDER           — a surname that is also a department name.
SOLANO = ("CC", "70000011", "beatriz solano")
SOLANO_PEREZ = ("CC", "70000012", "beatriz solano perez")
LLANOS = ("CC", "70000021", "ana maria llanos")
CARIBE = ("CC", "70000022", "carlos rueda caribe")
SANTANDER = ("CC", "70000023", "pedro santander mejia")
DIRECTORY = [SOLANO, SOLANO_PEREZ, LLANOS, CARIBE, SANTANDER]

BUYER_QUERY = "alcaldia de barranquilla"
BUYER_RESOLVED = "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("external I/O forbidden")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


@pytest.fixture
def workflow(tmp_path, monkeypatch):
    resolver = resolver_for(tmp_path / "actors.sqlite", DIRECTORY)
    settings = Settings(
        deepseek_api_key=None,
        secop_actor_alias_index_path=resolver.index_path,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        secop_feedback_sqlite_enabled=False,
    )
    flow = SecopalWorkflow(settings)
    calls: list[str] = []

    def record(dataset, soql, *args, **kwargs):
        calls.append(soql)
        return []

    monkeypatch.setattr(flow.secop_client, "query", record)
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: 0)
    monkeypatch.setattr(flow.secop_client, "aggregate", record)
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kw: None)
    flow.source_calls = calls
    return flow


def _params(text: str) -> dict:
    return dict(QueryRouter().parse(text).params)


def _scope(params: dict) -> dict:
    scope = params.get("actor_scope")
    return scope if isinstance(scope, dict) else {}


def _topic_tokens(params: dict) -> set[str]:
    tokens: set[str] = set()
    for value in params.get("objeto") or []:
        values = value if isinstance(value, list) else [value]
        tokens.update(str(item).lower() for item in values)
    return tokens


def _select_from_pending(flow, chat_id, rank):
    """Answer a pending list the way the UI does, by rank."""
    return flow.run_query(str(rank), chat_id=chat_id)


# ══════════════════════════════════════════════════════════════════════════
# A — ACTOR-IDENTITY-SPLIT-BRAIN-001
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.xfail(strict=True, reason="ACTOR-IDENTITY-SPLIT-BRAIN-001 — frozen before slice A")
def test_explicit_document_replaces_the_selected_identity(workflow):
    """A new typed document is a *replacement*, never a second identity."""
    first = workflow.run_query(
        f"contratos de beatriz solano con {BUYER_QUERY}", chat_id="split",
    )
    assert first["resolved_params"].get("actor_pending"), "expected a pending list"
    selected = _select_from_pending(workflow, "split", 2)
    assert selected["resolved_params"]["contratista"] == [SOLANO_PEREZ[1]]

    replaced = workflow.run_query(
        f"y ahora: BEATRIZ SOLANO — CC {SOLANO[1]}", chat_id="split",
    )
    params = replaced["resolved_params"]
    assert params["contratista"] == [SOLANO[1]]
    assert _scope(params).get("base_number") == SOLANO[1]
    # The orthogonal buyer filter is not actor state and survives the swap.
    assert params.get("entidad_resolved") == BUYER_RESOLVED


@pytest.mark.xfail(strict=True, reason="ACTOR-IDENTITY-SPLIT-BRAIN-001 — frozen before slice A")
def test_replacement_by_document_keeps_orthogonal_year(workflow):
    workflow.run_query("contratos de ana maria llanos en 2025", chat_id="year")
    replaced = workflow.run_query(
        f"ahora BEATRIZ SOLANO — CC {SOLANO[1]}", chat_id="year",
    )
    params = replaced["resolved_params"]
    assert params["contratista"] == [SOLANO[1]]
    assert _scope(params).get("base_number") == SOLANO[1]
    assert params.get("fecha_desde") == "2025-01-01"


@pytest.mark.xfail(strict=True, reason="ACTOR-IDENTITY-SPLIT-BRAIN-001 — frozen before slice A")
def test_no_frame_carries_two_actor_identities(workflow):
    """INV-ACTOR-SINGLE-IDENTITY across every replacement path."""
    workflow.run_query(
        f"contratos de beatriz solano con {BUYER_QUERY}", chat_id="parity",
    )
    _select_from_pending(workflow, "parity", 2)
    for turn in (
        f"y ahora: BEATRIZ SOLANO — CC {SOLANO[1]}",
        "contratos de ana maria llanos",
        f"cambiame a ANA MARIA LLANOS",
    ):
        params = workflow.run_query(turn, chat_id="parity")["resolved_params"]
        scope = _scope(params)
        if not scope.get("base_number") or not params.get("contratista"):
            continue
        assert any(
            str(value).startswith(scope["base_number"])
            for value in params["contratista"]
        ), f"scope/contratista diverged after {turn!r}: {scope} vs {params['contratista']}"


@pytest.mark.xfail(strict=True, reason="ACTOR-IDENTITY-SPLIT-BRAIN-001 — frozen before slice A")
def test_split_brain_frame_fails_closed_before_soql():
    """The guard is a seat belt: an incoherent frame never reaches SECOP."""
    frame = {
        "dataset": "contratos",
        "contratista": [SOLANO[1]],
        "actor_scope": {
            "role": "contractor",
            "resolution_method": "user_selected_candidate",
            "requires_resolution": False,
            "document_type": "CC",
            "base_number": SOLANO_PEREZ[1],
        },
        "actor_resolution_method": "user_selected_candidate",
        "actor_resolution": "user_selected_candidate",
        "actor_resolution_status": "RESOLVED",
    }
    plan = compute_query_plan(frame, "contratos del actor")
    assert plan.should_execute is False
    assert plan.clarification_question


@pytest.mark.xfail(strict=True, reason="ACTOR-IDENTITY-SPLIT-BRAIN-001 — frozen before slice A")
def test_split_brain_frame_makes_no_source_call(workflow):
    workflow.run_query(
        f"contratos de beatriz solano con {BUYER_QUERY}", chat_id="guard",
    )
    _select_from_pending(workflow, "guard", 2)
    before = len(workflow.source_calls)
    workflow.run_query(
        "contratos del actor",
        chat_id="guard",
        seed_resolved_params={
            "dataset": "contratos",
            "contratista": [SOLANO[1]],
            "actor_scope": {
                "role": "contractor",
                "resolution_method": "user_selected_candidate",
                "requires_resolution": False,
                "document_type": "CC",
                "base_number": SOLANO_PEREZ[1],
            },
            "actor_resolution_method": "user_selected_candidate",
            "actor_resolution": "user_selected_candidate",
            "actor_resolution_status": "RESOLVED",
        },
    )
    assert len(workflow.source_calls) == before


# ══════════════════════════════════════════════════════════════════════════
# B — ACTOR-GEO-SPAN-OWNERSHIP-001
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.xfail(strict=True, reason="ACTOR-GEO-SPAN-OWNERSHIP-001 — frozen before slice B")
def test_region_alias_inside_actor_span_is_not_a_filter(workflow):
    params = workflow.run_query(
        "contratos de ana maria llanos", chat_id="geo1",
    )["resolved_params"]
    assert params["contratista"] == [LLANOS[1]]
    assert "region" not in params
    assert "departamentos_in" not in params


def test_region_alias_outside_actor_span_still_filters(workflow):
    params = workflow.run_query(
        "contratos de beatriz solano perez en los llanos", chat_id="geo2",
    )["resolved_params"]
    assert params.get("region") == "Orinoquia"
    assert "Meta" in (params.get("departamentos_in") or [])


def test_explicit_department_outside_actor_span_survives(workflow):
    params = workflow.run_query(
        "contratos de ana maria llanos en Meta", chat_id="geo3",
    )["resolved_params"]
    assert params["contratista"] == [LLANOS[1]]
    assert params.get("departamento_resolved") == "Meta"
    assert "region" not in params


def test_organization_region_tail_is_not_a_region(workflow):
    params = workflow.run_query(
        "contratos del proveedor carlos rueda caribe", chat_id="geo4",
    )["resolved_params"]
    assert params["contratista"] == [CARIBE[1]]
    assert "region" not in params


def test_organization_region_tail_with_explicit_department(workflow):
    params = workflow.run_query(
        "contratos del proveedor carlos rueda caribe en Atlantico", chat_id="geo5",
    )["resolved_params"]
    assert params["contratista"] == [CARIBE[1]]
    assert params.get("departamento_resolved") == "Atlántico"
    assert "region" not in params


def test_department_word_inside_actor_span_is_not_a_filter(workflow):
    params = workflow.run_query(
        "contratos de pedro santander mejia", chat_id="geo6",
    )["resolved_params"]
    assert params["contratista"] == [SANTANDER[1]]
    assert "departamento_resolved" not in params


def test_department_word_outside_actor_span_still_filters(workflow):
    params = workflow.run_query(
        "contratos de ana maria llanos en Santander", chat_id="geo7",
    )["resolved_params"]
    assert params["contratista"] == [LLANOS[1]]
    assert params.get("departamento_resolved") == "Santander"


# ══════════════════════════════════════════════════════════════════════════
# C — ACTOR-ASSERTED-NAME-DOCUMENT-LEAK-001
# ══════════════════════════════════════════════════════════════════════════

def test_role_marker_surface_already_consumes_the_asserted_name():
    """The working baseline: an explicit role marker opens the existing door."""
    params = _params("busca proveedor BEATRIZ SOLANO — CC 70000011")
    assert params["contratista"] == [SOLANO[1]]
    assert not _topic_tokens(params) & {"beatriz", "solano"}


@pytest.mark.parametrize("surface", [
    "busca BEATRIZ SOLANO — CC 70000011",
    "BEATRIZ SOLANO — CC 70000011",
    "ahora BEATRIZ SOLANO — CC 70000011",
    "muestrame BEATRIZ SOLANO con CC 70000011",
])
@pytest.mark.xfail(strict=True, reason="ACTOR-ASSERTED-NAME-DOCUMENT-LEAK-001 — frozen before slice C")
def test_asserted_name_beside_document_never_becomes_topic(surface):
    params = _params(surface)
    assert params["contratista"] == [SOLANO[1]]
    assert _scope(params).get("base_number") == SOLANO[1]
    assert not _topic_tokens(params) & {"beatriz", "solano"}


def test_asserted_name_does_not_swallow_topic_or_money():
    params = _params("contratos de mantenimiento en 2024 con NIT 900123456")
    assert "mantenimiento" in _topic_tokens(params)
    assert params.get("fecha_desde") == "2024-01-01"


def test_asserted_name_does_not_swallow_an_amount():
    params = _params("contratos de mas de 1000 millones con NIT 900123456")
    assert params.get("valor_min")


# ══════════════════════════════════════════════════════════════════════════
# D — BUYER-TERMINAL-PUNCTUATION-BOUNDARY-001
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("mark", ["?", "!", ".", ";", ":", ","])
@pytest.mark.xfail(strict=True, reason="BUYER-TERMINAL-PUNCTUATION-BOUNDARY-001 — frozen before slice D")
def test_terminal_punctuation_is_not_buyer_identity(mark):
    params = _params(f"contratos con {BUYER_QUERY}{mark}")
    assert params.get("entidad_resolved") == BUYER_RESOLVED


@pytest.mark.parametrize("mark", [",", ";", ":"])
@pytest.mark.xfail(strict=True, reason="BUYER-TERMINAL-PUNCTUATION-BOUNDARY-001 — frozen before slice D")
def test_punctuation_before_a_modifier_keeps_both(mark):
    params = _params(f"contratos con {BUYER_QUERY}{mark} en 2026")
    assert params.get("entidad_resolved") == BUYER_RESOLVED
    assert params.get("fecha_desde") == "2026-01-01"


def test_terminal_punctuation_does_not_leak_into_topic():
    params = _params(f"contratos con {BUYER_QUERY}?")
    assert not _topic_tokens(params) & {"?", "barranquilla?", "barranquilla"}


@pytest.mark.parametrize("name,expected", [
    ("hospital san rafael e.s.e.", "san rafael e.s.e."),
    ("hospital universitario s.a.s.", "universitario s.a.s."),
])
def test_internal_institutional_punctuation_is_preserved(name, expected):
    """A period that closes an abbreviation is name material, not syntax."""
    spans = {candidate.consumed_span for candidate in rewrite_entity(name)}
    assert f"hospital {expected}" in spans


@pytest.mark.xfail(strict=True, reason="BUYER-TERMINAL-PUNCTUATION-BOUNDARY-001 — frozen before slice D")
def test_terminal_question_mark_is_not_captured_into_the_name():
    spans = {c.consumed_span for c in rewrite_entity("hospital san jose de maicao?")}
    assert "hospital san jose de maicao" in spans


@pytest.mark.xfail(strict=True, reason="BUYER-TERMINAL-PUNCTUATION-BOUNDARY-001 — frozen before slice D")
def test_buyer_punctuation_followup_keeps_actor_and_year(workflow):
    workflow.run_query("contratos de ana maria llanos", chat_id="dpunct")
    params = workflow.run_query(
        f"ok que tiene en 2026 con {BUYER_QUERY}?", chat_id="dpunct",
    )["resolved_params"]
    assert params["contratista"] == [LLANOS[1]]
    assert params.get("fecha_desde") == "2026-01-01"
    assert params.get("fecha_hasta") == "2026-12-31"
    assert params.get("entidad_resolved") == BUYER_RESOLVED


# ══════════════════════════════════════════════════════════════════════════
# Cross-slice matrix
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.xfail(strict=True, reason="split brain + asserted-name leak — frozen before slices A and C")
def test_cross_slice_document_replacement_keeps_buyer_and_drops_name_topic(workflow):
    workflow.run_query(
        f"contratos de ana maria llanos con {BUYER_QUERY}", chat_id="cross",
    )
    params = workflow.run_query(
        f"busca BEATRIZ SOLANO — CC {SOLANO[1]}", chat_id="cross",
    )["resolved_params"]
    assert params["contratista"] == [SOLANO[1]]
    assert _scope(params).get("base_number") == SOLANO[1]
    assert not _topic_tokens(params) & {"beatriz", "solano"}


@pytest.mark.xfail(strict=True, reason="span ownership + buyer punctuation — frozen before slices B and D")
def test_cross_slice_actor_span_and_explicit_buyer_coexist(workflow):
    params = workflow.run_query(
        f"contratos de ana maria llanos con {BUYER_QUERY}?", chat_id="cross2",
    )["resolved_params"]
    assert params["contratista"] == [LLANOS[1]]
    assert params.get("entidad_resolved") == BUYER_RESOLVED
    assert "region" not in params
