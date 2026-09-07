"""An explicit document answering a pending actor list selects, never restarts.

The defect frozen here predates ADR-094 and was recorded in
``artifacts/audits/actor_exact_personal_prefix_collision_v1/
existing_document_followup_baseline.json``: a filtered actor query produced an
ambiguous list, the user replied with one candidate's document, identity was
resolved — and every filter of the original frame was lost.

A documentary reply to a pending list means exactly one of two things, and the
system may never guess between them:

* the document identifies one candidate of the list — that is a *selection*,
  and it must reach the same frame the rank selection reaches;
* the document identifies nobody on the list — that is a *new search*, and it
  must inherit nothing from the pending frame.
"""
import socket

import pytest
from hypothesis import HealthCheck, given, settings as hypothesis_settings, strategies as st

from app.config import Settings
from app.core.actor_identity import IdentityKey
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from tests.actor_prefix_fixtures import CHALLENGERS, EXACT, NAME, resolver_for


DOCUMENT_PENDING_MATCH = "MATCH"
DOCUMENT_PENDING_NO_MATCH = "NO_MATCH"
DOCUMENT_PENDING_NO_DOCUMENT = "NO_DOCUMENT"
DOCUMENT_PENDING_INCOMPATIBLE = "INCOMPATIBLE"

SAME_ALIAS = [("CC", "70000001", NAME), ("CC", "70000002", NAME)]
CANDIDATE_2_DOCUMENT = "CC 70000003"
FOREIGN_DOCUMENT = "CC 79482763"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("external I/O forbidden")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


def _workflow(tmp_path, monkeypatch, rows=None):
    resolver = resolver_for(tmp_path / "actors.sqlite", rows or [EXACT, *CHALLENGERS])
    settings = Settings(
        deepseek_api_key=None,
        secop_actor_alias_index_path=resolver.index_path,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        secop_feedback_sqlite_enabled=False,
    )
    workflow = SecopalWorkflow(settings)
    calls: list[str] = []

    def record(dataset, soql, *args, **kwargs):
        calls.append(soql)
        return []

    monkeypatch.setattr(workflow.secop_client, "query", record)
    monkeypatch.setattr(workflow.secop_client, "count", lambda *a, **k: 0)
    monkeypatch.setattr(workflow.secop_client, "aggregate", record)
    monkeypatch.setattr(workflow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    return workflow, calls


def _pending(result):
    return result["resolved_params"]["actor_pending"]


def _reply_params(text):
    return dict(QueryRouter().parse(text).params)


def _match(reply_text, candidates):
    """Import late so the workflow oracles fail on behavior, not on collection."""
    from app.core.actor_pending_document import match_pending_document

    return match_pending_document(_reply_params(reply_text), candidates)


# ---------------------------------------------------------------------------
# Pure matcher — identity, never name, rank or similarity.
# ---------------------------------------------------------------------------


def test_document_of_one_candidate_matches_that_candidate():
    candidates = [
        {"document_type": "CC", "base_number": "70000001", "rank": 1, "observed_name": "A"},
        {"document_type": "CC", "base_number": "70000003", "rank": 2, "observed_name": "B"},
    ]
    selection = _match(CANDIDATE_2_DOCUMENT, candidates)
    assert selection.outcome == DOCUMENT_PENDING_MATCH
    assert selection.rank == 2
    assert selection.identity == IdentityKey("CC", "70000003")


def test_document_outside_the_pending_list_is_no_match():
    candidates = [{"document_type": "CC", "base_number": "70000001", "rank": 1}]
    selection = _match(FOREIGN_DOCUMENT, candidates)
    assert selection.outcome == DOCUMENT_PENDING_NO_MATCH
    assert selection.rank is None


def test_repeated_identity_in_pending_fails_closed_instead_of_picking_first():
    candidates = [
        {"document_type": "CC", "base_number": "70000003", "rank": 1},
        {"document_type": "CC", "base_number": "70000003", "rank": 2},
    ]
    selection = _match(CANDIDATE_2_DOCUMENT, candidates)
    assert selection.outcome == DOCUMENT_PENDING_INCOMPATIBLE
    assert selection.rank is None


@pytest.mark.parametrize("text", ["2", "opción 2", "ana maria lopez diaz", "ese", "gracias"])
def test_non_documentary_reply_is_not_a_document_selection(text):
    candidates = [{"document_type": "CC", "base_number": "70000003", "rank": 2}]
    assert _match(text, candidates).outcome == (
        DOCUMENT_PENDING_NO_DOCUMENT
    )


@pytest.mark.parametrize("text", ["cc 70000003 en 2024", "procesos del cc 70000003"])
def test_document_carrying_extra_intent_is_not_a_pending_selection(text):
    """A reply that also changes the frame is a new search, not a list answer."""
    candidates = [{"document_type": "CC", "base_number": "70000003", "rank": 2}]
    assert _match(text, candidates).outcome == (
        DOCUMENT_PENDING_NO_DOCUMENT
    )


@pytest.mark.parametrize(
    "text", ["nit 900429803", "nit 9004298035", "nit 900429803-5"],
)
def test_nit_representations_reach_the_same_pending_identity(text):
    candidates = [{"document_type": "NIT", "base_number": "900429803", "rank": 1}]
    selection = _match(text, candidates)
    assert selection.outcome == DOCUMENT_PENDING_MATCH
    assert selection.identity == IdentityKey("NIT", "900429803")


def test_untyped_number_does_not_claim_a_typed_candidate():
    """INV-ID-TYPE: an undetermined type is not evidence of CC or NIT."""
    candidates = [{"document_type": "CC", "base_number": "70000003", "rank": 2}]
    assert _match("70000003", candidates).outcome == (
        DOCUMENT_PENDING_NO_MATCH
    )


# ---------------------------------------------------------------------------
# Workflow — the original frame survives a documentary selection.
# ---------------------------------------------------------------------------


def test_year_survives_explicit_document_selection(tmp_path, monkeypatch):
    workflow, calls = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query(f"contratos del proveedor {NAME} en 2025", chat_id="year")
    assert first["needs_clarification"]
    calls.clear()
    selected = workflow.run_query(CANDIDATE_2_DOCUMENT, chat_id="year")
    params = selected["resolved_params"]
    assert selected["route_reason"] == "actor_profile"
    assert params["actor_scope"]["base_number"] == "70000003"
    assert params["fecha_desde"] == "2025-01-01"
    assert params["fecha_hasta"] == "2025-12-31"
    assert params["actor_resolution_method"] == "explicit_document_pending_selection"
    assert "actor_pending" not in params
    assert calls and all(
        "documento_proveedor = '70000003'" in soql and "2025-01-01" in soql
        for soql in calls
    )


def test_buyer_survives_explicit_document_selection(tmp_path, monkeypatch):
    workflow, _ = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query(
        f"contratos del proveedor {NAME} con alcaldia de barranquilla", chat_id="buyer",
    )
    assert first["needs_clarification"]
    buyer = first["resolved_params"]["entidad_resolved"]
    selected = workflow.run_query(CANDIDATE_2_DOCUMENT, chat_id="buyer")
    params = selected["resolved_params"]
    assert params["entidad_resolved"] == buyer
    assert params["actor_scope"]["base_number"] == "70000003"


def test_buyer_and_year_survive_explicit_document_selection(tmp_path, monkeypatch):
    workflow, _ = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query(
        f"contratos del proveedor {NAME} con alcaldia de barranquilla en 2025",
        chat_id="both",
    )
    assert first["needs_clarification"]
    buyer = first["resolved_params"]["entidad_resolved"]
    params = workflow.run_query(CANDIDATE_2_DOCUMENT, chat_id="both")["resolved_params"]
    assert params["entidad_resolved"] == buyer
    assert params["fecha_desde"] == "2025-01-01"
    assert params["actor_scope"]["base_number"] == "70000003"


def test_value_filter_survives_explicit_document_selection(tmp_path, monkeypatch):
    workflow, _ = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query(
        f"contratos del proveedor {NAME} por mas de 100 millones", chat_id="value",
    )
    assert first["needs_clarification"]
    params = workflow.run_query(CANDIDATE_2_DOCUMENT, chat_id="value")["resolved_params"]
    assert params["valor_min"] == 100000000
    assert params["actor_scope"]["base_number"] == "70000003"


def test_same_alias_ambiguity_document_selection_keeps_the_year(tmp_path, monkeypatch):
    """The exact baseline recorded before ADR-094, now corrected."""
    workflow, _ = _workflow(tmp_path, monkeypatch, rows=SAME_ALIAS)
    first = workflow.run_query(f"contratos del proveedor {NAME} en 2025", chat_id="alias")
    assert first["resolved_params"]["actor_resolution_method"] == "exact_ambiguous_alias"
    params = workflow.run_query("CC 70000002", chat_id="alias")["resolved_params"]
    assert params["actor_scope"]["base_number"] == "70000002"
    assert params["fecha_desde"] == "2025-01-01"


def test_unrelated_document_starts_a_fresh_search(tmp_path, monkeypatch):
    workflow, _ = _workflow(tmp_path, monkeypatch)
    workflow.run_query(
        f"contratos del proveedor {NAME} con alcaldia de barranquilla en 2025",
        chat_id="fresh",
    )
    params = workflow.run_query(FOREIGN_DOCUMENT, chat_id="fresh")["resolved_params"]
    assert params["actor_scope"]["base_number"] == "79482763"
    assert params["actor_resolution_method"] == "explicit_document"
    assert params.get("fecha_desde") is None
    assert params.get("entidad_resolved") is None
    assert "actor_selection_rank" not in params


def test_rank_and_document_selection_agree_on_params_and_soql(tmp_path, monkeypatch):
    """The central oracle: two ways of naming the same candidate, one universe."""
    provenance = {"actor_resolution", "actor_resolution_method"}
    frames = {}
    universes = {}
    for label, reply in (("rank", "2"), ("document", CANDIDATE_2_DOCUMENT)):
        workflow, calls = _workflow(tmp_path / label, monkeypatch)
        workflow.run_query(
            f"contratos del proveedor {NAME} con alcaldia de barranquilla en 2025",
            chat_id=label,
        )
        calls.clear()
        result = workflow.run_query(reply, chat_id=label)
        frames[label] = {
            key: value for key, value in result["resolved_params"].items()
            if key not in provenance
        }
        universes[label] = list(calls)
    assert frames["rank"] == frames["document"]
    assert universes["rank"] == universes["document"]
    assert universes["document"]


def test_expired_pending_does_not_revive_the_original_frame(tmp_path, monkeypatch):
    workflow, _ = _workflow(tmp_path, monkeypatch)
    workflow.run_query(f"contratos del proveedor {NAME} en 2025", chat_id="stale")
    from app.core.conversation_store import Turn
    monkeypatch.setattr(Turn, "age_minutes", lambda self: 999.0)
    params = workflow.run_query(CANDIDATE_2_DOCUMENT, chat_id="stale")["resolved_params"]
    assert params["actor_scope"]["base_number"] == "70000003"
    assert params.get("fecha_desde") is None
    assert params["actor_resolution_method"] == "explicit_document"


def test_document_selection_never_reopens_actor_retrieval(tmp_path, monkeypatch):
    workflow, _ = _workflow(tmp_path, monkeypatch)
    workflow.run_query(f"contratos del proveedor {NAME} en 2025", chat_id="closed")

    def forbidden(*args, **kwargs):
        raise AssertionError("selected identity reopened the resolver")

    monkeypatch.setattr(type(workflow.actor_name_resolver), "resolve_exact", forbidden)
    monkeypatch.setattr(type(workflow.actor_name_resolver), "resolve", forbidden)
    params = workflow.run_query(CANDIDATE_2_DOCUMENT, chat_id="closed")["resolved_params"]
    assert params["actor_scope"]["base_number"] == "70000003"
    assert params["fecha_desde"] == "2025-01-01"


def test_inconclusive_explicit_document_keeps_its_adr_094_behavior(tmp_path, monkeypatch):
    from app.data import actor_exact_prefix as prefix
    monkeypatch.setattr(prefix, "EXISTENCE_BUDGET_MS", 0)
    workflow, _ = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query(
        f"contratos del proveedor {NAME} en 2025", chat_id="inconclusive",
    )
    assert first["resolved_params"]["actor_resolution_status"] == "INCONCLUSIVE"
    assert first["resolved_params"].get("actor_pending") is None
    params = workflow.run_query(
        CANDIDATE_2_DOCUMENT, chat_id="inconclusive",
    )["resolved_params"]
    assert params["actor_scope"]["base_number"] == "70000003"
    assert params["actor_resolution_method"] == "explicit_document"
    assert params["fecha_desde"] == "2025-01-01"


# ---------------------------------------------------------------------------
# Properties.
# ---------------------------------------------------------------------------


_SAFE_FRAMES = st.fixed_dictionaries({}, optional={
    "fecha_desde": st.just("2025-01-01"),
    "fecha_hasta": st.just("2025-12-31"),
    "valor_min": st.integers(min_value=1, max_value=10**10),
    "valor_max": st.integers(min_value=10**10, max_value=10**12),
    "entidad_resolved": st.just("DISTRITO DE BARRANQUILLA"),
    "estado": st.just("activo"),
    "modalidad": st.just("contratacion directa"),
    "objeto": st.just("vigilancia"),
    "ordering_signal": st.just("mayor_valor"),
})


def _pending_payload(candidates, original_params):
    return {
        "version": 1,
        "origin": "strong_actor_syntax",
        "original_query": "contratos del proveedor " + NAME,
        "original_params": dict(original_params),
        "actor_name": NAME,
        "resolution_status": "AMBIGUOUS",
        "candidates": candidates,
        "selection_token": "token",
    }


@hypothesis_settings(max_examples=60, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(frame=_SAFE_FRAMES)
def test_rank_and_document_selection_preserve_the_same_filters(frame):
    from app.core.actor_resolution import apply_actor_identity, candidate_from_payload
    candidates = [
        {"document_type": "CC", "base_number": "70000001", "rank": 1, "observed_name": "A"},
        {"document_type": "CC", "base_number": "70000003", "rank": 2, "observed_name": "B"},
    ]
    pending = _pending_payload(candidates, frame)
    selection = _match(CANDIDATE_2_DOCUMENT, candidates)
    assert selection.outcome == DOCUMENT_PENDING_MATCH
    by_rank = next(c for c in candidates if c["rank"] == 2)
    identity, observed = candidate_from_payload(by_rank)
    from_rank = apply_actor_identity(
        pending["original_params"], identity,
        method="user_selected_candidate", observed_name=observed,
    )
    from_document = apply_actor_identity(
        pending["original_params"], identity,
        method="explicit_document_pending_selection", observed_name=observed,
    )
    provenance = {"actor_resolution", "actor_resolution_method"}
    assert {k: v for k, v in from_rank.items() if k not in provenance} == {
        k: v for k, v in from_document.items() if k not in provenance
    }
    for key, value in frame.items():
        assert from_document[key] == value


@given(base=st.integers(min_value=10**6, max_value=10**9))
def test_a_document_outside_the_list_never_matches(base):
    candidates = [
        {"document_type": "CC", "base_number": "70000001", "rank": 1},
        {"document_type": "CC", "base_number": "70000003", "rank": 2},
    ]
    selection = _match(f"cc {base}", candidates)
    if str(base) in {"70000001", "70000003"}:
        assert selection.outcome == DOCUMENT_PENDING_MATCH
    else:
        assert selection.outcome == DOCUMENT_PENDING_NO_MATCH


@given(order=st.permutations([1, 2, 3]))
def test_identity_selection_is_independent_of_candidate_order(order):
    bases = ["70000001", "70000003", "70000002"]
    candidates = [
        {"document_type": "CC", "base_number": bases[index], "rank": rank}
        for rank, index in zip(order, range(3))
    ]
    selection = _match(CANDIDATE_2_DOCUMENT, candidates)
    assert selection.outcome == DOCUMENT_PENDING_MATCH
    assert selection.identity == IdentityKey("CC", "70000003")
