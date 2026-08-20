"""AP3-C — two-door arbitration, authority and contextual selection."""
from __future__ import annotations

import socket
from pathlib import Path
from types import SimpleNamespace

import pytest
from burr.core import State

from app.config import Settings
from app.core.actor_arbitration import detect_bare_actor_probe
from app.core.actor_identity import IdentityKey, normalize_identity
from app.core.actor_name import ActorResolutionStatus, normalize_actor_name
from app.core.actor_resolution import (
    METHOD_EXACT_UNIQUE_ALIAS,
    METHOD_USER_SELECTED_CANDIDATE,
    actor_resolution_method,
    is_authoritative_actor_resolution,
)
from app.core.actor_scope_policy import evaluate_actor_scope
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.core.workflow.handlers.actor_selection import handle_actor_selection
from app.core.workflow.context import HandlerContext
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver


def _row(document_type: str, document: str, name: str, occurrences: int = 5) -> dict:
    return {
        "tipodocproveedor": document_type,
        "documento_proveedor": document,
        "proveedor_adjudicado": name,
        "occurrences": occurrences,
        "first_seen": "2024-01-01",
        "last_seen": "2026-01-01",
    }


def _resolver(tmp_path: Path, rows: list[dict]) -> ActorNameResolver:
    path = tmp_path / "actors.sqlite"
    build_actor_alias_index(
        rows,
        path,
        source_dataset="ap3-c-fixture",
        built_at="2026-08-19T12:00:00+00:00",
    )
    return ActorNameResolver(path)


def _resolve_action(query: str, resolver: ActorNameResolver) -> dict:
    params = dict(QueryRouter().parse(query).params)
    state = State({"parsed_params": params, "raw_query": query})
    return dict(resolve_actor_name(state, resolver)["parsed_params"])


def _identity(document: str, document_type: str = "NIT") -> IdentityKey:
    match = normalize_identity(document, document_type=document_type)
    assert match.key is not None
    return match.key


def test_strong_syntax_uses_full_resolver_and_promotes_implicit_dataset(tmp_path):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "FUNDACARIBE")])

    params = _resolve_action("qué ha contratado FUNDACARIBE", resolver)

    assert params["dataset"] == "contratos"
    assert params["actor_resolution_status"] == ActorResolutionStatus.RESOLVED
    assert actor_resolution_method(params) == METHOD_EXACT_UNIQUE_ALIAS
    assert is_authoritative_actor_resolution(params)
    assert params["actor_scope"]["base_number"] == _identity("900111222").base_number


def test_explicit_document_is_canonicalized_between_parse_and_plan(tmp_path):
    params = _resolve_action(
        "contratos del NIT 860002400",
        ActorNameResolver(tmp_path / "resolver-must-not-be-opened.sqlite"),
    )

    assert params["actor_resolution"] == "explicit_document"
    assert params["actor_resolution_method"] == "explicit_document"
    assert params["actor_resolution_status"] == ActorResolutionStatus.RESOLVED
    assert is_authoritative_actor_resolution(params)


def test_authority_fails_closed_when_legacy_and_canonical_methods_diverge(tmp_path):
    params = _resolve_action(
        "contratos del NIT 860002400",
        ActorNameResolver(tmp_path / "resolver-must-not-be-opened.sqlite"),
    )
    params["actor_resolution"] = "name_unresolved"

    assert actor_resolution_method(params) is None
    assert not is_authoritative_actor_resolution(params)


def test_bare_exact_unique_becomes_actor_and_preserves_year_and_order(tmp_path):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "FUNDACARIBE")])

    params = _resolve_action(
        "contratos de FUNDACARIBE en 2025 por mayor valor",
        resolver,
    )

    assert params.get("objeto") is None
    assert params["fecha_desde"] == "2025-01-01"
    assert params["fecha_hasta"] == "2025-12-31"
    assert params["ordering_signal"] == "valor_desc"
    assert is_authoritative_actor_resolution(params)


@pytest.mark.parametrize(
    ("query", "expected_name"),
    [
        (
            "contratos de FUNDACIÓN MANOS EN ACCIÓN",
            "fundacion manos en accion",
        ),
        (
            "contratos de CORPORACIÓN PARA EL DESARROLLO SOCIAL",
            "corporacion para el desarrollo social",
        ),
        ("contratos de FUNDACIÓN CON AMOR", "fundacion con amor"),
        (
            "contratos de FUNDACARIBE en 2025 por mayor valor",
            "fundacaribe",
        ),
    ],
)
def test_bare_probe_preserves_internal_prepositions_and_cuts_real_tails(
    query,
    expected_name,
):
    params = dict(QueryRouter().parse(query).params)

    probe = detect_bare_actor_probe(query, params)

    assert probe is not None
    assert probe.actor_name == expected_name


def test_bare_probe_does_not_resolve_a_truncated_wrong_actor(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "900111222", "FUNDACION MANOS"),
    ])
    query = "contratos de FUNDACIÓN MANOS EN ACCIÓN"
    params = _resolve_action(query, resolver)

    assert params["actor_resolution_status"] == ActorResolutionStatus.UNRESOLVED
    assert params["actor_scope"]["observed_name"] == "fundacion manos en accion"
    assert params["actor_intent_conflict"]["kind"] == "entity_like_no_exact"
    assert params.get("contratista") is None
    assert params.get("objeto") is None
    assert evaluate_actor_scope(params).should_execute is False


def test_bare_ambiguous_clarification_uses_pending_actor_name(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "900111222", "FUNDACION MANOS EN ACCION", 9),
        _row("NIT", "900333444", "FUNDACION MANOS EN ACCION", 8),
    ])

    params = _resolve_action("contratos de FUNDACIÓN MANOS EN ACCIÓN", resolver)
    question = evaluate_actor_scope(params).clarification_question or ""

    assert params["actor_resolution_status"] == ActorResolutionStatus.AMBIGUOUS
    assert params.get("actor_scope") is None
    assert "FUNDACION MANOS EN ACCION" in question
    assert "None" not in question


def test_ambiguous_clarification_has_a_neutral_last_resort_name():
    decision = evaluate_actor_scope({
        "actor_resolution_status": ActorResolutionStatus.AMBIGUOUS,
        "actor_pending": {"candidates": []},
    })
    question = decision.clarification_question or ""

    assert "INDICADO" in question
    assert "None" not in question


@pytest.mark.parametrize(
    ("query", "expected_name"),
    [
        ("contratos del proveedor UNIVERSIDAD X", "universidad x"),
        ("contratos del contratista HOSPITAL X", "hospital x"),
        ("contratos adjudicados a UNIVERSIDAD X", "universidad x"),
    ],
)
def test_buyer_like_name_with_explicit_actor_role_survives_projection(
    tmp_path,
    query,
    expected_name,
):
    params = _resolve_action(query, ActorNameResolver(tmp_path / "missing.sqlite"))

    assert params["actor_scope"]["observed_name"] == expected_name
    assert params["actor_resolution_status"] == ActorResolutionStatus.INDEX_UNAVAILABLE
    assert params.get("entidad") is None
    assert params.get("entidad_resolved") is None
    assert params.get("objeto") is None


def test_bare_probe_miss_or_unavailable_preserves_baseline_topic(tmp_path):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "FUNDACARIBE")])
    query = "contratos de mantenimiento"
    baseline = dict(QueryRouter().parse(query).params)

    miss = _resolve_action(query, resolver)
    unavailable = _resolve_action(query, ActorNameResolver(tmp_path / "missing.sqlite"))

    assert miss == baseline
    assert unavailable == baseline
    assert miss["objeto"] == ["mantenimiento"]


@pytest.mark.parametrize(
    "topic",
    ["alimentación", "mantenimiento", "software", "vigilancia"],
)
def test_known_topics_survive_exact_miss_without_actor_clarification(tmp_path, topic):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "ACTOR DISTINTO")])
    query = f"contratos de {topic}"
    baseline = dict(QueryRouter().parse(query).params)

    assert _resolve_action(query, resolver) == baseline


def test_article_led_buyer_survives_an_exact_actor_miss(tmp_path):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "ACTOR DISTINTO")])
    query = "contratos de la alcaldía de Medellín"
    baseline = dict(QueryRouter().parse(query).params)

    params = _resolve_action(query, resolver)

    assert params == baseline
    assert params.get("entidad_resolved")
    assert params.get("actor_resolution_status") is None


@pytest.mark.parametrize(
    "name",
    [
        "ASILO DE SAN ANTONIO",
        "ASILO HOGAR GRANJA SAN JOSE",
        "FUNDACIÓN ANA DELIA",
        "TUS ABUELOS BARRANQUILLA S.A.S.",
        "FUNDACION HOGAR DE SAN CAMILO",
        "FUNDACION HOGAR DE PASO MIS MEJORES AÑOS",
    ],
)
def test_inv_bare_name_safe_nohit_external_holdouts_never_become_topics(
    tmp_path,
    name,
):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "ACTOR DISTINTO")])
    query = f"contratos de {name}"

    params = _resolve_action(query, resolver)
    decision = evaluate_actor_scope(params)

    assert params["actor_resolution_status"] == ActorResolutionStatus.UNRESOLVED
    assert params["actor_scope"]["observed_name"] == normalize_actor_name(name)
    assert params["actor_intent_conflict"]["kind"] == "entity_like_no_exact"
    assert params.get("objeto") is None
    assert params.get("objeto_phrases") is None
    assert params.get("entidad") is None
    assert params.get("entidad_resolved") is None
    assert params.get("ciudad") is None
    assert params.get("departamento_resolved") is None
    assert decision.should_execute is False
    assert decision.reason == "actor_or_topic_ambiguous"
    assert "NIT o cédula" in (decision.clarification_question or "")


def test_entity_shaped_bare_name_blocks_when_index_is_unavailable(tmp_path):
    query = "contratos de FUNDACION HOGAR DE SAN CAMILO"

    params = _resolve_action(query, ActorNameResolver(tmp_path / "missing.sqlite"))
    decision = evaluate_actor_scope(params)

    assert params["actor_resolution_status"] == ActorResolutionStatus.INDEX_UNAVAILABLE
    assert params.get("objeto") is None
    assert decision.should_execute is False
    assert decision.reason == "actor_index_unavailable"


def test_explicit_actor_index_unavailable_has_a_dedicated_execution_block(
    tmp_path,
    monkeypatch,
):
    query = "contratos del proveedor FUNDACARIBE"

    def network_forbidden(*args, **kwargs):
        raise AssertionError("network fallback attempted")

    monkeypatch.setattr(socket, "create_connection", network_forbidden)

    params = _resolve_action(query, ActorNameResolver(tmp_path / "missing.sqlite"))
    decision = evaluate_actor_scope(params)
    plan = compute_query_plan(params, query)

    assert params["actor_resolution_status"] == ActorResolutionStatus.INDEX_UNAVAILABLE
    assert params.get("objeto") is None
    assert decision.should_execute is False
    assert decision.reason == "actor_index_unavailable"
    assert decision.risk_flags == ("actor_index_unavailable",)
    assert decision.missing_slots == ("actor_document",)
    assert "NIT o la cédula" in (decision.clarification_question or "")
    assert plan.should_execute is False
    assert plan.reason == "actor_index_unavailable"
    assert "NIT o la cédula" in (plan.clarification_question or "")


def test_bare_probe_never_uses_fuzzy_as_intent_evidence(monkeypatch):
    class ProbeOnlyResolver:
        def resolve_exact(self, query):
            from app.core.actor_name import ActorResolutionResult

            return ActorResolutionResult(
                status=ActorResolutionStatus.UNRESOLVED,
                normalized_query=str(query),
                method="no_exact_match",
            )

        def resolve(self, query):
            raise AssertionError("bare actor syntax must never call full/fuzzy resolution")

    query = "contratos de software"
    params = dict(QueryRouter().parse(query).params)
    state = State({"parsed_params": params, "raw_query": query})

    projected = resolve_actor_name(state, ProbeOnlyResolver())["parsed_params"]

    assert projected == params


def test_resolution_orchestration_is_inert_when_feature_flag_is_off(monkeypatch):
    class ResolverMustNotRun:
        def resolve(self, query):
            raise AssertionError("resolver ran with feature flag off")

        def resolve_exact(self, query):
            raise AssertionError("exact probe ran with feature flag off")

    monkeypatch.setattr(
        "app.core.workflow.actions.actor_resolution.actor_profile_enabled",
        lambda: False,
    )
    query = "contratos de FUNDACARIBE"
    params = dict(QueryRouter().parse(query).params)
    state = State({"parsed_params": params, "raw_query": query})

    projected = resolve_actor_name(state, ResolverMustNotRun())["parsed_params"]

    assert projected == params


def test_known_topic_plus_exact_actor_alias_requires_intent_clarification(tmp_path):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "MANTENIMIENTO")])

    params = _resolve_action("contratos de mantenimiento", resolver)
    decision = evaluate_actor_scope(params)

    assert params["objeto"] == ["mantenimiento"]
    assert params["actor_intent_conflict"]["actor_name"] == "mantenimiento"
    assert not is_authoritative_actor_resolution(params)
    assert decision.should_execute is False
    assert decision.reason == "actor_or_topic_ambiguous"


def test_full_fuzzy_candidates_are_pending_and_not_authoritative(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "900111222", "FUNDACION PARA EL CARIBE"),
    ])
    query = "contratos del proveedor FUNDACION CARIBE"

    params = _resolve_action(query, resolver)

    assert params["actor_resolution_status"] == ActorResolutionStatus.CANDIDATES
    assert params["actor_pending"]["original_query"] == query
    assert params["actor_pending"]["original_params"]["dataset"] == "contratos"
    assert not is_authoritative_actor_resolution(params)
    assert evaluate_actor_scope(params).should_execute is False


def test_strong_actor_name_does_not_leak_into_topic_or_geography(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "900111222", "FUNDACION PARA EL CARIBE"),
    ])

    params = _resolve_action(
        "contratos del proveedor FUNDACION CARIBE en 2025 por mayor valor",
        resolver,
    )

    assert params.get("objeto") is None
    assert params.get("region") is None
    assert params.get("departamentos_in") is None
    assert params["fecha_desde"] == "2025-01-01"
    assert params["ordering_signal"] == "valor_desc"


@pytest.mark.parametrize(
    ("query", "expected_modifier", "expected_value"),
    [
        (
            "contratos del proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDER",
            None,
            None,
        ),
        (
            "contratos del proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDER en 2025",
            "fecha_desde",
            "2025-01-01",
        ),
        (
            "contratos del proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDER en Bolívar",
            "departamento_resolved",
            "Bolívar",
        ),
    ],
)
def test_inv_actor_longest_authoritative_span_keeps_internal_santander(
    tmp_path,
    query,
    expected_modifier,
    expected_value,
):
    resolver = _resolver(tmp_path, [
        _row("NIT", "890201213", "UNIVERSIDAD INDUSTRIAL DE SANTANDER", 234),
    ])

    params = _resolve_action(query, resolver)

    assert is_authoritative_actor_resolution(params)
    assert params["actor_scope"]["observed_name"] == (
        "universidad industrial de santander"
    )
    assert params["actor_scope"]["base_number"] == "890201213"
    assert params.get("entidad") is None
    assert params.get("entidad_resolved") is None
    assert params.get("departamento") != "de santander"
    assert params.get("departamento_resolved") != "Santander"
    if expected_modifier:
        assert params[expected_modifier] == expected_value


def test_fuzzy_never_claims_a_longer_actor_boundary(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "890201213", "UNIVERSIDAD INDUSTRIAL DE SANTANDER", 234),
    ])

    params = _resolve_action(
        "contratos del proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDE en Bolívar",
        resolver,
    )

    assert params["actor_resolution_status"] == ActorResolutionStatus.CANDIDATES
    assert params.get("contratista") is None
    assert not is_authoritative_actor_resolution(params)
    assert params["departamento_resolved"] == "Bolívar"


def test_ambiguous_cc_choices_are_masked_in_clarification(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("Cédula de Ciudadanía", "1140835135", "PERSONA OBSERVADA", 9),
        _row("Cédula de Ciudadanía", "79482763", "PERSONA OBSERVADA", 8),
    ])
    params = _resolve_action("qué contratos tiene PERSONA OBSERVADA", resolver)

    question = evaluate_actor_scope(params).clarification_question or ""

    assert params["actor_resolution_status"] == ActorResolutionStatus.AMBIGUOUS
    assert "1140835135" not in question
    assert "79482763" not in question
    assert "****5135" in question


def test_numbered_selection_mutates_the_original_frame_with_honest_provenance(
    tmp_path,
    monkeypatch,
):
    store = ConversationStore(base_path=tmp_path / "conversations")
    original = {
        "dataset": "contratos",
        "fecha_desde": "2025-01-01",
        "fecha_hasta": "2025-12-31",
        "ordering_signal": "valor_desc",
        "actor_resolution": "name_unresolved",
        "actor_resolution_method": "name_unresolved",
        "actor_resolution_status": ActorResolutionStatus.UNRESOLVED,
        "actor_scope": {"observed_name": "fundacaribe", "requires_resolution": True},
    }
    pending_params = dict(original)
    pending_params["actor_pending"] = {
        "version": 1,
        "origin": "strong_actor_syntax",
        "original_query": "contratos del proveedor FUNDACARIBE en 2025 por mayor valor",
        "original_params": original,
        "actor_name": "fundacaribe",
        "resolution_status": ActorResolutionStatus.CANDIDATES,
        "candidates": [{
            "document_type": "NIT",
            "base_number": "900111222",
            "matched_alias": "fundacaribe",
            "observed_name": "FUNDACARIBE SAS",
            "rank": 1,
            "occurrences": 8,
        }],
    }
    store.append_turn(
        chat_id="actor-chat",
        user_query="consulta original",
        response="elige",
        parsed_params=pending_params,
    )
    captured = {}

    def fake_normal_flow(deps, ctx):
        captured.update(deps.query_router.parse(ctx.user_query).params)
        captured["pipeline_query"] = ctx.user_query
        return {"route_reason": "captured"}

    monkeypatch.setattr(
        "app.core.workflow.handlers.normal_flow.handle_normal_flow",
        fake_normal_flow,
    )
    deps = SimpleNamespace(
        conv_store=store,
        query_router=QueryRouter(),
        feedback=SimpleNamespace(),
    )
    ctx = HandlerContext(user_query="1", chat_id="actor-chat")

    result = handle_actor_selection(deps, ctx)

    assert result == {"route_reason": "captured", "followup": True}
    assert captured["fecha_desde"] == "2025-01-01"
    assert captured["fecha_hasta"] == "2025-12-31"
    assert captured["ordering_signal"] == "valor_desc"
    assert captured["actor_resolution_method"] == METHOD_USER_SELECTED_CANDIDATE
    assert captured["actor_resolution"] == METHOD_USER_SELECTED_CANDIDATE
    assert captured["actor_resolution_method"] == "user_selected_candidate"
    assert captured["actor_resolution"] == "user_selected_candidate"
    assert captured["actor_scope"]["resolution_method"] == "user_selected_candidate"
    assert captured["actor_resolution_status"] == ActorResolutionStatus.RESOLVED
    assert "actor_pending" not in captured
    assert captured["pipeline_query"].startswith("contratos del proveedor")


def test_candidate_selection_resumes_real_pipeline_with_original_modifiers(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "900111222", "FUNDACION PARA EL CARIBE"),
    ])
    settings = Settings(
        secop_actor_alias_index_path=resolver.index_path,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        secop_feedback_sqlite_enabled=False,
    )
    workflow = SecopalWorkflow(settings)
    workflow.secop_client.query = lambda *args, **kwargs: []
    workflow.secop_client.count = lambda *args, **kwargs: 0
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    query = (
        "contratos del proveedor FUNDACION CARIBE en 2025 por mayor valor"
    )

    pending = workflow.run_query(query, chat_id="ap3-c-e2e")
    selected = workflow.run_query("1", chat_id="ap3-c-e2e")

    assert pending["needs_clarification"] is True
    assert pending["resolved_params"]["actor_resolution_status"] == "CANDIDATES"
    assert selected["resolved_params"]["actor_resolution_method"] == (
        METHOD_USER_SELECTED_CANDIDATE
    )
    assert selected["resolved_params"]["actor_resolution_method"] == (
        "user_selected_candidate"
    )
    assert selected["resolved_params"]["fecha_desde"] == "2025-01-01"
    assert selected["resolved_params"]["fecha_hasta"] == "2025-12-31"
    assert selected["resolved_params"]["ordering_signal"] == "valor_desc"
    assert "documento_proveedor =" in selected["soql_query"]
    assert "fecha_de_firma >= '2025-01-01'" in selected["soql_query"]
    assert "ORDER BY valor_del_contrato DESC" in selected["soql_query"]
    history = workflow.conv_store.get_history("ap3-c-e2e", last_n=1)
    assert history[-1].user_query == "1"
    assert history[-1].parsed_params["actor_resolution_method"] == (
        METHOD_USER_SELECTED_CANDIDATE
    )
