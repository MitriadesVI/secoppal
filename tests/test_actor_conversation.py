"""AP3-F systemic actor-conversation operations and invariants."""
from __future__ import annotations

from pathlib import Path

import pytest
from burr.core import State

from app.config import Settings
from app.core.actor_conversation import (
    ActorConversationOperation,
    interpret_actor_conversation,
    parse_actor_candidate_selection,
    purge_actor_state,
)
from app.core.actor_name import ActorResolutionStatus
from app.core.actor_resolution import (
    METHOD_EXACT_LEGAL_FORM_VARIANT,
    METHOD_USER_SELECTED_CANDIDATE,
    is_authoritative_actor_resolution,
)
from app.core.actor_scope_policy import evaluate_actor_scope
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
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
        source_dataset="ap3-f-fixture",
        built_at="2026-08-20T12:00:00+00:00",
    )
    return ActorNameResolver(path)


def _resolve(query: str, resolver: ActorNameResolver) -> dict:
    params = dict(QueryRouter().parse(query).params)
    state = State({"parsed_params": params, "raw_query": query})
    return dict(resolve_actor_name(state, resolver)["parsed_params"])


def _workflow(tmp_path: Path, rows: list[dict]) -> SecopalWorkflow:
    resolver = _resolver(tmp_path, rows)
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
    return workflow


@pytest.mark.parametrize(
    ("text", "rank"),
    [
        ("2", 2), ("el 2", 2), ("la 2", 2), ("opcion 2", 2),
        ("opción 2", 2), ("el segundo", 2), ("la segunda", 2),
        ("segundo", 2), ("segunda", 2), ("el décimo", 10), ("17", 17),
    ],
)
def test_candidate_selection_language_v1(text, rank):
    assert parse_actor_candidate_selection(text) == rank


def test_pending_actor_selection_precedes_generic_routing_and_preserves_context(
    tmp_path,
):
    workflow = _workflow(tmp_path, [
        _row("NIT", "900111222", "ACTOR ALFA", 9),
        _row("NIT", "900333444", "ACTOR ALFA", 8),
    ])

    pending = workflow.run_query(
        "contratos del proveedor ACTOR ALFA en 2025 por mayor valor",
        chat_id="selection",
    )
    selected = workflow.run_query("la segunda", chat_id="selection")

    assert pending["needs_clarification"] is True
    params = selected["resolved_params"]
    assert params["actor_scope"]["base_number"] == "900333444"
    assert params["actor_resolution_method"] == METHOD_USER_SELECTED_CANDIDATE
    assert params["fecha_desde"] == "2025-01-01"
    assert params["fecha_hasta"] == "2025-12-31"
    assert params["ordering_signal"] == "valor_desc"


def test_invalid_pending_rank_asks_again_instead_of_falling_through(tmp_path):
    workflow = _workflow(tmp_path, [
        _row("NIT", "900111222", "ACTOR ALFA", 9),
        _row("NIT", "900333444", "ACTOR ALFA", 8),
    ])
    workflow.run_query("busca proveedor ACTOR ALFA", chat_id="invalid-rank")

    invalid = workflow.run_query("17", chat_id="invalid-rank")

    assert invalid["needs_clarification"] is True
    assert invalid["route_reason"] == "actor_selection_invalid"


def test_con_and_en_project_different_contextual_operations():
    con = interpret_actor_conversation("con Córdoba?")
    en = interpret_actor_conversation("en Córdoba?")

    assert con.operation == ActorConversationOperation.SET_BUYER
    assert con.buyer_mention == "cordoba"
    assert en.operation == ActorConversationOperation.SET_TERRITORY
    assert en.territory_mention == "cordoba"


def test_contextual_con_and_en_keep_actor_and_year_but_change_different_roles(
    tmp_path,
):
    workflow = _workflow(
        tmp_path, [_row("NIT", "900334689", "FUNSOCOM", 20)]
    )
    workflow.run_query("busca proveedor funsocom", chat_id="con")
    workflow.run_query("que ha hecho en 2026?", chat_id="con")
    buyer = workflow.run_query("con cordoba?", chat_id="con")["resolved_params"]

    workflow.run_query("busca proveedor funsocom", chat_id="en")
    workflow.run_query("que ha hecho en 2026?", chat_id="en")
    territory = workflow.run_query("en cordoba?", chat_id="en")["resolved_params"]

    assert buyer["actor_scope"]["base_number"] == "900334689"
    assert buyer["fecha_desde"] == "2026-01-01"
    assert buyer["entidad_resolved"] == "DEPARTAMENTO DE CORDOBA"
    assert buyer.get("departamento_resolved") is None
    assert territory["actor_scope"]["base_number"] == "900334689"
    assert territory["fecha_desde"] == "2026-01-01"
    assert territory["departamento_resolved"] == "Córdoba"
    assert territory.get("entidad_resolved") is None


def test_actor_buyer_relation_arbitrates_actor_and_topic(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("Cédula de Ciudadanía", "10000001", "JOSE CANTILLO", 5),
        _row("Cédula de Ciudadanía", "10000002", "JOSE CANTILLO", 4),
    ])
    actor = _resolve(
        "contrato de jose cantillo con alcaldia de barranquilla", resolver
    )
    topic = dict(QueryRouter().parse(
        "contrato de mantenimiento con alcaldia de barranquilla"
    ).params)

    assert actor["actor_pending"]["actor_name"] == "jose cantillo"
    assert actor["entidad_resolved"].startswith("DISTRITO ESPECIAL")
    assert actor.get("objeto") is None
    assert topic["objeto"] == ["mantenimiento"]
    assert topic.get("actor_scope") is None
    assert topic["entidad_resolved"].startswith("DISTRITO ESPECIAL")


def test_generic_governacion_is_clarified_not_guessed(tmp_path):
    resolver = _resolver(tmp_path, [_row("NIT", "900111222", "ACTOR ALFA")])
    params = _resolve("que contratos tiene ACTOR ALFA con gobernacion?", resolver)
    decision = evaluate_actor_scope(params)

    assert decision.should_execute is False
    assert decision.reason == "actor_relation_buyer_ambiguous"
    assert params.get("entidad_resolved") is None
    assert "cuál gobernacion" in (decision.clarification_question or "").lower()


@pytest.mark.parametrize(
    ("query", "expected_type"),
    [
        ("mirame que ha hecho esta persona 1140835135", "UNKNOWN"),
        ("mirame que ha hecho la persona con cedula 1140835135", "CC"),
        ("que ha hecho la persona con cédula 1140835135", "CC"),
        ("que ha hecho CC 1140835135", "CC"),
        ("que ha hecho c.c. 1140835135", "CC"),
    ],
)
def test_person_language_does_not_infer_type_by_length(query, expected_type):
    params = dict(QueryRouter().parse(query).params)

    assert params["actor_scope"]["document_type"] == expected_type
    assert params.get("objeto") in (None, [])


@pytest.mark.parametrize(
    ("name", "document", "word_count"),
    [
        (
            "EMPRESA PARA LA COMPETITIVIDAD TERRITORIAL DEL MUNICIPIO DE GIRARDOTA",
            "901464458", 9,
        ),
        (
            "EMPRESA DE DESARROLLO URBANO Y RURAL DEL MUNICIPIO DE BELLO",
            "901465133", 10,
        ),
        (
            "FONDO MIXTO PARA LA PROMOCION DE LA CULTURA Y LAS ARTES DE BOYACA",
            "820000107", 13,
        ),
    ],
)
def test_long_strong_actor_names_use_structural_boundaries(
    tmp_path, name, document, word_count,
):
    assert len(name.split()) == word_count
    resolver = _resolver(tmp_path, [_row("NIT", document, name, 20)])

    params = _resolve(f"contratos del proveedor {name}", resolver)

    assert is_authoritative_actor_resolution(params)
    assert params["actor_scope"]["base_number"] == document
    assert params["actor_scope"]["observed_name"] == name.lower()


def test_actor_replacement_purges_old_identity_and_preserves_orthogonal_year(
    tmp_path,
):
    workflow = _workflow(tmp_path, [
        _row("NIT", "900111222", "ACTOR ALFA", 8),
        _row("NIT", "900333444", "ACTOR BETA", 7),
    ])
    workflow.run_query("busca proveedor ACTOR ALFA", chat_id="replace")
    workflow.run_query("solo 2025", chat_id="replace")

    replaced = workflow.run_query(
        "cambia a ACTOR BETA", chat_id="replace"
    )["resolved_params"]

    assert replaced["actor_scope"]["base_number"] == "900333444"
    assert "900111222" not in replaced["contratista"]
    assert replaced["fecha_desde"] == "2025-01-01"
    assert replaced.get("objeto") in (None, [])


def test_purge_actor_state_removes_every_identity_and_provenance_field():
    old_actor = {
        "contratista": ["900111222"],
        "actor_scope": {"base_number": "900111222"},
        "actor_pending": {"candidates": []},
        "actor_intent_conflict": {"reason": "old"},
        "actor_resolution": "exact_unique_alias",
        "actor_resolution_method": "exact_unique_alias",
        "actor_resolution_status": "RESOLVED",
        "actor_selection_rank": 2,
        "actor_conversation_probe": "actor alfa",
        "actor_conversation_operation": "SET_ACTOR",
        "actor_conversation_construction": "natural_search_actor",
        "fecha_desde": "2025-01-01",
    }

    purged = purge_actor_state(old_actor)

    assert purged == {"fecha_desde": "2025-01-01"}


def test_legal_form_variant_unique_has_explicit_authority(tmp_path):
    resolver = _resolver(
        tmp_path, [_row("NIT", "900111222", "ACME S.A.S.", 10)]
    )

    params = _resolve("contratos del proveedor ACME SAS", resolver)

    assert params["actor_resolution_status"] == ActorResolutionStatus.RESOLVED
    assert params["actor_resolution_method"] == METHOD_EXACT_LEGAL_FORM_VARIANT
    assert is_authoritative_actor_resolution(params)


def test_legal_form_variant_conflict_never_auto_resolves(tmp_path):
    resolver = _resolver(tmp_path, [
        _row("NIT", "900111222", "ACME S.A.S.", 10),
        _row("NIT", "900333444", "ACME S.A.S.", 9),
    ])

    params = _resolve("contratos del proveedor ACME SAS", resolver)

    assert params["actor_resolution_status"] == ActorResolutionStatus.AMBIGUOUS
    assert params.get("contratista") is None
    assert not is_authoritative_actor_resolution(params)
    assert compute_query_plan(params, "contratos del proveedor ACME SAS").should_execute is False


def test_fuzzy_candidates_require_independent_actor_evidence(tmp_path):
    resolver = _resolver(
        tmp_path, [_row("NIT", "900111222", "SOFTWARE HOUSE SAS", 8)]
    )
    query = "contratos de software"
    baseline = dict(QueryRouter().parse(query).params)

    params = _resolve(query, resolver)

    assert params == baseline
    assert params["objeto"] == ["software"]
    assert params.get("actor_pending") is None


@pytest.mark.parametrize(
    "query",
    [
        "busca contratos de aseo en huila",
        "busca procesos de educacion en bogota",
        "busca licitacion publica de obras",
        "busca contratacion directa que incluye suministro",
    ],
)
def test_explicit_new_search_grammar_is_not_an_actor_operation(query):
    params = dict(QueryRouter().parse(query).params)

    assert params.get("actor_resolution") is None
    assert params.get("actor_conversation_operation") is None


def test_money_unit_in_modifier_tail_is_not_actor_shape_evidence(tmp_path):
    resolver = _resolver(
        tmp_path, [_row("NIT", "900111222", "UNRELATED ACTOR SAS", 8)]
    )
    query = "contratos de alimentacion mayores a 1000 SMMLV en 2026"

    params = _resolve(query, resolver)

    assert params["objeto"] == ["alimentacion"]
    assert params.get("actor_resolution") is None
    assert params["valor_min"] == 1_750_905_000


def test_article_does_not_let_buyer_prefix_consume_complete_actor_name(tmp_path):
    resolver = _resolver(
        tmp_path, [_row("NIT", "900111222", "UNRELATED ACTOR SAS", 8)]
    )
    query = "contratos de LA PREVISORA S.A. COMPAÑÍA DE SEGUROS"

    params = _resolve(query, resolver)

    assert params["actor_resolution_method"] == "name_unresolved"
    assert params.get("entidad_resolved") is None
    assert params.get("objeto") is None
    assert compute_query_plan(params, query).should_execute is False
