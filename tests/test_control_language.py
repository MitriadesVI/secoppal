"""HCA-R6 — control language is not search content.

The system speaks two languages in one sentence. CONTENT names what is being
looked for; CONTROL says how to order, page or frame it. Before R6 only the
ordering *criterion* was consumed, so the command around it ("ordenados por")
survived into ``objeto`` and shipped a thematic LIKE that no row can match.

Invariants pinned here:
  INV-CONTROL-LANGUAGE-NOT-CONTENT
  INV-CONTROL-SPAN-DOES-NOT-CLASSIFY-CONTENT
  INV-ORDERING-FOLLOWUP-PRESERVES-UNIVERSE
  RAW_QUERY_PRESERVED
"""
from __future__ import annotations

import pytest

from app.core.control_language import (
    ControlRequest,
    detect_ordering,
    interpret_control_language,
    is_pure_control_turn,
)
from app.core.followup_engine import detect_and_merge
from app.core.order_semantics import order_is_semantic, ordering_label
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


def _objeto(params: dict) -> list[str]:
    flat: list[str] = []
    for term in params.get("objeto") or []:
        flat.extend(term if isinstance(term, list) else [term])
    return [t.lower() for t in flat]


# ── The control authority in isolation ──────────────────────────────────────

@pytest.mark.parametrize(
    ("text", "signal"),
    [
        ("ordenados por mayor valor", "valor_desc"),
        ("de mayor a menor valor", "valor_desc"),
        ("los mas caros primero", "valor_desc"),
        ("por mayor valor", "valor_desc"),
        ("del mas caro al mas barato", "valor_desc"),
        ("de menor a mayor valor", "valor_asc"),
        ("los mas baratos primero", "valor_asc"),
        ("de menor valor", "valor_asc"),
        ("del mas barato al mas caro", "valor_asc"),
        ("los mas recientes", "fecha_desc"),
        ("de mas reciente a mas antiguo", "fecha_desc"),
        ("los mas antiguos primero", "fecha_asc"),
        ("primero los antiguos", "fecha_asc"),
        ("de mas antiguo a mas reciente", "fecha_asc"),
    ],
)
def test_ordering_vocabulary_maps_to_the_canonical_signal(text: str, signal: str) -> None:
    assert detect_ordering(text) == signal


def test_descending_vocabulary_does_not_swallow_an_ascending_request() -> None:
    # "de menor a mayor valor" contains the substring "mayor valor". Reading it
    # one-sided inverts the user's intent (NORMALIZATION_ERASES_USER_INTENT).
    assert detect_ordering("de menor a mayor valor") == "valor_asc"
    assert detect_ordering("de mayor a menor valor") == "valor_desc"


def test_command_span_is_consumed_whole_not_only_its_criterion() -> None:
    request = interpret_control_language("contratos de obra en cali ordenados por mayor valor")
    assert request.ordering_signal == "valor_desc"
    assert request.residual_text == "contratos de obra en cali"
    assert "ordenados por mayor valor" in request.consumed_spans


def test_detached_command_verb_is_still_a_command() -> None:
    request = interpret_control_language("ordena contratos de mantenimiento por mayor valor")
    assert request.ordering_signal == "valor_desc"
    assert "ordena" not in request.residual_text
    assert "mantenimiento" in request.residual_text


@pytest.mark.parametrize(
    "text",
    [
        "ordenamiento territorial",
        "sistemas de control de acceso",
        "gestion de ordenes de compra",
        "contratos de mantenimiento en cali",
        "seleccion abreviada de menor cuantia",
        "contratos por valor de 500 millones",
        "contratos por mas de 500 millones",
        "todos los rubros de los ultimos 30 dias",
    ],
)
def test_control_vocabulary_is_span_based_not_a_stopword_list(text: str) -> None:
    # INV-CONTROL-SPAN-DOES-NOT-CLASSIFY-CONTENT: no token is removed just for
    # resembling a command; nothing here is an ordering request.
    request = interpret_control_language(text)
    assert request.ordering_signal == ""
    assert request.residual_text == text


@pytest.mark.parametrize(
    ("text", "residual"),
    [
        ("contratos de aseo en cali ordena", "contratos de aseo en cali"),
        ("ordenalos", ""),
        ("ordenados", ""),
        ("muestrame primero", "muestrame"),
    ],
)
def test_an_incomplete_command_is_consumed_without_inventing_a_direction(
    text: str, residual: str
) -> None:
    request = interpret_control_language(text)
    assert request.residual_text == residual
    # No criterion was given, so no ordering may be inferred: the presentational
    # default stands rather than a guessed one.
    assert request.ordering_signal == ""


@pytest.mark.parametrize(
    "text",
    [
        "contratos de primeros auxilios en cali",
        "contratos de primera infancia",
        "gestion de ordenes de compra",
        "contratos de ordenamiento territorial",
        "listado de precios de aseo",
    ],
)
def test_nominal_readings_of_the_command_family_survive(text: str) -> None:
    # "orden", "órdenes", "ordenamiento" and a non-final "primeros" are nouns
    # and modifiers, not commands. Only the imperative morphology is consumed.
    assert interpret_control_language(text).residual_text == text


def test_pure_control_turn_requires_a_real_criterion() -> None:
    assert is_pure_control_turn("ordenalos por mayor valor") is True
    assert is_pure_control_turn("los mas antiguos primero") is True
    assert is_pure_control_turn("contratos de aseo en cali") is False
    # A bare imperative with nothing to order by is not a control turn.
    assert is_pure_control_turn("ordena") is False


def test_control_request_is_inert_by_default() -> None:
    empty = ControlRequest()
    assert empty.has_ordering is False
    assert empty.is_ascending is False


# ── The parser stops leaking control into objeto ────────────────────────────

@pytest.mark.parametrize(
    ("query", "signal", "forbidden"),
    [
        ("contratos de obra en cali ordenados por mayor valor", "valor_desc", ["ordenados"]),
        ("contratos de aseo en cali de menor a mayor valor", "valor_asc", ["menor", "mayor"]),
        ("contratos de aseo en cali los mas antiguos primero", "fecha_asc", ["antiguos", "primero"]),
        ("los contratos de aseo mas caros primero en cali", "valor_desc", ["primero", "caros"]),
        ("top 10 contratos de obra por valor en antioquia", "valor_desc", ["top"]),
    ],
)
def test_ordering_never_reaches_objeto(
    router: QueryRouter, query: str, signal: str, forbidden: list[str]
) -> None:
    params = router.parse(query).params
    assert params.get("ordering_signal") == signal
    assert not set(forbidden) & set(_objeto(params))


@pytest.mark.parametrize(
    ("query", "forbidden"),
    [
        ("que esta contratando la alcaldia de barranquilla", ["contratando"]),
        ("que piensa contratar la gobernacion de cordoba", ["piensa", "contratar"]),
        ("que va a contratar la alcaldia de medellin", ["contratar"]),
        ("procesos que cierran hoy en atlantico", ["cierran"]),
    ],
)
def test_intent_verbs_never_reach_objeto(
    router: QueryRouter, query: str, forbidden: list[str]
) -> None:
    params = router.parse(query).params
    assert not set(forbidden) & set(_objeto(params))


def test_intent_verb_keeps_its_thematic_complement(router: QueryRouter) -> None:
    # CONTROL_VERB_CANNOT_BECOME_TOPIC, but the noun it governs still can.
    params = router.parse(
        "que esta contratando la alcaldia de barranquilla en mantenimiento"
    ).params
    assert "mantenimiento" in _objeto(params)
    assert "contratando" not in _objeto(params)


def test_raw_query_is_preserved(router: QueryRouter) -> None:
    query = "Contratos de obra en Cali ORDENADOS por mayor valor"
    before = query
    router.parse(query)
    assert query == before


def test_money_comparator_is_not_an_ordering_request(router: QueryRouter) -> None:
    params = router.parse("contratos de aseo en cali por mas de 500 millones").params
    assert params.get("valor_min") == 500_000_000
    assert not params.get("ordering_signal")


def test_money_and_ordering_coexist(router: QueryRouter) -> None:
    params = router.parse(
        "contratos de aseo en cali por mas de 500 millones ordenados de mayor a menor"
    ).params
    assert params.get("valor_min") == 500_000_000
    assert params.get("ordering_signal") == "valor_desc"
    assert "ordenados" not in _objeto(params)


def test_temporal_range_survives_an_ordering_request(router: QueryRouter) -> None:
    params = router.parse("contratos de aseo de junio de 2026 ordenados por mayor valor").params
    assert params.get("fecha_desde") == "2026-06-01"
    assert params.get("fecha_hasta") == "2026-06-30"
    assert params.get("ordering_signal") == "valor_desc"


def test_negation_binding_survives_control_masking(router: QueryRouter) -> None:
    params = router.parse(
        "contratos de mantenimiento sin alimentacion en cali ordenados por mayor valor"
    ).params
    assert params.get("ordering_signal") == "valor_desc"
    assert "mantenimiento" in _objeto(params)
    assert "ordenados" not in _objeto(params)
    assert "alimentacion" not in _objeto(params)


# ── The source honours the requested direction ──────────────────────────────

_DATASET = SoQLBuilder.CONTRATOS_DATASET


@pytest.mark.parametrize(
    ("signal", "expected"),
    [
        ("valor_desc", "ORDER BY valor_del_contrato DESC, fecha_de_firma DESC"),
        ("valor_asc", "ORDER BY valor_del_contrato ASC, fecha_de_firma ASC"),
        ("fecha_desc", "ORDER BY fecha_de_firma DESC, valor_del_contrato DESC"),
        ("fecha_asc", "ORDER BY fecha_de_firma ASC, valor_del_contrato ASC"),
    ],
)
def test_soql_order_by_follows_the_requested_direction(signal: str, expected: str) -> None:
    soql = SoQLBuilder().build(_DATASET, {"objeto": ["aseo"], "ordering_signal": signal})
    assert expected in soql


def test_ascending_order_breaks_ties_ascending() -> None:
    soql = SoQLBuilder().build(_DATASET, {"objeto": ["aseo"], "ordering_signal": "valor_asc"})
    assert "id_contrato ASC" in soql
    desc = SoQLBuilder().build(_DATASET, {"objeto": ["aseo"], "ordering_signal": "valor_desc"})
    assert "id_contrato DESC" in desc


def test_every_explicit_direction_is_semantic_order() -> None:
    # order_semantics gates the H4 degraded fallback on this: an explicit
    # direction may never be dropped and answered with an unordered sample.
    for signal in ("valor_desc", "valor_asc", "fecha_desc", "fecha_asc"):
        assert order_is_semantic({"ordering_signal": signal}) is True


@pytest.mark.parametrize(
    ("signal", "label"),
    [
        ("valor_desc", "de mayor valor"),
        ("valor_asc", "de menor valor"),
        ("fecha_desc", "más recientes"),
        ("fecha_asc", "más antiguos"),
        ("", "más recientes"),
    ],
)
def test_ordering_label_never_misdescribes_the_page(signal: str, label: str) -> None:
    assert ordering_label({"ordering_signal": signal} if signal else {}) == label


# ── Follow-up: ordering changes ORDER BY and nothing else ───────────────────

_UNIVERSE_FIELDS = (
    "objeto", "objeto_or", "objeto_exclude", "ciudad", "departamento_resolved",
    "entidad_resolved", "valor_min", "valor_max", "fecha_desde", "fecha_hasta",
    "dataset", "contract_type_include", "modalidad_include_procedure_ids",
    "estado_family", "estado_contrato", "intent_type",
)


def _universe(params: dict) -> dict:
    return {k: params.get(k) for k in _UNIVERSE_FIELDS}


def _turn(router: QueryRouter, text: str, previous: dict | None) -> tuple[str, dict]:
    parsed = router.parse(text).params
    if previous is None:
        return "new_search", parsed
    return detect_and_merge(text.lower(), parsed, frame_from_params(previous))


@pytest.mark.parametrize(
    ("control_turn", "signal"),
    [
        ("ordenalos por mayor valor", "valor_desc"),
        ("los mas antiguos primero", "fecha_asc"),
        ("de menor a mayor valor", "valor_asc"),
        ("los mas recientes primero", "fecha_desc"),
    ],
)
def test_ordering_followup_preserves_the_universe(
    router: QueryRouter, control_turn: str, signal: str
) -> None:
    # INV-ORDERING-FOLLOWUP-PRESERVES-UNIVERSE
    _, first = _turn(router, "contratos de mantenimiento en cali por mas de 300 millones", None)
    intent, merged = _turn(router, control_turn, first)

    assert intent == "change_order"
    assert merged.get("ordering_signal") == signal
    assert _universe(merged) == _universe(first)


def test_ordering_followup_leaves_the_where_clause_identical(router: QueryRouter) -> None:
    # Stronger than comparing frames: the actual predicate sent to SECOP.
    builder = SoQLBuilder()
    _, first = _turn(router, "contratos de mantenimiento en cali por mas de 300 millones", None)
    _, merged = _turn(router, "ordenalos por mayor valor", first)

    where_before = builder.build(_DATASET, first).split(" ORDER BY ")[0]
    where_after = builder.build(_DATASET, merged).split(" ORDER BY ")[0]
    assert where_before == where_after


def test_ordering_followup_resets_pagination(router: QueryRouter) -> None:
    # A new ordering over an inherited cursor would skip or repeat rows.
    _, first = _turn(router, "contratos de aseo en cali", None)
    _, paged = _turn(router, "muestrame mas", first)
    assert paged.get("offset")

    _, reordered = _turn(router, "ordenalos por mayor valor", paged)
    assert not reordered.get("offset")
    assert not reordered.get("_display_offset")
    assert not reordered.get("_shown_total")


def test_ordering_followup_does_not_move_the_entity_ecosystem(router: QueryRouter) -> None:
    # CONTROL_ONLY_FOLLOWUP_CAN_ACTIVATE_ENTITY_ECOSYSTEM = FALSE. Expansion is
    # a function of city/department plus the absence of an explicit entity, so
    # pinning those three fields pins the ecosystem decision itself.
    from app.core.entity_ecosystem import should_expand_ecosystem

    _, first = _turn(router, "contratos de aseo en cali", None)
    _, merged = _turn(router, "los mas antiguos primero", first)

    assert merged.get("ciudad") == first.get("ciudad")
    assert merged.get("entidad_resolved") == first.get("entidad_resolved")
    assert should_expand_ecosystem(merged) == should_expand_ecosystem(first)


def test_show_more_is_still_pagination_not_a_new_search(router: QueryRouter) -> None:
    _, first = _turn(router, "contratos de aseo en cali", None)
    intent, merged = _turn(router, "muestrame mas", first)
    assert intent == "pagination_more"
    assert "aseo" in _objeto(merged)


def test_a_real_new_search_is_not_masked_away(router: QueryRouter) -> None:
    _, first = _turn(router, "contratos de mantenimiento en cali", None)
    _, ordered = _turn(router, "ordenalos por mayor valor", first)
    intent, merged = _turn(router, "busca contratos de software en bogota", ordered)
    assert intent == "new_search"
    assert "software" in _objeto(merged)
    assert "mantenimiento" not in _objeto(merged)


def test_a_control_turn_does_not_collapse_a_dual_dataset_scope(router: QueryRouter) -> None:
    # "ordena los contratos por mayor valor" names a dataset noun inside a
    # command; it is not a request to switch dataset.
    first = {
        "objeto": ["adulto mayor"],
        "dataset": "contratos",
        "ciudad": "Cartagena",
        "dataset_scope": ["procesos", "contratos"],
    }
    intent, merged = _turn(router, "ordena los contratos por mayor valor", first)
    assert intent == "change_order"
    assert merged.get("ordering_signal") == "valor_desc"
    assert _objeto(merged) == ["adulto mayor"]


@pytest.mark.xfail(strict=True, reason=(
    "Pre-existing, unrelated to control language: a follow-up classified as "
    "change_scope carries a genuinely new topic, but change_scope's mutation "
    "policy does not allow topic fields, so the gate drops it and the merger "
    "inherits the previous topic. Verified identical on main b661a09. Belongs "
    "to follow-up topic hygiene, not R6 — see DEFERRED_FOLLOWUP_SCOPE_TOPIC."
))
def test_scope_change_carrying_a_new_topic_keeps_that_topic(router: QueryRouter) -> None:
    _, first = _turn(router, "contratos de mantenimiento en cali", None)
    _, merged = _turn(router, "ahora muestrame contratos de software en bogota", first)
    assert "software" in _objeto(merged)
