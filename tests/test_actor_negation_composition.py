"""Un marcador negativo gobierna una COLECCIÓN de actores, no uno solo.

`sin el proveedor A ni el proveedor B; del proveedor C` tiene tres actores y
dos polaridades. La representación anterior era singular: el alcance negado
terminaba en el primer documento acreditado, así que B escapaba del marcador y
el extractor positivo lo recogía como si el usuario lo hubiera pedido. El
resultado no era "de menos": era el complemento exacto de lo pedido, y además
desplazaba a C.

Las propiedades que fija este archivo no son sobre el WHERE —la negación de
ACTOR es CLARIFY, nunca se ejecuta— sino sobre el estado resuelto: ningún
actor negado puede reaparecer como positivo, el actor positivo se conserva,
cada actor negado es un request propio, y la aclaración nombra la colección
entera.
"""
from __future__ import annotations

import re

import pytest
from hypothesis import given, settings, strategies as st

from app.config import Settings
from app.core import actor_negation_group as actor_group_module
from app.core.actor_negation_group import ActorAtom, ActorNegationGroup
from app.core.conversation_store import ConversationStore
from app.core.negation_binding import (
    NegationDimension,
    NegationRequest,
    NegationVerdict,
    bind_negations,
    negation_clarification_question,
)
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import _CITY_PATTERN_RE, QueryRouter


router = QueryRouter()

NEGATED_A = "900711074"
NEGATED_B = "79482763"
NEGATED_C = "830053105"
POSITIVE = "800197268"

_POSITIVE_KEYS = ("actor_scope", "actor_resolution", "contratista")


def _actor_requests(parsed) -> tuple:
    return tuple(
        request for request in parsed.negation_requests
        if request.dimension is NegationDimension.ACTOR
    )


def _negated_numbers(parsed) -> list[str]:
    """Los documentos que quedaron negados, en el orden en que se emitieron."""
    return [
        number
        for request in _actor_requests(parsed)
        for number in re.findall(r"\d{5,}", request.value)
    ]


def _assert_never_positive(parsed, *numbers: str) -> None:
    """Ningún actor negado sobrevive en ninguna autoridad positiva."""
    scope = parsed.params.get("actor_scope") or {}
    contratista = parsed.params.get("contratista") or []
    for number in numbers:
        assert scope.get("base_number") != number
        assert number not in contratista
        assert number not in parsed.positive_text


def _recording_workflow(tmp_path):
    settings = Settings(
        _env_file=None,
        deepseek_api_key=None,
        secop_feedback_path=tmp_path / "traces.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
    )
    workflow = SecopalWorkflow(settings)
    workflow.conv_store = ConversationStore(tmp_path / "conversations")
    calls: list[tuple[str, str]] = []

    def record(operation):
        def inner(dataset_id, _soql_query, **_kwargs):
            calls.append((operation, dataset_id))
            return [] if operation != "count" else 0

        return inner

    workflow.secop_client.query = record("query")
    workflow.secop_client.count = record("count")
    workflow.secop_client.aggregate = record("aggregate")
    return workflow, calls


# ── Cardinalidad de la colección: 1, 2 y 3 actores ───────────────────────

@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "contratos sin proveedor NIT 900711074",
            [NEGATED_A],
        ),
        (
            "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763",
            [NEGATED_A, NEGATED_B],
        ),
        (
            "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763 "
            "ni proveedor NIT 830053105",
            [NEGATED_A, NEGATED_B, NEGATED_C],
        ),
        (
            "contratos sin el proveedor NIT 900711074, el proveedor CC 79482763 "
            "ni el proveedor NIT 830053105",
            [NEGATED_A, NEGATED_B, NEGATED_C],
        ),
    ],
)
def test_every_coordinated_actor_is_its_own_negated_request(
    query: str, expected: list[str],
) -> None:
    parsed = router.parse(query)

    assert _negated_numbers(parsed) == expected
    assert len(_actor_requests(parsed)) == len(expected)
    _assert_never_positive(parsed, *expected)
    assert all(not parsed.params.get(key) for key in _POSITIVE_KEYS)


def test_a_collapsed_collection_would_not_satisfy_the_contract() -> None:
    """Un request con la colección entera enmascara bien y niega mal."""
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763"
    )
    requests = _actor_requests(parsed)

    assert len(requests) == 2
    for request in requests:
        # Ningún valor transporta a dos actores a la vez: eso sería un blob
        # que ninguna autoridad de identidad puede resolver después.
        assert not (NEGATED_A in request.value and NEGATED_B in request.value)
        assert request.verdict is NegationVerdict.CLARIFY
    assert requests[0].context is None
    assert requests[1].context == "actor_group"
    assert requests[0].marker_start == requests[1].marker_start


# ── Dos polaridades bajo la misma frase ──────────────────────────────────

@pytest.mark.parametrize(
    "query",
    [
        "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763; "
        "del proveedor NIT 800197268",
        "contratos del proveedor NIT 800197268; sin proveedor NIT 900711074 "
        "ni proveedor CC 79482763",
        "menos de 5 contratos del proveedor NIT 800197268; sin proveedor "
        "NIT 900711074 ni proveedor CC 79482763",
        "contratos sin proveedor NIT 900711074 y del proveedor NIT 800197268",
    ],
)
def test_the_positive_actor_survives_the_negated_collection(query: str) -> None:
    parsed = router.parse(query)

    _assert_never_positive(parsed, NEGATED_A)
    assert parsed.params["actor_scope"]["base_number"] == POSITIVE
    assert parsed.params["contratista"] == [POSITIVE]
    assert parsed.params["actor_resolution"] == "explicit_document"
    assert POSITIVE not in "".join(
        request.value for request in _actor_requests(parsed)
    )


def test_a_semicolon_is_not_a_coordinator() -> None:
    """`;` cierra la colección aunque lo que siga sea otro actor."""
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074; proveedor NIT 800197268"
    )
    assert len(_actor_requests(parsed)) == 1
    assert NEGATED_A in _actor_requests(parsed)[0].value
    assert parsed.params["actor_scope"]["base_number"] == POSITIVE


def test_a_preposition_opens_a_positive_constituent_without_punctuation() -> None:
    """Sin puntuación, `del` es la única frontera que protege al positivo."""
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763 "
        "del proveedor NIT 800197268"
    )
    assert _negated_numbers(parsed) == [NEGATED_A, NEGATED_B]
    assert parsed.params["actor_scope"]["base_number"] == POSITIVE


# ── La colección no depende de qué marcador la abrió ─────────────────────

@pytest.mark.parametrize(
    "query",
    [
        "contratos sin proveedor NIT 900711074 ni proveedor NIT 830053105",
        "contratos excepto proveedor NIT 900711074 ni proveedor NIT 830053105",
        "contratos salvo el proveedor NIT 900711074 y el proveedor NIT 830053105",
        "contratos donde el proveedor no sea NIT 900711074 ni NIT 830053105",
        "contratos donde el proveedor pero no NIT 900711074 ni NIT 830053105",
    ],
)
def test_every_negative_marker_governs_the_whole_collection(query: str) -> None:
    parsed = router.parse(query)

    assert _negated_numbers(parsed) == [NEGATED_A, NEGATED_C]
    _assert_never_positive(parsed, NEGATED_A, NEGATED_C)


# ── Aclaración: la colección entera, no su primer elemento ───────────────

def test_the_clarification_names_every_negated_actor() -> None:
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763 "
        "ni proveedor NIT 830053105"
    )
    question = negation_clarification_question(parsed.negation_requests)

    assert NEGATED_A in question
    assert NEGATED_B in question
    assert NEGATED_C in question
    assert "proveedores o contratistas" in question
    assert "no puedo" in question


def test_the_single_actor_clarification_is_unchanged() -> None:
    parsed = router.parse("contratos sin proveedor NIT 900711074")
    question = negation_clarification_question(parsed.negation_requests)

    assert question.startswith("Entendí que quieres excluir al proveedor o contratista")
    assert NEGATED_A in question


def test_a_negated_collection_blocks_before_every_source_call(tmp_path) -> None:
    workflow, calls = _recording_workflow(tmp_path)

    result = workflow.run_query(
        "contratos sin el proveedor NIT 900711074 ni el proveedor CC 79482763",
        channel="streamlit",
    )

    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "unsupported_negation"
    assert result["soql_query"] == ""
    assert calls == []
    assert all(not result["resolved_params"].get(key) for key in _POSITIVE_KEYS)
    question = result["query_plan"]["clarification_question"]
    assert NEGATED_A in question
    assert NEGATED_B in question


# ── Documentos inválidos dentro de la colección ──────────────────────────

def test_a_partial_coordinate_never_joins_the_collection() -> None:
    text = "contratos sin proveedor nit 900711074 ni proveedor cc 79482763"
    first = text.index(NEGATED_A)
    second = text.index(NEGATED_B)
    binding = bind_negations(
        text,
        city_pattern=_CITY_PATTERN_RE,
        actor_document_spans=(
            (first, first + len(NEGATED_A)),
            (second + 1, second + len(NEGATED_B)),
        ),
    )
    actor = [
        request for request in binding.requests
        if request.dimension is NegationDimension.ACTOR
    ]
    assert len(actor) == 1
    assert NEGATED_B not in actor[0].value


@pytest.mark.parametrize(
    "spans",
    [
        ((-1, 8),),
        ((100, 120),),
        ((True, 26),),
        ((10, 10),),
    ],
)
def test_invalid_coordinates_cannot_open_a_collection(spans) -> None:
    binding = bind_negations(
        "contratos sin alimentacion ni transporte",
        city_pattern=_CITY_PATTERN_RE,
        actor_document_spans=spans,
    )
    assert {request.dimension for request in binding.requests} == {
        NegationDimension.OBJECT,
    }


# ── Mutaciones: cada mitad del defecto tiene que morir ───────────────────

def test_mutation_singular_boundary_revives_the_second_actor(monkeypatch) -> None:
    """Volver a un solo actor bajo el marcador: B reaparece como positivo."""
    monkeypatch.setattr(
        actor_group_module, "_continues_collection", lambda _gap, _mention: False,
    )
    parsed = QueryRouter().parse(
        "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763"
    )
    assert parsed.params.get("actor_scope", {}).get("base_number") == NEGATED_B
    assert len(_actor_requests(parsed)) == 1


def test_mutation_collapsing_the_collection_loses_the_second_actor(monkeypatch) -> None:
    """Enmascarar bien pero emitir un solo request pierde a B en la aclaración."""
    real = actor_group_module.locate_group

    def collapsed(text, **kwargs):
        group = real(text, **kwargs)
        if group is None or not group.governed or len(group.atoms) < 2:
            return group
        return ActorNegationGroup(
            atoms=(ActorAtom(
                group.atoms[0].mention_start,
                group.atoms[-1].document_start,
                group.atoms[-1].document_end,
            ),),
            governed=True,
        )

    monkeypatch.setattr(actor_group_module, "locate_group", collapsed)
    parsed = QueryRouter().parse(
        "contratos sin proveedor NIT 900711074 ni proveedor CC 79482763"
    )
    requests = _actor_requests(parsed)
    # La mutación sigue enmascarando a los dos: el daño es el blob.
    _assert_never_positive(parsed, NEGATED_A, NEGATED_B)
    assert len(requests) == 1
    assert NEGATED_A in requests[0].value and NEGATED_B in requests[0].value


def test_mutation_without_the_preposition_veto_swallows_the_positive_actor(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        actor_group_module, "_ACTOR_MENTION_PREPOSITION_RE", re.compile(r"(?!)"),
    )
    parsed = QueryRouter().parse(
        "contratos sin proveedor NIT 900711074 y del proveedor NIT 800197268"
    )
    assert parsed.params.get("actor_scope") is None
    assert POSITIVE in "".join(
        request.value for request in _actor_requests(parsed)
    )


def test_mutation_accepting_any_coordination_gap_crosses_a_boundary(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        actor_group_module, "_ACTOR_COORDINATION_RE", re.compile(r"^.*$", re.DOTALL),
    )
    parsed = QueryRouter().parse(
        "contratos sin proveedor NIT 900711074; proveedor NIT 800197268"
    )
    assert parsed.params.get("actor_scope") is None


# ── Propiedad: número, orden y tipo de documento ─────────────────────────

_DOCUMENTS = (
    ("NIT", "900711074"),
    ("CC", "79482763"),
    ("NIT", "830053105"),
    ("cedula", "1140835135"),
)


@settings(max_examples=40, deadline=None)
@given(
    chosen=st.lists(
        st.sampled_from(_DOCUMENTS), min_size=1, max_size=3, unique_by=lambda item: item[1],
    ),
    coordinator=st.sampled_from((" ni ", " y ", " o ", ", ")),
    role=st.sampled_from(("proveedor", "contratista", "el proveedor")),
    with_positive=st.booleans(),
)
def test_any_ordered_collection_stays_negated_and_separate(
    chosen: list[tuple[str, str]],
    coordinator: str,
    role: str,
    with_positive: bool,
) -> None:
    mentions = [f"{role} {label} {number}" for label, number in chosen]
    query = "contratos sin " + coordinator.join(mentions)
    if with_positive:
        query += f"; del proveedor NIT {POSITIVE}"
    parsed = router.parse(query)

    numbers = [number for _label, number in chosen]
    assert _negated_numbers(parsed) == numbers
    assert len(_actor_requests(parsed)) == len(numbers)
    _assert_never_positive(parsed, *numbers)
    if with_positive:
        assert parsed.params["actor_scope"]["base_number"] == POSITIVE
    else:
        assert parsed.params.get("actor_scope") is None

    question = negation_clarification_question(parsed.negation_requests)
    assert all(number in question for number in numbers)


def test_the_first_request_carries_its_structural_mention() -> None:
    """El primer átomo empieza donde empieza el marcador, no en el número."""
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 ni proveedor NIT 830053105"
    )
    requests = _actor_requests(parsed)

    assert requests[0].value == "proveedor nit 900711074"
    assert requests[1].value == "proveedor nit 830053105"


def test_two_markers_are_two_exclusions_not_one_collection() -> None:
    """La colección es de UN marcador; dos marcadores no se funden."""
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 sin proveedor NIT 830053105"
    )
    requests = _actor_requests(parsed)
    assert len({request.marker_start for request in requests}) == 2

    question = negation_clarification_question(parsed.negation_requests)
    assert "proveedores o contratistas" not in question
    assert "no puedo ejecutarlas juntas" in question


def test_a_mixed_blocked_set_is_never_read_as_an_actor_collection() -> None:
    """El mensaje de colección exige que TODO lo bloqueado sea ACTOR.

    Se construye a mano porque un solo marcador que reparta ACTOR y otra
    dimensión no es alcanzable hoy desde el router; el contrato de la función
    no puede depender de esa suerte.
    """
    def request(dimension: NegationDimension, value: str) -> NegationRequest:
        return NegationRequest(
            dimension=dimension, value=value, value_start=0, value_end=1,
            marker="sin", marker_start=10, marker_end=13,
            verdict=NegationVerdict.CLARIFY,
        )

    question = negation_clarification_question([
        request(NegationDimension.ACTOR, "proveedor nit 900711074"),
        request(NegationDimension.CITY, "cali"),
    ])

    assert "proveedores o contratistas" not in question
    assert "cali" not in question


def test_a_repeated_actor_is_named_once() -> None:
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 ni proveedor NIT 900711074"
    )
    question = negation_clarification_question(parsed.negation_requests)

    assert question.count(NEGATED_A) == 1


def test_a_closing_parenthesis_belongs_to_the_atom_that_opened_it() -> None:
    parsed = router.parse(
        "contratos sin proveedor NIT 900711074 (proveedor NIT 830053105)"
    )
    for request in _actor_requests(parsed):
        assert request.value.count(")") == request.value.count("(")


@pytest.mark.parametrize(
    "spans",
    [
        ((True, 10),),
        ((1, 10),),
    ],
)
def test_booleans_are_not_coordinates(spans) -> None:
    """`True == 1`: sin el guard de bool, un span booleano acredita texto.

    Se prueba en el módulo dueño porque `bind_negations` nunca deja llegar un
    span que empiece antes del marcador; ahí el guard es inobservable.
    """
    text = "a900711074 contratos"
    validated = actor_group_module.validate_document_spans(text, spans)
    assert validated == (() if isinstance(spans[0][0], bool) else ((1, 10),))


# ── Frontera declarada: el residuo es un vocabulario, no dos casos ───────
#
# La colección se reconoce cuando el marcador gobierna actores Y el enlace
# está en un conjunto enumerado. Los dos enumerados son el residuo:
#
#   ROL      `contratistas` abre colección y `proveedores` no. La diferencia
#            no es la pluralidad: el normalizador lematiza el primero y no el
#            segundo. `adjudicatarios` tampoco.
#   ENLACE   `ni`, `y`, `o` continúan la colección. `tampoco`, `ademas de`,
#            `e`, `u` y un artículo suelto ante el número (`ni el 79482763`)
#            no, y el actor que sigue reaparece como positivo.
#
# El peor caso NO se queda en aclarar: EJECUTA con la polaridad invertida.
# "contratos sin proveedores NIT 900711074 ni 79482763" resuelve
# contratista=['900711074'] y sale a la fuente; el usuario pidió excluir a
# dos proveedores y recibe los contratos del primero. Es la familia que
# ADR-069 prohíbe, y ningún vocabulario más la cierra: cerrarla pide un sello
# de polaridad sobre el alcance del marcador, como `_seal_negated_state` hace
# con STATE. Queda fuera de este foundation a propósito.
#
# Todo lo de abajo reproduce igual en la base de esta rama y en main.

@pytest.mark.xfail(
    strict=True,
    reason=(
        "FRONTERA: un rol que el normalizador no lematiza no abre colección; "
        "el actor negado reaparece positivo Y el turno ejecuta"
    ),
)
def test_an_unlemmatized_role_still_inverts_the_polarity() -> None:
    parsed = router.parse("contratos sin proveedores NIT 900711074 ni 79482763")
    _assert_never_positive(parsed, NEGATED_A, NEGATED_B)


def test_a_lemmatized_plural_role_does_open_a_collection() -> None:
    """La pluralidad no es la causa del hueco de arriba; la lematización sí."""
    parsed = router.parse(
        "contratos sin contratistas NIT 900711074 ni NIT 830053105"
    )
    assert _negated_numbers(parsed) == [NEGATED_A, NEGATED_C]
    _assert_never_positive(parsed, NEGATED_A, NEGATED_C)


@pytest.mark.parametrize(
    "linker",
    [" tampoco ", " ademas de ", " e ", " u ", " ni el "],
)
@pytest.mark.xfail(
    strict=True,
    reason="FRONTERA: enlace fuera del conjunto enumerado; el actor que sigue revive",
)
def test_an_unenumerated_linker_still_loses_the_next_actor(linker: str) -> None:
    parsed = router.parse(
        f"contratos sin proveedor NIT 900711074{linker}79482763"
    )
    _assert_never_positive(parsed, NEGATED_A, NEGATED_B)


@pytest.mark.xfail(
    strict=True,
    reason="FRONTERA: una colección que empieza en BUYER no alcanza al actor",
)
def test_a_buyer_led_collection_still_loses_its_actor() -> None:
    parsed = router.parse(
        "contratos sin la alcaldia de cali ni el proveedor NIT 900711074"
    )
    _assert_never_positive(parsed, NEGATED_A)
