"""Gate anti-vocabulario: la familia entera, no las formas observadas.

El defecto de fondo de este sprint es que cada dimensión semántica se reconocía
con una lista de formas superficiales mantenida a mano. Añadir a esa lista las
formas que fallaron cierra los casos y deja la clase abierta.

Por eso estas pruebas no comprueban "compró" ni "licitar": recorren la familia
declarada completa. Si mañana alguien añade un lema a la familia sin conectarlo
a todas sus ramas, aquí falla.
"""
from __future__ import annotations

import pytest

from app.core.followup_control_tokens import (
    classify_control_intent,
    requests_context_reset,
)
from app.core.intent_vocabulary import (
    BIDDER_PARTICIPATION_VERBS,
    PROCUREMENT_ACTION_VERB_FORMS,
    extract_intent,
)
from app.core.query_router import QueryRouter


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


# ── Intención de participación ───────────────────────────────────────────

@pytest.mark.parametrize("verb", BIDDER_PARTICIPATION_VERBS)
@pytest.mark.parametrize("lead", ["necesito", "quiero", "quisiera", "deseo", "para"])
def test_every_participation_verb_reaches_the_same_intent(
    verb: str, lead: str,
) -> None:
    """Misma intención, cualquier verbo de la familia, cualquier introductor."""
    config, span = extract_intent(f"{lead} {verb} suministro de uniformes en cordoba")
    assert config.get("estado_family") == "oferta_abierta", (
        f"'{lead} {verb}' no expresó intención de participar: {config}"
    )
    assert span


@pytest.mark.parametrize("verb", BIDDER_PARTICIPATION_VERBS)
def test_participation_verb_never_becomes_the_topic(
    router: QueryRouter, verb: str,
) -> None:
    params = router.parse(f"necesito {verb} suministro de uniformes en cordoba").params
    assert verb not in (params.get("objeto") or [])
    assert params.get("intent_type") == "opportunity_search"


# ── Verbos de compra: sujeto comprador, nunca objeto contractual ─────────

@pytest.mark.parametrize("verb", sorted(PROCUREMENT_ACTION_VERB_FORMS))
def test_conjugated_purchase_verb_is_never_a_topic(
    router: QueryRouter, verb: str,
) -> None:
    """Con sustantivo al lado o sin él: un verbo conjugado no es un tema."""
    for query in (
        f"que {verb} la alcaldia de pasto para salud durante 2025",
        f"que {verb} la alcaldia de pasto en 2025",
    ):
        objeto = router.parse(query).params.get("objeto") or []
        assert verb not in objeto, f"'{query}' dejó el verbo como tema: {objeto}"


@pytest.mark.parametrize(
    "verb", ["compro", "adquirio", "celebro", "suscribio", "gasto"],
)
def test_past_purchase_selects_contracts(router: QueryRouter, verb: str) -> None:
    """Una compra ya ejecutada es un contrato, no un proceso en curso."""
    params = router.parse(f"que {verb} la alcaldia de pasto en 2025").params
    assert params["dataset"] == "contratos"


def test_present_tense_purchase_still_looks_at_processes(router: QueryRouter) -> None:
    """"Qué compra" pregunta por lo que está comprando ahora."""
    params = router.parse("que compra la alcaldia de pasto para salud durante 2025").params
    assert params["dataset"] == "procesos"


def test_procurement_nominalisation_can_still_be_a_topic(router: QueryRouter) -> None:
    """"oportunidades de contratación" sí pregunta por contratación."""
    params = router.parse("oportunidades de contratacion en cali").params
    assert "contratacion" in (params.get("objeto") or [])


# ── Paginación ───────────────────────────────────────────────────────────

_PAGINATION_PHRASES = (
    "siguientes",
    "muestrame los siguientes",
    "los siguientes resultados",
    "muestrame mas",
    "dame mas",
    "ver mas",
    "muestrame la siguiente pagina",
    "dame mas resultados",
    "continua con los siguientes",
    "pasa a la siguiente pagina",
    "siguiente pagina",
    "continuar",
    "sigue",
)


@pytest.mark.parametrize("phrase", _PAGINATION_PHRASES)
def test_pagination_variants_are_control_not_topic(phrase: str) -> None:
    assert classify_control_intent(phrase, {}) == "pagination_more"


@pytest.mark.parametrize("phrase", _PAGINATION_PHRASES)
def test_pagination_words_never_reach_the_object(
    router: QueryRouter, phrase: str,
) -> None:
    objeto = router.parse(phrase).params.get("objeto") or []
    assert "pagina" not in objeto and "resultados" not in objeto


# ── Reinicio ─────────────────────────────────────────────────────────────

_RESET_PHRASES = (
    "limpia todos los filtros y empecemos de cero",
    "borra los filtros",
    "reinicia la busqueda",
    "quitalo todo y empezamos otra vez",
    "empecemos de cero",
    "nueva busqueda",
    "limpia los filtros",
    "borra todo",
    "resetea la busqueda",
)


@pytest.mark.parametrize("phrase", _RESET_PHRASES)
def test_reset_variants_are_recognised(phrase: str) -> None:
    assert requests_context_reset(phrase)
    assert classify_control_intent(phrase, {}) == "reset_context"


@pytest.mark.parametrize("phrase", _RESET_PHRASES)
def test_reset_words_never_reach_the_object(
    router: QueryRouter, phrase: str,
) -> None:
    objeto = router.parse(phrase).params.get("objeto") or []
    leaked = {"filtros", "busqueda", "cero", "limpia", "borra", "reinicia", "quitalo"}
    assert not (leaked & set(objeto)), f"'{phrase}' dejó control como tema: {objeto}"


@pytest.mark.parametrize(
    "phrase",
    [
        "quita el rango de valor",
        "quita el estado",
        "vuelve a contratos",
        "contratos de obra en cali",
    ],
)
def test_reset_grammar_does_not_swallow_specific_commands(phrase: str) -> None:
    """Un borrado de campo concreto sigue siendo eso, no un reinicio."""
    assert not requests_context_reset(phrase)
