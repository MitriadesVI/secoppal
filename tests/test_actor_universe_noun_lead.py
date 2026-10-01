"""A universe noun heads a search for records; it is never the name of an actor.

«busca licitaciones de alcaldía de barranquilla en 2024» asked for tenders of a
buyer, but the bare-search actor grammar read «licitaciones de alcaldía de
barranquilla» as a contractor name (FOUNDATION_ENTRY_ADMISSION M0/L2).  The
lexicon knew «licitacion» but not its plural, the other universe nouns, or a
leading article.  These tests enumerate verb x article x noun x tail instead of
sampling, and keep the shapes that really are actors as negative controls.
"""

from __future__ import annotations

import itertools
from collections import Counter

import pytest

from app.core import actor_conversation
from app.core.actor_conversation import (
    ActorConversationOperation as Op,
    interpret_actor_conversation,
)
from tests import test_foundation_route_ownership as base
from tests.actor_prefix_fixtures import resolver_for

make_flow = base.make_flow  # fixture re-export: pytest resolves it from this module

SEARCH_VERBS = ["busca", "buscame", "buscate", "mira", "mirame", "consulta", "revisa"]
REPLACE_VERBS = ["cambia a", "cambia al", "cambiame a", "cambia por", "ahora cambia a", "ahora cambiame por"]
ARTICLES = ["", "el ", "la ", "los ", "las ", "un ", "una ", "unos ", "unas "]

# Every noun the lexicon is expected to know, singular and plural, as the user types it.
UNIVERSE_NOUNS = [
    "contrato", "contratos", "proceso", "procesos", "licitacion", "licitaciones",
    "licitación", "licitaciones públicas", "convocatoria", "convocatorias",
    "oportunidad", "oportunidades", "compra", "compras", "contratacion", "contrataciones",
    "selección abreviada", "selecciones abreviadas", "mínima cuantía", "concurso de méritos",
    "concursos de méritos",
]
BUYER_TAILS = [
    "de la alcaldía de soledad", "de alcaldía de barranquilla", "de la gobernación del atlántico",
    "del distrito de cartagena", "del ministerio de educación", "de la ESE Hospital San Juan",
    "de la alcaldía de soledad en 2025", "de alcaldía de barranquilla en 2024",
]
TOPIC_TAILS = ["de aseo en cali", "de aseo", "de vigilancia en 2025", "de mantenimiento vial"]
TAILS = BUYER_TAILS + TOPIC_TAILS

ACTOR_OPERATIONS = {Op.SET_ACTOR, Op.REPLACE_ACTOR}


def _texts(verbs, nouns=UNIVERSE_NOUNS, tails=TAILS):
    for verb, article, noun, tail in itertools.product(verbs, ARTICLES, nouns, tails):
        yield f"{verb} {article}{noun} {tail}"


def _actor_instructions(texts):
    return [text for text in texts if interpret_actor_conversation(text).operation in ACTOR_OPERATIONS]


def test_a_bare_search_headed_by_a_universe_noun_is_never_an_actor_instruction():
    assert _actor_instructions(_texts(SEARCH_VERBS)) == []


def test_a_replacement_headed_by_a_universe_noun_is_never_an_actor_instruction():
    assert _actor_instructions(_texts(REPLACE_VERBS)) == []


@pytest.mark.parametrize("text", [
    "busca licitaciones de alcaldía de barranquilla en 2024",
    "busca las licitaciones de la alcaldía de soledad en 2025",
    "mira convocatorias de la alcaldía de soledad en 2025",
    "busca las convocatorias de la alcaldía de soledad en 2025",
    "busca selecciones abreviadas de la alcaldía de soledad en 2025",
])
def test_the_measured_defect_shapes_stop_being_actor_instructions(text):
    assert interpret_actor_conversation(text).operation is None


def test_the_lexicon_is_covered_by_this_enumeration():
    # A noun added to the lexicon without a row in UNIVERSE_NOUNS would escape the property.
    from app.core.actor_name import normalize_actor_name

    covered = {normalize_actor_name(noun) for noun in UNIVERSE_NOUNS}
    covered_heads = {noun.split()[0] for noun in covered}
    lexicon = actor_conversation._NON_ACTOR_SEARCH_LEADS
    assert lexicon, "lexicon must exist"
    for entry in lexicon:
        assert entry.split()[0] in covered_heads, entry


def test_the_router_never_projects_an_actor_for_a_universe_noun_search(monkeypatch):
    from app.config import get_settings
    from app.core.query_router import QueryRouter

    monkeypatch.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
    get_settings.cache_clear()
    router = QueryRouter()
    actor_keys = ("actor_scope", "actor_pending", "actor_conversation_probe", "contratista",
                  "actor_conversation_operation")
    leaked = []
    for text in _texts(["busca", "mira"], tails=BUYER_TAILS[:3] + BUYER_TAILS[6:] + TOPIC_TAILS[:2]):
        params = router.parse(text).params
        if any(params.get(key) for key in actor_keys):
            leaked.append(text)
    get_settings.cache_clear()
    assert leaked == []


# Negative controls: shapes that really name an actor stay actors.
@pytest.mark.parametrize(("text", "operation"), [
    ("busca Inversiones Pérez SAS", Op.SET_ACTOR),
    ("busca la Fundación Caribe Verde", Op.SET_ACTOR),
    ("mira Consorcio Vial del Norte", Op.SET_ACTOR),
    ("revisa Aseo Total SAS", Op.SET_ACTOR),
    ("cambia a Inversiones Pérez SAS", Op.REPLACE_ACTOR),
    ("cambia al Consorcio Vial del Norte", Op.REPLACE_ACTOR),
    ("contratos del contratista con NIT 900373913", Op.SET_ACTOR),
])
def test_a_name_without_a_universe_noun_head_is_still_an_actor_instruction(text, operation):
    assert interpret_actor_conversation(text).operation == operation


@pytest.mark.parametrize("text", ["cambia a procesos", "cambia a contratos", "ahora cambia a procesos"])
def test_changing_the_universe_noun_is_a_dataset_mutation_not_an_actor(text):
    assert interpret_actor_conversation(text).operation is None


@pytest.mark.parametrize("text", ["cambia a las licitaciones", "cambia a convocatorias",
                                  "ahora cambia a los procesos", "cambia a selecciones abreviadas"])
def test_changing_to_a_plural_or_articled_universe_noun_is_a_dataset_mutation(text):
    assert interpret_actor_conversation(text).operation is None


def test_a_universe_noun_inside_a_name_is_not_a_head():
    # Only the FIRST word (after an article) heads the search; later words are part of a name.
    for text in ("busca Servicios de Contratos SAS", "busca Consorcio Licitaciones del Norte SAS",
                 "busca Inversiones Convocatorias SAS"):
        assert interpret_actor_conversation(text).operation == Op.SET_ACTOR, text


# End to end, legacy, frozen empty source: the ACTOR owner keeps exactly the actor turns.
ALIAS_ROWS = [
    ("NIT", "900111222", "fundacaribe"),
    ("NIT", "900222333", "consorcio vial del norte"),
    ("NIT", "900333444", "inversiones perez sas"),
    ("NIT", "900444555", "o'brien & asociados s.a.s"),
]
ACTOR_CONTROLS = [
    "busca proveedor FUNDACARIBE",
    "contratos del proveedor FUNDACARIBE",
    "contratos del contratista con NIT 900373913",
    "busca Inversiones Pérez SAS",
    "busca contratos de Inversiones Pérez SAS en 2025",
    "necesito los contratos de CONSORCIO VIAL DEL NORTE",
    "contratos de O'Brien & Asociados S.A.S",
]


@pytest.mark.parametrize("text", ACTOR_CONTROLS)
def test_actor_shapes_are_still_owned_by_actor_in_legacy(make_flow, tmp_path, text):
    flow = make_flow("legacy")
    flow.actor_name_resolver = resolver_for(tmp_path / "controls.sqlite", ALIAS_ROWS)
    assert base.run(flow, text, chat_id="ctl").get("_continuation_owner") == "ACTOR"


BARRANQUILLA = "busca licitaciones de alcaldía de barranquilla en 2024"


def test_barranquilla_tenders_is_a_tabular_search_with_the_buyer_resolved_in_legacy(make_flow):
    flow = make_flow("legacy")
    result = base.run(flow, BARRANQUILLA, chat_id="bq")
    assert result.get("_continuation_owner") == "TABULAR", result.get("_continuation_owner")
    params = result["parsed_params"]
    assert not any(params.get(key) for key in ("actor_scope", "actor_pending", "contratista"))
    assert "BARRANQUILLA" in result["resolved_params"]["entidad"].upper()
    assert result["dataset_id"] == "p6dx-8zbt"
    # the source was asked for the buyer's processes, never for a person called "licitaciones ..."
    assert flow.source.calls and all("BARRANQUILLA" in soql for _, _, soql in flow.source.calls)
    assert "CC" not in str(result["response"]).split("—")[0]

