"""Corpus sistémico de la petición semántica del perfil institucional.

Una fila por caso, y cada fila declara lo que el sistema debe ENTENDER, no lo
que debe encontrar: fuente pedida y su procedencia, evento temporal, tema
residual, si la pregunta requiere aclaración y qué datasets se consultan.

No es un corpus feliz. Incluye contradicciones de fuente, eventos sin fecha
productiva, estados actuales, combinaciones evento+estado y —sobre todo— los
pares adversariales donde una palabra parece una dimensión y no lo es
("firma digital" no es una fecha de firma; "publicación de avisos" no es una
fecha de publicación).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET
from app.core.entity_profile.source_request import RequestedSources, SourceProvenance
from app.core.entity_profile.temporal_event import TemporalEvent
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


TODAY = date(2026, 9, 5)
X = "alcaldia de baranoa"

P = RequestedSources.PROCESSES
C = RequestedSources.CONTRACTS
B = RequestedSources.BOTH
EXPLICIT = SourceProvenance.EXPLICIT
DEFAULT = SourceProvenance.DEFAULT

NONE = TemporalEvent.NONE
PUB = TemporalEvent.PROCESS_PUBLICATION
SIGN = TemporalEvent.CONTRACT_SIGNATURE


@dataclass(frozen=True)
class Case:
    query: str
    sources: RequestedSources | None = None
    provenance: SourceProvenance | None = None
    event: TemporalEvent = NONE
    topic: tuple[str, ...] = ()
    clarifies: bool = False
    focus: str = "overview"
    is_profile: bool = True


CORPUS: tuple[Case, ...] = (
    # ── overview por defecto: nadie eligió fuente ────────────────────────────
    Case(f"analiza {X}", B, DEFAULT),
    Case(f"analizame {X}", B, DEFAULT),
    Case(f"perfil de {X}", B, DEFAULT),
    Case(f"radiografia de {X}", B, DEFAULT),
    Case(f"que sabes de {X}", B, DEFAULT),
    Case(f"como contrata {X}", B, DEFAULT),
    Case(f"analiza {X} en 2026", B, DEFAULT),
    Case(f"analiza {X} en agosto", B, DEFAULT),

    # ── procesos explícitos ──────────────────────────────────────────────────
    Case(f"analiza procesos de {X}", P, EXPLICIT),
    Case(f"analiza los procesos de {X}", P, EXPLICIT),
    Case(f"analiza licitaciones de {X}", P, EXPLICIT),
    Case(f"analiza convocatorias de {X}", P, EXPLICIT),
    Case(f"analiza procesos de {X} en 2026", P, EXPLICIT),
    Case(f"como contrata {X} en sus procesos", P, EXPLICIT),

    # ── contratos explícitos ─────────────────────────────────────────────────
    Case(f"analiza contratos de {X}", C, EXPLICIT),
    Case(f"analiza los contratos de {X}", C, EXPLICIT),
    Case(f"analizame que contratos firmo {X} en agosto", C, EXPLICIT, SIGN),
    Case(f"analiza que contratos celebro {X} en 2026", C, EXPLICIT, SIGN),
    Case(f"analiza que contratos suscribio {X} en agosto", C, EXPLICIT, SIGN),
    Case(f"analiza contratos firmados en agosto de {X}", C, EXPLICIT, SIGN),

    # ── ambas explícitas ─────────────────────────────────────────────────────
    Case(f"analiza procesos y contratos de {X}", B, EXPLICIT),
    Case(f"analiza los procesos y los contratos de {X}", B, EXPLICIT),
    Case(f"analiza procesos y contratos de {X} en 2026", B, EXPLICIT),
    Case(f"analiza licitaciones y contratos de {X}", B, EXPLICIT),

    # ── contradicción de fuente: se pregunta, no se amplía ───────────────────
    Case(f"analiza procesos de {X} con proveedores", P, EXPLICIT,
         clarifies=True, focus="suppliers"),
    Case(f"analiza los proveedores de los procesos de {X}", P, EXPLICIT,
         clarifies=True, focus="suppliers"),
    Case(f"analiza valores de procesos de {X}", P, EXPLICIT,
         clarifies=True, focus="values"),
    Case(f"analiza modalidades de contratos de {X}", C, EXPLICIT,
         clarifies=True, focus="modalities"),

    # ── evento temporal ──────────────────────────────────────────────────────
    Case(f"analiza que procesos publico {X} en agosto", P, EXPLICIT, PUB),
    Case(f"analiza procesos publicados en agosto por {X}", P, EXPLICIT, PUB),
    Case(f"analiza procesos publicados en 2026 por {X}", P, EXPLICIT, PUB),
    Case(f"analiza contratos firmados en 2026 de {X}", C, EXPLICIT, SIGN),
    Case(f"analiza contratos celebrados en agosto de {X}", C, EXPLICIT, SIGN),
    # Sin periodo no hay evento que anclar.
    Case(f"analiza contratos firmados de {X}", C, EXPLICIT, NONE),
    Case(f"analiza procesos publicados de {X}", P, EXPLICIT, NONE),

    # ── estado actual ────────────────────────────────────────────────────────
    Case(f"analiza procesos actualmente publicados de {X}", P, EXPLICIT, NONE),
    Case(f"analiza procesos actualmente abiertos de {X}", P, EXPLICIT, NONE),
    Case(f"analiza contratos actualmente en ejecucion de {X}", C, EXPLICIT, NONE),
    Case(f"analiza contratos actualmente terminados de {X}", C, EXPLICIT, NONE),
    Case(f"analiza contratos liquidados de {X}", C, EXPLICIT, NONE),

    # ── evento + estado, ambos vivos ─────────────────────────────────────────
    Case(f"analiza procesos publicados en agosto y todavia abiertos de {X}",
         P, EXPLICIT, PUB),
    Case(f"analiza contratos firmados en agosto y actualmente terminados de {X}",
         C, EXPLICIT, SIGN),
    Case(f"analiza contratos firmados en 2026 y actualmente en ejecucion de {X}",
         C, EXPLICIT, SIGN),

    # ── eventos sin fecha productiva: aclaración, cero llamadas ──────────────
    Case(f"analiza contratos terminados en agosto de {X}", C, EXPLICIT,
         clarifies=True),
    Case(f"analiza contratos liquidados en agosto de {X}", C, EXPLICIT,
         clarifies=True),
    Case(f"analiza contratos cedidos en 2026 de {X}", C, EXPLICIT, clarifies=True),
    Case(f"analiza procesos adjudicados en agosto de {X}", P, EXPLICIT,
         clarifies=True),
    Case(f"analiza procesos iniciados en agosto de {X}", P, EXPLICIT,
         clarifies=True),
    Case(f"analiza contratos finalizados en 2026 de {X}", C, EXPLICIT,
         clarifies=True),

    # ── tema real: sobrevive junto a la palabra de fuente ────────────────────
    Case(f"analiza contratos de mantenimiento de {X}", C, EXPLICIT,
         topic=("mantenimiento",)),
    Case(f"analiza contratos de mantenimiento de {X} en agosto", C, EXPLICIT,
         topic=("mantenimiento",)),
    Case(f"analiza procesos de vias de {X}", P, EXPLICIT, topic=("vias",)),
    # El router agrupa el bigrama temático; el perfil lo recibe ya formado.
    Case(f"analiza {X} en alimentacion escolar", B, DEFAULT,
         topic=("alimentacion_escolar",)),
    Case(f"analiza contratos de interventoria de {X}", C, EXPLICIT,
         topic=("interventoria",)),

    # ── tema + evento ────────────────────────────────────────────────────────
    Case(f"analiza procesos de vias publicados por {X} en agosto", P, EXPLICIT,
         PUB, ("vias",)),
    Case(f"analiza contratos de mantenimiento firmados en agosto de {X}",
         C, EXPLICIT, SIGN, ("mantenimiento",)),

    # ── pares adversariales: la palabra parece dimensión y no lo es ──────────
    Case(f"analiza contratos de firma digital de {X}", C, EXPLICIT,
         topic=("firma", "digital")),
    Case(f"analiza contratos de firma digital de {X} en agosto", C, EXPLICIT,
         NONE, ("firma", "digital")),
    Case(f"analiza contratos de firma electronica de {X}", C, EXPLICIT,
         topic=("firma", "electronica")),
    Case(f"analiza procesos de publicacion de avisos de {X}", P, EXPLICIT,
         topic=("publicacion", "avisos")),
    Case(f"analiza contratos de mantenimiento de publicaciones de {X}",
         C, EXPLICIT, topic=("mantenimiento", "publicaciones")),

    # ── foco ─────────────────────────────────────────────────────────────────
    Case(f"analiza los proveedores de {X}", B, DEFAULT, focus="suppliers"),
    Case(f"analiza las modalidades de {X}", B, DEFAULT, focus="modalities"),
    Case(f"analiza los valores de {X}", B, DEFAULT, focus="values"),
    Case(f"analiza la evolucion de {X}", B, DEFAULT, focus="temporal"),
    Case(f"analiza los ultimos contratos de {X}", C, EXPLICIT, focus="recent"),
    Case(f"analiza los procesos recientes de {X}", P, EXPLICIT, focus="recent"),

    # ── listado, no perfil ───────────────────────────────────────────────────
    Case(f"que contratos firmo {X} en agosto", is_profile=False),
    Case(f"muestrame los contratos que firmo {X}", is_profile=False),
    Case(f"contratos de mantenimiento de {X}", is_profile=False),
    Case(f"procesos publicados de {X}", is_profile=False),
    Case(f"dame los contratos de {X} en 2026", is_profile=False),
    Case(f"cuales son los procesos de {X}", is_profile=False),
)


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _turn(router: QueryRouter, text: str):
    return classify_entity_profile_turn(text, router.parse(text, today=TODAY).params)


def _ids(case: Case) -> str:
    return case.query


@pytest.mark.parametrize("case", CORPUS, ids=_ids)
def test_the_corpus_understands_what_was_asked(router, case: Case):
    turn = _turn(router, case.query)

    if not case.is_profile:
        assert turn.detected is False
        return

    assert turn.detected is True
    assert turn.focus == case.focus
    assert turn.request is not None
    assert turn.request.sources is case.sources
    assert turn.request.sources_provenance is case.provenance
    assert turn.request.event is case.event
    assert turn.request.topic == case.topic
    assert (turn.clarification is not None) is case.clarifies


@pytest.mark.parametrize("case", [c for c in CORPUS if c.is_profile], ids=_ids)
def test_the_corpus_executes_only_the_sources_it_understood(router, case: Case):
    turn = _turn(router, case.query)
    client = FakeClient()
    if turn.clarification:
        assert turn.plan.jobs == ()
        return
    build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=client, soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    datasets = {dataset for _, dataset, _ in client.calls}
    capability = turn.plan.effective_sources
    if capability is RequestedSources.CONTRACTS:
        assert datasets == {CONTRACT_DATASET}
    elif capability is RequestedSources.PROCESSES:
        assert datasets == {PROCESS_DATASET}
    else:
        assert datasets == {PROCESS_DATASET, CONTRACT_DATASET}


@pytest.mark.parametrize("case", [c for c in CORPUS if c.is_profile], ids=_ids)
def test_the_corpus_never_leaks_a_predicate_across_schemas(router, case: Case):
    turn = _turn(router, case.query)
    if turn.clarification:
        return
    client = FakeClient()
    build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=client, soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    for _, dataset, soql in client.calls:
        where = soql.split(" WHERE ", 1)[1]
        if dataset == CONTRACT_DATASET:
            assert "estado_del_procedimiento" not in where
            assert "estado_de_apertura_del_proceso" not in where
        else:
            assert "estado_contrato" not in where


@pytest.mark.parametrize("case", [c for c in CORPUS if c.is_profile], ids=_ids)
def test_the_corpus_never_filters_by_a_contractual_action_verb(router, case: Case):
    """Ni el verbo del usuario ni el sustantivo homónimo acaban en el objeto."""
    turn = _turn(router, case.query)
    assert turn.request is not None
    forbidden = {
        "firmo", "firmaron", "celebro", "celebraron", "suscribio", "suscribieron",
        "publico", "publicaron", "analiza", "analizame",
    }
    assert not (set(turn.request.topic) & forbidden)


# ── Compradores cuyo nombre contiene una palabra de fuente ──────────────────

@pytest.mark.parametrize("buyer_words", [
    "agencia nacional de procesos", "instituto de contratos publicos",
])
def test_a_source_word_inside_the_buyer_name_never_chooses_a_source(buyer_words):
    from app.core.entity_profile.source_request import resolve_requested_sources

    params = {
        "entidad_resolved": buyer_words.upper(),
        "entidad": buyer_words,
        "entidad_resolution": {
            "value": buyer_words.upper(), "kind": "buyer_entity",
            "confidence": "high", "metadata": {"rewritten_from": buyer_words},
        },
    }
    source = resolve_requested_sources(f"analiza {buyer_words}", params)
    assert source.sources is RequestedSources.BOTH
    assert source.provenance is SourceProvenance.DEFAULT
