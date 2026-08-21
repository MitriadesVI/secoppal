"""INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION — AP3-E.1.

El defecto que congela este archivo solo era visible con el directorio
completo. Con 474 identidades, recortar ``E.S.E HOSPITAL PEDRO LEON ALVAREZ
DIAZ`` hasta ``e s`` no encontraba nada y el fallo era un falso negativo
inocuo. Con 1,2 millones de aliases, ``e s`` es el alias exacto de la empresa
E&S, y el fragmento obtenía ``exact_unique_alias`` — autoridad plena, sin
clarificación — sobre la actividad de otra empresa.

De ahí la propiedad que estas pruebas exigen y que un índice muestreado no
puede comprobar: **el fragmento truncado existe deliberadamente como otro
actor real en el índice de prueba**. Un test donde el fragmento no coincide
con nada recrearía justo el punto ciego que dejó pasar el P0.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.actor_intent import strong_actor_name_prefixes
from app.core.workflow.actions.actor_resolution import (
    _EXTERNAL_MODIFIER_LEAD_RE,
    resolve_actor_name,
)
from app.core.actor_name import ActorResolutionStatus
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver

REPROS = Path("tests/fixtures/actor_full_index/p0_repros.jsonl")


class _State(dict):
    """Doble mínimo del State de Burr para ejercitar la acción aislada."""

    def get(self, key, default=None):
        return dict.get(self, key, default)

    def update(self, **kwargs):  # type: ignore[override]
        merged = _State(self)
        dict.update(merged, kwargs)
        return merged


def _group(document, name, *, source_type="NIT", occurrences=4):
    return {
        "tipodocproveedor": source_type,
        "documento_proveedor": document,
        "proveedor_adjudicado": name,
        "occurrences": occurrences,
        "first_seen": "2024-01-01",
        "last_seen": "2026-01-01",
    }


@pytest.fixture(scope="module")
def trap_index(tmp_path_factory):
    """Índice donde el FRAGMENTO de un nombre largo es otro actor real.

    Reproduce en miniatura la trampa del directorio de producción: si el
    algoritmo acorta la mención, encontrará un alias exacto y podrá creerse
    autorizado.
    """
    target = tmp_path_factory.mktemp("trap") / "index.sqlite"
    build_actor_alias_index(
        [
            # El fragmento corto: un actor REAL y distinto.
            _group("900047333", "E&S"),
            _group("900111111", "ALCALDIA DE PUERTO SALGAR", source_type="Cédula de Ciudadanía"),
            _group("900222222", "FUNDACION NORTE"),
            # El actor largo, que NO está bajo su nombre completo: así el
            # único alias alcanzable por acortamiento es el del otro actor.
            _group("890680027", "ESE HOSPITAL PEDRO LEON ALVAREZ DIAZ DE LA MESA"),
        ],
        target,
        source_dataset="jbjy-vk9h",
    )
    return target


def _resolve(query: str, index_path) -> dict:
    parsed = QueryRouter().parse(query)
    state = _State(parsed_params=dict(parsed.params), raw_query=query)
    resolved = resolve_actor_name(
        state, actor_name_resolver=ActorNameResolver(index_path)
    )
    return dict(resolved.get("parsed_params", {}))


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
    from app.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# El invariante.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("query", "trap_base"),
    [
        ("contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025", "900047333"),
        ("contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ por mayor valor", "900047333"),
        ("contratos del proveedor ALCALDIA DE PUERTO SALGAR CUNDINAMARCA", "900111111"),
        ("contratos del contratista FUNDACION NORTE DEL CARIBE ATLANTICO", "900222222"),
    ],
)
def test_a_truncated_fragment_never_takes_the_identity_of_another_actor(
    query, trap_base, trap_index,
):
    """El fragmento existe en el índice y aun así no puede reclamar autoridad."""
    params = _resolve(query, trap_index)
    scope = params.get("actor_scope") or {}

    assert scope.get("base_number") != trap_base
    assert params.get("actor_resolution_method") != "exact_unique_alias"


@pytest.mark.parametrize(
    "query",
    [
        "contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025",
        "contratos del proveedor ALCALDIA DE PUERTO SALGAR CUNDINAMARCA",
    ],
)
def test_a_fragmented_mention_fails_closed_instead_of_executing(query, trap_index):
    params = _resolve(query, trap_index)
    assert compute_query_plan(params, query).should_execute is False


def test_the_full_mention_survives_the_boundary_search(trap_index):
    """Lo que se conserva es la mención completa, no un prefijo cómodo."""
    query = "contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025"
    scope = _resolve(query, trap_index).get("actor_scope") or {}
    mention = scope.get("observed_name") or ""

    assert "hospital" in mention
    assert "alvarez" in mention
    assert mention != "e s"


def test_the_recognized_modifier_is_still_detached(trap_index):
    """El año sigue siendo año: el arreglo no come modificadores externos."""
    params = _resolve(
        "contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025",
        trap_index,
    )
    assert params.get("fecha_desde") == "2025-01-01"


# ---------------------------------------------------------------------------
# Qué puede desprenderse del final de una mención.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("query", "expected_suffix"),
    [
        ("contratos del proveedor universidad industrial de santander en bolivar", "en bolivar"),
        ("contratos del proveedor fundacion norte en 2025", "en 2025"),
        ("contratos del proveedor fundacion norte por mayor valor", "por mayor valor"),
    ],
)
def test_an_introduced_tail_is_a_detachable_modifier(query, expected_suffix):
    """Un modificador externo viene introducido por un conector."""
    assert _EXTERNAL_MODIFIER_LEAD_RE.match(expected_suffix)
    assert query.endswith(expected_suffix)


@pytest.mark.parametrize(
    "glued_suffix",
    [
        "cundinamarca",                      # ALCALDIA … PUERTO SALGAR CUNDINAMARCA
        "bolivar 2024",                      # CONSORCIO SALUD BOLIVAR 2024
        "e hospital pedro leon alvarez diaz",  # el resto tras recortar a "e s"
        "atlantico",
    ],
)
def test_a_glued_tail_is_part_of_the_name_not_a_modifier(glued_suffix):
    """Sin conector no hay cola: el token pertenece al nombre.

    Es la regla que separa "… en Bolívar" —filtro— de "CONSORCIO SALUD
    BOLIVAR 2024" —así se llama la unión temporal—.
    """
    assert not _EXTERNAL_MODIFIER_LEAD_RE.match(glued_suffix)


def test_an_entity_in_the_tail_means_the_cut_split_a_proper_name(trap_index):
    """Si el resto se reparsea como entidad, el corte partió un nombre.

    Era la grieta concreta: el gazetteer reconocía "hospital pedro leon
    alvarez diaz" como entidad compradora, la cola se daba por válida y el
    fragmento "e s" se quedaba con la autoridad.
    """
    params = _resolve(
        "contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025",
        trap_index,
    )
    assert (params.get("actor_scope") or {}).get("base_number") is None


def test_prefix_candidates_are_longest_first():
    """El más largo primero: la mención más defendible gana la carrera."""
    query = "contratos del proveedor fundacion norte del caribe"
    candidates = strong_actor_name_prefixes(query, minimum_name="fundacion norte")
    lengths = [len(candidate.split()) for candidate in candidates]
    assert lengths == sorted(lengths, reverse=True)
    assert candidates[0] == "fundacion norte del caribe"


# ---------------------------------------------------------------------------
# Los repros congelados, tal como se capturaron antes del arreglo.
# ---------------------------------------------------------------------------


def test_the_frozen_p0_repros_no_longer_resolve_to_the_wrong_actor(trap_index):
    """Las tres consultas que el directorio completo destapó.

    Se conservan verbatim: el fixture congeló la mención truncada y la
    identidad equivocada ANTES de tocar runtime.
    """
    repros = [json.loads(line) for line in REPROS.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(repros) == 3

    for repro in repros:
        params = _resolve(repro["raw_query"], trap_index)
        scope = params.get("actor_scope") or {}
        wrong = repro["wrong_resolved_identity"]["base_number"]

        assert scope.get("base_number") != wrong, repro["case_id"]
        assert scope.get("observed_name") != repro["actual_truncated_mention"], repro["case_id"]
