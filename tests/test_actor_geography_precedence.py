"""INV-BARE-GEOGRAPHY-PRECEDENCE — AP3-E.1 §8.

``contratos de Bogotá`` es una consulta territorial. El directorio completo
contiene dos cédulas cuyo nombre observado en SECOP es literalmente "Bogota",
y con eso la consulta pasaba a ambigüedad de actor y dejaba de ejecutarse;
``contratos de Barranquilla`` iba peor todavía — resolvía a un actor y
ejecutaba como perfil.

El problema de fondo no es el acierto o el error de un caso: es que el
routing de una geografía canónica pasaba a depender de qué personas figuren
en el directorio, y eso lo vuelve no determinista entre reconstrucciones.
"""
from __future__ import annotations

import pytest

from app.core.actor_arbitration import has_canonical_geography_evidence
from app.core.actor_name import ActorResolutionStatus
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver


class _State(dict):
    def get(self, key, default=None):
        return dict.get(self, key, default)

    def update(self, **kwargs):  # type: ignore[override]
        merged = _State(self)
        dict.update(merged, kwargs)
        return merged


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
    from app.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope="module")
def geography_named_index(tmp_path_factory):
    """Directorio donde varios topónimos SON nombres de actor.

    Reproduce lo que el directorio real contiene: personas cuyo nombre
    observado en SECOP es un topónimo. Si el fixture no los tuviera, la
    prueba pasaría sin ejercitar nada.
    """
    target = tmp_path_factory.mktemp("geo") / "index.sqlite"
    build_actor_alias_index(
        [
            {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "1015431555",
             "proveedor_adjudicado": "Bogota", "occurrences": 5,
             "first_seen": "2024-01-01", "last_seen": "2026-01-01"},
            {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "79999111",
             "proveedor_adjudicado": "Barranquilla", "occurrences": 4,
             "first_seen": "2024-01-01", "last_seen": "2026-01-01"},
            {"tipodocproveedor": "NIT", "documento_proveedor": "900555444",
             "proveedor_adjudicado": "ATLANTICO", "occurrences": 7,
             "first_seen": "2024-01-01", "last_seen": "2026-01-01"},
        ],
        target,
        source_dataset="jbjy-vk9h",
    )
    return target


def _run(query: str, index_path):
    parsed = QueryRouter().parse(query)
    resolved = resolve_actor_name(
        _State(parsed_params=dict(parsed.params), raw_query=query),
        actor_name_resolver=ActorNameResolver(index_path),
    )
    params = dict(resolved.get("parsed_params", {}))
    return params, compute_query_plan(params, query)


@pytest.mark.parametrize(
    "query",
    [
        "contratos de Bogota",
        "contratos de Bogotá",
        "contratos de Barranquilla",
        "contratos de Atlantico",
    ],
)
def test_bare_canonical_geography_never_becomes_actor_intent(query, geography_named_index):
    params, plan = _run(query, geography_named_index)

    assert params.get("actor_resolution_status") is None
    assert params.get("actor_scope") is None
    assert plan.should_execute is True


@pytest.mark.parametrize(
    "query",
    ["contratos de Bogota", "contratos de Barranquilla"],
)
def test_the_territorial_frame_survives_intact(query, geography_named_index):
    params, _ = _run(query, geography_named_index)
    assert params.get("ciudad") or params.get("departamento_resolved")


def test_routing_of_canonical_geography_does_not_depend_on_the_directory(
    geography_named_index, tmp_path,
):
    """La misma consulta, con y sin esos actores en el índice, decide igual.

    Es la propiedad que importa: si el directorio pudiera cambiar el routing,
    reconstruirlo cambiaría el comportamiento del producto sin que nadie
    tocara una línea de código.
    """
    empty = tmp_path / "empty_index.sqlite"
    build_actor_alias_index(
        [{"tipodocproveedor": "NIT", "documento_proveedor": "900000111",
          "proveedor_adjudicado": "ALGO DISTINTO", "occurrences": 3,
          "first_seen": "2024-01-01", "last_seen": "2026-01-01"}],
        empty,
        source_dataset="jbjy-vk9h",
    )
    with_geo, plan_with = _run("contratos de Bogota", geography_named_index)
    without_geo, plan_without = _run("contratos de Bogota", empty)

    assert with_geo.get("actor_resolution_status") == without_geo.get("actor_resolution_status")
    assert plan_with.should_execute == plan_without.should_execute
    assert with_geo.get("departamento_resolved") == without_geo.get("departamento_resolved")


@pytest.mark.parametrize(
    "query",
    [
        "contratos del proveedor Barranquilla",
        "contratos del contratista Barranquilla",
    ],
)
def test_explicit_actor_syntax_may_still_resolve_a_place_name(query, geography_named_index):
    """El usuario dijo "proveedor": ahí sí pregunta por un actor.

    La precedencia protege la mención desnuda, no prohíbe que alguien se
    llame como un lugar.
    """
    params, _ = _run(query, geography_named_index)
    assert params.get("actor_resolution_status") in {
        ActorResolutionStatus.RESOLVED,
        ActorResolutionStatus.AMBIGUOUS,
        ActorResolutionStatus.CANDIDATES,
    }


@pytest.mark.parametrize(
    "name", ["bogota", "Bogotá", "BARRANQUILLA", "atlantico", "Valle del Cauca"],
)
def test_canonical_places_are_recognized_from_the_repository_sources(name):
    assert has_canonical_geography_evidence(name, {}) is True


@pytest.mark.parametrize("name", ["FUNDACARIBE", "E&S", "TUS ABUELOS BARRANQUILLA SAS"])
def test_an_actor_name_is_not_mistaken_for_a_place(name):
    """Contener un topónimo no convierte un nombre en geografía."""
    assert has_canonical_geography_evidence(name, {}) is False
