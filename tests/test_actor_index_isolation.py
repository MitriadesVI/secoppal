"""§6 — la suite no puede depender del directorio local de la máquina.

El artefacto de producción pesa 662 MB, no se versiona y existe o no según
quién ejecute las pruebas. Cuando se leía de forma ambiental, la suite daba
dos resultados distintos: con el directorio presente fallaban dos pruebas y
sin él pasaban todas. Un resultado que depende de un archivo no versionado no
prueba nada.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core.actor_name import ActorResolutionStatus
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver

PRODUCTION_INDEX = Path("data/actor_alias_index.sqlite")


def test_the_configured_index_path_is_never_the_production_artifact():
    """El fixture de conftest desvía la ruta antes de cada prueba."""
    configured = Path(get_settings().secop_actor_alias_index_path).resolve()
    assert configured != PRODUCTION_INDEX.resolve()
    assert not configured.is_file()


def test_a_resolver_built_from_settings_fails_closed_by_default():
    """Sin índice explícito no hay resolución, haya o no artefacto en disco."""
    result = ActorNameResolver.from_settings().resolve("FUNDACARIBE")
    assert result.status == ActorResolutionStatus.INDEX_UNAVAILABLE


@pytest.mark.skipif(
    not PRODUCTION_INDEX.is_file(),
    reason="sin directorio local no hay nada que aislar",
)
def test_the_production_index_does_not_leak_into_the_default_resolver():
    """La comprobación que da sentido al aislamiento.

    Se ejecuta solo cuando el artefacto existe —es decir, en la máquina donde
    el acoplamiento sería visible— y demuestra que aun así no se usa.
    FUNDACARIBE sí está en el directorio real (NIT 900429803), así que si
    hubiera fuga, esto resolvería.
    """
    assert ActorNameResolver.from_settings().resolve(
        "FUNDACARIBE"
    ).status == ActorResolutionStatus.INDEX_UNAVAILABLE


def test_an_explicit_fixture_index_still_resolves(tmp_path):
    """Aislar no es amputar: un test que quiera índice construye el suyo."""
    target = tmp_path / "index.sqlite"
    build_actor_alias_index(
        [{
            "tipodocproveedor": "NIT", "documento_proveedor": "900429803",
            "proveedor_adjudicado": "FUNDACARIBE", "occurrences": 9,
            "first_seen": "2024-01-01", "last_seen": "2026-01-01",
        }],
        target,
        source_dataset="jbjy-vk9h",
    )
    result = ActorNameResolver(target).resolve("FUNDACARIBE")
    assert result.status == ActorResolutionStatus.RESOLVED
    assert result.identity is not None
    assert result.identity.base_number == "900429803"


def test_the_environment_override_is_what_drives_the_path(monkeypatch, tmp_path):
    """El aislamiento usa la ruta configurable real, no un parche interno."""
    custom = tmp_path / "elsewhere.sqlite"
    monkeypatch.setenv("SECOP_ACTOR_ALIAS_INDEX_PATH", str(custom))
    get_settings.cache_clear()
    try:
        assert Path(Settings().secop_actor_alias_index_path) == custom
    finally:
        get_settings.cache_clear()


def test_no_actor_test_module_reads_the_production_path_literally():
    """Nadie debe volver a codificar la ruta del artefacto en un test.

    Esta prueba y el módulo de aislamiento son la única excepción: aquí la
    ruta se nombra justamente para comprobar que no se usa.
    """
    allowed = {"test_actor_index_isolation.py"}
    offenders = []
    for path in Path("tests").glob("test_actor*.py"):
        if path.name in allowed:
            continue
        if "data/actor_alias_index.sqlite" in path.read_text(encoding="utf-8"):
            offenders.append(path.name)
    assert offenders == []
