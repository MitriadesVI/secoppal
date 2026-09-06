from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolate_runtime_persistence(monkeypatch, tmp_path):
    """Toda persistencia de tests vive en tmp_path, nunca en rutas del proyecto."""
    project_sidecars = set((ROOT / "data" / "conversations").glob("*.pagination.json"))
    monkeypatch.setenv("SECOP_FEEDBACK_PATH", str(tmp_path / "feedback.jsonl"))
    monkeypatch.setenv("SECOP_FEEDBACK_DB_PATH", str(tmp_path / "secoppal.db"))
    monkeypatch.setenv("CONVERSATION_STORE_DIR", str(tmp_path / "conversations"))
    yield
    assert set((ROOT / "data" / "conversations").glob("*.pagination.json")) == project_sidecars


@pytest.fixture(autouse=True)
def actor_profile_flag_on(monkeypatch):
    """Fija AP2 encendido para que la suite no dependa del entorno del operador.

    El default productivo ya es ON. Esta fixture evita que un kill-switch local
    haga que la suite mida accidentalmente el comportamiento legacy.

    Se hace por entorno y no parcheando funciones para ejercitar la ruta real
    de configuración. ``tests/test_actor_flag_off.py`` apaga el flag por su
    cuenta y verifica tanto el default como las dos posiciones del entorno.
    """
    from app.config import get_settings

    monkeypatch.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def isolate_actor_alias_index(monkeypatch, tmp_path):
    """La suite nunca ve el directorio de actores de la máquina.

    El artefacto de producción pesa 662 MB, vive fuera de git y existe o no
    según quién ejecute las pruebas. Cuando la suite lo leía de forma
    ambiental, su resultado dependía de eso: con el directorio presente dos
    pruebas fallaban y sin él pasaban — y ninguna de las dos versiones era
    "la verdad".

    Por defecto se apunta a una ruta inexistente dentro de ``tmp_path``, que
    es el caso base honesto: sin índice, la resolución por nombre falla
    cerrado. Un test que necesite un índice construye el suyo y se lo pasa
    explícitamente al resolver.
    """
    from app.config import get_settings

    monkeypatch.setenv(
        "SECOP_ACTOR_ALIAS_INDEX_PATH", str(tmp_path / "no_actor_index.sqlite"),
    )
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope='module')
def frozen_municipal_baseline_router(tmp_path_factory):
    """Absence contracts name the pre-promotion catalog, even after Retry #2."""
    from app.core.entity_resolver import EntityResolver
    from app.core.query_router import QueryRouter
    from scripts.audits.promote_municipal_m1a import encode, read

    path = tmp_path_factory.mktemp('municipal-baseline') / 'aliases.json'
    path.write_bytes(encode(read(ROOT / 'artifacts/audits/municipal_m1a/before_aliases_db.json.gz')))
    return QueryRouter(EntityResolver(path))
