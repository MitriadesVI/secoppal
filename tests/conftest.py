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
    """AP2 encendido en la suite, aunque su default de producción sea apagado.

    El default apagado protege un merge a main; esta fixture protege lo
    contrario — que el camino que se va a desplegar esté realmente probado.
    Sin ella la suite mediría el comportamiento legacy y AP2 viajaría sin
    cobertura hasta el día que alguien exportara la variable.

    Se hace por entorno y no parcheando funciones para ejercitar la ruta real
    de configuración. ``tests/test_actor_flag_off.py`` apaga el flag por su
    cuenta y verifica el default declarado leyendo el campo de ``Settings``.
    """
    from app.config import get_settings

    monkeypatch.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
