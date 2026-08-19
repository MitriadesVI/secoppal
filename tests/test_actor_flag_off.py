"""INV-FLAG — con ACTOR_PROFILE apagado, el sistema es el de antes de AP2.

El flag no es un adorno de despliegue: es la garantía de que un bloque que
cambia routing, SoQL y render se puede revertir sin desplegar código. Estas
pruebas comparan comportamiento observable —params, SoQL y decisión de
ejecución— entre las dos posiciones del interruptor.
"""
from __future__ import annotations

import pytest

from app.config import Settings, actor_profile_enabled
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

builder = SoQLBuilder()


@pytest.fixture
def flag_off(monkeypatch):
    """Apaga el flag en los tres módulos que lo consultan.

    Se parchea el nombre importado en cada módulo, no ``app.config``: los
    consumidores hacen ``from app.config import actor_profile_enabled``, así
    que parchear solo el origen dejaría las referencias ya resueltas.
    """
    for module in ("app.core.query_router", "app.core.query_plan"):
        monkeypatch.setattr(f"{module}.actor_profile_enabled", lambda: False)
    yield


def parse(text: str) -> dict:
    return dict(QueryRouter().parse(text).params)


def soql_for(text: str) -> str:
    params = parse(text)
    return builder.build(builder.dataset_id_for(params.get("dataset")), params)


def test_the_declared_default_is_off():
    """El default de producción es apagado; la suite lo enciende aparte.

    Se lee el campo declarado y no ``actor_profile_enabled()``, porque la
    fixture de ``conftest`` ya encendió el flag para todo el resto de la
    suite: preguntarle a la función mediría el entorno de pruebas, no la
    decisión de despliegue.

    Importa que un fast-forward a main no active un routing nuevo por el
    solo hecho de integrar el código.
    """
    field = Settings.model_fields["secoppal_actor_profile_enabled"]
    assert field.default is False
    assert field.alias == "SECOPPAL_ACTOR_PROFILE_ENABLED"


def test_the_suite_runs_with_the_flag_on():
    """Lo que se despliega el día que se encienda es lo que se está probando."""
    assert actor_profile_enabled() is True


# ---------------------------------------------------------------------------
# Params: ninguna clave de AP2 aparece con el flag apagado.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "contratos del NIT 860002400",
        "contratos de la cedula 1140835135",
        "contratos del proveedor FUNDACARIBE",
        "procesos del NIT 860002400",
        "qué ha contratado FUNSOCOM",
    ],
)
def test_no_actor_keys_are_emitted_when_the_flag_is_off(flag_off, text):
    params = parse(text)
    assert "actor_resolution" not in params
    assert "actor_scope" not in params


def test_the_legacy_nit_extraction_governs_when_the_flag_is_off(flag_off):
    """Sin flag vuelve la regla previa: el DV solo se separa si venía separado.

    Es literalmente INCIDENTAL_FINDING #2 de AP0 — el defecto que AP2
    corrige— y aquí se comprueba que sigue ahí, porque eso es lo que
    significa "comportamiento de antes".
    """
    assert parse("contratos del NIT 8600345941")["contratista"] == ["8600345941"]
    assert parse("contratos del NIT 860.034.594-1")["contratista"] == [
        "860034594", "8600345941",
    ]


def test_the_legacy_contractor_name_path_returns_when_the_flag_is_off(flag_off):
    """Vuelve el LIKE sobre el nombre del proveedor, con su falsa atribución."""
    params = parse("contratos del proveedor FUNDACARIBE")
    assert params["contratista"] == "fundacaribe"
    assert params.get("actor_resolution") is None


def test_the_legacy_dataset_selection_is_untouched_when_the_flag_is_off(flag_off):
    """Sin flag no hay promoción a contratos: "historial contractual" sigue
    resolviendo a procesos, como antes de AP2."""
    assert parse("historial contractual del NIT 860002400")["dataset"] == "procesos"


# ---------------------------------------------------------------------------
# SoQL: el WHERE vuelve a su forma previa.
# ---------------------------------------------------------------------------


def test_the_where_loses_the_third_variant_when_the_flag_is_off(flag_off):
    soql = soql_for("contratos del NIT 800.091.140-4")
    assert "documento_proveedor = '800091140'" in soql
    assert "documento_proveedor = '8000911404'" in soql
    assert "documento_proveedor = '800091140-4'" not in soql


def test_an_actor_word_falls_back_into_objeto_when_the_flag_is_off(flag_off):
    """La regresión que AP2 cierra, restaurada: "persona" vuelve a ser un tema."""
    soql = soql_for("qué ha celebrado la persona 1140835135 en 2025")
    assert "persona" in soql.lower()


# ---------------------------------------------------------------------------
# Plan: P17 no bloquea nada con el flag apagado.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "procesos del NIT 860002400",
        "contratos del proveedor FUNDACARIBE",
    ],
)
def test_the_plan_never_blocks_on_actor_rules_when_the_flag_is_off(flag_off, text):
    plan = compute_query_plan(parse(text), text)
    assert plan.reason not in {"actor_dataset_unsupported", "actor_name_requires_resolution"}


def test_the_same_query_blocks_with_the_flag_on_and_runs_with_it_off(monkeypatch):
    """El contraste directo, en una sola prueba: el interruptor decide."""
    text = "procesos del NIT 860002400"

    on_plan = compute_query_plan(parse(text), text)
    assert on_plan.should_execute is False

    for module in ("app.core.query_router", "app.core.query_plan"):
        monkeypatch.setattr(f"{module}.actor_profile_enabled", lambda: False)
    off_plan = compute_query_plan(parse(text), text)
    assert off_plan.reason != "actor_dataset_unsupported"
