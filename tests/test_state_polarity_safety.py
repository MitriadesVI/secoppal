"""INV-NO-SILENT-SEMANTIC-FALLBACK aplicado a la polaridad de estado.

Estas pruebas son deliberadamente **generativas sobre la familia completa**, no
sobre las formas que alguien observó fallar. El hallazgo que las motiva es que
"no estén abiertos" y "no estén publicados" fallaban de maneras opuestas —una
invertía la polaridad, la otra la perdía— solo porque una forma estaba en un
catálogo y la otra no. Comprobar las dos formas observadas habría dejado la
clase abierta.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from app.core.estado_families import (
    LIVENESS_TOKENS,
    NATURAL_TO_FAMILY,
    resolve_estado,
)
from app.core.negation_binding import NegationDimension, bind_negations
from app.core.query_router import _CITY_PATTERN_RE, QueryRouter
from app.core.state_vocabulary import STATE_SURFACE_FORMS

_POSITIVE_STATE_FIELDS = (
    "estado",
    "estado_field",
    "estado_family",
    "estado_contrato",
    "estado_de_apertura_del_proceso",
    "estado_del_procedimiento",
)

# Marcos negativos frecuentes en español. La cópula es la parte que el binder
# leía como contenido; incluir varias impide que el arreglo dependa de una.
_NEGATIVE_FRAMES = (
    "procesos que no esten {form}",
    "procesos que no sean {form}",
    "contratos que no esten {form}",
    "procesos que no se encuentren {form}",
)


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


@pytest.mark.parametrize("form", sorted(STATE_SURFACE_FORMS))
def test_every_state_form_binds_as_state_under_negation(form: str) -> None:
    """Ninguna forma de estado negada queda libre para el extractor positivo."""
    text = f"procesos de obra que no esten {form}"
    binding = bind_negations(text, city_pattern=_CITY_PATTERN_RE)

    dimensions = {request.dimension for request in binding.requests}
    assert NegationDimension.STATE in dimensions, (
        f"'{form}' negado no se reconoció como estado: {binding.requests}"
    )
    start = text.index(form)
    assert form not in binding.masked_text[start:], (
        f"'{form}' sobrevivió al enmascarado y puede volverse filtro positivo"
    )


@pytest.mark.parametrize("form", sorted(STATE_SURFACE_FORMS))
def test_negated_state_never_projects_a_positive_filter(
    router: QueryRouter, form: str,
) -> None:
    """NEGATED_STATE_CAN_TRIGGER_POSITIVE_FILTER = 0, sobre TODA la familia."""
    parsed = router.parse(f"procesos de obra en caldas que no esten {form}")
    projected = {
        field: parsed.params.get(field)
        for field in _POSITIVE_STATE_FIELDS
        if parsed.params.get(field)
    }
    assert not projected, (
        f"'{form}' negado proyectó estado positivo: {projected}"
    )


@pytest.mark.parametrize("frame", _NEGATIVE_FRAMES)
def test_copula_paradigm_does_not_change_the_verdict(
    router: QueryRouter, frame: str,
) -> None:
    """La cópula es gramática: cambiarla no puede cambiar la semántica."""
    parsed = router.parse(frame.format(form="publicados"))
    projected = {
        field: parsed.params.get(field)
        for field in _POSITIVE_STATE_FIELDS
        if parsed.params.get(field)
    }
    assert not projected, f"'{frame}' proyectó estado positivo: {projected}"


def test_multiple_negated_states_are_all_sealed() -> None:
    """Un segundo atributo tras ``ni`` también está dentro de la negación."""
    binding = bind_negations(
        "contratos que no esten cerrados ni terminados",
        city_pattern=_CITY_PATTERN_RE,
    )
    values = {
        request.value for request in binding.requests
        if request.dimension is NegationDimension.STATE
    }
    assert values == {"cerrados", "terminados"}


def test_positive_dimension_after_the_state_survives() -> None:
    """El estado se delimita solo: el territorio posterior sigue siendo positivo."""
    parsed = QueryRouter().parse("procesos que no esten adjudicados en caldas")
    assert parsed.params.get("departamento_resolved") == "Caldas"


# ── Autoridad léxica única ───────────────────────────────────────────────

def test_state_recognition_covers_every_semantic_catalogue() -> None:
    """Un token que la semántica resuelve, el reconocimiento debe verlo."""
    assert set(NATURAL_TO_FAMILY) <= STATE_SURFACE_FORMS
    assert {"publicados", "adjudicados", "liquidados"} <= STATE_SURFACE_FORMS


# ── Vigencia: una petición, dos ciclos de vida ───────────────────────────

@pytest.mark.parametrize("token", LIVENESS_TOKENS)
def test_liveness_is_dataset_aware(token: str) -> None:
    procesos = resolve_estado(token, "procesos")
    contratos = resolve_estado(token, "contratos")
    assert procesos is not None and contratos is not None
    assert procesos["family"] == "oferta_abierta"
    assert contratos["family"] == "contrato_activo"
    # Un proceso vivo NO puede proyectarse como expediente cerrado.
    assert "Cerrado" not in procesos["filters"].get(
        "estado_de_apertura_del_proceso", []
    )


def test_liveness_query_does_not_contradict_its_own_family() -> None:
    parsed = QueryRouter().parse("oportunidades vigentes de dotacion escolar en nariño")
    assert parsed.params["estado_family"] == "oferta_abierta"
    assert parsed.params.get("estado_de_apertura_del_proceso") != ["Cerrado"]
    assert "Seleccionado" not in (
        parsed.params.get("estado_del_procedimiento") or []
    )


# ── DUPLICATE_STATE_KEYS = 0 ─────────────────────────────────────────────

def test_no_state_dictionary_declares_a_literal_key_twice() -> None:
    """Independiente de la configuración de ruff: la fuente no puede mentir.

    Una clave repetida se pierde en silencio y deja un comentario afirmando lo
    contrario al lado del código muerto. Es la forma más barata de que una capa
    declare una semántica y otra proyecte la opuesta.
    """
    module = ast.parse(Path("app/core/estado_families.py").read_text())
    offenders: list[tuple[str, str]] = []
    for node in ast.walk(module):
        if not isinstance(node, ast.Dict):
            continue
        seen: set[str] = set()
        for key in node.keys:
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                continue
            if key.value in seen:
                offenders.append((str(node.lineno), key.value))
            seen.add(key.value)
    assert not offenders, f"claves duplicadas en estado_families.py: {offenders}"
