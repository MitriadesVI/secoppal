"""INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE (HCA-FOLLOWUP-016).

Un follow-up que solo reordena puede cambiar ORDER BY y el cursor. Nada más.

El gate se pone sobre las SOURCE CALLS reales, no sobre el QueryFrame: el
defecto original vivía justamente en el hueco entre ambos —el frame heredaba
``entidad_ecosystem`` y la fuente lo materializaba como un OR de diez
compradores— así que comparar frames no lo habría detectado.
"""
from __future__ import annotations

import re
import uuid

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow

_ROWS = [{
    "id_contrato": "CO-1",
    "referencia_del_contrato": "REF-1",
    "nombre_entidad": "ENTIDAD SINTETICA",
    "objeto_del_contrato": "Objeto sintetico",
    "valor_del_contrato": "500000000",
    "precio_base": "500000000",
    "fecha_de_firma": "2026-01-01",
    "fecha_de_publicacion_del": "2026-01-01",
    "estado_contrato": "En ejecución",
    "ciudad": "Cali",
    "departamento": "Valle del Cauca",
}]


def _workflow() -> tuple[SecopalWorkflow, list[str]]:
    """Workflow con la fuente instrumentada. La persistencia la aísla conftest."""
    calls: list[str] = []
    wf = SecopalWorkflow(Settings())

    def _query(dataset_id, soql_query, **kwargs):
        calls.append(soql_query)
        return list(_ROWS)

    wf.secop_client.query = _query
    wf.secop_client.count = lambda *a, **kw: len(_ROWS)
    wf.secop_client.aggregate = lambda *a, **kw: []
    wf.narrator.narrate_with_grounding = lambda **kw: None
    return wf, calls


def _where(soql: str) -> str:
    """Filtros de una SoQL: todo entre WHERE y ORDER BY/LIMIT."""
    match = re.search(r"\bWHERE\b(.*?)(?:\bORDER BY\b|\bLIMIT\b|$)", soql, re.S)
    return re.sub(r"\s+", " ", match.group(1)).strip() if match else ""


def _order_by(soql: str) -> str:
    match = re.search(r"\bORDER BY\b(.*?)(?:\bLIMIT\b|$)", soql, re.S)
    return re.sub(r"\s+", " ", match.group(1)).strip() if match else ""


@pytest.mark.parametrize(
    ("base_query", "reorder_query"),
    [
        # El caso del defecto: territorio municipal sin comprador explícito.
        ("contratos de obra en cali por mas de 300 millones", "los mas antiguos primero"),
        # Territorio departamental.
        ("contratos de aseo en cesar", "ordenalos por mayor valor"),
        # Comprador explícito: el universo también debe quedar intacto.
        ("contratos del icbf en antioquia", "ordenalos por mayor valor"),
        # Tiempo + dinero + tipo contractual.
        ("contratos de obra en cali por mas de 300 millones en 2025", "ordenalos por mayor valor"),
        # Entidad resuelta por reescritura.
        ("contratos de la gobernacion de boyaca", "los mas antiguos primero"),
        # Estado explícito.
        ("contratos en ejecucion de obra en cali", "ordenalos por mayor valor"),
    ],
)
def test_ordering_only_followup_preserves_source_where(base_query, reorder_query):
    wf, calls = _workflow()
    chat_id = f"inv-ordering-{uuid.uuid4().hex[:10]}"

    wf.run_query(base_query, chat_id=chat_id)
    assert calls, f"el turno base no consultó la fuente: {base_query!r}"
    where_before = _where(calls[-1])
    order_before = _order_by(calls[-1])

    calls.clear()
    wf.run_query(reorder_query, chat_id=chat_id)
    assert calls, f"el follow-up no consultó la fuente: {reorder_query!r}"
    where_after = _where(calls[-1])
    order_after = _order_by(calls[-1])

    assert where_after == where_before, (
        "un follow-up de solo-orden cambió el universo de la fuente\n"
        f"  antes:   {where_before}\n"
        f"  después: {where_after}"
    )
    assert order_after != order_before, (
        f"el follow-up {reorder_query!r} no cambió el orden — el caso no prueba nada"
    )


def test_ordering_followup_does_not_inject_entity_ecosystem():
    """El síntoma concreto: un OR de compradores que nadie pidió."""
    wf, calls = _workflow()
    chat_id = f"inv-ecosystem-{uuid.uuid4().hex[:10]}"

    wf.run_query("contratos de obra en cali por mas de 300 millones", chat_id=chat_id)
    calls.clear()
    result = wf.run_query("los mas antiguos primero", chat_id=chat_id)

    assert "OR unaccent(UPPER(nombre_entidad))" not in calls[-1]
    assert not (result.get("parsed_params") or {}).get("entidad_ecosystem")


def test_explicit_buyer_survives_reordering():
    """El complemento: purgar el derivado no puede llevarse el universo real."""
    wf, calls = _workflow()
    chat_id = f"inv-explicit-{uuid.uuid4().hex[:10]}"

    wf.run_query("contratos del icbf en antioquia", chat_id=chat_id)
    calls.clear()
    wf.run_query("ordenalos por mayor valor", chat_id=chat_id)

    assert "ICBF" in calls[-1], "el comprador declarado por el usuario se perdió"
