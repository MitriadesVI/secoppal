"""Proyección completa de una familia de estado sobre los params.

INV-STATE-FAMILY-PROJECTS-COMPLETE (HCA-STATE-007).

Si el frame afirma ``estado_family = contrato_activo``, la consulta a la fuente
debe materializar TODA la familia autorizada por ADR-003 — En ejecución,
Modificado y Prorrogado — y no solo el primer miembro.

Por qué existe este módulo
--------------------------
La familia se puede declarar por dos caminos distintos y solo uno la
materializaba:

* ``estado_families.detect_estado_family`` devuelve familia **y** filtros de
  campo, así que la proyección viene incluida;
* ``intent_vocabulary`` declara ``estado_family`` como configuración de intent y
  no conoce el dataset todavía, así que nunca escribió filtros. Cuando ese
  camino ganaba —"contratos en ejecución de la alcaldía de Medellín"— el span ya
  quedaba consumido, el detector de familia no lo volvía a ver y el extractor
  legacy dejaba un escalar: ``estado_contrato = 'En ejecución'``. La familia se
  anunciaba en el frame y se truncaba en la fuente.

El invariante se aplica aquí, sobre los params ya resueltos, en vez de parchear
cada camino: cualquier ruta que declare una familia queda cubierta.

Relación con INV-EXACT-STATE-DOES-NOT-WIDEN-TO-FAMILY
-----------------------------------------------------
Este módulo RELLENA, nunca ensancha. Un campo de estado que ya trae valores es
una restricción que alguien decidió —el catálogo léxico exacto de
``estado_families``, un follow-up del usuario o un llamador estructurado— y se
respeta intacta. Los dos invariantes se necesitan mutuamente: sin este, una
familia declarada se proyecta a medias; sin el otro, este ensancharía
"publicados" al funnel completo.

Puro, determinista, sin estado y sin I/O.
"""
from __future__ import annotations

from typing import Any

from app.core.estado_families import ESTADO_FAMILIES

__all__ = ["STATE_FILTER_FIELDS", "project_estado_family"]

# Campos por los que una familia puede materializarse en la fuente.
STATE_FILTER_FIELDS: tuple[str, ...] = (
    "estado_de_apertura_del_proceso",
    "estado_del_procedimiento",
    "estado_contrato",
    "estado_contrato_adicional",
)


def _has_value(params: dict[str, Any], field: str) -> bool:
    return params.get(field) not in (None, "", [], (), {}, False)


def project_estado_family(params: dict[str, Any]) -> dict[str, Any]:
    """Materializa la familia declarada cuando ningún filtro de estado la cubre.

    Muta y devuelve ``params`` — el router trabaja sobre un dict propio.
    """
    family_name = params.get("estado_family")
    dataset = params.get("dataset")
    if not family_name or not dataset:
        return params

    dataset_config = ESTADO_FAMILIES.get(str(family_name), {}).get(str(dataset), {})
    projectable = {
        field: list(values)
        for field, values in dataset_config.items()
        if values
    }
    if not projectable:
        # La familia no aplica a este dataset (p. ej. oferta_abierta sobre
        # contratos). No inventamos un filtro: esa contradicción la resuelve
        # ``contract_invariants``, no esta capa.
        return params

    # Cualquier filtro de estado ya presente es una decisión previa: si el
    # usuario acotó a un literal, rellenar la familia le devolvería lo que
    # justamente excluyó.
    if any(_has_value(params, field) for field in STATE_FILTER_FIELDS):
        return params

    for field, values in projectable.items():
        params[field] = values
    return params
