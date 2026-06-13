"""keys.py — identidad y snapshot de un resultado de radar.

Tres piezas:

- ``radar_result_key``  — clave estable que identifica un proceso/contrato
  a través de corridas. Misma lógica de campos que ``formatter._dedupe_results``
  (procesos: ``id_del_proceso`` | ``referencia_del_proceso``; contratos:
  ``id_contrato`` | ``referencia_del_contrato``).
- ``extract_fields``    — proyecta una fila SECOP cruda a los campos que el
  radar persiste (title, entity, value, status, urlproceso, referencia,
  contractor). Lee los nombres de campo de ``SoQLBuilder.SPECS`` para no
  hardcodearlos por dataset.
- ``snapshot_hash``     — sha1 de los campos sensibles a cambio (estado y
  valor). Si el hash difiere entre corridas, hubo un cambio que el diff
  clasifica como ``changed_state`` o ``changed_value``.
"""
from __future__ import annotations

import hashlib

from app.core.soql_builder import SoQLBuilder

PROCESOS_DATASET = SoQLBuilder.PROCESOS_DATASET  # "p6dx-8zbt"
CONTRATOS_DATASET = SoQLBuilder.CONTRATOS_DATASET  # "jbjy-vk9h"


def _key_fields(dataset_id: str) -> tuple[str, str]:
    if dataset_id == PROCESOS_DATASET:
        return ("id_del_proceso", "referencia_del_proceso")
    return ("id_contrato", "referencia_del_contrato")


def radar_result_key(row: dict, dataset_id: str) -> str | None:
    """Clave estable del resultado, o ``None`` si la fila no trae identidad.

    Filas sin clave no se pueden seguir entre corridas; el caller las omite.
    """
    for field in _key_fields(dataset_id):
        value = row.get(field)
        if value not in (None, "", [], {}):
            return str(value)
    return None


def _spec(dataset_id: str):
    return SoQLBuilder.SPECS.get(dataset_id) or SoQLBuilder.SPECS[PROCESOS_DATASET]


def _as_str(value) -> str:
    return "" if value in (None, [], {}) else str(value).strip()


def extract_fields(row: dict, dataset_id: str) -> dict:
    """Proyecta una fila SECOP a los campos que persiste el radar.

    Usa ``SoQLBuilder.SPECS[dataset_id]`` para resolver los nombres de campo
    según el dataset (procesos vs contratos).
    """
    spec = _spec(dataset_id)
    contractor = ""
    if spec.contractor:
        contractor = _as_str(row.get(spec.contractor))
    return {
        "title": _as_str(row.get(spec.object_name)),
        "entity": _as_str(row.get(spec.entity)),
        "value": _as_str(row.get(spec.value)),
        "status": _as_str(row.get(spec.state)),
        "urlproceso": _as_str(row.get(spec.url)),
        "referencia": _as_str(row.get(spec.reference)) if spec.reference else "",
        "contractor": contractor,
    }


def _value_token(raw_value: str) -> str:
    """Normaliza un valor monetario a entero string para que '1000' y
    '1000.00' produzcan el mismo hash (SECOP a veces devuelve decimales)."""
    if not raw_value:
        return ""
    try:
        return str(int(round(float(raw_value))))
    except (ValueError, TypeError):
        return raw_value


def snapshot_hash(row: dict, dataset_id: str) -> str:
    """sha1 de (status, value) — los dos campos cuyo cambio el radar reporta."""
    fields = extract_fields(row, dataset_id)
    payload = "|".join([
        fields["status"].lower(),
        _value_token(fields["value"]),
    ])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()
