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
- ``extract_event_metadata`` — proyecta los datos que describen un evento ya
  decidido (modalidad, cierre, contratista, firma). NO participa en la decisión
  de cambio: ver su docstring.
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


def extract_event_metadata(row: dict, dataset_id: str) -> dict:
    """Proyecta los datos que describen un evento ya decidido. Función PURA.

    Vive separada de ``extract_fields`` a propósito, y la separación es la regla
    de negocio, no una preferencia de estilo:

    - ``extract_fields``          → campos que DECIDEN si hubo cambio. Los lee el
      diff (``status``) y ``snapshot_hash`` (``status``+``value``). Ampliarla
      metería estos cuatro campos en esa decisión y un cambio de modalidad o de
      fecha de cierre fingiría un ``changed_state``/``changed_value``.
    - ``extract_event_metadata``  → campos que DESCRIBEN mejor un evento que ya
      fue decidido. Nadie los compara; solo se congelan.

    Se congelan en el evento porque ``radar_seen_results`` es estado actual
    mutable: completar una notificación leyéndolo mostraría el valor de HOY junto
    al cambio de AYER. El evento es un hecho fechado (``radar_events.ts``).

    Los nombres de columna salen de ``SoQLBuilder.SPECS`` —nunca se adivinan— y
    un dataset que no expone el hecho devuelve ``None`` (procesos no tiene firma;
    contratos no tiene cierre de ofertas).

    Falla CERRADO ante un dataset desconocido, y por eso no usa ``_spec()``: ese
    helper cae al spec de procesos, así que una fila ajena que por casualidad
    traiga ``modalidad_de_contratacion`` se leería como si fuera un proceso. Para
    ``extract_fields`` ese fallback es comportamiento heredado que este bloque no
    toca; para un hecho que se congela y luego se le muestra a alguien, adivinar
    el dataset es peor que no saber.
    """
    spec = SoQLBuilder.SPECS.get(dataset_id)
    if spec is None:
        return {
            "modality": None, "closing_date": None,
            "contractor": None, "signature_date": None,
        }
    return {
        "modality": _or_none(row, spec.modality),
        "closing_date": _or_none(row, spec.closing_date),
        "contractor": _or_none(row, spec.contractor),
        "signature_date": _or_none(row, spec.signature_date),
    }


def _or_none(row: dict, field_name: str | None) -> str | None:
    """Texto fuente del campo, o ``None`` si el dataset no lo expone o viene vacío.

    ``""`` y ``"   "`` colapsan a ``None``: "presente pero vacío" y "ausente" son
    el mismo hecho —no sabemos el dato— y guardarlos distinto obligaría a cada
    lector a tratar ambos casos. Solo se recortan los bordes; el texto fuente se
    conserva sin interpretar (no se parsean fechas, no se traducen modalidades).
    """
    if not field_name:
        return None
    value = _as_str(row.get(field_name))
    return value or None


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
