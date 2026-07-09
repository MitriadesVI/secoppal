"""diff.py — clasifica una corrida de radar contra la última revisión.

Compara los resultados deduplicados de la corrida actual contra el mapa de
resultados ya vistos (``radar_seen_results``, indexado por ``result_key``) y
los reparte en:

- ``new``            — clave nunca antes vista.
- ``changed_state``  — misma clave, cambió el estado.
- ``changed_value``  — misma clave, cambió el valor (pero no el estado).
- ``window_churn``   — clave vista antes que ya no aparece en la ventana actual.
- ``disappeared``    — clave verificada explícitamente como desaparecida.
- ``unchanged``      — misma clave, mismo snapshot.

Marca además ``contractor_new`` en los resultados nuevos cuyo contratista no
había aparecido en ninguna corrida previa (señal de "operador nuevo").

El criterio de cambio es ``snapshot_hash`` (status+value); estado vs valor se
distingue comparando los campos crudos contra el snapshot guardado.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.core.radar.keys import extract_fields, radar_result_key, snapshot_hash


@dataclass
class DiffItem:
    """Un resultado clasificado, con lo necesario para persistir y narrar."""

    key: str
    row: dict
    fields: dict
    snapshot_hash: str
    contractor_new: bool = False
    # Para cambios: valores previos (para narrar "de X a Y").
    prev_status: str = ""
    prev_value: str = ""


@dataclass
class RadarDiff:
    new: list[DiffItem] = field(default_factory=list)
    changed_state: list[DiffItem] = field(default_factory=list)
    changed_value: list[DiffItem] = field(default_factory=list)
    window_churn: list[DiffItem] = field(default_factory=list)
    disappeared: list[DiffItem] = field(default_factory=list)
    unchanged: list[DiffItem] = field(default_factory=list)

    @property
    def new_count(self) -> int:
        return len(self.new)

    @property
    def changed_count(self) -> int:
        return len(self.changed_state) + len(self.changed_value)

    @property
    def has_novelty(self) -> bool:
        return bool(self.new or self.changed_state or self.changed_value or self.disappeared)


def classify(
    current_results: list[dict],
    seen_map: dict[str, dict],
    dataset_id: str,
    *,
    current_window_keys: set[str] | None = None,
    confirmed_disappeared_keys: set[str] | None = None,
) -> RadarDiff:
    """Clasifica ``current_results`` (ya deduplicados) contra ``seen_map``.

    ``seen_map`` mapea ``result_key`` → registro guardado, que debe traer al
    menos ``snapshot_hash``, ``status``, ``value`` y ``contractor``.

    ``current_results`` puede incluir filas de la ventana reciente y filas
    refrescadas por lookup de clave. ``current_window_keys`` permite conservar
    la señal de churn de ventana: si una clave vista no aparece en esa ventana,
    se registra en ``window_churn`` aunque el refresh la haya encontrado.

    La ausencia en la ventana no prueba desaparición en SECOP. ``disappeared``
    solo se llena cuando el caller entrega claves verificadas explícitamente.
    """
    diff = RadarDiff()
    confirmed_disappeared_keys = confirmed_disappeared_keys or set()

    # Contratistas conocidos de corridas previas (para detectar operador nuevo).
    known_contractors = {
        (rec.get("contractor") or "").strip().lower()
        for rec in seen_map.values()
        if (rec.get("contractor") or "").strip()
    }

    current_keys: set[str] = set()
    for row in current_results:
        key = radar_result_key(row, dataset_id)
        if key is None:
            continue
        current_keys.add(key)
        fields = extract_fields(row, dataset_id)
        snap = snapshot_hash(row, dataset_id)
        seen = seen_map.get(key)

        if seen is None:
            contractor = fields["contractor"].strip().lower()
            item = DiffItem(
                key=key, row=row, fields=fields, snapshot_hash=snap,
                contractor_new=bool(contractor) and contractor not in known_contractors,
            )
            diff.new.append(item)
            continue

        if snap == seen.get("snapshot_hash"):
            diff.unchanged.append(
                DiffItem(key=key, row=row, fields=fields, snapshot_hash=snap)
            )
            continue

        # Hubo cambio: ¿estado o valor?
        prev_status = (seen.get("status") or "").strip()
        item = DiffItem(
            key=key, row=row, fields=fields, snapshot_hash=snap,
            prev_status=prev_status, prev_value=(seen.get("value") or ""),
        )
        if fields["status"].strip().lower() != prev_status.lower():
            diff.changed_state.append(item)
        else:
            diff.changed_value.append(item)

    if current_window_keys is None:
        current_window_keys = set(current_keys)

    # Ausentes de la ventana: churn de ventana, no desaparición confirmada.
    for key, rec in seen_map.items():
        if key in current_window_keys:
            continue
        item = DiffItem(
            key=key, row=dict(rec), fields={
                "title": rec.get("title", ""),
                "entity": rec.get("entity", ""),
                "value": rec.get("value", ""),
                "status": rec.get("status", ""),
                "urlproceso": rec.get("urlproceso", ""),
                "referencia": rec.get("referencia", ""),
                "contractor": rec.get("contractor", ""),
            },
            snapshot_hash=rec.get("snapshot_hash", ""),
            prev_status=rec.get("status", ""),
            prev_value=rec.get("value", ""),
        )
        if key in confirmed_disappeared_keys and key not in current_keys:
            diff.disappeared.append(item)
        else:
            diff.window_churn.append(item)

    return diff
