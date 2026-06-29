"""Auditoría diferencial de recall — localiza el predicado que vacía un refinamiento.

Etapa 3 del PLAN_DOGFOOD_REMEDIACION_2026-06-27. Un refinamiento conversacional
puede tener params CORRECTOS y aun así devolver 0 filas (campo territorial
equivocado, valor en otra columna, AND imposible, dato ausente, etc.). Reducir el
timeout no arregla un cero de recall: primero hay que saber QUÉ predicado lo mata.

Diseño (decisión del asesor):
- **Por EXISTENCIA (`LIMIT 1`), no `COUNT(*)`.** El conteo es sospechoso de la
  latencia (Etapa 5/6); un count por predicado convertiría el diagnóstico en una
  fábrica de timeouts. Para hallar el predicado asesino basta saber si el conjunto
  quedó vacío. El conteo exacto es opcional (``deep=True``).
- **Reutiliza el WHERE de producción** (``SoQLBuilder.build``): el waterfall mide
  el MISMO predicado que ejecuta el pipeline, no una aproximación.
- **Solo para diagnóstico**: zero-after-refinement, casos marcados en dogfood, o
  modo debug. NUNCA en una consulta normal (el cableado al path de zero-results es
  Etapa 4).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from app.core.soql_builder import SoQLBuilder


class _Client(Protocol):
    def query(self, dataset_id: str, soql_query: str) -> list[dict]: ...
    def count(self, dataset_id: str, soql_count_query: str) -> int: ...


# Capas de predicado, de lo más fundamental (tema) a lo más restrictivo. El orden
# importa: el primer nivel que vacía el conjunto es el "killer".
_LAYERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("tema", ("objeto", "objeto_or", "objeto_phrases", "objeto_exclude")),
    ("scope", (
        "entidad_resolved", "entidad_like", "entidad_ecosystem",
        "ciudad", "ciudad_resolved",
        "departamento_resolved", "departamentos_in",
    )),
    ("fecha", ("fecha_desde", "fecha_hasta")),
    ("estado", (
        "estado", "estado_family", "estado_field",
        "estado_de_apertura_del_proceso", "estado_del_procedimiento",
        "estado_contrato",
    )),
    ("valor", ("valor_min", "valor_max")),
    ("modalidad", ("modalidad",)),
)

# Claves que el builder necesita en cada probe pero que no son predicados-capa.
_CARRY_KEYS: tuple[str, ...] = ("dataset", "dataset_explicit", "intent_type")

_EMPTY = (None, [], "", {}, False)


def _has(params: dict, key: str) -> bool:
    return params.get(key) not in _EMPTY


@dataclass(frozen=True)
class PredicateStep:
    predicate: str
    added_fields: dict[str, Any]
    exists: bool
    killer_predicate: bool = False
    count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"predicate": self.predicate, "exists": self.exists}
        if self.killer_predicate:
            out["killer_predicate"] = True
        if self.count is not None:
            out["count"] = self.count
        out["added_fields"] = sorted(self.added_fields)
        return out


def present_layers(params: dict) -> list[tuple[str, list[str]]]:
    """Capas con al menos un campo presente en params, en orden de auditoría."""
    return [
        (name, [k for k in keys if _has(params, k)])
        for name, keys in _LAYERS
        if any(_has(params, k) for k in keys)
    ]


def build_predicate_waterfall(
    params: dict,
    client: _Client,
    *,
    deep: bool = False,
) -> list[PredicateStep]:
    """Activa predicados de a uno y mide existencia (`LIMIT 1`) hasta que se vacía.

    Devuelve los pasos hasta el primer ``exists=False`` (marcado
    ``killer_predicate``). Si todos existen, el refinamiento no se vacía por un
    predicado — la causa es aguas abajo (ranking/validación), no el WHERE.
    """
    builder = SoQLBuilder()
    dataset_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    cumulative: dict[str, Any] = {k: params[k] for k in _CARRY_KEYS if _has(params, k)}

    steps: list[PredicateStep] = []
    for name, keys in present_layers(params):
        added = {k: params[k] for k in keys}
        cumulative.update(added)
        probe = {**cumulative, "limit": 1}
        rows = client.query(dataset_id, builder.build(dataset_id, probe))
        exists = bool(rows)
        count = None
        if deep:
            count = client.count(dataset_id, builder.build_count(dataset_id, cumulative))
        killer = not exists
        steps.append(PredicateStep(name, added, exists, killer, count))
        if killer:
            break
    return steps


def killer_predicate(steps: list[PredicateStep]) -> str | None:
    """Nombre del predicado que vació el conjunto, o None si ninguno lo hizo."""
    for step in steps:
        if step.killer_predicate:
            return step.predicate
    return None


def waterfall_to_dict(steps: list[PredicateStep]) -> dict[str, Any]:
    """Forma serializable para trace / eval_dogfood."""
    return {
        "predicate_waterfall": [s.to_dict() for s in steps],
        "killer_predicate": killer_predicate(steps),
    }
