"""Todo filtro que cambia el universo, tipado y en un solo objeto.

`INV-FILTER-IN-SQL-IMPLIES-FILTER-DECLARED`.

La revisión independiente (F4) mostró que el alcance visible del perfil omitía
filtros que sí llegaban al WHERE:

    estado_contrato = 'terminado'          -> el alcance no tenía campo de estado
    NOT (coalesce(...interventoria...))    -> exclusión omitida
    modalidad_de_contratacion IN (...)     -> omitida
    tipo_de_contrato = 'Obra'              -> omitido
    precio_base >= 1e8 / valor >= 1e8      -> sin declarar que son dos magnitudes

y —lo más grave— que ``objeto`` y ``objeto_or`` producían SQL distinto con el
MISMO snapshot: "mantenimiento Y vías" y "mantenimiento O vías" se declaraban
igual. Un usuario no puede distinguir dos universos distintos.

La causa era que el alcance se copiaba del ``SemanticRequest``, cuya tupla
``topic`` había aplanado los operadores. Aquí se conserva la expresión con su
forma, y el alcance se deriva de los params YA PROYECTADOS por cada SourcePlan
—los mismos que el builder convierte en WHERE—, de modo que
`RenderedScope == ExecutedScope` deja de ser una afirmación y pasa a ser una
consecuencia estructural.

Este módulo NO vuelve a parsear texto: consume las dimensiones que el parser ya
resolvió (``objeto``, ``objeto_or``, ``objeto_phrases``, ``objeto_exclude``,
``estado_*``, ``modalidad_include_procedure_ids``, ``contract_type_include``,
``valor_min``/``valor_max``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET


VALUE_LABEL = {
    PROCESS_DATASET: "presupuesto estimado",
    CONTRACT_DATASET: "valor del contrato",
}

_STATE_KEYS = (
    "estado_del_procedimiento", "estado_de_apertura_del_proceso",
    "estado_contrato", "estado_contrato_adicional",
)


def _terms(value: object) -> tuple[str, ...]:
    """Aplana un valor de tema conservando su orden y sin inventar términos."""
    if value in (None, "", [], ()):
        return ()
    if isinstance(value, str):
        return (value,)
    items: list[str] = []
    for item in value:
        if isinstance(item, (list, tuple)):
            items.extend(str(sub) for sub in item if sub)
        elif item:
            items.append(str(item))
    return tuple(dict.fromkeys(items))


@dataclass(frozen=True, slots=True)
class TopicExpression:
    """El tema con su forma: AND, OR, frase y exclusión NO son lo mismo."""

    all_terms: tuple[str, ...] = ()
    any_terms: tuple[str, ...] = ()
    phrases: tuple[str, ...] = ()
    excluded_terms: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not (
            self.all_terms or self.any_terms or self.phrases or self.excluded_terms
        )

    @classmethod
    def from_params(cls, params: dict) -> "TopicExpression":
        return cls(
            all_terms=_terms(params.get("objeto")),
            any_terms=_terms(params.get("objeto_or")),
            phrases=_terms(params.get("objeto_phrases")),
            excluded_terms=_terms(params.get("objeto_exclude")),
        )

    def describe(self) -> str | None:
        """Una lectura humana en la que AND y OR no pueden confundirse."""
        parts: list[str] = []
        if self.all_terms:
            parts.append(" y ".join(self.all_terms))
        if self.any_terms:
            parts.append(" o ".join(self.any_terms))
        for phrase in self.phrases:
            parts.append(f"«{phrase}»")
        if not parts and not self.excluded_terms:
            return None
        described = "; ".join(parts) if parts else "cualquier tema"
        if self.excluded_terms:
            described += f", excepto {' y '.join(self.excluded_terms)}"
        return described

    def to_dict(self) -> dict:
        return {
            "all_terms": list(self.all_terms),
            "any_terms": list(self.any_terms),
            "phrases": list(self.phrases),
            "excluded_terms": list(self.excluded_terms),
        }

    @classmethod
    def from_dict(cls, raw: object) -> "TopicExpression":
        if not isinstance(raw, dict):
            return cls()
        return cls(
            all_terms=_terms(raw.get("all_terms")),
            any_terms=_terms(raw.get("any_terms")),
            phrases=_terms(raw.get("phrases")),
            excluded_terms=_terms(raw.get("excluded_terms")),
        )

    @property
    def flat_terms(self) -> tuple[str, ...]:
        """Vista plana, sólo para compatibilidad; nunca para declarar alcance."""
        return tuple(dict.fromkeys(
            (*self.all_terms, *self.any_terms, *self.phrases),
        ))


def _modality_labels(ids: tuple[str, ...]) -> tuple[str, ...]:
    """IDs de procedimiento traducidos por la autoridad de modalidad.

    Un ID interno no le dice nada a una persona. El catálogo ya publica
    ``display_name``; no se inventa una segunda tabla de etiquetas.
    """
    if not ids:
        return ()
    try:
        from app.core.modality_models import load_catalog

        catalog = load_catalog()
    except Exception:
        return ids
    labels: list[str] = []
    for procedure_id in ids:
        classification = catalog.by_id(procedure_id)
        labels.append(
            getattr(classification, "display_name", None) or procedure_id,
        )
    return tuple(dict.fromkeys(labels))


def _money(amount: float) -> str:
    from app.core.analytics.base import format_currency

    return f"${format_currency(amount)}"


@dataclass(frozen=True, slots=True)
class MaterialFilters:
    """Los filtros de UNA fuente, leídos de los params que ejecutará."""

    dataset_id: str
    topic: TopicExpression = TopicExpression()
    current_state: tuple[str, ...] = ()
    modality_ids: tuple[str, ...] = ()
    contract_types: tuple[str, ...] = ()
    value_min: float | None = None
    value_max: float | None = None

    @classmethod
    def from_params(cls, dataset_id: str, params: dict) -> "MaterialFilters":
        state: list[str] = []
        for key in _STATE_KEYS:
            state.extend(_terms(params.get(key)))
        return cls(
            dataset_id=dataset_id,
            topic=TopicExpression.from_params(params),
            current_state=tuple(dict.fromkeys(state)),
            modality_ids=_terms(params.get("modalidad_include_procedure_ids")),
            contract_types=_terms(params.get("contract_type_include")),
            value_min=_number(params.get("valor_min")),
            value_max=_number(params.get("valor_max")),
        )

    @property
    def has_value_filter(self) -> bool:
        return self.value_min is not None or self.value_max is not None

    def value_line(self, *, name_magnitude: bool) -> str | None:
        """El rango, y con dos fuentes también QUÉ magnitud mide cada una."""
        if not self.has_value_filter:
            return None
        if self.value_min is not None and self.value_max is not None:
            span = f"entre {_money(self.value_min)} y {_money(self.value_max)}"
        elif self.value_min is not None:
            span = f"desde {_money(self.value_min)}"
        else:
            span = f"hasta {_money(self.value_max)}"
        if name_magnitude:
            return f"{VALUE_LABEL[self.dataset_id].capitalize()}: {span}."
        return f"Valor: {span}."

    def describe(self, *, name_magnitude: bool = False) -> tuple[str, ...]:
        """Una línea por dimensión material presente en el WHERE."""
        lines: list[str] = []
        topic = self.topic.describe()
        if topic:
            lines.append(f"Tema: {topic}.")
        if self.current_state:
            lines.append(f"Estado actual: {', '.join(self.current_state)}.")
        if self.modality_ids:
            lines.append(
                f"Modalidad: {', '.join(_modality_labels(self.modality_ids))}.",
            )
        if self.contract_types:
            lines.append(f"Tipo de contrato: {', '.join(self.contract_types)}.")
        value = self.value_line(name_magnitude=name_magnitude)
        if value:
            lines.append(value)
        return tuple(lines)

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "topic": self.topic.to_dict(),
            "current_state": list(self.current_state),
            "modality_ids": list(self.modality_ids),
            "contract_types": list(self.contract_types),
            "value_min": self.value_min,
            "value_max": self.value_max,
        }


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def filters_for(plan: Any) -> MaterialFilters:
    """Los filtros que ESTE plan de fuente va a ejecutar realmente."""
    return MaterialFilters.from_params(plan.dataset_id, plan.params)
