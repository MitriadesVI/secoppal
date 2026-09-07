"""Los valores tipados del Frame: una dimensión, una forma, una serialización.

Existen aparte del Frame porque los publican los *claims* antes de que exista
un Frame, y porque el compilador los consume después de que el Frame se
confirmó. Un tipo compartido evita la tentación de volver a leer `params`
en cualquiera de los dos extremos.

Tres decisiones importan más que el resto:

* El tema es un ÁRBOL. `("mantenimiento", "vias")` no puede distinguir
  "mantenimiento y vías" de "mantenimiento o vías", y esa indistinción fue el
  hallazgo F4 de la revisión independiente: dos universos con el mismo alcance
  visible.
* Un periodo guarda sus límites YA resueltos y la fecha civil con la que se
  resolvieron. Recargar "agosto" seis meses después no vuelve a evaluarlo.
* El dinero es ``Decimal`` y se serializa como texto. ``float`` no es una
  serialización canónica y ``int()`` trunca; ninguna de las dos cosas puede
  pasarle a un límite que la persona escribió.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum


PROCESS_DATASET = "p6dx-8zbt"
CONTRACT_DATASET = "jbjy-vk9h"


class SourceKind(str, Enum):
    PROCESSES = "PROCESSES"
    CONTRACTS = "CONTRACTS"

    @property
    def dataset_id(self) -> str:
        return PROCESS_DATASET if self is SourceKind.PROCESSES else CONTRACT_DATASET

    @property
    def noun(self) -> str:
        return "procesos" if self is SourceKind.PROCESSES else "contratos"


class SourceOrigin(str, Enum):
    """Quién eligió la fuente. Sólo EXPLICIT es una palabra de la persona."""

    EXPLICIT = "EXPLICIT"
    DEFAULT = "DEFAULT"


class PredicateOrigin(str, Enum):
    EXPLICIT = "EXPLICIT"
    INHERITED = "INHERITED"
    DEFAULT = "DEFAULT"


class TemporalEventKind(str, Enum):
    """El hecho fechado que SECOP II publica para cada fuente."""

    NONE = "NONE"
    PROCESS_PUBLICATION = "PROCESS_PUBLICATION"
    CONTRACT_SIGNATURE = "CONTRACT_SIGNATURE"

    @property
    def source(self) -> "SourceKind | None":
        if self is TemporalEventKind.PROCESS_PUBLICATION:
            return SourceKind.PROCESSES
        if self is TemporalEventKind.CONTRACT_SIGNATURE:
            return SourceKind.CONTRACTS
        return None


DATE_FIELD = {
    SourceKind.PROCESSES: "fecha_de_publicacion_del",
    SourceKind.CONTRACTS: "fecha_de_firma",
}

VALUE_FIELD = {
    SourceKind.PROCESSES: "precio_base",
    SourceKind.CONTRACTS: "valor_del_contrato",
}

VALUE_MAGNITUDE = {
    SourceKind.PROCESSES: "presupuesto estimado",
    SourceKind.CONTRACTS: "valor del contrato",
}


@dataclass(frozen=True, slots=True)
class Period:
    """Límites resueltos, con la referencia civil que los produjo."""

    lower: str | None = None
    upper: str | None = None
    lower_inclusive: bool = True
    upper_inclusive: bool = True
    civil_reference: str | None = None
    expression: str | None = None

    @property
    def is_unbounded(self) -> bool:
        return self.lower is None and self.upper is None

    def fingerprint(self) -> str:
        if self.is_unbounded:
            return "period:UNBOUNDED"
        low = "[" if self.lower_inclusive else "("
        high = "]" if self.upper_inclusive else ")"
        return f"period:{low}{self.lower or ''},{self.upper or ''}{high}"

    def to_dict(self) -> dict:
        return {
            "lower": self.lower, "upper": self.upper,
            "lower_inclusive": self.lower_inclusive,
            "upper_inclusive": self.upper_inclusive,
            "civil_reference": self.civil_reference,
            "expression": self.expression,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "Period":
        if not isinstance(raw, dict):
            raise ValueError("period must be an object")
        return cls(
            lower=_optional_text(raw.get("lower")),
            upper=_optional_text(raw.get("upper")),
            lower_inclusive=bool(raw.get("lower_inclusive", True)),
            upper_inclusive=bool(raw.get("upper_inclusive", True)),
            civil_reference=_optional_text(raw.get("civil_reference")),
            expression=_optional_text(raw.get("expression")),
        )


UNBOUNDED_PERIOD = Period()


class TopicNode:
    """Nodo del árbol temático. Ningún consumidor lo aplana."""

    @property
    def is_true(self) -> bool:
        return False

    def fingerprint(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def to_dict(self) -> dict:  # pragma: no cover - overridden
        raise NotImplementedError

    @staticmethod
    def from_dict(raw: object) -> "TopicNode":
        if not isinstance(raw, dict):
            raise ValueError("topic node must be an object")
        kind = raw.get("kind")
        if kind == "TRUE":
            return TopicTrue()
        if kind == "TERM":
            return TopicTerm(str(raw["term"]))
        if kind == "PHRASE":
            return TopicPhrase(str(raw["canonical"]))
        if kind == "ALL":
            return TopicAll(tuple(TopicNode.from_dict(c) for c in raw["children"]))
        if kind == "ANY":
            return TopicAny(tuple(TopicNode.from_dict(c) for c in raw["children"]))
        if kind == "NOT":
            return TopicNot(TopicNode.from_dict(raw["operand"]))
        raise ValueError(f"unknown topic node: {kind!r}")


@dataclass(frozen=True, slots=True)
class TopicTrue(TopicNode):
    @property
    def is_true(self) -> bool:
        return True

    def fingerprint(self) -> str:
        return "TRUE"

    def to_dict(self) -> dict:
        return {"kind": "TRUE"}


@dataclass(frozen=True, slots=True)
class TopicTerm(TopicNode):
    term: str

    def fingerprint(self) -> str:
        return f"TERM({self.term})"

    def to_dict(self) -> dict:
        return {"kind": "TERM", "term": self.term}


@dataclass(frozen=True, slots=True)
class TopicPhrase(TopicNode):
    """Una frase de catálogo. Su política de coincidencia es la del catálogo."""

    canonical: str

    def fingerprint(self) -> str:
        return f"PHRASE({self.canonical})"

    def to_dict(self) -> dict:
        return {"kind": "PHRASE", "canonical": self.canonical}


@dataclass(frozen=True, slots=True)
class TopicAll(TopicNode):
    children: tuple[TopicNode, ...]

    def __post_init__(self) -> None:
        if not self.children:
            raise ValueError("ALL requires children")

    def fingerprint(self) -> str:
        return "ALL(" + ",".join(c.fingerprint() for c in self.children) + ")"

    def to_dict(self) -> dict:
        return {"kind": "ALL", "children": [c.to_dict() for c in self.children]}


@dataclass(frozen=True, slots=True)
class TopicAny(TopicNode):
    children: tuple[TopicNode, ...]

    def __post_init__(self) -> None:
        if not self.children:
            raise ValueError("ANY requires children")

    def fingerprint(self) -> str:
        return "ANY(" + ",".join(c.fingerprint() for c in self.children) + ")"

    def to_dict(self) -> dict:
        return {"kind": "ANY", "children": [c.to_dict() for c in self.children]}


@dataclass(frozen=True, slots=True)
class TopicNot(TopicNode):
    operand: TopicNode

    def fingerprint(self) -> str:
        return f"NOT({self.operand.fingerprint()})"

    def to_dict(self) -> dict:
        return {"kind": "NOT", "operand": self.operand.to_dict()}


@dataclass(frozen=True, slots=True)
class StatePredicate:
    field: str
    values: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StateExpression:
    """El estado ACTUAL de una fuente. Vacío es TRUE, no "sin definir"."""

    predicates: tuple[StatePredicate, ...] = ()

    @property
    def is_true(self) -> bool:
        return not self.predicates

    @property
    def field(self) -> str | None:
        return self.predicates[0].field if len(self.predicates) == 1 else None

    @property
    def values(self) -> tuple[str, ...]:
        return self.predicates[0].values if len(self.predicates) == 1 else ()

    def fingerprint(self) -> str:
        if self.is_true:
            return "state:TRUE"
        return "state:" + ";".join(
            f"{p.field}={'|'.join(p.values)}" for p in self.predicates
        )

    def to_dict(self) -> dict:
        return {"predicates": [
            {"field": p.field, "values": list(p.values)} for p in self.predicates
        ]}

    @classmethod
    def from_dict(cls, raw: object) -> "StateExpression":
        if not isinstance(raw, dict):
            return cls()
        return cls(tuple(
            StatePredicate(str(item["field"]), tuple(str(v) for v in item["values"]))
            for item in raw.get("predicates") or ()
        ))


@dataclass(frozen=True, slots=True)
class Selection:
    """Inclusiones y exclusiones de un vocabulario cerrado (modalidad, tipo)."""

    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.include and not self.exclude

    def fingerprint(self) -> str:
        return f"in:{'|'.join(self.include)}/ex:{'|'.join(self.exclude)}"

    def to_dict(self) -> dict:
        return {"include": list(self.include), "exclude": list(self.exclude)}

    @classmethod
    def from_dict(cls, raw: object) -> "Selection":
        if not isinstance(raw, dict):
            return cls()
        return cls(
            include=tuple(str(v) for v in raw.get("include") or ()),
            exclude=tuple(str(v) for v in raw.get("exclude") or ()),
        )


@dataclass(frozen=True, slots=True)
class ValueConstraint:
    """Un límite monetario exacto, con su magnitud declarada."""

    lower: Decimal | None = None
    upper: Decimal | None = None
    magnitude: str = ""
    currency: str = "COP"

    @property
    def is_unbounded(self) -> bool:
        return self.lower is None and self.upper is None

    def fingerprint(self) -> str:
        if self.is_unbounded:
            return "value:UNBOUNDED"
        return f"value:{self.lower or ''},{self.upper or ''}:{self.magnitude}"

    def to_dict(self) -> dict:
        return {
            "lower": None if self.lower is None else str(self.lower),
            "upper": None if self.upper is None else str(self.upper),
            "magnitude": self.magnitude, "currency": self.currency,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "ValueConstraint":
        if not isinstance(raw, dict):
            return cls()
        return cls(
            lower=decimal_or_none(raw.get("lower")),
            upper=decimal_or_none(raw.get("upper")),
            magnitude=str(raw.get("magnitude") or ""),
            currency=str(raw.get("currency") or "COP"),
        )


UNBOUNDED_VALUE = ValueConstraint()


def decimal_or_none(value: object) -> Decimal | None:
    """Un límite que no es un número finito no es un límite."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return None
    return number if number.is_finite() else None


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
