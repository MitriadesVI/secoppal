"""El WHERE del perfil, emitido desde predicados YA acreditados.

`_build_where(params)` no sirve para esto y no es un defecto suyo: es una ruta
de RECUPERACIÓN. Degrada AND a OR cuando los términos son raros, aplica la
política de oportunidad, añade estados por defecto y reinterpreta claves
legacy. Todo eso es correcto para traer candidatos que después se rerankean, y
es exactamente lo contrario de lo que necesita un agregado institucional, donde
el universo es la respuesta y no hay ranking posterior que lo corrija.

Lo que se hace aquí es reutilizar sus EMISORES —comprador, término, frase,
estado, modalidad, tipo, fecha, valor— sin su capa de decisiones. No se crea
un segundo catálogo de modalidades, ni un vocabulario de estados propio, ni
otra resolución de comprador: son las mismas funciones, con una entrada que ya
está decidida.

Si una forma del árbol temático no tiene emisor seguro, se declara no
soportada y el compilador aclara. No se aproxima aplanando ni cambiando
conectores.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.core.contract_type_projection import project_contract_types
from app.core.entity_profile.frame import BuyerScope, BuyerScopeKind, SourcePredicates
from app.core.entity_profile.frame_values import (
    DATE_FIELD,
    VALUE_FIELD,
    SourceKind,
    TopicAll,
    TopicAny,
    TopicNode,
    TopicNot,
    TopicPhrase,
    TopicTerm,
    TopicTrue,
)
from app.core.modality_filter import ModalityFilter
from app.core.modality_dataset_projection import project_modality_filter


class UnsupportedPredicateError(ValueError):
    """Una forma que no tiene emisor acreditado. Nunca se aproxima."""


@dataclass(frozen=True, slots=True)
class CompiledPredicates:
    """El WHERE congelado y los trozos de los que salió."""

    where: str
    clauses: tuple[str, ...]


def compile_predicates(
    builder,
    source: SourceKind,
    predicates: SourcePredicates,
    buyer: BuyerScope,
) -> CompiledPredicates:
    """Predicados acreditados -> WHERE, en un orden estable y reproducible."""
    spec = builder.SPECS[source.dataset_id]
    clauses: list[str] = [_buyer_clause(builder, spec, buyer)]

    if source is SourceKind.CONTRACTS:
        from app.core.contract_universe import predicate
        universe_clause = predicate(predicates.contract_universe)
        if universe_clause:
            clauses.append(universe_clause)

    topic = _topic_clause(builder, spec, predicates.topic)
    if topic:
        clauses.append(topic)

    for predicate in predicates.current_state.predicates:
        builder._add_estado_clause(clauses, predicate.field, list(predicate.values))

    modality = _modality_clause(builder, source, predicates)
    if modality:
        clauses.append(modality)

    contract_type = _contract_type_clause(spec, predicates)
    if contract_type:
        clauses.append(contract_type)

    period = predicates.temporal.period
    date_field = DATE_FIELD[source]
    if period.lower:
        operator = ">=" if period.lower_inclusive else ">"
        clauses.append(f"{date_field} {operator} '{builder._escape(period.lower)}'")
    if period.upper:
        operator = "<=" if period.upper_inclusive else "<"
        clauses.append(f"{date_field} {operator} '{builder._escape(period.upper)}'")

    value_field = VALUE_FIELD[source]
    if predicates.value.lower is not None:
        clauses.append(f"{value_field} >= {_number(predicates.value.lower)}")
    if predicates.value.upper is not None:
        clauses.append(f"{value_field} <= {_number(predicates.value.upper)}")

    return CompiledPredicates(" AND ".join(clauses), tuple(clauses))


def _buyer_clause(builder, spec, buyer: BuyerScope) -> str:
    """La identidad del comprador la proyecta su propia autoridad."""
    if buyer.kind is BuyerScopeKind.BUYER_GROUP:
        from app.core.buyer_groups import get_groups

        group = get_groups().get(buyer.group_id)
        if group is None:
            raise UnsupportedPredicateError(f"unknown buyer group: {buyer.group_id}")
        return builder._exact_entity_clause(spec.entity, group.names_for(spec.dataset_id))
    clauses = [
        builder._entity_clause(spec, name, is_like_fallback=False)
        for name in buyer.canonical_buyers
    ]
    return clauses[0] if len(clauses) == 1 else f"({' OR '.join(clauses)})"


def _topic_clause(builder, spec, node: TopicNode) -> str:
    """ALL es AND, ANY es OR, y ninguno se convierte en el otro."""
    if isinstance(node, TopicTrue):
        return ""
    if isinstance(node, TopicTerm):
        conditions = builder._object_term_conditions(spec, node.term)
        if not conditions:
            raise UnsupportedPredicateError(f"no emitter for term {node.term!r}")
        return f"({' OR '.join(conditions)})"
    if isinstance(node, TopicPhrase):
        from app.core.phrase_catalog import spec_for_canonical

        phrase_spec = spec_for_canonical(node.canonical)
        clause = (
            builder._phrase_recall_conditions(spec, phrase_spec) if phrase_spec else ""
        )
        if not clause:
            raise UnsupportedPredicateError(f"no catalog phrase: {node.canonical!r}")
        return clause
    if isinstance(node, TopicAll):
        parts = [_topic_clause(builder, spec, child) for child in node.children]
        return "(" + " AND ".join(p for p in parts if p) + ")"
    if isinstance(node, TopicAny):
        parts = [_topic_clause(builder, spec, child) for child in node.children]
        return "(" + " OR ".join(p for p in parts if p) + ")"
    if isinstance(node, TopicNot):
        # La exclusión se emite null-safe, igual que `objeto_exclude`: un LIKE
        # sobre NULL no es falso, y sin coalesce el NOT descartaría las filas
        # sin descripción.
        if not isinstance(node.operand, TopicTerm):
            raise UnsupportedPredicateError("only term exclusion has an emitter")
        conditions = builder._object_term_conditions(
            spec, node.operand.term, null_safe=True,
        )
        if not conditions:
            raise UnsupportedPredicateError("no emitter for the excluded term")
        return f"NOT ({' OR '.join(conditions)})"
    raise UnsupportedPredicateError(f"unknown topic node: {type(node).__name__}")


def _modality_clause(builder, source: SourceKind, predicates: SourcePredicates) -> str:
    if predicates.modality.is_empty:
        return ""
    projection = project_modality_filter(
        ModalityFilter(
            include_procedure_ids=predicates.modality.include,
            exclude_procedure_ids=predicates.modality.exclude,
        ),
        source.noun,
    )
    return builder._modality_clause(projection)


def _contract_type_clause(spec, predicates: SourcePredicates) -> str:
    if not predicates.contract_type.include:
        return ""
    projection = project_contract_types(
        list(predicates.contract_type.include),
        type_field=spec.contract_type,
        justification_field=spec.contract_justification,
        text_fields=tuple(
            field for field in (spec.object_name, spec.description) if field
        ),
    )
    return projection.clause or ""


def _number(amount: Decimal) -> str:
    """Un límite exacto. ``int()`` truncaba decimales que la persona escribió."""
    if amount == amount.to_integral_value():
        return str(int(amount))
    return str(amount)
