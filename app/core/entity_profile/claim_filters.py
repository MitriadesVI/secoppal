"""Claims de las dimensiones que el parser ya resolvió, sin gobierno posicional.

Tema, modalidad, tipo, valor, foco y operación no se disputan ocurrencias entre
sí: ninguna reclama a otra como sujeto. Por eso viven aparte de los adaptadores
de cláusula, cuyo trabajo entero es decidir quién gobierna a quién.

El árbol temático se reconstruye de la forma que el parser YA publica —una
lista anidada en ``objeto`` es un grupo OR, ``objeto_or`` es una disyunción,
``objeto_phrases`` son frases de catálogo y ``objeto_exclude`` exclusiones—.
Ese anidamiento existía y se aplanaba al llegar aquí; conservarlo es todo lo
que hacía falta para que AND y OR dejaran de ser el mismo universo.
"""
from __future__ import annotations

from app.core.entity_profile.claims import (
    ClaimDimension,
    SemanticClaim,
    UnresolvedIssue,
)
from app.core.entity_profile.frame_values import (
    Selection,
    TopicAll,
    TopicAny,
    TopicNode,
    TopicNot,
    TopicPhrase,
    TopicTerm,
    ValueConstraint,
    decimal_or_none,
)


TOPIC_AUTHORITY = ("query_router.objeto", "1")
MODALITY_AUTHORITY = ("modality_resolver", "1")
CONTRACT_TYPE_AUTHORITY = ("contract_type_projection", "1")
VALUE_AUTHORITY = ("money_authority", "1")
FOCUS_AUTHORITY = ("entity_profile.focus", "1")
OPERATION_AUTHORITY = ("entity_profile.operation", "1")


def topic_expression(params: dict) -> TopicNode | None:
    """El árbol que el parser publicó, con su agrupamiento intacto."""
    conjuncts: list[TopicNode] = []
    for entry in params.get("objeto") or ():
        node = _term_node(entry)
        if node is not None:
            conjuncts.append(node)
    disjuncts = [
        TopicTerm(str(term)) for term in params.get("objeto_or") or () if term
    ]
    if len(disjuncts) == 1:
        conjuncts.append(disjuncts[0])
    elif disjuncts:
        conjuncts.append(TopicAny(tuple(disjuncts)))
    for canonical in params.get("objeto_phrases") or ():
        if canonical:
            conjuncts.append(TopicPhrase(str(canonical)))
    for term in params.get("objeto_exclude") or ():
        if term:
            conjuncts.append(TopicNot(TopicTerm(str(term))))
    if not conjuncts:
        return None
    return conjuncts[0] if len(conjuncts) == 1 else TopicAll(tuple(conjuncts))


def _term_node(entry: object) -> TopicNode | None:
    """Una entrada de ``objeto``: término requerido, o grupo OR del parser."""
    if isinstance(entry, (list, tuple)):
        children = tuple(TopicTerm(str(item)) for item in entry if item)
        if not children:
            return None
        return children[0] if len(children) == 1 else TopicAny(children)
    return TopicTerm(str(entry)) if entry else None


def modality_selection(params: dict) -> Selection:
    return Selection(
        include=_ids(params.get("modalidad_include_procedure_ids")),
        exclude=_ids(params.get("modalidad_exclude_procedure_ids")),
    )


def contract_type_selection(params: dict) -> tuple[Selection, UnresolvedIssue | None]:
    """El tipo negativo está modelado, pero la base no acredita su proyección."""
    include = _ids(params.get("contract_type_include"))
    exclude = _ids(params.get("contract_type_exclude"))
    if exclude:
        return Selection(include=include), UnresolvedIssue(
            "CONTRACT_TYPE_EXCLUDE_UNSUPPORTED",
            "el tipo de contrato negativo no tiene proyección acreditada",
            exclude,
        )
    return Selection(include=include), None


def value_constraint(params: dict, magnitude: str) -> ValueConstraint:
    lower = decimal_or_none(params.get("valor_min"))
    upper = decimal_or_none(params.get("valor_max"))
    if lower is None and upper is None:
        return ValueConstraint()
    return ValueConstraint(lower=lower, upper=upper, magnitude=magnitude)


def filter_claims(
    params: dict, *, turn_id: str, span: tuple[int, int], next_id,
) -> tuple[list[SemanticClaim], list[UnresolvedIssue]]:
    """Un claim por dimensión material presente, todos sobre el mismo tramo.

    El span es el del turno completo a propósito: estas dimensiones no compiten
    por ocurrencias y el parser no publica offsets para ellas. Un span
    inventado más estrecho afirmaría una posición que nadie acreditó.
    """
    claims: list[SemanticClaim] = []
    issues: list[UnresolvedIssue] = []

    def publish(dimension: ClaimDimension, value: object, authority) -> None:
        claims.append(SemanticClaim(
            id=next_id(dimension.value.lower()), turn_id=turn_id, span=span,
            dimension=dimension, canonical_value=value,
            authority=authority[0], authority_version=authority[1],
        ))

    topic = topic_expression(params)
    if topic is not None:
        publish(ClaimDimension.TOPIC, topic, TOPIC_AUTHORITY)

    modality = modality_selection(params)
    if not modality.is_empty:
        publish(ClaimDimension.MODALITY, modality, MODALITY_AUTHORITY)

    contract_type, issue = contract_type_selection(params)
    if issue is not None:
        issues.append(issue)
    if not contract_type.is_empty:
        publish(ClaimDimension.CONTRACT_TYPE, contract_type, CONTRACT_TYPE_AUTHORITY)

    return claims, issues


def _ids(value: object) -> tuple[str, ...]:
    if not value or isinstance(value, (str, bytes)):
        return (str(value),) if isinstance(value, str) and value else ()
    return tuple(str(item) for item in value if item)
