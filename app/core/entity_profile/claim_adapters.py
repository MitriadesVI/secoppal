"""Las autoridades existentes publican ocurrencias VINCULADAS, no listas planas.

No hay parser nuevo aquí. El vocabulario de fuente, el de eventos, el de
marcadores de actualidad, la autoridad temporal, la de estados y la de
comprador son las mismas de V1.1. Lo único que cambia es lo que se conserva:
en vez de colapsar cada dimensión en un escalar, cada ocurrencia queda con su
tramo y con la relación que la gobierna.

Dos consecuencias directas de eso:

* ``resolve_temporal_events`` deduplica por tipo de evento. Aquí no: dos
  ocurrencias de "firmados" son dos claims. Se reutiliza su vocabulario, no su
  colapso.
* ``resolve_temporal_scope`` se invoca **por cláusula**, que es lo que permite
  que "procesos publicados en julio y contratos firmados en agosto" tenga dos
  periodos. Es la misma autoridad temporal, aplicada al tramo que la gobierna,
  igual que ``clauses.py`` hizo con fuente y evento.

Cuando una relación no puede acreditarse, se publica una cuestión sin resolver.
Nunca se elige el vínculo que volvería ejecutable la consulta.
"""
from __future__ import annotations

from datetime import date as Date
import re
from typing import Any

from app.core.entity_profile.claim_filters import (
    FOCUS_AUTHORITY,
    OPERATION_AUTHORITY,
    VALUE_AUTHORITY,
    filter_claims,
    value_constraint,
)
from app.core.entity_profile.claims import (
    ClaimDimension,
    ClaimRelation,
    ClaimSet,
    RelationKind,
    SemanticClaim,
    UnresolvedIssue,
)
from app.core.entity_profile.clauses import Clause, split_clauses
from app.core.entity_profile.frame_values import (
    Period,
    SourceKind,
    StatePredicate,
    TemporalEventKind,
)
from app.core.entity_profile.source_request import (
    _ANY_SOURCE_WORD,
    _PROCESS_WORDS,
    _buyer_surfaces,
)
from app.core.entity_profile.temporal_event import (
    _CURRENT_MARKER,
    _PUBLICATION,
    _SIGNATURE,
    _UNSUPPORTED_EVENTS,
)
from app.core.entity_profile.text_offsets import NormalizedText, normalize_with_offsets
from app.core.entity_profile.turn import _FOCUS, _PROFILE_NOISE
from app.core.estado_families import detect_estado_family
from app.core.query_text_consumption import mask_consumed_spans
from app.core.temporal_semantics import civil_today, resolve_temporal_scope


SOURCE_AUTHORITY = ("entity_profile.source_request", "2")
EVENT_AUTHORITY = ("entity_profile.temporal_event", "2")
PERIOD_AUTHORITY = ("temporal_semantics", "1")
STATE_AUTHORITY = ("estado_families", "1")
BUYER_AUTHORITY = ("entity_profile.scope", "1")
LEGACY_STATE_AUTHORITY = ("entity_profile.legacy_state", "1")

_STATE_FIELDS = {
    SourceKind.PROCESSES: frozenset(
        {"estado_del_procedimiento", "estado_de_apertura_del_proceso"},
    ),
    SourceKind.CONTRACTS: frozenset(
        {"estado_contrato", "estado_contrato_adicional"},
    ),
}

# Una elección entre fuentes ("procesos o contratos") no es una coordinación:
# la persona pide una de las dos y el sistema no puede escoger por ella.
_SOURCE_OR = re.compile(
    r"\b(?:procesos?|licitaciones?|convocatorias?|procedimientos?|contratos?)\s+o\s+"
    r"(?:procesos?|licitaciones?|convocatorias?|procedimientos?|contratos?)\b"
)
# Lo único que puede preceder al núcleo de fuente sin dejar de ser núcleo:
# el lenguaje con que se pide un perfil —``_PROFILE_NOISE`` ya es esa
# autoridad— más determinantes y conectores. Cualquier otra palabra abre un
# complemento: en "gestión de procesos industriales" la ocurrencia pertenece
# al tema, no a la elección de fuente.
_HEAD_FILLERS = frozenset({
    "de", "del", "la", "el", "los", "las", "un", "una", "y", "e", "o",
    "solo", "solamente", "unicamente", "ahora", "tambien", "sobre", "en",
    "que", "quiero", "ver", "muestra", "muestrame", "dame", "como", "ha",
    "analisis", "hoy", "actualmente", "aun", "todavia",
})
_CLEAR_PERIOD = re.compile(
    r"\bsin\s+(?:filtro\s+de\s+)?fecha\b|\bsin\s+(?:filtro\s+)?tempora\w*\b"
    r"|\bquita\s+(?:el\s+)?(?:filtro\s+de\s+)?fecha\b|\btodo\s+el\s+historico\b"
)


class _Ids:
    """Identificadores estables dentro del turno: mismo texto, mismos ids."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        self._counts[prefix] = self._counts.get(prefix, 0) + 1
        return f"{prefix}-{self._counts[prefix]}"


def collect_claims(
    user_query: str,
    params: dict,
    *,
    turn_id: str,
    buyer_resolution: Any = None,
    today: Date | None = None,
) -> ClaimSet:
    """Texto y autoridades -> ocurrencias vinculadas del turno."""
    normalized = normalize_with_offsets(user_query)
    masked = _mask_buyer(normalized, params)
    clauses = split_clauses(masked)
    clock = civil_today(today)
    next_id = _Ids()
    claims: list[SemanticClaim] = []
    issues: list[UnresolvedIssue] = []

    if not normalized.faithful:
        # Sin un mapa fiel al original no se publican tramos: señalar mal es
        # peor que no señalar.
        issues.append(UnresolvedIssue(
            "TEXT_OFFSETS_UNMAPPABLE", "la normalización no es reversible",
        ))
        return ClaimSet(turn_id, user_query, (), tuple(issues))

    buyer = _buyer_claim(normalized, params, buyer_resolution, turn_id, next_id)
    if buyer is not None:
        claims.append(buyer)

    sources = _source_claims(normalized, clauses, turn_id, next_id)
    claims.extend(sources.values())
    if _SOURCE_OR.search(masked):
        issues.append(UnresolvedIssue(
            "SOURCE_CHOICE_AMBIGUOUS",
            "la petición ofrece una alternativa entre fuentes",
        ))

    events, event_issues = _event_claims(
        normalized, clauses, sources, turn_id, next_id,
    )
    claims.extend(events.values())
    issues.extend(event_issues)

    period_claims, period_issues = _period_claims(
        normalized, clauses, sources, events, turn_id, next_id, clock,
    )
    claims.extend(period_claims)
    issues.extend(period_issues)

    state_claims = _state_claims(normalized, clauses, sources, turn_id, next_id)
    claims.extend(state_claims)
    legacy_states, legacy_issues = _legacy_state_claims(
        normalized, params, sources, state_claims, turn_id, next_id,
    )
    claims.extend(legacy_states)
    issues.extend(legacy_issues)

    whole = normalized.to_original((0, max(1, len(normalized.text)))) or (
        0, len(user_query)
    )
    material, material_issues = filter_claims(
        params, turn_id=turn_id, span=whole, next_id=next_id,
    )
    claims.extend(material)
    issues.extend(material_issues)
    claims.extend(_value_claims(params, sources, turn_id, whole, next_id))
    claims.extend(_intent_claims(masked, normalized, turn_id, whole, next_id))

    return ClaimSet(turn_id, user_query, tuple(claims), tuple(issues))


def _mask_buyer(normalized: NormalizedText, params: dict) -> str:
    """El nombre del comprador no elige fuente ni evento.

    Se enmascara por cadena, no por offset, y con la misma asimetría que
    ``source_request`` documentó: enmascarar de más pierde una elección y cae
    al default conservador; enmascarar de menos inventaría una elección que la
    persona no hizo. La máscara conserva la longitud, así que los offsets del
    texto normalizado siguen siendo válidos.
    """
    spans: list[tuple[int, int]] = []
    for surface in _buyer_surfaces(params):
        needle = normalize_with_offsets(surface).text
        start = normalized.text.find(needle)
        if needle and start >= 0:
            spans.append((start, start + len(needle)))
    return mask_consumed_spans(normalized.text, spans) if spans else normalized.text


def _buyer_claim(normalized, params, resolution, turn_id, next_id):
    scope = getattr(resolution, "scope", None)
    identity = getattr(resolution, "identity", None)
    if scope is None or identity is None:
        return None
    span = _buyer_span(normalized, params)
    return SemanticClaim(
        id=next_id("buyer"), turn_id=turn_id, span=span,
        dimension=ClaimDimension.BUYER,
        canonical_value=identity.display_name,
        authority=BUYER_AUTHORITY[0], authority_version=BUYER_AUTHORITY[1],
    )


def _buyer_span(normalized: NormalizedText, params: dict) -> tuple[int, int]:
    """El tramo que la autoridad de comprador reconoció, o el turno completo.

    El comprador no gobierna ocurrencias por posición, así que un tramo amplio
    no puede apropiarse de nada. Se prefiere la superficie exacta cuando existe.
    """
    for surface in _buyer_surfaces(params):
        needle = normalize_with_offsets(surface).text
        start = normalized.text.find(needle)
        if needle and start >= 0:
            mapped = normalized.to_original((start, start + len(needle)))
            if mapped is not None:
                return mapped
    return (0, len(normalized.original))


def _source_claims(
    normalized: NormalizedText, clauses: tuple[Clause, ...], turn_id, next_id,
) -> dict[int, SemanticClaim]:
    """La PRIMERA palabra de fuente de cada cláusula, indexada por cláusula.

    Sólo la cabeza: una segunda aparición vive dentro de un complemento del
    tema —"contratos de gestión de procesos industriales"— y no es una
    elección de fuente.
    """
    found: dict[int, SemanticClaim] = {}
    for index, clause in enumerate(clauses):
        head = _clause_head(clause.text)
        if head is None:
            continue
        span = normalized.to_original(
            (clause.start + head.start(), clause.start + head.end()),
        )
        if span is None:
            continue
        kind = (
            SourceKind.PROCESSES
            if _PROCESS_WORDS.fullmatch(head.group(0))
            else SourceKind.CONTRACTS
        )
        found[index] = SemanticClaim(
            id=next_id("source"), turn_id=turn_id, span=span,
            dimension=ClaimDimension.SOURCE, canonical_value=kind,
            authority=SOURCE_AUTHORITY[0], authority_version=SOURCE_AUTHORITY[1],
        )
    return found


def _clause_head(text: str) -> re.Match[str] | None:
    """La palabra de fuente que ENCABEZA la cláusula, si la encabeza.

    ``source_request`` leía la primera aparición de la cláusula, y eso basta
    para "procesos y contratos" pero no para "gestión de procesos
    industriales": ahí la primera aparición ya está dentro del complemento. La
    cabeza exige que todo lo anterior sea lenguaje de petición, no un
    sustantivo con su propio complemento.
    """
    for match in _ANY_SOURCE_WORD.finditer(text):
        prefix = text[:match.start()].split()
        if all(
            token in _PROFILE_NOISE or token in _HEAD_FILLERS for token in prefix
        ):
            return match
        return None
    return None


def _event_claims(normalized, clauses, sources, turn_id, next_id):
    """Todas las ocurrencias de hecho, cada una con el sujeto de su cláusula."""
    events: dict[int, SemanticClaim] = {}
    issues: list[UnresolvedIssue] = []
    single_source = _single_source(sources)
    for index, clause in enumerate(clauses):
        governed = bool(_CURRENT_MARKER.search(clause.text))
        for pattern, kind in (
            (_PUBLICATION, TemporalEventKind.PROCESS_PUBLICATION),
            (_SIGNATURE, TemporalEventKind.CONTRACT_SIGNATURE),
        ):
            for match in pattern.finditer(clause.text):
                if governed:
                    # "actualmente publicados" describe el estado de hoy, no un
                    # hecho fechado; su cláusula lo trata como estado.
                    continue
                span = normalized.to_original(
                    (clause.start + match.start(), clause.start + match.end()),
                )
                if span is None:
                    continue
                subject = sources.get(index) or single_source
                relations = (
                    (ClaimRelation(RelationKind.HAS_SUBJECT, subject.id),)
                    if subject is not None else ()
                )
                if subject is None:
                    issues.append(UnresolvedIssue(
                        "EVENT_SUBJECT_UNRESOLVED",
                        "el hecho no nombra la fuente que lo produce",
                        (match.group(0),),
                    ))
                events[index] = SemanticClaim(
                    id=next_id("event"), turn_id=turn_id, span=span,
                    dimension=ClaimDimension.EVENT, canonical_value=kind,
                    authority=EVENT_AUTHORITY[0],
                    authority_version=EVENT_AUTHORITY[1],
                    relations=relations,
                )
        for match in _UNSUPPORTED_EVENTS.finditer(clause.text):
            if governed:
                continue
            issues.append(UnresolvedIssue(
                "UNSUPPORTED_EVENT",
                "SECOP II no publica una fecha propia de ese hecho",
                (match.group(0),),
            ))
    return events, issues


def _period_claims(normalized, clauses, sources, events, turn_id, next_id, clock):
    """El periodo de cada cláusula, anclado a lo que esa cláusula nombró."""
    claims: list[SemanticClaim] = []
    issues: list[UnresolvedIssue] = []
    for index, clause in enumerate(clauses):
        resolution = resolve_temporal_scope(clause.text, today=clock)
        if not resolution.matched or resolution.span is None:
            continue
        span = normalized.to_original(
            (clause.start + resolution.span[0], clause.start + resolution.span[1]),
        )
        if span is None:
            continue
        anchor = events.get(index) or sources.get(index)
        if anchor is None or anchor.dimension is ClaimDimension.SOURCE:
            issue = _orphan_period_issue(clause, index, events)
            if issue is not None:
                issues.append(issue)
        claims.append(SemanticClaim(
            id=next_id("period"), turn_id=turn_id, span=span,
            dimension=ClaimDimension.PERIOD,
            canonical_value=Period(
                lower=resolution.fecha_desde, upper=resolution.fecha_hasta,
                civil_reference=clock.isoformat(), expression=resolution.phrase,
            ),
            authority=PERIOD_AUTHORITY[0], authority_version=PERIOD_AUTHORITY[1],
            relations=(
                (ClaimRelation(RelationKind.PERIOD_OF, anchor.id),)
                if anchor is not None else ()
            ),
        ))
    return claims, issues


def _orphan_period_issue(clause: Clause, index: int, events) -> UnresolvedIssue | None:
    """Un periodo junto a un hecho consumido por «actualmente» es ambiguo.

    "hoy los contratos terminados en agosto" no dice si agosto acota la
    terminación —que no tiene fecha publicada— o la firma. Elegir la firma
    sería contestar otra pregunta; elegir el estado dejaría agosto sin
    referente. Se pregunta.
    """
    if index in events:
        return None
    if not _CURRENT_MARKER.search(clause.text):
        return None
    lifecycle = _UNSUPPORTED_EVENTS.search(clause.text) or _PUBLICATION.search(
        clause.text,
    ) or _SIGNATURE.search(clause.text)
    if lifecycle is None:
        return None
    return UnresolvedIssue(
        "AMBIGUOUS_PERIOD_ANCHOR",
        "el periodo podría acotar el hecho nombrado o el hecho por defecto",
        (lifecycle.group(0),),
    )


def _state_claims(normalized, clauses, sources, turn_id, next_id):
    """El estado ACTUAL que cada cláusula pide, sobre el esquema de SU fuente."""
    claims: list[SemanticClaim] = []
    single_source = _single_source(sources)
    for index, clause in enumerate(clauses):
        source = sources.get(index) or single_source
        if source is None:
            continue
        family = detect_estado_family(clause.text, source.canonical_value.noun)
        if not family:
            continue
        token = str(family.get("consumed_span") or "")
        match = re.search(rf"\b{re.escape(token)}\b", clause.text) if token else None
        if match is None:
            continue
        span = normalized.to_original(
            (clause.start + match.start(), clause.start + match.end()),
        )
        if span is None:
            continue
        for field, values in (family.get("filters") or {}).items():
            claims.append(SemanticClaim(
                id=next_id("state"), turn_id=turn_id, span=span,
                dimension=ClaimDimension.CURRENT_STATE,
                canonical_value=StatePredicate(
                    str(field), tuple(str(v) for v in values),
                ),
                authority=STATE_AUTHORITY[0], authority_version=STATE_AUTHORITY[1],
                relations=(ClaimRelation(RelationKind.MODIFIES, source.id),),
            ))
    return claims


def _legacy_state_claims(
    normalized, params, sources, text_claims, turn_id, next_id,
):
    """Accredit an unambiguous material state that has no textual claim.

    Old callers can still supply ``estado``/``estado_field``.  Accepting those
    params and then dropping them widens the universe, while blindly copying
    them can attach a contracts field to processes.  The physical field is the
    ownership proof and every duplicate representation must agree.
    """
    raw_values = params.get("estado")
    if raw_values in (None, "", (), []):
        return [], []
    field = params.get("estado_field")
    values = _state_values(raw_values)
    owner_kinds = {
        claim.canonical_value
        for claim in sources.values()
        if isinstance(field, str) and field in _STATE_FIELDS[claim.canonical_value]
    }
    if not values or len(owner_kinds) != 1:
        return [], [UnresolvedIssue(
            "LEGACY_STATE_INVALID_CONTEXT",
            "el estado legacy no acredita un campo y una fuente únicos",
        )]
    owner_kind = next(iter(owner_kinds))
    physical = _state_values(params.get(field))
    if physical and physical != values:
        return [], [UnresolvedIssue(
            "LEGACY_STATE_INVALID_CONTEXT",
            "estado y filtro físico legacy se contradicen",
        )]
    existing = [
        claim for claim in text_claims
        if claim.canonical_value.field == field
    ]
    if existing:
        if any(claim.canonical_value.values != values for claim in existing):
            return [], [UnresolvedIssue(
                "LEGACY_STATE_INVALID_CONTEXT",
                "el estado material contradice la ocurrencia textual",
            )]
        return [], []
    owner = next(
        claim for claim in sources.values() if claim.canonical_value is owner_kind
    )
    return [SemanticClaim(
        id=next_id("state"), turn_id=turn_id,
        span=(0, len(normalized.original)),
        dimension=ClaimDimension.CURRENT_STATE,
        canonical_value=StatePredicate(str(field), values),
        authority=LEGACY_STATE_AUTHORITY[0],
        authority_version=LEGACY_STATE_AUTHORITY[1],
        relations=(ClaimRelation(RelationKind.MODIFIES, owner.id),),
    )], []


def _state_values(raw: object) -> tuple[str, ...]:
    if raw in (None, "", (), []):
        return ()
    values = raw if isinstance(raw, (list, tuple)) else (raw,)
    return tuple(dict.fromkeys(str(value) for value in values if value not in (None, "")))


def _value_claims(params, sources, turn_id, span, next_id):
    """Un límite monetario mide presupuesto en procesos y valor en contratos."""
    single = _single_source(sources)
    magnitude = (
        "presupuesto estimado"
        if single is not None and single.canonical_value is SourceKind.PROCESSES
        else "valor del contrato"
    )
    constraint = value_constraint(params, magnitude)
    if constraint.is_unbounded:
        return ()
    return (SemanticClaim(
        id=next_id("value"), turn_id=turn_id, span=span,
        dimension=ClaimDimension.VALUE, canonical_value=constraint,
        authority=VALUE_AUTHORITY[0], authority_version=VALUE_AUTHORITY[1],
    ),)


def _intent_claims(masked: str, normalized, turn_id, span, next_id):
    """Foco y operación: el vocabulario del perfil, no un parser nuevo."""
    claims: list[SemanticClaim] = []
    for focus, pattern in _FOCUS:
        if pattern.search(masked):
            claims.append(SemanticClaim(
                id=next_id("focus"), turn_id=turn_id, span=span,
                dimension=ClaimDimension.FOCUS, canonical_value=focus,
                authority=FOCUS_AUTHORITY[0], authority_version=FOCUS_AUTHORITY[1],
            ))
            break
    if _CLEAR_PERIOD.search(masked):
        claims.append(SemanticClaim(
            id=next_id("operation"), turn_id=turn_id, span=span,
            dimension=ClaimDimension.OPERATION, canonical_value="CLEAR_PERIOD",
            authority=OPERATION_AUTHORITY[0],
            authority_version=OPERATION_AUTHORITY[1],
        ))
    return claims


def _single_source(sources: dict[int, SemanticClaim]) -> SemanticClaim | None:
    """La única fuente del turno, cuando todas las cláusulas nombran la misma."""
    kinds = {claim.canonical_value for claim in sources.values()}
    if len(kinds) != 1:
        return None
    return next(iter(sources.values()))
