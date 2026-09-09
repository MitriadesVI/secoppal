"""Bounded explicit record requests. Negation never widens the universe."""
import re
from dataclasses import dataclass
from app.core.contract_universe import ContractUniverse, POLICY_VERSION, conflict_message, stamp

_STATE = r'(?:cancelad[oa]s?|(?:en\s+)?borrador(?:es)?|enviados?\s+(?:al\s+)?proveedor|en\s+aprobacion)'
_PATTERNS = (
    (r'\btodos\s+los\s+registros\s+(?:del\s+dataset|de\s+la\s+fuente)\b', None),
    (rf'\b(?:registros?|contratos?)\s+(?P<states>{_STATE}(?:\s+y\s+(?:(?:registros?|contratos?)\s+)?{_STATE})*)\b', 'states'),
    (rf'\b(?P<states>{_STATE})\s+(?=de\s+(?:la\s+)?\w+\b)', 'states'),
)


def _state(text):
    if 'cancelad' in text:
        return 'Cancelado'
    if 'borrador' in text:
        return 'Borrador'
    if 'enviad' in text:
        return 'enviado Proveedor'
    return 'En aprobación'


@dataclass(frozen=True)
class UniverseRequest:
    universe: ContractUniverse
    span: tuple[int, int]
    state: str | None = None
    conflict: bool = False


def request(text: str, *, protected_surfaces=()) -> UniverseRequest | None:
    from app.core.entity_profile.text_offsets import normalize_with_offsets
    normalized = normalize_with_offsets(text)
    protected = []
    for surface in protected_surfaces:
        needle = normalize_with_offsets(surface).text
        protected.extend((m.start(), m.end()) for m in re.finditer(re.escape(needle), normalized.text) if needle)
    from app.core.contract_conditions import read_conditions
    from app.core.entity_profile.clauses import split_clauses
    from app.core.source_occurrence import _is_request_framing, source_request_occurrences
    occurrences = source_request_occurrences(normalized.text)
    # Every condition the request states, read before any of them is selected.
    composition = read_conditions(normalized.text, occurrences=occurrences,
                                  protected=tuple(protected))
    for occurrence in occurrences:
        if occurrence.is_process or any(a < occurrence.span[1] and occurrence.span[0] < b for a, b in protected):
            continue
        contradictory = re.match(rf'\s+(?:celebrados?|firmados?|suscritos?)\s+{_STATE}\b',
                                 normalized.text[occurrence.span[1]:])
        if contradictory:
            span = normalized.to_original((occurrence.span[0], occurrence.span[1] + contradictory.end()))
            if span is not None:
                return UniverseRequest(ContractUniverse.CELEBRATED, span, conflict=True)
    for pattern, state in _PATTERNS:
        for match in re.finditer(pattern, normalized.text):
            clause = next(c for c in split_clauses(normalized.text) if c.start <= match.start() <= c.end)
            prefix = normalized.text[clause.start:match.start()]
            if any(clause.start <= o.span[0] < match.start() for o in occurrences):
                continue  # A prior source head owns its following object complement.
            if any(a < match.end() and match.start() < b for a, b in protected):
                continue
            if re.search(r'\b(?:ha contratado|proveedor|contratista)\s*$', prefix):
                continue
            if not _is_request_framing(prefix):
                continue
            span = normalized.to_original(match.span())
            if span is None:
                continue
            states = tuple(_state(t) for t in re.split(r'\s+y\s+', match.group('states'))) if state else ()
            # The adapter transports one state inside one universe. A second
            # condition anywhere in the request — before the match, after it or
            # in another coordinated clause — is clarified, never dropped, and
            # the answer cannot depend on which of the two was written first.
            conflict = len(states) > 1 or not composition.representable
            # The legacy record adapter cannot accredit a second source child.
            # Preserve the whole request as clarification, before either SQL job.
            conflict |= any(o.is_process for o in occurrences)
            if conflict and composition.span is not None:
                whole = normalized.to_original((min(match.start(), composition.span[0]),
                                                max(match.end(), composition.span[1])))
                span = whole if whole is not None else span
            return UniverseRequest(ContractUniverse.SOURCE_RECORDS, span,
                                   states[0] if len(states) == 1 else None, conflict)
    if not composition.representable and composition.span is not None:
        # No explicit-record pattern owns this request, but it still states an
        # incompatible composition: clarify rather than widen or execute half.
        span = normalized.to_original(composition.span)
        if span is not None:
            return UniverseRequest(ContractUniverse.CELEBRATED, span, conflict=True)
    for occurrence in occurrences:
        if occurrence.is_process or any(a < occurrence.span[1] and occurrence.span[0] < b for a, b in protected):
            continue
        clause = split_clauses(normalized.text)[occurrence.clause_index]
        prefix = normalized.text[clause.start:occurrence.span[0]]
        if re.search(r'\b(?:ha contratado|proveedor|contratista)\b', prefix):
            continue
        if (re.search(r'\btodos\s+los\s+$', prefix)
                or re.match(r'\s+celebrados?\b', normalized.text[occurrence.span[1]:])):
            return UniverseRequest(ContractUniverse.CELEBRATED, (0, 0))
    return None


def apply_request(params: dict, intent: UniverseRequest | None) -> dict:
    clean = dict(params)
    if (intent is None and clean.get('contract_universe') == 'source_records'
            and 'contract_universe_version' not in clean and clean.get('intent_type') != 'direct_lookup'):
        clean['contract_universe'] = 'celebrated'
        clean['contract_universe_error'] = 'El universo de registros sin versión requiere revisión.'
    if intent:
        clean['dataset'] = 'contratos'
        clean['dataset_explicit'] = True
        for key in ('intent_type', 'opportunity_mode', 'filter_active_only'):
            clean.pop(key, None)
        clean['contract_universe'] = intent.universe.value
        clean['contract_universe_version'] = POLICY_VERSION
        if intent.conflict:
            clean['contract_universe_error'] = (
                'No puedo representar esta composición completa de fuentes o estados. '
                'Aclara qué fuente y estado quieres consultar antes de ejecutar.'
            )
        if intent.state:
            for key in ('estado', 'estado_field', 'estado_family', 'estado_contrato_adicional',
                        'estado_de_apertura_del_proceso', 'estado_del_procedimiento'):
                clean.pop(key, None)
            clean['estado_contrato'] = [intent.state]
        elif intent.universe is ContractUniverse.SOURCE_RECORDS:
            clean.pop('estado_contrato', None)
    clean = stamp(clean)
    error = conflict_message(clean)
    if error:
        clean['contract_universe_error'] = error
    return clean


def state_authorized(dataset, values, intent):
    """Lexical extraction cannot claim excluded contractual state occurrences."""
    from app.core.contract_universe import EXCLUDED_STATES, fold_state
    values = values if isinstance(values, (list, tuple)) else (values,)
    excluded = {fold_state(s) for s in EXCLUDED_STATES}
    selected = {fold_state(value) for value in values} & excluded
    return (dataset != 'contratos' or not selected or bool(
        intent and intent.state and selected == {fold_state(intent.state)}))
