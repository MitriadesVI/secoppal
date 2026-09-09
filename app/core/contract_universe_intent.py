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
    from app.core.entity_profile.clauses import split_clauses
    from app.core.source_occurrence import _is_request_framing, source_request_occurrences
    for pattern, state in _PATTERNS:
        for match in re.finditer(pattern, normalized.text):
            clause = next(c for c in split_clauses(normalized.text) if c.start <= match.start() <= c.end)
            prefix = normalized.text[clause.start:match.start()]
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
            # Multiple states require clarification until a complete typed union
            # can be transported by both adapters. Never execute a first match.
            conflict = len(states) > 1 or any(
                re.match(r"\s+(?:celebrados?|firmados?|suscritos?)\b", normalized.text[o.span[1]:])
                for o in source_request_occurrences(normalized.text) if not o.is_process)
            tail = normalized.text[match.end():]
            conflict |= bool(re.match(
                r'\s+y\s+(?:(?:registros?|contratos?)\s+)?(?:suspendidos?|prorrogados?|aprobados?|cerrados?|terminados?|celebrados?|firmados?)\b', tail))
            # The legacy record adapter cannot accredit a second source child.
            # Preserve the whole request as clarification, before either SQL job.
            conflict |= any(o.is_process for o in source_request_occurrences(normalized.text))
            return UniverseRequest(ContractUniverse.SOURCE_RECORDS, span,
                                   states[0] if len(states) == 1 else None, conflict)
    if re.search(r'\b(?:todos\s+los\s+contratos|contratos\s+celebrados)\b', normalized.text):
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
    return dataset != 'contratos' or intent is not None or not any(
        fold_state(value) in excluded for value in values)
