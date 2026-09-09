"""Bounded explicit record requests. Negation never widens the universe."""
import re
from dataclasses import dataclass
from app.core.contract_universe import ContractUniverse, EXCLUDED_STATES, fold_state, stamp

_PATTERNS = (
    (r'\btodos\s+los\s+registros\s+(?:del\s+dataset|de\s+la\s+fuente)\b', None),
    (r'\b(?:registros?\s+)?cancelad[oa]s?\b', 'Cancelado'),
    (r'\b(?:registros?\s+)?(?:en\s+)?borrador(?:es)?\b', 'Borrador'),
    (r'\b(?:registros?\s+)?enviados?\s+(?:al\s+)?proveedor\b', 'enviado Proveedor'),
    (r'\b(?:registros?\s+)?en\s+aprobacion\b', 'En aprobación'),
)


@dataclass(frozen=True)
class UniverseRequest:
    universe: ContractUniverse
    span: tuple[int, int]
    state: str | None = None
    conflict: bool = False


def request(text: str) -> UniverseRequest | None:
    from app.core.entity_profile.text_offsets import normalize_with_offsets
    normalized = normalize_with_offsets(text)
    if re.search(r"\b(?:procesos?|licitaciones?|convocatorias?|oportunidades?)\b", normalized.text):
        return None  # This bounded adapter never reassigns an explicit process subject.
    for pattern, state in _PATTERNS:
        for match in re.finditer(pattern, normalized.text):
            prefix = normalized.text[:match.start()]
            if re.search(r'\b(?:sin|excepto|excluyendo|no)\s+(?:los?\s+)?$', prefix):
                continue
            span = normalized.to_original(match.span())
            if span is None:
                continue
            conflict = bool(state and re.search(r'\b(?:firmados?|celebrados?|suscritos?)\b', normalized.text))
            return UniverseRequest(ContractUniverse.SOURCE_RECORDS, span, state, conflict)
    if re.search(r'\b(?:todos\s+los\s+contratos|contratos\s+celebrados)\b', normalized.text):
        return UniverseRequest(ContractUniverse.CELEBRATED, (0, 0))
    return None


def apply_request(params: dict, intent: UniverseRequest | None) -> dict:
    clean = dict(params)
    if intent:
        clean['dataset'] = 'contratos'
        clean['dataset_explicit'] = True
        for key in ('intent_type', 'opportunity_mode', 'filter_active_only'):
            clean.pop(key, None)
        clean['contract_universe'] = intent.universe.value
        if intent.conflict:
            clean['contract_universe_error'] = (
                'La petición combina contratos celebrados con registros preparatorios o cancelados. '
                'Aclara qué universo quieres consultar.'
            )
        if intent.state:
            for key in ('estado', 'estado_field', 'estado_family', 'estado_contrato_adicional',
                        'estado_de_apertura_del_proceso', 'estado_del_procedimiento'):
                clean.pop(key, None)
            clean['estado_contrato'] = [intent.state]
        elif intent.universe is ContractUniverse.SOURCE_RECORDS:
            clean.pop('estado_contrato', None)
    # Preserve access for existing, explicitly materialized state callers.
    elif 'contract_universe' not in clean and clean.get('dataset') == 'contratos':
        states = clean.get('estado_contrato') or clean.get('estado') or []
        states = states if isinstance(states, (list, tuple)) else [states]
        if any(fold_state(s) in {fold_state(v) for v in EXCLUDED_STATES} for s in states):
            clean['contract_universe'] = ContractUniverse.SOURCE_RECORDS.value
    return stamp(clean)
