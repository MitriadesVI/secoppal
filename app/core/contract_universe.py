"""Contract Universe V1: operational inclusion, independent of identity and dates.

The allowlist is adjudicated in docs/audits/contract_universe_v1/. A date
cannot rescue an excluded/unknown state; an admitted historical state needs
no invented date. Native SECOP labels are not Socrata state literals.
"""
from enum import Enum
import unicodedata

POLICY_VERSION = 'contract-universe-1'


class ContractUniverse(str, Enum):
    CELEBRATED = 'celebrated'
    SOURCE_RECORDS = 'source_records'


# Aprobado is admitted operationally from the versioned catalog and bounded
# public evidence. This does not assert universal legal perfection or a date.
CELEBRATED_STATES = (
    'En ejecución', 'Cerrado', 'Modificado', 'terminado',
    'Aprobado', 'cedido', 'Suspendido', 'Prorrogado',
)
EXCLUDED_STATES = ('Cancelado', 'Borrador', 'enviado Proveedor', 'En aprobación')


def fold_state(value: object) -> str:
    if not isinstance(value, str):
        return ''
    return ''.join(c for c in unicodedata.normalize('NFD', value.strip(' ').upper())
                   if not unicodedata.combining(c))


def eligible(row: dict, universe=ContractUniverse.CELEBRATED) -> bool:
    universe = ContractUniverse(universe)
    return (universe is ContractUniverse.SOURCE_RECORDS
            or fold_state(row.get('estado_contrato')) in {fold_state(s) for s in CELEBRATED_STATES})


def universe_from_params(params: dict) -> ContractUniverse:
    if params.get("intent_type") == "direct_lookup":
        return ContractUniverse.SOURCE_RECORDS
    version = params.get('contract_universe_version', POLICY_VERSION)
    if version != POLICY_VERSION:
        raise ValueError('El universo contractual guardado requiere revisión antes de continuar.')
    return ContractUniverse(params.get('contract_universe', ContractUniverse.CELEBRATED))


def stamp(params: dict) -> dict:
    """Resolve before compilation/publication; never mutate a caller's filters."""
    clean = dict(params)
    if clean.get('dataset') in ('contratos', 'jbjy-vk9h'):
        clean['contract_universe'] = universe_from_params(clean).value
        clean['contract_universe_version'] = POLICY_VERSION
    return clean


def predicate(universe=ContractUniverse.CELEBRATED) -> str:
    if ContractUniverse(universe) is ContractUniverse.SOURCE_RECORDS:
        return ''
    values = ', '.join("'" + fold_state(state) + "'" for state in CELEBRATED_STATES)
    return f"upper(unaccent(trim(estado_contrato))) IN ({values})"


def label(universe=ContractUniverse.CELEBRATED) -> str:
    if ContractUniverse(universe) is ContractUniverse.SOURCE_RECORDS:
        return 'Registros de la fuente solicitados explícitamente.'
    return 'Contratos celebrados según las señales publicadas en SECOP.'


def compatible(params: dict) -> bool:
    if params.get('dataset') not in ('contratos', 'jbjy-vk9h'):
        return True
    return (params.get('contract_universe_version') == POLICY_VERSION
            and params.get('contract_universe') in {u.value for u in ContractUniverse})


def conflict_message(params: dict) -> str | None:
    """An explicit state cannot silently become an empty intersection on reload."""
    if params.get('dataset') not in ('contratos', 'jbjy-vk9h'):
        return None
    if universe_from_params(params) is ContractUniverse.SOURCE_RECORDS:
        return None
    states = params.get('estado_contrato') or params.get('estado') or ()
    states = states if isinstance(states, (list, tuple)) else (states,)
    if any(fold_state(s) in {fold_state(v) for v in EXCLUDED_STATES} for s in states):
        return ('El estado solicitado queda fuera del universo de contratos celebrados. '
                'Aclara si quieres conservar ese filtro de registros o consultar contratos celebrados.')
    return None


def record_terms(response: str, universe) -> str:
    """Render record metrics as records; retain official dataset source labels."""
    if ContractUniverse(universe) is ContractUniverse.CELEBRATED:
        return response
    import re
    return re.sub(r"\bcontratos\b", "registros", response).replace("Contrato reciente:", "Registro reciente:")
