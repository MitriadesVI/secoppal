"""Compatible provenance for an already selected contractual state.

The physical filter owns execution. A family can be retained only when its
complete contractual projection equals that filter, never a wider set.
"""
from app.core.estado_families import ESTADO_FAMILIES, resolve_estado


def preserve_state_metadata(params: dict, explicit_state: str | None = None) -> dict:
    if params.get('dataset') not in ('contratos', 'jbjy-vk9h'):
        return params
    values = params.get('estado_contrato')
    if not values:
        return params
    values = list(values) if isinstance(values, (list, tuple)) else [values]
    family = params.get('estado_family')
    if family:
        projection = ESTADO_FAMILIES.get(family, {}).get('contratos', {})
        if projection != {'estado_contrato': values}:
            params.pop('estado_family', None)
    if explicit_state is not None:
        params['estado'] = explicit_state
        params['estado_field'] = 'estado_contrato'
        resolved = resolve_estado(explicit_state, 'contratos')
        if resolved:
            family = resolved['family']
            projection = ESTADO_FAMILIES[family].get('contratos', {})
            if projection == {'estado_contrato': values}:
                params['estado_family'] = family
    return params
