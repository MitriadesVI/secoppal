"""Pure display derivation for the resolved SECOP II contract parameters.

Precedences mirror _build_where; this module never builds or modifies queries.
Registry objects, when needed, must be supplied by scope_context, not loaded here.
"""
from __future__ import annotations

from app.core.actor_profile.scope_models import ActorProfileAppliedScope
from app.core.buyer_groups import BuyerGroup
from app.core.contract_type_projection import _EXACT_TYPES
from app.core.modality_dataset_projection import project_modality_filter
from app.core.modality_filter import ModalityFilter
from app.core.modality_models import ModalityCatalog
from app.core.phrase_catalog import spec_for_canonical

_CONTRACT_LABELS = {
    **_EXACT_TYPES,
    'arrendamiento': 'Arrendamiento',
    'convenio': 'Convenio',
    'convenio_interadministrativo': 'Convenio interadministrativo',
    'convenio_asociacion': 'Convenio de asociación',
    'convenio_solidario': 'Convenio solidario',
    'contrato_interadministrativo': 'Contrato interadministrativo',
}
_STATE_FIELDS = (
    'estado_de_apertura_del_proceso', 'estado_del_procedimiento',
    'estado_contrato', 'estado_contrato_adicional',
)


def _labels(value) -> tuple[str, ...]:
    if value is None or value == '':
        return ()
    values = value if isinstance(value, (list, tuple)) else [value]
    return tuple(dict.fromkeys(str(v) for v in values if v not in (None, '')))


def _buyer(params: dict, group: BuyerGroup | None) -> dict:
    result: dict = {}
    like = params.get('entidad_like')
    resolved = params.get('entidad_resolved')
    if like or resolved:
        result.update(buyer_label=str(like or resolved), buyer_scope_kind='approximate' if like else 'exact')
    elif params.get('buyer_group'):
        if group is None or params.get('buyer_group') != group.reference():
            raise ValueError('display group must match the executed reference')
        result.update(buyer_label=group.label, buyer_scope_kind='group')
    matches = []
    if not (like or resolved):
        for key in ('entidad_ecosystem', 'entidad_in_candidates'):
            if isinstance(params.get(key), list) and params[key]:
                matches.append('Alguna de: ' + ' o '.join(_labels(params[key])))
    if isinstance(params.get('entidad_contains_all'), list) and params['entidad_contains_all']:
        matches.append('Todas las coincidencias: ' + ', '.join(_labels(params['entidad_contains_all'])))
    return result | {'buyer_match_labels': tuple(matches)}


def _geography(params: dict) -> tuple[str, ...]:
    labels = []
    if params.get('departamento_resolved'):
        labels.append('Departamento: ' + str(params['departamento_resolved']))
    elif params.get('departamentos_in'):
        labels.append('Departamentos: ' + ' o '.join(_labels(params['departamentos_in'])))
    if params.get('ciudad'):
        exact = (params.get('municipal_territory') or {}).get('status') == 'resolved'
        labels.append(('Ciudad: ' if exact else 'Ciudad por coincidencia: ') + str(params['ciudad']))
    return tuple(labels)


def _states(params: dict) -> tuple[str, ...]:
    fields = [key for key in _STATE_FIELDS if params.get(key)]
    if not fields and params.get('estado'):
        fields = ['estado']
    # Each family contributes an AND predicate; values within a family are OR.
    return tuple(' o '.join(_labels(params[key])) for key in fields)


def _modalities(params: dict, catalog: ModalityCatalog | None) -> dict:
    filter_ = ModalityFilter.from_params(params)
    if filter_.is_empty():
        return {}
    if catalog is None:
        raise ValueError('modality display requires the existing execution catalog')
    projection = project_modality_filter(filter_, 'contratos', catalog=catalog)
    includes = []
    excludes = []
    for ids, labels in (
        (filter_.include_procedure_ids, includes),
        (filter_.exclude_procedure_ids, excludes),
    ):
        for procedure_id in ids:
            procedure = catalog.by_id(procedure_id)
            if procedure and procedure.applies_to('contratos'):
                labels.append(procedure.display_name)
    if projection.include_unsatisfiable:
        # The effective predicate is 1=0, including no exclusions.
        includes = [
            catalog.by_id(pid).display_name + ' (sin valores compatibles en SECOP II contratos)'
            for pid in filter_.include_procedure_ids
        ]
        excludes = []
    return {'modality_labels': tuple(includes), 'excluded_modality_labels': tuple(excludes)}


def _topics(params: dict) -> tuple[str, ...]:
    labels = []
    for term in params.get('objeto') or []:
        labels.append(' o '.join(_labels(term)))
    if params.get('objeto_or'):
        labels.append(' o '.join(_labels(params['objeto_or'])))
    for canonical in params.get('objeto_phrases') or []:
        spec = spec_for_canonical(str(canonical))
        if spec and spec.ordered_anchors:
            labels.append(spec.canonical)
    return tuple(dict.fromkeys(label for label in labels if label))


def build_actor_profile_scope(
    params: dict, *, buyer_group: BuyerGroup | None = None,
    modality_catalog: ModalityCatalog | None = None,
) -> ActorProfileAppliedScope:
    """Project only display semantics; no I/O, identity lookup, or mutation."""
    contract_ids = tuple(dict.fromkeys(params.get('contract_type_include') or []))
    if any(cid not in _CONTRACT_LABELS for cid in contract_ids):
        raise ValueError('unknown contract type in applied scope')
    return ActorProfileAppliedScope(
        **_buyer(params, buyer_group),
        geography_labels=_geography(params),
        date_from=params.get('fecha_desde') or None,
        date_to=params.get('fecha_hasta') or None,
        value_min=int(params['valor_min']) if params.get('valor_min') is not None else None,
        value_max=int(params['valor_max']) if params.get('valor_max') is not None else None,
        state_labels=_states(params),
        **_modalities(params, modality_catalog),
        contract_type_labels=tuple(_CONTRACT_LABELS[cid] for cid in contract_ids),
        topic_labels=_topics(params),
        excluded_topic_labels=_labels(params.get('objeto_exclude')),
    )
