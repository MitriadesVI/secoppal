"""Compact, channel-neutral presentation of the applied query scope."""
from __future__ import annotations

from app.core.actor_profile.scope_models import ActorProfileAppliedScope
from app.core.analytics.base import format_currency


def _period(scope: ActorProfileAppliedScope) -> str:
    start, end = scope.date_from, scope.date_to
    if start and end:
        if start.endswith('-01-01') and end == start[:4] + '-12-31':
            return start[:4]
        return f'{start} a {end}'
    return f'Desde {start}' if start else f'Hasta {end}'


def render_actor_profile_scope(scope: ActorProfileAppliedScope) -> str:
    if scope.is_empty():
        return ''
    lines = []
    if scope.buyer_label:
        title = {
            'exact': 'Entidad', 'group': 'Grupo de entidades',
            'approximate': 'Entidad filtrada por coincidencia',
        }[scope.buyer_scope_kind]
        lines.append(f'{title}: {scope.buyer_label}')
    lines.extend('Entidad por coincidencia — ' + label for label in scope.buyer_match_labels)
    if scope.geography_labels:
        lines.append(' · '.join(scope.geography_labels))
    temporal_value = []
    if scope.date_from or scope.date_to:
        temporal_value.append('Periodo: ' + _period(scope))
    bounds = []
    for label, value in (('desde', scope.value_min), ('hasta', scope.value_max)):
        if value is not None:
            bounds.append(f'{label} ${format_currency(value)}')
    if bounds:
        temporal_value.append('Valor contractual: ' + ' '.join(bounds))
    if temporal_value:
        lines.append(' · '.join(temporal_value))
    classifications = []
    for label, values in (
        ('Estado', scope.state_labels), ('Modalidad', scope.modality_labels),
        ('Excluir modalidades', scope.excluded_modality_labels),
        ('Tipo contractual', scope.contract_type_labels),
    ):
        if values:
            classifications.append(label + ': ' + '; '.join(values))
    if classifications:
        lines.append(' · '.join(classifications))
    for label, values in (
        ('Objeto (búsqueda ampliada)', scope.topic_labels),
        ('Excluir del objeto', scope.excluded_topic_labels),
    ):
        if values:
            lines.append(label + ': ' + '; '.join(values))
    return '*Filtros aplicados*\n' + '\n'.join('- ' + line for line in lines)
