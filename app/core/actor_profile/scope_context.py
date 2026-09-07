"""Supply the same cached registries already used by the eight SQL builds."""
from __future__ import annotations

import logging

from app.core.actor_profile.scope import build_actor_profile_scope
from app.core.actor_profile.scope_models import ActorProfileAppliedScope
from app.core.buyer_groups import group_for_params
from app.core.modality_dataset_projection import _default_catalog
from app.core.modality_filter import ModalityFilter

logger = logging.getLogger(__name__)


def scope_from_resolved_params(params: dict) -> ActorProfileAppliedScope:
    """Called after query execution; adds no actor/entity resolution or DB read."""
    try:
        group = group_for_params(params)
        modalities = ModalityFilter.from_params(params)
        catalog = None if modalities.is_empty() else _default_catalog()
        return build_actor_profile_scope(params, buyer_group=group, modality_catalog=catalog)
    except ValueError as exc:
        # Invalid executable params already failed the SQL builds. Preserve
        # their Outcome.ERROR; presentation must not trigger a tabular retry.
        logger.warning('actor scope display rejected invalid query parameters: %s', exc)
        return ActorProfileAppliedScope()
