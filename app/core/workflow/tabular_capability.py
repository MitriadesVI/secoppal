"""Bind the published capability to the actual buffer consumed by pagination."""
from dataclasses import dataclass
import hashlib
import json

from app.core.pagination_buffer import PaginationBuffer, PAGINATION_CONTRACT_VERSION
from app.core.workflow.continuation import TabularGroup


def cursor_id(buffer):
    return hashlib.sha256(json.dumps(buffer.to_dict(), sort_keys=True,
                                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def capability(buffer, generation):
    return TabularGroup(buffer.group, cursor_id(buffer), buffer.query_fingerprint,
                        PAGINATION_CONTRACT_VERSION, generation, buffer.expires_at_utc)


def published_groups(store, reservation):
    payload = store._read_pagination_payload(store._pagination_path_for(reservation.chat_id))
    groups = {}
    for name, raw in payload.items():
        buffer = PaginationBuffer.from_dict(raw)
        if name != buffer.group:
            raise ValueError('buffer stored under foreign group')
        groups[name] = capability(buffer, reservation.result_id)
    return groups


@dataclass(frozen=True)
class TabularReferent:
    params: dict
    buffer: PaginationBuffer | None = None


def validate_tabular(store, reservation, contract, query):
    from app.core.workflow.handlers.pagination import _requested_group

    requested = _requested_group(query)
    groups = contract.tabular_groups or {}
    if requested and requested not in groups:
        raise ValueError('requested group has no published cursor')
    if not groups:
        raise ValueError('current result has no cursor')
    if len(groups) > 1 and requested is None:
        return None
    name = requested or next(iter(groups))
    raw = store._read_pagination_payload(store._pagination_path_for(reservation.chat_id)).get(name)
    if not isinstance(raw, dict):
        raise ValueError('published cursor is absent')
    buffer = PaginationBuffer.from_dict(raw)
    generation = reservation.previous.envelope.continuity_generation
    if groups[name] != capability(buffer, generation) or buffer.is_expired():
        raise ValueError('published cursor is incompatible or expired')
    return TabularReferent(dict(buffer.query_params), buffer)


def export_params(store, contract, chat):
    if contract.owner.value != 'TABULAR' or not contract.export_ref:
        return None
    history = store.get_history(chat, last_n=1)
    if len(history) != 1 or history[0].turn_id != contract.export_ref:
        return None
    return dict(history[0].parsed_params)
