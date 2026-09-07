"""Independent counterexamples for SECOPPAL Frame V1.2-A.1.

Target reviewed: 9164df0606b0ba9424d627aa37b12e742a7cd707.
Copy this file into the TARGET worktree's tests/ directory and run only it.
This file does not contact SECOP, change flags, or modify production sources.

Review provenance:
- Mutation and source-ownership behavior reproduced in isolated local copies.
- These tests against the full target repository HAVE NOT been run in this review.
- The last migration test is a static-review hypothesis to adjudicate, not an
  independently executed full-migration finding.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy

import pytest

from app.core.entity_profile.immutability import deep_freeze
from app.core.source_occurrence import mask_source_heads, source_head_occurrences


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


@pytest.mark.parametrize("operation", [
    lambda value: dict.__setitem__(value, "month", "agosto"),
    lambda value: dict.update(value, {"month": "agosto"}),
    lambda value: value.__init__({"month": "agosto"}),
], ids=["base-setitem", "base-update", "inherited-init"])
def test_mapping_snapshot_cannot_be_mutated_through_inherited_api(operation):
    frozen = deep_freeze({"month": "julio"})
    before = deepcopy(_plain(frozen))
    try:
        operation(frozen)
    except (TypeError, AttributeError):
        pass
    assert _plain(frozen) == before


@pytest.mark.parametrize("operation", [
    lambda value: list.__setitem__(value, 0, "agosto"),
    lambda value: list.append(value, "agosto"),
    lambda value: value.__init__(["agosto"]),
], ids=["base-setitem", "base-append", "inherited-init"])
def test_nested_list_snapshot_cannot_be_mutated_through_inherited_api(operation):
    frozen = deep_freeze({"periods": ["julio"]})
    before = deepcopy(_plain(frozen))
    try:
        operation(frozen["periods"])
    except (TypeError, AttributeError):
        pass
    assert _plain(frozen) == before


@pytest.mark.parametrize("prefix", [
    "", "busca ", "buscar ", "cuales ", "por favor ",
])
def test_request_framing_does_not_erase_explicit_contract_source(prefix):
    text = prefix + "contratos de software"
    occurrences = source_head_occurrences(text)
    assert len(occurrences) == 1
    assert occurrences[0].surface == "contratos"
    assert occurrences[0].span == (len(prefix), len(prefix) + len("contratos"))


def test_coordinated_topic_does_not_become_a_second_source_header():
    text = "contratos de gestion de procesos y contratos"
    occurrences = source_head_occurrences(text)
    assert len(occurrences) == 1
    assert occurrences[0].span == (0, len("contratos"))
    assert " ".join(mask_source_heads(text).split()) == (
        "de gestion de procesos y contratos"
    )


def test_incompatible_physical_state_in_legacy_context_must_not_disappear():
    """Static-review hypothesis: verify against the complete target repository."""
    from app.core.entity_profile.frame_migration import (
        MigrationStatus, migrate_legacy_context,
    )

    legacy_request = {
        "focus": "overview",
        "sources": "CONTRACTS",
        "sources_provenance": "EXPLICIT",
        "sources_origin": "EXPLICIT",
        "time_from": "2026-08-01",
        "time_to": "2026-08-31",
        "event": "CONTRACT_SIGNATURE",
        "temporal_claims": [
            {"event": "CONTRACT_SIGNATURE", "span": [9, 17], "term": "firmados"},
        ],
        "unsupported_events": [],
        "keeps_current_state": True,
        "topic": [],
        "modality": [],
        "value_min": None,
        "value_max": None,
    }
    params = {
        "entidad_resolved": "ALCALDIA DE BARANOA",
        "fecha_desde": "2026-08-01",
        "fecha_hasta": "2026-08-31",
        "estado_del_procedimiento": ["Publicado"],
    }
    result = migrate_legacy_context(legacy_request, params)
    assert result.status is MigrationStatus.INVALID_CONTEXT
    assert result.frame is None
