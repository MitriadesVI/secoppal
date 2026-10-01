"""M1-H0: a buyer alias that several entities claim is asked, never resolved to the last writer.

`EntityResolver.alias_index` keeps one owner per alias (last writer wins) and Tier 1 calls that
`method="exact"`.  The host must not accept it.  Legacy keeps using `alias_index` untouched.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import AMBIGUOUS_BUYER, admit_proposal
from app.core.foundation_messages import KNOWN_CODES, user_message
from app.core.foundation_producers import decode_proposal
from app.core.interpretation_contract import AcceptanceStatus
from app.utils.money import normalize_text

ALIASES = Path("app/data/aliases_db.json")
RESOLVER = EntityResolver(ALIASES)
MULTI = sorted(alias for alias, owners in RESOLVER.owners_by_alias.items() if len(owners) > 1)
SINGLE_EXACT = sorted(
    alias for alias, owners in RESOLVER.owners_by_alias.items()
    if len(owners) == 1 and RESOLVER.resolve_entidad(alias).method == "exact")


def admit(alias, *, resolver=RESOLVER, tail=" contratos"):
    """«<alias> contratos»: the buyer cites the alias; the universe cites the noun after it."""
    raw = f"{alias}{tail}"
    universe_at = raw.rindex("contratos")
    payload = {
        "requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                      "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                      "universe_evidence": {"start": universe_at, "end": universe_at + 9, "quote": "contratos"},
                      "condition_expression": {"operator": "ATOM", "condition_id": "c0"}}],
        "conditions": [{"condition_id": "c0", "request_ids": ["r1"], "dimension": "BUYER", "operator": "is",
                        "polarity": "POSITIVE", "evidence": [{"start": 0, "end": len(alias), "quote": alias}]}],
    }
    return admit_proposal(decode_proposal(payload, raw, arm="C"), raw, entity_resolver=resolver)


# ── the index records ownership without changing what legacy reads ───────────────────────────────

def test_there_are_multi_owner_aliases_and_the_resolver_calls_them_exact():
    # The premise of the defect: this is what the host used to accept.
    assert len(MULTI) >= 100
    exact = [a for a in MULTI if RESOLVER.resolve_entidad(a).method == "exact"]
    assert len(exact) >= 100 and {"icbf", "invias", "sena"} <= set(MULTI)


def test_alias_owners_is_pure_and_empty_for_unknown_aliases():
    assert RESOLVER.alias_owners("zzz no existe") == frozenset()
    owners = RESOLVER.alias_owners("icbf")
    assert isinstance(owners, frozenset) and len(owners) > 1
    assert RESOLVER.alias_owners("icbf") == owners


def test_alias_index_keeps_the_legacy_last_writer_value():
    """Recompute the legacy last-writer index from the data file: byte-for-byte the same mapping."""
    last_writer = {}
    for record in json.loads(ALIASES.read_text(encoding="utf-8")):
        for alias in record.get("aliases", []):
            key = normalize_text(alias)
            if key:
                last_writer[key] = record["official_name"]
    assert RESOLVER.alias_index == last_writer
    assert RESOLVER.resolve_entidad("icbf").value == last_writer["icbf"]  # legacy still resolves it (out of scope)


# ── every multi-owner alias is asked by the host ─────────────────────────────────────────────────

@pytest.mark.parametrize("alias", MULTI)
def test_no_multi_owner_alias_is_ever_ready_in_the_host(alias):
    accepted = admit(alias)
    assert accepted.status is not AcceptanceStatus.READY
    if RESOLVER.resolve_entidad(alias).method == "exact":
        assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
        assert accepted.reason == AMBIGUOUS_BUYER == "AMBIGUOUS_HOST_DIMENSION:BUYER"


def test_the_contract_example_icbf_is_asked_instead_of_tolima():
    accepted = admit("ICBF", tail=" contratos en 2025")
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == AMBIGUOUS_BUYER


def test_ownership_decides_not_the_spelling():
    for alias in ("ICBF", "Icbf", "  icbf", "InViAs"):
        assert admit(alias).reason == AMBIGUOUS_BUYER


# ── an alias with one owner keeps resolving ──────────────────────────────────────────────────────

@pytest.mark.parametrize("alias", ["alcaldia de barranquilla", "gobernacion del huila"])
def test_the_named_single_owner_aliases_still_resolve(alias):
    (owner,) = RESOLVER.alias_owners(alias)
    accepted = admit(alias)
    assert accepted.status is AcceptanceStatus.READY
    assert [r.resolved_ids for r in accepted.resolutions if r.target_id == "c0"] == [(owner,)]


def test_a_sample_of_single_owner_aliases_still_resolves_to_their_owner():
    sample = SINGLE_EXACT[::25]
    assert len(sample) >= 100
    for alias in sample:
        accepted = admit(alias)
        (owner,) = RESOLVER.alias_owners(alias)
        if accepted.status is AcceptanceStatus.READY:
            assert [r.resolved_ids for r in accepted.resolutions if r.target_id == "c0"] == [(owner,)]
        else:  # whatever other guard stops it, it must not be the new ambiguity code
            assert accepted.reason != AMBIGUOUS_BUYER, alias
    ready = sum(admit(alias).status is AcceptanceStatus.READY for alias in sample)
    assert ready >= len(sample) * 0.9


# ── what the user reads and the no-source-call guarantee ─────────────────────────────────────────

def test_the_code_is_registered_and_has_its_own_message():
    assert AMBIGUOUS_BUYER in KNOWN_CODES
    message = user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER)
    assert "más de una entidad" in message.text and AMBIGUOUS_BUYER not in message.text


def test_an_ambiguous_buyer_makes_no_source_call(tmp_path, monkeypatch):
    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
    from app.core.soql_builder import SoQLBuilder
    from tests.product_contract.harness import ProductHarness

    raw = "contratos del ICBF"
    now = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
    payload = {
        "requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                      "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                      "universe_evidence": {"start": 0, "end": 9, "quote": "contratos"},
                      "condition_expression": {"operator": "ATOM", "condition_id": "c0"}}],
        "conditions": [{"condition_id": "c0", "request_ids": ["r1"], "dimension": "BUYER", "operator": "is",
                        "polarity": "POSITIVE", "evidence": [{"start": 14, "end": 18, "quote": "ICBF"}]}],
    }
    produced = decode_proposal(payload, raw, arm="C")
    rows = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS[dataset]})]
            for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows, reference_instant=now, today=now.date(),
                             foundation_producer=lambda _: produced,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(raw)
    assert turn.result["foundation_terminal"] == "NEEDS_CLARIFICATION"
    assert turn.result["foundation_internal_reason"] == AMBIGUOUS_BUYER
    assert harness.transport.snapshot() == []
