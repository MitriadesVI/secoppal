"""M1-H0 R1: the clarification of an ambiguous buyer names owners the user can type, and typing one works.

ZEUS: «ICBF» asked for «el nombre completo», and «Instituto Colombiano de Bienestar Familiar» was the same
question again (a loop).  The message now lists the owners of the alias (headquarters first, at most 6, «y N más»),
and an owner is only offered when the host itself accepts its official name as one entity.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from app.core.foundation_admission import AMBIGUOUS_BUYER
from app.core.foundation_buyer_choices import MAX_SHOWN, buyer_choices, typable
from app.core.foundation_messages import user_message
from app.core.foundation_producers import decode_proposal
from app.core.interpretation_contract import AcceptanceStatus
from tests.test_foundation_h0_buyer_ambiguity import MULTI, RESOLVER, admit


def _proposal_for(alias):
    accepted = admit(alias)
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    return accepted.proposal


def _owner_resolution(accepted):
    return [r.resolved_ids for r in accepted.resolutions if r.target_id == "c0"]


# ── the candidates are the owners, ordered, capped, and each one is typable ──────────────────────────

def test_icbf_names_the_national_seat_first_and_counts_the_rest():
    found = buyer_choices(_proposal_for("ICBF"), RESOLVER)
    assert found.shown[0] == "ICBF SEDE NACIONAL"
    assert len(found.shown) == MAX_SHOWN == 6
    assert found.hidden == len(RESOLVER.alias_owners("icbf")) - MAX_SHOWN == 6
    assert list(found.shown[1:]) == sorted(found.shown[1:], key=str.casefold)
    message = user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER, _proposal_for("ICBF"), entity_resolver=RESOLVER)
    assert "«ICBF SEDE NACIONAL»" in message.text and "y 6 más" in message.text
    assert message.choices == found.shown and AMBIGUOUS_BUYER not in message.text


def test_an_official_name_that_is_itself_shared_or_unprojectable_is_never_offered():
    from app.core.soql_builder import SoQLBuilder

    contracts = SoQLBuilder.CONTRATOS_DATASET
    # «SENA REGIONAL ATLANTICO» is claimed by two entities: typing it would be asked again.
    assert RESOLVER.alias_owners("sena regional atlantico") != frozenset({"SENA REGIONAL ATLANTICO"})
    assert not typable(RESOLVER, "SENA REGIONAL ATLANTICO", (contracts,))
    # «ICBF REGIONAL BOGOTA» is an exact single owner for the host but has no source name: the projection
    # would answer UNSUPPORTED, so it is not READY and is counted in «y N más», never offered.
    assert typable(RESOLVER, "ICBF REGIONAL BOGOTA") and not typable(RESOLVER, "ICBF REGIONAL BOGOTA", (contracts,))
    found = buyer_choices(_proposal_for("ICBF"), RESOLVER)
    assert "ICBF REGIONAL BOGOTA" not in found.shown


def test_sena_has_no_verifiable_candidate_and_says_so_instead_of_looping():
    found = buyer_choices(_proposal_for("SENA"), RESOLVER)
    assert found.shown == () and found.hidden == len(RESOLVER.alias_owners("sena"))
    message = user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER, _proposal_for("SENA"), entity_resolver=RESOLVER)
    assert message.choices == ()
    assert "nombre completo" not in message.text and "Soledad" not in message.text  # the two loops of R0


def test_a_resolver_less_or_proposal_less_call_falls_back_to_the_generic_text_without_options():
    for proposal, resolver in ((None, RESOLVER), (_proposal_for("ICBF"), None)):
        message = user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER, proposal, entity_resolver=resolver)
        assert message.choices == () and "más de una entidad" in message.text
        assert "Soledad" not in message.text  # the old example that did not resolve the case


@pytest.mark.parametrize("alias", MULTI)
def test_every_offered_candidate_of_every_multi_owner_alias_resolves_to_exactly_itself(alias):
    from app.core.buyer_source_names import project_buyer_names
    from app.core.soql_builder import SoQLBuilder

    accepted = admit(alias)
    if accepted.status is not AcceptanceStatus.NEEDS_CLARIFICATION:
        pytest.skip("not asked by the host (already stopped by another guard)")
    found = buyer_choices(accepted.proposal, RESOLVER)
    owners = RESOLVER.alias_owners(alias)
    assert found is not None and len(found.shown) <= MAX_SHOWN
    assert found.hidden == len(owners) - len(found.shown) >= 0 and set(found.shown) <= owners
    for name in found.shown:
        assert project_buyer_names(name, SoQLBuilder.CONTRATOS_DATASET)  # the projection will not refuse it
        typed = admit(name)
        assert typed.status is AcceptanceStatus.READY, (alias, name)
        assert _owner_resolution(typed) == [(name,)], (alias, name)
    # Nothing verifiable is hidden by the cap: no candidate is offered only when no owner is typable.
    typable_owners = [n for n in owners if typable(RESOLVER, n, (SoQLBuilder.CONTRATOS_DATASET,))]
    assert (not found.shown) == (not typable_owners)


def test_the_aliases_without_a_verifiable_owner_are_the_declared_source_binding_debt():
    from app.core.soql_builder import SoQLBuilder

    silent = [a for a in MULTI if admit(a).status is AcceptanceStatus.NEEDS_CLARIFICATION
              and not buyer_choices(_proposal_for(a), RESOLVER).shown]
    # Declared (CANDIDATE.md R1), not hidden: the source has no physical name for any owner, so even the
    # unambiguous official name would be UNSUPPORTED in the Foundation (BUYER_SOURCE_PROJECTION_REQUIRED).
    assert 100 <= len(silent) <= 125 and {"sena", "invias", "ids"} <= set(silent)
    for alias in silent:
        assert not any(typable(RESOLVER, n, (SoQLBuilder.CONTRATOS_DATASET,)) for n in RESOLVER.alias_owners(alias))


# ── end to end: ask, show candidates, type one, READY with that exact buyer ──────────────────────────

def _harness(tmp_path, monkeypatch):
    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
    from app.core.soql_builder import SoQLBuilder
    from tests.product_contract.harness import ProductHarness

    now = datetime.fromisoformat("2026-09-26T12:00:00-05:00")

    def producer(raw):
        prefix = "contratos del " if raw.startswith("contratos del ") else "contratos de "
        buyer = raw[len(prefix):-len(" en 2025")]
        b_at, t_at = raw.index(buyer), raw.index("2025")
        payload = {
            "requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                          "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                          "universe_evidence": {"start": 0, "end": 9, "quote": "contratos"},
                          "condition_expression": {"operator": "AND", "children": [
                              {"operator": "ATOM", "condition_id": "c0"}, {"operator": "ATOM", "condition_id": "c1"}]}}],
            "conditions": [
                {"condition_id": "c0", "request_ids": ["r1"], "dimension": "BUYER", "operator": "is",
                 "polarity": "POSITIVE", "evidence": [{"start": b_at, "end": b_at + len(buyer), "quote": buyer}]},
                {"condition_id": "c1", "request_ids": ["r1"], "dimension": "TIME", "operator": "during_year",
                 "polarity": "POSITIVE", "evidence": [{"start": t_at, "end": t_at + 4, "quote": "2025"}]}],
        }
        return decode_proposal(payload, raw, arm="C")

    rows = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS[dataset]})]
            for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    return ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows, reference_instant=now, today=now.date(),
                          foundation_producer=producer,
                          settings_overrides={"secoppal_foundation_mode": "foundation_v3"})


def test_end_to_end_icbf_the_shown_candidates_typed_by_the_user_are_ready_with_that_buyer(tmp_path, monkeypatch):
    harness = _harness(tmp_path, monkeypatch)
    first = harness.say("contratos del ICBF en 2025")
    assert first.result["foundation_terminal"] == "NEEDS_CLARIFICATION"
    assert first.result["foundation_internal_reason"] == AMBIGUOUS_BUYER
    assert harness.transport.snapshot() == []  # asking costs no source call
    shown = first.result["foundation_choices"]
    assert shown[0] == "ICBF SEDE NACIONAL" and len(shown) == 6
    assert all(f"«{name}»" in first.result["response"] for name in shown)
    assert "y 6 más" in first.result["response"]

    for at, chosen in enumerate(shown):
        # A new conversation: the Foundation is first-turn only (INELIGIBLE_HISTORY), so READY is judged here.
        second = harness.say(f"contratos de {chosen} en 2025", chat_id=f"fresh-{at}")
        assert second.result["foundation_terminal"] == "READY", (chosen, second.result["response"])
        queries = [call["soql"] for call in second.source_calls if call.get("soql")]
        assert queries and all(f"nombre_entidad = '{chosen}'" in soql for soql in queries), (chosen, queries)

    # The same conversation answers through the legacy owner: it must name the same entity, not the old last writer.
    same = harness.say(f"contratos de {shown[0]} en 2025")
    assert same.result["resolved_params"]["entidad_resolved"] == shown[0] != "ICBF REGIONAL TOLIMA"


def test_end_to_end_sena_asks_without_a_loop_and_without_a_source_call(tmp_path, monkeypatch):
    harness = _harness(tmp_path, monkeypatch)
    first = harness.say("contratos del SENA en 2025")
    assert first.result["foundation_internal_reason"] == AMBIGUOUS_BUYER
    assert first.result["foundation_choices"] == [] and harness.transport.snapshot() == []
    assert "nombre completo" not in first.result["response"]
