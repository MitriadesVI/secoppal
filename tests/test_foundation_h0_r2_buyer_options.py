"""M1-H0 R2: a list of buyer options is never a partial list passed off as the whole choice.

ZEUS R1 (P1): the options were the owners the host can type, so the entity the user named («IDRD», «ICBF
Bogotá») could be missing from «Opciones» while its siblings were offered, and «no sean del ICBF» offered
a sibling (which would invert the exclusion).  Contract fixed here:
  - every owner is accounted for: offered, over the cap («y N más», a real count) or unavailable;
  - any unavailable owner => the text says the list is partial, names those owners, and carries no buttons;
  - an owner whose name holds every cited word (a minority of the owners) goes first; if it is unavailable the
    text says so and offers nothing else;
  - a NEGATIVE buyer gets no options at all;
  - the cited phrase is read with ``material_phrase``, the host's own reader.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import AMBIGUOUS_BUYER, admit_proposal
from app.core.foundation_buyer_choices import MAX_SHOWN, buyer_choices, typable
from app.core.foundation_messages import _EXACT, user_message
from app.core.foundation_producers import decode_proposal
from app.core.interpretation_contract import AcceptanceStatus
from app.core.soql_builder import SoQLBuilder
from tests.test_foundation_h0_buyer_ambiguity import ALIASES, MULTI, RESOLVER

CONTRACTS = SoQLBuilder.CONTRATOS_DATASET


def _payload(raw, buyers, *, polarity="POSITIVE", extra=()):
    """«<raw>» with a contracts universe at its end; `buyers` = list of evidence lists [(start, end), ...]."""
    universe_at = raw.rindex("contratos")
    conditions = [{"condition_id": f"c{i}", "request_ids": ["r1"], "dimension": "BUYER", "operator": "is",
                   "polarity": polarity,
                   "evidence": [{"start": a, "end": b, "quote": raw[a:b]} for a, b in spans]}
                  for i, spans in enumerate(buyers)]
    conditions += list(extra)
    atoms = [{"operator": "ATOM", "condition_id": c["condition_id"]} for c in conditions]
    expression = atoms[0] if len(atoms) == 1 else {"operator": "AND", "children": atoms}
    return {"requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                          "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                          "universe_evidence": {"start": universe_at, "end": universe_at + 9, "quote": "contratos"},
                          "condition_expression": expression}],
            "conditions": conditions}


def _ask(raw, buyers, resolver=RESOLVER, **kwargs):
    proposal = decode_proposal(_payload(raw, buyers, **kwargs), raw, arm="C")
    return proposal, admit_proposal(proposal, raw, entity_resolver=resolver)


def _span(raw, phrase, nth=0):
    at = -1
    for _ in range(nth + 1):
        at = raw.index(phrase, at + 1)
    return (at, at + len(phrase))


def _message(alias, resolver=RESOLVER, **kwargs):
    raw = f"{alias} contratos"
    proposal, accepted = _ask(raw, [[(0, len(alias))]], resolver, **kwargs)
    return proposal, raw, accepted, user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER, proposal,
                                                 entity_resolver=resolver, raw=raw)


class _Resolver(EntityResolver):
    """The real resolver plus synthetic aliases (to build the shapes the data does not contain)."""

    def __init__(self, extra):
        super().__init__(ALIASES)
        self.extra = {k: frozenset(v) for k, v in extra.items()}

    def alias_owners(self, normalized):
        return self.extra.get(normalized) or super().alias_owners(normalized)

    def resolve_entidad(self, text, *args, **kwargs):
        if normalized := self.extra.get(text.strip().lower()):
            return SimpleNamespace(method="exact", value=sorted(normalized)[0])
        return super().resolve_entidad(text, *args, **kwargs)


# ── ZEUS repros: the entity the user named is never hidden behind its siblings ───────────────────────────

@pytest.mark.parametrize("alias, named", [("idrd", "IDRD - ENTIDAD OFICIAL."), ("icbf bogota", "ICBF REGIONAL BOGOTA")])
def test_a_named_owner_that_cannot_be_taken_by_name_is_said_so_and_no_sibling_is_offered(alias, named):
    _, _, accepted, message = _message(alias)
    assert accepted.reason == AMBIGUOUS_BUYER
    assert f"«{named}»" in message.text and "no puedo consultar" in message.text
    assert message.choices == () and "Opciones" not in message.text
    others = RESOLVER.alias_owners(alias) - {named}
    assert others and not any(f"«{other}»" in message.text for other in others)  # no sibling to steer to


@pytest.mark.parametrize("alias", ["idrd", "el idrd", "idrd bogota", "instituto distrital de recreacion y deporte",
                                   "icbf bogota", "icbf d.c.", "icbf distrito capital", "bogota distrito capital",
                                   "instituto colombiano de bienestar familiar bogota"])
def test_no_variant_of_the_zeus_repros_presents_a_partial_list_as_the_choice(alias):
    proposal, raw, accepted, message = _message(alias)
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    found = buyer_choices(proposal, RESOLVER, raw)
    assert found.unavailable, alias  # all of these have an owner the host cannot take by name
    assert message.choices == () and "Opciones" not in message.text
    assert all(f"«{name}»" in message.text for name in found.unavailable[:4])  # the missing owner is named
    if found.shown and not found.named_unavailable:
        assert "parcial" in message.text


def test_icbf_is_declared_partial_with_real_counts_and_no_buttons():
    proposal, raw, _, message = _message("icbf")
    found = buyer_choices(proposal, RESOLVER, raw)
    owners = RESOLVER.alias_owners("icbf")
    assert found.unavailable == ("ICBF REGIONAL BOGOTA",) and found.named_unavailable == ()  # «icbf» names none
    assert len(found.shown) + found.hidden + len(found.unavailable) == len(owners)
    assert "Esta lista es parcial: hay 1 más que no puedo consultar por nombre todavía" in message.text
    assert "y 5 más que sí puedo consultar" in message.text and message.choices == ()


def test_a_complete_list_keeps_its_options_and_does_not_claim_to_be_partial():
    proposal, raw, _, message = _message("sdh")
    found = buyer_choices(proposal, RESOLVER, raw)
    assert found.complete and found.unavailable == () and found.hidden == 0
    assert message.choices == found.shown == ("SECRETARIA DISTRITAL DE HACIENDA", "SECRETARIA DISTRITAL DEL HABITAT-")
    assert "Opciones:" in message.text and "parcial" not in message.text


def test_with_no_offerable_owner_the_text_is_the_generic_one_without_options():
    for alias in ("sena", "invias", "ids"):
        _, _, _, message = _message(alias)
        assert message == _EXACT[AMBIGUOUS_BUYER] and message.choices == ()
        assert "Opciones" not in message.text and "Algunas de las entidades" not in message.text


# ── the named owner goes first when it can be taken by name ─────────────────────────────────────────────

def test_a_distinctive_offerable_owner_goes_first_even_past_the_cap_and_the_headquarters_rule():
    names = ["ICBF SEDE NACIONAL", "ICBF REGIONAL ANTIOQUIA", "ICBF REGIONAL ATLANTICO", "ICBF REGIONAL CAUCA",
             "ICBF REGIONAL CUNDINAMARCA", "ICBF REGIONAL GUAJIRA", "ICBF REGIONAL HUILA", "ICBF REGIONAL TOLIMA"]
    resolver = _Resolver({"regional tolima x": names})
    assert all(typable(resolver, name, (CONTRACTS,)) for name in names)  # every owner is offerable
    found = buyer_choices(*_proposal_raw("regional tolima x", resolver))
    # words cited: regional, tolima, x -> no owner holds «x»: nothing distinctive, plain order (HQ first)
    assert found.shown[0] == "ICBF SEDE NACIONAL"
    resolver = _Resolver({"tolima": names})
    found = buyer_choices(*_proposal_raw("tolima", resolver))
    assert found.complete and found.shown[0] == "ICBF REGIONAL TOLIMA"  # named first although it sorts last
    assert len(found.shown) == MAX_SHOWN and found.hidden == len(names) - MAX_SHOWN == 2
    message = _message("tolima", resolver)[3]  # complete: the hidden owners are a real count, buttons stay
    assert message.choices == found.shown and message.text.count("«") == MAX_SHOWN + 1  # alias + 6 options
    assert f"y {found.hidden} más. " in message.text and "parcial" not in message.text


def _proposal_raw(alias, resolver):
    raw = f"{alias} contratos"
    proposal, _ = _ask(raw, [[(0, len(alias))]], resolver)
    return proposal, resolver, raw


def test_a_long_list_of_unavailable_owners_is_capped_with_a_real_count():
    offerable = ["ICBF SEDE NACIONAL", "ICBF REGIONAL ANTIOQUIA", "ICBF REGIONAL CAUCA"]
    missing = [f"ENTIDAD SIN NOMBRE FISICO {n}" for n in range(1, 7)]
    resolver = _Resolver({"alias sintetico": offerable + missing})
    proposal, raw, accepted, message = _message("alias sintetico", resolver)
    assert accepted.reason == AMBIGUOUS_BUYER
    found = buyer_choices(proposal, resolver, raw)
    assert set(found.shown) == set(offerable) and set(found.unavailable) == set(missing) and found.hidden == 0
    assert "hay 6 más que no puedo consultar" in message.text
    assert all(f"«{name}»" in message.text for name in found.unavailable[:4])
    assert "y 2 más)" in message.text and not any(f"«{name}»" in message.text for name in found.unavailable[4:])
    assert message.choices == ()


def test_a_citation_that_names_most_owners_promotes_none_of_them():
    # «icbf» is in the name of 11 of its 12 owners: nothing is distinctive, the headquarters rule decides.
    found = buyer_choices(*_proposal_raw("icbf", RESOLVER))
    assert found.shown[0] == "ICBF SEDE NACIONAL"


# ── negation: choosing an owner would invert the scope ─────────────────────────────────────────────────

@pytest.mark.parametrize("alias", ["icbf", "sena", "sdh", "icbf bogota", "idrd"])
def test_a_negative_buyer_is_never_offered_options(alias):
    raw = f"{alias} contratos"
    proposal, accepted = _ask(raw, [[(0, len(alias))]], polarity="NEGATIVE")
    if accepted.reason != AMBIGUOUS_BUYER:  # another guard may stop negation earlier: then no buyer text exists
        pytest.skip(f"stopped before the buyer guard: {accepted.reason}")
    found = buyer_choices(proposal, RESOLVER, raw)
    assert found.negated and found.shown == () and found.hidden == 0
    message = user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER, proposal, entity_resolver=RESOLVER, raw=raw)
    assert message.choices == () and "Opciones" not in message.text and "excluir" in message.text
    assert not any(f"«{owner}»" in message.text for owner in RESOLVER.alias_owners(alias))


def test_the_negative_buyer_is_reached_by_the_buyer_guard_for_at_least_one_alias():
    # Guards the skip above: if no negated alias ever reached the buyer guard the negation test would be vacuous.
    reached = []
    for alias in ("icbf", "sdh", "sena"):
        raw = f"{alias} contratos"
        _, accepted = _ask(raw, [[(0, len(alias))]], polarity="NEGATIVE")
        reached.append(accepted.reason == AMBIGUOUS_BUYER)
    assert any(reached)


# ── every owner is accounted for, over every alias of the index ─────────────────────────────────────────

@pytest.mark.parametrize("alias", MULTI)
def test_every_multi_owner_alias_is_accounted_for_and_worded_by_its_completeness(alias):
    raw = f"{alias} contratos"
    proposal, accepted = _ask(raw, [[(0, len(alias))]])
    if accepted.reason != AMBIGUOUS_BUYER:
        pytest.skip("not asked by the buyer guard")
    found = buyer_choices(proposal, RESOLVER, raw)
    message = user_message("NEEDS_CLARIFICATION", AMBIGUOUS_BUYER, proposal, entity_resolver=RESOLVER, raw=raw)
    owners = RESOLVER.alias_owners(alias)
    assert len(found.shown) + found.hidden + len(found.unavailable) == len(owners)
    if not found.shown:
        assert message == _EXACT[AMBIGUOUS_BUYER]
    elif found.named_unavailable:
        assert message.choices == () and all(f"«{n}»" in message.text for n in found.named_unavailable)
        assert "Opciones" not in message.text and not any(f"«{n}»" in message.text for n in found.shown)
    elif found.unavailable:
        assert message.choices == () and "parcial" in message.text and all(f"«{n}»" in message.text for n in found.shown)
    else:
        assert message.choices == found.shown and "Opciones:" in message.text and "parcial" not in message.text
        assert (f" y {found.hidden} más." in message.text) == bool(found.hidden)
    if found.hidden:
        assert f"y {found.hidden} más" in message.text


# ── several conditions: only the BUYER one with several owners is read ────────────────────────────────

def test_only_a_buyer_condition_is_read_and_a_single_owner_buyer_is_not_ambiguous():
    raw = "icbf del contrato de sdh contratos"
    topic = {"condition_id": "t0", "request_ids": ["r1"], "dimension": "TOPIC", "operator": "contains",
             "polarity": "POSITIVE", "evidence": [{"start": 0, "end": 4, "quote": "icbf"}]}
    proposal = decode_proposal(_payload(raw, [[_span(raw, "sdh")]], extra=[topic]), raw, arm="C")
    found = buyer_choices(proposal, RESOLVER, raw)
    assert found.alias == "sdh" and set(found.shown) == RESOLVER.alias_owners("sdh")  # not «icbf» (a TOPIC)
    only_topic = decode_proposal(_payload(raw, [], extra=[topic]), raw, arm="C")
    assert buyer_choices(only_topic, RESOLVER, raw) is None


def test_a_single_owner_buyer_is_not_ambiguous_and_the_next_multi_owner_one_is_read():
    raw = "alcaldia de barranquilla icbf contratos"
    assert len(RESOLVER.alias_owners("alcaldia de barranquilla")) == 1
    proposal = decode_proposal(_payload(raw, [[_span(raw, "alcaldia de barranquilla")], [_span(raw, "icbf")]]),
                               raw, arm="C")
    found = buyer_choices(proposal, RESOLVER, raw)
    assert found is not None and found.alias == "icbf"
    single = decode_proposal(_payload(raw, [[_span(raw, "alcaldia de barranquilla")]]), raw, arm="C")
    assert buyer_choices(single, RESOLVER, raw) is None


# ── the phrase is read with material_phrase, the host's own reader ─────────────────────────────────────

def test_a_repeated_citation_is_one_phrase_and_a_split_one_is_not_guessed():
    raw = "ICBF, es decir icbf contratos"  # same phrase up to case: «ICBF icbf» is not an alias of anyone
    proposal, accepted = _ask(raw, [[_span(raw, "ICBF"), _span(raw, "icbf")]])
    assert accepted.reason == AMBIGUOUS_BUYER  # the host read «icbf» once
    found = buyer_choices(proposal, RESOLVER, raw)
    assert found is not None and found.alias == "ICBF"  # joining the quotes would have looked up «icbf icbf»
    assert buyer_choices(proposal, RESOLVER, None) is None  # no raw text: nothing to read, generic text
    raw = "icbf de sdh contratos"
    split = decode_proposal(_payload(raw, [[_span(raw, "icbf"), _span(raw, "sdh")]]), raw, arm="C")
    assert buyer_choices(split, RESOLVER, raw) is None  # a word between two citations: no declared meaning


def test_the_datasets_checked_are_those_of_the_requests_that_carry_the_buyer(monkeypatch):
    import app.core.foundation_buyer_choices as module

    raw = "sdh contratos y otra cosa de procesos"
    payload = _payload(raw, [[(0, 3)]])
    at = raw.rindex("procesos")
    payload["requests"].append({"request_id": "r2", "scope_id": "r2", "operation": "SEARCH", "subject": "processes",
                                "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                                "universe_evidence": {"start": at, "end": at + 8, "quote": "procesos"}})
    proposal = decode_proposal(payload, raw, arm="C")
    # The source only has physical names for CONTRATOS: the buyer sits on the contracts request (r1) alone.
    monkeypatch.setattr(module, "project_buyer_names", lambda name, dataset: dataset == CONTRACTS)
    found = module.buyer_choices(proposal, RESOLVER, raw)
    assert found.alias == "sdh" and found.complete and len(found.shown) == 2  # r2's dataset is not asked
    monkeypatch.setattr(module, "project_buyer_names", lambda name, dataset: dataset != CONTRACTS)
    assert module.buyer_choices(proposal, RESOLVER, raw).shown == ()  # contracts has no name: nothing offered


# ── typable: each of its conditions is pinned ──────────────────────────────────────────────────────────

class _Fake:
    def __init__(self, owners, method="exact", value=None):
        self.owners, self.method, self.value = owners, method, value

    def alias_owners(self, normalized):
        return self.owners

    def resolve_entidad(self, name):
        return SimpleNamespace(method=self.method, value=self.value or name)


def test_typable_needs_a_single_owner_an_exact_resolution_to_itself_and_a_source_name():
    name = "ICBF SEDE NACIONAL"
    assert typable(_Fake(frozenset({name})), name, (CONTRACTS,))
    assert not typable(_Fake(frozenset({name, "OTRA"})), name, (CONTRACTS,))
    assert not typable(_Fake(frozenset({"OTRA"})), name, (CONTRACTS,))
    assert not typable(_Fake(frozenset({name}), method="fuzzy"), name, (CONTRACTS,))
    assert not typable(_Fake(frozenset({name}), method="exact_official"), name, (CONTRACTS,))
    assert not typable(_Fake(frozenset({name}), value="OTRA"), name, (CONTRACTS,))
    unprojected = "ZZZ SIN NOMBRE FISICO"  # a single exact owner, but the source has no physical name for it
    assert typable(_Fake(frozenset({unprojected})), unprojected) and not typable(
        _Fake(frozenset({unprojected})), unprojected, (CONTRACTS,))
