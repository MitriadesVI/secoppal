"""Legacy shadow: per-dimension parity of legacy params vs a READY Foundation acceptance.

The legacy side is a frozen capture of ``QueryRouter().parse`` (tests/fixtures/
legacy_shadow_inventory.json: 83 queries, today 2026-09-30) so the contract of the instrument does
not move with the parser; ``TestLiveInventory`` re-parses the same queries to detect key drift.
The Foundation side is hand-built with ``decode_proposal`` and admitted by the real host.
"""

from __future__ import annotations

import copy
import json
import socket
from datetime import date
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import admit_proposal
from app.core.foundation_producers import decode_proposal
from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance
from app.core.legacy_shadow import (
    DIMENSION_ORDER, LEGACY_IGNORED_KEYS, LEGACY_KEY_INVENTORY, ShadowStatus, Verdict, compare,
)

TODAY = date(2026, 9, 30)
FIXTURE = json.loads(Path("tests/fixtures/legacy_shadow_inventory.json").read_text(encoding="utf-8"))
LEGACY = FIXTURE["params_by_query"]
CORPUS = Path("docs/audits/foundation_entry_admission_m0/harness/synthetic.txt")


@pytest.fixture(scope="module")
def resolver():
    return EntityResolver(Path("app/data/aliases_db.json"))


def build(resolver, raw, conditions=(), *, subject="contracts", universe="contratos",
          kind="records", expression="AND"):
    """Admit a hand-built proposal. conditions: (dimension, operator, polarity, quote)."""
    start = raw.index(universe)
    payload = {"requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH",
                             "subject": subject, "universe_kind": kind,
                             "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                             "universe_evidence": {"start": start, "end": start + len(universe),
                                                   "quote": universe}}],
               "conditions": []}
    atoms = []
    for index, (dimension, operator, polarity, quote) in enumerate(conditions, 1):
        at = raw.index(quote)
        payload["conditions"].append({
            "condition_id": f"c{index}", "request_ids": ["r1"], "dimension": dimension,
            "operator": operator, "polarity": polarity,
            "evidence": [{"start": at, "end": at + len(quote), "quote": quote}]})
        atoms.append({"operator": "ATOM", "condition_id": f"c{index}"})
    if len(atoms) == 1:
        payload["requests"][0]["condition_expression"] = atoms[0]
    elif atoms:
        payload["requests"][0]["condition_expression"] = {"operator": expression, "children": atoms}
    proposal = decode_proposal(payload, raw, arm="C")
    return admit_proposal(proposal, raw, entity_resolver=resolver)


def run(resolver, raw, conditions=(), **kwargs):
    accepted = build(resolver, raw, conditions, **kwargs)
    assert accepted.status is AcceptanceStatus.READY, accepted.reason
    return compare(raw, TODAY, LEGACY[raw], accepted)


def verdicts(diff):
    return {item.dimension: item.verdict.value for item in diff.dimensions}


def by_dimension(diff, name):
    return next(item for item in diff.dimensions if item.dimension == name)


TOPIC = "TOPIC", "contains", "POSITIVE"
AGREE, ONLY_LEGACY, ONLY_FOUNDATION, DIFFER = (v.value for v in Verdict)


class TestAgreement:
    def test_topic_geography_year_agree_totally(self, resolver):
        raw = "contratos de aseo en Antioquia en 2024"
        diff = run(resolver, raw, [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "POSITIVE", "Antioquia"),
                                   ("TIME", "during_year", "POSITIVE", "2024")])
        assert verdicts(diff) == {"UNIVERSE": AGREE, "TOPIC": AGREE, "GEOGRAPHY": AGREE, "TIME": AGREE}
        assert diff.is_parity and diff.unmapped_legacy_keys == ()
        assert by_dimension(diff, "TIME").foundation[0]["value"] == {
            "from": "2024-01-01", "to_exclusive": "2025-01-01"}

    def test_buyer_and_topic(self, resolver):
        raw = "contratos del Ministerio de Educación en 2023"
        diff = run(resolver, raw, [("BUYER", "is", "POSITIVE", "Ministerio de Educación"),
                                   ("TIME", "during_year", "POSITIVE", "2023")])
        assert verdicts(diff) == {"UNIVERSE": AGREE, "BUYER": AGREE, "TIME": AGREE}

    def test_buyer_resolved_to_another_entity_differs(self, resolver):
        raw = "contratos del Ministerio de Educación en 2023"
        accepted = build(resolver, raw, [("BUYER", "is", "POSITIVE", "Ministerio de Educación")])
        other = {**LEGACY[raw], "entidad_resolved": "MINISTERIO DE DEFENSA NACIONAL"}
        assert by_dimension(compare(raw, TODAY, other, accepted), "BUYER").verdict is Verdict.DIFFER

    def test_contract_type(self, resolver):
        raw = "contratos de prestación de servicios"
        diff = run(resolver, raw, [("CONTRACT_TYPE_OR_NATURE", "is", "POSITIVE", "prestación de servicios")])
        assert verdicts(diff) == {"UNIVERSE": AGREE, "CONTRACT_TYPE_OR_NATURE": AGREE}

    def test_modality(self, resolver):
        raw = "procesos de mínima cuantía en Santander"
        diff = run(resolver, raw, [("MODALITY", "is", "POSITIVE", "mínima cuantía"),
                                   ("GEOGRAPHY", "is", "POSITIVE", "Santander")],
                   subject="processes", universe="procesos")
        assert verdicts(diff) == {"UNIVERSE": AGREE, "GEOGRAPHY": AGREE, "MODALITY": AGREE}

    def test_value_agrees_but_declares_the_boundary_difference(self, resolver):
        raw = "contratos de más de 500 millones"
        diff = run(resolver, raw, [("VALUE", "greater_than", "POSITIVE", "más de 500 millones")])
        value = by_dimension(diff, "VALUE")
        assert value.verdict is Verdict.AGREE
        assert value.notes == ("BOUND_INCLUSIVITY_DIFFERS",)
        assert (value.legacy[0]["value"]["inclusive"], value.foundation[0]["value"]["inclusive"]) == (True, False)

    def test_value_direction_is_compared(self, resolver):
        raw = "contratos de menos de 100 millones de pesos"
        diff = run(resolver, raw, [("VALUE", "less_than", "POSITIVE", "menos de 100 millones de pesos")])
        assert by_dimension(diff, "VALUE").verdict is Verdict.AGREE
        wrong = {**LEGACY[raw], "valor_min": LEGACY[raw]["valor_max"]}
        wrong.pop("valor_max")
        accepted = build(resolver, raw, [("VALUE", "less_than", "POSITIVE", "menos de 100 millones de pesos")])
        assert by_dimension(compare(raw, TODAY, wrong, accepted), "VALUE").verdict is Verdict.DIFFER

    def test_state_literal_and_family(self, resolver):
        published = "procesos publicados en el último mes"
        diff = run(resolver, published, [("STATE", "is", "POSITIVE", "publicados")],
                   subject="processes", universe="procesos")
        assert by_dimension(diff, "STATE").verdict is Verdict.AGREE
        funnel = "procesos abiertos de construcción"
        diff = run(resolver, funnel, [("STATE", "family", "POSITIVE", "abiertos"),
                                      (*TOPIC, "construcción")],
                   subject="processes", universe="procesos")
        assert by_dimension(diff, "STATE").verdict is Verdict.AGREE
        assert by_dimension(diff, "TOPIC").verdict is Verdict.AGREE

    def test_year_is_a_half_open_interval(self, resolver):
        raw = "contratos de 2023"
        diff = run(resolver, raw, [("TIME", "during_year", "POSITIVE", "2023")])
        assert by_dimension(diff, "TIME").verdict is Verdict.AGREE

    def test_opportunities_agree_on_intent_and_list_the_policy(self, resolver):
        raw = "oportunidades de software"
        diff = run(resolver, raw, [(*TOPIC, "software")], subject="processes",
                   universe="oportunidades", kind="opportunities")
        assert verdicts(diff)["RETRIEVAL_INTENT"] == AGREE
        assert verdicts(diff)["UNIVERSE"] == AGREE and verdicts(diff)["TOPIC"] == AGREE
        assert verdicts(diff)["STATE"] == ONLY_LEGACY  # the legacy funnel default
        assert diff.foundation_policies == ("O1B@1",)


class TestDifferences:
    def test_only_legacy_city_and_order(self, resolver):
        raw = "contratos de aseo más caros en Medellín"
        diff = run(resolver, raw, [(*TOPIC, "aseo")])
        assert verdicts(diff) == {"UNIVERSE": AGREE, "TOPIC": AGREE, "GEOGRAPHY": ONLY_LEGACY,
                                  "GEOGRAPHY_MUNICIPALITY": ONLY_LEGACY, "ORDER": ONLY_LEGACY}
        assert by_dimension(diff, "ORDER").legacy[0]["value"] == {"signal": "valor_desc"}
        assert by_dimension(diff, "GEOGRAPHY_MUNICIPALITY").legacy[0]["value"] == {"municipality": "medellin"}
        assert by_dimension(diff, "GEOGRAPHY").notes == ("DEPARTMENT_DERIVED_FROM_MUNICIPALITY",)
        assert not diff.is_parity

    def test_department_agrees_while_the_city_stays_legacy(self, resolver):
        raw = "contratos de aseo en Medellín, Antioquia"
        diff = run(resolver, raw, [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "POSITIVE", "Antioquia")])
        assert verdicts(diff)["GEOGRAPHY"] == AGREE
        assert verdicts(diff)["GEOGRAPHY_MUNICIPALITY"] == ONLY_LEGACY

    def test_topic_differs(self, resolver):
        raw = "contratos de aseo y cafetería"
        diff = run(resolver, raw, [(*TOPIC, "aseo")])
        topic = by_dimension(diff, "TOPIC")
        assert topic.verdict is Verdict.DIFFER
        assert topic.legacy[0]["value"] == {"all_of": ["aseo", "cafeteria"]}
        assert topic.foundation[0]["value"] == {"all_of": ["aseo"]}

    def test_geography_differs(self, resolver):
        raw = "contratos de aseo en Antioquia en 2024"
        diff = run(resolver, raw, [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "POSITIVE", "Antioquia")])
        wrong = {**LEGACY[raw], "departamento_resolved": "Bolívar"}
        accepted = build(resolver, raw, [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "POSITIVE", "Antioquia")])
        assert verdicts(compare(raw, TODAY, wrong, accepted))["GEOGRAPHY"] == DIFFER
        assert verdicts(diff)["TIME"] == ONLY_LEGACY

    def test_year_range_is_not_a_year(self, resolver):
        raw = "contratos entre 2020 y 2023"
        accepted = build(resolver, "contratos de 2023", [("TIME", "during_year", "POSITIVE", "2023")])
        assert by_dimension(compare(raw, TODAY, LEGACY[raw], accepted), "TIME").verdict is Verdict.DIFFER

    def test_universe_differs(self, resolver):
        raw = "contratos de aseo en Antioquia en 2024"
        accepted = build(resolver, "procesos de aseo", [(*TOPIC, "aseo")], subject="processes",
                         universe="procesos")
        assert by_dimension(compare(raw, TODAY, LEGACY[raw], accepted), "UNIVERSE").verdict is Verdict.DIFFER

    def test_actor_only_in_legacy(self, resolver):
        raw = "contratos del contratista NIT 900123456"
        diff = run(resolver, raw)
        assert verdicts(diff) == {"UNIVERSE": AGREE, "ACTOR": ONLY_LEGACY}
        assert by_dimension(diff, "ACTOR").legacy[0]["value"] == {"nit": "900123456"}

    def test_or_group_never_agrees_with_a_conjunction(self, resolver):
        raw = "contratos de aseo y cafetería"  # legacy AND-bag {aseo, cafeteria}
        both = [(*TOPIC, "aseo"), (*TOPIC, "cafetería")]
        anded = compare(raw, TODAY, LEGACY[raw], build(resolver, raw, both))
        ored = compare(raw, TODAY, LEGACY[raw], build(resolver, raw, both, expression="OR"))
        assert by_dimension(anded, "TOPIC").verdict is Verdict.AGREE
        assert by_dimension(ored, "TOPIC").verdict is Verdict.DIFFER

    def test_legacy_or_group_is_not_a_conjunction(self, resolver):
        raw = "contratos de aseo o cafetería en Soacha"
        accepted = build(resolver, raw, [(*TOPIC, "aseo"), (*TOPIC, "cafetería")])
        topic = by_dimension(compare(raw, TODAY, LEGACY[raw], accepted), "TOPIC")
        assert topic.legacy[0]["value"] == {"any_of": ["aseo", "cafeteria"]}
        assert topic.verdict is Verdict.DIFFER

    def test_negated_buyer_is_invisible_in_legacy_params(self, resolver):
        raw = "contratos de aseo que no sean de la Gobernación de Antioquia"
        assert "entidad_resolved" not in LEGACY[raw]
        diff = run(resolver, raw, [(*TOPIC, "aseo"),
                                   ("BUYER", "is", "NEGATIVE", "Gobernación de Antioquia")])
        buyer = by_dimension(diff, "BUYER")
        assert buyer.verdict is Verdict.ONLY_FOUNDATION and buyer.foundation[0]["polarity"] == "NEGATIVE"

    def test_legacy_polarity_inversion_is_a_differ(self, resolver):
        raw = "contratos de aseo sin contar Antioquia"  # legacy keeps Antioquia POSITIVE
        diff = run(resolver, raw, [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "NEGATIVE", "Antioquia")])
        geography = by_dimension(diff, "GEOGRAPHY")
        assert geography.verdict is Verdict.DIFFER
        assert (geography.legacy[0]["polarity"], geography.foundation[0]["polarity"]) == ("POSITIVE", "NEGATIVE")


class TestPolarity:
    RAW = "contratos que no sean de aseo"

    def test_negated_topic_agrees_with_legacy_exclusion(self, resolver):
        diff = run(resolver, self.RAW, [("TOPIC", "contains", "NEGATIVE", "aseo")])
        topic = by_dimension(diff, "TOPIC")
        assert topic.verdict is Verdict.AGREE
        assert topic.legacy[0] == {"polarity": "NEGATIVE", "value": {"none_of": ["aseo"]}}

    def test_negated_topic_is_not_the_topic(self, resolver):
        diff = run(resolver, self.RAW, [(*TOPIC, "aseo")])
        assert by_dimension(diff, "TOPIC").verdict is Verdict.DIFFER
        legacy_positive = {**LEGACY["busca contratos de aseo"]}
        accepted = build(resolver, self.RAW, [("TOPIC", "contains", "NEGATIVE", "aseo")])
        assert by_dimension(compare(self.RAW, TODAY, legacy_positive, accepted), "TOPIC").verdict is Verdict.DIFFER

    def test_multiword_negative_phrase_is_not_the_legacy_token_exclusion(self, resolver):
        raw = "contratos excepto de obra civil"
        diff = run(resolver, raw, [("TOPIC", "contains", "NEGATIVE", "obra civil")])
        assert by_dimension(diff, "TOPIC").verdict is Verdict.DIFFER


class TestNotReady:
    def test_unresolved_foundation_is_declared_and_not_compared(self, resolver):
        raw = "contratos de aseo más caros en Medellín"
        accepted = build(resolver, raw, [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "POSITIVE", "Medellín")])
        assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
        diff = compare(raw, TODAY, LEGACY[raw], accepted)
        assert diff.status is ShadowStatus.FOUNDATION_NOT_READY
        assert diff.dimensions == () and diff.foundation_status == "NEEDS_CLARIFICATION"
        assert diff.reason == "UNRESOLVED_HOST_DIMENSION:GEOGRAPHY" and not diff.is_parity

    def test_no_acceptance(self):
        diff = compare("x", TODAY, LEGACY["busca contratos de aseo"], None)
        assert (diff.status, diff.reason, diff.dimensions) == (
            ShadowStatus.FOUNDATION_NOT_READY, "NO_ACCEPTANCE", ())

    def test_acceptance_without_proposal(self):
        accepted = InterpretationAcceptance(None, AcceptanceStatus.PROVIDER_ERROR, reason="PROVIDER_TIMEOUT")
        diff = compare("x", TODAY, {}, accepted)
        assert diff.status is ShadowStatus.FOUNDATION_NOT_READY and diff.reason == "PROVIDER_TIMEOUT"

    def test_free_text_reason_is_not_echoed(self):
        accepted = InterpretationAcceptance(None, AcceptanceStatus.INVALID_PROPOSAL,
                                            reason="contratos de aseo en Cali")
        assert compare("x", TODAY, {}, accepted).reason == "UNSPECIFIED"

    def test_multi_request_acceptance_is_not_compared(self, resolver):
        raw = "contratos y procesos de aseo"
        spans = {"contratos": raw.index("contratos"), "procesos": raw.index("procesos")}
        requests = [{"request_id": rid, "scope_id": "s1", "operation": "SEARCH", "subject": subject,
                     "universe_kind": "records",
                     "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                     "universe_evidence": {"start": spans[word], "end": spans[word] + len(word), "quote": word},
                     "condition_expression": {"operator": "ATOM", "condition_id": "c1"}}
                    for rid, subject, word in (("r1", "contracts", "contratos"), ("r2", "processes", "procesos"))]
        at = raw.index("aseo")
        payload = {"requests": requests,
                   "conditions": [{"condition_id": "c1", "request_ids": ["r1", "r2"], "dimension": "TOPIC",
                                   "operator": "contains", "polarity": "POSITIVE",
                                   "evidence": [{"start": at, "end": at + 4, "quote": "aseo"}]}],
                   "relations": [{"kind": "SHARES_SCOPE", "members": ["r1", "r2"]}]}
        accepted = admit_proposal(decode_proposal(payload, raw, arm="C"), raw, entity_resolver=resolver)
        assert accepted.status is AcceptanceStatus.READY and len(accepted.proposal.requests) == 2
        diff = compare(raw, TODAY, LEGACY["busca contratos de aseo"], accepted)
        assert (diff.status, diff.dimensions) == (ShadowStatus.FOUNDATION_MULTI_REQUEST, ())

    def test_non_mapping_legacy_params(self, resolver):
        accepted = build(resolver, "busca contratos de aseo", [(*TOPIC, "aseo")])
        assert compare("x", TODAY, None, accepted).status is ShadowStatus.LEGACY_PARAMS_INVALID


class TestPurity:
    RAW = "contratos de aseo en Antioquia en 2024"
    CONDITIONS = [(*TOPIC, "aseo"), ("GEOGRAPHY", "is", "POSITIVE", "Antioquia"),
                  ("TIME", "during_year", "POSITIVE", "2024")]

    def test_inputs_are_not_mutated(self, resolver):
        accepted = build(resolver, self.RAW, self.CONDITIONS)
        params = copy.deepcopy(LEGACY[self.RAW])
        params_before, accepted_before = copy.deepcopy(params), copy.deepcopy(accepted)
        compare(self.RAW, TODAY, params, accepted)
        assert params == params_before and accepted == accepted_before

    def test_result_is_json_and_stable_under_key_order(self, resolver):
        accepted = build(resolver, self.RAW, self.CONDITIONS)
        params = LEGACY[self.RAW]
        shuffled = dict(reversed(list(params.items())))
        first = json.dumps(compare(self.RAW, TODAY, params, accepted).to_dict(), sort_keys=False)
        second = json.dumps(compare(self.RAW, TODAY, shuffled, accepted).to_dict(), sort_keys=False)
        assert first == second
        assert json.loads(first)["status"] == "COMPARED"

    def test_raw_only_contributes_a_digest(self, resolver):
        accepted = build(resolver, self.RAW, self.CONDITIONS)
        left = compare(self.RAW, TODAY, LEGACY[self.RAW], accepted).to_dict()
        right = compare("otro texto cualquiera", TODAY, LEGACY[self.RAW], accepted).to_dict()
        assert left.pop("raw_digest") != right.pop("raw_digest")
        assert left == right
        assert len(compare(self.RAW, TODAY, {}, None).raw_digest) == 16
        assert self.RAW not in json.dumps(compare(self.RAW, TODAY, LEGACY[self.RAW], accepted).to_dict())
        assert compare(None, TODAY, {}, None).raw_digest is None

    def test_no_network(self, resolver, monkeypatch):
        def forbidden(*args, **kwargs):
            raise AssertionError("no network in the shadow")

        monkeypatch.setattr(socket.socket, "connect", forbidden)
        accepted = build(resolver, self.RAW, self.CONDITIONS)
        assert compare(self.RAW, TODAY, LEGACY[self.RAW], accepted).is_parity


class TestInventory:
    def test_every_captured_key_is_classified(self):
        classified = {k for keys in LEGACY_KEY_INVENTORY.values() for k in keys} | LEGACY_IGNORED_KEYS
        unknown = {q: sorted(set(p) - classified) for q, p in LEGACY.items() if set(p) - classified}
        assert unknown == {}
        assert not ({k for keys in LEGACY_KEY_INVENTORY.values() for k in keys} & LEGACY_IGNORED_KEYS)

    def test_a_new_legacy_key_is_reported_not_swallowed(self, resolver):
        accepted = build(resolver, "busca contratos de aseo", [(*TOPIC, "aseo")])
        diff = compare("x", TODAY, {**LEGACY["busca contratos de aseo"], "brand_new_key": 1}, accepted)
        assert diff.unmapped_legacy_keys == ("brand_new_key",)

    def test_every_inventoried_dimension_is_produced_by_some_captured_query(self):
        from app.core.legacy_shadow import _legacy_side

        produced = {d for params in LEGACY.values() for d in _legacy_side(params)[0]}
        assert produced == set(LEGACY_KEY_INVENTORY)

    def test_dimension_order_covers_the_vocabulary(self):
        from app.core.semantic_dimensions import SemanticDimension

        assert {d.value for d in SemanticDimension} - {"UNSUPPORTED"} <= set(DIMENSION_ORDER)
        assert set(LEGACY_KEY_INVENTORY) <= set(DIMENSION_ORDER)

    def test_live_parser_still_emits_only_classified_keys(self, monkeypatch):
        from app.core.query_router import QueryRouter

        def forbidden(*args, **kwargs):
            raise AssertionError("no network in the parser")

        monkeypatch.setattr(socket.socket, "connect", forbidden)
        router = QueryRouter()
        classified = {k for keys in LEGACY_KEY_INVENTORY.values() for k in keys} | LEGACY_IGNORED_KEYS
        for query in LEGACY:
            keys = set(router.parse(query, today=TODAY).params)
            assert keys <= classified, (query, sorted(keys - classified))


def corpus_texts():
    return [line.split("\t", 1)[1] for line in CORPUS.read_text(encoding="utf-8").splitlines()
            if "\t" in line]


class TestProperties:
    def test_synthetic_corpus_never_raises_and_is_idempotent(self):
        from app.core.query_router import QueryRouter

        router, texts = QueryRouter(), corpus_texts()
        assert len(texts) >= 30
        for text in texts:
            params = router.parse(text, today=TODAY).params
            first = compare(text, TODAY, params, None)
            second = compare(text, TODAY, copy.deepcopy(params), None)
            assert first.status is ShadowStatus.FOUNDATION_NOT_READY
            assert first.to_dict() == second.to_dict()

    @settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(st.dictionaries(
        st.sampled_from(sorted({k for keys in LEGACY_KEY_INVENTORY.values() for k in keys}
                               | LEGACY_IGNORED_KEYS | {"zzz"})),
        st.recursive(st.none() | st.booleans() | st.integers() | st.floats() | st.text(max_size=12),
                     lambda children: st.lists(children, max_size=3)
                     | st.dictionaries(st.text(max_size=5), children, max_size=3), max_leaves=6),
        max_size=12))
    def test_arbitrary_legacy_params_never_raise_and_stay_json(self, resolver, params):
        accepted = build(resolver, "busca contratos de aseo", [(*TOPIC, "aseo")])
        for acceptance in (accepted, None):
            diff = compare("x", TODAY, params, acceptance)
            assert diff.status is not ShadowStatus.SHADOW_ERROR
            assert json.loads(json.dumps(diff.to_dict())) == diff.to_dict()
