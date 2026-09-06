"""Evidence invariants: failed, malformed or contradictory calls are not zero."""

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.models import SectionStatus, SourceCall
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.sections import build_sections
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


@pytest.fixture
def profile_turn():
    text = "analiza la alcaldia de galapa"
    return classify_entity_profile_turn(text, QueryRouter().parse(text).params)


def _build(turn, client, params=None):
    return build_entity_profile(
        scope=turn.scope, identity=turn.identity,
        params=turn.params if params is None else params, focus="overview",
        secop_client=client, soql_builder=SoQLBuilder(),
    )


@pytest.mark.parametrize("rows", [[], [{}], [{"contract_count": "invalid"}],
    [{"contract_count": "1", "total_contract_value": None}],
    [{"contract_count": "0.5", "total_contract_value": "0"}],
    [{"contract_count": "-1", "total_contract_value": "0"}],
    [{"contract_count": "0", "total_contract_value": "100"}],
])
def test_malformed_contract_aggregate_cannot_be_zero(profile_turn, rows):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return rows
            return super().aggregate(dataset, soql)
    profile = _build(profile_turn, Client())
    assert profile.activity.fact("contract_count") is None
    assert profile.values.fact("total_contract_value") is None
    assert profile.values.status is SectionStatus.UNAVAILABLE
    assert "Se observan 0 contratos" not in render_entity_profile(profile)


def test_zero_contradicted_by_other_contract_calls_is_unavailable(profile_turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return [{"contract_count": "0", "total_contract_value": None}]
            return super().aggregate(dataset, soql)
    profile = _build(profile_turn, Client())
    assert profile.recent.fact("contracts")
    assert profile.activity.fact("contract_count") is None
    assert profile.values.fact("total_contract_value") is None
    assert "contradictori" in render_entity_profile(profile)


def test_confirmed_zero_is_rendered_when_all_contract_calls_agree(profile_turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if dataset == "jbjy-vk9h":
                return [{"contract_count": "0"}] if "GROUP BY" not in soql else []
            return super().aggregate(dataset, soql)
        def query(self, dataset, soql):
            return [] if dataset == "jbjy-vk9h" else super().query(dataset, soql)
    profile = _build(profile_turn, Client())
    assert profile.activity.fact("contract_count") == 0
    assert "Se observan 0 contratos por $0" in render_entity_profile(profile)


@pytest.mark.parametrize("extra", [
    {"contratista": ["900123456"]},
    {"actor_scope": {"requires_resolution": True, "observed_name": "actor"}},
    {"entidad_like": "OTRO COMPRADOR"},
    {"entidad_resolved": "OTRO COMPRADOR"},
    {"entidad_contains_all": ["OTRO"]},
])
def test_profile_scope_mismatch_fails_before_source_calls(profile_turn, extra):
    client = FakeClient()
    with pytest.raises(ValueError, match="scope"):
        _build(profile_turn, client, {**profile_turn.params, **extra})
    assert client.calls == []


@settings(max_examples=40, derandomize=True)
@given(status=st.sampled_from(["TIMEOUT", "ERROR", "BUILD_ERROR", "BUDGET_EXHAUSTED"]),
       count=st.integers(min_value=0, max_value=10**15))
def test_failed_call_never_contributes_facts_even_with_residual_rows(status, count):
    from app.core.entity_profile.models import EntityProfileScope, ScopeType
    scope = EntityProfileScope(ScopeType.BUYER_UNIT, ("MUNICIPIO DE GALAPA.",))
    call = SourceCall("contract_values", "activity_values", "jbjy-vk9h",
                      "aggregate", status, 0, 0, "SELECT scoped")
    rows = {"contract_values": [{"contract_count": str(count), "total_contract_value": "0"}]}
    sections = build_sections("overview", scope, rows, {"contract_values": call}, SoQLBuilder())
    assert sections[0].fact("contract_count") is None
    assert sections[1].fact("total_contract_value") is None
    assert sections[4].fact("contract_value") is None


@settings(max_examples=40, derandomize=True)
@given(count=st.integers(min_value=1, max_value=10**18))
def test_contract_count_is_not_rounded_through_float(count):
    from app.core.entity_profile.aspects.values import parse_contract_values
    parsed = parse_contract_values([{"contract_count": str(count), "total_contract_value": "10"}])
    assert parsed.contract_count == count
