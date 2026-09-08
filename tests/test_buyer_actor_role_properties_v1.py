"""Deterministic role invariance under changes to synthetic alias data."""
from copy import deepcopy
from datetime import date
from tempfile import TemporaryDirectory
from pathlib import Path

import pytest
from burr.core import State
from hypothesis import given, settings, strategies as st

from app.core.buyer_actor_role import buyer_owns_complete_mention
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.data.actor_alias_resolver import ActorNameResolver
from tests.test_buyer_actor_role_arbitration_v1 import SEEDS, directory, row


@settings(max_examples=40, derandomize=True, deadline=None)
@given(
    aliases=st.lists(st.tuples(st.sampled_from(["CC", "NIT"]), st.integers(1, 10000)), max_size=6),
    reverse=st.booleans(), unrelated=st.integers(0, 4),
)
def test_alias_data_cannot_change_buyer_role_or_sql(aliases, reverse, unrelated):
    router, builder = QueryRouter(), SoQLBuilder()
    for query, mention, buyer, amount in SEEDS:
        initial = dict(router.parse(query, today=date(2026, 9, 8)).params)
        expected_sql = builder.build("jbjy-vk9h", initial)
        rows = [row(mention, str(70000001 + n), kind, frequency)
                for n, (kind, frequency) in enumerate(aliases)]
        rows.extend(row(f"SYNTHETIC UNRELATED {n}", str(80000001 + n)) for n in range(unrelated))
        if reverse:
            rows.reverse()
        with TemporaryDirectory(prefix="buyer-role-property-") as temporary:
            index = directory(Path(temporary) / "actors.sqlite", rows)
            resolver = ActorNameResolver(index)
            resolved = resolve_actor_name(State({"parsed_params": initial, "raw_query": query}), resolver)["parsed_params"]
            assert dict(resolved) == initial
            assert builder.build("jbjy-vk9h", resolved) == expected_sql
            assert resolved["entidad_resolved"] == buyer
            assert resolved["valor_min"] == amount
            assert not resolved.get("actor_scope")
            # Explicit provider role remains stable, but identity is allowed
            # to vary between unresolved, unique and ambiguous with the data.
            provider_query = "contratos del proveedor " + mention
            provider = dict(router.parse(provider_query).params)
            final = resolve_actor_name(State({"parsed_params": provider, "raw_query": provider_query}), resolver)["parsed_params"]
            assert final.get("actor_scope", {}).get("role") == "contractor"
            if len(aliases) == 1:
                assert final["actor_resolution_status"] == "RESOLVED"
            elif len(aliases) > 1:
                assert final["actor_resolution_status"] == "AMBIGUOUS"
                assert final.get("actor_pending")


@pytest.mark.parametrize("damage", ["fuzzy", "city", "unconfirmed", "value", "kind", "missing"])
def test_only_confirmed_buyer_provenance_defeats_probe(damage):
    query, mention, _, _ = SEEDS[0]
    params = deepcopy(QueryRouter().parse(query).params)
    assert buyer_owns_complete_mention(mention, params)
    resolution = params["entidad_resolution"]
    if damage == "fuzzy":
        resolution["metadata"]["resolution_method"] = "fuzzy(99)"
    elif damage == "city":
        resolution["method"] = "city_to_entity"
    elif damage == "unconfirmed":
        resolution["clarification_needed"] = True
    elif damage == "value":
        resolution["value"] = "OTHER BUYER"
    elif damage == "kind":
        resolution["kind"] = "territory"
    else:
        params.pop("entidad_resolution")
    assert not buyer_owns_complete_mention(mention, params)


@pytest.mark.parametrize("mention", ["FUNDACARIBE", "cartagena", "alcaldia de cartagena servicios integrados", "alcaldia"])
def test_another_or_partial_mention_cannot_borrow_buyer_identity(mention):
    params = QueryRouter().parse(SEEDS[0][0]).params
    assert not buyer_owns_complete_mention(mention, params)
