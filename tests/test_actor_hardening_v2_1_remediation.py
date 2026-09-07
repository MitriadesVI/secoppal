"""Counterexamples from the independent review of Actor Dogfood Hardening V2.

The V2 block was adjudicated NO_GO. Three P1 and one P2 survived its own tests
because each of them checked the shape the fix happened to produce rather than
the contract the fix promised:

* **P1-A** — the coherence guard promised that no incoherent frame reaches
  SECOP, but only inspected ``list``/``tuple``. ``SoQLBuilder`` also accepts a
  scalar, so a contradictory scalar passed both doors.
* **P1-B** — span ownership was decided by asking whether the matched alias
  appears *lexically* in the actor name. That cannot tell two occurrences of
  the same word apart, so an explicitly requested region was deleted.
* **P1-C** — the asserted-name door authorised consumption with a function
  that only ever offered *name-shape* evidence, so an acronym let a topic be
  swallowed and a resolved buyer disappear.
* **P2-B** — Entity Profile purges ``region``/``departamentos_in`` and leaves
  ``region_resolution`` behind, so the cluster claim was not true.

Ported minimal from ``audit/actor-dogfood-hardening-v2-independent``; the
evidence files stay in the audit branch.
"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.core.actor_identity import IdentityKey
from app.core.actor_identity_coherence import actor_identity_split_brain
from app.core.actor_resolution import apply_actor_identity, document_variants
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import execute_resolved_params
from tests.test_actor_dogfood_hardening_v2 import (  # noqa: F401
    CARIBE,
    LLANOS,
    no_network,
    workflow,
)


CC_A = "70000011"
CC_B = "70000012"
NIT_A = "900123456"


def _topic(params) -> str:
    return str(params.get("objeto"))


def _asserted(params):
    return (params.get("actor_scope") or {}).get("asserted_name")


# ══════════════════════════════════════════════════════════════════════════
# P1-A — the guard must cover every representation an executable door accepts
# ══════════════════════════════════════════════════════════════════════════

def _contradictory_frame(shape):
    params = apply_actor_identity(
        {"dataset": "contratos"}, IdentityKey("CC", CC_A), method="explicit_document",
    )
    params["contratista"] = CC_B if shape is str else shape([CC_B])
    return params


def _doors(params):
    """Both doors to SoQL, instrumented: the planner and the direct executor."""
    builder = Mock(wraps=SoQLBuilder())
    client = Mock()
    client.query.return_value = []
    formatter = Mock()
    formatter.format_for_channel.return_value = ("", [])
    deps = SimpleNamespace(soql_builder=builder, secop_client=client, formatter=formatter)
    plan = compute_query_plan(params, "contratos del actor")
    direct = execute_resolved_params(deps, params)
    return plan, direct, builder.build.call_count, client.query.call_count


def test_scalar_contradiction_reaches_no_source_call():
    params = _contradictory_frame(str)
    assert actor_identity_split_brain(params)
    plan, direct, builds, calls = _doors(params)
    assert not plan.should_execute
    assert direct is None
    assert builds == calls == 0


@pytest.mark.parametrize("shape", [list, tuple])
def test_sequence_contradiction_already_reaches_no_source_call(shape):
    """The V2 behaviour that must not regress."""
    plan, direct, builds, calls = _doors(_contradictory_frame(shape))
    assert not plan.should_execute
    assert direct is None
    assert builds == calls == 0


# A coherent scalar passes today only because the guard ignores str entirely.
# After the fix it must still pass, now because the projection agrees.
def test_coherent_scalar_is_not_a_split_brain():
    params = apply_actor_identity({}, IdentityKey("CC", CC_A), method="explicit_document")
    params["contratista"] = CC_A
    assert not actor_identity_split_brain(params)


@pytest.mark.parametrize("subset", [False, True])
def test_nit_projection_and_foreign_variant(subset):
    identity = IdentityKey("NIT", NIT_A)
    params = apply_actor_identity({}, identity, method="explicit_document")
    variants = document_variants(identity)
    params["contratista"] = variants[:1] if subset else variants
    assert not actor_identity_split_brain(params)
    params["contratista"] = [*params["contratista"], CC_B]
    assert actor_identity_split_brain(params)


@pytest.mark.parametrize("index", [0, 1, 2])
def test_every_nit_variant_passes_as_a_scalar(index):
    identity = IdentityKey("NIT", NIT_A)
    params = apply_actor_identity({}, identity, method="explicit_document")
    params["contratista"] = document_variants(identity)[index]
    assert not actor_identity_split_brain(params)


# ══════════════════════════════════════════════════════════════════════════
# P1-B — ownership belongs to the occurrence, not to the word
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("query,expected", [
    ("contratos del proveedor ana maria llanos", None),
    ("contratos del proveedor ana maria llanos en Meta", None),
    ("contratos del proveedor ana maria llanos en los llanos", "Orinoquia"),
    ("contratos del proveedor carlos rueda caribe", None),
    ("contratos del proveedor carlos rueda caribe en Caribe", "Caribe"),
])
def test_exact_actor_owns_only_its_own_occurrence(workflow, query, expected):
    params = workflow.run_query(query, chat_id="occ")["resolved_params"]
    assert params.get("actor_resolution_status") == "RESOLVED"
    assert params.get("region") == expected
    if expected is None:
        assert "region_resolution" not in params


def test_repeated_region_survives_a_fuzzy_candidate_selection(workflow):
    pending = workflow.run_query(
        "contratos del proveedor ana mariia llanos en los llanos", chat_id="fz",
    )["resolved_params"]
    assert pending.get("actor_pending")
    assert pending.get("region") == "Orinoquia"

    before = len(workflow.source_calls)
    selected = workflow.run_query("1", chat_id="fz")["resolved_params"]
    assert selected.get("actor_resolution_status") == "RESOLVED"
    assert len(workflow.source_calls) > before
    assert selected.get("region") == "Orinoquia"


def test_a_bare_alias_repeated_inside_and_outside_the_span(workflow):
    """``llanos`` twice: once as the surname, once as the territory asked for.

    This already holds, and it is the control that keeps the occurrence fix
    honest: a lexical rule that merely got luckier would break it.
    """
    params = workflow.run_query(
        "contratos del proveedor ana maria llanos en llanos", chat_id="bare",
    )["resolved_params"]
    assert params["contratista"] == [LLANOS[1]]
    assert params.get("region") == "Orinoquia"


def test_organization_tail_repeated_outside_the_span(workflow):
    params = workflow.run_query(
        "contratos del proveedor carlos rueda caribe en Caribe", chat_id="org",
    )["resolved_params"]
    assert params["contratista"] == [CARIBE[1]]
    assert params.get("region") == "Caribe"


# ══════════════════════════════════════════════════════════════════════════
# P2-B — the region cluster mutates transactionally, evidence included
# ══════════════════════════════════════════════════════════════════════════

def test_profile_projection_purges_region_evidence():
    from app.core.entity_profile.turn import profile_query_params

    projected = profile_query_params({
        "region": "Orinoquia",
        "departamentos_in": ["Meta"],
        "region_resolution": {"matched_alias": "llanos", "value": "Orinoquia"},
    })
    assert "region" not in projected
    assert "departamentos_in" not in projected
    assert "region_resolution" not in projected


def test_region_evidence_never_outlives_its_region(workflow):
    """Whatever mutates the territory must carry the evidence with it."""
    turns = ["contratos de beatriz solano perez en los llanos", "ahora en Meta",
             "ahora en la region Caribe", "reset", "contratos de mantenimiento"]
    for query in turns:
        params = workflow.run_query(query, chat_id="life")["resolved_params"] or {}
        if params.get("region_resolution"):
            assert params.get("region") == params["region_resolution"]["value"]
            assert params.get("departamentos_in")


# ══════════════════════════════════════════════════════════════════════════
# P1-C — the asserted-name door may not swallow an independent dimension
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("surface", [
    f"busca WILSON HERRERA — CC {CC_A}",
    f"WILSON HERRERA — CC {CC_A}",
    f"ahora WILSON HERRERA — CC {CC_A}",
    f"muestra ORGANIZACION EJEMPLO — NIT {NIT_A}",
])
def test_positive_surfaces_still_consume_the_name(surface):
    params = QueryRouter().parse(surface).params
    assert params.get("contratista")
    assert _asserted(params)
    assert not params.get("objeto")


@pytest.mark.xfail(strict=True, reason="P1-C — an acronym lets a topic be swallowed")
def test_a_topic_beside_a_document_is_never_consumed_as_a_name():
    params = QueryRouter().parse(f"busca mantenimiento referencia XYZ — CC {CC_A}").params
    assert params["contratista"] == [CC_A]
    assert "mantenimiento" in _topic(params)
    assert not _asserted(params)


@pytest.mark.xfail(strict=True, reason="P1-C — a resolved buyer is erased by the name door")
def test_a_resolved_buyer_beside_a_document_is_never_consumed_as_a_name():
    """BASE resolves this buyer; V2 replaced it with an asserted name."""
    params = QueryRouter().parse(f"busca LA PREVISORA S.A. — NIT {NIT_A}").params
    assert params["contratista"][0] == NIT_A
    assert params.get("entidad_resolved")
    assert not _asserted(params)


@pytest.mark.parametrize("surface,key", [
    (f"busca mantenimiento vial — CC {CC_A}", "objeto"),
    (f"busca procesos BEATRIZ SOLANO — CC {CC_A}", "objeto"),
    (f"busca BEATRIZ SOLANO 2025 — CC {CC_A}", "fecha_desde"),
    (f"busca BEATRIZ SOLANO por 100 millones — NIT {NIT_A}", "valor_min"),
])
def test_recognized_dimensions_beside_a_document_already_survive(surface, key):
    """The V2 controls that must not regress."""
    params = QueryRouter().parse(surface).params
    assert params.get(key), params


@pytest.mark.parametrize("text,expected_name", [
    # INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION. A zone alias is an
    # ordinary phrase, and DE is the connector of half the registered names in
    # the country. Introducing the observed alias with DE truncated this one.
    ("contratos del proveedor CORPORACION DE LA COSTA", "corporacion de la costa"),
    ("contratos del proveedor FUNDACION DE LOS LLANOS", "fundacion de los llanos"),
])
def test_a_zone_alias_after_de_stays_inside_the_name(text, expected_name):
    params = QueryRouter().parse(text).params
    assert params["actor_scope"]["observed_name"] == expected_name


@pytest.mark.parametrize("text", [
    "contratos del proveedor ana maria llanos en los llanos",
    "contratos del proveedor ana maria llanos para los llanos",
])
def test_a_zone_alias_after_en_or_para_is_an_external_modifier(text):
    params = QueryRouter().parse(text).params
    assert params["actor_scope"]["observed_name"] == "ana maria llanos"
    assert params.get("region") == "Orinoquia"


def test_every_territorial_cluster_inventory_carries_its_provenance():
    """The next omission of region_resolution fails here, not in production.

    Four modules keep their own list of the territorial cluster. A region
    filter and its provenance must appear or disappear together, so any
    inventory naming ``region`` must also name ``region_resolution``.
    """
    from app.core.entity_profile.turn import _GEOGRAPHY_PARAM_KEYS
    from app.core.followup_mutations import SCOPE_FIELDS
    from app.core.query_frame import _SCOPE_KEYS
    from app.core.workflow.actions.actor_resolution import _BOUNDARY_MODIFIER_KEYS

    for name, inventory in {
        "entity_profile._GEOGRAPHY_PARAM_KEYS": _GEOGRAPHY_PARAM_KEYS,
        "followup_mutations.SCOPE_FIELDS": SCOPE_FIELDS,
        "query_frame._SCOPE_KEYS": _SCOPE_KEYS,
        "actor_resolution._BOUNDARY_MODIFIER_KEYS": _BOUNDARY_MODIFIER_KEYS,
    }.items():
        assert "region" in inventory, name
        assert "region_resolution" in inventory, f"{name} drops region provenance"
