"""ADR-102 R1: when legacy gives a turn to ACTOR, Foundation never touches it.

The oracle is legacy itself. Every text runs with Foundation OFF and ON. Whenever
the OFF run ends with the ACTOR continuation owner, the ON run must call neither
the Foundation producer nor its basic fallback, must reach the same source calls
and answer, and must not carry a Foundation mark. No corporate-form list lives
here or in the gate: the texts vary the shape, legacy decides the owner.
"""

from __future__ import annotations

import itertools
from collections import Counter

import pytest

from tests import test_foundation_route_ownership as base
from tests.actor_prefix_fixtures import resolver_for

make_flow = base.make_flow  # fixture re-export: pytest resolves it from this module
run, observable = base.run, base.observable

# The eight ZEUS reproductions and the one corpus case (`contratos de O'Brien & Asociados S.A.S`).
ZEUS_CASES = [
    "busca contratos de Fundación Sin Nombre SAS",
    "quiero contratos de Servicios Integrales del Caribe SAS",
    "necesito los contratos de CONSORCIO VIAL DEL NORTE",
    "muéstrame contratos de Ingeniería y Construcciones LTDA",
    "contratos de UNION TEMPORAL AGUAS DEL SUR",
    "quiero contratos de Inversiones Los Andes SAS en 2025",
    "busca contratos de Aseo Total SAS de más de 100 millones",
    "quiero contratos de Fundación Sin Nombre SAS con NIT 900123456-7",
    "contratos de O'Brien & Asociados S.A.S",
]

FORMS = ["{} SAS", "{} S.A.S.", "{} LTDA", "{} LIMITADA", "CONSORCIO {}", "UNIÓN TEMPORAL {}", "UT {}",
         "FUNDACIÓN {}", "CORPORACIÓN {}", "E.S.E. {}", "IPS {}"]
NAMES = ["Aguas del Sur", "Ingeniería y Construcciones", "Los Andes con Socios", "Tecnología Verde"]
TAILS = ["", " en 2025", " de más de 100 millones", " con NIT 900123456-7"]
MATRIX = [f"quiero contratos de {form.format(name)}{tail}"
          for form, name, tail in itertools.product(FORMS, NAMES, TAILS)]

# Shapes whose owner legacy decides: the oracle, not this list, says who keeps them.
CONTROLS = [
    "contratos de fundaciones para atención infantil",
    "contratos de unión temporal de tuberías",
    "contratos de mantenimiento de sociedades comerciales",
    "contratos de Bogotá",
    "contratos de vivienda",
    "contratos de Fundación",
    "busca contratos de aseo",
]

# Actor negation: not an ACTOR turn in legacy; Foundation must not run the name as positive.
NEGATED = [
    "busca contratos que no sean de Aseo Total SAS",
    "quiero contratos excepto los de CONSORCIO VIAL DEL NORTE",
]

ALIAS_ROWS = [("NIT", "900123456", "aseo total sas"), ("NIT", "900222333", "consorcio vial del norte")]


def _pair(make_flow):
    return make_flow("foundation_v3"), make_flow("legacy")


def _assert_actor_keeps_the_turn(on, off, text):
    result_on, result_off = run(on, text, chat_id="own"), run(off, text, chat_id="own")
    if result_off.get("_continuation_owner") != "ACTOR":
        return None  # not an ACTOR turn for legacy: no ownership claim to defend
    assert on.foundation_producer.call_count == 0, text
    assert "foundation_mode" not in result_on and "foundation_telemetry" not in result_on, text
    assert str(result_on.get("route_reason", "")) != "foundation_basic_fallback", text
    assert observable(result_on) == observable(result_off), text
    assert Counter(on.source.calls) == Counter(off.source.calls), text
    return True


@pytest.mark.parametrize("text", ZEUS_CASES)
def test_the_zeus_actor_cases_are_owned_by_actor_in_legacy_and_kept(make_flow, text):
    on, off = _pair(make_flow)
    assert _assert_actor_keeps_the_turn(on, off, text) is True, "legacy no longer owns this as ACTOR"


@pytest.mark.parametrize("text", ZEUS_CASES)
def test_a_provider_outage_never_turns_an_actor_turn_into_a_topic_search(make_flow, text):
    on, off = _pair(make_flow)
    on.foundation_producer.side_effect = RuntimeError("simulated provider outage")
    result_on, result_off = run(on, text, chat_id="own"), run(off, text, chat_id="own")
    assert result_off.get("_continuation_owner") == "ACTOR"
    assert on.foundation_producer.call_count == 0
    assert str(result_on.get("route_reason", "")) != "foundation_basic_fallback"
    assert Counter(on.source.calls) == Counter(off.source.calls)


def test_whenever_legacy_assigns_actor_foundation_is_never_called_over_the_form_matrix(make_flow):
    owned = 0
    for text in MATRIX + CONTROLS:
        on, off = _pair(make_flow)
        owned += bool(_assert_actor_keeps_the_turn(on, off, text))
    assert owned >= len(ZEUS_CASES)  # the matrix really exercised the property


@pytest.mark.parametrize("text", ZEUS_CASES[:3] + MATRIX[::7] + CONTROLS)
def test_the_property_holds_with_a_present_alias_index(make_flow, tmp_path, text):
    on, off = _pair(make_flow)
    for flow in (on, off):
        flow.actor_name_resolver = resolver_for(tmp_path / f"idx-{id(flow)}.sqlite", ALIAS_ROWS)
    _assert_actor_keeps_the_turn(on, off, text)


@pytest.mark.parametrize("text", ["contratos de Aseo Total SAS", "necesito los contratos de CONSORCIO VIAL DEL NORTE"])
def test_an_exact_alias_hit_keeps_the_actor_owner(make_flow, tmp_path, text):
    on, off = _pair(make_flow)
    for flow in (on, off):
        flow.actor_name_resolver = resolver_for(tmp_path / f"exact-{id(flow)}.sqlite", ALIAS_ROWS)
    assert _assert_actor_keeps_the_turn(on, off, text) is True


@pytest.mark.parametrize("text", NEGATED)
def test_a_negated_actor_is_never_run_as_a_positive_filter_by_foundation(make_flow, text):
    # Legacy does not own this as ACTOR (it runs a tabular search); the negation is
    # Foundation's to refuse. The stub producer knows only the simple search, so the
    # turn ends without a source call: the actor name never becomes a positive filter.
    on, _ = _pair(make_flow)
    result = run(on, text, chat_id="neg")
    assert result.get("_continuation_owner") != "ACTOR"
    assert on.source.calls == []
    assert result.get("foundation_terminal") != "READY"


POLICY_NEGATION = "contratos de Aseo Total SAS que no sean de Cali"


def test_the_gate_declines_exactly_when_legacy_gives_the_turn_to_actor(make_flow):
    # A negation legacy must clarify (CLARIFY) stops resolve_actor_params before any name is
    # read, so legacy does not own it as ACTOR; Foundation keeps the claim it always had.
    on, off = _pair(make_flow)
    result_on, result_off = run(on, POLICY_NEGATION, chat_id="pol"), run(off, POLICY_NEGATION, chat_id="pol")
    assert result_off.get("_continuation_owner") != "ACTOR"
    assert on.foundation_producer.call_count == 1
    assert on.source.calls == [] and result_on.get("_continuation_owner") != "ACTOR"


def test_a_parse_that_already_names_the_actor_is_claimed_without_any_lookup():
    from types import SimpleNamespace

    from app.core.foundation_route_owner import actor_claims

    class Exploding:
        def __getattr__(self, name):
            raise AssertionError("no directory lookup for a turn the parse already gives to ACTOR")

    ctx = SimpleNamespace(user_query="contratos de Aseo Total SAS", today=base.TODAY)
    workflow = SimpleNamespace(actor_name_resolver=Exploding())
    for key in ("actor_scope", "actor_pending", "contratista"):
        # method already fixed: the shared resolution has nothing to add, yet ACTOR owns the turn
        params = {key: {"role": "contractor"}, "actor_resolution_method": "explicit_document"}
        assert actor_claims(workflow, ctx, SimpleNamespace(params=params, negation_requests=())) is True


def test_a_thematic_search_is_still_foundations(make_flow):
    on, _ = _pair(make_flow)
    result = run(on, base.SEARCH, chat_id="ok")
    assert on.foundation_producer.call_count == 1 and result["foundation_terminal"] == "READY"
