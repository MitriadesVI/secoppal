"""INV-ACTOR-MENTION-ATOMICITY under the contracted-action intent.

Contract Universe V1.1 added an intent pattern for "(que) ha contratado" so
that the purchase action selects contracts even with the Actor flag off. The
pattern's semantics are right; its text ownership was not. It consumed its own
marker before the actor doors ran, and those doors match an ATOMIC span that
includes the marker — "que ha contratado ASILO DE SAN ANTONIO" is one span, not
a marker plus a leftover. The unmatched replace left the name in ``objeto`` and
in reach of the buyer gazetteer, which resolved "SAN ANTONIO" to a hospital.

The fix defers the marker's scrub until after the actor doors have claimed
their span, next to the command-framing rule that already works this way.
Adjudicated in docs/audits/contract_universe_v1_1_final_closeout/.
"""
from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter


ASILO = "890.102.896-1"
CASES = [
    f"qué ha contratado ASILO DE SAN ANTONIO con NIT {ASILO}",
    f"que ha contratado ASILO DE SAN ANTONIO NIT {ASILO}",
    "qué ha contratado FUNSOCOM",
    "que ha contratado FUNDACARIBE",
]


@pytest.fixture
def flag_off(monkeypatch):
    for module in ("app.core.query_router", "app.core.query_plan"):
        monkeypatch.setattr(f"{module}.actor_profile_enabled", lambda: False)
    yield


def parse(text: str) -> dict:
    return dict(QueryRouter().parse(text).params)


@pytest.mark.parametrize("text", CASES)
def test_the_contracted_action_selects_contracts_with_the_flag_on(text):
    assert parse(text)["dataset"] == "contratos"


@pytest.mark.parametrize("text", CASES)
def test_the_contracted_action_selects_contracts_with_the_flag_off(text, flag_off):
    """The pattern's whole purpose: the dataset must not depend on the flag."""
    assert parse(text)["dataset"] == "contratos"


@pytest.mark.parametrize("text", CASES)
def test_the_marker_itself_never_becomes_a_topic(text, flag_off):
    """Whatever the flag says about the name, 'contratado' is never a subject."""
    assert "contratado" not in (parse(text).get("objeto") or [])


@pytest.mark.parametrize("text", CASES)
def test_an_accredited_actor_owns_its_whole_span(text):
    params = parse(text)
    assert params.get("actor_resolution") in {"explicit_document", "name_unresolved"}
    assert params.get("objeto") is None
    assert params.get("entidad") is None
    assert params.get("entidad_resolved") is None
    assert params.get("departamento_resolved") is None
    assert params.get("ciudad") is None


@pytest.mark.parametrize("text", CASES[:2])
def test_the_document_stays_the_execution_authority(text):
    """The name is context; the document filters. Never a buyer, never a topic."""
    params = parse(text)
    assert params["actor_resolution"] == "explicit_document"
    assert params["actor_scope"]["asserted_name"] == "asilo de san antonio"
    assert params["actor_scope"]["base_number"] == "890102896"
    assert params["contratista"] == ["890102896", "8901028961", "890102896-1"]
    # The regression that motivated this module: half the actor's name was
    # resolving to a different, real buyer.
    assert "HOSPITAL SAN ANTONIO" not in str(params.get("entidad_resolved"))


@pytest.mark.parametrize("text", [
    f"contratos del proveedor ASILO DE SAN ANTONIO NIT {ASILO}",
    f"historial contractual de ASILO DE SAN ANTONIO NIT {ASILO}",
])
def test_the_sibling_constructions_are_unchanged(text):
    params = parse(text)
    assert params["actor_resolution"] == "explicit_document"
    assert params.get("objeto") is None
    assert params.get("entidad_resolved") is None


def test_a_legitimate_buyer_under_the_same_marker_is_still_a_buyer():
    """The marker does not turn every following name into an Actor."""
    params = parse("qué ha contratado la alcaldia de barranquilla")
    assert params["dataset"] == "contratos"
    assert params.get("actor_resolution") is None
    assert params.get("objeto") is None
    assert params["entidad_resolved"] == (
        "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
    )
