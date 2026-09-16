"""The seam between the flag, the credential and the pure contract.

Off is the default, and everything missing resolves to "no veto", which is the
V1 contract unchanged. Nothing here performs a provider call: the provider is
substituted, which is the point of it living in its own module.
"""
from __future__ import annotations

import json
import pathlib
from datetime import date

import pytest

from app.config import Settings, get_settings
from app.core.capability_binding import supplier_semantics_wiring as wiring
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.capability_binding.supplier_semantics import MentionRef, VetoOutcome
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter

TODAY = date(2026, 9, 15)
QUERY = "proveedores de la alcaldía de Galapa"


@pytest.fixture(scope="module")
def router():
    return QueryRouter(entity_resolver=EntityResolver(pathlib.Path("app/data/aliases_db.json")))


@pytest.fixture
def accredited(router):
    return router.parse(QUERY, today=TODAY).params


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_the_flag_is_off_by_default():
    assert Settings().secoppal_a2_supplier_polarity_sidecar_enabled is False


def test_off_wires_no_veto_at_all(monkeypatch):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: False)
    monkeypatch.setattr(wiring, "classify", _never_called)
    assert wiring.configured_polarity_veto() is None


def test_on_without_a_credential_wires_no_veto(monkeypatch):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)
    monkeypatch.setattr(wiring, "classify", _never_called)
    monkeypatch.setattr(wiring, "get_settings", lambda: _settings(api_key=None))
    assert wiring.configured_polarity_veto() is None


def test_unreadable_settings_wire_no_veto(monkeypatch):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)

    def boom():
        raise RuntimeError("broken .env")

    monkeypatch.setattr(wiring, "get_settings", boom)
    assert wiring.configured_polarity_veto() is None


def test_on_with_a_credential_wires_a_veto_that_blocks_a_negative(monkeypatch, accredited):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)
    monkeypatch.setattr(wiring, "get_settings", lambda: _settings())
    monkeypatch.setattr(wiring, "classify", _answering("NEGATED"))
    veto = wiring.configured_polarity_veto()
    assert veto is not None
    assert decide(QUERY, accredited, polarity_veto=veto).outcome is Outcome.CLARIFY


def test_on_with_a_credential_lets_a_positive_through(monkeypatch, accredited):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)
    monkeypatch.setattr(wiring, "get_settings", lambda: _settings())
    monkeypatch.setattr(wiring, "classify", _answering("POSITIVE"))
    veto = wiring.configured_polarity_veto()
    assert decide(QUERY, accredited, polarity_veto=veto).outcome is Outcome.EXECUTE


def test_a_provider_that_returns_nothing_blocks(monkeypatch, accredited):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)
    monkeypatch.setattr(wiring, "get_settings", lambda: _settings())
    monkeypatch.setattr(wiring, "classify", lambda *a, **k: None)
    decision = decide(QUERY, accredited, polarity_veto=wiring.configured_polarity_veto())
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "POLARITY_SIDECAR_UNAVAILABLE"


def test_a_provider_that_raises_is_contained_by_the_provider_module(monkeypatch):
    """The raise must not escape into the contract; classify returns None."""
    from app.core.capability_binding import supplier_semantics_provider as provider

    class Boom:
        def __init__(self, *a, **k):
            raise TimeoutError("network down")

    monkeypatch.setattr("openai.OpenAI", Boom)
    assert provider.classify(QUERY, (MentionRef("proveedores", 1),),
                             api_key="k", model="m", timeout_seconds=1.0) is None


def test_the_rendered_prompt_carries_the_query_and_every_ref():
    from app.core.capability_binding.supplier_semantics_provider import render_prompt

    refs = (MentionRef("proveedores", 1), MentionRef("proveedores", 2))
    rendered = render_prompt("proveedores, proveedores", refs)
    assert "{{QUERY}}" not in rendered and "{{REFS}}" not in rendered
    assert "proveedores, proveedores" in rendered
    assert 'quote: "proveedores", occurrence: 1' in rendered
    assert 'quote: "proveedores", occurrence: 2' in rendered


def _settings(api_key: str | None = "test-key"):
    class Stub:
        deepseek_api_key = api_key
        deepseek_model = "deepseek-chat"
        secoppal_a2_supplier_polarity_timeout_seconds = 8.0
    return Stub()


def _never_called(*args, **kwargs):  # pragma: no cover - guard
    raise AssertionError("the provider must not be reached")


def _answering(label: str):
    def classify(user_query, refs, quantities=(), **kwargs):
        return {"mentions": [
            {"quote": r.quote, "occurrence": r.occurrence, "polarity": label} for r in refs
        ]}
    return classify
