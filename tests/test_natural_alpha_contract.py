import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from hypothesis import given, strategies as st

from app.config import Settings
from app.core.natural_alpha.admission import eligible, observe
from app.core.natural_alpha.grounding import ground_exact, validate_and_ground
from app.core.natural_alpha.prompt import provider_request
from app.core.natural_alpha.provider import DeepSeekResponsesProvider, ProviderResult


def payload(quote="busca", **overrides):
    return json.dumps({"jobs": [{"action": "SEARCH", "aspect": "OPEN_OPPORTUNITIES",
                                  "evidence_quote": quote, "occurrence": 1, **overrides}]})


def config(**overrides):
    return Settings(_env_file=None, secoppal_natural_alpha_mode="alpha",
                    secoppal_natural_alpha_email="alpha@example.test", **overrides)


IDENTITY = {"email": " ALPHA@EXAMPLE.TEST ", "is_anonymous": False}


@pytest.mark.parametrize("mode", ["off", "typo", "ALPHA", " alpha ", ""])
def test_off_and_invalid_mode_no_calls_or_metadata(mode):
    settings = config()
    settings.secoppal_natural_alpha_mode = mode
    spy = Mock()
    assert observe(settings, "busca", "streamlit", IDENTITY, spy) is None
    spy.observe.assert_not_called()


@pytest.mark.parametrize("channel,identity,auth,email", [
    ("whatsapp", IDENTITY, True, "alpha@example.test"),
    ("streamlit", None, True, "alpha@example.test"),
    ("streamlit", {**IDENTITY, "is_anonymous": True}, True, "alpha@example.test"),
    ("streamlit", {"email": "alpha@example.test"}, True, "alpha@example.test"),
    ("streamlit", {**IDENTITY, "is_anonymous": 0}, True, "alpha@example.test"),
    ("streamlit", {**IDENTITY, "email": "other@example.test"}, True, "alpha@example.test"),
    ("streamlit", IDENTITY, False, "alpha@example.test"),
    ("streamlit", IDENTITY, True, ""),
    ("streamlit", IDENTITY, True, None),
])
def test_non_eligible_is_zero_call(channel, identity, auth, email):
    settings = config(secoppal_auth_enabled=auth)
    settings.secoppal_natural_alpha_email = email
    spy = Mock()
    assert not eligible(settings, channel, identity)
    assert observe(settings, "busca", channel, identity, spy) is None
    spy.observe.assert_not_called()


@pytest.mark.parametrize("mode", ["alpha", "shadow"])
def test_eligible_single_observation(mode):
    settings = config()
    settings.secoppal_natural_alpha_mode = mode
    spy = Mock(observe=Mock(return_value=ProviderResult(payload(), provider_calls=1)))
    result = observe(settings, "busca oportunidades", "streamlit", IDENTITY, spy)
    assert result.status == "MATCHED"
    assert result.metadata()["natural_alpha_jobs"] == [
        {"action": "SEARCH", "aspect": "OPEN_OPPORTUNITIES", "evidence_interval": [0, 5]},
    ]
    spy.observe.assert_called_once()
    assert not result.terminal


@pytest.mark.parametrize("change", [
    {"action": "EXECUTE"}, {"aspect": "SUPPLIERS"}, {"params": {}},
    {"occurrence": True}, {"occurrence": 1.0}, {"occurrence": "1"},
    {"occurrence": 0}, {"occurrence": 2}, {"evidence_quote": "BUSCA"},
    {"evidence_quote": ""}, {"evidence_quote": "buscar"}, {"evidence_quote": None},
])
def test_untrusted_proposal_rejected_whole(change):
    with pytest.raises(ValueError):
        validate_and_ground("busca", payload(**change))


@pytest.mark.parametrize("document", [
    "[]", "null", "{}", '{"jobs": [], "params": {}}',
    '{"jobs": [], "jobs": []}', '{"jobs": "SEARCH"}',
    json.dumps({"jobs": json.loads(payload())["jobs"] * 4}),
])
def test_envelope_and_max_three(document):
    with pytest.raises(ValueError):
        validate_and_ground("busca", document)


def test_empty_and_other_do_not_block():
    settings = config()
    for raw_payload, status in [("{\"jobs\": []}", "SKIPPED"),
                                (payload(action="OTHER", aspect="OTHER"), "MATCHED")]:
        spy = Mock(observe=Mock(return_value=ProviderResult(raw_payload, provider_calls=1)))
        result = observe(settings, "busca", "streamlit", IDENTITY, spy)
        assert result.status == status
        assert not result.terminal


def test_one_bad_job_invalidates_entire_proposal():
    jobs = json.loads(payload())["jobs"] + json.loads(payload("missing"))["jobs"]
    spy = Mock(observe=Mock(return_value=ProviderResult(json.dumps({"jobs": jobs}), provider_calls=1)))
    result = observe(config(), "busca", "streamlit", IDENTITY, spy)
    assert result.status == "INVALID"
    assert not result.jobs


@given(st.text(min_size=1, max_size=30), st.integers(min_value=1, max_value=6))
def test_exact_grounding_preserves_raw_unicode_and_occurrence(quote, occurrence):
    # Enumerate all starts independently, including overlapping appearances.
    raw = quote * occurrence
    expected = [i for i in range(len(raw)) if raw[i:i + len(quote)] == quote]
    start, end = ground_exact(raw, quote, occurrence)
    assert start == expected[occurrence - 1]
    assert raw[start:end] == quote
    with pytest.raises(ValueError):
        ground_exact(raw, quote, len(expected) + 1)


def test_raw_combining_marks_not_normalized():
    assert ground_exact("a\u0301 a\u0301", "a\u0301", 2) == (3, 5)
    with pytest.raises(ValueError):
        ground_exact("a\u0301", "á", 1)


def test_provider_one_request_fixed_contract_and_own_timeout():
    client = Mock()
    client.responses.create.return_value = SimpleNamespace(output_text=payload())
    provider = DeepSeekResponsesProvider(api_key=None, timeout_seconds=2.5, client=client)
    request = provider_request("busca")
    result = provider.observe("turn-1", request)
    assert result.provider_calls == 1
    client.responses.create.assert_called_once_with(**request, timeout=2.5)
    assert request["temperature"] == 0
    assert request["reasoning"] == {"effort": "none"}
    assert request["tools"] == []
    assert request["text"] == {"format": {"type": "json_object"}}


def test_provider_error_is_safe_and_not_retried():
    client = Mock()
    client.responses.create.side_effect = TimeoutError("private secret text")
    provider = DeepSeekResponsesProvider(api_key=None, timeout_seconds=1, client=client)
    result = provider.observe("turn-1", provider_request("busca"))
    assert result == ProviderResult(error="PROVIDER_TIMEOUT", provider_calls=1)
    client.responses.create.assert_called_once()
    assert "private" not in repr(result)


def test_provider_missing_credentials_zero_calls():
    result = DeepSeekResponsesProvider(api_key=None, timeout_seconds=1).observe("k", {})
    assert result == ProviderResult(error="MISSING_API_KEY")
