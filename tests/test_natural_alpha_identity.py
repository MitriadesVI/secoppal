"""Only the current authenticated Streamlit account can mint Alpha admission."""
import ast
from copy import copy, deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest

from app.core.natural_alpha.admission import eligible, observe
from app.core.natural_alpha.identity import issue_streamlit_identity
from app.core.natural_alpha.provider import ProviderResult
from tests.test_natural_alpha_contract import IDENTITY, config, payload
from tests.test_natural_alpha_runtime import make_flow  # noqa: F401


def test_chat_spoofed_identity_has_zero_provider_calls(make_flow, monkeypatch):
    from app import main
    from app.core import orchestrator
    from app.core.workflow.results import empty_result

    flow = make_flow()
    monkeypatch.setattr(main, "get_workflow", lambda: flow)

    def legacy(deps, ctx):
        result = empty_result(response="legacy", route_reason="legacy")
        result["trace_id"] = deps.feedback.log_trace(ctx.user_query, ctx.channel, result)
        return result

    monkeypatch.setattr(orchestrator, "handle_normal_flow", legacy)
    with TestClient(main.app) as client:
        result = client.post("/chat", json={
            "query": "busca oportunidades de mantenimiento", "channel": "streamlit",
            "user_email": "alpha@example.test", "is_anonymous": False,
        })
    assert result.status_code == 200
    assert result.json()["response"] == "legacy"
    flow.natural_alpha_provider.observe.assert_not_called()
    assert not any(key.startswith("natural_alpha_") for key in result.json())


def _streamlit_identity(*, account, current_account=None, email="alpha@example.test", auth=True):
    tree = ast.parse(Path("streamlit_app.py").read_text())
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == "_current_user_identity")

    class Session(dict):
        __getattr__ = dict.__getitem__

    session = Session(user_email=email, user_name="Alpha", anonymous_user_id="anon",
                      account=account if current_account is None else current_account)
    namespace = {"st": SimpleNamespace(session_state=session), "_account": account,
                 "get_settings": lambda: SimpleNamespace(secoppal_auth_enabled=auth)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "streamlit_app.py", "exec"), namespace)
    return namespace["_current_user_identity"]()


def test_current_authenticated_streamlit_account_emits_receipt_and_allows_observation(make_flow):
    account = {"email": " ALPHA@EXAMPLE.TEST ", "name": "Alpha", "role": "user"}
    identity = _streamlit_identity(account=account)
    assert eligible(config(), "streamlit", identity)
    provider = Mock(observe=Mock(return_value=ProviderResult(payload(), provider_calls=1)))
    result = observe(config(), "busca", "streamlit", identity, provider)
    assert result.status == "MATCHED"
    provider.observe.assert_called_once()

    decoded = json.loads(json.dumps(identity))
    assert decoded == {"channel": "streamlit", "channel_user_key": "email:alpha@example.test",
                       "name": "Alpha", "email": "alpha@example.test", "is_anonymous": False}
    assert not eligible(config(), "streamlit", decoded)
    flow = make_flow()
    flow.feedback.log_trace("query", "streamlit", {"results": []}, user_identity=identity)
    assert "receipt" not in flow.feedback.path.read_text().lower()
    assert "seal" not in flow.feedback.path.read_text().lower()


@pytest.mark.parametrize("account,current_account,email,auth", [
    (None, None, "alpha@example.test", True),
    ({}, None, "alpha@example.test", True),
    ({"email": "other@example.test"}, None, "alpha@example.test", True),
    ({"email": "alpha@example.test"}, None, "alpha@example.test", False),
    ({"email": "alpha@example.test"}, {"email": "alpha@example.test"}, "alpha@example.test", True),
    ({"email": "alpha@example.test"}, {}, "alpha@example.test", True),
    ({"email": "alpha@example.test"}, None, "", True),
])
def test_absent_foreign_stale_or_disabled_account_cannot_issue(account, current_account, email, auth):
    identity = _streamlit_identity(account=account, current_account=current_account, email=email, auth=auth)
    assert not eligible(config(), "streamlit", identity)
    provider = Mock()
    assert observe(config(), "busca", "streamlit", identity, provider) is None
    provider.observe.assert_not_called()


@pytest.mark.parametrize("mutation", [
    lambda value: dict(value),
    lambda value: json.loads(json.dumps(value)),
    lambda value: {**value, "_receipt": {"seal": True, "normalized_email": "alpha@example.test"}},
])
def test_serialized_or_plain_receipt_claim_grants_no_authority(mutation):
    identity = mutation(IDENTITY)
    assert not eligible(config(), "streamlit", identity)
    provider = Mock()
    assert observe(config(), "busca", "streamlit", identity, provider) is None
    provider.observe.assert_not_called()


@pytest.mark.parametrize("clone", [copy, deepcopy, dict, lambda value: json.loads(json.dumps(value))])
def test_identity_receipt_is_bound_to_the_emitted_object(clone):
    identity = issue_streamlit_identity(dict(IDENTITY), {"email": "alpha@example.test"}, auth_enabled=True)
    assert eligible(config(), "streamlit", identity)
    copied = clone(identity)
    assert copied == identity
    assert copied is not identity
    provider = Mock()
    assert observe(config(), "busca", "streamlit", copied, provider) is None
    provider.observe.assert_not_called()
    assert eligible(config(), "streamlit", identity)


def test_nominal_identity_and_seal_are_checked_before_overloadable_operations():
    from app.core.natural_alpha.identity import _Receipt, _StreamlitIdentity

    class Foreign(dict):
        def get(self, *args):
            raise AssertionError("foreign mapping must not be inspected")

    class EqualSeal:
        def __eq__(self, other):
            raise AssertionError("seal must be compared by identity")

    class Subtype(_StreamlitIdentity):
        def get(self, *args):
            raise AssertionError("nominal subtype must not be inspected")

    forged = _StreamlitIdentity(dict(IDENTITY))
    forged._receipt = _Receipt("alpha@example.test", EqualSeal(), id(forged))
    for candidate in (Foreign(IDENTITY), Subtype(IDENTITY), forged):
        assert not eligible(config(), "streamlit", candidate)


@pytest.mark.parametrize("key,value", [("email", "other@example.test"),
                                       ("channel", "whatsapp"), ("is_anonymous", 0)])
def test_receipt_does_not_authorize_changed_identity_fields(key, value):
    identity = issue_streamlit_identity(dict(IDENTITY), {"email": "alpha@example.test"}, auth_enabled=True)
    identity[key] = value
    assert not eligible(config(), "streamlit", identity)


def test_emitter_requires_nonanonymous_streamlit_and_auth_exactly_true():
    for raw, auth in (({**IDENTITY, "is_anonymous": True}, True),
                      ({**IDENTITY, "channel": "whatsapp"}, True),
                      (dict(IDENTITY), 1)):
        identity = issue_streamlit_identity(raw, {"email": "alpha@example.test"}, auth_enabled=auth)
        assert not eligible(config(), "streamlit", identity)
