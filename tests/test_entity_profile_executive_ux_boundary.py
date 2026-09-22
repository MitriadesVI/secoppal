"""Bound the explicitly authorized profile and Alpha UI additions by exact bytes."""
from pathlib import Path
import hashlib

import pytest


def _pre_alpha_streamlit(text: str) -> str:
    additions = (
        "    from app.core.natural_alpha.presentation import project\n\n"
        "    alpha_view = project(result)\n",
        '        **({"natural_alpha": alpha_view} if alpha_view else {}),\n',
        "    from app.core.natural_alpha.presentation import render\n\n"
        '    render(st, item.get("natural_alpha"), settings=get_settings(), identity=_current_user_identity())\n',
        "        from app.core.natural_alpha.identity import issue_streamlit_identity\n\n"
        '        account = globals().get("_account")\n'
        '        if account is None or account is not st.session_state.get("account"):\n'
        "            return identity\n"
        "        return issue_streamlit_identity(\n"
        "            identity, account, auth_enabled=get_settings().secoppal_auth_enabled,\n"
        "        )\n",
        # DOGFOOD-001 C: the trace header said "LLM: No" on a turn Natural Alpha
        # decided, and said nothing about which profile job had which status.
        # Both additions are read-only projections of fields the result already
        # carries; test_dogfood_001_execution_budget covers what they display.
        '        "natural_alpha_status": result.get("natural_alpha_status", ""),\n'
        '        "profile_job_receipts": result.get("profile_job_receipts", []),\n',
        '        alpha_status = debug.get("natural_alpha_status", "")\n'
        "        st.markdown(\n"
        "            f\"**Natural Alpha:** {'Si' if alpha_status else 'No'}\"\n"
        '            + (f" (`{alpha_status}`)" if alpha_status else "")\n'
        '            + " — el indicador **LLM** de arriba es el fallback legacy de parseo."\n'
        "        )\n"
        '        job_receipts = debug.get("profile_job_receipts") or []\n'
        "        if job_receipts:\n"
        '            st.markdown("**Jobs del perfil (intentado / estado / filas / ms):**")\n'
        "            st.dataframe(job_receipts, use_container_width=True)\n\n",
    )
    for addition in additions:
        assert text.count(addition) == 1
        text = text.replace(addition, "", 1)
    binding = '        identity = {\n            "channel": "streamlit",\n'
    assert text.count(binding) == 1
    text = text.replace(binding, '        return {\n            "channel": "streamlit",\n', 1)
    # The full pre-Alpha file at 4bde6992, before stripping the profile hooks.
    # This also bounds edits inside functions the historical guard removes.
    assert hashlib.sha256(text.encode()).hexdigest() == (
        "84ce0fbc5e25f5c70f7e67297051f19c5107d50105c465d31217070803db9b53"
    )
    return text


@pytest.mark.parametrize("original,replacement", [
    ("    alpha_view = project(result)\n", "    alpha_view = project({})\n"),
    ('        **({"natural_alpha": alpha_view} if alpha_view else {}),\n',
     '        **({"natural_alpha": alpha_view} if alpha_view else {}),\n' * 2),
    ('    view = item.get("entity_profile_view")\n',
     '    st.write("unapproved")\n    view = item.get("entity_profile_view")\n'),
    ('        if account is None or account is not st.session_state.get("account"):\n',
     '        if False:\n'),
])
def test_alpha_inverse_byte_guard_rejects_unapproved_ui_changes(original, replacement):
    text = Path("streamlit_app.py").read_text()
    assert text.count(original) == 1
    with pytest.raises(AssertionError):
        _pre_alpha_streamlit(text.replace(original, replacement, 1))


def test_results_divergence_is_only_optional_presentation_key():
    text = Path("app/core/workflow/results.py").read_text()
    for addition in ('    "entity_profile_view",\n', '    entity_profile_view: dict | None\n'):
        assert text.count(addition) == 1
        text = text.replace(addition, "", 1)
    assert hashlib.sha256(text.encode()).hexdigest() == (
        "b4e869d985dadcb50679a19aafda27d92ead18ef756d4833a0ad9e4100d51057"
    )


def test_streamlit_divergence_is_only_profile_history_and_render_hook():
    text = _pre_alpha_streamlit(Path("streamlit_app.py").read_text())
    additions = (
        "from app.entity_profile_ui import (\n    render_entity_profile_content,\n)\n",
        '        "entity_profile_view": result.get("entity_profile_view"),\n',
        '        "profile_action_ref": _profile_action_ref(result),\n',
        "profile_action_event: dict | None = None\n",
    )
    for addition in additions:
        assert text.count(addition) == 1
        text = text.replace(addition, "", 1)
    start = text.index("def _profile_action_ref(")
    end = text.index("def _assistant_history_item(", start)
    text = text[:start] + text[end:]
    pending = '''if getattr(st.session_state, "_pending_profile_action", None):
    profile_action_event = st.session_state._pending_profile_action
    del st.session_state._pending_profile_action

'''
    assert text.count(pending) == 1
    text = text.replace(pending, "", 1)
    start = text.index("if profile_action_event:")
    end = text.index("if incoming_query:", start)
    text = text[:start] + text[end:]
    start = text.index("def _render_assistant_content(")
    end = text.index("def _render_excel_export(", start)
    text = text[:start] + text[end:]
    hook = "            _render_assistant_content(item)\n"
    assert text.count(hook) == 1
    text = text.replace(hook, '            st.markdown(item["content"])\n', 1)
    assert hashlib.sha256(text.encode()).hexdigest() == (
        "5d82284c3489ea57f1440248e32d1352c58e4314fcba94e8092dda2f906e879d"
    )


def test_normal_flow_divergence_is_only_view_transport():
    text = Path("app/core/workflow/handlers/normal_flow.py").read_text()
    addition = '        "entity_profile_view": state.get("entity_profile_view"),\n'
    assert text.count(addition) == 1
    text = text.replace(addition, "", 1)
    # ANALYTIC_CONTROL_UPSTREAM_V1: the declared limitation block, and nothing else.
    limitation = (
        "    # INV-ANALYTIC-CONTROL-HAS-OWNER-OR-LIMITATION: a count or breakdown the\n"
        "    # serving capability did not perform is declared, never silently dropped.\n"
        '    limitation = state.get("analytic_limitation") or ""\n'
        '    if limitation and not result.get("needs_clarification"):\n'
        '        result["response"] = limitation + result.get("response", "")\n'
        "\n"
    )
    assert text.count(limitation) == 1
    text = text.replace(limitation, "", 1)
    assert hashlib.sha256(text.encode()).hexdigest() == (
        "807b0e91cfda5351a5e271722d551bec7146213ccb80bf1362f638b05a1a78f6"
    )
