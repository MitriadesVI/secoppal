"""Bound the two deliberate presentation-only public-file repins."""
from pathlib import Path
import hashlib


def test_results_divergence_is_only_optional_presentation_key():
    text = Path("app/core/workflow/results.py").read_text()
    for addition in ('    "entity_profile_view",\n', '    entity_profile_view: dict | None\n'):
        assert text.count(addition) == 1
        text = text.replace(addition, "", 1)
    assert hashlib.sha256(text.encode()).hexdigest() == (
        "b4e869d985dadcb50679a19aafda27d92ead18ef756d4833a0ad9e4100d51057"
    )


def test_streamlit_divergence_is_only_profile_history_and_render_hook():
    text = Path("streamlit_app.py").read_text()
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
    assert hashlib.sha256(text.encode()).hexdigest() == (
        "807b0e91cfda5351a5e271722d551bec7146213ccb80bf1362f638b05a1a78f6"
    )
