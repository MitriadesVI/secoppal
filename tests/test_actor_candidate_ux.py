"""AP3-G visible identities and safe Streamlit candidate controls."""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

from app.core.actor_candidate_ui import actor_candidate_options
from app.core.actor_identity import IdentityKey
from app.core.actor_name import format_actor_identity_for_user, mask_identity_key
from app.core.actor_scope_policy import evaluate_actor_scope


STREAMLIT_APP = Path(__file__).parents[1] / "streamlit_app.py"


def _pending_params() -> dict:
    return {
        "dataset": "contratos",
        "actor_resolution_status": "AMBIGUOUS",
        "actor_resolution_method": "name_ambiguous",
        "actor_pending": {
            "version": 1,
            "resolution_status": "AMBIGUOUS",
            "actor_name": "jorge verbel",
            "selection_token": "opaque-current-pending",
            "candidates": [
                {
                    "rank": 1, "observed_name": "JORGE VERBEL MARTELO",
                    "document_type": "CC", "base_number": "1140830735",
                },
                {
                    "rank": 2, "observed_name": "CODETEC S.A.S.",
                    "document_type": "NIT", "base_number": "900123456",
                },
            ],
        },
    }


def test_user_formatter_shows_full_typed_documents_but_internal_mask_remains():
    cc = IdentityKey("CC", "1140830735")
    nit = IdentityKey("NIT", "900123456")

    assert format_actor_identity_for_user(cc) == "CC 1140830735"
    assert format_actor_identity_for_user(nit) == "NIT 900123456"
    assert mask_identity_key(cc) == "CC:******0735"


def test_unknown_document_is_not_invented_as_cc_or_nit():
    rendered = format_actor_identity_for_user(IdentityKey("UNKNOWN", "123456789"))

    assert rendered == "Documento 123456789"
    assert "CC" not in rendered and "NIT" not in rendered


def test_text_and_ui_candidates_share_full_identity_representation_and_order():
    params = _pending_params()
    text = evaluate_actor_scope(params).clarification_question or ""
    options = actor_candidate_options(params)

    assert [option.rank for option in options] == [1, 2]
    for option in options:
        assert f"{option.rank}. {option.observed_name} — {option.identity_text}" in text
    assert "******" not in text


class _Rerun(Exception):
    pass


class _Capture:
    def __init__(self, click_rank: int | None = None):
        self.click_rank = click_rank
        self.output: list[str] = []
        self.session_state = SimpleNamespace()

    def container(self, **_kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def columns(self, spec):
        return [self for _ in range(len(spec))]

    def markdown(self, value, **_kwargs):
        self.output.append(str(value))

    def button(self, _label, *, key, **_kwargs):
        return self.click_rank is not None and f"_{self.click_rank}_" in key

    def rerun(self):
        raise _Rerun


def _load_streamlit_candidate_renderer(capture: _Capture):
    tree = ast.parse(STREAMLIT_APP.read_text(), filename=str(STREAMLIT_APP))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_render_actor_candidates"
    )
    namespace = {"st": capture}
    exec(
        compile(ast.Module(body=[function], type_ignores=[]), str(STREAMLIT_APP), "exec"),
        namespace,
    )
    return namespace["_render_actor_candidates"]


def test_streamlit_renders_full_identity_and_submits_only_rank_and_token():
    candidates = [option.as_dict() for option in actor_candidate_options(_pending_params())]
    capture = _Capture(click_rank=2)
    renderer = _load_streamlit_candidate_renderer(capture)

    try:
        renderer(candidates, 7)
    except _Rerun:
        pass

    rendered = "\n".join(capture.output)
    assert "CC 1140830735" in rendered
    assert "NIT 900123456" in rendered
    event = capture.session_state._pending_actor_selection
    assert event == {
        "candidate_rank": 2,
        "pending_token": "opaque-current-pending",
    }
    assert "document_type" not in event and "base_number" not in event
