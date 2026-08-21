#!/usr/bin/env python3
"""Run the bounded AP3-G actor-candidate UX mutation campaign."""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Mutation:
    identifier: str
    invariant: str
    path: str
    original: str
    mutated: str
    tests: tuple[str, ...]


MUTATIONS = (
    Mutation(
        "M01", "CC becomes non-complete in actor UI", "app/core/actor_name.py",
        '    if identity.document_type in {"NIT", "CC"}:\n',
        '    if identity.document_type == "NIT":\n',
        ("tests/test_actor_candidate_ux.py::test_user_formatter_shows_full_typed_documents_but_internal_mask_remains",),
    ),
    Mutation(
        "M02", "NIT becomes non-complete in actor UI", "app/core/actor_name.py",
        '    if identity.document_type in {"NIT", "CC"}:\n',
        '    if identity.document_type == "CC":\n',
        ("tests/test_actor_candidate_ux.py::test_user_formatter_shows_full_typed_documents_but_internal_mask_remains",),
    ),
    Mutation(
        "M03", "click selects a different rank", "streamlit_app.py",
        '                    "candidate_rank": rank,\n',
        '                    "candidate_rank": rank + 1,\n',
        ("tests/test_actor_candidate_ux.py::test_streamlit_renders_full_identity_and_submits_only_rank_and_token",),
    ),
    Mutation(
        "M04", "click provenance becomes exact_unique_alias",
        "app/core/workflow/handlers/actor_selection.py",
        "            method=METHOD_USER_SELECTED_CANDIDATE,\n",
        '            method="exact_unique_alias",\n',
        ("tests/test_actor_conversation.py::test_click_and_text_selection_are_metamorphically_equivalent",),
    ),
    Mutation(
        "M05", "click loses year", "app/core/workflow/handlers/actor_selection.py",
        '    selected_params["actor_selection_rank"] = selected_rank\n',
        '    selected_params.pop("fecha_desde", None)\n    selected_params["actor_selection_rank"] = selected_rank\n',
        ("tests/test_actor_conversation.py::test_click_selects_exact_or_fuzzy_candidate_with_all_context",),
    ),
    Mutation(
        "M06", "click loses buyer", "app/core/workflow/handlers/actor_selection.py",
        '    selected_params["actor_selection_rank"] = selected_rank\n',
        '    selected_params.pop("entidad_resolved", None)\n    selected_params["actor_selection_rank"] = selected_rank\n',
        ("tests/test_actor_conversation.py::test_click_selects_exact_or_fuzzy_candidate_with_all_context",),
    ),
    Mutation(
        "M07", "click loses territory", "app/core/workflow/handlers/actor_selection.py",
        '    selected_params["actor_selection_rank"] = selected_rank\n',
        '    selected_params.pop("departamento_resolved", None)\n    selected_params["actor_selection_rank"] = selected_rank\n',
        ("tests/test_actor_conversation.py::test_click_selects_exact_or_fuzzy_candidate_with_all_context",),
    ),
    Mutation(
        "M08", "click loses value and ordering", "app/core/workflow/handlers/actor_selection.py",
        '    selected_params["actor_selection_rank"] = selected_rank\n',
        '    selected_params.pop("valor_min", None)\n    selected_params.pop("ordering_signal", None)\n    selected_params["actor_selection_rank"] = selected_rank\n',
        ("tests/test_actor_conversation.py::test_click_selects_exact_or_fuzzy_candidate_with_all_context",),
    ),
    Mutation(
        "M09", "UI sends identity material", "streamlit_app.py",
        '                    "candidate_rank": rank,\n                    "pending_token": token,\n',
        '                    "candidate_rank": rank,\n                    "pending_token": token,\n                    "base_number": candidate.get("base_number"),\n',
        ("tests/test_actor_candidate_ux.py::test_streamlit_renders_full_identity_and_submits_only_rank_and_token",),
    ),
    Mutation(
        "M10", "stale token is accepted", "app/core/workflow/handlers/actor_selection.py",
        "        if submitted_token != current_token:\n",
        "        if False and submitted_token != current_token:\n",
        ("tests/test_actor_conversation.py::test_click_token_cannot_inject_or_select_from_stale_pending_state",),
    ),
    Mutation(
        "M11", "click bypasses authoritative resolution status",
        "app/core/workflow/handlers/actor_selection.py",
        '    selected_params["actor_selection_rank"] = selected_rank\n',
        '    selected_params["actor_resolution_status"] = "CANDIDATES"\n    selected_params["actor_selection_rank"] = selected_rank\n',
        ("tests/test_actor_conversation.py::test_click_and_text_selection_are_metamorphically_equivalent",),
    ),
    Mutation(
        "M12", "duplicate callback falls through and executes again",
        "app/core/workflow/handlers/actor_selection.py",
        '    pending = last_turn.parsed_params.get("actor_pending")\n    if not isinstance(pending, dict):\n        return _stale_selection_result() if structured is not None else None\n',
        '    pending = last_turn.parsed_params.get("actor_pending")\n    if not isinstance(pending, dict):\n        return None\n',
        ("tests/test_actor_conversation.py::test_click_after_reset_and_duplicate_click_fail_closed",),
    ),
    Mutation(
        "M13", "Streamlit reverses canonical candidate order",
        "app/core/actor_candidate_ui.py",
        "    for candidate in candidates[:10]:\n",
        "    for candidate in reversed(candidates[:10]):\n",
        ("tests/test_actor_candidate_ux.py::test_text_and_ui_candidates_share_full_identity_representation_and_order",),
    ),
    Mutation(
        "M14", "UNKNOWN is labelled CC", "app/core/actor_name.py",
        '    return f"Documento {identity.base_number}"\n',
        '    return f"CC {identity.base_number}"\n',
        ("tests/test_actor_candidate_ux.py::test_unknown_document_is_not_invented_as_cc_or_nit",),
    ),
    Mutation(
        "M15", "internal CC masking is removed", "app/core/actor_name.py",
        "    tail = identity.base_number[-4:]\n",
        '    return f"CC:{identity.base_number}"\n    tail = identity.base_number[-4:]\n',
        ("tests/test_actor_candidate_ux.py::test_user_formatter_shows_full_typed_documents_but_internal_mask_remains",),
    ),
)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    initial = {mutation.path: _hash(ROOT / mutation.path) for mutation in MUTATIONS}
    detected = 0
    for mutation in MUTATIONS:
        path = ROOT / mutation.path
        before = path.read_bytes()
        source = before.decode("utf-8")
        if source.count(mutation.original) != 1:
            print(f"{mutation.identifier} SETUP_ERROR: {mutation.invariant}")
            continue
        environment = os.environ.copy()
        environment["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
        try:
            path.write_text(
                source.replace(mutation.original, mutation.mutated, 1),
                encoding="utf-8",
            )
            process = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", *mutation.tests],
                cwd=ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        finally:
            path.write_bytes(before)
        caught = process.returncode != 0
        detected += int(caught)
        print(
            f"{mutation.identifier} {'DETECTED' if caught else 'SURVIVED'}: "
            f"{mutation.invariant}"
        )
        if not caught:
            print(process.stdout.strip())

    restored = all(_hash(ROOT / path) == digest for path, digest in initial.items())
    print(f"MUTATIONS: TOTAL {len(MUTATIONS)} · DETECTED {detected}")
    print(f"RESTORED_BYTE_FOR_BYTE: {'YES' if restored else 'NO'}")
    return 0 if detected == len(MUTATIONS) and restored else 1


if __name__ == "__main__":
    raise SystemExit(main())
