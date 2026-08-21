#!/usr/bin/env python3
"""Run the AP3-F anti-invariant mutation campaign.

Each mutant changes one policy decision, runs the smallest defending pytest
slice in a fresh interpreter, and restores the source file byte-for-byte in a
``finally`` block.  A mutant is detected only when that defending slice fails.

This is intentionally a bounded, deterministic campaign rather than a general
mutation fuzzer: the 15 mutations correspond to the conversational invariants
introduced or hardened by AP3-F.
"""
from __future__ import annotations

import argparse
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


MUTATIONS: tuple[Mutation, ...] = (
    Mutation(
        "M01", "natural numeric selection grammar", "app/core/actor_conversation.py",
        'r"^(?:(?:el|la)\\s+|opci[oó]n\\s+)?(?P<rank>\\d+)$"',
        'r"^(?P<rank>\\d+)$"',
        ("tests/test_actor_conversation.py::test_candidate_selection_language_v1",),
    ),
    Mutation(
        "M02", "ordinal selection mapping", "app/core/actor_conversation.py",
        '"segundo": 2, "segunda": 2,',
        '"segundo": 3, "segunda": 3,',
        ("tests/test_actor_conversation.py::test_candidate_selection_language_v1",),
    ),
    Mutation(
        "M03", "pending selection precedence", "app/core/orchestrator.py",
        "            handle_reset,\n            handle_actor_selection,\n            handle_radar_command,",
        "            handle_reset,\n            handle_radar_command,",
        (
            "tests/test_actor_conversation.py::test_pending_actor_selection_precedes_generic_routing_and_preserves_context",
        ),
    ),
    Mutation(
        "M04", "selection provenance", "app/core/workflow/handlers/actor_selection.py",
        "            method=METHOD_USER_SELECTED_CANDIDATE,",
        '            method="exact_unique_alias",',
        (
            "tests/test_actor_conversation.py::test_pending_actor_selection_precedes_generic_routing_and_preserves_context",
        ),
    ),
    Mutation(
        "M05", "invalid selection fails closed", "app/core/workflow/handlers/actor_selection.py",
        "    if selected is None:\n",
        "    if False and selected is None:\n",
        ("tests/test_actor_conversation.py::test_invalid_pending_rank_asks_again_instead_of_falling_through",),
    ),
    Mutation(
        "M06", "CON projects buyer", "app/core/actor_conversation.py",
        "            operation=ActorConversationOperation.SET_BUYER,\n            buyer_mention=buyer,",
        "            operation=ActorConversationOperation.SET_TERRITORY,\n            buyer_mention=buyer,",
        ("tests/test_actor_conversation.py::test_con_and_en_project_different_contextual_operations",),
    ),
    Mutation(
        "M07", "EN projects territory", "app/core/actor_conversation.py",
        "            operation=ActorConversationOperation.SET_TERRITORY,\n            territory_mention=_clean_mention(",
        "            operation=ActorConversationOperation.SET_BUYER,\n            territory_mention=_clean_mention(",
        ("tests/test_actor_conversation.py::test_con_and_en_project_different_contextual_operations",),
    ),
    Mutation(
        "M08", "actor and buyer remain separate roles", "app/core/actor_conversation.py",
        "            actor_mention=actor,\n            buyer_mention=buyer,",
        "            actor_mention=buyer,\n            buyer_mention=actor,",
        ("tests/test_actor_conversation.py::test_actor_buyer_relation_arbitrates_actor_and_topic",),
    ),
    Mutation(
        "M09", "ambiguous buyer never executes", "app/core/actor_scope_policy.py",
        "        return ActorScopeDecision(\n            should_execute=False,\n            missing_slots=(\"buyer_entity\",),",
        "        return ActorScopeDecision(\n            should_execute=True,\n            missing_slots=(\"buyer_entity\",),",
        ("tests/test_actor_conversation.py::test_generic_governacion_is_clarified_not_guessed",),
    ),
    Mutation(
        "M10", "known topics do not become actors", "app/core/actor_arbitration.py",
        "    if normalized in _KNOWN_TOPIC_TERMS:\n        return False",
        "    if normalized in _KNOWN_TOPIC_TERMS:\n        return True",
        ("tests/test_actor_conversation.py::test_fuzzy_candidates_require_independent_actor_evidence",),
    ),
    Mutation(
        "M11", "distinctive typo remains actor-shaped", "app/core/actor_arbitration.py",
        "    if len(tokens) >= 2 and any(\n",
        "    if False and len(tokens) >= 2 and any(\n",
        ("tests/test_actor_conversation_dogfood.py::test_d4_distinctive_actor_token_cannot_degrade_to_topic",),
    ),
    Mutation(
        "M12", "declared person document type", "app/core/actor_intent.py",
        "_CEDULA_MARKER_RE = re.compile(\n    r\"(?:\\bc[eé]dulas?\\b|(?<!\\w)c\\.c\\.?(?!\\w)|\\bc\\s+c\\b|\\bcc\\b|\"\n    r\"\\bpersona\\s+natural\\b|\\bdocumento\\s+de\\s+identidad\\b)\",\n    re.IGNORECASE,\n)",
        "_CEDULA_MARKER_RE = re.compile(r\"\\bc[eé]dulas?\\b\", re.IGNORECASE)",
        ("tests/test_actor_conversation.py::test_person_language_does_not_infer_type_by_length",),
    ),
    Mutation(
        "M13", "strong names have structural not word-count boundary", "app/core/actor_intent.py",
        "def _name_after_role_marker(\n    normalized_query: str,\n    recognized_modifier_tails: Iterable[str] = (),\n    *,\n    max_words: int | None = None,",
        "def _name_after_role_marker(\n    normalized_query: str,\n    recognized_modifier_tails: Iterable[str] = (),\n    *,\n    max_words: int | None = 8,",
        ("tests/test_actor_conversation.py::test_long_strong_actor_names_use_structural_boundaries",),
    ),
    Mutation(
        "M14", "actor replacement purges old identity", "app/core/actor_conversation.py",
        '    "contratista", "actor_scope", "actor_pending", "actor_intent_conflict",',
        '    "contratista", "actor_pending", "actor_intent_conflict",',
        ("tests/test_actor_conversation.py::test_purge_actor_state_removes_every_identity_and_provenance_field",),
    ),
    Mutation(
        "M15", "legal-form ambiguity never auto-selects", "app/data/actor_alias_resolver.py",
        "                if len(legal_exact) == 1:\n",
        "                if len(legal_exact) >= 1:\n",
        ("tests/test_actor_conversation.py::test_legal_form_variant_conflict_never_auto_resolves",),
    ),
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_mutation(mutation: Mutation, *, verbose: bool = False) -> tuple[bool, str]:
    path = ROOT / mutation.path
    before = path.read_bytes()
    source = before.decode("utf-8")
    occurrences = source.count(mutation.original)
    if occurrences != 1:
        return False, f"setup error: expected one mutation site, found {occurrences}"

    mutated = source.replace(mutation.original, mutation.mutated, 1).encode("utf-8")
    environment = os.environ.copy()
    environment["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
    command = [sys.executable, "-m", "pytest", "-q", *mutation.tests]
    try:
        path.write_bytes(mutated)
        process = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    finally:
        path.write_bytes(before)

    if path.read_bytes() != before:
        raise RuntimeError(f"byte restoration failed for {mutation.identifier}: {path}")
    detected = process.returncode != 0
    detail = process.stdout.strip()
    if verbose or not detected:
        return detected, detail
    return detected, detail.splitlines()[-1] if detail else "pytest failed as expected"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    initial_hashes = {
        mutation.path: _sha256((ROOT / mutation.path).read_bytes())
        for mutation in MUTATIONS
    }
    detected = 0
    for mutation in MUTATIONS:
        caught, detail = run_mutation(mutation, verbose=args.verbose)
        detected += int(caught)
        status = "DETECTED" if caught else "SURVIVED"
        print(f"{mutation.identifier} {status}: {mutation.invariant}")
        if detail and (args.verbose or not caught):
            print(detail)

    final_hashes = {
        path: _sha256((ROOT / path).read_bytes()) for path in initial_hashes
    }
    restored = final_hashes == initial_hashes
    print(f"MUTATIONS: TOTAL {len(MUTATIONS)} · DETECTED {detected} · "
          f"UNDETECTED {len(MUTATIONS) - detected}")
    print(f"RESTORED_BYTE_FOR_BYTE: {'YES' if restored else 'NO'}")
    return 0 if detected == len(MUTATIONS) and restored else 1


if __name__ == "__main__":
    raise SystemExit(main())
