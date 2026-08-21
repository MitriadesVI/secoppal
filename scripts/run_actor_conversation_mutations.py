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
        "M01", "pending selection falls through to generic routing",
        "app/core/orchestrator.py",
        "            handle_reset,\n            handle_actor_selection,\n            handle_radar_command,",
        "            handle_reset,\n            handle_radar_command,",
        (
            "tests/test_actor_conversation.py::test_pending_actor_selection_precedes_generic_routing_and_preserves_context",
        ),
    ),
    Mutation(
        "M02", "selected candidate is mislabelled exact_unique_alias",
        "app/core/workflow/handlers/actor_selection.py",
        "            method=METHOD_USER_SELECTED_CANDIDATE,",
        '            method="exact_unique_alias",',
        (
            "tests/test_actor_conversation.py::test_pending_actor_selection_precedes_generic_routing_and_preserves_context",
        ),
    ),
    Mutation(
        "M03", "CON projects geography instead of buyer",
        "app/core/actor_conversation.py",
        "            operation=ActorConversationOperation.SET_BUYER,\n            buyer_mention=buyer,",
        "            operation=ActorConversationOperation.SET_TERRITORY,\n            buyer_mention=buyer,",
        ("tests/test_actor_conversation.py::test_con_and_en_project_different_contextual_operations",),
    ),
    Mutation(
        "M04", "EN projects buyer instead of geography",
        "app/core/actor_conversation.py",
        "            operation=ActorConversationOperation.SET_TERRITORY,\n            territory_mention=territory,",
        "            operation=ActorConversationOperation.SET_BUYER,\n            territory_mention=territory,",
        ("tests/test_actor_conversation.py::test_con_and_en_project_different_contextual_operations",),
    ),
    Mutation(
        "M05", "JOSE CANTILLO loses actor evidence and leaks into objeto",
        "app/core/actor_conversation.py",
        "            actor_evidence=actor_likeness(actor, strong_syntax=strong),",
        '            actor_evidence="topic",',
        ("tests/test_actor_conversation_dogfood.py::test_d3_actor_and_buyer_are_separate_roles_and_not_topic",),
    ),
    Mutation(
        "M06", "buyer mention leaks into actor role",
        "app/core/actor_conversation.py",
        "            actor_mention=actor,\n            buyer_mention=buyer,",
        "            actor_mention=buyer,\n            buyer_mention=actor,",
        ("tests/test_actor_conversation.py::test_actor_buyer_relation_arbitrates_actor_and_topic",),
    ),
    Mutation(
        "M07", "LMP distinctive token is discarded",
        "app/core/actor_arbitration.py",
        "    if len(tokens) >= 2 and any(\n",
        "    if False and len(tokens) >= 2 and any(\n",
        ("tests/test_actor_conversation_dogfood.py::test_d4_distinctive_actor_token_cannot_degrade_to_topic",),
    ),
    Mutation(
        "M08", "fuzzy retrieval invents actor intent without evidence",
        "app/core/workflow/actions/actor_resolution.py",
        "        entity_like = has_entity_like_name_evidence(probe)\n",
        "        entity_like = True\n",
        ("tests/test_actor_conversation.py::test_fuzzy_candidates_require_independent_actor_evidence",),
    ),
    Mutation(
        "M09", "strong framing verb leaks into objeto after selection",
        "app/core/workflow/handlers/actor_selection.py",
        "    selected_params[\"actor_selection_rank\"] = selected_rank\n",
        "    selected_params[\"actor_selection_rank\"] = selected_rank\n    selected_params[\"objeto\"] = [\"buscate\"]\n",
        ("tests/test_actor_conversation_dogfood.py::test_d2_candidate_two_has_selection_precedence_and_no_framing_leak",),
    ),
    Mutation(
        "M10", "strong names restore the historical eight-word ceiling",
        "app/core/actor_intent.py",
        "def _name_after_role_marker(\n    normalized_query: str,\n    recognized_modifier_tails: Iterable[str] = (),\n    *,\n    max_words: int | None = None,",
        "def _name_after_role_marker(\n    normalized_query: str,\n    recognized_modifier_tails: Iterable[str] = (),\n    *,\n    max_words: int | None = 8,",
        ("tests/test_actor_conversation.py::test_long_strong_actor_names_use_structural_boundaries",),
    ),
    Mutation(
        "M11", "actor replacement retains old IdentityKey",
        "app/core/actor_conversation.py",
        '    "contratista", "actor_scope", "actor_pending", "actor_intent_conflict",',
        '    "contratista", "actor_pending", "actor_intent_conflict",',
        ("tests/test_actor_conversation.py::test_purge_actor_state_removes_every_identity_and_provenance_field",),
    ),
    Mutation(
        "M12", "ambiguous gobernacion is allowed to execute",
        "app/core/actor_scope_policy.py",
        "        return ActorScopeDecision(\n            should_execute=False,\n            missing_slots=(\"buyer_entity\",),",
        "        return ActorScopeDecision(\n            should_execute=True,\n            missing_slots=(\"buyer_entity\",),",
        ("tests/test_actor_conversation.py::test_generic_governacion_is_clarified_not_guessed",),
    ),
    Mutation(
        "M13", "conflicting legal-form variants auto-resolve",
        "app/data/actor_alias_resolver.py",
        "                if len(legal_exact) == 1:\n",
        "                if len(legal_exact) >= 1:\n",
        ("tests/test_actor_conversation.py::test_legal_form_variant_conflict_never_auto_resolves",),
    ),
    Mutation(
        "M14", "complete-mention boundary accepts the first matching prefix",
        "app/core/workflow/actions/actor_resolution.py",
        "                suffix_modifiers = _parse_exact_boundary_modifier_suffix(\n                    raw_query, candidate,\n                )",
        "                suffix_modifiers = {}",
        ("tests/test_actor_mention_completeness.py::test_the_frozen_p0_repros_no_longer_resolve_to_the_wrong_actor",),
    ),
    Mutation(
        "M15", "bare canonical geography loses to actor alias",
        "app/core/workflow/actions/actor_resolution.py",
        "    if has_canonical_geography_evidence(probe.actor_name, params):\n",
        "    if False and has_canonical_geography_evidence(probe.actor_name, params):\n",
        ("tests/test_actor_geography_precedence.py::test_bare_canonical_geography_never_becomes_actor_intent",),
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
