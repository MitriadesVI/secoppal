"""Executable contract for the AP3-F natural-language actor corpus."""
from __future__ import annotations

from pathlib import Path

import yaml
from burr.core import State

from app.config import Settings
from app.core.actor_conversation import (
    ActorConversationOperation,
    interpret_actor_conversation,
    parse_actor_candidate_selection,
)
from app.core.actor_name import normalize_actor_name
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver


CORPUS = Path("tests/fixtures/actor_conversation/natural_actor_v1.yaml")


def _load() -> dict:
    return yaml.safe_load(CORPUS.read_text(encoding="utf-8"))


def _resolver(tmp_path, actors: list[dict]) -> ActorNameResolver:
    rows = []
    for rank, actor in enumerate(actors, 1):
        rows.append({
            "tipodocproveedor": actor.get("document_type", "NIT"),
            "documento_proveedor": actor.get("base_number", f"90{rank:07d}"),
            "proveedor_adjudicado": actor["name"],
            "occurrences": 20 - rank,
            "first_seen": "2024-01-01",
            "last_seen": "2026-01-01",
        })
    index = tmp_path / "natural-corpus.sqlite"
    build_actor_alias_index(
        rows, index, source_dataset="ap3-f-corpus",
        built_at="2026-08-20T12:00:00+00:00",
    )
    return ActorNameResolver(index)


def _workflow(tmp_path, actors: list[dict]) -> SecopalWorkflow:
    resolver = _resolver(tmp_path, actors)
    settings = Settings(
        secop_actor_alias_index_path=resolver.index_path,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        secop_feedback_sqlite_enabled=False,
    )
    workflow = SecopalWorkflow(settings)
    workflow.conv_store = ConversationStore(tmp_path / "conversations")
    workflow.secop_client.query = lambda *args, **kwargs: []
    workflow.secop_client.count = lambda *args, **kwargs: 0
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    return workflow


def test_corpus_meets_size_and_sequence_targets():
    corpus = _load()
    utterances = (
        len(corpus["real_actors"]) * len(corpus["strong_families"])
        + len(corpus["exact_dogfood"])
        + len(corpus["topic_controls"])
    )
    sequences = len(corpus["real_actors"]) * len(corpus["sequence_families"])

    assert utterances >= 120
    assert sequences >= 30
    assert all("expected" in case for case in corpus["exact_dogfood"])
    assert all(
        turn.get("expected")
        for family in corpus["sequence_families"]
        for turn in family["turns"]
    )


def test_all_expanded_strong_families_produce_clean_actor_frames(tmp_path):
    corpus = _load()
    resolver = _resolver(tmp_path, corpus["real_actors"])
    router = QueryRouter()

    for family in corpus["strong_families"]:
        for actor in corpus["real_actors"]:
            query = family["template"].format(actor=actor["name"])
            parsed = dict(router.parse(query).params)
            state = State({"parsed_params": parsed, "raw_query": query})
            params = dict(resolve_actor_name(state, resolver)["parsed_params"])
            scope = params.get("actor_scope", {})
            pending = params.get("actor_pending", {})
            mention = pending.get("actor_name") or scope.get("observed_name")
            assert params["actor_conversation_operation"] == (
                family["expected"]["operation"]
            ), (family["id"], actor["name"], params)
            assert mention == normalize_actor_name(actor["name"]), (
                family["id"], actor["name"], params
            )
            assert params.get("objeto") in (None, []), (
                family["id"], actor["name"], params.get("objeto")
            )


def test_topic_controls_remain_topics_and_never_open_fuzzy_resolution(tmp_path):
    corpus = _load()
    resolver = _resolver(tmp_path, corpus["real_actors"])
    router = QueryRouter()

    for topic in corpus["topic_controls"]:
        query = f"contratos de {topic}"
        baseline = dict(router.parse(query).params)
        state = State({"parsed_params": baseline, "raw_query": query})
        params = dict(resolve_actor_name(state, resolver)["parsed_params"])
        assert params == baseline, (topic, params)
        assert params.get("objeto"), topic
        assert params.get("actor_pending") is None


def test_exact_dogfood_operation_examples_match_the_algebra():
    cases = {case["text"]: case["expected"] for case in _load()["exact_dogfood"]}

    for text, expected in cases.items():
        if expected.get("operation") == ActorConversationOperation.SELECT_ACTOR_CANDIDATE:
            assert parse_actor_candidate_selection(text) == expected["rank"]
            continue
        instruction = interpret_actor_conversation(text)
        # Explicit role/document forms are projected by QueryRouter after the
        # algebra's pre-pass; contextual and relation operations are visible
        # directly here.
        if instruction.operation is not None:
            assert instruction.operation == expected.get("operation"), text
        if expected.get("buyer_mention") and instruction.buyer_mention:
            assert instruction.buyer_mention == expected["buyer_mention"]
        if expected.get("territory_mention"):
            assert instruction.territory_mention == expected["territory_mention"]


def test_all_expanded_sequence_families_have_deterministic_operation_steps():
    corpus = _load()
    expanded = 0
    for actor in corpus["real_actors"]:
        for family in corpus["sequence_families"]:
            expanded += 1
            for turn in family["turns"]:
                text = turn["text"].format(actor=actor["name"])
                expected = turn["expected"]
                operation = expected.get("operation")
                if operation:
                    instruction = interpret_actor_conversation(text)
                    # Strong role syntax is finalized in QueryRouter.
                    if instruction.operation is not None:
                        assert instruction.operation == operation, (
                            family["id"], text, instruction
                        )
    assert expanded >= 30


def test_all_expanded_sequences_mutate_real_query_frames(tmp_path):
    corpus = _load()
    workflow = _workflow(tmp_path, corpus["real_actors"])
    actors_by_name = {actor["name"]: actor for actor in corpus["real_actors"]}
    funsocom_base = actors_by_name["FUNSOCOM"]["base_number"]
    expanded = 0

    for actor_rank, actor in enumerate(corpus["real_actors"], 1):
        initial_base = actor.get("base_number", f"90{actor_rank:07d}")
        for family in corpus["sequence_families"]:
            expanded += 1
            chat_id = f"corpus-{actor_rank}-{family['id']}"
            expected_actor = initial_base
            for turn in family["turns"]:
                text = turn["text"].format(actor=actor["name"])
                expected = turn["expected"]
                params = workflow.run_query(text, chat_id=chat_id)["resolved_params"]

                if expected.get("operation"):
                    assert params["actor_conversation_operation"] == (
                        expected["operation"]
                    ), (family["id"], text, params)
                if expected.get("old_identity_cleared"):
                    expected_actor = funsocom_base
                if expected.get("actor_replaced") or expected.get("actor_preserved"):
                    assert params["actor_scope"]["base_number"] == expected_actor, (
                        family["id"], text, params
                    )
                if expected.get("time_preserved"):
                    assert params.get("fecha_desde"), (family["id"], text, params)
                if expected.get("buyer_preserved"):
                    assert params.get("entidad_resolved"), (
                        family["id"], text, params
                    )
                if expected.get("old_identity_cleared"):
                    assert params["actor_scope"]["base_number"] == funsocom_base
                    if initial_base != funsocom_base:
                        assert initial_base not in params.get("contratista", [])

    assert expanded >= 30
