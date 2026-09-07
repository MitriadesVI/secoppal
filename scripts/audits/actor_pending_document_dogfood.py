#!/usr/bin/env python3
"""Synthetic deterministic dogfood for pending-list documentary selection.

Every actor here is fabricated and the SECOP client is an EMPTY double, so the
report accredits routing, frame preservation and source-call admission only —
never counts, never a real identity.  Documents are masked in rendered text.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.config import Settings, actor_profile_enabled, get_settings
from app.core.orchestrator import SecopalWorkflow
from tests.actor_prefix_fixtures import CHALLENGERS, EXACT, NAME, resolver_for

SAME_ALIAS = [("CC", "70000001", NAME), ("CC", "70000002", NAME)]
FOREIGN = "CC 79482763"

CASES = (
    ("ambiguity_year", [EXACT, *CHALLENGERS],
     f"contratos del proveedor {NAME} en 2025", "CC 70000003"),
    ("ambiguity_buyer", [EXACT, *CHALLENGERS],
     f"contratos del proveedor {NAME} con alcaldia de barranquilla", "CC 70000003"),
    ("prefix_ambiguity", [EXACT, *CHALLENGERS],
     f"contratos del proveedor {NAME} en 2025", "CC 70000002"),
    ("same_alias_ambiguity", SAME_ALIAS,
     f"contratos del proveedor {NAME} en 2025", "CC 70000002"),
    ("unrelated_document", [EXACT, *CHALLENGERS],
     f"contratos del proveedor {NAME} con alcaldia de barranquilla en 2025", FOREIGN),
)

_FRAME_KEYS = (
    "fecha_desde", "fecha_hasta", "entidad_resolved", "valor_min", "valor_max",
    "estado", "modalidad", "objeto",
)


@contextmanager
def isolated_globals(directory, index_path):
    with patch.dict(os.environ, {
        "SECOP_FEEDBACK_PATH": str(directory / "feedback.jsonl"),
        "SECOP_FEEDBACK_DB_PATH": str(directory / "feedback.sqlite"),
        "CONVERSATION_STORE_DIR": str(directory / "conversations"),
        "SECOP_ACTOR_ALIAS_INDEX_PATH": str(index_path),
    }):
        get_settings.cache_clear()
        try:
            yield
        finally:
            get_settings.cache_clear()


def _mask(text):
    return re.sub(r"\d{5,}", lambda m: "*" * (len(m[0]) - 4) + m[0][-4:], str(text))


def _frame(params):
    return {key: params.get(key) for key in _FRAME_KEYS if params.get(key)}


def _run_case(directory, label, rows, first_query, reply):
    index_path = directory / f"{label}.sqlite"
    resolver = resolver_for(index_path, rows)
    with isolated_globals(directory, index_path):
        settings = Settings(
            deepseek_api_key=None,
            secop_actor_alias_index_path=resolver.index_path,
            secop_feedback_path=directory / "feedback.jsonl",
            secop_feedback_db_path=directory / "feedback.sqlite",
            secop_feedback_sqlite_enabled=False,
        )
        workflow = SecopalWorkflow(settings)
        soql = []

        def record(dataset, statement, *args, **kwargs):
            soql.append(statement)
            return []

        workflow.secop_client.query = record
        workflow.secop_client.aggregate = record
        workflow.secop_client.count = lambda *a, **k: 0
        workflow.narrator.narrate_with_grounding = lambda **kwargs: None

        first = workflow.run_query(first_query, chat_id=label)
        first_params = first.get("resolved_params") or {}
        pending = first_params.get("actor_pending") or {}
        soql.clear()
        answer = workflow.run_query(reply, chat_id=label)
        params = answer.get("resolved_params") or {}
        document_soql = sorted(_mask(statement) for statement in soql)
        rank_soql = None
        matched = [
            candidate for candidate in pending.get("candidates") or []
            if f"{candidate['document_type']} {candidate['base_number']}" == reply
        ]
        if matched:
            workflow.run_query(first_query, chat_id=f"{label}-rank")
            soql.clear()
            workflow.run_query(str(matched[0]["rank"]), chat_id=f"{label}-rank")
            rank_soql = sorted(_mask(statement) for statement in soql)
        return {
            "case": label,
            "first_query": first_query,
            "first_method": first_params.get("actor_resolution_method"),
            "first_frame": _frame(first_params),
            "pending_candidates": len(pending.get("candidates") or []),
            "reply": _mask(reply),
            "document_belongs_to_pending": bool(matched),
            "answer_route": answer.get("route_reason"),
            "answer_method": params.get("actor_resolution_method"),
            "answer_base_number_masked": _mask((params.get("actor_scope") or {}).get("base_number")),
            "answer_frame": _frame(params),
            "frame_preserved": _frame(params) == _frame(first_params),
            "document_soql": document_soql,
            "rank_soql": rank_soql,
            "soql_parity": rank_soql is None or rank_soql == document_soql,
            "response_redacted": _mask(answer.get("response")),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with TemporaryDirectory() as temporary, \
            patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")), \
            patch.object(socket, "create_connection", side_effect=AssertionError("network forbidden")):
        directory = Path(temporary)
        flag = actor_profile_enabled()
        cases = [_run_case(directory, *case) for case in CASES]
    report = {
        "head_loaded": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "worktree": str(ROOT),
        "runtime_diff_sha256": hashlib.sha256(subprocess.check_output(
            ["git", "diff", "HEAD", "--", "app/"], cwd=ROOT)).hexdigest(),
        "actor_profile_flag_effective": flag,
        "fixture": "synthetic alias index built per case; sockets blocked; SECOP EMPTY double",
        "external_calls": 0,
        "sockets_blocked": True,
        "cases": cases,
        "limitation": (
            "Routing, frame preservation and SoQL shape only; an EMPTY double "
            "never accredits counts nor identity by activity."
        ),
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps([
        {key: case[key] for key in (
            "case", "answer_method", "frame_preserved", "soql_parity",
            "document_belongs_to_pending")}
        for case in cases
    ], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
