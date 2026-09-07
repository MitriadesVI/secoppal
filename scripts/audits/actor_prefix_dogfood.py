#!/usr/bin/env python3
"""Offline real-directory dogfood with isolated persistence and a SECOP double."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
import hashlib
import json
from pathlib import Path
import re
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.config import Settings, actor_profile_enabled, get_settings
from app.core.actor_identity import IdentityKey
from app.core.actor_name import mask_identity_key
from app.core.actor_resolution_trace import actor_resolution_trace
from app.core.orchestrator import SecopalWorkflow


QUERIES = (
    "contratos de jose david cantillo",
    "contratos de jose david cantillo arcon",
    "contratos de jose david cantillo solano",
    "contratos del proveedor jose cantillo",
    "contratos de transporte",
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-path", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with TemporaryDirectory() as temporary, isolated_globals(Path(temporary), args.index_path), patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")), patch.object(socket, "create_connection", side_effect=AssertionError("network forbidden")):
        isolated = Path(temporary)
        settings = Settings(deepseek_api_key=None, secop_actor_alias_index_path=args.index_path,
                            secop_feedback_path=isolated / "feedback.jsonl",
                            secop_feedback_db_path=isolated / "feedback.sqlite",
                            secop_feedback_sqlite_enabled=False)
        effective_flag = actor_profile_enabled()
        workflow = SecopalWorkflow(settings)
        calls = []
        def empty(*arguments, **kwargs):
            calls.append(1)
            return []
        workflow.secop_client.query = empty
        workflow.secop_client.aggregate = empty
        workflow.secop_client.count = lambda *a, **k: 0
        workflow.narrator.narrate_with_grounding = lambda **kwargs: None
        evidence = []
        for position, query in enumerate(QUERIES):
            calls.clear()
            result = workflow.run_query(query, chat_id=f"prefix-dogfood-reset-{position}")
            params = result.get("resolved_params") or {}
            candidates = (params.get("actor_pending") or {}).get("candidates", [])
            candidates = [{"rank": c["rank"], "matched_alias": c["matched_alias"],
                           "identity_masked": mask_identity_key(IdentityKey(c["document_type"], c["base_number"]))} for c in candidates]
            evidence.append({"query": query, "reset_previous": True, "route_reason": result.get("route_reason"),
                             "needs_clarification": result.get("needs_clarification"),
                             "trace": actor_resolution_trace(params), "candidates": candidates,
                             "source_double_calls": len(calls),
                             "response_redacted": re.sub(r"\d{5,}", lambda match: "*" * (len(match[0]) - 4) + match[0][-4:], str(result.get("response")))})
        with workflow.actor_name_resolver._connect_readonly() as connection:
            metadata = dict(connection.execute("SELECT * FROM actor_index_metadata").fetchone())
    with args.index_path.open("rb") as source:
        index_sha = hashlib.file_digest(source, "sha256").hexdigest()
    report = {
        "head_loaded": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "worktree": str(ROOT), "runtime_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD", "--", "app/"], cwd=ROOT)).hexdigest(),
        "actor_profile_flag_effective": effective_flag, "index_path": str(args.index_path),
        "index_sha256": index_sha, "schema": metadata["schema_version"], "N2": metadata["normalization_version"],
        "index_built_at": metadata["built_at"], "channel": "local workflow, isolated chats, real readonly index, SECOP EMPTY double",
        "external_calls": 0, "sockets_blocked": True, "reset_previous": True, "cases": evidence,
        "limitation": "Routing, selection requests and source-call admission only; empty double never accredits current contract counts or identity by activity.",
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps([{k: case[k] for k in ("query", "route_reason", "trace", "source_double_calls")} for case in evidence], ensure_ascii=False))


if __name__ == "__main__":
    main()
