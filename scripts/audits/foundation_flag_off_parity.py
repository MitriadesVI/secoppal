"""Flag-OFF observable parity for the Foundation promotion (ADR-102 §15.7).

Runs every query of a list through the real SecopalWorkflow with production
defaults (SECOPPAL_FOUNDATION_MODE unset = legacy) over a frozen offline source,
and writes one normalized JSON line per query: route, clarification, dataset,
SoQL, params, total, response, continuation owner, effective source calls and
the persisted feedback trace. Run it from the base and from the candidate
worktree with the same query list and compare the outputs byte for byte.

    python scripts/audits/foundation_flag_off_parity.py <queries.json> <out.jsonl> [profile]

profile: "default" (production defaults) or "frame_actor" (Frame and Actor
Profile on). All persistence is redirected to a temporary directory.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from datetime import date
from pathlib import Path

VOLATILE = re.compile(r"(_ms$|^timings|trace_id|turn_id|result_id|latency|timestamp|run_started|created_at|"
                      r"rated_at|^id$|query_hash|duration|elapsed|reservation|revision)")


def _normalize(value):
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in sorted(value.items()) if not VOLATILE.search(str(k))}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    if isinstance(value, str):
        return re.sub(r"\b[0-9a-f]{12,64}\b", "<hex>", value)
    return value


def main() -> None:
    queries = json.loads(Path(sys.argv[1]).read_text())
    out = Path(sys.argv[2])
    profile = sys.argv[3] if len(sys.argv) > 3 else "default"
    root = Path(tempfile.mkdtemp(prefix="foundation-parity-"))
    os.environ.pop("SECOPPAL_FOUNDATION_MODE", None)
    os.environ["CONVERSATION_STORE_DIR"] = str(root / "conversations")
    if profile == "frame_actor":
        os.environ["SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED"] = "true"
        os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
    import burr.telemetry

    burr.telemetry.g_telemetry_enabled = False
    from app.config import Settings, get_settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow
    from app.core.soql_builder import SoQLBuilder
    from tests.product_contract.frozen_source import FrozenTransport, frozen_client

    get_settings.cache_clear()
    columns = {dataset: dict.fromkeys(SoQLBuilder.SPECS[dataset].select_fields)
               for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    lines = []
    for index, query in enumerate(queries):
        case = root / f"q{index}"
        settings = Settings(_env_file=None, deepseek_api_key=None, secop_app_token=None,
                            secop_feedback_path=case / "traces.jsonl", secop_feedback_db_path=case / "feedback.db",
                            secop_actor_alias_index_path=case / "no_actor_index.sqlite")
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(case / "conversations")
        transport = FrozenTransport({dataset: [dict(row)] for dataset, row in columns.items()})
        flow.secop_client = frozen_client(transport, timeout=settings.secop_timeout_seconds)
        try:
            result = flow.run_query(query, chat_id=f"parity-{index}", today=date(2026, 9, 26))
            error = None
        except Exception as exc:  # noqa: BLE001 - an exception is an observable too
            result, error = {}, f"{type(exc).__name__}: {exc}"[:300]
        traces = [json.loads(line) for line in (case / "traces.jsonl").read_text().splitlines()] \
            if (case / "traces.jsonl").exists() else []
        record = {
            "query": query, "error": error,
            **{key: result.get(key) for key in ("route_reason", "needs_clarification", "dataset_id", "soql_query",
                                                "parsed_params", "resolved_params", "total_count", "response",
                                                "_continuation_owner", "foundation_mode", "source_call_receipts")},
            "result_keys": sorted(result),
            "calls": [(call.get("operation"), call.get("dataset"), call.get("soql")) for call in transport.calls],
            "traces": traces,
        }
        lines.append(json.dumps(_normalize(record), ensure_ascii=False, sort_keys=True, default=str))
    out.write_text("\n".join(lines) + "\n")
    print(f"{len(lines)} queries -> {out}")


if __name__ == "__main__":
    main()
