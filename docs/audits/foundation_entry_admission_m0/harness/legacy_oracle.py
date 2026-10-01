"""Oráculo legacy: qué dueño y ruta da main con Foundation OFF, sobre fuente congelada vacía."""
import json, os, socket, sys, time
from datetime import date
from pathlib import Path
SP = Path(sys.argv[1]); REPO = Path.cwd()
def forbidden(*a, **k): raise AssertionError("no network")
socket.socket.connect = forbidden
import burr.telemetry; burr.telemetry.g_telemetry_enabled = False
from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore
from tests.test_foundation_route_ownership import RecordingSource
from tests.test_natural_alpha_contract import IDENTITY

root = SP / "legacy_run"; root.mkdir(exist_ok=True)
settings = Settings(_env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
                    secoppal_a2_suppliers_binding_enabled=True, secoppal_contract_activity_binding_enabled=True,
                    secoppal_natural_follow_binding_enabled=True, secoppal_foundation_mode="legacy",
                    secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
                    secop_actor_alias_index_path=Path(os.environ.get("SECOPPAL_ACTOR_INDEX", REPO / "data/actor_alias_index.sqlite")))
flow = SecopalWorkflow(settings)
flow.conv_store = ConversationStore(root / "conversations")
src = RecordingSource(); flow.secop_client, flow.source = src.client, src
probe = json.load(open(SP / "probe_out.json"))
res = []
t0 = time.time()
for o in probe:
    try:
        r = flow.run_query(o["q"], channel="streamlit", user_identity=IDENTITY, today=date(2026, 9, 30))
        rp = r.get("resolved_params") or {}
        res.append({"i": o["i"], "route_reason": r.get("route_reason"), "owner": r.get("_continuation_owner"),
                    "needs_clar": r.get("needs_clarification"), "analytical": r.get("analytical_intent"),
                    "objeto": rp.get("objeto"), "entidad": rp.get("entidad"), "contratista": rp.get("contratista"),
                    "actor_scope": bool(rp.get("actor_scope")), "dataset": r.get("dataset_id"),
                    "resp": (r.get("response") or "")[:160]})
    except Exception as exc:
        res.append({"i": o["i"], "error": f"{type(exc).__name__}: {exc}"[:200]})
json.dump(res, open(SP / "legacy_out.json", "w"), ensure_ascii=False, default=str, indent=0)
print("done", len(res), round(time.time() - t0, 1), "s")
