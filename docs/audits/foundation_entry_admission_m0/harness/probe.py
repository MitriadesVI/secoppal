"""Paso 0: la puerta de entrada de Foundation, offline, sobre consultas reales etiquetadas."""
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
from app.core.workflow.context import HandlerContext
from app.core import foundation_route_owner as fro
from app.core.foundation_runtime import eligible_first_search

root = SP / "run"; root.mkdir(exist_ok=True)
settings = Settings(_env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
                    secoppal_a2_suppliers_binding_enabled=True, secoppal_contract_activity_binding_enabled=True,
                    secoppal_natural_follow_binding_enabled=True, secoppal_foundation_mode="foundation_v3",
                    secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
                    secop_actor_alias_index_path=Path(os.environ.get("SECOPPAL_ACTOR_INDEX", REPO / "data/actor_alias_index.sqlite")))
flow = SecopalWorkflow(settings)
flow.conv_store = ConversationStore(root / "conversations")
TODAY = date(2026, 9, 30)

def owners(q):
    ctx = HandlerContext(user_query=q, channel="streamlit", today=TODAY)
    elig = eligible_first_search(flow, ctx, None)
    parsed = fro._parse(flow, ctx)
    p = parsed.params
    # desglose de ACTOR: parse vs resolución
    actor_parse = bool(p.get("actor_scope") or p.get("actor_pending") or p.get("contratista"))
    actor_any = fro.actor_claims(flow, ctx, parsed)
    owner = fro.specialized_owner(flow, ctx, parsed)
    return {"eligible": elig, "owner": owner, "actor_parse": actor_parse,
            "actor_resolution_only": actor_any and not actor_parse,
            "entidad": p.get("entidad"), "actor_scope": (p.get("actor_scope") or {}).get("role") if isinstance(p.get("actor_scope"), dict) else p.get("actor_scope"),
            "contratista": p.get("contratista"), "objeto": p.get("objeto")}

rows = json.load(open(SP / "wild_raw.json"))
labels = dict(l.split(" ", 1) for l in open(SP / "labels.txt").read().splitlines())
out = []
t0 = time.time()
for i, r in enumerate(rows):
    try:
        o = owners(r["q"])
    except Exception as exc:
        o = {"error": f"{type(exc).__name__}: {exc}"[:200]}
    o.update(i=i, q=r["q"], gold=labels[str(i)].strip())
    out.append(o)
extra = [l for l in open(SP / "synthetic.txt").read().splitlines() if l.strip()] if (SP / "synthetic.txt").exists() else []
for j, line in enumerate(extra):
    gold, q = line.split("\t")
    try: o = owners(q)
    except Exception as exc: o = {"error": f"{type(exc).__name__}: {exc}"[:200]}
    o.update(i=f"x{j}", q=q, gold=gold); out.append(o)
json.dump(out, open(SP / "probe_out.json", "w"), ensure_ascii=False, default=str, indent=0)
print("done", len(out), round(time.time() - t0, 1), "s")
