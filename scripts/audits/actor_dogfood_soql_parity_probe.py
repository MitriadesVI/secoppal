"""Emit the SoQL of a fixed set of healthy queries, for byte comparison
between BASE and HEAD. Synthetic actor index; persistence redirected."""
import json, sys, tempfile
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from tests.actor_prefix_fixtures import resolver_for

TMP = Path(tempfile.mkdtemp())
ROWS = [("CC","70000011","beatriz solano"),("CC","70000012","beatriz solano perez"),
        ("CC","70000021","ana maria llanos"),("CC","70000022","carlos rueda caribe"),
        ("CC","70000023","pedro santander mejia"),("NIT","900123456","fundacaribe sas")]
r = resolver_for(TMP/"a.sqlite", ROWS)
wf = SecopalWorkflow(Settings(deepseek_api_key=None, secop_actor_alias_index_path=r.index_path,
    secop_feedback_path=TMP/"f.jsonl", secop_feedback_db_path=TMP/"f.sqlite",
    secop_feedback_sqlite_enabled=False))
CALLS = []
wf.secop_client.query = lambda d,q,*a,**k:(CALLS.append(q),[])[1]
wf.secop_client.count = lambda *a,**k: 0
wf.secop_client.aggregate = lambda d,q,*a,**k:(CALLS.append(q),[])[1]
wf.narrator.narrate_with_grounding = lambda **kw: None

HEALTHY = [
    "contratos de mantenimiento en Bogota en 2025",
    "contratos de alimentacion escolar con alcaldia de medellin",
    "procesos de vigilancia en Antioquia",
    "contratos de aseo con la gobernacion del huila en 2024",
    "contratos de software en Cali por mayor valor",
    "contratos de adulto mayor en la costa caribe",
    "contratos del proveedor fundacaribe sas",
    "contratos del NIT 900123456",
    "contratos de transporte escolar en los llanos",
    "contratos con la universidad nacional en 2025",
    "contratos de obras en el eje cafetero",
    "contratos de ana maria llanos en Meta",
    "contratos con hospital san rafael",
    "contratos de mas de 1000 millones en Santander",
    "oportunidades de mantenimiento en Bolivar",
    "contratos de pedro santander mejia",
    "contratos con municipio de soacha en 2026",
    "contratos de nutricion con ese hospital del sur",
]
out = {}
for q in HEALTHY:
    before = len(CALLS)
    res = wf.run_query(q, chat_id=f"p{abs(hash(q))%9999}")
    out[q] = {"soql": CALLS[before:], "clarify": bool(res.get("needs_clarification")),
              "route": res.get("route_reason")}
print(json.dumps(out, indent=1, ensure_ascii=False, sort_keys=True))
