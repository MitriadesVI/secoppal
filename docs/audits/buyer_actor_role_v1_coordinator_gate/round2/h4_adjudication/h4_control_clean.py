"""Control: does a CLEAN single-turn 'educacion en cali' filter by education at all?"""
import json, socket, sys, traceback
from pathlib import Path
ATTEMPTS = []
_oc = socket.socket.connect
def _c(s, a):
    if s.family in (socket.AF_INET, socket.AF_INET6):
        ATTEMPTS.append(''.join(traceback.format_stack(limit=25))); raise AssertionError(a)
    return _oc(s, a)
socket.socket.connect = _c
socket.create_connection = lambda *a, **k: (_ for _ in ()).throw(AssertionError(a))
import requests.adapters, httpx
def _deny(o):
    def send(self, request, *a, **k):
        ATTEMPTS.append(f'HTTP {request.method} {request.url}'); raise AssertionError(request.url)
    return send
requests.adapters.HTTPAdapter.send = _deny(requests.adapters.HTTPAdapter.send)
httpx.HTTPTransport.handle_request = _deny(httpx.HTTPTransport.handle_request)

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
SEEN = []
def wf_new():
    wf = SecopalWorkflow(Settings())
    def _q(ds, soql):
        SEEN.append(soql)
        return [{"nombre_del_procedimiento": "T", "entidad": "E", "precio_base": "100",
                 "fecha_de_publicacion_del": "2026-06-01T00:00:00.000", "urlproceso": "u",
                 "referencia_del_proceso": "R", "valor_del_contrato": "100"}]
    wf.secop_client.query = _q
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    return wf

out = {}
for label, phrase in [("clean_educacion_cali", "muestrame contratos de educacion en cali"),
                      ("clean_adulto_mayor_cartagena", "los contratos mas caros de adulto mayor en cartagena")]:
    del SEEN[:]
    r = wf_new().run_query(phrase, channel="streamlit", chat_id="clean_" + label)
    out[label] = {"ordering_signal": (r.get("resolved_params") or {}).get("ordering_signal"),
                  "objeto_phrases": (r.get("resolved_params") or {}).get("objeto_phrases"),
                  "resolved_params_keys": sorted((r.get("resolved_params") or {}).keys()),
                  "soql": SEEN[:]}
out["external_attempts"] = len(ATTEMPTS)
Path(sys.argv[1]).write_text(json.dumps(out, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items() if kk != 'soql'}) for k, v in out.items()}, indent=2, ensure_ascii=False))  # noqa: T201
