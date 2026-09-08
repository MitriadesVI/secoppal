"""H4 adjudication diagnostic: does the candidate really execute the new search?

Synthetic transport only (same substitution the test performs), plus an external
socket/HTTP fence recorded independently of what the product swallows.
"""
import json
import os
import socket
import sys
import traceback
from pathlib import Path

ATTEMPTS = []

# ---- external network ledger, installed before importing the product ----
_original_connect = socket.socket.connect
_original_create = socket.create_connection


def _connect(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        ATTEMPTS.append(''.join(traceback.format_stack(limit=25)))
        raise AssertionError(f'AUDIT_NETWORK_FORBIDDEN: {address}')
    return _original_connect(sock, address)


def _create(*args, **kwargs):
    ATTEMPTS.append(''.join(traceback.format_stack(limit=25)))
    raise AssertionError(f'AUDIT_NETWORK_FORBIDDEN: {args}')


socket.socket.connect = _connect
socket.create_connection = _create

import requests.adapters  # noqa: E402
import httpx  # noqa: E402


def _deny(original):
    def send(self, request, *args, **kwargs):
        ATTEMPTS.append(f'HTTP {request.method} {request.url}\n' + ''.join(traceback.format_stack(limit=25)))
        raise AssertionError(f'AUDIT_HTTP_FORBIDDEN: {request.method} {request.url}')
    return send


requests.adapters.HTTPAdapter.send = _deny(requests.adapters.HTTPAdapter.send)
httpx.HTTPTransport.handle_request = _deny(httpx.HTTPTransport.handle_request)

from app.config import Settings, actor_profile_enabled, entity_profile_frame_enabled  # noqa: E402
from app.core.orchestrator import SecopalWorkflow  # noqa: E402

SEEN_SOQL = []


def _conv_workflow(timeout=False):
    """Byte-identical to tests/test_h4_fallback.py::_conv_workflow, plus SoQL capture."""
    wf = SecopalWorkflow(Settings())
    if timeout:
        def _q(ds, soql):
            SEEN_SOQL.append(soql)
            raise TimeoutError("SECOP timeout")
        wf.secop_client.query = _q
    else:
        def _q(ds, soql):
            SEEN_SOQL.append(soql)
            return [{
                "nombre_del_procedimiento": "T", "entidad": "E", "precio_base": "100",
                "fecha_de_publicacion_del": "2026-06-01T00:00:00.000", "urlproceso": "u",
                "referencia_del_proceso": "R", "valor_del_contrato": "100"}]
        wf.secop_client.query = _q
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    return wf


def snapshot(result, soql_before):
    params = result.get("resolved_params")
    return {
        "has_resolved_params": params is not None,
        "resolved_params": params,
        "ordering_signal": (params or {}).get("ordering_signal"),
        "needs_clarification": result.get("needs_clarification"),
        "clarification_reason": result.get("clarification_reason"),
        "query_error": result.get("query_error"),
        "result_order_guarantee": result.get("result_order_guarantee"),
        "fallback_used": result.get("fallback_used"),
        "fallback_attempted": result.get("fallback_attempted"),
        "row_count": len(result.get("rows") or result.get("results") or []),
        "total_count": result.get("total_count"),
        "soql_this_turn": SEEN_SOQL[soql_before:],
        "response_head": (result.get("response") or "")[:400],
    }


def scenario_new_topic():
    """The disputed case: turn 1 asks for most-expensive; turn 2 is a NEW search."""
    del SEEN_SOQL[:]
    wf = _conv_workflow(timeout=False)
    r1 = wf.run_query("los contratos mas caros de adulto mayor en cartagena",
                      channel="streamlit", chat_id="pf_clear")
    mark = len(SEEN_SOQL)
    s1 = snapshot(r1, 0)
    r2 = wf.run_query("muestrame contratos de educacion en cali",
                      channel="streamlit", chat_id="pf_clear")
    s2 = snapshot(r2, mark)
    return {"turn1": s1, "turn2": s2}


def scenario_refinement():
    """Inverse control: 'ahora los mas caros' as a REFINEMENT must SET valor_desc."""
    del SEEN_SOQL[:]
    wf = _conv_workflow(timeout=False)
    r1 = wf.run_query("contratos de adulto mayor en cartagena",
                      channel="streamlit", chat_id="pf_refine")
    mark = len(SEEN_SOQL)
    s1 = snapshot(r1, 0)
    r2 = wf.run_query("ahora los mas caros", channel="streamlit", chat_id="pf_refine")
    s2 = snapshot(r2, mark)
    return {"turn1": s1, "turn2": s2}


def scenario_explicit_order_preserved():
    """Second inverse control: a NEW search that itself asks for an order keeps it."""
    del SEEN_SOQL[:]
    wf = _conv_workflow(timeout=False)
    r1 = wf.run_query("los contratos mas caros de adulto mayor en cartagena",
                      channel="streamlit", chat_id="pf_explicit")
    mark = len(SEEN_SOQL)
    s1 = snapshot(r1, 0)
    r2 = wf.run_query("muestrame los contratos mas caros de educacion en cali",
                      channel="streamlit", chat_id="pf_explicit")
    s2 = snapshot(r2, mark)
    return {"turn1": s1, "turn2": s2}


def main():
    out = {
        "flags": {"actor": actor_profile_enabled(), "frame": entity_profile_frame_enabled()},
        "new_topic": scenario_new_topic(),
        "refinement_control": scenario_refinement(),
        "explicit_order_control": scenario_explicit_order_preserved(),
    }
    # The ledger is checked AFTER the product ran, so a swallowed error cannot hide it.
    out["external_attempts"] = len(ATTEMPTS)
    out["external_attempt_traces"] = ATTEMPTS[:3]
    Path(sys.argv[1]).write_text(json.dumps(out, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps({k: out[k] for k in ("flags", "external_attempts")}, indent=2))  # noqa: T201
    return 0 if not ATTEMPTS else 1


if __name__ == '__main__':
    raise SystemExit(main())
