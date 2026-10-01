"""A4: ¿quién se queda hoy con cada caso NEIGHBOR? Medición OFFLINE de fronteras de capacidad.

Uso (cwd = raíz del worktree, PYTHONPATH=.):
    python scripts/audits/m1_search/boundary_probe.py [--gold PATH] [--out PATH] [--scratch DIR]

Sin red (los sockets se bloquean), sin telemetría Burr, sin Natural Alpha (sin proveedor LLM) y con TODA la
persistencia (feedback, radar, conversaciones) redirigida a --scratch (por defecto un tmpdir). No modifica app/.

Para cada caso mide dos cosas distintas:

1. DESCOMPOSICIÓN DE LA PUERTA (la lógica de `foundation_runtime` + `foundation_route_owner`):
   `first_search_ineligibility` (regex léxica) y `specialized_owner` (ACTOR > ENTITY_PROFILE > CONTRACT_ACTIVITY >
   ANALYTICS), más el veredicto individual de cada detector vecino sobre el mismo parse determinista.
   `specialized_owner` aislado = lo que pasaría cuando M1.5 BORRE la puerta léxica.
2. DESPACHO REAL: `SecopalWorkflow.run_query` con `foundation_mode=foundation_v3` y un productor que sólo anota
   "Foundation reclamó el turno" y aborta. Si el productor corre, la puerta admitió; si no, el turno cayó a
   legacy y `route_reason`/`_continuation_owner` dicen quién lo atendió (los datos del handler legacy son
   irrelevantes: sin red devuelve fallos, pero la ruta ya quedó fijada).

Natural Alpha / bindings A2, CA y FOLLOW no se pueden medir offline: dependen de la salida de un LLM.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import socket
import sys
import tempfile
import time
from datetime import date
from pathlib import Path

REPO = Path.cwd()
TODAY = date(2026, 9, 30)
ACTOR_INDEX = Path(os.environ.get("SECOPPAL_ACTOR_INDEX", "/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite"))


def _no_network(*args, **kwargs):
    raise AssertionError("no network in boundary probe")


socket.socket.connect = _no_network
import burr.telemetry  # noqa: E402

burr.telemetry.g_telemetry_enabled = False


# Claves de params que llevan condiciones materiales; sirven para ver QUÉ condiciones sobreviven a legacy.
RESOLVED_KEYS = ("dataset", "entidad_resolved", "departamento_resolved", "ciudad", "fecha_desde", "fecha_hasta",
                 "valor_min", "valor_max", "objeto", "estado", "estado_contrato", "modalidad", "modalidad_resolved",
                 "ordering_signal", "contratista", "actor_resolution_method")


class Claimed(Exception):
    """El productor de Foundation corrió: la puerta admitió el turno."""


def _stub_producer(_query):
    raise Claimed()


def build_workflow(scratch: Path):
    from app.config import Settings
    from app.core import orchestrator
    from app.core.conversation_store import ConversationStore as RealStore

    root = scratch / "run"
    root.mkdir(parents=True, exist_ok=True)
    # Persistencia SIEMPRE fuera del producto: ni el constructor por defecto toca data/.
    orchestrator.ConversationStore = lambda *a, **k: RealStore(root / "conversations")
    settings = Settings(
        _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
        secoppal_foundation_mode="foundation_v3",
        secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
        secop_actor_alias_index_path=ACTOR_INDEX,
    )
    flow = orchestrator.SecopalWorkflow(settings)
    flow.foundation_producer = _stub_producer
    return flow


def _owner_of(result: dict) -> str:
    reason = str(result.get("route_reason") or "")
    cont = result.get("_continuation_owner")
    if reason.startswith("entity_profile"):
        return "ENTITY_PROFILE"
    if reason.startswith("actor_profile"):
        return "ACTOR_PROFILE"
    if reason.startswith("analytics_") or result.get("analytical_intent"):
        return "CONTRACT_ACTIVITY" if "contract_activity" in reason else "ANALYTICS_LEGACY"
    if reason.startswith("radar") or cont == "RADAR":
        return "RADAR"
    if cont == "ACTOR":
        return "ACTOR_RESOLUTION"
    if reason.startswith("direct_lookup"):
        return "PROTOCOL"
    if reason.startswith(("reset", "pagination", "suggestion", "admission")):
        return "PROTOCOL"
    return "LEGACY_TABULAR"


def probe_case(flow, text: str) -> dict:
    from app.core import foundation_route_owner as fro
    from app.core.foundation_runtime import first_search_ineligibility
    from app.core.workflow.context import HandlerContext

    out: dict = {}
    ctx = HandlerContext(user_query=text, channel="streamlit", chat_id=f"a4-gate-{abs(hash(text)) % 10**8}", today=TODAY)
    inel = first_search_ineligibility(flow, ctx, None)
    out["gate_lexical"] = None if inel is None else inel.value
    try:
        parsed = fro._parse(flow, ctx)
    except Exception as exc:  # pragma: no cover - diagnóstico
        out["parse_error"] = f"{type(exc).__name__}: {exc}"[:200]
        return out
    p = parsed.params
    out["parse"] = {
        "dataset": p.get("dataset"), "entidad": p.get("entidad"), "objeto": p.get("objeto"),
        "ciudad": p.get("ciudad"), "departamento": p.get("departamento_resolved") or p.get("departamento"),
        "fecha_desde": str(p.get("fecha_desde") or "") or None, "ordering_signal": p.get("ordering_signal"),
        "actor_scope": bool(p.get("actor_scope")), "actor_pending": bool(p.get("actor_pending")),
        "contratista": p.get("contratista"), "needs_llm": bool(parsed.needs_llm),
    }
    det: dict = {}
    from app.core.radar.commands import parse_radar_command
    from app.core.direct_lookup import detect_direct_lookup
    from app.core.conversation_store import is_pagination_phrase, is_reset_command

    cmd = parse_radar_command(text)
    det["radar_command"] = cmd.kind if cmd else None
    det["direct_lookup"] = bool(detect_direct_lookup(text))
    det["protocol_phrase"] = bool(is_pagination_phrase(text) or is_reset_command(text))
    # ACTOR
    try:
        det["actor_parse"] = bool(p.get("actor_scope") or p.get("actor_pending") or p.get("contratista"))
        det["actor_claims"] = bool(fro.actor_claims(flow, ctx, parsed))
        from app.core.workflow.actions.actor_resolution import resolve_actor_params
        projected = resolve_actor_params(text, dict(p), flow.actor_name_resolver,
                                         [r.to_state() for r in getattr(parsed, "negation_requests", ())])
        det["actor_projection"] = None if projected is None else {
            "method": projected.get("actor_resolution_method"),
            "status": str(projected.get("actor_resolution_status") or ""),
            "pending": bool(projected.get("actor_pending")),
            "conflict": bool(projected.get("actor_intent_conflict")),
            "contratista": bool(projected.get("contratista"))}
        from app.core.actor_profile.turn import is_actor_profile_turn
        eff = projected if projected is not None else p
        det["actor_profile_would_answer"] = bool(is_actor_profile_turn(eff))
    except Exception as exc:  # pragma: no cover
        det["actor_error"] = f"{type(exc).__name__}: {exc}"[:160]
    # ENTITY PROFILE (el detector que usa la puerta, y la clasificación completa que ejecuta el perfil)
    try:
        from app.core.entity_profile.turn import classify_entity_profile_turn, detect_entity_profile_turn
        d, focus = detect_entity_profile_turn(text, p)
        det["entity_profile_detect"] = bool(d)
        det["entity_profile_focus"] = focus
        det["entity_profile_classify"] = bool(classify_entity_profile_turn(text, dict(p)).detected) if d else False
    except Exception as exc:  # pragma: no cover
        det["entity_error"] = f"{type(exc).__name__}: {exc}"[:160]
    # CONTRACT ACTIVITY
    try:
        from app.core.analytics.contract_activity.admission import admit_contract_activity
        ev = parsed.semantic_evidence.to_state() if parsed.semantic_evidence else None
        if ev is None:
            det["contract_activity"] = {"status": "NO_EVIDENCE", "reason": None}
        else:
            adm = admit_contract_activity(text, p, today=TODAY, semantic_evidence=ev)
            det["contract_activity"] = {"status": adm.status.value, "reason": adm.reason}
    except Exception as exc:  # pragma: no cover
        det["contract_activity"] = {"status": "ERROR", "reason": f"{type(exc).__name__}: {exc}"[:120]}
    # ANALYTICS (REGISTRY legacy)
    try:
        from app.core.analytics.registry import REGISTRY
        det["analytics_intent"] = next((a.intent for a in REGISTRY if a.detect(text, p)), None)
    except Exception as exc:  # pragma: no cover
        det["analytics_error"] = f"{type(exc).__name__}: {exc}"[:160]
    out["detectors"] = det
    try:
        out["gate_owner"] = fro.specialized_owner(flow, ctx, parsed)
    except Exception as exc:  # pragma: no cover
        out["gate_owner"] = f"ERROR:{type(exc).__name__}"
    out["foundation_claims_by_gate"] = bool(inel is None and out["gate_owner"] is None)
    # Sin la puerta léxica seguirían precediendo a Foundation los handlers estructurados (radar, protocolo,
    # lookup directo) y los dueños de `specialized_owner`; todo lo demás sería de Foundation.
    pre_handlers = det["radar_command"] or det["direct_lookup"] or det["protocol_phrase"]
    out["foundation_claims_if_gate_deleted"] = out["gate_owner"] is None and not pre_handlers
    return out


def dispatch(flow, text: str, chat_id: str) -> dict:
    """Un chat_id fresco por caso: los canales reales siempre lo traen (Radar y la puerta lo leen) y sin historial."""
    t0 = time.perf_counter()
    try:
        result = flow.run_query(text, channel="streamlit", chat_id=chat_id, today=TODAY)
    except Claimed:
        return {"claimed_by": "FOUNDATION_SEARCH", "route_reason": "foundation_admitted", "seconds": round(time.perf_counter() - t0, 2)}
    except Exception as exc:  # pragma: no cover - diagnóstico
        return {"claimed_by": "ERROR", "error": f"{type(exc).__name__}: {exc}"[:200],
                "seconds": round(time.perf_counter() - t0, 2)}
    rp = result.get("resolved_params") or {}
    return {"claimed_by": _owner_of(result), "route_reason": result.get("route_reason"),
            "continuation_owner": result.get("_continuation_owner"),
            "analytical_intent": result.get("analytical_intent"),
            "resolved": {k: rp.get(k) for k in RESOLVED_KEYS if rp.get(k) not in (None, "", [], {})},
            "actor_scope_method": (rp.get("actor_scope") or {}).get("resolution_method") if isinstance(rp.get("actor_scope"), dict) else None,
            "response_head": (result.get("response") or "")[:140].replace("\n", " / "),
            "seconds": round(time.perf_counter() - t0, 2)}


# ---------------------------------------------------------------------------------------------------------
# Sensibilidad: los actores inventados del Gold NO existen en el directorio (UNRESOLVED). Esta sección repite
# las formas frontera de ACTOR con un alias genérico que SÍ está en el directorio, para medir qué hace Actor
# Profile cuando la identidad se resuelve y si honra las demás condiciones (§6, fila "ACTOR + otra dimensión").
RESOLVABLE_ACTOR = "CENTRAL DE SUMINISTROS SAS"
SENSITIVITY_TEMPLATES = (
    ("ACTOR solo", "contratos de {X}", "SEARCH", ("contratista",)),
    ("ACTOR + BUYER", "contratos del proveedor {X} con la Gobernación del Huila", "SEARCH", ("contratista", "entidad_resolved")),
    ("ACTOR + GEO", "contratos de {X} en Córdoba", "SEARCH", ("contratista", "departamento_resolved")),
    ("ACTOR + TIME", "contratos de {X} en 2024", "SEARCH", ("contratista", "fecha_desde")),
    ("ACTOR + VALUE", "contratos de {X} mayores a 100 millones", "SEARCH", ("contratista", "valor_min")),
    ("ACTOR + TOPIC", "contratos de aseo de {X}", "SEARCH", ("contratista", "objeto")),
    ("ACTOR + STATE", "contratos terminados de {X}", "SEARCH", ("contratista", "estado_contrato")),
    ("ACTOR + MODALITY", "contratos de {X} por contratación directa", "SEARCH", ("contratista", "modalidad_resolved")),
    ("ACTOR + ORDER", "los contratos más caros de {X}", "SEARCH", ("contratista", "ordering_signal")),
    ("PROFILE_ACTOR marcador", "perfil del proveedor {X}", "PROFILE_ACTOR", ("contratista",)),
    ("PROFILE_ACTOR nombre desnudo", "{X}", "PROFILE_ACTOR", ("contratista",)),
    ("PROFILE_ACTOR 'quién es'", "quién es {X}", "PROFILE_ACTOR", ("contratista",)),
    ("PROFILE_ACTOR 'información de'", "información de {X}", "PROFILE_ACTOR", ("contratista",)),
    ("COUNT sobre ACTOR", "cuántos contratos tiene {X}", "COUNT", ("contratista",)),
)


def sensitivity(flow) -> dict:
    status = str(flow.actor_name_resolver.resolve_exact(RESOLVABLE_ACTOR).status)
    block: dict = {"actor": RESOLVABLE_ACTOR, "directory_status": status, "rows": []}
    if "RESOLVED" not in status.upper():
        block["skipped"] = "alias no resoluble en el índice disponible"
        return block
    for label, template, op, required in SENSITIVITY_TEMPLATES:
        text = template.format(X=RESOLVABLE_ACTOR)
        d = dispatch(flow, text, f"a4-sens-{len(block['rows'])}")
        got = d.get("resolved", {})
        row = {"form": label, "text": text, "expected_operation": op, "claimed_by": d["claimed_by"],
               "route_reason": d.get("route_reason"), "resolved": got,
               "conditions_missing_from_execution": [k for k in required if k not in got]
               if d["claimed_by"] != "FOUNDATION_SEARCH" else "n/a (Foundation producer would run; its ACTOR vocabulary is absent)",
               "actor_as_topic": bool(got.get("objeto")) and "contratista" not in got and d["claimed_by"] != "FOUNDATION_SEARCH"}
        block["rows"].append(row)
    return block


# Sondeos fuera del Gold: formas SEARCH legítimas que comparten vocabulario con un vecino (no son datos de evaluación).
SPOT_CHECKS = (
    "busca contratos de aseo por año en Cali",
    "quiero ver los contratos históricos de aseo de la Alcaldía de Cali",
    "busca los procesos de mayor valor de la Gobernación del Huila",
    "busca contratos de la fundación universitaria de Cali en 2025",
    "quiero ver contratos de la corporación autónoma regional del Valle",
    "busca contratos de la empresa de acueducto de Bogotá",
    "busca contratos de la sociedad de activos especiales",
    "busca contratos de la universidad nacional",
    "busca contratos del proveedor de aseo del SENA",
    "necesito los contratos del NIT 899999001 con la Alcaldía de Soledad",
    "busca contratos de alimentación escolar, y dime cuántos hay",
)


def spot_checks(flow) -> list[dict]:
    rows = []
    for at, text in enumerate(SPOT_CHECKS):
        probe = probe_case(flow, text)
        disp = dispatch(flow, text, f"a4-spot-{at}")
        det = probe.get("detectors", {})
        rows.append({"text": text, "gate_lexical": probe.get("gate_lexical"), "gate_owner": probe.get("gate_owner"),
                     "today_owner": disp["claimed_by"], "route_reason": disp.get("route_reason"),
                     "contract_activity": det.get("contract_activity"), "analytics_intent": det.get("analytics_intent"),
                     "actor_parse": det.get("actor_parse")})
    return rows


def verdict(rec: dict, probe: dict, disp: dict) -> dict:
    """Clasifica el caso contra el dueño aceptable del contrato (§6) sin tocar el Gold."""
    nb = rec["neighbor"]
    acceptable = set(nb["acceptable_owners_during_m1"])
    is_search = rec["operation"] == "SEARCH"
    today_owner = disp.get("claimed_by")
    gate_owner = probe.get("gate_owner")
    foundation_today = today_owner == "FOUNDATION_SEARCH"
    v: dict = {"expected_operation": rec["operation"], "turn_context": rec["turn_context"],
               "acceptable_owners": sorted(acceptable),
               "today_owner": today_owner, "gate_owner": gate_owner,
               "foundation_claims_today": foundation_today,
               "foundation_claims_if_gate_deleted": probe.get("foundation_claims_if_gate_deleted")}
    if not is_search:
        v["hijack_today"] = foundation_today and "FOUNDATION_SEARCH" not in acceptable
        v["hijack_if_gate_deleted"] = bool(probe.get("foundation_claims_if_gate_deleted")) and "FOUNDATION_SEARCH" not in acceptable
        v["veto"] = None
        return v
    v["hijack_today"] = v["hijack_if_gate_deleted"] = False
    # SEARCH: ¿lo veta un vecino? (la puerta léxica es otra cosa: se reporta aparte)
    if gate_owner is not None:
        mapped = {"ACTOR": "ACTOR_PROFILE", "ENTITY_PROFILE": "ENTITY_PROFILE",
                  "CONTRACT_ACTIVITY": "CONTRACT_ACTIVITY", "ANALYTICS": "ANALYTICS_LEGACY"}.get(gate_owner, gate_owner)
        v["veto"] = {"by": gate_owner, "acceptable_by_contract": mapped in acceptable,
                     "conditionally_acceptable": mapped in set(nb.get("conditional_owners") or {})}
    else:
        v["veto"] = None
    v["lexically_ineligible"] = probe.get("gate_lexical")
    v["search_reaches_foundation_today"] = foundation_today
    return v


def summarize(rows: list[dict]) -> dict:
    s: dict = collections.OrderedDict()
    non_search = [r for r in rows if r["verdict"]["expected_operation"] != "SEARCH"]
    search = [r for r in rows if r["verdict"]["expected_operation"] == "SEARCH"]
    s["cases"] = len(rows)
    s["non_search_cases"] = len(non_search)
    s["search_cases"] = len(search)
    # Los casos NEEDS_HISTORY (sólo significan algo con un turno previo) se miden pero no se cuentan: la puerta
    # actual exige historial vacío y aquí no hay historial, así que su "secuestro" es un artefacto de la sonda.
    scored = [r for r in non_search if r["verdict"]["turn_context"] != "NEEDS_HISTORY"]
    s["needs_history_not_scored"] = [r["gold_id"] for r in rows if r["verdict"]["turn_context"] == "NEEDS_HISTORY"]
    s["non_search_scored"] = len(scored)
    s["non_search_hijacked_today"] = [r["gold_id"] for r in scored if r["verdict"]["hijack_today"]]
    s["non_search_hijacked_if_gate_deleted"] = [r["gold_id"] for r in scored if r["verdict"]["hijack_if_gate_deleted"]]
    by_neighbor: dict = collections.defaultdict(lambda: {"cases": 0, "hijacked_today": 0, "hijacked_if_gate_deleted": 0})
    for r in scored:
        slot = by_neighbor[r["expected_owner"]]
        slot["cases"] += 1
        slot["hijacked_today"] += bool(r["verdict"]["hijack_today"])
        slot["hijacked_if_gate_deleted"] += bool(r["verdict"]["hijack_if_gate_deleted"])
    s["by_expected_owner"] = dict(by_neighbor)
    s["search_vetoed_by_neighbor"] = [
        {"gold_id": r["gold_id"], "by": r["verdict"]["veto"]["by"],
         "acceptable_by_contract": r["verdict"]["veto"]["acceptable_by_contract"],
         "conditionally_acceptable": r["verdict"]["veto"]["conditionally_acceptable"]}
        for r in search if r["verdict"].get("veto")]
    s["search_ineligible_lexical"] = [r["gold_id"] for r in search if r["verdict"].get("lexically_ineligible")]
    s["search_reaching_foundation_today"] = [r["gold_id"] for r in search if r["verdict"].get("search_reaches_foundation_today")]
    s["owner_today_by_expected_operation"] = {
        op: dict(collections.Counter(r["verdict"]["today_owner"] for r in rows if r["verdict"]["expected_operation"] == op))
        for op in sorted({r["verdict"]["expected_operation"] for r in rows})}
    s["gate_owner_distribution"] = dict(collections.Counter(str(r["probe"].get("gate_owner")) for r in rows))
    mism = [r["gold_id"] for r in rows
            if (r["verdict"]["today_owner"] == "FOUNDATION_SEARCH") != bool(r["probe"].get("foundation_claims_by_gate"))]
    s["gate_vs_dispatch_disagreements"] = mism
    return s


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", default="docs/audits/m1_search_foundation/gold/neighbor.jsonl")
    ap.add_argument("--out", default="docs/audits/m1_search_foundation/a4_boundary_probe.json")
    ap.add_argument("--scratch", default=None)
    args = ap.parse_args(argv)
    scratch = Path(args.scratch) if args.scratch else Path(tempfile.mkdtemp(prefix="a4_boundary_"))
    flow = build_workflow(scratch)
    gold = [json.loads(line) for line in open(args.gold, encoding="utf-8") if line.strip()]
    rows = []
    for rec in gold:
        text = rec["text"]
        try:
            probe = probe_case(flow, text)
        except Exception as exc:  # pragma: no cover
            probe = {"probe_error": f"{type(exc).__name__}: {exc}"[:200]}
        disp = dispatch(flow, text, f"a4-{rec['gold_id']}")
        rows.append({"gold_id": rec["gold_id"], "text": text, "expected_operation": rec["operation"],
                     "pair_id": rec["neighbor"]["pair_id"], "expected_owner": rec["neighbor"]["expected_owner"],
                     "probe": probe, "dispatch": disp, "verdict": verdict(rec, probe, disp)})
    payload = {"meta": {"today": TODAY.isoformat(), "foundation_mode": "foundation_v3", "natural_alpha": "off (offline)",
                        "actor_index": str(ACTOR_INDEX), "network": "blocked", "scratch": str(scratch)},
               "summary": summarize(rows), "sensitivity_resolvable_actor": sensitivity(flow), "spot_checks": spot_checks(flow), "cases": rows}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=1, default=str) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
