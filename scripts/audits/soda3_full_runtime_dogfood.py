#!/usr/bin/env python3
"""Dogfood LIVE SODA3-only del runtime productivo completo de SECOPPAL.

El proceso debe arrancar con ``SECOPPAL_SOCRATA_TRANSPORT=soda3``. El runner
obtiene el singleton productivo con ``app.service.get_workflow()``, comprueba el
transporte seleccionado y solo entonces añade observación pasiva al cliente ya
construido. No instancia ``Soda3HttpTransport`` ni un ``SecopClient`` paralelo.

Las conversaciones y el feedback se aíslan en un temporal. La única escritura
productiva opcional es la pasada supervisada del radar piloto indicado por
``--radar-name``; ``run_radar`` actualiza su feed, pero no envía notificaciones.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.audits.audit_isolation import isolated_runtime_stores  # noqa: E402
from scripts.audits.soda3_productive_live_smoke import (  # noqa: E402
    Ledger,
    RecordingHttpClient,
    RecordingSecopClient,
)

REFERENCE_DATE = date(2026, 8, 28)
ALLOWED_DATASETS = frozenset({"p6dx-8zbt", "jbjy-vk9h"})


# Dieciocho turnos humanos útiles. Los seis primeros comparten conversación y
# recorren refinamiento, cambio temporal, cambio de orden, paginación y una
# búsqueda nueva. El resto proviene del corpus/canario ya usado por el proyecto.
TURNS: tuple[dict[str, Any], ...] = (
    {"id": "FU-01", "chat": "dogfood-followups", "category": "normal_search",
     "query": "contratos de aseo en cali de junio de 2026"},
    {"id": "FU-02", "chat": "dogfood-followups", "category": "followup_refinement",
     "query": "y solo los mayores de 300 millones"},
    {"id": "FU-03", "chat": "dogfood-followups", "category": "followup_date",
     "query": "ahora en 2025"},
    {"id": "FU-04", "chat": "dogfood-followups", "category": "followup_order",
     "query": "ordenalos por mayor valor"},
    {"id": "FU-05", "chat": "dogfood-followups", "category": "pagination",
     "query": "muestrame mas"},
    {"id": "FU-06", "chat": "dogfood-followups", "category": "new_search",
     "query": "busca contratos de software en bogota"},
    {"id": "TAB-PROC", "category": "normal_search",
     "query": "procesos de salud en valle del cauca"},
    {"id": "TAB-ENTITY-VALUE-DATE", "category": "normal_search",
     "query": "contratos de alimentacion de la gobernacion de sucre por mas de 200 millones en 2025"},
    {"id": "OPP-OPEN", "category": "opportunity",
     "query": "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas"},
    {"id": "OBJ-OR", "category": "normal_search",
     "query": "que oportunidades hay para construccion de canchas o parques?"},
    {"id": "NEGATION", "category": "normal_search",
     "query": "contratos de alimentacion sin transporte"},
    {"id": "PHRASE", "category": "phrase_aware",
     "query": "contratos para elaboracion o actualizacion de manuales de contratacion en 2024"},
    {"id": "DIRECT-CONTRACT", "category": "direct_lookup",
     "query": "CO1.PCCNTR.7754467"},
    {"id": "DIRECT-PROCESS", "category": "direct_lookup",
     "query": "CO1.REQ.9445488"},
    {"id": "ACTOR", "category": "actor_profile",
     "query": "contratos del NIT 860002400"},
    {"id": "ANALYTICS-SUM", "category": "analytics",
     "query": "cuanto suman los contratos de primera infancia en barranquilla 2026"},
    {"id": "ANALYTICS-STATS", "category": "analytics",
     "query": "valor promedio de los contratos de obra en Antioquia"},
    {"id": "H4-CANDIDATE", "category": "h4_candidate",
     "query": "contratos de interventoria tecnica de obra"},
)


def _git(*args: str) -> str | None:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=False, capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _error_class(exc: BaseException) -> str:
    names = [type(exc).__name__]
    cause = exc.__cause__ or exc.__context__
    while cause is not None and len(names) < 4:
        names.append(type(cause).__name__)
        cause = cause.__cause__ or cause.__context__
    return " <- ".join(names)


def _turn_summary(result: dict, *, elapsed_ms: float) -> dict[str, Any]:
    metrics = result.get("http_metrics") or []
    statuses = Counter(str(m.get("status") or "unknown") for m in metrics)
    timeouts = sum(bool(m.get("timeout")) for m in metrics)
    return {
        "route": result.get("route_reason"),
        "dataset_id": result.get("dataset_id"),
        "runtime_status": "ok",
        "query_error": result.get("query_error"),
        "needs_clarification": bool(result.get("needs_clarification")),
        "clarification_reason": result.get("clarification_reason"),
        "result_count": len(result.get("results") or result.get("rows") or []),
        "total_count": result.get("total_count"),
        "analytical_intent": result.get("analytical_intent"),
        "direct_lookup_status": result.get("direct_lookup_status"),
        "direct_lookup_reason": result.get("direct_lookup_reason"),
        "fallback_attempted": bool(result.get("fallback_attempted")),
        "fallback_used": bool(result.get("fallback_used")),
        "fallback_strategy": result.get("fallback_strategy"),
        "fallback_query_status": result.get("fallback_query_status"),
        "primary_query_status": result.get("primary_query_status"),
        "degraded": bool(result.get("degraded")),
        "result_order_guarantee": result.get("result_order_guarantee"),
        "http_metric_count": len(metrics),
        "http_metric_statuses": dict(statuses),
        "timeout_count": timeouts,
        "source_partial": bool(result.get("source_partial")),
        "actor_profile_partial": bool(result.get("partial")),
        "observer_present": result.get("universe_insights") is not None,
        "observer_computed_from": getattr(
            result.get("universe_insights"), "computed_from", None,
        ),
        "observer_partial": getattr(result.get("universe_insights"), "partial", None),
        "elapsed_ms": round(elapsed_ms, 3),
        "soql_query": result.get("soql_query"),
    }


def _run_turn(workflow: Any, ledger: Ledger, case: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        result = workflow.run_query(
            case["query"], channel="streamlit",
            chat_id=case.get("chat", f"dogfood-{case['id'].lower()}"),
            today=REFERENCE_DATE,
        )
        summary = _turn_summary(result, elapsed_ms=(time.perf_counter() - started) * 1000)
    except BaseException as exc:  # noqa: BLE001 - auditoría continúa para clasificar
        summary = {
            "route": None, "dataset_id": None, "runtime_status": "error",
            "error_class": _error_class(exc), "query_error": str(exc)[:300],
            "result_count": 0, "total_count": None, "timeout_count": 0,
            "fallback_attempted": False, "fallback_used": False,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    ledger.emit(
        "human_turn", human_query=case["query"], category=case["category"], **summary,
    )
    return summary


def _run_radar(
    workflow: Any, ledger: Ledger, *, db_path: Path, radar_name: str,
) -> dict[str, Any]:
    from app.core.radar.runner import run_radar
    from app.core.radar.store import RadarStore

    store = RadarStore(db_path)
    candidates = [r for r in store.list_all_enabled_radars() if r.name == radar_name]
    if len(candidates) != 1:
        summary = {
            "status": "not_exercised", "reason": f"radar_matches={len(candidates)}",
            "radar_name": radar_name,
        }
        ledger.emit("radar_case", **summary)
        return summary

    radar = candidates[0]
    workflow.radar_store = store
    started = time.perf_counter()
    try:
        result = run_radar(workflow.deps, store, radar)
        summary = {
            "status": result.status,
            "outcome": result.outcome,
            "reason": result.reason,
            "total": result.total,
            "execution_ms": result.execution_ms,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            "new_count": result.diff.new_count if result.diff else 0,
            "changed_count": result.diff.changed_count if result.diff else 0,
            "radar_id": radar.id,
            "radar_name": radar.name,
            "owner": radar.owner,
            "notification_sent": False,
        }
    except BaseException as exc:  # noqa: BLE001
        summary = {
            "status": "error", "reason": _error_class(exc),
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            "radar_id": radar.id, "radar_name": radar.name,
            "notification_sent": False,
        }
    ledger.emit("radar_case", **summary)
    return summary


def _overlap_count(events: list[dict[str, Any]]) -> int:
    """Cuenta pares de operaciones remotas cuyos intervalos se solapan."""
    spans: list[tuple[datetime, datetime]] = []
    for event in events:
        if event.get("kind") != "socrata_op" or event.get("status") != "ok":
            continue
        try:
            end = datetime.fromisoformat(event["ts"])
            start = end - timedelta(milliseconds=float(event["elapsed_ms"]))
        except (KeyError, TypeError, ValueError):
            continue
        spans.append((start, end))
    overlaps = 0
    for index, (left_start, left_end) in enumerate(spans):
        for right_start, right_end in spans[index + 1:]:
            if left_start < right_end and right_start < left_end:
                overlaps += 1
    return overlaps


def _gate_summary(
    ledger: Ledger, turns: dict[str, dict[str, Any]], radar: dict[str, Any],
) -> dict[str, Any]:
    ops = [e for e in ledger.events if e.get("kind") == "socrata_op"]
    http = [e for e in ledger.events if e.get("kind") == "soda3_http"]
    turn_events = [e for e in ledger.events if e.get("kind") == "human_turn"]
    fatal = sum(e.get("runtime_status") == "error" for e in turn_events)
    contract_errors = sum(
        isinstance(e.get("http_status"), int) and int(e["http_status"]) >= 400
        for e in http
    )
    # Una operación remota con timeout que el workflow convierte en H4, total
    # aproximado o respuesta transparente NO es un error fatal. Se conserva su
    # conteo por separado; fatal significa que el turno abortó o hubo contrato
    # HTTP inválido.
    handled_operation_errors = sum(e.get("status") == "error" for e in ops)
    datasets = sorted({e["dataset_id"] for e in ops if e.get("dataset_id")})
    actor_events = [e for e in ops if e.get("case") == "ACTOR"]
    observer_events = [
        e for e in ops if e.get("case") not in {"ACTOR", "ANALYTICS-SUM", "ANALYTICS-STATS"}
        and e.get("operation") == "aggregate"
    ]
    concurrent_overlap = _overlap_count(actor_events) + _overlap_count(observer_events)

    def ok(*case_ids: str) -> bool:
        return all(
            turns[c].get("runtime_status") == "ok"
            and not turns[c].get("query_error")
            and not turns[c].get("needs_clarification")
            for c in case_ids
        )

    normal_ids = (
        "FU-01", "FU-06", "TAB-PROC", "TAB-ENTITY-VALUE-DATE", "OPP-OPEN",
        "OBJ-OR", "NEGATION", "PHRASE",
    )
    h4 = turns["H4-CANDIDATE"]
    gates = {
        "normal_search": ok(*normal_ids),
        "followups": ok("FU-02", "FU-03", "FU-04"),
        "pagination": ok("FU-05") and turns["FU-05"].get("route") in {
            "pagination_more", "pagination_exhausted",
        },
        "direct_lookup": ok("DIRECT-CONTRACT", "DIRECT-PROCESS")
        and all(str(turns[c].get("route") or "").startswith("direct_lookup")
                for c in ("DIRECT-CONTRACT", "DIRECT-PROCESS")),
        "actor_profile": ok("ACTOR") and turns["ACTOR"].get("analytical_intent") == "actor_profile",
        "analytics_observer": ok("ANALYTICS-SUM", "ANALYTICS-STATS") and bool(observer_events),
        "h4": h4.get("runtime_status") == "ok" and not h4.get("query_error")
        and (not h4.get("fallback_attempted") or h4.get("fallback_query_status") in {"completed", "empty", "timeout", "error"}),
        "radar": radar.get("status") == "ok" and radar.get("outcome") in {"radar_ok", "radar_empty"},
        "concurrent_runtime_path": concurrent_overlap > 0,
    }
    all_green = (
        all(gates.values()) and fatal == 0
        and contract_errors == 0 and set(datasets) == ALLOWED_DATASETS
    )
    return {
        "git_branch": _git("branch", "--show-current"),
        "git_head": _git("rev-parse", "HEAD"),
        "transport": "soda3",
        "human_turns": len(turn_events),
        "remote_calls": len(ops),
        "operation_counts": dict(Counter(e.get("operation") for e in ops)),
        "datasets": datasets,
        "gates": gates,
        "fatal_transport_errors": fatal + contract_errors,
        "handled_operation_errors": handled_operation_errors,
        "http_contract_errors": contract_errors,
        "timeouts": sum(int(e.get("timeout_count") or 0) for e in turn_events),
        "fallback_turns": sum(bool(e.get("fallback_attempted")) for e in turn_events),
        "actor_profile_partial": bool(turns["ACTOR"].get("actor_profile_partial")),
        "concurrent_overlap_pairs": concurrent_overlap,
        "soda3_only_reproducible_failures": 0 if all_green else None,
        "confirmed_semantic_regressions": 0 if all_green else None,
        "verdict": "SODA3_FULL_RUNTIME_DOGFOOD_GO" if all_green else "NEEDS_ADJUDICATION",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--out", type=Path, default=Path("/tmp/soda3_full_runtime_dogfood.jsonl"))
    parser.add_argument("--radar-db", type=Path, default=Path("data/secoppal.db"))
    parser.add_argument("--radar-name", default="alimentacion costa")
    parser.add_argument("--skip-radar", action="store_true")
    args = parser.parse_args()

    load_dotenv(args.env_file, override=False)
    configured = os.environ.get("SECOPPAL_SOCRATA_TRANSPORT")
    if configured != "soda3":
        raise SystemExit(
            "SECOPPAL_SOCRATA_TRANSPORT=soda3 debe existir antes de construir get_workflow()"
        )

    ledger = Ledger(args.out)
    ledger.emit(
        "preflight", git_branch=_git("branch", "--show-current"),
        git_head=_git("rev-parse", "HEAD"),
        tracked_dirty=bool(_git("status", "--short", "--untracked-files=no")),
        transport_env=configured, actor_profile_env=os.environ.get("SECOPPAL_ACTOR_PROFILE_ENABLED"),
        reference_date=REFERENCE_DATE.isoformat(), started_at=datetime.now(UTC).isoformat(),
    )

    radar_context = nullcontext(None) if args.skip_radar else nullcontext(args.radar_db)
    with isolated_runtime_stores(prefix="soda3_full_dogfood_"):
        # Imports tardíos: el flag de transporte y las rutas aisladas deben existir
        # antes de que Settings/get_workflow se construyan o se cacheen.
        from app.config import get_settings
        from app.service import get_workflow

        get_settings.cache_clear()
        get_workflow.cache_clear()
        workflow = get_workflow()
        selected = workflow.secop_client.transport_name
        same_deps_client = workflow.deps.secop_client is workflow.secop_client
        if selected != "soda3" or not same_deps_client:
            raise SystemExit(
                f"wiring inválido: transport={selected!r} same_deps_client={same_deps_client}"
            )

        workflow.secop_client = RecordingSecopClient(workflow.secop_client, ledger)
        transport = workflow.secop_client._explicit_transport
        transport._http_client = RecordingHttpClient(transport.http_client, ledger)
        ledger.emit(
            "workflow_built", configured_env=configured,
            settings_transport=workflow.settings.secoppal_socrata_transport,
            transport_name=selected,
            deps_client_is_workflow_client=workflow.deps.secop_client is workflow.secop_client,
            client_timeout_s=workflow.secop_client.timeout,
        )

        turns: dict[str, dict[str, Any]] = {}
        for case in TURNS:
            with ledger.scope("soda3", case["id"]):
                turns[case["id"]] = _run_turn(workflow, ledger, case)

        with radar_context as radar_db:
            if radar_db is None:
                radar = {"status": "not_exercised", "reason": "--skip-radar"}
                with ledger.scope("soda3", "RADAR"):
                    ledger.emit("radar_case", **radar)
            else:
                with ledger.scope("soda3", "RADAR"):
                    radar = _run_radar(
                        workflow, ledger, db_path=Path(radar_db), radar_name=args.radar_name,
                    )

    with ledger.scope("summary", "summary"):
        summary = _gate_summary(ledger, turns, radar)
        ledger.emit("summary", **summary)
    ledger.flush()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"eventos: {len(ledger.events)} → {args.out}")
    return 0 if summary["verdict"] == "SODA3_FULL_RUNTIME_DOGFOOD_GO" else 2


if __name__ == "__main__":
    raise SystemExit(main())
