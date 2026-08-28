"""Replay focal de política de ordenamiento sobre la clase H4 del corpus congelado.

El Runtime Canary V1 aplicó ``orderingSpecifier=total`` a las 249 llamadas, incluidas
las que el runtime emite *deliberadamente sin* ``ORDER BY``. El microprobe causal
(``docs/audits/2026-08-27_SODA3_FALLBACK_ORDERING_PROBE.md``) demostró sobre
``RT-TAB-002__call_002`` que esa opción —y no SODA3— causaba el timeout reproducible.

Este replay responde una sola pregunta, sin reabrir la campaña:

    ¿``orderingSpecifier=no_preserve`` es operacionalmente seguro para la clase exacta
    de operaciones donde el fallback H4 de ``execution.py`` retiró el ``ORDER BY``?

Brazos (mismo SoQL congelado, mismo presupuesto capturado, mismo App Token):

  A  classic            GET  /resource/{dataset}.json
  B  soda3_no_preserve  POST /api/v3/views/{dataset}/query.json  orderingSpecifier=no_preserve

No se prueban ``total``, ``discard`` ni el default: el microprobe ya los separó
causalmente. No se reconstruye SoQL por runtime; nada de ``app/`` se importa ni se
ejecuta. Sin retries. No persiste token ni cabeceras.

Uso:
    python scripts/audits/soda3_h4_policy_replay.py \
      --env-file .env --output /tmp/soda3_h4_policy_replay.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

MANIFEST_PATH = Path("/tmp/soda3_runtime_canary_manifest_frozen_v3.json")
MANIFEST_SHA256 = "880e63ecaf2672e217238c234db09d2847c89d95da6648c8c8490d9952c5076a"

# Replay V1 congelado: única fuente de la alcanzabilidad de cada fallback (el
# resultado SODA3 de su query PRINCIPAL). No se reejecutan primarias aquí.
V1_REPLAY_PATH = Path("/tmp/soda3_runtime_canary_replay_v3.jsonl")
V1_REPLAY_SHA256 = "0e35f51afb5483d98352ab6420e809266283546afddb103fbdc3e3735feb7bb7"

# ── Firma de control flow del fallback H4 ────────────────────────────────────────
# Espejo READ-ONLY de constantes productivas; el script no importa app/.
#   app/core/deadline.py:31  PRIMARY_FETCH_SECONDS  = 8.0
#   app/core/deadline.py:32  FALLBACK_FETCH_SECONDS = 3.0
#   app/core/workflow/actions/execution.py:193  fb_params["limit"] = 50  (WIDE_LIMIT)
#   app/core/workflow/actions/execution.py:194  build(..., include_order=False)
# El fallback vive en el `except` y solo se arma con `is_timeout` — de ahí que su
# antecesor inmediato en el mismo caso sea SIEMPRE una query principal agotada.
PRIMARY_FETCH_SECONDS = 8.0
FALLBACK_FETCH_SECONDS = 3.0
WIDE_LIMIT = 50

ROUNDS = 3
SEED = 20260827
ORDERING_SPECIFIER = "no_preserve"

ARMS: tuple[tuple[str, str], ...] = (
    ("classic", "classic"),
    ("soda3_no_preserve", "soda3"),
)

ID_FIELDS = ("id_contrato", "id_del_proceso")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _evidence_gap(reason: str) -> "SystemExit":
    return SystemExit(f"FINAL_REPLAY = EVIDENCE_GAP ({reason})")


def _verified_json(path: Path, expected_sha: str, label: str) -> Any:
    if not path.exists():
        raise _evidence_gap(f"{label} ausente: {path}")
    actual = _sha256_file(path)
    if actual != expected_sha:
        raise _evidence_gap(f"sha256 de {label} no coincide: {actual}")
    return path


def load_manifest() -> dict[str, Any]:
    _verified_json(MANIFEST_PATH, MANIFEST_SHA256, "el manifiesto")
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _soql_limit(soql: str) -> int | None:
    match = re.search(r"\bLIMIT\s+(\d+)", soql, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def identify_h4_fallbacks(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Clasifica por CONTROL FLOW, no por ``"ORDER BY" not in soql``.

    El corpus tiene 42 operaciones sin ``ORDER BY``; la inmensa mayoría son ``count``,
    ``aggregate`` y ``direct_lookup`` legítimamente no ordenados. Un fallback H4 es la
    conjunción —verificable en el manifiesto congelado— de las seis condiciones que el
    código impone y ninguna otra ruta reproduce:

      1. es una ``query``;
      2. corrió bajo el presupuesto fijo del fallback (``FALLBACK_FETCH_SECONDS``);
      3. su antecesor inmediato en el mismo caso humano es una ``query``
      4. …que agotó con ``TimeoutError`` bajo ``PRIMARY_FETCH_SECONDS`` y llevaba
         ``ORDER BY`` (la condición que abre el ``except``);
      5. no lleva ``ORDER BY`` (consecuencia de ``include_order=False``);
      6. lleva ``LIMIT 50`` (``WIDE_LIMIT`` que el fallback impone).
    """
    operations = manifest.get("operations") or []
    if not operations:
        raise _evidence_gap("el manifiesto no contiene operaciones")

    by_case: dict[str, dict[int, dict[str, Any]]] = {}
    for op in operations:
        by_case.setdefault(op["human_case_id"], {})[op["call_order"]] = op

    fallbacks: list[dict[str, Any]] = []
    for op in operations:
        if op["operation"] != "query":
            continue
        if float(op["captured_timeout_seconds"]) != FALLBACK_FETCH_SECONDS:
            continue
        primary = by_case[op["human_case_id"]].get(op["call_order"] - 1)
        if primary is None or primary["operation"] != "query":
            continue
        if primary["capture_status"] != "error":
            continue
        if primary.get("capture_error_class") != "TimeoutError":
            continue
        if float(primary["captured_timeout_seconds"]) != PRIMARY_FETCH_SECONDS:
            continue
        if "ORDER BY" not in primary["exact_soql"].upper():
            continue
        if "ORDER BY" in op["exact_soql"].upper():
            continue
        if _soql_limit(op["exact_soql"]) != WIDE_LIMIT:
            continue
        recorded = op.get("soql_sha256")
        if recorded and _sha256_bytes(op["exact_soql"].encode("utf-8")) != recorded:
            raise _evidence_gap(f"{op['call_id']}: el SoQL no coincide con su hash congelado")
        fallbacks.append({**op, "primary_call_id": primary["call_id"]})

    if not fallbacks:
        raise _evidence_gap("ninguna operación satisface la firma de control flow H4")
    fallbacks.sort(key=lambda item: item["call_id"])
    return fallbacks


def load_v1_primary_outcomes(fallbacks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Alcanzabilidad desde el replay V1 congelado, sin reejecutar primarias.

    Las primarias SÍ llevan ``ORDER BY``, de modo que una política consciente de la
    ruta las mantendría en ``total`` — exactamente la configuración bajo la que V1 ya
    las midió. Su evidencia es reutilizable sin repetir la campaña.
    """
    _verified_json(V1_REPLAY_PATH, V1_REPLAY_SHA256, "el replay V1")
    primary_ids = {item["primary_call_id"] for item in fallbacks}
    statuses: dict[str, list[str]] = {call_id: [] for call_id in primary_ids}
    with V1_REPLAY_PATH.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("event_type") != "execution":
                continue
            if record.get("transport") != "soda3":
                continue
            call_id = record.get("case_id")
            if call_id in statuses:
                statuses[call_id].append(str(record.get("status")))

    outcomes: dict[str, dict[str, Any]] = {}
    for item in fallbacks:
        primary_id = item["primary_call_id"]
        rounds = statuses.get(primary_id, [])
        if not rounds:
            raise _evidence_gap(f"{primary_id}: sin ejecuciones SODA3 en el replay V1")
        primary_ok = all(status == "ok" for status in rounds)
        outcomes[item["call_id"]] = {
            "primary_call_id": primary_id,
            "v1_primary_soda3_statuses": rounds,
            "v1_primary_soda3_all_ok": primary_ok,
            # Si la principal SODA3 responde, el runtime devuelve y el `except` nunca
            # se abre: ese fallback es inalcanzable bajo un runtime SODA3.
            "fallback_reachable_under_soda3_primary": not primary_ok,
        }
    return outcomes


def _row_ids(rows: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for row in rows:
        for field in ID_FIELDS:
            value = row.get(field)
            if value is not None:
                ids.append(str(value))
                break
    return ids


def execute_arm(
    session: requests.Session,
    *,
    domain: str,
    operation: dict[str, Any],
    arm: str,
    transport: str,
    round_number: int,
    position: int,
    execution_index: int,
) -> dict[str, Any]:
    soql = operation["exact_soql"]
    dataset = operation["dataset_id"]
    timeout_seconds = int(operation["captured_timeout_seconds"])
    record: dict[str, Any] = {
        "record_kind": "execution",
        "timestamp_utc": _utc_now(),
        "execution_index": execution_index,
        "round": round_number,
        "position_in_round": position,
        "case_id": operation["human_case_id"],
        "operation_id": operation["call_id"],
        "primary_call_id": operation["primary_call_id"],
        "category": operation["category"],
        "runtime_route": operation["runtime_route"],
        "operation": operation["operation"],
        "dataset": dataset,
        "arm": arm,
        "transport": transport,
        "ordering_specifier": ORDERING_SPECIFIER if transport == "soda3" else None,
        "soql_sha256": _sha256_bytes(soql.encode("utf-8")),
        "frozen_soql_sha256": operation.get("soql_sha256"),
        "timeout_budget_seconds": timeout_seconds,
        "captured_timeout_seconds": operation["captured_timeout_seconds"],
        "elapsed_ms": None,
        "status": None,
        "http_status": None,
        "error_class": None,
        "response_bytes": 0,
        "row_count": None,
        "ordered_ids": [],
        "raw_hash": None,
        "canonical_hash": None,
        "content_type": None,
    }
    started = time.perf_counter()
    try:
        if transport == "classic":
            response = session.get(
                f"https://{domain}/resource/{dataset}.json",
                params={"$query": soql},
                timeout=timeout_seconds,
            )
        else:
            response = session.post(
                f"https://{domain}/api/v3/views/{dataset}/query.json",
                json={
                    "query": soql,
                    "timeout": timeout_seconds,
                    "includeSynthetic": False,
                    "orderingSpecifier": ORDERING_SPECIFIER,
                },
                timeout=timeout_seconds,
            )
    except requests.Timeout as exc:
        record["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        record["status"] = "timeout"
        record["error_class"] = type(exc).__name__
        return record
    except requests.RequestException as exc:
        record["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        record["status"] = "network_error"
        record["error_class"] = type(exc).__name__
        return record

    record["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
    record["http_status"] = response.status_code
    record["response_bytes"] = len(response.content)
    record["content_type"] = (
        response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    )
    if response.status_code not in (200, 202):
        record["status"] = "http_error"
        record["error_class"] = "HTTPError"
        return record

    try:
        decoded = response.json()
    except ValueError as exc:
        record["status"] = "decode_error"
        record["error_class"] = type(exc).__name__
        return record
    if not isinstance(decoded, list) or not all(isinstance(row, dict) for row in decoded):
        record["status"] = "shape_error"
        record["error_class"] = "UnexpectedPayload"
        return record

    ordered_ids = _row_ids(decoded)
    record["status"] = "ok"
    record["row_count"] = len(decoded)
    record["ordered_ids"] = ordered_ids
    # Hash de CONJUNTO: la ruta es `unordered_sample`; el orden no es contractual.
    record["canonical_hash"] = _sha256_bytes("\n".join(sorted(ordered_ids)).encode("utf-8"))
    record["raw_hash"] = _sha256_bytes(
        json.dumps(
            sorted(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in decoded),
            ensure_ascii=False,
        ).encode("utf-8")
    )
    return record


def _arm_view(records: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [item for item in records if item["status"] == "ok"]
    return {
        "attempts": len(records),
        "ok": len(ok),
        "timeout": sum(1 for item in records if item["status"] == "timeout"),
        "http_error": sum(1 for item in records if item["status"] == "http_error"),
        "contract_error": sum(
            1 for item in records
            if item["status"] in ("decode_error", "shape_error", "network_error")
        ),
        "statuses": sorted({str(item["status"]) for item in records}),
        "http_statuses": sorted({str(item["http_status"]) for item in records}),
        "row_counts": sorted({item["row_count"] for item in ok}),
        "distinct_canonical_hashes": len({item["canonical_hash"] for item in ok}),
        "elapsed_ms_min": min((item["elapsed_ms"] for item in records), default=None),
        "elapsed_ms_max": max((item["elapsed_ms"] for item in records), default=None),
    }


def adjudicate(
    executions: list[dict[str, Any]],
    fallbacks: list[dict[str, Any]],
    reachability: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    per_operation: list[dict[str, Any]] = []
    soda3_only_regressions: list[str] = []
    soda3_only_reachable: list[str] = []

    for item in fallbacks:
        call_id = item["call_id"]
        rows = [record for record in executions if record["operation_id"] == call_id]
        classic = [record for record in rows if record["transport"] == "classic"]
        soda3 = [record for record in rows if record["transport"] == "soda3"]
        classic_view = _arm_view(classic)
        soda3_view = _arm_view(soda3)
        classic_reproducible_ok = classic_view["ok"] == ROUNDS
        soda3_reproducible_fail = soda3_view["ok"] == 0 and soda3_view["attempts"] == ROUNDS
        regression = classic_reproducible_ok and soda3_reproducible_fail
        reach = reachability[call_id]
        if regression:
            soda3_only_regressions.append(call_id)
            if reach["fallback_reachable_under_soda3_primary"]:
                soda3_only_reachable.append(call_id)

        ok_hashes_classic = {r["canonical_hash"] for r in classic if r["status"] == "ok"}
        ok_hashes_soda3 = {r["canonical_hash"] for r in soda3 if r["status"] == "ok"}
        per_operation.append({
            "operation_id": call_id,
            "case_id": item["human_case_id"],
            "human_query": item["human_query"],
            "primary_call_id": item["primary_call_id"],
            "dataset": item["dataset_id"],
            "timeout_budget_seconds": int(item["captured_timeout_seconds"]),
            "classic": classic_view,
            "soda3_no_preserve": soda3_view,
            "soda3_only_regression": regression,
            **reach,
            # Evidencia adicional, NO contractual: la ruta es unordered_sample.
            "set_identity_observed": bool(
                ok_hashes_classic and ok_hashes_classic == ok_hashes_soda3
            ),
        })

    classic_all = [r for r in executions if r["transport"] == "classic"]
    soda3_all = [r for r in executions if r["transport"] == "soda3"]
    classic_view = _arm_view(classic_all)
    soda3_view = _arm_view(soda3_all)

    integrity_soql = all(
        record["frozen_soql_sha256"] in (None, record["soql_sha256"]) for record in executions
    )
    integrity_timeout = all(
        record["timeout_budget_seconds"] == int(record["captured_timeout_seconds"])
        for record in executions
    )
    integrity_coverage = (
        len({record["operation_id"] for record in executions}) == len(fallbacks)
        and len(executions) == len(fallbacks) * len(ARMS) * ROUNDS
    )
    g1 = integrity_soql and integrity_timeout and integrity_coverage
    g2 = soda3_view["http_error"] == 0 and soda3_view["contract_error"] == 0
    g3 = (
        soda3_view["timeout"] <= classic_view["timeout"]
        and not soda3_only_reachable
    )
    blocker = next(
        (row for row in per_operation if row["operation_id"] == "RT-TAB-002__call_002"),
        None,
    )
    g4 = bool(blocker) and not blocker["soda3_only_regression"]

    policy = "PASS" if (g1 and g2 and g3 and g4) else "FAIL"
    return {
        "record_kind": "adjudication",
        "h4_operations_identified": len(fallbacks),
        "requests_executed": len(executions),
        "classic": classic_view,
        "soda3_no_preserve": soda3_view,
        "per_operation": per_operation,
        "soda3_only_regressions": soda3_only_regressions,
        "soda3_only_reachable_regressions": soda3_only_reachable,
        "http_contract_errors": soda3_view["http_error"] + soda3_view["contract_error"],
        "gates": {
            "G1_integrity": g1,
            "G1_exact_soql_preserved": integrity_soql,
            "G1_timeout_budget_preserved": integrity_timeout,
            "G1_full_coverage": integrity_coverage,
            "G2_no_errors": g2,
            "G3_stability": g3,
            "G4_original_blocker_cleared": g4,
        },
        "rt_tab_002_status": "PASS" if g4 else "FAIL",
        "h4_no_preserve_policy": policy,
        "remaining_true_migration_blockers": len(soda3_only_reachable),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path("/tmp/soda3_h4_policy_replay.jsonl"),
    )
    args = parser.parse_args()

    if args.env_file:
        load_dotenv(args.env_file, override=False)
    app_token = os.getenv("SECOP_APP_TOKEN")
    if not app_token:
        raise SystemExit("SECOP_APP_TOKEN is required for the H4 policy replay")
    domain = os.getenv("DATOS_GOV_DOMAIN", "www.datos.gov.co")

    manifest = load_manifest()
    fallbacks = identify_h4_fallbacks(manifest)
    reachability = load_v1_primary_outcomes(fallbacks)

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "SECOPPAL-SODA3-discovery-v1",
        "X-App-Token": app_token,
    })

    rng = random.Random(SEED)
    executions: list[dict[str, Any]] = []
    schedules: list[dict[str, Any]] = []
    execution_index = 0
    for round_number in range(1, ROUNDS + 1):
        plan: list[tuple[dict[str, Any], str, str]] = []
        order = list(fallbacks)
        rng.shuffle(order)
        for operation in order:
            arms = list(ARMS)
            rng.shuffle(arms)  # aleatoriza el orden A/B dentro de la operación
            for arm, transport in arms:
                plan.append((operation, arm, transport))
        schedules.append({
            "round": round_number,
            "plan": [f"{op['call_id']}:{arm}" for op, arm, _ in plan],
        })
        for position, (operation, arm, transport) in enumerate(plan, start=1):
            execution_index += 1
            executions.append(execute_arm(
                session,
                domain=domain,
                operation=operation,
                arm=arm,
                transport=transport,
                round_number=round_number,
                position=position,
                execution_index=execution_index,
            ))

    adjudication = adjudicate(executions, fallbacks, reachability)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "record_kind": "header",
            "probe": "SODA3_H4_POLICY_REPLAY",
            "started_at_utc": executions[0]["timestamp_utc"],
            "finished_at_utc": _utc_now(),
            "domain": domain,
            "manifest_path": str(MANIFEST_PATH),
            "manifest_sha256": MANIFEST_SHA256,
            "v1_replay_path": str(V1_REPLAY_PATH),
            "v1_replay_sha256": V1_REPLAY_SHA256,
            "rounds": ROUNDS,
            "seed": SEED,
            "retries": 0,
            "ordering_specifier": ORDERING_SPECIFIER,
            "includeSynthetic": False,
            "h4_signature": {
                "operation": "query",
                "captured_timeout_seconds": FALLBACK_FETCH_SECONDS,
                "primary_timeout_seconds": PRIMARY_FETCH_SECONDS,
                "primary_error_class": "TimeoutError",
                "wide_limit": WIDE_LIMIT,
                "order_by_present": False,
            },
            "h4_operations": [
                {
                    "operation_id": item["call_id"],
                    "primary_call_id": item["primary_call_id"],
                    "dataset": item["dataset_id"],
                    "category": item["category"],
                    "soql_sha256": item["soql_sha256"],
                    "timeout_budget_seconds": int(item["captured_timeout_seconds"]),
                }
                for item in fallbacks
            ],
            "schedules": schedules,
            "total_requests": len(executions),
        }, ensure_ascii=False) + "\n")
        for record in executions:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.write(json.dumps(adjudication, ensure_ascii=False) + "\n")

    print(json.dumps(adjudication, indent=2, ensure_ascii=False))
    print(f"\nJSONL: {args.output}")


if __name__ == "__main__":
    main()
