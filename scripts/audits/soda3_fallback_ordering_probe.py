"""Microprobe causal sobre el único bloqueador V1.1: RT-TAB-002__call_002.

Hipótesis falsable: la regresión SODA3 de ese fallback H4 se debe a
``orderingSpecifier=total`` —impuesto por el Runtime Canary V1 a *todas* las
llamadas SODA3— y no a una incapacidad intrínseca de SODA3 para ejecutar ese
SoQL. El fallback productivo elimina deliberadamente el ``ORDER BY`` remoto
para abaratar la consulta, de modo que exigir un orden total contradice el
propósito de la ruta.

Brazos (mismo SoQL congelado, mismo timeout de cliente, mismo App Token):

  A  classic            GET  /resource/{dataset}.json
  B1 soda3_total        POST /api/v3/views/{dataset}/query.json  orderingSpecifier=total
  B2 soda3_default      ídem, **omitiendo** orderingSpecifier
  B3 soda3_no_preserve  ídem, orderingSpecifier=no_preserve
  B4 soda3_discard      ídem, orderingSpecifier=discard

El SoQL se lee EXCLUSIVAMENTE del manifiesto V1 congelado, cuyo SHA-256 se
verifica antes de emitir cualquier request; no se reconstruye por runtime ni se
modifica. Sin retries. No persiste secretos ni cabeceras.

Uso:
    python scripts/audits/soda3_fallback_ordering_probe.py \
      --env-file .env --output /tmp/soda3_fallback_ordering_probe.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

MANIFEST_PATH = Path("/tmp/soda3_runtime_canary_manifest_frozen_v3.json")
MANIFEST_SHA256 = "880e63ecaf2672e217238c234db09d2847c89d95da6648c8c8490d9952c5076a"
TARGET_CALL_ID = "RT-TAB-002__call_002"

ROUNDS = 5
TIMEOUT_SECONDS = 3
SEED = 20260827

# orderingSpecifier efectivo por brazo. ``None`` = omitido del payload.
ARMS: tuple[tuple[str, str, str | None], ...] = (
    ("classic", "classic", None),
    ("soda3_total", "soda3", "total"),
    ("soda3_default", "soda3", None),
    ("soda3_no_preserve", "soda3", "no_preserve"),
    ("soda3_discard", "soda3", "discard"),
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


def load_frozen_operation() -> dict[str, Any]:
    """Devuelve la operación congelada, o aborta con EVIDENCE_GAP."""
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"MICROPROBE: EVIDENCE_GAP (manifiesto ausente: {MANIFEST_PATH})")
    actual = _sha256_file(MANIFEST_PATH)
    if actual != MANIFEST_SHA256:
        raise SystemExit(
            f"MICROPROBE: EVIDENCE_GAP (sha256 del manifiesto no coincide: {actual})"
        )
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    matches = [op for op in manifest["operations"] if op.get("call_id") == TARGET_CALL_ID]
    if len(matches) != 1:
        raise SystemExit(
            f"MICROPROBE: EVIDENCE_GAP ({TARGET_CALL_ID}: {len(matches)} coincidencias)"
        )
    operation = matches[0]
    soql = operation["exact_soql"]
    recorded = operation.get("soql_sha256")
    if recorded and _sha256_bytes(soql.encode("utf-8")) != recorded:
        raise SystemExit("MICROPROBE: EVIDENCE_GAP (el SoQL no coincide con su hash congelado)")
    if "ORDER BY" in soql.upper():
        raise SystemExit("MICROPROBE: EVIDENCE_GAP (el SoQL congelado contiene ORDER BY)")
    return operation


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
    dataset: str,
    soql: str,
    arm: str,
    transport: str,
    ordering_specifier: str | None,
    round_number: int,
    position: int,
    execution_index: int,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp_utc": _utc_now(),
        "execution_index": execution_index,
        "round": round_number,
        "position_in_round": position,
        "arm": arm,
        "transport": transport,
        "ordering_specifier": ordering_specifier,
        "dataset": dataset,
        "call_id": TARGET_CALL_ID,
        "soql_sha256": _sha256_bytes(soql.encode("utf-8")),
        "client_timeout_seconds": TIMEOUT_SECONDS,
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
                timeout=TIMEOUT_SECONDS,
            )
        else:
            payload: dict[str, Any] = {
                "query": soql,
                "timeout": TIMEOUT_SECONDS,
                "includeSynthetic": False,
            }
            if ordering_specifier is not None:
                payload["orderingSpecifier"] = ordering_specifier
            response = session.post(
                f"https://{domain}/api/v3/views/{dataset}/query.json",
                json=payload,
                timeout=TIMEOUT_SECONDS,
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
    # Hash de conjunto (insensible al orden): este SoQL es unordered_sample.
    record["canonical_hash"] = _sha256_bytes("\n".join(sorted(ordered_ids)).encode("utf-8"))
    record["raw_hash"] = _sha256_bytes(
        json.dumps(
            sorted(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in decoded),
            ensure_ascii=False,
        ).encode("utf-8")
    )
    return record


def summarize(executions: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for arm, _transport, _specifier in ARMS:
        rows = [record for record in executions if record["arm"] == arm]
        successes = [record for record in rows if record["status"] == "ok"]
        latencies = sorted(record["elapsed_ms"] for record in rows)
        summary[arm] = {
            "attempts": len(rows),
            "ok": len(successes),
            "timeout": sum(1 for record in rows if record["status"] == "timeout"),
            "other_failure": sum(
                1 for record in rows if record["status"] not in ("ok", "timeout")
            ),
            "statuses": sorted({str(record["status"]) for record in rows}),
            "http_statuses": sorted({str(record["http_status"]) for record in rows}),
            "row_counts": sorted({record["row_count"] for record in successes}),
            "distinct_canonical_hashes": len({record["canonical_hash"] for record in successes}),
            "elapsed_ms_min": latencies[0] if latencies else None,
            "elapsed_ms_max": latencies[-1] if latencies else None,
        }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/soda3_fallback_ordering_probe.jsonl"),
    )
    args = parser.parse_args()

    if args.env_file:
        load_dotenv(args.env_file, override=False)
    app_token = os.getenv("SECOP_APP_TOKEN")
    if not app_token:
        raise SystemExit("SECOP_APP_TOKEN is required for the fallback ordering microprobe")
    domain = os.getenv("DATOS_GOV_DOMAIN", "www.datos.gov.co")

    operation = load_frozen_operation()
    soql = operation["exact_soql"]
    dataset = operation["dataset_id"]

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "SECOPPAL-SODA3-discovery-v1",
            "X-App-Token": app_token,
        }
    )

    rng = random.Random(SEED)
    executions: list[dict[str, Any]] = []
    execution_index = 0
    for round_number in range(1, ROUNDS + 1):
        schedule = list(ARMS)
        rng.shuffle(schedule)
        for position, (arm, transport, ordering_specifier) in enumerate(schedule, start=1):
            execution_index += 1
            executions.append(
                execute_arm(
                    session,
                    domain=domain,
                    dataset=dataset,
                    soql=soql,
                    arm=arm,
                    transport=transport,
                    ordering_specifier=ordering_specifier,
                    round_number=round_number,
                    position=position,
                    execution_index=execution_index,
                )
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        header = {
            "record_kind": "header",
            "probe": "SODA3_FALLBACK_ORDERING_PROBE",
            "started_at_utc": executions[0]["timestamp_utc"],
            "finished_at_utc": _utc_now(),
            "call_id": TARGET_CALL_ID,
            "dataset": dataset,
            "domain": domain,
            "manifest_path": str(MANIFEST_PATH),
            "manifest_sha256": MANIFEST_SHA256,
            "soql_sha256": _sha256_bytes(soql.encode("utf-8")),
            "soql_contains_order_by": False,
            "rounds": ROUNDS,
            "seed": SEED,
            "client_timeout_seconds": TIMEOUT_SECONDS,
            "arms": [
                {"arm": arm, "transport": transport, "orderingSpecifier": specifier}
                for arm, transport, specifier in ARMS
            ],
            "retries": 0,
            "total_requests": len(executions),
        }
        handle.write(json.dumps(header, ensure_ascii=False) + "\n")
        for record in executions:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.write(
            json.dumps(
                {"record_kind": "summary", "per_arm": summarize(executions)},
                ensure_ascii=False,
            )
            + "\n"
        )

    print(json.dumps(summarize(executions), indent=2, ensure_ascii=False))
    print(f"\nJSONL: {args.output}")


if __name__ == "__main__":
    main()
