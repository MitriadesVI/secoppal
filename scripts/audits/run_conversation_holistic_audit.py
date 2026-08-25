"""HCA-1 — holistic conversational audit runner (structural layer).

Drives the REAL workflow (SecopalWorkflow.run_query) with the real parser,
follow-up engine, resolvers and SoQL builder. Only SECOP execution is mocked;
the narrator LLM is stubbed; every writable persistence path is redirected to
a throwaway directory. Read-only against runtime code and production data.

INV-AUDIT-RUNNER-NO-PRODUCTION-WRITES: correr este script no escribe en los
stores reales del proyecto, sin variables de entorno previas y sea cual sea el
directorio de trabajo. El aislamiento vive en ``scripts/audits/audit_isolation``
y se aplica ANTES de importar ``app``; los recursos de solo lectura (gazetteer
de alias, índice SQLite de actores) se siguen leyendo de su ubicación real.

Usage:
    .venv/bin/python scripts/audits/run_conversation_holistic_audit.py \
        [--corpus tests/fixtures/audits/conversation_holistic_v1.yaml] \
        [--out <dir>] [--only CASE_ID_PREFIX]

Outputs (machine-readable, written under --out, default a fresh temp dir que
NO se borra — la salida es del usuario, no persistencia interna):
    results.jsonl   one record per executed turn with full evidence
    verdicts.json   per-case PASS/FAIL + failed check details + metrics
    isolation.json  rutas efectivas de cada store escribible de la corrida

Check DSL (per turn; all optional):
    expect: execute | block          outcome contract
    ds: contratos | procesos         dataset selected
    eq: {param: value}               exact match on parsed_params[param]
    has: [param, ...]                param present and truthy
    absent: [param, ...]             param absent or falsy
    objeto_has: [substr, ...]        substring present in some objeto term
    objeto_not: [substr, ...]        substring absent from every objeto term
    soql_has: [substr, ...]          substring in generated SoQL (accent/case-insensitive)
    soql_not: [substr, ...]          substring NOT in generated SoQL
    resp_has: [substr, ...]          substring in user-facing response
    resp_not: [substr, ...]          substring NOT in response

Case kinds:
    turns: [...]                     conversation (single- or multi-turn)
    clock: YYYY-MM-DD                optional frozen civil clock for every turn
    same_frame_as: OTHER_CASE_ID     metamorphic — material frame must equal
    differ_from: {case: ID, fields: [...]}   anti-metamorphic — fields must differ
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import unicodedata
from datetime import date as Date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
# El worktree de auditoría no versiona data/: el índice de producción vive en
# el checkout principal. El resolver lo abre mode=ro&immutable=1 — recurso de
# solo lectura, por eso no se redirige al sandbox.
_MAIN_INDEX = Path("/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite")
os.environ.setdefault("SECOP_ACTOR_ALIAS_INDEX_PATH", str(_MAIN_INDEX))
os.environ.pop("DEEPSEEK_API_KEY", None)

import yaml  # noqa: E402

from scripts.audits.audit_isolation import (  # noqa: E402
    AuditIsolation,
    isolated_runtime_stores,
)

# Campos materiales del frame para comparación metamórfica.
MATERIAL_FIELDS = (
    "dataset", "objeto", "objeto_or", "objeto_exclude", "valor_min", "valor_max",
    "fecha_desde", "fecha_hasta", "entidad_resolved", "departamento_resolved",
    "ciudad", "modalidad_include_procedure_ids", "modalidad_exclude_procedure_ids",
    "contract_type_include", "estado_family", "estado_contrato",
    "estado_del_procedimiento", "ordering_signal", "intent_type",
)


def _norm(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _mk_rows(n: int = 5) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append({
            "nombre_del_procedimiento": f"Objeto sintetico {i}",
            "objeto_del_contrato": f"Objeto sintetico {i}",
            "descripcion_del_proceso": f"Descripcion {i}",
            "descripci_n_del_procedimiento": f"Descripcion {i}",
            "entidad": "ENTIDAD SINTETICA",
            "nombre_entidad": "ENTIDAD SINTETICA",
            "departamento": "Atlantico",
            "departamento_entidad": "Atlantico",
            "ciudad_entidad": "Barranquilla",
            "ciudad": "Barranquilla",
            "precio_base": str(100_000_000 * (i + 1)),
            "valor_del_contrato": str(100_000_000 * (i + 1)),
            "estado_de_apertura_del_proceso": "Abierto",
            "estado_contrato": "Activo",
            "fecha_de_publicacion_del": "2026-02-15T00:00:00.000",
            "fecha_de_firma": "2026-02-15T00:00:00.000",
            "modalidad_de_contratacion": "Licitacion Publica",
            "proveedor_adjudicado": "PROVEEDOR SINTETICO SAS",
            "documento_proveedor": "900123456",
            "referencia_del_contrato": f"REF-{i}",
            "referencia_del_proceso": f"PROC-{i}",
            "id_del_proceso": f"ID-{i}",
            "id_contrato": f"CO-{i}",
            "urlproceso": {"url": "https://example.com"},
        })
    return rows


def material_frame(params: dict) -> dict:
    out = {}
    for k in MATERIAL_FIELDS:
        v = (params or {}).get(k)
        if v in (None, [], "", {}, False):
            continue
        if isinstance(v, list):
            v = sorted(str(x) for x in v)
        out[k] = v
    return out


def _case_clock(case: dict) -> Date | None:
    value = case.get("clock")
    if value is None:
        return None
    if isinstance(value, Date):
        return value
    try:
        return Date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(
            f"case {case.get('id')}: invalid clock {value!r}"
        ) from exc


def check_turn(turn: dict, record: dict) -> list[str]:
    fails: list[str] = []
    params = record.get("params") or {}
    soql = _norm(record.get("soql") or "")
    resp = _norm(record.get("response") or "")

    expect = turn.get("expect")
    clar = bool(record.get("needs_clarification"))
    if expect == "block" and not clar:
        fails.append("expected block, got execution")
    if expect == "execute":
        if clar:
            fails.append("expected execution, got clarification")
        elif not record.get("soql"):
            fails.append("expected execution, empty SoQL")
    if "ds" in turn and params.get("dataset") != turn["ds"]:
        fails.append(f"dataset={params.get('dataset')!r}, expected {turn['ds']!r}")
    for key, want in (turn.get("eq") or {}).items():
        got = params.get(key)
        if got != want:
            fails.append(f"{key}={got!r}, expected {want!r}")
    for key in turn.get("has") or []:
        if params.get(key) in (None, [], "", {}, False):
            fails.append(f"missing param {key}")
    for key in turn.get("absent") or []:
        if params.get(key) not in (None, [], "", {}, False):
            fails.append(f"param {key} present ({params.get(key)!r}), expected absent")
    objeto_terms = [_norm(str(t)) for t in (params.get("objeto") or [])]
    objeto_terms += [_norm(str(t)) for grp in (params.get("objeto_or") or []) for t in (grp if isinstance(grp, list) else [grp])]
    for sub in turn.get("objeto_has") or []:
        if not any(_norm(sub) in t for t in objeto_terms):
            fails.append(f"objeto lacks {sub!r} (objeto={objeto_terms})")
    for sub in turn.get("objeto_not") or []:
        if any(_norm(sub) in t for t in objeto_terms):
            fails.append(f"objeto leaks {sub!r} (objeto={objeto_terms})")
    for sub in turn.get("soql_has") or []:
        if _norm(sub) not in soql:
            fails.append(f"soql lacks {sub!r}")
    for sub in turn.get("soql_not") or []:
        if _norm(sub) in soql:
            fails.append(f"soql contains {sub!r}")
    for sub in turn.get("resp_has") or []:
        if _norm(sub) not in resp:
            fails.append(f"response lacks {sub!r}")
    for sub in turn.get("resp_not") or []:
        if _norm(sub) in resp:
            fails.append(f"response contains {sub!r}")
    return fails


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=str(
        ROOT / "tests" / "fixtures" / "audits" / "conversation_holistic_v1.yaml"))
    ap.add_argument("--out", default=None)
    ap.add_argument("--only", default=None)
    args = ap.parse_args()

    # La salida sobrevive a la corrida: es el producto que el usuario lee.
    # Sin --out se crea un temporal propio que NADIE borra — distinto del
    # sandbox de stores, que se elimina siempre.
    out_dir = Path(args.out) if args.out else Path(
        tempfile.mkdtemp(prefix="hca1_audit_out_"))

    with isolated_runtime_stores() as stores:
        run_audit(args, out_dir, stores)


def run_audit(args, out_dir: Path, stores: AuditIsolation) -> None:
    # Importar app DENTRO del sandbox: ningún módulo debe materializar rutas
    # de producción en import time.
    from app.config import Settings, get_settings

    get_settings.cache_clear()

    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow

    corpus_path = Path(args.corpus)
    corpus_bytes = corpus_path.read_bytes()
    corpus_sha = hashlib.sha256(corpus_bytes).hexdigest()
    corpus = yaml.safe_load(corpus_bytes)
    cases = corpus["cases"]
    if args.only:
        cases = [c for c in cases if str(c["id"]).startswith(args.only)]

    out_dir.mkdir(parents=True, exist_ok=True)
    results_f = (out_dir / "results.jsonl").open("w", encoding="utf-8")

    # Overrides explícitos: el aislamiento no depende de que pydantic lea el
    # entorno ni de los defaults de producción de Settings.
    wf = SecopalWorkflow(Settings(**stores.settings_overrides))

    def _query(dataset_id, soql_query, **kw):
        return _mk_rows(5)

    def _count(dataset_id, soql_query, **kw):
        return 5

    wf.secop_client.query = _query  # type: ignore[method-assign]
    wf.secop_client.count = _count  # type: ignore[method-assign]
    try:
        wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore
    except Exception:
        pass
    wf.conv_store = ConversationStore(base_path=stores.conversation_dir)

    # Evidencia de rutas efectivas sobre los objetos realmente construidos,
    # no sobre la configuración declarada. El test de no-escritura la lee.
    effective_stores = {
        "sandbox_root": str(stores.root),
        "feedback_jsonl": str(wf.feedback.path),
        "feedback_db": str(wf.feedback.db_path),
        "conversation_dir": str(wf.conv_store.base_path),
        "radar_db": str(wf.radar_store.db_path),
        "modality_telemetry": str(wf.settings.modality_followup_telemetry_path),
    }
    outside = sorted(
        name for name, value in effective_stores.items()
        if name != "sandbox_root" and not stores.contains(value)
    )
    (out_dir / "isolation.json").write_text(
        json.dumps(
            {"stores": effective_stores, "outside_sandbox": outside},
            ensure_ascii=False, indent=1,
        ),
        encoding="utf-8",
    )
    if outside:
        raise RuntimeError(
            "INV-AUDIT-RUNNER-NO-PRODUCTION-WRITES violado: stores fuera del "
            f"sandbox: {outside}"
        )

    frames_by_case: dict[str, dict] = {}
    verdicts: list[dict] = []

    for case in cases:
        cid = str(case["id"])
        clock = _case_clock(case)
        chat_id = f"hca1-{cid}"
        fails: list[str] = []
        error: str | None = None
        last_record: dict = {}
        for i, turn in enumerate(case.get("turns") or []):
            q = turn["q"]
            try:
                result = wf.run_query(
                    q,
                    channel="telegram",
                    chat_id=chat_id,
                    today=clock,
                )
            except Exception as exc:  # noqa: BLE001
                error = f"turn {i + 1} {q!r}: {type(exc).__name__}: {exc}"
                break
            record = {
                "case": cid, "turn": i + 1, "q": q,
                "route_reason": result.get("route_reason"),
                "needs_llm": result.get("needs_llm"),
                "needs_clarification": result.get("needs_clarification"),
                "followup": result.get("followup"),
                "params": result.get("parsed_params"),
                "soql": result.get("soql_query"),
                "response": (result.get("response") or "")[:600],
            }
            results_f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            last_record = record
            turn_fails = check_turn(turn, record)
            fails.extend(f"turn {i + 1} {q!r}: {f}" for f in turn_fails)

        frames_by_case[cid] = material_frame(last_record.get("params") or {})

        ref = case.get("same_frame_as")
        if ref:
            other = frames_by_case.get(str(ref))
            if other is None:
                fails.append(f"same_frame_as {ref}: reference not yet executed")
            elif other != frames_by_case[cid]:
                diff = {
                    k: (other.get(k), frames_by_case[cid].get(k))
                    for k in set(other) | set(frames_by_case[cid])
                    if other.get(k) != frames_by_case[cid].get(k)
                }
                fails.append(f"metamorphic mismatch vs {ref}: {diff}")
        anti = case.get("differ_from")
        if anti:
            other = frames_by_case.get(str(anti["case"]))
            if other is None:
                fails.append(f"differ_from {anti['case']}: reference not yet executed")
            else:
                for fld in anti.get("fields") or []:
                    if other.get(fld) == frames_by_case[cid].get(fld):
                        fails.append(
                            f"anti-metamorphic: field {fld} identical to "
                            f"{anti['case']} ({other.get(fld)!r})"
                        )

        verdicts.append({
            "id": cid,
            "category": case.get("category"),
            "severity_if_failed": case.get("severity_if_failed"),
            "notes": case.get("notes"),
            "status": "ERROR" if error else ("FAIL" if fails else "PASS"),
            "error": error,
            "fails": fails,
            "final_frame": frames_by_case[cid],
        })

    results_f.close()
    n_pass = sum(1 for v in verdicts if v["status"] == "PASS")
    n_fail = sum(1 for v in verdicts if v["status"] == "FAIL")
    n_err = sum(1 for v in verdicts if v["status"] == "ERROR")
    summary = {
        "corpus": str(corpus_path),
        "corpus_sha256": corpus_sha,
        "cases": len(verdicts),
        "pass": n_pass, "fail": n_fail, "error": n_err,
        "verdicts": verdicts,
    }
    (out_dir / "verdicts.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1, default=str),
        encoding="utf-8",
    )
    print(json.dumps({
        "corpus_sha256": corpus_sha, "cases": len(verdicts),
        "pass": n_pass, "fail": n_fail, "error": n_err,
        "out": str(out_dir),
    }, indent=1))


if __name__ == "__main__":
    main()
