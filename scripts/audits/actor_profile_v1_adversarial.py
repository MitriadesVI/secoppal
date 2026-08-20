#!/usr/bin/env python3
"""APV1-AA — auditoría adversarial de aceptación de Actor Profile v1.

Subcomandos:

    acquire   descubre casos reales en SECOP y escribe evidencia de fuente
    freeze    estratifica, congela el corpus y calcula el SHA256
    run       ejecuta SECOPPAL sobre el corpus congelado
    oracle    compara perfiles contra SoQL independiente (no usa SoQLBuilder)

La adquisición usa una muestra ancla ordenada por ``:id`` —el patrón que
AP3-B ya validó— en vez de agregar el universo: nunca descarga contratos
crudos masivamente ni escanea 6M de filas.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIXTURES = ROOT / "tests" / "fixtures" / "actor_profile_adversarial"
DATASET = "jbjy-vk9h"
SEED = 20260819
ANCHOR_PAGES = ((0, 20_000), (1_500_000, 15_000), (4_000_000, 15_000))

_CALLS = {"n": 0}


def _env() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def soql(query: str, timeout: int = 180) -> list[dict]:
    env = _env()
    domain = env.get("DATOS_GOV_DOMAIN") or "www.datos.gov.co"
    token = env.get("SECOP_APP_TOKEN") or ""
    url = f"https://{domain}/resource/{DATASET}.json?$query=" + urllib.parse.quote(query)
    request = urllib.request.Request(url, headers={"X-App-Token": token} if token else {})
    _CALLS["n"] += 1
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------


def cmd_acquire(args: argparse.Namespace) -> int:
    """Muestra ancla + agregaciones acotadas por documento seleccionado."""
    raw_rows: list[dict] = []
    for offset, limit in ANCHOR_PAGES:
        page = soql(
            "SELECT documento_proveedor, tipodocproveedor, proveedor_adjudicado, "
            "id_contrato, fecha_de_firma, departamento "
            f"ORDER BY :id LIMIT {limit} OFFSET {offset}"
        )
        raw_rows.extend(page)
        print(f"  ancla offset={offset} limit={limit} -> {len(page)} filas", flush=True)

    write_jsonl(FIXTURES / "_anchor_sample.jsonl", raw_rows)
    print(f"anchor rows: {len(raw_rows)}  llamadas={_CALLS['n']}")
    return 0


def cmd_confirm(args: argparse.Namespace) -> int:
    """Evidencia de fuente por documento: nombres, tipos y volumen reales.

    Es ORACLE C. Se consulta en lotes con ``IN`` para no emitir una llamada
    por caso, y nunca descarga contratos crudos: solo grupos agregados.
    """
    selection = json.loads((FIXTURES / "_selection.json").read_text(encoding="utf-8"))
    documents = sorted({row["document"] for rows in selection.values() for row in rows})
    evidence: list[dict] = []
    batch = 40
    for offset in range(0, len(documents), batch):
        chunk = documents[offset:offset + batch]
        values = ", ".join("'" + d.replace("'", "''") + "'" for d in chunk)
        rows = soql(
            "SELECT documento_proveedor, tipodocproveedor, proveedor_adjudicado, "
            "count(*) AS cnt, min(fecha_de_firma) AS first_signature, "
            "max(fecha_de_firma) AS last_signature "
            f"WHERE documento_proveedor IN ({values}) "
            "GROUP BY documento_proveedor, tipodocproveedor, proveedor_adjudicado "
            "ORDER BY documento_proveedor, cnt DESC LIMIT 20000"
        )
        evidence.extend(rows)
        print(f"  lote {offset // batch + 1}: {len(chunk)} docs -> {len(rows)} grupos", flush=True)
    write_jsonl(FIXTURES / "source_evidence.jsonl", evidence)
    print(f"source evidence: {len(evidence)} grupos, llamadas={_CALLS['n']}")
    return 0


def _pipeline():
    """parse -> resolve_actor_name -> plan, sin red y sin Settings de escritura."""
    from app.core.query_plan import compute_query_plan
    from app.core.query_router import QueryRouter
    from app.core.soql_builder import SoQLBuilder
    from app.data.actor_alias_resolver import ActorNameResolver

    router, builder = QueryRouter(), SoQLBuilder()
    resolver = ActorNameResolver(ROOT / "data" / "actor_alias_index.sqlite")

    class _State(dict):
        def get(self, key, default=None):
            return dict.get(self, key, default)

        def update(self, **kwargs):  # type: ignore[override]
            merged = _State(self)
            dict.update(merged, kwargs)
            return merged

    from app.core.workflow.actions.actor_resolution import resolve_actor_name

    def run(query: str) -> dict:
        parsed = router.parse(query)
        state = _State(parsed_params=dict(parsed.params), raw_query=query)
        resolved_state = resolve_actor_name(state, actor_name_resolver=resolver)
        params = dict(resolved_state.get("parsed_params", {}))
        plan = compute_query_plan(params, query)
        soql = None
        if plan.should_execute:
            try:
                soql = builder.build(
                    builder.dataset_id_for(plan.params.get("dataset")), plan.params,
                )
            except Exception as exc:
                soql = f"<BUILD_ERROR: {type(exc).__name__}: {exc}>"
        return {
            "parsed_params": dict(parsed.params),
            "params": params,
            "plan_should_execute": plan.should_execute,
            "plan_reason": plan.reason,
            "clarification": plan.clarification_question,
            "soql": soql,
            "route_reason": parsed.route_reason,
        }

    return run


def cmd_run(args: argparse.Namespace) -> int:
    cases = read_jsonl(FIXTURES / "frozen_cases.jsonl")
    run = _pipeline()
    results = []
    for index, case in enumerate(cases, 1):
        try:
            outcome = run(case["query"])
        except Exception as exc:
            outcome = {"error": f"{type(exc).__name__}: {exc}"}
        results.append({"case_id": case["case_id"], "stratum": case["stratum"],
                        "query": case["query"], "kind": case["kind"],
                        "invariants": case["invariants"],
                        "source_truth": case["source_truth"], **outcome})
        if index % 200 == 0:
            print(f"  {index}/{len(cases)}", flush=True)
    write_jsonl(FIXTURES / "_run_results.jsonl", results)
    print(f"ejecutados: {len(results)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    acquire = sub.add_parser("acquire")
    acquire.set_defaults(func=cmd_acquire)
    confirm = sub.add_parser("confirm")
    confirm.set_defaults(func=cmd_confirm)
    runner = sub.add_parser("run")
    runner.set_defaults(func=cmd_run)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
