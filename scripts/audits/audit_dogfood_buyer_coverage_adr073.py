#!/usr/bin/env python3
"""Reproductor read-only de la cobertura ADR-073 para los compradores del dogfood.

No decide nada: OBSERVA. Para cada identidad recorre el mismo orden que ADR-073
exige y deja por escrito cada paso, de modo que un nombre físico nunca entra al
sidecar «porque parece correcto»:

    canónico del resolver
      -> NIT acreditado por el propio dataset (igualdad exacta sobre el nombre)
      -> TODAS las formas físicas que ese NIT publica, con sus filas
      -> qué universo devuelve HOY el fallback LIKE del builder
      -> latencia de LIKE contra la del binding exacto, mismo universo

La última comparación es la que decide si un binding puede escribirse sin
cambiar semántica: si el LIKE alcanza formas o NITs que el binding exacto no
alcanzaría, el binding NO es equivalente y este script lo dice en vez de
resolverlo.

Los predicados no se reconstruyen a mano: se piden a `SoQLBuilder`, que es quien
los emite en producción. Reconstruirlos aquí mediría una consulta que el
producto nunca manda.

Uso:  .venv/bin/python scripts/audits/audit_dogfood_buyer_coverage_adr073.py [--reps N]
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timezone, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.soql_builder import SoQLBuilder  # noqa: E402

OUT = ROOT / "artifacts/audits/dogfood_buyer_coverage_adr073.json"

# La columna de entidad de cada dataset. `nit_entidad` es común a los dos.
DATASETS = {
    SoQLBuilder.PROCESOS_DATASET: "entidad",
    SoQLBuilder.CONTRATOS_DATASET: "nombre_entidad",
}

# Lo que la persona escribe. El canónico NO se escribe aquí: lo produce el
# resolver vivo, que es quien se lo pasa al builder en producción.
COHORT = (
    "alcaldia de soledad",
    "alcaldia de galapa",
    "alcaldia de barranquilla",
    "alcaldia de cartagena",
)

# Fuera de la cohorte y por qué. Malambo no tiene identidad canónica: el
# resolver devuelve None para «municipio de malambo», y «malambo» a secas se lo
# lleva un hospital por fuzzy. Sin identidad no hay clave para un binding, y
# fabricarla aquí sería exactamente lo que ADR-073 prohíbe.
EXCLUDED = {
    "alcaldia de malambo": "NO_CANONICAL_IDENTITY",
}


def read_only_settings() -> Settings:
    """`Settings` con la persistencia desviada a un temporal.

    Este script no escribe feedback ni trazas, pero construir `Settings()` a
    secas fuera de pytest apunta la persistencia al almacén real, y «hoy no
    escribo» no es una garantía: la regla es que toda construcción fuera de
    pytest redirija, y ese invariante no admite excepciones por buena conducta.
    """
    scratch = Path(tempfile.mkdtemp(prefix="adr073-audit-"))
    return Settings(
        secop_feedback_path=scratch / "feedback.jsonl",
        secop_feedback_db_path=scratch / "feedback.db",
    )


class Source:
    """Lector Socrata mínimo y de sólo lectura, con latencia por llamada."""

    def __init__(self, settings: Settings) -> None:
        self._domain = settings.datos_gov_domain
        self._token = settings.secop_app_token or ""

    def get(self, dataset: str, params: dict) -> tuple[list[dict], float]:
        url = f"https://{self._domain}/resource/{dataset}.json?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, headers={"X-App-Token": self._token})
        started = time.perf_counter()
        with urllib.request.urlopen(request, timeout=120) as response:
            rows = json.load(response)
        return rows, (time.perf_counter() - started) * 1000

    def count(self, dataset: str, where: str) -> tuple[int | None, float, str]:
        """Un timeout NO es un error del experimento: es la medición.

        Que el fallback no termine dentro del presupuesto es precisamente lo que
        se está midiendo, así que se registra con su tope en vez de abortar la
        auditoría y perder el resto de la evidencia.
        """
        started = time.perf_counter()
        try:
            rows, latency = self.get(dataset, {"$select": "count(*) as n", "$where": where})
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            return None, (time.perf_counter() - started) * 1000, type(exc).__name__
        return int(rows[0]["n"]), latency, "OK"


def accredit_nit(source: Source, dataset: str, field: str, canonical: str) -> list[dict]:
    """El NIT que el dataset asocia a ESE nombre físico exacto. Sin fuzzy."""
    where = SoQLBuilder._exact_entity_clause(field, (canonical,))
    rows, _ = source.get(dataset, {
        "$select": "nit_entidad,count(*) as rows", "$group": "nit_entidad",
        "$where": where, "$order": "count(*) DESC", "$limit": "50",
    })
    return [{"nit": row.get("nit_entidad"), "rows": int(row["rows"])} for row in rows]


def physical_forms(source: Source, dataset: str, field: str, nit: str) -> list[dict]:
    """TODAS las formas físicas del NIT. Es el paso que descubre lo que el
    canónico no alcanza; omitirlo es cómo se escribe un binding incompleto."""
    rows, _ = source.get(dataset, {
        "$select": f"{field},count(*) as rows", "$group": field,
        "$where": f"nit_entidad='{SoQLBuilder._escape(nit)}'",
        "$order": "count(*) DESC", "$limit": "200",
    })
    return [{"name": row.get(field), "rows": int(row["rows"])} for row in rows]


def like_universe(source: Source, dataset: str, field: str, canonical: str) -> list[dict]:
    """Lo que el fallback devuelve HOY, con su NIT: la línea base de semántica."""
    where = SoQLBuilder._accent_fold_like(field, SoQLBuilder._escape(canonical))
    rows, _ = source.get(dataset, {
        "$select": f"nit_entidad,{field},count(*) as rows",
        "$group": f"nit_entidad,{field}", "$where": where,
        "$order": "count(*) DESC", "$limit": "200",
    })
    return [{"nit": row.get("nit_entidad"), "name": row.get(field),
             "rows": int(row["rows"])} for row in rows]


def benchmark(source: Source, dataset: str, field: str, canonical: str,
              names: tuple[str, ...], reps: int) -> dict:
    like = SoQLBuilder._accent_fold_like(field, SoQLBuilder._escape(canonical))
    exact = SoQLBuilder._exact_entity_clause(field, names)
    like_ms, exact_ms = [], []
    like_n = exact_n = None
    like_status, exact_status = [], []
    for _ in range(reps):
        count, latency, status = source.count(dataset, like)
        like_ms.append(latency)
        like_status.append(status)
        if count is not None:
            like_n = count
        count, latency, status = source.count(dataset, exact)
        exact_ms.append(latency)
        exact_status.append(status)
        if count is not None:
            exact_n = count
    like_median = statistics.median(like_ms)
    exact_median = statistics.median(exact_ms)
    return {
        "like_where": like, "exact_where": exact,
        "like_rows": like_n, "exact_rows": exact_n,
        # Un LIKE que nunca contestó no "coincide": no se sabe. None, no True.
        "same_universe": (None if like_n is None or exact_n is None else like_n == exact_n),
        "like_status": like_status, "exact_status": exact_status,
        "like_timeouts": sum(status != "OK" for status in like_status),
        "exact_timeouts": sum(status != "OK" for status in exact_status),
        "like_ms_median": round(like_median, 1),
        "exact_ms_median": round(exact_median, 1),
        "like_ms": [round(v, 1) for v in like_ms],
        "exact_ms": [round(v, 1) for v in exact_ms],
        "speedup": round(like_median / exact_median, 1) if exact_median else None,
    }


def observe(source: Source, resolver: EntityResolver, phrase: str, reps: int) -> dict:
    resolution = resolver.resolve_entidad(phrase).to_dict()
    canonical = resolution.get("value")
    record: dict = {
        "phrase": phrase,
        "resolution": {k: resolution.get(k) for k in ("value", "method", "confidence")},
        "canonical_name": canonical,
        "datasets": {},
    }
    if not canonical:
        record["status"] = "NO_CANONICAL_IDENTITY"
        return record

    nits: dict[str, list[dict]] = {}
    for dataset, field in DATASETS.items():
        nits[dataset] = accredit_nit(source, dataset, field, canonical)

    accredited = {entry["nit"] for rows in nits.values() for entry in rows}
    record["identity_key_candidates"] = sorted(n for n in accredited if n)
    if len(record["identity_key_candidates"]) != 1:
        # Un canónico que dos NIT publican no es un binding: es una pregunta de
        # identidad, y se declara sin responderla.
        record["status"] = "AMBIGUOUS_IDENTITY"
        record["nit_by_dataset"] = nits
        return record
    nit = record["identity_key_candidates"][0]
    record["identity_key"] = {"type": "NIT", "value": nit}

    for dataset, field in DATASETS.items():
        forms = physical_forms(source, dataset, field, nit)
        universe = like_universe(source, dataset, field, canonical)
        reached = {entry["name"] for entry in universe}
        foreign = sorted({entry["nit"] for entry in universe if entry["nit"] != nit and entry["nit"]})
        record["datasets"][dataset] = {
            "entity_field": field,
            "nit_rows": sum(entry["rows"] for entry in forms),
            "physical_forms": forms,
            "like_universe": universe,
            "forms_reached_by_like": sorted(reached),
            "forms_not_reached_by_like": sorted(
                entry["name"] for entry in forms if entry["name"] not in reached
            ),
            "foreign_nits_reached_by_like": foreign,
            "benchmark": benchmark(source, dataset, field, canonical,
                                   tuple(sorted(reached)), reps),
        }

    # El binding que NO cambia semántica es el conjunto de formas que el LIKE ya
    # alcanza. Una forma real que el LIKE no alcanza hoy queda DECLARADA y fuera
    # del binding: incorporarla ampliaría el universo en silencio, y esa es una
    # decisión de producto (BUYER_GROUP), no de proyección de nombres.
    binding: dict[str, list[str]] = {}
    declared_only: dict[str, list[str]] = {}
    for dataset, data in record["datasets"].items():
        binding[dataset] = data["forms_reached_by_like"]
        if data["forms_not_reached_by_like"]:
            declared_only[dataset] = data["forms_not_reached_by_like"]
    record["proposed_binding"] = binding
    record["declared_not_bound"] = declared_only
    record["observed_rows"] = {
        dataset: sum(entry["rows"] for entry in data["like_universe"])
        for dataset, data in record["datasets"].items()
    }
    # `same_universe is True` a propósito: un None (el LIKE no contestó) no
    # acredita equivalencia, y tratarlo como verdadero escribiría un binding
    # sobre una comparación que nunca ocurrió.
    record["semantics_preserved"] = all(
        data["benchmark"]["same_universe"] is True
        and not data["foreign_nits_reached_by_like"]
        for data in record["datasets"].values()
    )
    record["status"] = "READY" if record["semantics_preserved"] else "NOT_EQUIVALENT"
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    settings = read_only_settings()
    source = Source(settings)
    resolver = EntityResolver(settings.secop_alias_db_path)

    observations = [observe(source, resolver, phrase, args.reps) for phrase in COHORT]
    excluded = []
    for phrase, reason in EXCLUDED.items():
        resolution = resolver.resolve_entidad(phrase).to_dict()
        excluded.append({
            "phrase": phrase, "reason": reason,
            "resolution": {k: resolution.get(k) for k in ("value", "method", "confidence")},
        })

    payload = {
        "audit": "DOGFOOD_BUYER_COVERAGE_ADR073",
        "observed_at": date.today().isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "domain": settings.datos_gov_domain,
        "reps": args.reps,
        "script": "scripts/audits/audit_dogfood_buyer_coverage_adr073.py",
        "observations": observations,
        "excluded": excluded,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    try:
        shown = args.out.relative_to(ROOT)
    except ValueError:  # --out fuera del repo (una corrida de prueba)
        shown = args.out
    print(f"escrito {shown}")
    for item in observations:
        print(f"  {item['canonical_name']!r}: {item.get('status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
