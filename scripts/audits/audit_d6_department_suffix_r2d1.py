#!/usr/bin/env python3
"""Auditor read-only D6 — R2D.1: geografía vs institución que termina en geografía.

`_is_unqualified_dept` decidía si un alias era "topónimo desnudo" con
``alias.endswith(clave)``, sin frontera de palabra. Por eso *corpocaldas*
"terminaba en" Caldas y el scan lo descartaba, junto con dos mil aliases
institucionales legítimos.

Este auditor congela la evidencia que autoriza el cambio y compara tres reglas
sobre el mismo HEAD, sin tocar runtime ni ``aliases_db``:

* ``CURRENT``    — ``endswith`` textual, la deuda D6;
* ``EXACT_ONLY`` — igualdad estricta contra ``DEPARTAMENTOS``; refutada aquí;
* ``B_PRIME``    — retira wrappers **sólo al inicio** y exige pertenencia exacta.

Mide tres ejes independientes: el universo de aliases rechazados, el barrido
territorial (¿alguna frase de territorio pasa a comprador?) y la ruta real del
producto (``QueryRouter``) sobre las 33 CAR/CDS, con su volumen reciente.

El volumen sale del artefacto de R2D, no de la red: el auditor es offline.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.generic_buyer import department_root_buyer_target  # noqa: E402
from app.core.query_router import QueryRouter  # noqa: E402
from app.data.departamentos import DEPARTAMENTOS  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
R2D_ARTIFACT = ROOT / "artifacts/audits/car_core_r2d_candidates.json"
OUT = ROOT / "artifacts/audits/d6_department_suffix_r2d1.json"

# Wrappers que sólo se retiran al INICIO del alias. No se tocan tokens internos
# ni finales: ahí es donde vive la diferencia entre "el cauca" y
# "universidad del cauca".
LEADING_WRAPPERS = ("el", "la", "los", "las", "de", "del", "en", "al")


def geography_forms(alias: str) -> list[str]:
    """Alias original más cada forma tras retirar wrappers iniciales."""
    forms = [alias]
    words = alias.split()
    while words and words[0] in LEADING_WRAPPERS:
        words = words[1:]
        forms.append(" ".join(words))
    return [form for form in forms if form]


def _make_rule(mode: str):
    """Réplica de ``_is_unqualified_dept`` con el criterio de geografía variable.

    Las demás cláusulas (precedencia del comprador raíz, calificador propio,
    calificador previo en el texto) son idénticas en las tres reglas: lo único
    que cambia es cómo se decide que un alias *es* geografía.
    """

    def rule(self: EntityResolver, alias: str, full_text: str) -> bool:
        stripped = alias.strip()
        root = department_root_buyer_target(stripped)
        if root and root in DEPARTAMENTOS:
            return False

        if mode == "CURRENT":
            is_geo = any(k == stripped or stripped.endswith(k) for k in DEPARTAMENTOS)
        elif mode == "EXACT_ONLY":
            is_geo = stripped in DEPARTAMENTOS
        else:
            is_geo = any(f in DEPARTAMENTOS for f in geography_forms(stripped))
        if not is_geo:
            return False

        first = stripped.split()[0] if stripped else ""
        if first in self._ENTITY_QUALIFIERS:
            return False

        pos = full_text.find(stripped)
        if pos > 0:
            words = full_text[:pos].rstrip().split()
            if words:
                if words[-1] in self._ENTITY_QUALIFIERS:
                    return False
                if words[-1] == "de" and len(words) >= 2 and words[-2] in self._ENTITY_QUALIFIERS:
                    return False
        return True

    return rule


RULES = {mode: _make_rule(mode) for mode in ("CURRENT", "EXACT_ONLY", "B_PRIME")}
LIVE_RULE = EntityResolver._is_unqualified_dept


# ---------------------------------------------------------------------------
# Eje 1 — el universo de aliases que la regla vigente rechaza
# ---------------------------------------------------------------------------

def _debt_keys() -> list[str]:
    """Claves de ``DEPARTAMENTOS`` que no nombran a su propio departamento."""
    debt = []
    for key, official in sorted(DEPARTAMENTOS.items()):
        target = geography_forms(key)[-1]
        if not re.search(rf"\b{re.escape(target)}\b", normalize_text(official)):
            debt.append(key)
    return debt


def classify(resolver: EntityResolver, rejected: list[str], debt: set[str]) -> dict:
    buckets: dict[str, list] = {
        name: [] for name in (
            "BARE_GEOGRAPHY", "LEADING_WRAPPER_GEOGRAPHY",
            "LEGITIMATE_INSTITUTION_ENDING_IN_GEOGRAPHY",
            "TEXTUAL_SUFFIX_FALSE_POSITIVE", "GEOGRAPHY_ALIAS_DEBT", "OTHER",
        )
    }
    for alias in rejected:
        owners = sorted(resolver.owners_by_alias.get(alias, ()))
        if alias in DEPARTAMENTOS:
            buckets["BARE_GEOGRAPHY"].append([alias, alias, owners])
            continue
        wrapper = next((f for f in geography_forms(alias) if f in DEPARTAMENTOS), None)
        if wrapper:
            buckets["LEADING_WRAPPER_GEOGRAPHY"].append([alias, wrapper, owners])
            continue
        keys = sorted((k for k in DEPARTAMENTOS if alias.endswith(k)), key=len, reverse=True)
        if not keys:
            buckets["OTHER"].append([alias, None, owners])
            continue
        key = keys[0]
        if key in debt:
            buckets["GEOGRAPHY_ALIAS_DEBT"].append([alias, key, owners])
        elif re.search(rf"\b{re.escape(key)}$", alias):
            buckets["LEGITIMATE_INSTITUTION_ENDING_IN_GEOGRAPHY"].append([alias, key, owners])
        else:
            buckets["TEXTUAL_SUFFIX_FALSE_POSITIVE"].append([alias, key, owners])
    return buckets


# ---------------------------------------------------------------------------
# Eje 2 — barrido territorial sistemático
# ---------------------------------------------------------------------------

TEMPLATES = (
    "contratos en {d}", "procesos en {d}", "licitaciones en el {d}",
    "contratos en la {d}", "obras en {d}", "contratos de {d} en 2025",
    "contratos del departamento del {d}", "procesos del {d}",
    "contratos en el departamento de {d}", "licitaciones de {d} 2025",
)


def territorial_sweep(resolver: EntityResolver) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for official in sorted(set(DEPARTAMENTOS.values())):
        for template in TEMPLATES:
            query = normalize_text(template.format(d=official))
            found = resolver.scan_entity(query)
            if found:
                hits[query] = [found.official_name, found.matched_alias]
    return hits


# ---------------------------------------------------------------------------
# Eje 3 — la ruta del producto: QueryRouter sobre las 33 CAR/CDS
# ---------------------------------------------------------------------------

def car_matrix(router: QueryRouter, universe: list[dict]) -> dict:
    by_name, by_acronym, wrong, fabricated = set(), set(), [], []
    for entry in universe:
        acronym = entry["acronym"]
        physical = entry["physical"]
        for form, bucket in ((entry["name"], by_name), (acronym, by_acronym)):
            params = router.parse(f"contratos de {form} en 2025").params
            resolved = params.get("entidad_resolved")
            if resolved == physical:
                bucket.add(acronym)
                if params.get("departamento_resolved"):
                    fabricated.append([acronym, form, params["departamento_resolved"]])
            elif resolved is not None:
                wrong.append([acronym, form, resolved])
    return {
        "NAME_REACHABLE": sorted(by_name),
        "ACRONYM_REACHABLE": sorted(by_acronym),
        "NAME_BLOCKED": sorted({e["acronym"] for e in universe} - by_name),
        "ACRONYM_BLOCKED": sorted({e["acronym"] for e in universe} - by_acronym),
        "WRONG_OWNER": wrong,
        "FABRICATED_TERRITORY": fabricated,
    }


def volume(universe: list[dict], reachable: list[str], totals: dict) -> dict:
    procesos = sum(e["procesos"] for e in universe if e["acronym"] in set(reachable))
    contratos = sum(e["contratos"] for e in universe if e["acronym"] in set(reachable))
    return {
        "processes": round(100 * procesos / totals["recent_12m_processes"], 1),
        "contracts": round(100 * contratos / totals["recent_12m_contracts"], 1),
    }


def load_universe() -> tuple[list[dict], dict]:
    artifact = json.loads(R2D_ARTIFACT.read_text())
    universe = [
        {
            "acronym": e["acronym"],
            "name": e["official_institutional_name"],
            "physical": e["physical_names"][0],
            "procesos": e["recent_12m_procesos"],
            "contratos": e["recent_12m_contratos"],
        }
        for e in artifact["entities"]
    ]
    return universe, artifact["volume"]


# ---------------------------------------------------------------------------

TERRITORY_CANARIES = (
    "licitaciones en el Cauca", "licitaciones en el Valle del Cauca",
    "licitaciones en el Distrito Capital de Bogotá", "licitaciones en el Arauca",
    "contratos en Meta", "contratos en el Meta", "contratos en Antioquia",
    "procesos en Santander", "contratos en Norte de Santander",
    "procesos en San Andrés",
)
BUYER_ROOT_CANARIES = (
    "contratos del Departamento del Meta",
    "contratos de la Gobernación de Santander",
    "contratos de la Gobernación del Cauca",
    "contratos del Gobierno Departamental del Tolima",
    "contratos de la Gobernación de San Andrés",
    "contratos del Departamento de San Andrés",
)
NON_CAR_CANARIES = (
    "secretaría general de la alcaldía mayor de Bogotá",
    "ESE del departamento de Meta",
    "Empresa de Acueducto y Alcantarillado de Bogotá",
    "Instituto de Caminos y Construcciones de Cundinamarca",
    "Canal Teleantioquia",
    "Industria Licorera de Caldas",
    "Terminal de Transportes de Santa Marta Magdalena",
)


def probe(router: QueryRouter, phrase: str) -> dict:
    params = router.parse(phrase).params
    return {
        "entidad": params.get("entidad_resolved"),
        "departamento": params.get("departamento_resolved"),
        "method": (params.get("entidad_resolution") or {}).get("method"),
    }


def run_all(resolver: EntityResolver, router: QueryRouter, universe, totals) -> dict:
    aliases = sorted(resolver.owners_by_alias)
    rejected = [a for a in aliases if resolver._is_unqualified_dept(a, a)]
    matrix = car_matrix(router, universe)
    any_reachable = sorted(set(matrix["NAME_REACHABLE"]) | set(matrix["ACRONYM_REACHABLE"]))
    return {
        "REJECTED_ALIASES": len(rejected),
        "rejected": rejected,
        "TERRITORIAL_SWEEP_HITS": territorial_sweep(resolver),
        "CAR_ROUTER": matrix,
        "CAR_ANY_FORM_REACHABLE": len(any_reachable),
        "VOLUME_ANY_FORM": volume(universe, any_reachable, totals),
        "VOLUME_BY_NAME": volume(universe, matrix["NAME_REACHABLE"], totals),
        "TERRITORY_CANARIES": {p: probe(router, p) for p in TERRITORY_CANARIES},
        "BUYER_ROOT_CANARIES": {p: probe(router, p) for p in BUYER_ROOT_CANARIES},
        "NON_CAR_CANARIES": {p: probe(router, p) for p in NON_CAR_CANARIES},
    }


def main() -> int:
    resolver = EntityResolver(DB)
    router = QueryRouter(entity_resolver=resolver)
    universe, totals = load_universe()
    debt = set(_debt_keys())

    results: dict[str, dict] = {}
    for mode, rule in RULES.items():
        EntityResolver._is_unqualified_dept = rule
        results[mode] = run_all(resolver, router, universe, totals)
    EntityResolver._is_unqualified_dept = LIVE_RULE
    results["LIVE"] = run_all(resolver, router, universe, totals)

    live_rejected = set(results["LIVE"]["rejected"])
    live_matches = [m for m in RULES if set(results[m]["rejected"]) == live_rejected]

    baseline_hits = results["CURRENT"]["TERRITORIAL_SWEEP_HITS"]
    def deltas(mode: str) -> dict:
        hits = results[mode]["TERRITORIAL_SWEEP_HITS"]
        return {
            "NEW": {q: v for q, v in hits.items() if q not in baseline_hits},
            "LOST": {q: v for q, v in baseline_hits.items() if q not in hits},
        }

    report = {
        "audit": "D6-DEPARTMENT-SUFFIX-R2D1",
        "type": "read-only/offline",
        "aliases_db_changed": False,
        "TOTAL_ALIASES": len(resolver.owners_by_alias),
        "DEPARTAMENTOS_KEYS": len(DEPARTAMENTOS),
        "GEOGRAPHY_ALIAS_REGISTRY_DEBT": sorted(debt),
        "LIVE_RULE_MATCHES": live_matches,
        "BASELINE": {
            "rule": "alias == clave or alias.endswith(clave)",
            "REJECTED_ALIASES": results["CURRENT"]["REJECTED_ALIASES"],
            "buckets": {
                name: {"count": len(items), "examples": items[:8]}
                for name, items in classify(
                    resolver, results["CURRENT"]["rejected"], debt
                ).items()
            },
            "CAR_ANY_FORM_REACHABLE": results["CURRENT"]["CAR_ANY_FORM_REACHABLE"],
            "CAR_NAME_REACHABLE": len(results["CURRENT"]["CAR_ROUTER"]["NAME_REACHABLE"]),
            "VOLUME_ANY_FORM": results["CURRENT"]["VOLUME_ANY_FORM"],
            "NAME_BLOCKED": results["CURRENT"]["CAR_ROUTER"]["NAME_BLOCKED"],
        },
        "EXACT_ONLY_REFUTATION": {
            "rule": "alias == clave",
            "REJECTED_ALIASES": results["EXACT_ONLY"]["REJECTED_ALIASES"],
            "NEW_UNSAFE_TERRITORY_TO_BUYER": deltas("EXACT_ONLY")["NEW"],
            "LOST": deltas("EXACT_ONLY")["LOST"],
        },
        "B_PRIME_EVIDENCE": {
            "rule": "alias o alias sin wrappers iniciales ∈ DEPARTAMENTOS",
            "leading_wrappers": list(LEADING_WRAPPERS),
            "REJECTED_ALIASES": results["B_PRIME"]["REJECTED_ALIASES"],
            "FREED_VS_CURRENT": results["CURRENT"]["REJECTED_ALIASES"]
            - results["B_PRIME"]["REJECTED_ALIASES"],
            "KEPT_VS_EXACT_ONLY": sorted(
                set(results["B_PRIME"]["rejected"]) - set(results["EXACT_ONLY"]["rejected"])
            ),
            "NEW_UNSAFE_TERRITORY_TO_BUYER": deltas("B_PRIME")["NEW"],
            "LOST": deltas("B_PRIME")["LOST"],
            "CAR_NAME_REACHABLE": len(results["B_PRIME"]["CAR_ROUTER"]["NAME_REACHABLE"]),
            "CAR_ACRONYM_BLOCKED": results["B_PRIME"]["CAR_ROUTER"]["ACRONYM_BLOCKED"],
            "WRONG_OWNER": results["B_PRIME"]["CAR_ROUTER"]["WRONG_OWNER"],
            "FABRICATED_TERRITORY": results["B_PRIME"]["CAR_ROUTER"]["FABRICATED_TERRITORY"],
            "VOLUME_BY_NAME": results["B_PRIME"]["VOLUME_BY_NAME"],
            "TERRITORY_CANARIES": results["B_PRIME"]["TERRITORY_CANARIES"],
            "BUYER_ROOT_CANARIES": results["B_PRIME"]["BUYER_ROOT_CANARIES"],
            "NON_CAR_CANARIES": results["B_PRIME"]["NON_CAR_CANARIES"],
        },
        "POST_IMPLEMENTATION": {
            "matches_reference_rule": live_matches,
            "REJECTED_ALIASES": results["LIVE"]["REJECTED_ALIASES"],
            "CAR_NAME_REACHABLE": len(results["LIVE"]["CAR_ROUTER"]["NAME_REACHABLE"]),
            "CAR_ACRONYM_BLOCKED": results["LIVE"]["CAR_ROUTER"]["ACRONYM_BLOCKED"],
            "WRONG_OWNER": results["LIVE"]["CAR_ROUTER"]["WRONG_OWNER"],
            "FABRICATED_TERRITORY": results["LIVE"]["CAR_ROUTER"]["FABRICATED_TERRITORY"],
            "VOLUME_BY_NAME": results["LIVE"]["VOLUME_BY_NAME"],
            "VOLUME_ANY_FORM": results["LIVE"]["VOLUME_ANY_FORM"],
            "NEW_UNSAFE_TERRITORY_TO_BUYER": deltas("LIVE")["NEW"],
            "TERRITORY_CANARIES": results["LIVE"]["TERRITORY_CANARIES"],
            "BUYER_ROOT_CANARIES": results["LIVE"]["BUYER_ROOT_CANARIES"],
            "NON_CAR_CANARIES": results["LIVE"]["NON_CAR_CANARIES"],
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    b = report["BASELINE"]
    p = report["POST_IMPLEMENTATION"]
    print(f"TOTAL_ALIASES               {report['TOTAL_ALIASES']}")
    print(f"REJECTED_BY_CURRENT         {b['REJECTED_ALIASES']}")
    for name, data in b["buckets"].items():
        print(f"  {name:46} {data['count']:5}")
    print(f"EXACT_ONLY new unsafe       "
          f"{len(report['EXACT_ONLY_REFUTATION']['NEW_UNSAFE_TERRITORY_TO_BUYER'])}")
    print(f"B_PRIME  new unsafe         "
          f"{len(report['B_PRIME_EVIDENCE']['NEW_UNSAFE_TERRITORY_TO_BUYER'])}")
    print(f"B_PRIME  freed aliases      {report['B_PRIME_EVIDENCE']['FREED_VS_CURRENT']}")
    print(f"CAR name reachable  before  {b['CAR_NAME_REACHABLE']}/33"
          f"   any-form {b['CAR_ANY_FORM_REACHABLE']}/33 {b['VOLUME_ANY_FORM']}")
    print(f"CAR name reachable  live    {p['CAR_NAME_REACHABLE']}/33  {p['VOLUME_BY_NAME']}")
    print(f"LIVE rule matches           {live_matches}")
    print(f"artifact                    {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
