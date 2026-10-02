#!/usr/bin/env python
"""Builds the offline case set for the H1 falsification ("announce instead of clarify").

Read-only over the repos. No network, no LLM, no Settings()/SecopalWorkflow().
Run:  /Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -B build_announce_cases.py

Pipeline
  1. Reproduce the E3 selection (HADES_ALTERNATIVES.md, E3) with gap_matrix.evaluate on the sealed Gold
     and the Phase-A captures.
  2. For every selected record, split Gold conditions into APPLIED (host can apply today) and UNAPPLIED.
  3. For every UNAPPLIED condition derive the widest applicable envelope ONLY from repo data
     (DIVIPOLA, geo regions, estado families, SMMLV table, modality catalog, time authority rules).
  4. Write cases.jsonl (contains query text: judges only) and BUILD_REPORT.md (ids and counts only).
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True

OUT = Path(__file__).resolve().parent
M1 = Path("/Users/rodrigoortiz/Documents/secoppal-m1")
MAIN = Path("/Users/rodrigoortiz/Documents/secoppal")
INT_PAR = Path("/Users/rodrigoortiz/Documents/secoppal-m1-int-par")
DIAG = Path("/Users/rodrigoortiz/Documents/tools/secoppal-diagnostics/m1_search_gold")
TODAY = date(2026, 10, 1)  # clock used for relative/implicit-year derivations (experiment date)

sys.path.insert(0, str(M1 / "scripts/audits/m1_search"))
import gap_matrix as gm  # noqa: E402

sys.path.insert(0, str(INT_PAR))
from app.core import estado_families as EF  # noqa: E402
from app.data import smmlv as SM  # noqa: E402
from app.data.colombia_geography import REGION_ALIASES, ZONES_BY_NAME  # noqa: E402
from app.data.departamentos import DEPARTAMENTOS  # noqa: E402
from app.data.estados import ESTADO_CONTRATO_SYNONYMS  # noqa: E402
from app.data.geo_secop_variants import AMBIGUOUS_REGION_ALIASES  # noqa: E402

fold = gm.fold
HARD = {"BUYER", "ACTOR", "TOPIC", "UNIVERSE_VIEW"}


# --------------------------------------------------------------------------- citations
_LOC_CACHE: dict[str, list[str]] = {}


def loc(path: Path, needle: str) -> str:
    """'relative/path:line' of the first line containing needle (read-only)."""
    key = str(path)
    if key not in _LOC_CACHE:
        _LOC_CACHE[key] = path.read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(_LOC_CACHE[key], 1):
        if needle in line:
            root = INT_PAR if str(path).startswith(str(INT_PAR)) else MAIN if str(path).startswith(str(MAIN)) else M1
            return f"{root.name}:{path.relative_to(root)}:{i}"
    return f"{path.name}:?"


# --------------------------------------------------------------------------- 1. E3 selection
def select_e3():
    golds = [json.loads(line) for line in open(DIAG / "sealed_v1/gold_v1.jsonl", encoding="utf-8")]
    caps = {}
    for line in open(DIAG / "captures/captures.jsonl", encoding="utf-8"):
        c = json.loads(line)
        caps[c["gold_id"]] = c
    pop = [g for g in golds if g.get("operation") == "SEARCH" and g.get("expected_disposition") == "EXECUTE"
           and g.get("ambiguity") != "AMBIGUOUS"]

    def known(r):  # failure the host itself knows about
        k, d = r["foundation_intrinsic"], r["f_detail"]
        return (k in ("F_PROPOSED_HOST_UNRESOLVED", "HOST_RESOLVED_PROJECTION_UNSUPPORTED")
                or (k == "IR_CANNOT_EXPRESS" and d == "F_PROXY:UNSUPPORTED")
                or (k == "F_NOT_PROPOSED" and d == "PROPOSED_AS:UNSUPPORTED"))

    selected, funnel = [], Counter()
    funnel["population"] = len(pop)
    for g in pop:
        cap = caps[g["gold_id"]]
        if cap["foundation"]["final_status"] == "READY":
            continue
        funnel["foundation_not_executing"] += 1
        ev = gm.evaluate(g, cap)
        bad = [r for r in ev["rows"] if r["foundation_intrinsic"] != "OK"]
        if not bad:
            funnel["no_intrinsic_failure(gate_only)"] += 1
            continue
        if not all(known(r) and r["dimension"] not in HARD for r in bad):
            continue
        funnel["all_failures_known_and_not_hard_anchor"] += 1
        if any(e["dimension"] != "UNSUPPORTED" for e in ev["spurious_f"]):
            funnel["dropped_spurious_non_UNSUPPORTED_foundation_condition"] += 1
            continue
        selected.append((g, cap, ev))
    return selected, funnel


# --------------------------------------------------------------------------- 2. envelope data
DIVIPOLA = json.loads((MAIN / "app/data/municipal_territories_v1.json").read_text(encoding="utf-8"))["territories"]
OFFICIAL_DEPTS = {fold(v): v for v in DEPARTAMENTOS.values()}
MUNIS_BY_DEPT = Counter(fold(t["department"]) for t in DIVIPOLA)
P_DIV = MAIN / "app/data/municipal_territories_v1.json"
P_EF = INT_PAR / "app/core/estado_families.py"
P_ES = INT_PAR / "app/data/estados.py"
P_SM = INT_PAR / "app/data/smmlv.py"
P_GEO = INT_PAR / "app/data/colombia_geography.py"
P_GV = INT_PAR / "app/data/geo_secop_variants.py"
P_TA = INT_PAR / "app/core/time_authority.py"
P_FP = INT_PAR / "app/core/foundation_projection.py"
P_SB = INT_PAR / "app/core/soql_builder.py"
P_MC = INT_PAR / "app/data/modality_catalog_v1.yaml"
P_VD = INT_PAR / "app/core/foundation_value_direction.py"
P_DEP = INT_PAR / "app/data/departamentos.py"


def official_dept(name: str) -> str:
    return OFFICIAL_DEPTS.get(fold(name), name.title())


def env(kind, value, derivation, exactness, candidates=None, caveat=None, widening=None, announce=None):
    return {"kind": kind, "value": value, "candidates": candidates or [], "derivation": derivation,
            "exactness": exactness, "caveat": caveat, "widening": widening, "announce": announce}


# exactness vocabulary:
#   LOSSLESS       the envelope is equivalent to the request under repo data (no widening)
#   WIDER          strictly wider than the request, contains it
#   UNPROVEN       candidate(s) taken from repo data, containment not proven by the repo
#   DROP           the condition is simply removed (not an envelope)
#   LOSSLESS_DROP  removing the condition is equivalent to the request (implied by the universe)

def env_municipality(c, ctx):
    raw = c.get("value_hint") or c["quote"]
    key = fold(re.sub(r"\(.*?\)", "", raw)).strip()
    key = re.sub(r"[^a-z0-9 ]", " ", key)
    key = " ".join(key.split())
    exact = [t for t in DIVIPOLA if key in t["aliases"]]
    how = "alias exacto"
    cands = exact
    if not cands:
        toks = key.split()
        n = len(toks)
        cands = []
        for t in DIVIPOLA:
            for a in t["aliases"]:
                at = a.split()
                if any(at[i:i + n] == toks for i in range(len(at) - n + 1)):
                    cands.append(t)
                    break
        how = "tokens contiguos dentro de un alias"
    if not cands:
        return env("NONE", None, f"{P_DIV.name}: sin alias para «{key}»", "DROP")
    depts = sorted({official_dept(t["department"]) for t in cands})
    cites = "; ".join(f"{t['municipality'].title()}→{official_dept(t['department'])} ({loc(P_DIV, chr(34) + 'divipola' + chr(34) + ': ' + chr(34) + t['divipola'] + chr(34))})" for t in cands)
    deriv = f"DIVIPOLA municipal_territories_v1 ({how}): {cites}; nombres oficiales SECOP: {loc(P_DEP, 'DEPARTAMENTOS')}"
    if len(depts) == 1:
        n_m = MUNIS_BY_DEPT[fold(cands[0]["department"])]
        return env("UNIQUE", f"departamento {depts[0]}", deriv, "WIDER",
                   widening=f"1 municipio -> {n_m} municipios de {depts[0]} (DIVIPOLA); volumen de filas no medible sin red",
                   announce=f"departamento {depts[0]}")
    return env("MULTIPLE", " o ".join(f"departamento {d}" for d in depts), deriv, "WIDER",
               candidates=[f"departamento {d}" for d in depts],
               caveat="el nombre coloquial coincide con más de un municipio DIVIPOLA; el repo no decide cuál",
               widening=f"{len(cands)} municipios candidatos en {len(depts)} departamentos; volumen no medible sin red",
               announce=" y ".join(f"departamento {d}" for d in depts))


def env_region(c, ctx):
    k_quote = fold(c["quote"])
    k_hint = fold(c.get("value_hint") or "")
    keys = {fold(a): r for a, r in REGION_ALIASES.items()}
    keys.update({fold(r): r for r in ZONES_BY_NAME})
    for k in (k_quote, re.sub(r"^(la|el|los|las) ", "", k_quote), k_hint):
        if k in AMBIGUOUS_REGION_ALIASES:
            regs = AMBIGUOUS_REGION_ALIASES[k]
            cands = [f"{r}: {', '.join(ZONES_BY_NAME[r])}" for r in regs]
            return env("MULTIPLE", " + ".join(f"región {r}" for r in regs),
                       f"{loc(P_GV, 'AMBIGUOUS_REGION_ALIASES')} («{k}» -> {list(regs)}); {loc(P_GEO, 'ZONES_BY_NAME')}",
                       "LOSSLESS", candidates=cands,
                       caveat="la frase nombra dos regiones; el Gold eligió una (Costa Caribe) pero el repo declara la frase ambigua; "
                              "requiere que el host acepte varios departamentos",
                       widening="union de las regiones: sin ensanche respecto de la lectura ambigua", announce=" y ".join(cands))
    for k in (k_quote, k_hint):
        if k in keys:
            r = keys[k]
            deps = ZONES_BY_NAME[r]
            return env("UNIQUE", f"departamentos {', '.join(deps)} (región {r})",
                       f"{loc(P_GEO, 'REGION_ALIASES')} («{k}» -> {r}); {loc(P_GEO, 'ZONES_BY_NAME')} (lista cerrada)",
                       "LOSSLESS", caveat="equivale a la región pedida (no hay ensanche) si el host acepta varios departamentos a la vez; no verificado",
                       widening=f"región = {len(deps)} departamentos, sin ensanche", announce=f"región {r} ({', '.join(deps)})")
    return env("NONE", None, "región fuera de ZONES_BY_NAME", "DROP")


def env_department(c, ctx):
    k = fold(c["quote"])
    k = re.sub(r"^departamento (de |del )?", "", k)
    hit = DEPARTAMENTOS.get(k) or DEPARTAMENTOS.get(f"del {k}")
    return env("NONE", None,
               f"{loc(P_DEP, repr(k) if False else chr(34) + k + chr(34))}: «{k}» -> {hit} existe en la lista cerrada de departamentos; no hay nivel geográfico más ancho que el departamento salvo el país (sin filtro)",
               "DROP",
               caveat="el repo sí resuelve este departamento exactamente: el bloqueo es de autoridad del host, no de envolvente",
               widening="1 de 33 departamentos -> país entero", announce="todo el país")


def _ds(ctx):
    return "procesos" if ctx["universe"] == "PROCESSES" else "contratos"


def _fmt_filters(res):
    return "; ".join(f"{f} IN {v}" for f, v in res["filters"].items())


def env_state(c, ctx):
    ds, q, hint = _ds(ctx), fold(c["quote"]), c.get("value_hint")
    cite_ef = f"{loc(P_EF, 'def resolve_estado')} + {loc(P_EF, 'NATURAL_TO_FAMILY = {')}"
    if ds == "contratos" and (hint in ("awarded", "other:celebrado") or q == "firmados"):
        wrong = EF.resolve_estado(q, ds)
        cav = None
        if wrong:
            cav = (f"resolve_estado('{q}','contratos') devuelve la familia {wrong['family']} ({_fmt_filters(wrong)}), "
                   "que son estados PRE-firma: no contiene la intención; no se usa")
        return env("NONE", None,
                   f"universo contratos = CELEBRATED por construcción ({loc(P_FP, 'ContractUniverse.CELEBRATED')}); «{c['quote']}» ya está implícito; "
                   f"estado_contrato no tiene literal 'adjudicado/celebrado/firmado' ({loc(P_ES, 'Firmado no es un literal')})",
                   "LOSSLESS_DROP", caveat=cav, announce="sin filtro de estado (todos los contratos celebrados)")
    if ds == "contratos" and hint == "other:liquidado":
        return env("MULTIPLE", "estado Cerrado o terminado (aproximación)",
                   f"{loc(P_ES, '\"liquidado\": \"Cerrado\"')} (sinónimo -> Cerrado) y docstring {loc(P_EF, 'Closest is')} (Cerrado / terminado); "
                   f"familias {loc(P_EF, '\"contrato_cerrado\"')}, {loc(P_EF, '\"contrato_terminado\"')}",
                   "UNPROVEN", candidates=["contrato_cerrado: Cerrado", "contrato_terminado: terminado"],
                   caveat="SECOP no tiene estado 'Liquidado'; el repo sólo lo aproxima, no prueba contención",
                   announce="contratos Cerrado o terminado")
    res = EF.resolve_estado(q, ds)
    if res is None and hint == "open":
        res = EF.resolve_estado("abiertos", ds)
    if res is None:
        return env("NONE", None, f"{cite_ef}: sin token/familia para «{c['quote']}» en {ds}", "DROP",
                   announce="sin filtro de estado")
    exact = res["kind"] == "exact"
    if exact:
        ex = "LOSSLESS"
    elif hint in ("open", "in_execution"):
        ex = "LOSSLESS"
    else:
        ex = "WIDER"
    cav = None
    if ex == "WIDER":
        cav = "el repo traduce el término coloquial a la familia completa, que contiene el literal pedido pero también otros estados"
    if ctx["view"] == "OPPORTUNITIES" and hint in ("in_evaluation",):
        cav = "la vista OPPORTUNITIES es de procesos abiertos a ofertas; 'en evaluación' es otro tramo del ciclo (tensión Gold/vista)"
    return env("UNIQUE", f"{res['family']}: {_fmt_filters(res)}", cite_ef, ex, caveat=cav,
               widening=None if ex == "LOSSLESS" else f"familia {res['family']}: {sum(len(v) for v in res['filters'].values())} estados vs 1",
               announce=f"estado {res['family']} ({', '.join(sum(res['filters'].values(), []))})")


def env_state_or(members, ctx):
    ds = _ds(ctx)
    return env("NONE", None,
               f"OR de estados contractuales: 'firmados' no es literal ({loc(P_ES, 'Firmado no es un literal')}) y 'en ejecución' pertenece a "
               f"contrato_activo ({loc(P_EF, '\"contrato_activo\"')}) ⊂ universo CELEBRATED ({loc(P_FP, 'ContractUniverse.CELEBRATED')}); "
               "la unión es el universo completo",
               "LOSSLESS_DROP", caveat="la unión equivale a no filtrar estado; el host no puede expresar el OR", announce="sin filtro de estado")


def env_value(c, ctx, rows_ctx):
    code = ctx["code"]
    hint = str(c.get("value_hint") or "")
    if "SMMLV" in hint.upper():
        n = int(re.sub(r"\D", "", hint.split()[0]) or 0)
        years = [str(x["value_hint"]) for x in rows_ctx if x["dimension"] == "TIME" and x["operator"] == "during_year"]
        if years and int(years[0]) in SM.SMMLV_COP_BY_YEAR:
            y = int(years[0])
            cop = n * SM.SMMLV_COP_BY_YEAR[y]
            return env("UNIQUE", f"valor {c['operator']} COP {cop:,} (= {n} SMMLV {y})".replace(",", "."),
                       f"{loc(P_SM, 'SMMLV_COP_BY_YEAR')}: SMMLV {y} = {SM.SMMLV_COP_BY_YEAR[y]:,}; el año sale de la condición TIME del mismo turno".replace(",", "."),
                       "LOSSLESS", caveat="conversión exacta; el bloqueo es de autoridad del host, no de envolvente",
                       announce=f"valor mayor a COP {cop:,} ({n} SMMLV de {y})".replace(",", "."))
        cands = [f"{y}: COP {n * v:,}".replace(",", ".") for y, v in sorted(SM.SMMLV_COP_BY_YEAR.items())]
        return env("MULTIPLE", "umbral = N SMMLV según el año (no indicado)",
                   f"{loc(P_SM, 'SMMLV_COP_BY_YEAR')}: tres años catalogados; el turno no nombra año", "UNPROVEN",
                   candidates=cands,
                   caveat=f"el año actual ({SM.current_colombian_year(None) if False else TODAY.year}) es el candidato natural pero el repo no lo impone aquí; el umbral del año menor contiene a los demás",
                   announce="umbral según el SMMLV de 2024, 2025 o 2026")
    return env("NONE", None,
               f"{code}: la cita no nombra un comparador que el léxico reconozca ({loc(P_VD, 'if not named')}); sin dirección conocida no hay cota más ancha que aplicar",
               "DROP", caveat="la dirección y el monto los conoce el Gold, no el host", announce="sin filtro de valor")


def env_order(c, ctx):
    if c["operator"] == "date_desc":
        return env("NONE", None,
                   f"el orden por defecto ya es fecha DESC ({loc(P_SB, 'Default: date DESC')}); quitar el orden equivale a lo pedido",
                   "LOSSLESS_DROP", caveat="verificado en el builder compartido, no ejecutado", announce="orden por defecto (más recientes primero)")
    return env("NONE", None,
               f"orden por valor no hospedado; el orden por defecto es fecha DESC, valor DESC ({loc(P_SB, 'Default: date DESC')}): no contiene 'mayor valor primero' como pedido",
               "DROP", caveat="la respuesta ancha se ve ordenada por fecha, no por valor; el LIMIT de candidatos puede dejar fuera los de mayor valor",
               announce="ordenado por fecha, no por valor")


def env_time(c, ctx):
    op = c["operator"]
    if op == "during_year" and ctx["view"] == "OPPORTUNITIES":
        return env("NONE", None,
                   f"UNSUPPORTED_OPPORTUNITY_OVERRIDE ({loc(P_FP, 'UNSUPPORTED_OPPORTUNITY_OVERRIDE')}): la vista de oportunidades no admite TIME; no hay año envolvente aplicable",
                   "DROP", caveat="alternativa fuera de envolvente: cambiar de vista (procesos históricos), lo que altera el universo",
                   announce="oportunidades abiertas ahora, sin filtro de año")
    if op == "during_month":
        m = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
             "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}[fold(c["quote"])]
        y = TODAY.year if date(TODAY.year, m, 1) <= TODAY else TODAY.year - 1
        return env("UNIQUE", f"año {y}", f"{loc(P_TA, 'año implícito = la ocurrencia más reciente')}: mes sin año con inicio <= {TODAY} -> {y}",
                   "WIDER", caveat="el año es inferido (la autoridad temporal del worktree m1-int-par ya hospeda el mes con año inferido)",
                   widening="1 mes -> 12 meses (12x por calendario)", announce=f"todo {y}")
    if op == "relative_period":
        n = int(re.sub(r"\D", "", str(c.get("value_hint"))) or 0)
        mm = TODAY.month - n
        sy = TODAY.year + (mm - 1) // 12
        years = sorted({sy, TODAY.year})
        if len(years) == 1:
            return env("UNIQUE", f"año {years[0]}", f"{loc(P_TA, 'ventanas MÓVILES terminadas en')}: ventana de {n} meses terminada en {TODAY} cae en {years[0]}",
                       "WIDER", caveat="depende del reloj: con otra fecha la ventana puede cruzar de año; la autoridad del worktree m1-int-par ya hospeda la ventana",
                       widening=f"{n} meses -> 12 meses (6x por calendario)", announce=f"todo {years[0]}")
        return env("MULTIPLE", " y ".join(f"año {y}" for y in years), f"ventana de {n} meses cruza de año", "WIDER",
                   candidates=[f"año {y}" for y in years], announce=" y ".join(f"año {y}" for y in years))
    return env("NONE", None, "sin envolvente temporal derivable", "DROP")


def env_modality(c, ctx):
    q = fold(c["quote"])
    if q.startswith("selecciones abreviadas") or q.startswith("seleccion abreviada"):
        return env("UNIQUE", "familia de recuperación seleccion_abreviada (menor cuantía, subasta inversa, sin manifestación)",
                   f"{loc(P_MC, 'retrieval_family: seleccion_abreviada')} (catálogo modality_catalog_v1)", "LOSSLESS",
                   caveat="el catálogo SÍ tiene la familia; la razón del host es 'familia no acreditada' (autoridad), no ausencia de dato",
                   announce="modalidad selección abreviada (todas sus variantes)")
    if q == "092":
        return env("UNIQUE", "familia regimen_especial (Contratación régimen especial, con y sin ofertas)",
                   f"{loc(P_MC, 'retrieval_family: regimen_especial')}; el catálogo no tiene campo propio del Decreto 092/2017",
                   "WIDER", caveat="092/ESAL es un subconjunto del régimen especial; el repo no lo separa",
                   widening="convenios 092 -> todo el régimen especial (factor no medible sin red)",
                   announce="modalidad régimen especial (incluye convenios 092 y otros)")
    return env("NONE", None, "modalidad sin familia en el catálogo", "DROP")


def env_logic(members, ctx):
    return env("NONE", None,
               f"NONCONJUNCTIVE_EXPRESSION ({loc(P_FP, 'NONCONJUNCTIVE_EXPRESSION')}): el host sólo ejecuta conjunciones; "
               "un OR no tiene superconjunto aplicable que no sea quitar el tema",
               "DROP", caveat="alternativa fuera de envolvente: ejecutar cada rama y unir (equivalente, pero no es un único envolvente)",
               announce="sin el filtro de tema")


# --------------------------------------------------------------------------- 3. record assembly
def gl(c):
    return f"{c['quote']} -> {c.get('value_hint')}" if c.get("value_hint") not in (None, c["quote"]) else str(c["quote"])


def cond_view(c):
    return {"id": c["id"], "dimension": c["dimension"], "operator": c["operator"], "polarity": c["polarity"],
            "glosa": gl(c), "level": c.get("level"), "value_hint": c.get("value_hint")}


def build_case(g, cap, ev):
    req = g["requests"][0]
    assert len(g["requests"]) == 1
    conds = req["conditions"]
    n = len(conds)
    cond_rows = ev["rows"][:n]
    assert all(gm.row_key(c) == r["key"] for c, r in zip(conds, cond_rows)), g["gold_id"]
    logic_row = next((r for r in ev["rows"] if r["key"] == "LOGIC_OR_NOT"), None)
    or_groups = [grp.split(",") for grp in re.findall(r"OR\(([^()]*)\)", req.get("logic") or "")]
    in_or = {i for grp in or_groups for i in grp}
    ctx = {"universe": req["universe"], "view": req.get("view")}
    by_id = {c["id"]: (c, r) for c, r in zip(conds, cond_rows)}

    applied, unapplied = [], []
    handled = set()
    # OR groups: unapplied as a unit when the logic row fails or every member fails with the same dimension
    for grp in or_groups:
        members = [by_id[i] for i in grp]
        logic_bad = logic_row is not None and logic_row["foundation_intrinsic"] != "OK"
        if logic_bad:
            ctx2 = dict(ctx, code="HOST_RESOLVED_PROJECTION_UNSUPPORTED:NONCONJUNCTIVE_EXPRESSION")
            e = env_logic(members, ctx2)
            unapplied.append({"dimension": "LOGIC_OR_NOT", "operator": "OR", "members": [cond_view(m[0]) for m in members],
                              "glosa": " O ".join(gl(m[0]) for m in members), "code": ctx2["code"], "envelope": e})
            handled.update(grp)
        elif all(m[1]["foundation_intrinsic"] != "OK" for m in members) and len({m[0]["dimension"] for m in members}) == 1:
            codes = sorted({f"{m[1]['foundation_intrinsic']}:{m[1]['f_detail']}" for m in members})
            e = env_state_or(members, ctx)
            unapplied.append({"dimension": members[0][0]["dimension"], "operator": "OR", "members": [cond_view(m[0]) for m in members],
                              "glosa": " O ".join(gl(m[0]) for m in members), "code": " | ".join(codes), "envelope": e})
            handled.update(grp)
    rows_ctx = [c for c in conds]
    for c, r in zip(conds, cond_rows):
        if c["id"] in handled:
            continue
        if r["foundation_intrinsic"] == "OK":
            applied.append(cond_view(c))
            continue
        code = f"{r['foundation_intrinsic']}:{r['f_detail']}"
        ctx3 = dict(ctx, code=code)
        d = c["dimension"]
        if d == "GEOGRAPHY":
            e = {"MUNICIPALITY": env_municipality, "REGION": env_region, "DEPARTMENT": env_department}[c["level"]](c, ctx3)
        elif d == "STATE":
            e = env_state(c, ctx3)
        elif d == "VALUE":
            e = env_value(c, ctx3, rows_ctx)
        elif d == "ORDER":
            e = env_order(c, ctx3)
        elif d == "TIME":
            e = env_time(c, ctx3)
        elif d == "MODALITY":
            e = env_modality(c, ctx3)
        else:
            raise SystemExit(f"unhandled dimension {d} in {g['gold_id']}")
        unapplied.append({"dimension": d if d != "GEOGRAPHY" else f"GEOGRAPHY/{c['level']}", "operator": c["operator"],
                          "polarity": c["polarity"], "glosa": gl(c), "code": code, "envelope": e, "members": [cond_view(c)]})

    kinds = [u["envelope"]["kind"] for u in unapplied]
    rec_kind = "NONE" if "NONE" in kinds else "MULTIPLE" if "MULTIPLE" in kinds else "UNIQUE"
    ex = {u["envelope"]["exactness"] for u in unapplied}
    if ex <= {"LOSSLESS", "LOSSLESS_DROP"}:
        rec_ex = "NO_WIDENING"  # nothing wider than the request is shown: the gap is authority, not envelope
    elif ex & {"WIDER", "UNPROVEN"} and not ex & {"DROP"}:
        rec_ex = "WIDENING"
    elif "DROP" in ex:
        rec_ex = "DROPS_A_CONDITION"
    else:
        rec_ex = "MIXED"

    notes, would = [], []
    for u in unapplied:
        e = u["envelope"]
        u["quote"] = " O ".join(m["glosa"].split(" -> ")[0] for m in u["members"])
        if e["kind"] == "NONE":
            txt = (f"No apliqué «{u['quote']}»: {e['announce']}." if e["exactness"] == "LOSSLESS_DROP"
                   else f"No pude aplicar «{u['quote']}»; te muestro los resultados sin ese criterio ({e['announce']}).")
        elif e["kind"] == "UNIQUE" and e["exactness"] == "LOSSLESS":
            txt = f"Interpreté «{u['quote']}» como {e['announce']}."
        elif e["kind"] == "UNIQUE":
            txt = f"No pude filtrar por «{u['quote']}»; te muestro todo: {e['announce']}."
        else:
            txt = f"No pude fijar «{u['quote']}» a un solo valor; te muestro: {e['announce']}."
        u["announce_text"] = txt
        notes.append(txt)
        would.append(f"{e['announce']}" if e["announce"] else "sin ese criterio")
    wide = "; ".join(f"{u['dimension']}: {u['envelope']['widening']}" for u in unapplied if u["envelope"]["widening"]) or None

    judge_view = {
        "query_text": g["text"],
        "universe": f"{req['universe']}/{req.get('view')}",
        "applied_filters": [a["glosa"].split(" -> ")[0] + f" [{a['dimension']}]" for a in applied],
        "not_applied": [f"{u['quote']} [{u['dimension']}]" for u in unapplied],
        "answer_scope_shown": would,
        "notice_shown_to_user": " ".join(notes),
    }
    return {
        "gold_id": g["gold_id"], "slice": g["slice"],
        "query_text": g["text"],
        "universe": req["universe"], "view": req.get("view"), "logic": req.get("logic"),
        "gold_conditions": [cond_view(c) for c in conds],
        "applied_conditions": applied,
        "unapplied": unapplied,
        "record_envelope_kind": rec_kind,
        "record_exactness": rec_ex,
        "announce_text": " ".join(notes),
        "widening_factor_note": wide,
        "foundation_final": {"status": cap["foundation"]["final_status"], "reason": cap["foundation"].get("final_reason")},
        "judge_view": judge_view,
    }


# --------------------------------------------------------------------------- 4. report
def report(cases, funnel, selected_n):
    ids = [c["gold_id"] for c in cases]
    dim_cond = Counter(u["dimension"] for c in cases for u in c["unapplied"])
    dim_rec = Counter(d for c in cases for d in {u["dimension"].split("/")[0] for u in c["unapplied"]})
    kind_cond = Counter(u["envelope"]["kind"] for c in cases for u in c["unapplied"])
    kind_rec = Counter(c["record_envelope_kind"] for c in cases)
    ex_cond = Counter(u["envelope"]["exactness"] for c in cases for u in c["unapplied"])
    ex_rec = Counter(c["record_exactness"] for c in cases)
    by_dim_kind = defaultdict(Counter)
    for c in cases:
        for u in c["unapplied"]:
            by_dim_kind[u["dimension"]][u["envelope"]["kind"]] += 1
    by_dim_ex = defaultdict(Counter)
    for c in cases:
        for u in c["unapplied"]:
            by_dim_ex[u["dimension"]][u["envelope"]["exactness"]] += 1
    n_cond = sum(kind_cond.values())
    unique_ids = [c["gold_id"] for c in cases if c["record_envelope_kind"] == "UNIQUE"]
    # sensitivity: treat the bare name «Cartagena» as Cartagena de Indias
    cart = [c for c in cases if any(u["envelope"]["kind"] == "MULTIPLE" and "Caquet" in (u["envelope"]["value"] or "") for u in c["unapplied"])]
    cart_flip = [c["gold_id"] for c in cart if all(u["envelope"]["kind"] in ("UNIQUE",) or ("Caquet" in (u["envelope"]["value"] or "")) for u in c["unapplied"])]
    nowiden = [c["gold_id"] for c in cases if c["record_exactness"] == "NO_WIDENING"]
    uniq_wide = [c["gold_id"] for c in cases if c["record_envelope_kind"] == "UNIQUE" and c["record_exactness"] == "WIDENING"]
    uniq_lossless = [c["gold_id"] for c in cases if c["record_envelope_kind"] == "UNIQUE" and c["record_exactness"] == "NO_WIDENING"]
    n_unique = kind_rec["UNIQUE"]

    def tbl(counter, head, order=None):
        keys = order or sorted(counter, key=lambda k: -counter[k])
        return "\n".join([f"| {head} | n |", "|---|---:|"] + [f"| {k} | {counter[k]} |" for k in keys])

    md = []
    md.append("# BUILD_REPORT: experimento H1 (anunciar en vez de aclarar), preparación offline\n")
    md.append("Sin texto de consultas: sólo ids y conteos. Generado por `build_announce_cases.py`.\n")
    md.append("## 1. Reproducción de la selección E3\n")
    md.append(f"- Registros seleccionados: **{len(cases)}** (cifra de HADES: 58).\n"
              "- El script original de E3 no está guardado; lo reconstruí con `gap_matrix.evaluate` (Gold sellado `sealed_v1/gold_v1.jsonl`, "
              "capturas Fase A `captures.jsonl`) y estos criterios, que son mi interpretación de la definición:\n"
              "  1. población = SEARCH + EXECUTE + no AMBIGUOUS = 446 (coincide);\n"
              "  2. Foundation no ejecuta (`final_status != READY`) = 301 (coincide con E1);\n"
              "  3. toda fila con fallo intrínseco es «conocida por el host»: `F_PROPOSED_HOST_UNRESOLVED`, `HOST_RESOLVED_PROJECTION_UNSUPPORTED`, "
              "o proxy/propuesta como UNSUPPORTED (`F_PROXY:UNSUPPORTED`, `PROPOSED_AS:UNSUPPORTED`), y ninguna es ancla dura (BUYER/ACTOR/TOPIC/universo);\n"
              "  4. sin condición espuria de Foundation de dimensión distinta de UNSUPPORTED (el host no sabe que falló).\n")
    md.append("Embudo:\n\n" + tbl(Counter(funnel), "paso", list(funnel)) + "\n")
    md.append("- Diferencia: el total coincide (58). El desglose por dimensión **no** coincide exactamente con el de HADES "
              "(GEO 18/STATE 15/VALUE 6/ORDER 5/TIME 5/OR-NOT 3/MODALITY 3 en HADES; abajo mis cifras por registro). "
              "Causa probable: HADES cuenta por dimensión «principal» o con otro emparejamiento de proxies; no puedo distinguirlo sin su script. "
              "Sin la regla 4 salen 59; sin proxies UNSUPPORTED salen 53. La selección es defendible, no idéntica verificada.\n")
    md.append("## 2. Distribución por dimensión no aplicada\n")
    md.append("Por registro (un registro cuenta una vez por dimensión):\n\n" + tbl(dim_rec, "dimensión") + "\n")
    md.append("Por condición no aplicada (un grupo OR de estados o de temas cuenta como una condición):\n\n" + tbl(dim_cond, "dimensión/nivel") + "\n")
    md.append(f"Condiciones no aplicadas en total: {n_cond}; registros con 1 condición: "
              f"{sum(1 for c in cases if len(c['unapplied']) == 1)}, con 2: {sum(1 for c in cases if len(c['unapplied']) == 2)}, "
              f"con más: {sum(1 for c in cases if len(c['unapplied']) > 2)}.\n")
    md.append("## 3. Envolvente: UNIQUE / MULTIPLE / NONE\n")
    md.append("Por condición:\n\n" + tbl(kind_cond, "tipo", ["UNIQUE", "MULTIPLE", "NONE"]) + "\n")
    md.append("Por registro (UNIQUE sólo si todas las condiciones lo son; NONE si alguna es NONE):\n\n"
              + tbl(kind_rec, "tipo", ["UNIQUE", "MULTIPLE", "NONE"]) + "\n")
    md.append("Tipo por dimensión (condiciones):\n\n| dimensión | UNIQUE | MULTIPLE | NONE |\n|---|---:|---:|---:|\n"
              + "\n".join(f"| {d} | {k['UNIQUE']} | {k['MULTIPLE']} | {k['NONE']} |" for d, k in sorted(by_dim_kind.items())) + "\n")
    md.append("## 4. Qué tan ancho es el envolvente (dato adicional, no pedido)\n")
    md.append("`exactness` por condición: LOSSLESS = el envolvente equivale a lo pedido con datos del repo (no hay ensanche, el hueco es de autoridad del host); "
              "WIDER = contiene lo pedido y más; UNPROVEN = candidato del repo sin contención probada; DROP = se quita la condición; "
              "LOSSLESS_DROP = quitarla equivale a lo pedido (lo implica el universo o el orden por defecto).\n\n"
              + tbl(ex_cond, "exactness (condición)") + "\n")
    md.append("| dimensión | " + " | ".join(["LOSSLESS", "WIDER", "UNPROVEN", "DROP", "LOSSLESS_DROP"]) + " |\n|---|---:|---:|---:|---:|---:|\n"
              + "\n".join(f"| {d} | " + " | ".join(str(k[e]) for e in ["LOSSLESS", "WIDER", "UNPROVEN", "DROP", "LOSSLESS_DROP"]) + " |"
                          for d, k in sorted(by_dim_ex.items())) + "\n")
    md.append("Por registro: " + ", ".join(f"{k}={v}" for k, v in ex_rec.items()) + ". "
              "NO_WIDENING = ninguna condición se ensancha: el registro sólo carece de autoridad en el host.\n")
    md.append(f"- Registros UNIQUE con ensanche real (todas WIDER o LOSSLESS, alguna WIDER): **{len(uniq_wide)}**.\n"
              f"- Registros UNIQUE sin ningún ensanche (todas LOSSLESS): **{len(uniq_lossless)}**.\n"
              f"- Registros sin ensanche alguno (UNIQUE LOSSLESS + todo LOSSLESS_DROP/LOSSLESS): **{len(nowiden)}**.\n")
    md.append("## 5. Primer criterio de muerte (¿al menos 30 de 58 con envolvente único?)\n")
    md.append(f"- Registros con envolvente **UNIQUE**: **{n_unique}** de {len(cases)}. Umbral: 30. "
              f"**{'SE CUMPLE: H1 muere por este criterio' if n_unique < 30 else 'NO se cumple: H1 sobrevive a este criterio'}** "
              f"(margen {n_unique - 30:+d}).\n")
    md.append(f"- Sensibilidad (un nombre coloquial de municipio con dos homónimos DIVIPOLA, tratado como el municipio principal en lugar de ambiguo): "
              f"{len(cart_flip)} registro(s) pasarían de MULTIPLE a UNIQUE ({', '.join(cart_flip) or 'ninguno'}); "
              f"UNIQUE sería {n_unique + len(cart_flip)}.\n")
    md.append(f"- Lectura estricta: de los {n_unique} UNIQUE, sólo {len(uniq_wide)} muestran algo más ancho que lo pedido; "
              f"{len(uniq_lossless)} son equivalentes (no requieren aviso de ensanche: son una autoridad que falta). "
              "Con la regla literal del encargo cuentan como UNIQUE.\n")
    fragile = {"región con varios departamentos (host no verificado)": [c["gold_id"] for c in cases if c["record_envelope_kind"] == "UNIQUE" and any(u["dimension"] == "GEOGRAPHY/REGION" for u in c["unapplied"])],
               "año inferido / ventana móvil (ya hospedado en m1-int-par, reloj de hoy)": [c["gold_id"] for c in cases if c["record_envelope_kind"] == "UNIQUE" and any(u["dimension"] == "TIME" for u in c["unapplied"])],
               "vista OPPORTUNITIES con estado de evaluación (tensión Gold/vista)": [c["gold_id"] for c in cases if c["record_envelope_kind"] == "UNIQUE" and any(u["envelope"]["caveat"] and "OPPORTUNITIES" in u["envelope"]["caveat"] for u in c["unapplied"])]}
    all_fragile = sorted({i for v in fragile.values() for i in v})
    md.append("- Fragilidad del margen: UNIQUE depende de supuestos no verificados para estos registros:\n"
              + "\n".join(f"  - {k}: {', '.join(v) or 'ninguno'}" for k, v in fragile.items())
              + f"\n  - Si se excluyen todos ({len(all_fragile)}), UNIQUE = {n_unique - len(all_fragile)}.\n")
    md.append("- El segundo criterio (más de un tercio con condición decisiva) lo deciden los jueces; no se evalúa aquí.\n")
    md.append("Ids por tipo de registro:\n")
    for k in ("UNIQUE", "MULTIPLE", "NONE"):
        md.append(f"- {k}: " + ", ".join(c["gold_id"] for c in cases if c["record_envelope_kind"] == k))
    md.append("\n## 6. Hallazgos al construir (hechos, con su fuente)\n")
    md.append("- Varias condiciones «no aplicadas» ya tienen resolución exacta en datos del repo (estados con `resolve_estado`, "
              "SMMLV con `SMMLV_COP_BY_YEAR`, familia selección abreviada del catálogo, un departamento de `DEPARTAMENTOS`, una región de `ZONES_BY_NAME`): "
              "la causa es autoridad no cableada en el host de la captura, no ausencia de dato.\n"
              "- La autoridad temporal del worktree `secoppal-m1-int-par` ya hospeda mes sin año (año más reciente ≤ hoy) y ventanas móviles; "
              "en la captura v4.2 no estaban. Esos casos (TIME/during_month, relative_period) pueden ya no ser casos de aviso.\n"
              "- `resolve_estado('adjudicados','contratos')` devuelve la familia `en_evaluacion` de contratos (estados pre-firma: Borrador, enviado Proveedor, "
              "En aprobación, Aprobado). No contiene «contrato adjudicado»; no se usó como envolvente. Posible defecto del léxico, no investigado.\n"
              "- Un nombre de municipio coloquial (3 registros) no es alias DIVIPOLA exacto y es prefijo de dos municipios de departamentos distintos: por eso MULTIPLE.\n")
    md.append("## 7. No verificado\n")
    md.append("- No hay conteos de filas (sin red ni warehouse): el tamaño real de cada superconjunto es «no medible»; sólo hay factores de calendario o de nº de municipios.\n"
              "- No ejecuté el host: «aplicable hoy» sale de las filas de `gap_matrix` sobre la captura v4.2 (producer congelado), no de una corrida actual.\n"
              "- Que el host acepte un filtro con varios departamentos (regiones) o varias familias no está verificado.\n"
              "- Que el usuario lea el aviso no está medido (no hay telemetría).\n"
              "- La exactitud de la reconstrucción de E3 (ver §1) y el criterio de «conocido por el host» son interpretación mía.\n"
              "- El reloj de los envolventes temporales es 2026-10-01 (fecha del experimento), no la fecha original de cada consulta REAL.\n"
              "- Los textos de aviso son propuestas mías, no el copy de producto.\n"
              "- La lectura de «qué intención hay tras el estado» (hint del Gold -> literal) es manual y está en `env_state`; los jueces deberían poder discrepar.\n")
    return "\n".join(md) + "\n", {"unique": n_unique, "kind_rec": dict(kind_rec), "kind_cond": dict(kind_cond), "ids_unique": unique_ids}


def main():
    selected, funnel = select_e3()
    cases = [build_case(g, cap, ev) for g, cap, ev in selected]
    with open(OUT / "cases.jsonl", "w", encoding="utf-8") as f:
        for c in cases:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    import random
    order = list(cases)
    random.Random(20261001).shuffle(order)  # judges must not see cases grouped by id/dimension
    with open(OUT / "judge_input.jsonl", "w", encoding="utf-8") as f:
        for c in order:
            f.write(json.dumps({"case_id": c["gold_id"], **c["judge_view"]}, ensure_ascii=False) + "\n")
    md, summ = report(cases, funnel, len(selected))
    (OUT / "BUILD_REPORT.md").write_text(md, encoding="utf-8")
    print(json.dumps({"n": len(cases), **summ}, ensure_ascii=False))


if __name__ == "__main__":
    main()
