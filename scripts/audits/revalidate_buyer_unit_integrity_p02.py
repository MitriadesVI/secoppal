"""Revalidación EN VIVO — Etapa 1. Read-only, sin tocar runtime ni datos."""
from __future__ import annotations
import json, os, re, string, sys, time, unicodedata
from collections import defaultdict
import requests

RESOURCE = "https://www.datos.gov.co/resource/{}.json"
PROCESOS, CONTRATOS = "p6dx-8zbt", "jbjy-vk9h"
TOKEN = None
for line in open(".env", encoding="utf-8"):
    if line.startswith("SECOP_APP_TOKEN="):
        TOKEN = line.split("=", 1)[1].strip()
HEADERS = {"Accept": "application/json", "User-Agent": "SECOPPAL-P02-REVALIDATE/1.0"}
if TOKEN:
    HEADERS["X-App-Token"] = TOKEN

def get(dataset, params):
    last = None
    for attempt in range(5):
        try:
            r = requests.get(RESOURCE.format(dataset), params=params, headers=HEADERS, timeout=180)
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, list):
                raise ValueError("respuesta no es lista")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last = exc; time.sleep(attempt + 1)
    raise last  # nunca convertir un error de red en cero

def names_by_nit(nits):
    """{nit: {'procesos'|'contratos': {nombre_fisico: filas}}}"""
    out = defaultdict(lambda: {"procesos": {}, "contratos": {}})
    clause = ",".join(f"'{n}'" for n in sorted(set(nits)))
    for ds, field, tag in ((PROCESOS, "entidad", "procesos"), (CONTRATOS, "nombre_entidad", "contratos")):
        rows = get(ds, {"$select": f"{field},nit_entidad,count(1) as n",
                        "$where": f"nit_entidad in ({clause})",
                        "$group": f"{field},nit_entidad", "$limit": 5000})
        for row in rows:
            out[str(row.get("nit_entidad"))][tag][row[field]] = int(row["n"])
    return out

def rows_for_name_like(pattern):
    out = {}
    for ds, field, tag in ((PROCESOS, "entidad", "procesos"), (CONTRATOS, "nombre_entidad", "contratos")):
        rows = get(ds, {"$select": "count(1) as n", "$where": f"upper({field}) like '{pattern}'"})
        out[tag] = int(rows[0]["n"]) if rows else 0
    return out

def is_punct(ch):
    return ch in string.punctuation or unicodedata.category(ch).startswith("P")

def one_trailing_punct(long, short):
    return len(long) == len(short) + 1 and long[:-1] == short and is_punct(long[-1])

def tokens(name):
    folded = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")
    return tuple(t for t in re.split(r"[^A-Za-z0-9]+", folded.upper()) if t)

report = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "blocks": {}}

# ── A. PUENTE ARANDA (P0) ────────────────────────────────────────────────
print("== A. PUENTE ARANDA ==", flush=True)
pa = names_by_nit(["899999061", "830039548"])
block = {}
for nit in ("899999061", "830039548"):
    hits = {}
    for tag in ("procesos", "contratos"):
        hits[tag] = {k: v for k, v in pa[nit][tag].items() if "PUENTE ARANDA" in k.upper()}
    block[nit] = hits
    print(f"  NIT {nit}:")
    for tag in ("procesos", "contratos"):
        for k, v in sorted(hits[tag].items(), key=lambda x: -x[1]):
            print(f"    [{tag:9}] {k!r} -> {v}")
        if not hits[tag]:
            print(f"    [{tag:9}] (ninguno con 'PUENTE ARANDA')")
report["blocks"]["puente_aranda"] = block

# ── B. MERGE PAIRS ───────────────────────────────────────────────────────
PAIRS = {
    "yopal":       {"nits": ["891855017"],  "long": "MUNICIPIO DE YOPAL...", "short": "MUNICIPIO DE YOPAL.."},
    "soacha":      {"nits": ["800094755"],  "long": "MUNICIPIO DE SOACHA..", "short": "MUNICIPIO DE SOACHA."},
    "ffds":        {"nits": ["8002469532"], "long": "FONDO FINANCIERO DISTRITAL DE SALUD..", "short": "FONDO FINANCIERO DISTRITAL DE SALUD."},
    "minjusticia": {"nits": ["900457461"],  "long": "MINISTERIO DE JUSTICIA Y DEL DERECHO..", "short": "MINISTERIO DE JUSTICIA Y DEL DERECHO."},
    "uaesp":       {"nits": ["900126860"],  "long": "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS..", "short": "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS."},
}
print("\n== B. PREDICADO DE FUSIÓN (5 condiciones) ==", flush=True)
allnits = [n for p in PAIRS.values() for n in p["nits"]]
live = names_by_nit(allnits)
merge_report = {}
for key, spec in PAIRS.items():
    nit = spec["nits"][0]
    proc, cont = live[nit]["procesos"], live[nit]["contratos"]
    long_, short_ = spec["long"], spec["short"]
    c1_nit = bool(proc or cont)
    c2_disjoint = (long_ in proc and long_ not in cont) and (short_ in cont and short_ not in proc)
    c3_punct = one_trailing_punct(long_, short_)
    c5_tokens = tokens(long_) == tokens(short_)
    verdict = "PASS" if all((c1_nit, c2_disjoint, c3_punct, c5_tokens)) else "REVIEW"
    merge_report[key] = {
        "nit": nit, "procesos": proc, "contratos": cont,
        "c1_nit_activo": c1_nit, "c2_procedencia_disjunta": c2_disjoint,
        "c3_un_signo_terminal": c3_punct, "c5_tokens_iguales": c5_tokens,
        "verdict": verdict,
    }
    print(f"  {key:<12} NIT {nit:<11} c1={c1_nit} c2={c2_disjoint} c3={c3_punct} c5={c5_tokens} -> {verdict}")
    for tag, d in (("procesos", proc), ("contratos", cont)):
        for k, v in sorted(d.items(), key=lambda x: -x[1])[:6]:
            print(f"      [{tag:9}] {k!r} -> {v}")
report["blocks"]["merge_pairs"] = merge_report

# ── C. MINJUSTICIA sintético ─────────────────────────────────────────────
print("\n== C. ¿SECOP publica 'MINJUSTICIA%'? ==", flush=True)
synthetic = rows_for_name_like("MINJUSTICIA%")
print("  ", synthetic)
report["blocks"]["minjusticia_synthetic_rows"] = synthetic

out = sys.argv[1] if len(sys.argv) > 1 else "revalidate_p02.json"
json.dump(report, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(f"\nartefacto -> {out}")
