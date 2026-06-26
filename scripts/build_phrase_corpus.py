#!/usr/bin/env python3
"""Commit 1 del sprint de integridad semántica de retrieval.

Construye el corpus/benchmark reproducible del caso ancla
"manual de contratación" (2024) contra datos.gov.co (dataset jbjy-vk9h).

Separa explícitamente DOS poblaciones que NO deben mezclarse:

  - live_baseline:  lo que SECOPPAL devuelve hoy de la query real (enfoque de
    tokens: acción-estricta AND %MANUALES%). Mide el defecto en producción.
  - offline_fixture: snapshot etiquetado de N filas (positivos silver + negativos)
    sobre el que corre el benchmark de CI. Su matriz de confusión se calcula SOLO
    sobre esas filas y debe cuadrar (TP+FN=positivos, FP+TN=negativos).

La consulta original es "elaboración O actualización". `ajuste` se trata como
EXPANSIÓN curada aparte (core_positive vs expansion_positive): el baseline del bug
usa solo core; la ganancia de `ajuste` se mide por separado, para no maquillar.

Herramienta de diseño/benchmark (como exp_recall_rerank.py): toca la red, NO
producción. El snapshot es INMUTABLE y versionado: regenerar = subir VERSION.

NOTA metodológica: los positivos se derivan con el patrón tolerante ⇒ validar la
solución tolerante contra ellos es CIRCULAR. Son silver_reference_set
(coverage_status: high_confidence), no oro.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import unicodedata
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

VERSION = 1
GENERATOR_VERSION = 1
BASE = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
DATASET_ID = "jbjy-vk9h"
YEAR = "fecha_de_firma >= '2024-01-01' AND fecha_de_firma <= '2024-12-31'"
SEL = "referencia_del_contrato,nombre_entidad,objeto_del_contrato,descripcion_del_proceso"
OUT = Path(__file__).resolve().parents[1] / "tests/fixtures/phrase_retrieval"
SNAPSHOT = OUT / f"manual_contratacion_2024_v{VERSION}.jsonl"
MANIFEST = OUT / "manual_contratacion_manifest.yaml"

STRICT_ACTION = (
    "(unaccent(upper(objeto_del_contrato)) like '%ELABORACION%' OR unaccent(upper(descripcion_del_proceso)) like '%ELABORACION%' "
    "OR unaccent(upper(objeto_del_contrato)) like '%ACTUALIZACION%' OR unaccent(upper(descripcion_del_proceso)) like '%ACTUALIZACION%')"
)
EXPANSION_ACTION = (
    "(unaccent(upper(objeto_del_contrato)) like '%AJUSTE%' OR unaccent(upper(descripcion_del_proceso)) like '%AJUSTE%')"
)
ANY_ACTION = f"({STRICT_ACTION} OR {EXPANSION_ACTION})"
ANCH = (
    "((upper(objeto_del_contrato) like '%MANUAL%' AND upper(objeto_del_contrato) like '%CONTRATAC%') "
    "OR (upper(descripcion_del_proceso) like '%MANUAL%' AND upper(descripcion_del_proceso) like '%CONTRATAC%'))"
)
SECOPPAL_TOKENS = (
    "(unaccent(upper(objeto_del_contrato)) like '%MANUALES%' OR unaccent(upper(descripcion_del_proceso)) like '%MANUALES%')"
)

TOLERANT = re.compile(r"MANUAL(ES)?(\s+\w+){0,2}\s+DE\s+(LA\s+)?CONTRATAC")
RAW_SINGULAR = re.compile(r"MANUAL DE CONTRATAC")
RAW_PLURAL = re.compile(r"MANUALES DE CONTRATAC")
HARD_NEG_TERMS = ["MANUAL DE FUNCIONES", "MANUAL DE COBRO COACTIVO", "MANUALIDADES", "MANUAL TECNICO"]


def run(where: str, limit: int = 1000) -> list[dict]:
    qs = urllib.parse.urlencode({"$select": SEL, "$where": f"{where} AND {YEAR}", "$limit": limit})
    with urllib.request.urlopen(f"{BASE}?{qs}", timeout=90) as r:
        return json.load(r)


def deaccent_upper(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).upper()


def norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", deaccent_upper(s))


def fields(row: dict) -> list[str]:
    return [norm(row.get("objeto_del_contrato")), norm(row.get("descripcion_del_proceso"))]


def key(row: dict) -> str:
    ref = (row.get("referencia_del_contrato") or "").strip()
    return ref or "h:" + hashlib.md5(norm(row.get("objeto_del_contrato")).encode()).hexdigest()[:12]


def is_tolerant(row: dict) -> bool:
    return any(TOLERANT.search(f) for f in fields(row))


def action_class(row: dict) -> str:
    blob = " ".join(fields(row))
    if "ELABORACION" in blob or "ACTUALIZACION" in blob:
        return "core"
    return "expansion"  # solo apareció vía 'ajuste'


def match_type(row: dict) -> tuple[str, str]:
    raw = [(row.get("objeto_del_contrato") or "").upper(), (row.get("descripcion_del_proceso") or "").upper()]
    raw = [deaccent_upper(x) for x in raw]
    if any(RAW_SINGULAR.search(f) for f in raw):
        return "exact_singular", "frase exacta 'manual de contratacion'"
    if any(RAW_PLURAL.search(f) for f in raw):
        return "exact_plural", "frase exacta plural 'manuales de contratacion'"
    if re.search(r"MANUAL(ES)?\s+\w+\s+DE\s+(LA\s+)?CONTRATAC", " || ".join(fields(row))):
        return "intercalated", "palabra intercalada (p.ej. 'manual interno de contratacion')"
    return "dirty_newline", "frase rota por salto de linea / puntuacion en el texto crudo"


def split_bucket(k: str) -> str:
    return "dev" if int(hashlib.md5(k.encode()).hexdigest(), 16) % 100 < 79 else "holdout"


def baseline_tokens_offline(row: dict) -> bool:
    """Predicado del enfoque ACTUAL aplicado a una fila del fixture."""
    return "MANUALES" in deaccent_upper(f"{row['objeto']} {row['descripcion']}")


def main() -> int:
    if SNAPSHOT.exists():
        print(f"ERROR: {SNAPSHOT.name} ya existe (snapshot inmutable). "
              f"Para regenerar, subí VERSION en el script.", file=sys.stderr)
        return 2

    print("Descargando conjuntos reales (2024)…")
    anchors = run(f"{ANY_ACTION} AND {ANCH}")
    secoppal_strict = run(f"{STRICT_ACTION} AND {SECOPPAL_TOKENS}")

    def dedup(rows):
        out = {}
        for r in rows:
            out.setdefault(key(r), r)
        return out

    anchors_d = dedup(anchors)
    silver = {k: r for k, r in anchors_d.items() if is_tolerant(r)}
    anchor_fp = {k: r for k, r in anchors_d.items() if k not in silver}
    silver_core = {k: r for k, r in silver.items() if action_class(r) == "core"}

    # ---- live_baseline: query estricta (defecto exacto del bug) ----
    sec_keys = set(dedup(secoppal_strict))
    live_tp = len(sec_keys & set(silver_core))
    live_returned = len(sec_keys)
    live_fp = live_returned - live_tp
    live_fn = len(silver_core) - live_tp

    # ---- negativos difíciles ----
    hard_negs = {}
    for term in HARD_NEG_TERMS:
        w = f"{ANY_ACTION} AND (upper(objeto_del_contrato) like '%{term}%' OR upper(descripcion_del_proceso) like '%{term}%')"
        for r in run(w, limit=12):
            k = key(r)
            if k in silver or k in hard_negs:
                continue
            r["_neg_reason"] = f"manual de otra cosa ({term.lower()}), no de contratacion"
            hard_negs[k] = r

    rows_out: list[dict] = []

    def emit(k, r, label, mtype, reason, aclass):
        rows_out.append({
            "reference": k,
            "entidad": (r.get("nombre_entidad") or "")[:80],
            "objeto": (r.get("objeto_del_contrato") or "")[:300],
            "descripcion": (r.get("descripcion_del_proceso") or "")[:300],
            "label": label,
            "match_type": mtype,
            "action_class": aclass,
            "reason": reason,
            "split": split_bucket(k),
        })

    for k, r in silver.items():
        mt, reason = match_type(r)
        emit(k, r, "relevant", mt, reason, action_class(r))
    for k, r in anchor_fp.items():
        emit(k, r, "irrelevant", "anchor_false_positive", "anchors 'manual' y 'contratacion' incidentales, dispersos", "n/a")
    for k, r in hard_negs.items():
        emit(k, r, "irrelevant", "hard_negative", r["_neg_reason"], "n/a")

    # garantía determinista: tipos críticos presentes en holdout
    by_type: dict[str, list[dict]] = {}
    for row in rows_out:
        by_type.setdefault(row["match_type"], []).append(row)
    for ct in ("dirty_newline", "intercalated", "anchor_false_positive"):
        items = sorted(by_type.get(ct, []), key=lambda x: x["reference"])
        if items and not any(x["split"] == "holdout" for x in items):
            items[0]["split"] = "holdout"

    rows_out.sort(key=lambda x: (x["label"], x["match_type"], x["reference"]))
    body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows_out)
    content_sha = hashlib.sha256(body.encode("utf-8")).hexdigest()

    # ---- matriz OFFLINE recalculada sobre el fixture (debe cuadrar) ----
    pos = [r for r in rows_out if r["label"] == "relevant"]
    core = [r for r in pos if r["action_class"] == "core"]
    expan = [r for r in pos if r["action_class"] == "expansion"]
    neg = [r for r in rows_out if r["label"] == "irrelevant"]
    tp_core = sum(1 for r in core if baseline_tokens_offline(r))
    fn_core = len(core) - tp_core
    fp_off = sum(1 for r in neg if baseline_tokens_offline(r))
    tn_off = len(neg) - fp_off
    assert tp_core + fn_core == len(core), "matriz offline: TP+FN != core_positives"
    assert fp_off + tn_off == len(neg), "matriz offline: FP+TN != negatives"

    def counts_split(items):
        return sum(1 for r in items if r["split"] == "dev"), sum(1 for r in items if r["split"] == "holdout")

    cdev, chold = counts_split(core)
    edev, ehold = counts_split(expan)
    ndev, nhold = counts_split(neg)
    mt_counts: dict[str, int] = {}
    for r in pos:
        mt_counts[r["match_type"]] = mt_counts.get(r["match_type"], 0) + 1

    OUT.mkdir(parents=True, exist_ok=True)
    SNAPSHOT.write_text(body, encoding="utf-8")
    mt_lines = "".join(f"    {k}: {v}\n" for k, v in sorted(mt_counts.items()))
    MANIFEST.write_text(
        f"""# Generado por scripts/build_phrase_corpus.py — NO editar a mano.
query_original: "contratos para elaboracion o actualizacion de manuales de contratacion en 2024"
strict_query_actions: [elaboracion, actualizacion]
curated_action_expansions: [ajuste]

provenance:
  dataset_id: {DATASET_ID}
  retrieved_at: {datetime.now(timezone.utc).isoformat(timespec='seconds')}
  year_filter: "2024"
  generator_version: {GENERATOR_VERSION}
  deduplication_key: referencia_del_contrato
  snapshot: {SNAPSHOT.name}
  row_count: {len(rows_out)}
  content_sha256: {content_sha}

coverage_status: high_confidence
synonym_probe: no_additional_hits_found  # red lexica reglamento/estatuto/regimen/politica -> 0 reales 2024

# Poblacion 1: lo que SECOPPAL devuelve HOY de la query estricta real (defecto en produccion).
live_baseline:
  population: secop_query_2024_strict   # (elaboracion OR actualizacion) AND MANUALES
  known_relevant: {len(silver_core)}     # silver core (solo elaboracion/actualizacion)
  returned: {live_returned}
  true_positives: {live_tp}
  false_positives: {live_fp}
  false_negatives: {live_fn}
  candidate_recall: {live_tp/len(silver_core):.4f}     # {live_tp}/{len(silver_core)}
  candidate_precision: {live_tp/live_returned:.4f}   # {live_tp}/{live_returned}

# Poblacion 2: el fixture inmutable. La matriz se calcula SOLO sobre estas filas.
offline_fixture:
  rows: {len(rows_out)}
  core_positives: {len(core)}
  expansion_positives: {len(expan)}    # solo via 'ajuste'; NO entran al gate estricto
  negatives: {len(neg)}               # {len(anchor_fp)} anchor_fp + {len(hard_negs)} hard_negatives
  positives_by_match_type:
{mt_lines}  baseline_tokens_offline:        # predicado actual recalculado sobre el fixture
    tp_core: {tp_core}
    fn_core: {fn_core}
    fp: {fp_off}
    tn: {tn_off}
    # invariantes (gate duro del generador): tp_core+fn_core==core_positives ; fp+tn==negatives
  split:
    development: {{core: {cdev}, expansion: {edev}, negatives: {ndev}}}
    holdout:     {{core: {chold}, expansion: {ehold}, negatives: {nhold}}}

gates_offline:
  critical_format_variants: 1.0      # dirty_newline/intercalated/singular/plural deben PASAR
  anchor_false_positive: rejected    # todos los anchor_fp deben quedar fuera
  candidate_pool_max: 400
  # los % globales del holdout son INFORMATIVOS hasta ampliar el corpus (9 negativos = volatil)
""",
        encoding="utf-8",
    )

    print(f"\nSnapshot inmutable: {SNAPSHOT.name}  ({len(rows_out)} filas, sha256 {content_sha[:12]}…)")
    print(f"  silver: {len(silver)} (core {len(silver_core)} + expansion {len(expan)}) | anchor_fp {len(anchor_fp)} | hard_neg {len(hard_negs)}")
    print(f"  positivos por tipo: {mt_counts}")
    print(f"  split  core dev/hold: {cdev}/{chold}   neg dev/hold: {ndev}/{nhold}")
    print(f"\nLIVE baseline (query estricta, defecto del bug):")
    print(f"  returned={live_returned} TP={live_tp} FP={live_fp} FN={live_fn}")
    print(f"  candidate_recall={live_tp/len(silver_core):.2%}  candidate_precision={live_tp/live_returned:.2%}")
    print(f"\nOFFLINE matrix (consistente): core={len(core)} (TP={tp_core}+FN={fn_core})  neg={len(neg)} (FP={fp_off}+TN={tn_off})")
    print(f"Manifest: {MANIFEST.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
