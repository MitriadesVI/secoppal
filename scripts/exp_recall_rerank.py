#!/usr/bin/env python3
"""Experimento de medición: ¿subir el fetch + rerankear más candidatos mejora
la precisión E2E sobre queries reales?

NO toca el pipeline de producción. Compara, por query real:

  baseline    — LIMIT 50, ORDER BY fecha DESC, BM25 solo si objeto>=3 tokens
                (réplica del comportamiento actual)
  treatment   — LIMIT FETCH_LIMIT, BM25 con umbral>=1 token, recorte a 50

Métrica: precision@10 (proxy automática). Un resultado cuenta como RELEVANTE
si su campo principal (objeto_del_contrato / nombre_del_procedimiento)
contiene TODOS los términos de tópico parseados por el router, comparando
accent-folded y por substring de palabra. Mira SOLO el título — no la
descripción, que es donde el LIKE '%x%' mete ruido. Es una proxy, pero captura
el efecto del ranking manteniendo fijos parser y filtros.

Uso:
    .venv/bin/python scripts/exp_recall_rerank.py
    .venv/bin/python scripts/exp_recall_rerank.py --fetch 600 --topn 10
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings
from app.core.query_router import QueryRouter
from app.core.relevance import RelevanceScorer
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder

REPO = Path(__file__).resolve().parents[1]
SECOPPAL_DB = REPO / "data" / "secoppal.db"

# Queries reales de tópico (fallback si no se usa --from-db).
QUERIES = [
    "contratos de mantenimiento vial en Antioquia",
    "suministro de medicamentos en Atlantico",
    "contratos de aseo y cafeteria en Bogota",
    "interventoria de obras en Cundinamarca",
    "contratos de alimentacion escolar en Valle del Cauca",
    "mantenimiento de parques en Medellin",
    "construccion de acueducto en Narino",
    "servicio de vigilancia en Santander",
    "contratos de consultoria ambiental en Boyaca",
    "dotacion de hospitales en Cordoba",
    "mantenimiento de alumbrado publico en Tolima",
    "transporte escolar en Cauca",
]


def _load_db_queries() -> list[str]:
    if not SECOPPAL_DB.exists():
        return []
    conn = sqlite3.connect(SECOPPAL_DB)
    rows = conn.execute(
        "SELECT DISTINCT user_query FROM traces "
        "WHERE user_query IS NOT NULL AND length(user_query) > 8"
    ).fetchall()
    conn.close()
    return [r[0].strip() for r in rows if r[0]]


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def _topic_tokens(params: dict) -> list[str]:
    out: list[str] = []
    for t in params.get("objeto", []) or []:
        if isinstance(t, list):
            out.extend(str(x) for x in t)
        else:
            out.append(str(t))
    return out


def _is_relevant(row: dict, field: str, topic: list[str]) -> bool:
    """RELEVANTE si el TÍTULO contiene todos los tokens de tópico (accent-folded)."""
    if not topic:
        return False
    title = _fold(str(row.get(field, "")))
    return all(_fold(tok) in title for tok in topic)


def _precision_at_k(rows: list[dict], field: str, topic: list[str], k: int) -> float:
    top = rows[:k]
    if not top:
        return 0.0
    rel = sum(1 for r in top if _is_relevant(r, field, topic))
    return rel / len(top)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", type=int, default=400, help="LIMIT de la fase recall (treatment)")
    ap.add_argument("--topn", type=int, default=10, help="k para precision@k")
    ap.add_argument("--display", type=int, default=50, help="recorte de presentación")
    ap.add_argument("--from-db", action="store_true", help="usar queries reales de traces")
    ap.add_argument("--max", type=int, default=60, help="máx queries a correr (red)")
    args = ap.parse_args(argv)

    s = Settings()
    s.secop_timeout_seconds = 30
    client = SecopClient(
        domain=s.datos_gov_domain,
        app_token=s.secop_app_token,
        timeout=30,
        max_retries=2,
    )
    router = QueryRouter()
    builder = SoQLBuilder()
    scorer = RelevanceScorer()

    # ── Selección de queries ───────────────────────────────────────────
    candidates = _load_db_queries() if args.from_db else QUERIES
    # Autofiltro: solo queries DE TÓPICO (objeto no vacío) — donde precision@k
    # tiene sentido. Excluye lookups por NIT, entidad-pura, analíticas sin tema.
    selected: list[tuple[str, dict]] = []
    for q in candidates:
        try:
            p = dict(router.parse(q).params)
        except Exception:
            continue
        if _topic_tokens(p):
            selected.append((q, p))
        if len(selected) >= args.max:
            break
    print(f"Seleccionadas {len(selected)} queries de tópico de {len(candidates)} candidatas\n")

    print(f"{'QUERY':<52} {'base@'+str(args.topn):>9} {'treat@'+str(args.topn):>10}  {'Δ':>6}  fetched")
    print("-" * 95)

    base_scores: list[float] = []
    treat_scores: list[float] = []
    zero_fetch = 0

    for q, params in selected:
        dataset = params.get("dataset")
        ds_id = builder.dataset_id_for(dataset)
        field = "objeto_del_contrato" if ds_id == builder.CONTRATOS_DATASET else "nombre_del_procedimiento"
        topic = _topic_tokens(params)

        # ── BASELINE: limit 50, sin rerank (orden del servidor = fecha DESC) ──
        try:
            base_params = {**params, "limit": args.display}
            base_soql = builder.build(ds_id, base_params)
            base_rows = client.query(ds_id, base_soql)
        except Exception as exc:
            print(f"{q[:50]:<52} ERROR baseline: {exc!s:.30}")
            continue
        # rerank solo si objeto>=3 (réplica de normal_flow)
        if len(topic) >= 3 and base_rows:
            scored = scorer.score_rows(topic, base_rows, field=field)
            base_rows = [r for r, _ in scored]
        base_p = _precision_at_k(base_rows, field, topic, args.topn)

        # ── TREATMENT: fetch grande, rerank umbral>=1, recorte a display ──
        try:
            treat_params = {**params, "limit": args.fetch}
            treat_soql = builder.build(ds_id, treat_params)
            treat_rows = client.query(ds_id, treat_soql)
        except Exception as exc:
            print(f"{q[:50]:<52} ERROR treatment: {exc!s:.30}")
            continue
        fetched = len(treat_rows)
        if fetched == 0:
            zero_fetch += 1
        if len(topic) >= 1 and treat_rows:
            scored = scorer.score_rows(topic, treat_rows, field=field)
            treat_rows = [r for r, _ in scored]
        treat_rows = treat_rows[: args.display]
        treat_p = _precision_at_k(treat_rows, field, topic, args.topn)

        base_scores.append(base_p)
        treat_scores.append(treat_p)
        delta = treat_p - base_p
        flag = "↑" if delta > 0.001 else ("↓" if delta < -0.001 else "·")
        print(f"{q[:50]:<52} {base_p:>9.2f} {treat_p:>10.2f}  {flag}{delta:>+5.2f}  {fetched:>5}")
        time.sleep(0.3)

    print("-" * 95)
    n = len(base_scores)
    if n:
        mb = sum(base_scores) / n
        mt = sum(treat_scores) / n
        print(f"{'PROMEDIO precision@'+str(args.topn):<52} {mb:>9.2f} {mt:>10.2f}  {mt-mb:>+6.2f}")
        wins = sum(1 for b, t in zip(base_scores, treat_scores) if t - b > 0.001)
        losses = sum(1 for b, t in zip(base_scores, treat_scores) if t - b < -0.001)
        print(f"\nQueries: {n}  |  mejoran: {wins}  empeoran: {losses}  igual: {n-wins-losses}")
        print(f"Fuga B (0 resultados pese a ser de tópico): {zero_fetch}/{n} queries")
        print(f"fetch={args.fetch}  display={args.display}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
