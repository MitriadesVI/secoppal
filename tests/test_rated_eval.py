"""Guardia de regresión sobre queries REALES calificadas por usuarios.

A diferencia de los tests que validan rutas internas con params sintéticos,
esto mide precisión parser-tier sobre el eval set construido desde
data/feedback.jsonl (tests/fixtures/rated_eval_v1.yaml). El número debe
SUBIR con cada fix y nunca bajar de la línea base.

Ver scripts/eval_rated.py para correrlo interactivo y por tag.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

yaml = pytest.importorskip("yaml")

from scripts.eval_rated import assert_expected  # noqa: E402

EVAL_SET = REPO_ROOT / "tests" / "fixtures" / "rated_eval_v1.yaml"

# Línea base 2026-06-01 → 2026-06-02 (70 calificadas de SQLite).
# 5 de 6 bugs parser-tier resueltos: RATED-016 (oferta→stopwords),
# 040 (sobre→boundary word en entity_types), 041 (oficial→stopwords),
# 059 (software→software en morpho), 067 ('o similares': \b en regex de montos
# + stopwords). RATED-069 (banco=municipio) se RECLASIFICÓ a pipeline-tier:
# el fix regex rompía Banco Agrario / Banco de Bogotá / Banco Mundial; es caso
# de desambiguación LLM, no de parser. Parser-tier quedó en 38 entradas, 38/38.
BASELINE_PASS = 38
BASELINE_TOTAL = 38


def _parser_entries() -> list[dict]:
    data = yaml.safe_load(EVAL_SET.read_text()) or {}
    return [e for e in data.get("queries", []) if e.get("tier", "parser") == "parser"]


@pytest.fixture(scope="module")
def router():
    from app.core.query_router import QueryRouter
    return QueryRouter()


def test_parser_tier_precision_floor(router):
    """La precisión parser-tier no debe caer por debajo de la línea base."""
    entries = _parser_entries()
    assert len(entries) == BASELINE_TOTAL, (
        f"el eval set cambió de tamaño ({len(entries)} vs {BASELINE_TOTAL}); "
        "actualiza BASELINE_TOTAL conscientemente"
    )
    passed = 0
    failures: list[str] = []
    for e in entries:
        actual = dict(router.parse(e["query"]).params)
        fails = assert_expected(actual, e.get("expected", {}) or {})
        if fails:
            failures.append(f"{e['id']}: {fails}")
        else:
            passed += 1
    assert passed >= BASELINE_PASS, (
        f"REGRESIÓN: precisión bajó a {passed}/{len(entries)} "
        f"(piso={BASELINE_PASS}). Fallos:\n  " + "\n  ".join(failures)
    )


def test_rated_thumbs_up_never_regress(router):
    """Las queries que el usuario calificó 👍 deben seguir parseando bien."""
    failures: list[str] = []
    for e in _parser_entries():
        if e.get("recorded_rating") != 1:
            continue
        actual = dict(router.parse(e["query"]).params)
        fails = assert_expected(actual, e.get("expected", {}) or {})
        if fails:
            failures.append(f"{e['id']} ({e['query'][:40]!r}): {fails}")
    assert not failures, "Queries 👍 regresaron:\n  " + "\n  ".join(failures)
