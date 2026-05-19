# SECOPPAL — Baseline de medición

Punto cero (M0) capturado antes del Sprint 1A del plan de robustecimiento sistémico
(ver [docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md](../docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md)
sección 9).

Sin este snapshot no hay forma honesta de saber si un sprint produjo retroceso o avance.

## Estructura

Por cada sprint S se capturan dos juegos:

- `MS_pytest_before.txt` / `MS_pytest_after.txt`
- `MS_corpus_before.txt` / `MS_corpus_after.txt`
- `MS_lint_before.txt` / `MS_lint_after.txt`

M0 es el estado del repo en su commit base, antes de tocar nada.

## M0 — 2026-05-18

| Métrica | Valor |
|---|---|
| Commit base | `929634c docs: align SECOPPAL documentation with corpus green state` |
| pytest | **579 passed, 3 xfailed** (72.95s) |
| corpus v1 (`--mode all`) | **88/88 PASS, 0 FAIL** |
| `make lint-core` | **app/core/ limpio** |

## Línea roja

Cualquier sprint que produzca:
- una transición `pytest passed → failed`,
- una transición `corpus PASS → FAIL`,
- `make lint-core` no verde,
- o modificaciones no intencionales en `data/feedback.jsonl`,

**no se mergea.** Cero excepciones. La línea roja se verifica con
`scripts/quality_report.py` (entregable del Sprint 0A — aún no existe).

## Cómo capturar un snapshot

```bash
source .venv/bin/activate
pytest -q | tee baseline/MX_pytest.txt
python scripts/run_query_corpus.py --mode all | tee baseline/MX_corpus.txt
make lint-core | tee baseline/MX_lint.txt
git log --oneline -1 > baseline/MX_commit.txt
```

Reemplazar `MX` por el identificador del snapshot (`M0`, `M1A_before`, `M1A_after`, etc.).
