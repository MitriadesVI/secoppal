# CORPUS BACKLOG — SECOPPAL Query Corpus V1

Fallos conocidos del corpus, agrupados por severidad. NO editar parser/core para hacer pasar estos casos — son bugs reales que requieren features o fixes separados.

**Última actualización:** 2026-05-18 — CORPUS-003 cerró los 6 high restantes. Corpus: **88/88 PASS, 0 FAIL.**

## Critical (0) ✅

Los 5 critical anteriores (R005, R027, R020, R028, R029) fueron cerrados en CORPUS-002:
- `convocadas` → `oferta_abierta`
- `oferta_abierta + procesos` → `opportunity_search`
- follow-up con topic nuevo ya no cae como `refine_filter`

## High (0) ✅

Los 6 high anteriores fueron cerrados en CORPUS-003A/B/C:
- CORPUS-V005 — "X o más" → valor_min
- CORPUS-V007 — "mil millones" → 1B, rango "desde X hasta Y"
- CORPUS-OC004/OC005 — bidder intent indirecto (quiero/donde me puedo presentar)
- CORPUS-R003 — `limpiar` → `limpieza`
- CORPUS-AN003 — "cuánto suma" analytical followup hereda contexto

## Low (0) ✅

Cerrados colateralmente durante CORPUS-002/CORPUS-003 o ya pasaban.

## Medium (0) ✅

Cerrados durante reconciliación de main.

---

## Pendientes fuera del corpus

Estos no son FAILs del corpus actual pero son bugs/features pendientes:

| ID | Descripción | Sprint |
|----|-------------|--------|
| DEMO-BLOCKERS-001 | 8 tareas UX/estado/scope (COMMAND-SLOT, OPP-UX, OPP-STATE, OPP-SUGGEST, OPP-RELEVANCE, OPP-TIMEOUT, STATE-PRIORITY, OPP-SCOPE) | Pendiente HIGH |
| VALUE-TYPO-001 | "milloones" normalizar a "millones", valor_min > valor_max → aclaración | Pendiente |
| REFERENCE-001 | Búsqueda exacta por referencia de proceso (test_dicar xfail) | Pendiente |
| CORPUS-CI-001 | known_fail/expected_fail para gate de CI | Pendiente |
