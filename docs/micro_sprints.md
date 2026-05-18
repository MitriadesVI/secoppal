# Micro-sprints propuestos (post OPP-001/003)

Orden recomendado por usuario (Rodrigo):
1. Micro-sprint 1 — SAFE-SOQL-001
2. Micro-sprint 2 — CACHE-001
3. Micro-sprint 3 — DEMO/SMOKE
4. Micro-sprint 4 — AQ-001B (diferido a otra sesión)

## Micro-sprint 1 — SAFE-SOQL-001 (prioridad alta)

**Objetivo:** Cerrar N3. SoQLBuilder nunca debe generar WHERE 1=1 por accidente.

**Criterios de éxito verificables:**
- build sin scope/topic → ValueError explícito
- suggestion selection sin scope → no ejecuta query global
- analytics aggregate sin scope → needs_clarification
- opportunity_search sin objeto/scope (si aplica) → pide aclaración o ventana segura

**Tests mínimos:**
- tests/test_soql_builder.py::test_build_without_scope_raises
- tests/test_query_router.py::test_suggestion_without_scope_no_global_query

**Notas:** Protege todo lo que viene (oportunidades, analytics, Radar).

## Micro-sprint 2 — CACHE-001

**Objetivo:** Caché de COUNT para reducir latencia y costo en queries repetidas.

**Implementación mínima:**
- cache count_soql por (dataset + query_hash)
- TTL corto (ej. 60-120s)

**Tests:**
- misma query dos veces → llama count solo una vez
- query diferente → no usa cache
- TTL expirado → vuelve a consultar

**Archivo sugerido:** app/core/cache.py o dentro de secop_client.py

## Micro-sprint 3 — DEMO/SMOKE

**Objetivo:** Convertir fixes manuales en rutina reproducible.

**Entregables:**
- scripts/smoke_test.py (contra datos reales + mocks)
- scripts/demo_conversations.py (escenarios A/B/C + opportunity)
- make pre-demo (lint + tests + smoke rápido)

**Validación:** make pre-demo pasa limpio antes de cualquier demo.

## Micro-sprint 4 — AQ-001B (top entidades / contratistas)

**Objetivo:** Métricas agregadas para dashboard / sugerencias.

**Ejemplos:**
- top contratistas por valor en oportunidades de mantenimiento
- top entidades por cantidad en Atlántico
- distribución por modalidad

**Nota:** Diferido a otra sesión. No tocar en este sprint.

---

**Estado actual (2026-05-17):** 
- v1.2 plan activo en paso 3
- OPP-001/003 documentados y testeados
- Pendiente: integrar estos micro-sprints al plan maestro o como ramas separadas

**Próximo paso recomendado:** Iniciar SAFE-SOQL-001 (pequeño, protector).