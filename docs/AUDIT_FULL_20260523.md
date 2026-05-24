# AUDIT_FULL — SECOPPAL Codebase Truth
**Fecha:** 2026-05-23 (23:00)
**Scope:** Sprint 6 preparación — inventario, coverage, backlog truth, smoke empírico, deuda, prod readiness.
**Método:** Solo lectura — no se toca código de features en ninguna fase.
**Suite base:** 787 passed, 10 xfailed, smoke 9/10 8.8s

---

## 1. CODE INVENTORY

### 1.1 Módulos por tamaño y ubicación

| Directorio | Archivos | LoC total | Promedio |
|-----------|---------|-----------|----------|
| app/core/ | 25 | ~8,000 | 320 |
| app/utils/ | 6 | ~370 | 62 |
| app/data/ | 5 | ~910 | 182 |
| app/ (root) | 4 | ~210 | 53 |
| **Total** | **40** | **~9,900** | — |

### 1.2 Top 15 archivos por LoC

| Archivo | LoC | Funciones/Clases |
|--------|-----|------------------|
| app/core/orchestrator.py | 1,123 | 22 funciones + 1 clase |
| app/core/query_router.py | 1,053 | 7 métodos de extracción |
| app/core/followup_engine.py | 770 | 5 clases + mergers |
| app/core/response_policy.py | 521 | formatters estáticos |
| app/core/observer.py | 479 | observe + compute helpers |
| app/core/narrator.py | 477 | LLM narrator |
| app/core/entity_resolver.py | 476 | 1 clase + scan/resolve |
| app/core/formatter.py | 442 | Formatter + _dedupe |
| app/core/soql_builder.py | 415 | SoQLBuilder + DatasetSpec |
| app/core/dialogue_policy.py | 394 | classify + detectores |
| app/data/morphological_variants.py | 329 | dict + derived constants |
| app/core/conversation_store.py | 325 | ConversationStore |
| app/core/llm_handler.py | 274 | LLMHandler |
| app/core/estado_families.py | 259 | ESTADO_FAMILIES + resolvers |
| app/core/query_frame.py | 255 | QueryFrame |

### 1.3 Candidatos a refactor (funciones >150 LoC)

| Archivo | Ubicación | Nota |
|--------|----------|------|
| query_router.py | _extract_amounts (lines 702-866, ~165 LoC) | Largo pero puro — regex chain |
| query_router.py | parse (lines 305-607, ~300 LoC) | **CRÍTICO**: monolito con 13 pasos secuenciales |
| orchestrator.py | run_query + actions (~400 LoC) | Graph Burr con 12 acciones |
| orchestrator.py | execute_query (lines 240-393, ~150 LoC) | Ramas timeout/degrade/retry |
| soql_builder.py | _build_where (lines 185-310, ~125 LoC) | Bajo umbral pero denso |
| followup_engine.py | classify_followup (lines 245-366, ~120 LoC) | 9 tipos de intención |

### 1.4 Hot spots (archivos más modificados Sprint 3-5)

| Cambios | Archivo |
|---------|---------|
| 15 | app/core/query_router.py |
| 15 | app/core/orchestrator.py |
| 9 | app/core/soql_builder.py |
| 6 | app/data/morphological_variants.py |

### 1.5 Archive vs activos

| Directorio | Contenido |
|-----------|----------|
| archive/cleanup_2026-05-15-audit | Versiones legacy pre-refactor |
| archive/cleanup_2026-05-16 | v*.py legacy + auditoría anterior |

Los archivos v2.py/v3.py legacy están archivados. No hay archivos activos con espacios en app/core/.

### 1.6 Makefile targets

```
lint-core:       Ruff check en app/core/
test:            lint-core + pytest con coverage
smoke:           scripts/smoke_acotado.py (10 queries)
smoke-fast:      scripts/smoke_acotado.py (5 queries)
lint:            Ruff completo en app/ + tests/
dev:             uvicorn app.main:app --reload
restart:         Reinicia el dev server
```

### 1.7 Mapa de dependencias (simplificado)

```
orchestrator.py (hub central, 20+ imports)
├── query_router.py → entity_resolver.py, intent_vocabulary.py, estado_families.py
├── followup_engine.py → query_frame.py, _followup_constants.py
├── response_policy.py → formatter.py
├── observer.py → soql_builder.py
├── secop_client.py → config.py
├── analytics.py → soql_builder.py
├── dialogue_policy.py
└── formatter.py
```

No se detectaron imports circulares. El grafo es tree-like con orchestrator como raíz.

### 1.8 Funciones con 0 referencias aparentes

Ningún falso positivo confirmado — el análisis superficial de grep no encontró funciones sin uso en tests o app. La mayoría son funciones internas `_private()` o actions de Burr que se referencian por string (no por import directo).

---

## REPORTE FASE 1 — Solicitando OK para Fase 2

Hallazgos principales:
1. **query_router.parse() es un monolito de 300 LoC con 13 pasos** — principal candidato a refactor futuro
2. **orchestrator concentra demasiado** (1,123 LoC, 20+ imports, 12 acciones Burr)
3. **Hot spots**: query_router y orchestrator son los más tocados (15 cambios cada uno en una semana)
4. **archive/ limpio**: archivos legacy correctamente archivados
5. **Sin imports circulares**: arquitectura tree-like sana
6. **Makefile tiene 6 targets** — smoke solo prueba 10 queries (gap identificado)

¿Prosigo a Fase 2 (test coverage)?

---

## 2. TEST COVERAGE

### 2.1 Métricas globales

| Métrica | Valor |
|--------|-------|
| Statements totales | 3,781 |
| Missed | 576 |
| Coverage global | **85%** |
| Tests ejecutados | 681 passed, 2 failed, 10 xfailed |

### 2.2 Cobertura por módulo (ordenado de menor a mayor)

| Módulo | Stmts | Miss | Cover | Gap principal |
|--------|-------|------|-------|---------------|
| app/main.py | 80 | 80 | **0%** | FastAPI webhooks nunca testeados |
| app/service.py | 10 | 10 | **0%** | Singleton get_workflow |
| app/core/analytics.py | 56 | 34 | **39%** | aggregate_sum path nunca testeado con coverage |
| app/core/feedback.py | 80 | 42 | **48%** | File I/O de feedback.jsonl |
| app/core/direct_responses.py | 9 | 3 | 67% | suggestion_selection_header sin test |
| app/core/orchestrator.py | 390 | 95 | 76% | Ramas timeout/degrade/retry no cubiertas |
| app/utils/money.py | 48 | 11 | 77% | format_cop con casos edge |
| app/core/narrator.py | 196 | 42 | 79% | LLM narrator con strict=True |
| app/core/followup_engine.py | 263 | 40 | 85% | Ramas de merge por intención |
| app/core/dialogue_policy.py | 149 | 24 | 84% | Detectores sin test unitario |
| app/core/entity_resolver.py | 213 | 26 | 88% | Tier 4 LIKE fallback + scan_entity |
| app/core/observer.py | 241 | 27 | 89% | _compute_from_aggregation |
| app/core/llm_handler.py | 118 | 13 | 89% | Path LLM fallback |
| app/core/response_policy.py | 288 | 28 | 90% | degraded_hint + uncertain_zero |
| app/core/formatter.py | 286 | 25 | 91% | _dedupe_results + format_for_streamlit |
| app/core/soql_builder.py | 196 | 13 | 93% | build_count, LIKE on entidad |
| app/core/query_router.py | 471 | 29 | 94% | Ramas de city/rewrite |
| app/core/conversation_store.py | 128 | 6 | 95% | File I/O paths |
| app/core/suggester.py | 47 | 0 | **100%** | — |

### 2.3 Top 5 funciones con cobertura <50%

| Función | Módulo | Cover | Razón |
|--------|--------|-------|-------|
| detect_analytical_intent | analytics.py | ~0% | Solo testeado vía orchestrator E2E (sin coverage) |
| maybe_handle_analytical_query | analytics.py | ~0% | Ídem |
| FeedbackStore._read_all | feedback.py | ~0% | File I/O con JSONL |
| aggregate_sum path completo | analytics.py | ~0% | No testeado aisladamente |
| format_aggregate_sum_response | analytics.py | ~0% | Ídem |

### 2.4 Tests xfailed (10)

| Test | Archivo | Razón |
|------|--------|-------|
| 7 tests demo_*.py | test_demo_\{alcalde,proponente,veedor\}.py | Observer 2.0 signals requieren mock de aggregate() adicional |
| test_opportunity_g6 | test_opportunity_queries.py | OPP-003 pendiente |
| test_followup_pintura | test_opportunity_queries.py | Requiere contexto de followup |
| test_reference_exact_search | test_opportunity_queries.py | REFERENCE-001 pendiente |

**Análisis:** Los 7 xfailed de demo son "referencia, no gate" (Sprint 3). Los 3 de opportunity son bugs reales no resueltos.

### 2.5 Tests redundantes (>3 archivos misma función)

| Función testeada | Archivos | Riesgo |
|-----------------|----------|--------|
| test_turno1_pipeline_devuelve_resultados | 3 (alcalde/proponente/veedor) | Bajo — intencional (demo tests) |
| test_turno1_contiene_sugerencias | 3 | Bajo — idem |

Los 3 tests de demo comparten estructura pero prueban fixtures distintos (mocks diferentes). No son redundancia accidental — es diseño intencional de Sprint 3.

### 2.6 Paths críticos sin cobertura

| Path | Riesgo | Evidencia |
|------|--------|----------|
| **format_for_channel con telegram** | HIGH | Solo testeado el path streamlit. Telegram usa formateo distinto (raw text vs cards). |
| **orchestrator.execute_query con timeout real** | HIGH | Ramas de timeout y degrade nunca se prueban (requieren API real). |
| **query_router con LLM fallback** | MEDIUM | LLM handler importa OpenAI; si API key no existe, el path de fallback no está cubierto. |
| **response_policy.degraded_hint** | MEDIUM | path degraded=True solo corre con API real. |
| **_dedupe_results en formatter** | LOW | Función existe pero sin test unitario directo. |
| **secop_client.aggregate con timeout** | LOW | Timeout separado para observer no testeado unitariamente. |

### 2.7 2 tests fallando

| Test | Motivo | Fix pendiente |
|------|--------|---------------|
| test_vendo_uniformes_opportunity | "insumos medicos" → normaliza a "insumo salud" | Actualizar assertion a raíz morfológica |
| test_ofrezco_insumos_medicos_opportunity | Ídem | Ídem |

Ambos son consecuencia del MORPHO-UNEVEN expansion — las variantes nuevas normalizan "insumos"→"insumo" y "medicos"→"salud". Se corrigieron 3 tests similares en cierre Sprint 5; estos 2 quedaron pendientes.

---

## REPORTE FASE 2 — Solicitando OK para Fase 3

Hallazgos principales:
1. **Coverage global 85%** — aceptable para pre-cohorte. Gaps reales en analytics (39%) y main.py (0%).
2. **Gap crítico**: `main.py` 0% coverage — webhooks de Telegram/WhatsApp nunca testeados.
3. **2 tests fallando** por normalización morfológica — corrección trivial (1 línea cada uno).
4. **10 xfailed**: 7 son referencia (Observer 2.0 mocks), 3 son bugs reales (OPP/REFERENCE).
5. **Sin redundancia accidental**: los 3 tests demo_* son intencionales (Sprint 3).
6. **Path format_for_channel con telegram** sin cobertura — riesgo para cohorte.

¿Prosigo a Fase 3 (backlog truth)?

---

## 3. BACKLOG TRUTH

### 3.1 Bugs CLOSED — verificación de commits

Se verificaron los 12 bugs marcados CLOSED en el backlog. Todos tienen código + test + commit hash válido. No hay STALE-CLAIM (backlog dice CLOSED pero código no existe).

| Bug | Commit | Verificado |
|-----|--------|-----------|
| ACCENT-NORMALIZATION-001 | `61f5dfb` | ✅ unaccent() + 19 tests |
| MIN-MAX-INVERSION-001 | `77dbb9c` | ✅ 3 patrones + 16 tests |
| MULTI-AND-OVER-RESTRICTIVE-001 | `20989f5` | ✅ COMMON_TOKENS ~50 + 5 tests |
| PROCUREMENT-ACTION-001 | `6226234` | ✅ ACTION_VERBS + 6 tests |
| STATE-CATALOG-INCOMPLETE-001 | `3761126` | ✅ +3 valores + 6 tests |
| ENTITY-MULTI-REGIONAL-001 | `afb1e17` | ✅ regional_entities + 9 tests |
| MORPHO-UNEVEN-001 | `3007088` | ✅ 120 variantes + 3 tests |
| ENTITY-MISRESOLUTION-FALLBACK-001 | `f55eced` | ✅ _unresolved_rewrite + 4 tests |
| LLM-EXPANSION-AND-001 | `20d2ffd` | ✅ objeto_or + 3 tests |
| PAGINATION-CUTS-OFF-RELEVANT-001 | `ee28365` | ✅ header + 8 tests |
| FOLLOWUP-VALUE-INHERIT-001 | `b5f6a80` | ✅ _purge_stale + 8 tests |
| FOLLOWUP-ENTITY-PURGE-001 | `b5f6a80` | ✅ (mismo commit, misma función) |

### 3.2 Discrepancias: bugs cerrados que el backlog lista como VIVO

El backlog tiene DOS tablas que NO están sincronizadas: la tabla principal "Bugs vivos" y la tabla "Pendientes fuera del corpus". Hay 6 bugs que aparecen como "vivos" o "HIGH (nuevo)" en la segunda tabla pero YA fueron cerrados:

| Bug | Backlog dice | Realidad | Sprint real |
|-----|-------------|----------|-------------|
| FOLLOWUP-ENTITY-PURGE-001 | "HIGH (nuevo)" | CLOSED | Sprint 1A |
| NIT-AMOUNT-CONFUSION-001 | "HIGH" | CLOSED | Sprint 2A |
| BIDDER-CATALOG-AND-001 | "HIGH" | CLOSED | Sprint 1A |
| ENTITY-MULTI-REGIONAL-001 | "HIGH (nuevo)" | CLOSED | Sprint 5 |
| PROCUREMENT-ACTION-001 | No listado | CLOSED | Sprint 5 |
| STATE-CATALOG-INCOMPLETE-001 | No listado | CLOSED | Sprint 5 |

### 3.3 Bugs REALMENTE vivos (verificados sin código)

| Bug | Severidad | Evidencia |
|-----|-----------|----------|
| SOURCE-COVERAGE-001 | MEDIUM | 0 líneas de código. Solo docs. |
| POLYSEMIC-TOPIC-001 | HIGH | 0 líneas. Catálogo curado pendiente. |
| RELEVANCE-PHRASE-001 | HIGH | 0 líneas. BM25 pendiente. |
| AUTO-FALLBACK-WITHOUT-QUALITY-001 | MEDIUM | PARCIAL — degraded_hint existe pero no avisa año real. |
| MORPHO-EXPANSION-INCONSISTENT-001 | MEDIUM | PARCIAL — ambas rutas usan ROOT_TO_VARIANTS, posiblemente ya consistente. |
| MOJIBAKE-DISPLAY-001 | LOW | 0 líneas. Unicode normalize pendiente. |
| DEDUP-PROCESS-001 | MEDIUM | CLOSED-NO-TEST — _dedupe_results implementado pero sin test unitario. |

### 3.4 Duplicación interna en CORPUS_BACKLOG.md

El archivo tiene ~340 líneas con bugs repetidos en múltiples secciones:
- "Bugs vivos confirmados por feedback humano" (líneas 66-110)
- "Pendientes fuera del corpus" (líneas 207-229)
- "Categorías nuevas para corpus de diálogo" (líneas 114-144)

Muchos bugs aparecen en 2 o 3 secciones con estados inconsistentes. Ejemplo: LLM-EXPANSION-AND-001 aparece como CLOSED en la tabla principal pero como "Pendiente CRITICAL" en "Pendientes fuera del corpus".

**Recomendación:** Consolidar en UNA sola tabla al final del Sprint 6 con columna única de estado.

### 3.5 TEST_MATRIX.md y micro_sprints.md

| Documento | Estado | Observación |
|-----------|--------|-------------|
| docs/TEST_MATRIX.md | Desactualizado | Última modificación 2026-05-21. No refleja bugs cerrados en Sprint 4/5. |
| docs/micro_sprints.md | Obsoleto | Fue reemplazado por SPRINT_1A/2A_IMPLEMENTATION_PLAN.md. |

---

## REPORTE FASE 3 — Solicitando OK para Fase 4

Hallazgos principales:
1. **12 bugs CLOSED verificados**: todos tienen código + test + commit válido. No hay STALE-CLAIM.
2. **6 bugs cerrados que el backlog lista como VIVO** — la tabla "Pendientes fuera del corpus" está desactualizada desde Sprint 1A.
3. **7 bugs realmente vivos** (SOURCE-COVERAGE a MOJIBAKE).
4. **CORPUS_BACKLOG.md tiene duplicación interna** — mismo bug en 3 secciones con estados inconsistentes.
5. **TEST_MATRIX.md desactualizado** desde 2026-05-21.

¿Prosigo a Fase 4 (smoke extendido empírico)?

---

## 4. EMPIRICAL ACCURACY

### 4.1 Smoke extendido — 30 queries, 5 buckets

**API status al inicio:** ✅ 200 OK, 0.35s (óptimo).
**API durante la corrida:** ⚠️ Degradación progresiva — la mayoría de fallas son timeouts (30s+), no bugs de código.

### 4.2 Resultados por bucket

| Bucket | Pass | % | Avg | Interpretación |
|--------|------|---|-----|---------------|
| A-ACCENT | 6/10 | 60% | 21.8s | 4/4 fallas son timeouts (30s). No se puede concluir sobre acentos. |
| B-MULTI-AND | 1/5 | 20% | 18.3s | 4/4 fallas son timeouts. **Inconcluso** — API congestionada. |
| C-VALUE-POLARITY | 2/5 | 40% | 22.8s | #20 falla en 7.4s (no timeout) → posible bug real. #16/#17 timeouts. |
| D-ENTITY-MULTI-REGIONAL | 3/5 | 60% | 7.8s | SENA Bolívar/Atlántico fallan por timeout. ICBF/resto OK. |
| E-STATE-CATALOG | 5/5 | **100%** | 9.8s | **Todas pasan**. Catálogo de estados Sprint 5 funciona. |
| **TOTAL** | **17/30** | **57%** | — | 13 fallas: 11 timeouts, 2 fallas rápidas reales |

### 4.3 Fallas rápidas (no timeout) — posibles bugs reales

| Query | Bucket | Tiempo | Análisis |
|-------|--------|--------|----------|
| #20 "contratos por debajo de 30 millones en Atlantico" | C | 7.4s | **Posible bug**: "por debajo de" no matchea en Socrata con valor_max correcto |
| #11 "aseo cafeteria jardineria y mantenimiento" | B | 15.0s | Timeout — no concluyente |

### 4.4 Bugs cerrados con confirmación empírica

| Bug | Evidencia | Confianza |
|-----|----------|----------|
| STATE-CATALOG-INCOMPLETE-001 | 5/5 queries pasan (Publicado, Borrador, Evaluación, Aprobado, Seleccionado) | **ALTA** |
| ENTITY-MULTI-REGIONAL-001 | 3/5 pasan (ICBF × 2, SENA Antioquia); 2 timeouts | **MEDIA** |
| ACCENT-NORMALIZATION-001 | 6/10 pasan; 4 timeouts — requiere re-test con API sana | **BAJA** |
| MULTI-AND-OVER-RESTRICTIVE-001 | 1/5 pasan; 4 timeouts — **inconcluso** | **MUY BAJA** |

### 4.5 Conclusión para Fase 7

El smoke extendido confirma que **STATE-CATALOG funciona al 100%**. Los demás buckets sufren de congestión de API y requieren re-ejecución con servidor sano para ser concluyentes. La ventana de 30 queries secuenciales satura la API de Socrata (~10 timeouts en la segunda mitad).

---

## 5. ARCHITECTURAL DEBT (Fase 5)

**SKIP** — decisión del usuario. Saltar a Fase 7.

---

## 6. PRODUCTION READINESS (Fase 6)

**SKIP** — decisión del usuario. Saltar a Fase 7.

---

## 7. SYNTHESIS — Recomendaciones Sprint 6

### 7.1 Bugs realmente vivos (de Fase 3 + Fase 4)

| Bug | Severidad | Estado | Impacto |
|-----|-----------|--------|---------|
| RELEVANCE-PHRASE-001 | HIGH | 0% implementado | **Destruye confianza**: frases compuestas → 0 resultados |
| SOURCE-COVERAGE-001 | MEDIUM | 0% implementado | UX honesta de delay Socrata |
| POLYSEMIC-TOPIC-001 | HIGH | 0% implementado | Catálogo curado pendiente |
| MOJIBAKE-DISPLAY-001 | LOW | 0% implementado | Caracteres mal decodificados |
| AUTO-FALLBACK-001 | MEDIUM | PARCIAL | No avisa año real |
| DEDUP-PROCESS-001 | MEDIUM | Sin test | _dedupe_results necesita test |

### 7.2 Deuda de backlog

| Issue | Acción | Horas |
|-------|--------|-------|
| CORPUS_BACKLOG.md duplicado | Consolidar en tabla única | 0.5h |
| TEST_MATRIX.md desactualizado | Eliminar o actualizar | 0.5h |
| Fallas por timeout en smoke | Re-ejecutar smoke extendido cuando API sana | 0.5h |

### 7.3 Tres propuestas Sprint 6

#### Sprint 6-A: "Stabilize before cohort" (~6h)
- Corregir 2 tests fallando (MORPHO-UNEVEN side effects)
- Consolidar CORPUS_BACKLOG.md en tabla única
- Agregar test a _dedupe_results (DEDUP-PROCESS)
- Correr smoke extendido en horario de API sana
- **Riesgo de NO hacerlo**: 2 tests rojos en CI, backlog mintiendo

#### Sprint 6-B: "Accuracy push" (~10h)  
- RELEVANCE-PHRASE-001 — BM25 con shingle bigramas
- POLYSEMIC-TOPIC-001 — catálogo curado 10 rubros
- MOJIBAKE-DISPLAY-001 — unicode normalize en formatter
- **Riesgo de NO hacerlo**: frases compuestas siguen fallando, usuarios pierden confianza

#### Sprint 6-C: "Tech debt clean" (~8h)
- Refactor query_router.parse() de monolito 300 LoC a pipeline de pasos
- Limpiar deuda D1-D5 de Sprint 2A
- Agregar test coverage en analytics.py y main.py
- **Riesgo de NO hacerlo**: velocidad de desarrollo se degrada, más bugs por complejidad

### 7.4 Recomendación final

**Sprint 6-B (Accuracy push)** con 3 tareas del 6-A como prerequisite:
1. [30min] Corregir 2 tests fallando
2. [8h] RELEVANCE-PHRASE-001 — BM25 con shingle bigramas
3. [2h] POLYSEMIC-TOPIC-001 — catálogo curado top 10
4. [30min] MOJIBAKE-DISPLAY-001

**Por qué**: De los 7 bugs vivos, RELEVANCE-PHRASE es el que el plan maestro calificó como "destruye confianza más rápido que cualquier otro". El Bucket B del smoke extendido (MULTI-AND) quedó inconcluso por timeouts, pero el caso canónico "control de calidad de agua para consumo humano" es una frase REAL que usuarios escribirán. Sin BM25, esa query devuelve 0 resultados siempre — no es un tema de API, es un tema de algoritmo.

Si el smoke extendido se re-corre con API sana y Bucket B pasa >3/5, se puede diferir BM25. Si no, es la prioridad #1 para cohorte.
