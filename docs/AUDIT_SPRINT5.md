# AUDIT SPRINT 5 — Bugs HIGH/MEDIUM pendientes en CORPUS_BACKLOG.md

Auditoría ejecutada: 2026-05-23. Fuente: `docs/CORPUS_BACKLOG.md` (actualizado post-Sprint 4).
Método: grep codebase + tests + docs para cada bug. Verificar (a) código, (b) test, (c) test pasa, (d) caso reproducible.

---

## Tabla de auditoría

| # | Bug ID | Severidad | Estado real | Función | Test existente | ¿Test pasa? | Caso reproducible | Sprint sugerido |
|---|--------|-----------|-------------|---------|----------------|-------------|-------------------|-----------------|
| 1 | STATE-CATALOG-INCOMPLETE-001 | CRITICAL | **PARCIAL — audit corrido, catalog no ampliado** | `estado_families.py` (verify: 2026-05-12) | No hay test que valide exhaustividad del catálogo | ✅ código compila | `discover_secop.py` existe pero su output no se usó para ampliar el catálogo. Mariquita SAMC-JCT-006 sigue sin aparecer probablemente. | Post-cohorte (Bloque C) |
| 2 | SOURCE-COVERAGE-001 | MEDIUM | **VIVO — 0% implementado en código** | Nada | No existe | N/A | Ningún path muestra "delay 24-48h de Socrata". Solo documentado en CORPUS_BACKLOG como intención. | Post-cohorte (UX decision) |
| 3 | FOLLOWUP-ENTITY-PURGE-001 | HIGH | **CLOSED Sprint 1A — backlog desactualizado** | `followup_engine.py:758-770` (`_purge_stale_inherited_params`: purga `entidad*` al cambiar scope sin `entidad_resolved` nuevo) | `test_plan_query_city_extraction.py` lines 40-52 (P9: entity autopromotion purge) + `test_sprint2a_canonical_smoke.py` line 28 | ✅ 736+ passed | Código verificado: si `_scope_differs` devuelve True y el nuevo scope no trae `entidad_resolved`, purga `entidad`, `entidad_resolved`, `entidad_resolution`, `entidad_like`. | **Cerrado Sprint 1A (B4)** |
| 4 | NIT-AMOUNT-CONFUSION-001 | HIGH | **CLOSED Sprint 2A — backlog desactualizado** | `query_router.py:868` (`_extract_nit()`) | `test_query_router.py:316` (`test_monto_no_confundido_con_nit`) | ✅ 736+ passed | "2000 millones" → `valor_min=2B`, `contratista=None`. Test confirma discriminación. | **Cerrado Sprint 2A** |
| 5 | BIDDER-CATALOG-AND-001 | HIGH | **CLOSED Sprint 1A — backlog desactualizado** | `intent_vocabulary.py:121-127` (bidder intent: vender/vendo/ofrezco → `estado_family=oferta_abierta`) + `query_router.py:958-993` (OR groups via "o") | `test_bidder_intent_commercial.py` (vendo uniformes, ofrezco insumos, quiero vender carpas) | ✅ 736+ passed | "vendo X, Y y Z" → objeto con grupos OR. Verificado en `_extract_object_terms`: detecta "o" entre tokens y agrupa como `list[list[str]]`. | **Cerrado Sprint 1A** |
| 6 | POLYSEMIC-TOPIC-001 | HIGH | **VIVO — 0% implementado en código** | Nada | No existe | N/A | "alojamiento" con 5 sentidos (hospedaje/hosting/logístico/albergue/militar) sin desambiguar. Solo documentado en SPRINT_1A_IMPLEMENTATION_PLAN.md. | Sprint 7 (catálogo curado) |
| 7 | FOLLOWUP-DATASET-PURGE-001 | HIGH | **CLOSED Sprint 1A — backlog desactualizado** | `followup_engine.py:576` (`_merge_change_dataset`: hereda scope/topic, NO hereda intent_type/estado_family) | `test_e2e_followup_context.py:31` (`test_followup_preserves_dataset_and_objeto`) | ✅ 736+ passed | Cambiar dataset en follow-up → solo hereda scope y topic (si hay overlap), no intent_type ni estado. | **Cerrado Sprint 1A (B6)** |
| 8 | PROCUREMENT-ACTION-001 | HIGH | **VIVO — 0% implementado en código** | Nada. "adquisición"/"compra"/"contratar" no aparecen como `objeto_signal` ni se excluyen del WHERE. | No existe | N/A | Estos verbos entran como AND obligatorio en el WHERE cuando deberían ser solo signal (no filtro). | Sprint 5 Fase 2 (~1h) |
| 9 | ENTITY-MULTI-REGIONAL-001 | HIGH | **VIVO — 0% implementado en código** | Nada. No hay catálogo de entidades multi-regional ni composición con departamento. | No existe | N/A | "SENA Bolívar" → SENA SECRETARIA GENERAL (Bogotá), no SENA REGIONAL BOLÍVAR. Requiere catálogo curado de regionales. | Sprint dedicado (fuera de Fase 2) |
| 10 | RELEVANCE-PHRASE-001 | HIGH | **VIVO — 0% implementado en código** | Nada. No hay BM25, shingle bigramas, ni boost de frase exacta. | No existe | N/A | "control de calidad de agua para consumo humano" → 6 ANDs → 0 resultados. Requiere capa de relevance. | Sprint 6 (BM25) |
| 11 | DEDUP-PROCESS-001 | MEDIUM | **CLOSED — implementado en formatter.py** | `formatter.py:73-94` (`_dedupe_results`: deduplica por `id_del_proceso` o `id_contrato`) | **No hay test explícito** de `_dedupe_results`. Probado colateralmente en tests de formatter. | ✅ La función compila | 4 casos confirmados en CORPUS_BACKLOG. La dedup existe pero sin test unitario — riesgo de regresión silenciosa. | **Cerrado (con deuda de test)** |
| 12 | AUTO-FALLBACK-WITHOUT-QUALITY-001 | MEDIUM | **PARCIAL — degraded_hint existe pero no avisa año** | `response_policy.py:260-262` (`degraded_hint`) + `orchestrator.py` (degrade_query) | `test_degrade_query.py` | ✅ 8 passed | Mensaje: "⚠️ No encontre resultados exactos. Te muestro resultados sin filtro de fechas". NO avisa que los resultados pueden ser de otros años (ej: 2023 cuando pidió 2026). | Sprint 5 Fase 2 (~1h) |
| 13 | MORPHO-EXPANSION-INCONSISTENT-001 | MEDIUM | **PARCIAL — ambas rutas usan ROOT_TO_VARIANTS pero en capas distintas** | `soql_builder.py:49,378-379` (usa `ROOT_TO_VARIANTS` en LIKE) + `query_router.py:26` (usa `VARIANT_TO_ROOT` en parser) | No hay test que compare expansión en ambas rutas | ✅ Tests pasan | En ruta `heuristic_only`, `_extract_object_terms` usa `VARIANT_TO_ROOT` para normalizar tokens. En ruta `heuristic_plus_llm`, el LLM puede devolver términos que el parser no expande. El SoQL builder aplica `ROOT_TO_VARIANTS` en ambos casos — debería ser consistente. | Sprint 5 Fase 2 (~30min verificar) |
| 14 | MORPHO-UNEVEN-001 | MEDIUM | **PARCIAL — cobertura desigual documentada** | `app/data/morphological_variants.py` (11KB, ~200 variantes) | No hay test de cobertura por rubro | ✅ Tests pasan | Cultura expandido, turismo no. Documentado en CORPUS_BACKLOG como "curación manual pendiente para 25 rubros frecuentes". | Sprint 5 Fase 2 (~1.5h) |
| 15 | HEADER-TOPIC-OMISSION-001 | LOW | **CLOSED — función implementada** | `response_policy.py:312-341` (`_build_interpretation`: renderiza objeto, entidad, depto, valor) | Probado colateralmente en tests de formatter | ✅ 736+ passed | Header muestra "Procesos de X en Y". Si `objeto` está vacío (bug de parsing), omite el topic — pero eso es upstream, no del formatter. | **Cerrado** |
| 16 | MOJIBAKE-DISPLAY-001 | LOW | **VIVO — 0% implementado en código** | Nada. No hay `unicode.normalize` ni reemplazo de `Ã³`/`Ã±` en formatter. | No existe | N/A | Caracteres mal decodificados de SECOP (`Ã³` por ó, `Ã±` por ñ). No se normalizan en el formatter. | Sprint 5 Fase 2 (~30min) |

---

## Resumen

| Estado | Count | Bugs |
|--------|-------|------|
| CLOSED (backlog desactualizado) | 6 | #3 FOLLOWUP-ENTITY-PURGE, #4 NIT-AMOUNT, #5 BIDDER-CATALOG, #7 FOLLOWUP-DATASET-PURGE, #11 DEDUP-PROCESS, #15 HEADER-TOPIC-OMISSION |
| PARCIAL | 4 | #1 STATE-CATALOG, #12 AUTO-FALLBACK, #13 MORPHO-EXPANSION, #14 MORPHO-UNEVEN |
| VIVO | 6 | #2 SOURCE-COVERAGE, #6 POLYSEMIC-TOPIC, #8 PROCUREMENT-ACTION, #9 ENTITY-MULTI-REGIONAL, #10 RELEVANCE-PHRASE, #16 MOJIBAKE-DISPLAY |

---

## Elegibles para Fase 2 (VIVO/PARCIAL, ≤2h, dentro de scope)

| Bug | Horas est. | Score (apariciones × severidad / horas) | Elegible |
|-----|-----------|----------------------------------------|----------|
| #8 PROCUREMENT-ACTION-001 | 1.0 | 1×2/1 = 2.0 | ✅ Fase 2 |
| #16 MOJIBAKE-DISPLAY-001 | 0.5 | 1×1/0.5 = 2.0 | ✅ Fase 2 |
| #12 AUTO-FALLBACK-001 | 1.0 | 1×1/1 = 1.0 | ✅ Fase 2 |
| #14 MORPHO-UNEVEN-001 | 1.5 | 1×1/1.5 = 0.67 | ⚠️ Opcional |
| #13 MORPHO-EXPANSION-001 | 0.5 | 1×1/0.5 = 2.0 | ✅ Fase 2 |
| #1 STATE-CATALOG-001 | 2.0+ | 1×3/3 = 1.0 | ❌ Requiere audit previo |
| #2 SOURCE-COVERAGE | ∞ | N/A | ❌ Decisión UX, no técnica |
| #6 POLYSEMIC-TOPIC | ∞ | N/A | ❌ Catálogo curado (Sprint 7) |
| #9 ENTITY-MULTI-REGIONAL | ∞ | N/A | ❌ Catálogo dedicado |
| #10 RELEVANCE-PHRASE | ∞ | N/A | ❌ Sprint 6 (BM25) |

## Bugs con deuda de test (no bloquean)

- **#11 DEDUP-PROCESS-001**: `_dedupe_results()` implementado pero sin test unitario. Bajo riesgo de regresión porque es función pura.
- **#4 NIT-AMOUNT-CONFUSION-001**: Test existe pero no cubre todos los edge cases (NIT con DV, formato con puntos, etc.).
- **#13 MORPHO-EXPANSION-INCONSISTENT-001**: Ambas rutas usan ROOT_TO_VARIANTS en SoQL builder — la inconsistencia puede ser falsa alarma.

---

## Recomendación Fase 2

Ejecutar 4 fixes rápidos (≤1h cada uno) en este orden:

1. **#8 PROCUREMENT-ACTION-001** (~1h): Agregar verbos contractuales a señal, no filtro WHERE.
2. **#16 MOJIBAKE-DISPLAY-001** (~30min): `unicodedata.normalize('NFKC')` en formatter.
3. **#12 AUTO-FALLBACK-WITHOUT-QUALITY-001** (~1h): degraded_hint incluya año real de resultados.
4. **#13 MORPHO-EXPANSION-INCONSISTENT-001** (~30min): Verificar que `ROOT_TO_VARIANTS` se aplica en ambos paths. Probablemente ya es consistente — solo falta test de verificación.

Total estimado: ~3h. Los 4 son independientes, mergeables uno a uno.
