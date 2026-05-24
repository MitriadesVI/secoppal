# Decisions Log — SECOPPAL

Registro de Decisiones Arquitectónicas (ADR). Append-only. Una entrada por decisión de fondo que cambia el comportamiento del sistema.

---

## ADR-001: FollowupEngine como cerebro único del seguimiento conversacional

**Fecha:** 2026-05-16  
**Contexto:** La lógica de follow-up estaba repartida entre `conversation_store.py` (merge_params), `query_frame.py` (classify_turn + merge_frames) y `orchestrator.py` (apply_context). Cada módulo hacía una clasificación distinta con categorías incompatibles. Esto causaba bugs como "muéstrame más contratos de adulto mayor" tratado como paginación pura con offset heredado.

**Decisión:** Crear `app/core/followup_engine.py` con clasificador explícito de 11 intenciones (`pagination_more`, `refine_filter`, `change_year`, `change_order`, `change_scope`, `change_dataset`, `contextual_requery`, `suggestion_selection`, `explain_result`, `new_search`, `unclear`) y políticas de merge distintas por tipo. `apply_context` en orchestrator.py usa `detect_and_merge()` como punto único.

**Alternativas descartadas:** Mantener merge_params con if-else dispersos (escala mal). Seguir con query_frame.classify_turn (categorías muy gruesas, no distingue cambio_de_año vs cambio_de_orden).

**Consecuencias:** QueryFrame queda como representación intermedia oficial entre params y FollowupEngine (vive en `apply_context` vía `frame_from_params` → `detect_and_merge` → `params_from_frame`). ConversationStore queda como memoria transaccional (no clasificador). Se expone `followup_intent_type` en el resultado de `run_query()`.

---

## ADR-002: Pagination header contextual (ordering-aware)

**Fecha:** 2026-05-16  
**Contexto:** El header de paginación siempre decía "más recientes" aunque la consulta estuviera ordenada por valor (`ORDER BY valor_del_contrato DESC`). Confundía al usuario.

**Decisión:** Separar dos funciones: `Formatter._ordering_label(params)` en formatter.py y `_build_ordering_label()` en response_policy.py. Ambas retornan "de mayor valor" si `ordering_signal=valor_desc`, "más recientes" en caso contrario. La paginación directa (orchestrator.py) también usa el label con "manteniendo orden por...".

**Alternativas descartadas:** Pasar el ordering_signal al formatter como string hardcodeado (duplica lógica). Usar LLM para generar el label (overkill, no lo necesita).

---

## ADR-003: Estado_families semánticas separadas

**Fecha:** 2026-05-16  
**Contexto:** La familia `con_contrato` mezclaba Cerrado, En ejecución, Modificado, terminado, cedido y Prorrogado en una sola bolsa. "Firmados" mapeaba a esta familia, lo que producía `estado_contrato = Cerrado` cuando el usuario pedía contratos firmados. En SECOP, Cerrado NO significa firmado — es cierre del expediente contractual.

**Decisión:** Separar `con_contrato` en cuatro familias semánticamente distintas:
- `contrato_activo`: En ejecución, Modificado, Prorrogado
- `contrato_cerrado`: Cerrado
- `contrato_terminado`: terminado
- `contrato_cedido`: cedido

"Firmados" ya no aplica ningún estado_contrato. Solo fuerza dataset="contratos" vía intent_vocabulary.py. "En ejecución" mapea a `contrato_activo`. Ver `docs/test_e2e_estado_followup_bug.py` para validación E2E.

**Alternativas descartadas:** Seguir con con_contrato como bolsa (el advisor corrigió la semántica). Mapear "firmados" a Cerrado (incorrecto jurídicamente, Cerrado no es firmado).

---

## ADR-004: Prioridad lista estado sobre scalar legacy en SoQLBuilder

**Fecha:** 2026-05-16  
**Contexto:** coexistían `estado`, `estado_field`, `estado_contrato`, `estado_del_procedimiento` y familias de estado.  
**Decisión:** cuando exista una lista específica de estados o una familia traducida a lista, esa lista tiene prioridad sobre el scalar legacy.  
**Alternativas descartadas:** mantener scalar legacy como fallback sin prioridad (generaba contradicciones como `estado_de_apertura='Abierto'` + `estado_del_procedimiento IN (...)`).  
**Consecuencias:** evita doble filtrado y reduce contradicciones en queries de oportunidad/contrato. La familia `oferta_abierta` ahora usa exclusivamente `estado_del_procedimiento`.

## 2026-05-17 — AQ-001A aggregate_sum integrado

Se agregó la primera ruta analítica determinística de SECOPPAL:
- detecta consultas tipo “cuánto se contrató”, “valor total contratado”, “cuánto suma”
- construye SoQL agregado con SUM + COUNT
- usa guard anti-global
- responde con texto analítico en vez de lista de contratos
- propaga analytical_intent, analytical_response y route_reason por el grafo
- evita que degrade_query, format_response y build_advisor_response sobrescriban la respuesta analítica

Alcance:
- solo aggregate_sum
- sin top_entities
- sin top_contractors
- sin distribuciones
- sin comparativas
- sin LLM classifier
- sin H10

Validación:
- make lint-core limpio
- tests/test_analytical_queries.py: 10 passed
- suite completa: 519 passed
- torture matrix verde
- feedback.jsonl intacto
## 2026-05-17 — OPP: estado de oportunidad y presentación de resultados

Se corrigió la política de filtrado para oportunidades SECOP. `oferta_abierta` deja de combinar filtros sobre `estado_de_apertura_del_proceso` y `estado_del_procedimiento`, porque el doble AND excluía procesos relevantes. La fuente operativa para la familia queda en `estado_del_procedimiento IN ('Publicado','Borrador','Abierto')`.

También se mejoró la presentación de oportunidades:
- deduplicación por identificador de proceso/contrato;
- títulos inteligentes cuando SECOP trae proveedor como nombre;
- URLs escapadas;
- corrección de `$` en Streamlit;
- sugerencias coherentes para oportunidad;
- soporte de frases como `mantenimiento correctivo y preventivo` y `parque automotor`.

Pendiente: OPP-003 para `intent_type=opportunity_search` explícito.

Validación: test_pamplonita 4/4, test_bugs_b1_b7 19/19, torture matrix verde, feedback.jsonl intacto.

## 2026-05-17 — OPP-003: intent_type explícito para opportunity_search + G1-G5

Se implementó `intent_type=opportunity_search` como ciudadano de primera clase:

- Detectado en parser por keywords + bidder intent ("para presentarme").
- Preservado a través de follow-ups vía round-trip en QueryFrame.
- Propagado al SoQL (orden por fecha) y suggester.
- UX de timeout específica (mensaje personalizado + sugerencias de acotación).

Cambios:
- G1: query_router + intent_vocabulary
- G2: QueryFrame round-trip
- G3: tests E2E con chat_id
- G5: timeout_suggestions + response_policy personalizado

Validación: 567 passed, torture matrix verde, feedback.jsonl intacto.

---

## 2026-05-17 — Bidder intent: verbos de intención comercial no son objeto contractual

Se amplió la política de bidder intent para interpretar frases como:
- “para poder presentarme”
- “quiero vender X”
- “vendo X”
- “ofrezco X”
- “soy proveedor de X”

**Decisión:**
Los verbos auxiliares/comerciales describen intención del usuario, no objeto contractual. El objeto debe ser el bien/servicio ofrecido.

**Ejemplos:**
- “quiero vender carpas” → objeto=carpas
- “para poder presentarme” → opportunity_search/procesos
- “ofrezco insumos médicos” → objeto=insumos/medicos

También se corrigió parsing de rangos monetarios compactos tipo "1000-3000 millones".

---

## ADR-005: Detección de valores atípicos en fuente (sin alteración silenciosa)

**Fecha:** 2026-05-17
**Contexto:** El dataset Contratos (`jbjy-vk9h`) de Socrata contiene errores de digitación donde `valor_del_contrato` aparece inflado x1000 frente a `valor_facturado`. Caso guía: Luruaco/CD-1028-2025 — `valor_del_contrato` = 250.000.000.000 vs `valor_facturado` = 250.000.000. Esto afecta ranking por mayor valor, lectura rápida y confianza del usuario.

**Decisión:** Detectar y marcar visiblemente — nunca corregir silenciosamente el dato fuente. Implementación:
1. `detect_value_anomaly()` en `app/utils/value_sanity.py`: compara ratio `valor_del_contrato / valor_facturado`; si ≥ 100, marca `value_quality="suspect"` con referencia de `valor_facturado`.
2. Formatter muestra warning por tarjeta: "⚠️ valor atípico en datos abiertos (valor facturado: $X)".
3. Si el primer resultado (mayor valor) es sospechoso, header global: "⚠️ El mayor valor mostrado tiene inconsistencia de fuente; verificar SECOP."
4. Solo opera sobre dataset Contratos. No toca Procesos.

**Alternativas descartadas:**
- Dividir valor por 1000 automáticamente: rompería contratos correctos. El ratio x1000 no es uniforme (hay casos de 489,867x en el DANE).
- Reemplazar `valor_del_contrato` por `valor_facturado`: este último puede ser 0, parcial, o tener semántica distinta.
- Umbral absoluto (ej: > 1 billón COP): contratos nacionales legítimos pueden superarlo.

**Consecuencias:** El dato fuente permanece intacto. El usuario ve el valor reportado y la advertencia simultáneamente. Futuros rankings y top N heredan la marca sin necesidad de lógica adicional. No afecta el pipeline de SoQL/ordenamiento.

---

## 2026-05-18 — SAFE-SOQL-001: defensa interna anti consultas globales

SoQLBuilder ya no emite `WHERE 1=1` salvo `allow_global=True` explícito.
El guard del orchestrator (`SCOPE_TOPIC_KEYS` en `execute_query`) sigue
siendo la primera defensa y el responsable del mensaje al usuario; el
builder queda como segunda defensa para que un bug futuro o una salida
LLM mal formada no puedan disparar una consulta global a SECOP.

**Decisión:**
Un filtro "substancial" es uno de: scope (departamento, ciudad, entidad,
contratista), topic (objeto), familia de estado o modalidad. fecha, valor
y orden por sí solos NO cuentan — no acotan nada sin scope o topic.

**Opt-in:**
Callers que requieran un global query legítimo (p.ej. `observer.py`
agregando sobre un universo ya validado) deben pasar `allow_global=True`
y quedar cubiertos por tests dedicados.

**Compromiso UX:**
Si el builder rechaza, `build_query` deja `soql_query=""` y el guard del
orchestrator entrega `needs_clarification` con un solo mensaje pidiendo
filtros.

Validación: 534 passed, corpus crítico 5/5, feedback.jsonl intacto.

---

## 2026-05-18 — Reconciliación de main y cierre CORPUS-002-HIGH

**Fecha:** 2026-05-18
**Contexto:** main contenía solo parte de los fixes del sprint; el worktree
`claude/zealous-yalow-296fb6` tenía SAFE-SOQL, corpus runner, critical fixes
y torture fixtures sin mergear. Además el working tree local tenía cambios no
commiteados (opportunity_policy.py, PTAR variants, morphological expansions).

**Decisión:** Reconciliar vía rama de rescate y cherry-picks quirúrgicos, no
merge completo del worktree. Los fixes se integraron en 14 commits validados
uno a uno contra lint, pytest, corpus y torture matrix.

**Resultado:**
- Corpus: 88/88 PASS, 0 FAIL (desde 82/88 inicial).
- Suite: 579 passed, 3 xfailed.
- Torture matrix: 12/12 invariantes.
- feedback.jsonl intacto.
- git status limpio.

**Sprint pendiente:** DEMO-BLOCKERS-001 — 8 tareas UX/estado/scope registradas como
fact en memoria estructurada. Prioridad HIGH. No ejecutar ahora.

---

## 2026-05-18 — Plan de robustecimiento sistémico (en pausa)

**Fecha:** 2026-05-18
**Contexto:** Plan aprobado en `docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md`.
Baseline M0 capturado en `baseline/` (579 passed, corpus 88/88, lint verde).
Commit base: `b16ed0b`.

**Estado:** En pausa. Bloqueado por Sprint 0A — esperando 20-30 queries humanas
reales del usuario para construir `dialogue_policy_corpus.yaml` (50-60 casos:
20-30 humanas + 30 sintéticas en 7 categorías).

**Al retomar:**
1. Leer `docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md` §7 (sprints) y §9 (medición).
2. Leer `baseline/README.md`.
3. Clasificar queries humanas → YAML del corpus → extender runner.
4. Sprint 1A (`query_plan.py`, `dialogue_policy.py`, nodo Burr) no arranca sin corpus 0A.

**Línea roja:** Cero regresiones pytest/corpus/lint/feedback.

---

## ADR-006: QueryPlan + DialoguePolicy como capa de decisión pre-ejecución

**Fecha:** 2026-05-21
**Contexto:** 80 queries humanas con ~20% éxito. Parchar `query_router.py` con condicionales aislados no escala. Sprint 1A demostró que los CRITICAL son resolubles puntualmente, pero los HIGH son patrones que requieren una capa que centralice la decisión.

**Decisión:** introducir `dialogue_policy.py` (clasificador puro de señales) + `query_plan.py` (compute determinístico con 9 reglas P1..P9). Insertarlos como nodo Burr `plan_query` entre `apply_context` y `resolve_entities`. Los 5 bugs HIGH canónicos se resuelven en la capa nueva, no en parches.

**Reglas implementadas:**
- P1: command_incomplete → clarificación sin ejecutar SECOP
- P2: non_query_followup → diagnóstico en vez de búsqueda
- P3: city_mention_as_topic → mover token de objeto a ciudad
- P4: implicit_opportunity → promover intent_type=opportunity_search
- P5: temporal_month → resolver "en mayo", "este mes", etc. a rango de fechas
- P6: number_phrase_scrub → eliminar verbos comparativos del objeto
- P7: discourse_filler → eliminar tokens de cortesía del objeto
- P8: invalid_value_range → bloquear rangos inválidos (min > max)
- P9: bare_city_no_entity_promotion → purgar autopromoción de entidad

**Alternativas descartadas:**
- 5 fixes puntuales en query_router (parche infinito).
- Capa con relevance/embeddings/LLM (sobrearquitectura para datos disponibles).
- Migrar stopwords inmediatamente (deferido a Sprint 3 o cuando dialogue_policy esté estabilizado).

**Consecuencias:** una sola capa nombrada para decisiones de "qué hacer con esta query". Cada bug nuevo cae en una regla nombrada (P1..P9) o se convierte en bug arquitectónico explícito. LoC topes (300/400) defienden contra god-object.

**Métricas al cierre:**
- pytest: 718 passed, 3 xfailed
- corpus v1: 88/88 PASS
- corpus dialogue: 60/60 PASS (100% todas las categorías)
- lint-core: verde
- query_plan.py: 247 LoC (<300)
- dialogue_policy.py: 381 LoC (<400)

**Bugs cerrados:** CITY-MENTION-AS-OBJECT-001, CITY-TO-ENTITY-AUTOPROMOTION-001, TEMPORAL-MONTH-PARSING-001, NUMBER-CONTEXT-PHRASE-001, COURTESY-FILLER-001, NO-FOLLOWUP-DETECTION-001.

**Bugs en PARTIAL:** ~~OPP-INTENT-001~~ → **CLOSED Sprint 2A-fix (B3).** P4 setea `intent_type` + B3 proyecta `estado_de_apertura_del_proceso = 'Abierto'` al WHERE. Cierre completo con dependencia resuelta.

---

## ADR-007: SecopClient inmutable por timeout vía with_timeout() para evitar race condition en ThreadPoolExecutor

**Fecha:** 2026-05-23
**Contexto:** `SecopClient.aggregate()` mutaba `self.client.timeout` globalmente mediante un patrón try/finally que seteaba y restauraba `client.timeout`. Cuando `observer.py` ejecutaba 5 agregaciones concurrentes vía `ThreadPoolExecutor`, cada worker ponía `timeout=4` en el cliente subyacente. Como `requests` libera el GIL durante I/O, el hilo principal que ejecutaba `count()` o `query()` leía `self.timeout = 4` en `sodapy._perform_request`, causando que las queries principales fallaran con timeout de 4s a pesar de `Settings.secop_timeout_seconds = 60`.

**Decisión:**
1. Eliminar la mutación de `client.timeout` en `aggregate()`.
2. Introducir `SecopClient.with_timeout(timeout)` que devuelve una instancia nueva de `SecopClient` con su propio `Socrata(..., timeout=timeout)`.
3. `observer.py` y `analytics.py` usan `with_timeout()` para agregaciones con timeout distinto al principal.
4. No compartir estado mutable entre hilos. Cada `Socrata` tiene su propio `requests.Session` y su propio `timeout`.

**Alternativas descartadas:**
- Thread-local storage: complejidad innecesaria para un wrapper thin.
- Lock global en `aggregate()`: serializaría las agregaciones concurrentes del observer.
- Pasar timeout como parámetro a `sodapy.Socrata.get()`: la librería no lo soporta; el timeout es propiedad de la sesión.

**Consecuencias:**
- Observer puede seguir corriendo agregaciones con timeout corto (4s) sin afectar las queries principales.
- Analytics puede usar timeout=10s para sumas agregadas sin afectar el cliente principal.
- Memoria: `with_timeout()` crea un nuevo `requests.Session` por llamada. Costo aceptable para 5-6 agregaciones por query.

**Métricas al cierre:**
- pytest: 716 passed, 3 xfailed
- corpus v1: 88/88 PASS
- corpus dialogue: 61/61 PASS (60 + GOB-DEPTO-001)
- lint-core: verde
- Smoke: API transient slow (9/25 with 5s timeout), unit tests confirm fix correctness

**Bugs cerrados:** OPP-TIMEOUT-001 (B5), GOB-DEPTO-RESOLUTION-001 (B6), RACE-TIMEOUT-MUTATION-001 (causa raíz real).

---

## ADR-008: Query-first, count-lazy para reducir latencia percibida

**Fecha:** 2026-05-23
**Contexto:** `count(*)` sobre datasets de millones de filas con WHERE compuesto tarda 10-15s. `_count_and_query_parallel` ejecutaba count y query en paralelo con `ThreadPoolExecutor(max_workers=2)`, pero el tiempo total era `max(count_time, query_time)`. Cuando count tardaba 15s y query 8s, el usuario esperaba 15s. Smoke acotado mostraba 2/10 fallos por count timeout y tiempo medio 13.9s/query.

**Decisión:**
1. Reemplazar `_count_and_query_parallel` con `_query_first_then_count` que ejecuta `query()` primero.
2. Si `len(results) < limit`: `total_count = len(results)` exacto, sin ejecutar count.
3. Si `len(results) == limit`: ejecutar `count()` con el cliente normal (timeout=15s). Si tiene éxito, usar ese valor. Si falla, `total_count = len(results)` con flag `total_count_approximate=True`.
4. El formatter y response_policy detectan `total_count_approximate=True` y renderizan "Encontré X+ resultados" en lugar de "Encontré X resultados".
5. No usar timeout corto para count — el cliente normal ya tiene timeout=15s. Si falla, degradamos graceful a "X+".

**Alternativas descartadas:**
- Cachear count por (dataset, filtros) con TTL 1h: buena idea pero queda para Sprint 4.
- Timeout corto de 8s para count: rompía mocks en tests y no mejora el caso happy path.
- Ejecutar count en background y editar mensaje de Telegram después: requiere reestructurar el grafo Burr.

**Consecuencias:**
- Queries que devuelven < limit resultados son ~50% más rápidas (no pagan count).
- Queries que devuelven == limit y count falla responden con query_time en lugar de count_time.
- El usuario ve "50+ resultados" cuando el total exacto no está disponible — UX honesta.

**Métricas al cierre:**
- pytest: 721 passed, 3 xfailed
- corpus v1: 88/88 PASS
- corpus dialogue: 61/61 PASS
- lint-core: verde
- Smoke acotado: 9/10 OK, tiempo medio 10.8s/query (vs 8/10, 13.9s anterior)

**Bugs cerrados:** COUNT-QUERY-LATENCY-001.

---

## ADR-009: Observer 2.0 — signals enriquecidos por categoría + suggester contextual por intent_type

**Fecha:** 2026-05-23
**Contexto:** El observer original reportaba 4 signals binarios (has_dominant_entity, has_temporal_concentration, has_value_outlier, has_diverse_modalities). Eran útiles pero poco granulares: no decían CUÁNTO domina una entidad, CUÁNTO se concentra en un trimestre, ni qué modalidad domina con qué porcentaje. Además, el suggester generaba las mismas 3 sugerencias sin importar si el usuario buscaba oportunidades, contratos o información de una entidad.

**Decisión:**
1. **Observer 2.0**: Agregar 4 signals cuantitativos nuevos a `UniverseInsights`:
   - `entity_concentration_pct` / `entity_concentration_name`: % del universo de la entidad top (sin threshold, siempre computado).
   - `modality_dominance_name` / `modality_dominance_pct`: nombre y % de la modalidad top si supera 50%.
   - `temporal_skew_quarter` / `temporal_skew_pct`: trimestre (Q1-Q4) con >70% de concentración, computado desde fechas individuales del sample o fallback a años para aggregation.
   - `value_outlier_ratio`: ratio max/mean cuando supera 10x.

2. **Exclusión de dimensiones fijadas**: `formatter._insights_summary` skipea signals que el usuario ya filtró explícitamente (entidad, modalidad, fechas). Evita tautologías como "INVIAS concentra 100%" cuando la query ya pedía "contratos del INVIAS".

3. **Suggester contextual por intent_type**: `generate_suggestions()` acepta `intent_type` y genera sugerencias diferenciadas:
   - `opportunity_search` → "Restringir a >100M", filtrar por modalidad dominante, "Ver contratos firmados"
   - `contract_search` → "Ver solo adjudicados", "Ordenar por mayor valor", "Buscar en procesos"
   - `entity_query` → "Ver procesos abiertos", "Ordenar por mayor valor", "Ver contratos"
   - Fallback genérico cuando hay espacio (estado, orden, dataset switch).

4. **Onboarding**: `/start` en Telegram y mensaje de bienvenida en Streamlit cuando no hay historial.

**Alternativas descartadas:**
- Usar LLM para generar insights a partir de agregaciones (overkill, los datos ya están disponibles).
- Agregar signals trimestrales como nueva query de agregación en soql_builder (complejidad innecesaria; quarter se computa desde fechas del sample).
- Hacer suggester stateful con memoria de preferencias del usuario (queda para Sprint 4 personalización).

**Consecuencias:**
- Los insights son más accionables: "INVIAS concentra el 65% del universo" en vez de "Hay una entidad dominante".
- Las sugerencias orientan al usuario según su perfil: un proponente ve filtros de cuantía, un veedor ve comparativas.
- `formatter.py` ahora recibe `params` en `_insights_summary` para la exclusión de dimensiones fijadas.

**Métricas al cierre:**
- pytest: 736 passed, 3 xfailed
- corpus v1: 88/88 PASS
- corpus dialogue: 60/60 PASS
- lint-core: verde
- Observer tests: test_observer_signals.py (9 tests)
- Suggester tests: test_suggester_contextual.py (6 tests)

**Bugs cerrados:** Ninguno nuevo — mejora UX incremental (Sprint 3 fase 2).

---

## ADR-010: ACCENT-NORMALIZATION — unaccent() en SoQL, NO regexp_replace

**Fecha:** 2026-05-23
**Contexto:** El bug ACCENT-NORMALIZATION-001 (9+ apariciones) requería que LIKE en SoQL fuera insensible a tildes para matchear datos de SECOP que contienen tildes (CONSULTORÍA, DISEÑO, JARDINERÍA, etc.). La implementación original usaba `unaccent(UPPER(field))` en ambos lados del LIKE.

Durante Sprint 4 se intentó migrar a `regexp_replace()` por instrucción del plan, bajo la hipótesis de que `unaccent()` era PostgreSQL-only y no estaría disponible en Socrata SoQL. 

**Hecho empírico:** Socrata rechazó `regexp_replace()` con 3 argumentos:
```
query.soql.no-such-function; No such function 'regexp_replace'; arity=3
```
El smoke test cayó de 9/10 a 6/10. Se revirtió el cambio con `git commit --amend`.

**Decisión:**
1. Mantener `unaccent()` como función de normalización de tildes en SoQL. Socrata lo soporta (backend PostgreSQL).
2. Agregar `strip_accents()` en `app/utils/text.py` para normalización Python-side (útil para valores que se inyectan como literales en WHERE).
3. NO intentar `regexp_replace` en SoQL — no está disponible en la API de Socrata.

**Alternativas descartadas:**
- `regexp_replace` con 2 argumentos (sin replacement): no existe en SoQL.
- Normalizar solo del lado Python (strip_accents antes de LIKE): no sirve porque los DATOS en SECOP tienen tildes.
- Usar COLLATE o funciones de normalización Unicode: no disponibles en SoQL.

**Consecuencias:**
- `unaccent()` es la función correcta y funciona en producción (smoke 9/10 verificado).
- `strip_accents()` Python es útil pero no reemplaza `unaccent()` en SoQL.
- Cualquier intento futuro de cambiar la normalización de tildes debe validarse contra la API real de Socrata ANTES de commit (no solo tests unitarios).

**Métricas al cierre:**
- Smoke: 9/10 (con unaccent), 6/10 (con regexp_replace — regresión confirmada)
- Tests: test_accent_normalization.py 19/19 pass
- Commit: `61f5dfb` (amend con revert)

**Bugs cerrados:** ACCENT-NORMALIZATION-001 (Sprint 4).

---

## ADR-011: COMMON_TOKENS expansion para AND→OR demotion

**Fecha:** 2026-05-23
**Contexto:** El mecanismo de degradación AND→OR (`_should_demote_objeto` + `_split_objeto_by_rarity`) existía desde Sprint 1A pero con solo 17 tokens en `COMMON_TOKENS`. Casos como "ASEO, CAFETERÍA, JARDINERÍA Y MANTENIMIENTO" (6 tokens) no se beneficiaban porque pocos tokens caían en la categoría "common".

**Decisión:**
1. Expandir `COMMON_TOKENS` de 17 a ~50 tokens en `app/core/intent_vocabulary.py`.
2. Criterio de inclusión: tokens que aparecen en miles de procesos SECOP y no discriminan (general, integral, obra, pública, profesional, etc.).
3. El mecanismo de degradación no cambia — solo se amplía el vocabulario.

**Tokens agregados (Sprint 4):**
integral, integrales, soporte, soportes, apoyos, obra, obras, publica, publico, pública, público, profesional, profesionales, asistencia, asistencias, tecnica, técnica, tecnicos, técnicos, proyecto, proyectos, actividad, actividades, nacional, nacionales, regional, regionales, suministrar, compra, compras, adecuacion, adecuación, mejoramiento, reparacion, reparación, construccion, construcción, rehabilitacion, rehabilitación.

**Alternativas descartadas:**
- Clasificar tokens por frecuencia real en SECOP (requiere analítica sobre el dataset — postergado).
- Usar LLM para decidir qué tokens degradar (overkill, no determinístico).

**Consecuencias:**
- Queries con ≥4 tokens ahora degradan correctamente los tokens genéricos a OR.
- El caso Putumayo (6 ANDs) ahora genera OR sobre mantenimiento, servicios, suministro, etc.

**Métricas al cierre:**
- Tests: test_multi_and_demotion.py 5/5 pass (incluye caso Putumayo 6-AND)
- Commit: `20989f5`

**Bugs cerrados:** MULTI-AND-OVER-RESTRICTIVE-001 (Sprint 4, sobre infraestructura Sprint 1A).

---

## ADR-012: Value polarity completeness — patrones faltantes

**Fecha:** 2026-05-23
**Contexto:** El parser de polaridad monetaria en `query_router.py` cubría ~15 frases (no mayor a, no menor a, por debajo de, por encima de, al menos, etc.) pero faltaban 3 patrones: "menor o igual a", "mayor o igual a", "a lo sumo".

**Decisión:**
1. Agregar `(?:o\s+igual\s+)?` opcional en los patrones de "menor(es)" y "mayor(es)" para cubrir "menor o igual a X" y "mayor o igual a X".
2. Agregar patrón "a lo sumo" → `valor_max` en el grupo de limitantes superiores.
3. No modificar la estructura de `polarity_patterns` — solo extender regex.

**Alternativas descartadas:**
- Reescribir el parser de polaridad como tabla de mapeo explícita (más mantenible pero rompe compatibilidad con el pipeline actual de regex+scrub).
- Usar LLM para desambiguar polaridad (no determinístico, latencia innecesaria).

**Consecuencias:**
- 12 frases de polaridad ahora cubiertas completamente (eran ~9 antes).
- El caso Palermo (MIN-MAX-INVERSION) ya estaba cubierto por patrones existentes; los nuevos cierran edge cases.

**Métricas al cierre:**
- Tests: test_value_polarity.py 16/16 pass (5 nuevos Sprint 4)
- Commit: `77dbb9c`

**Bugs cerrados:** MIN-MAX-INVERSION-001 (Sprint 4).