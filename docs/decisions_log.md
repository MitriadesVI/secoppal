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

---

## ADR-013: unaccent() obligatorio en todas las cláusulas LIKE de entidad

**Fecha:** 2026-05-24
**Contexto:** El smoke Fase 5 bucket D (ENTITY-MULTI-REGIONAL) reveló que SENA Bolivar y SENA Atlantico fallaban con 0 resultados. Las cláusulas LIKE de `nombre_entidad` usaban `UPPER() LIKE UPPER()` sin `unaccent()`, mientras que las cláusulas de objeto ya usaban `_accent_fold_like()` (agregado en Sprint 4 para ACCENT). Además, `entidad_resolved` contenía nombres sintéticos con tildes ("SENA REGIONAL BOLÍVAR") que no existen en SECOP (los nombres reales son "SENA REGIONAL BOLIVAR ..." sin tilde).

**Decisión:**
1. Aplicar `_accent_fold_like()` en TODAS las cláusulas de entidad: entidad única, ecosistema expansion, y ciudad LIKE.
2. Invertir precedencia en SoQL builder: `entidad_like` (nombre canónico corto) tiene prioridad sobre `entidad_resolved` (nombre display/regional).
3. `resolve_regional()` ahora retorna `tuple[str|None, str|None]` — (nombre_regional, nombre_canónico) — para que el caller pueda setear `entidad_like` al nombre canónico ("SENA", "ICBF") que sí existe en SECOP.
4. El query_router setea `params["entidad_like"] = canonical_short` al resolver multi-regional.

**Alternativas descartadas:**
- Usar `regexp_replace` con 3 argumentos (Socrata no lo soporta).
- Mantener `UPPER()` sin unaccent y normalizar los nombres en el gazetteer (no escala: nuevos nombres siempre pueden tener tildes).
- Usar el nombre sintético regional completo como LIKE (no matchea — los nombres reales tienen sufijos como "Grupo de Apoyo Administrativo Mixto").

**Consecuencias:**
- Todas las cláusulas `nombre_entidad`, `departamento_entidad`, `ciudad_entidad`, `proveedor_adjudicado` usan `_accent_fold_like` (unaccent).
- Entidades multi-regionales generan `LIKE unaccent('%SENA%')` en vez de `LIKE '%SENA REGIONAL BOLÍVAR%'`.
- `resolve_regional()` cambió firma — tests actualizados (9 tests en 2 archivos).
- Smoke D-bucket: 3/5 → 5/5 confirmado con queries individuales contra API real.

**Métricas al cierre:**
- Tests: test_entity_multi_regional.py 6/6, test_entity_multi_regional_edges.py 3/3
- Suite: 830 passed, 0 failed, 10 xfailed
- Commit: `b39a138`

---

## ADR-014: OR merge para 3+ rare terms en objeto (refinamiento MULTI-AND-DEMOTION)

**Fecha:** 2026-05-24
**Contexto:** El smoke Fase 5 bucket B (MULTI-AND+RELEVANCE) dio 0/10. Las queries con frases técnicas ("control de calidad de agua para consumo humano", 5 términos) generaban 5 cláusulas AND en SoQL — match matemáticamente imposible en 8.6M procesos. Sprint 4 había expandido COMMON_TOKENS (~50) pero no cubría el caso donde TODOS los términos son raros (fuera de COMMON_TOKENS). El mecanismo `_should_demote_objeto` separaba rare/common pero seguía uniendo los rare con AND.

**Decisión:**
Cuando `_should_demote_objeto` dispara Y `len(rare_terms) >= 3`: fusionar todos los rare_terms en un solo grupo OR (en vez de grupos AND individuales). El BM25 relevance scorer reordena después para recuperar precisión.

```python
# Antes: (control OR) AND (calidad OR) AND (agua OR) AND (consumo OR) AND (humano OR)
# Ahora: (control OR calidad OR agua OR consumo OR humano)  ← un solo grupo OR
```

**Alternativas descartadas:**
- Bajar threshold de demotion a 3 términos (genera falsos positivos en queries de 3 términos específicos).
- Usar OR solo si hay common_terms (no cubre el caso "todos raros").
- Dejar AND y confiar en BM25 (BM25 no puede rankear resultados que nunca llegaron de Socrata).

**Consecuencias:**
- Queries de 4+ términos técnicos ahora retornan resultados (validado: "control de calidad de agua..." 0→50 results).
- El trade-off precisión/recall se compensa con BM25 post-query.
- El comportamiento para 1-2 rare terms no cambia (siguen con AND).
- Tests: test_multi_and_demotion.py actualizado (5/5).

**Métricas al cierre:**
- Tests: 830 passed, 0 failed, 10 xfailed
- Commit: `dd0b2bf`

**Bugs cerrados:** MIN-MAX-INVERSION-001 (Sprint 4).

---

## ADR-016: Secondary sort en opportunity_search + skip de estado legacy duplicado

**Fecha:** 2026-05-25
**Contexto:** Dogfooding de query "procesos para ofertar en cundinamarca entre 100 y 500 millones en reparaciones o mantenimientos" reveló dos bugs en SoQL builder:

1. **Opportunity_search sin secondary sort:** La rama `opportunity_search` solo emitía `ORDER BY fecha DESC`, perdiendo `precio_base DESC`. Un universo de 50+ procesos ordenados solo por fecha no ayuda al proponente a identificar las oportunidades valiosas.

2. **Estado duplicado en WHERE:** `query_router.py` emitía `params["estado"] = "Abierto"` (legacy) Y `params["estado_de_apertura_del_proceso"] = ["Abierto", "Publicado"]` (resuelto desde `estado_family=oferta_abierta`). SoQL builder aplicaba ambos, generando WHERE con filtro duplicado.

**Decisión:**
1. Agregar `precio_base DESC` como secondary sort en la rama `opportunity_search`. El orden final es `fecha DESC, precio_base DESC`.
2. Saltar el legacy `params["estado"]` en SoQL builder si cualquier campo específico (`estado_de_apertura_del_proceso`, `estado_del_procedimiento`, `estado_contrato`, `estado_contrato_adicional`) ya está presente en params. Los campos específicos (provenientes de `estado_family`) tienen semántica más precisa que el legacy.

**Alternativas descartadas:**
- Limpiar `query_router.py` para no emitir ambos (más invasivo, mismo efecto).
- Quitar secondary sort y confiar en BM25 post-query (no aplica — BM25 reordena, no agrega dimensión de valor).

**Consecuencias:**
- Test `test_opportunity_ordering_fecha_desc` actualizado para reflejar la nueva semántica con comentario ADR.
- El guard "skip si campo específico existe" cubre 4 campos de estado. Si en el futuro se agregan más, deben incluirse en la tupla de verificación.

---

## ADR-017: Dogfooding UX fixes — URLs, fechas, refinamiento, rango, CASE WHEN

**Fecha:** 2026-05-25
**Contexto:** Dogfooding con query real "procesos abiertos hoy en Cundinamarca por mas de 1000 millones" reveló 5 bugs críticos (A-E) y 1 bug estructural (BadRequest) que bloqueaban la experiencia de demo. Los fixes fueron aplicados en sesión de trabajo con verificación de tests (945 passed, 10 xfailed).

**Decisión:**

1. **URLs por tipo de proceso (DOGFOOD-URL-005):**
   - `formatter.py:_safe_url()` y `secop_client.py:_normalize_row()` detectan prefijo del ID.
   - CO1.NTC.X → reconstruir URL pública via `OpportunityDetail/Index?noticeUID=`.
   - CO1.REQ.X → retornar "" (requisición pre-publicación, SECOP devuelve "argument out of range").
   - UI muestra "Vista pública no disponible (proceso pre-publicación)" cuando URL vacía.

2. **Fallback de fecha en cards (DOGFOOD-DATE-002):**
   - `formatter.py:_best_date_for_row()` implementa cascada: `fecha_de_publicacion_del` → `fecha_de_publicacion_fase_3` → `fecha_de_recepcion_de_respuestas_al_in` → `fecha_de_apertura_efectiva`.
   - Cada fecha trae etiqueta que indica fuente ("Publicación", "Recepción ofertas", "Apertura").
   - Si todas null → "Pendiente de publicación" (no "Sin fecha").

3. **Refinement buttons funcionales (DOGFOOD-REFINE-004):**
   - `orchestrator.py:_run_refinement()` — método nuevo que ejecuta query con params refinados vía bypass del pipeline SoQL.
   - `streamlit_app.py` consume `_pending_refinement` del session_state, construye `seed_resolved_params` con `apply_refinement()`, y pasa como `seed_resolved_params` a `run_query()`.
   - Bypass evita re-parsear la query natural — los params refinados se inyectan directamente al builder.

4. **Rango "entre X y Y" herencia de unidad (DOGFOOD-RANGE-005):**
   - `query_router.py:_extract_amounts()` detecta rango con regex `entre|de|desde ... y|a|hasta`.
   - Cuando un lado no trae unidad explícita, hereda del otro lado con regla:
     - Si el lado con unidad tiene dígito → hereda unidad compuesta ("mil millones").
     - Si el lado con unidad NO tiene dígito → hereda unidad atómica ("millones").
   - Ej: "entre 500 y mil millones" → valor_min=500M, valor_max=1000M.
   - Ej: "entre 1 y 2 mil millones" → valor_min=1000M, valor_max=2000M.

5. **ACTION_VERBS += "celebrar" (DOGFOOD-ACTION-005B):**
   - "celebrar", "celebró", "celebraron" añadidos a `ACTION_VERBS` en `intent_vocabulary.py`.
   - Scrub remueve verbo del objeto cuando aparece junto a sustantivos reales.
   - Ej: "qué contratos de prevención se celebraron" → objeto="prevencion" (no "celebraron").

6. **Eliminación de CASE WHEN en ORDER BY (DOGFOOD-SOQL-006):**
   - El `CASE WHEN estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado') THEN 0 ELSE 1 END` en ORDER BY para jbjy-vk9h rompía Socrata: el parser interno transformaba a `#CASE(x IN (...), 0, TRUE, 1)` y revienta con strings acentuados (400 Bad Request "Unexpected character").
   - Fix: eliminar CASE WHEN del default ordering. El boost "activos primero" se movió a post-procesamiento Python (`formatter._reorder_active_first`).
   - Impacto colateral: resolvió timeouts sistemáticos en contratos que se atribuían erróneamente a "API lenta".

**Alternativas descartadas:**
- Para URLs: construir URL con `referencia_del_proceso` como noticeUID (descubierto en diagnóstico histórico nov 2025 que referencia ≠ noticeUID — genera URLs rotos).
- Para fechas: usar solo `fecha_de_publicacion_del` con fallback "Sin fecha" (más del 50% de cards quedaban sin fecha visible).
- Para rango: regex simple sin herencia de unidad (fallaba con "entre 500 y mil millones" → 500 pesos vs 1000M).
- Para CASE WHEN: intentar escapar tildes en strings del IN (Socrata no lo soporta; el error es del parser interno, no del string literal).

**Consecuencias:**
- Los 5 bugs críticos del dogfooding quedan documentados con slugs DOGFOOD-XXX.
- El fix de CASE WHEN resolvió timeouts en contratos de forma colateral.
- Tests actualizados: `test_url_repair.py` (test obsoleto reescrito para nueva política).

**Métricas al cierre:**
- Tests: 945 passed, 10 xfailed
- Commit: múltiples commits en sesión de trabajo (no un commit único)
- Documentación: DOGFOOD_2026-05-25.md, funnel-full-ux-fixes-2026-05-25.md

---

## ADR-018: ICBF Regional Catalog como dataset separado del gazetteer

**Fecha:** 2026-05-26  
**Commit:** `9bbe79b`  
**Contexto:** ICBF tiene 32 regionales ~1:1 por departamento. Intentar meterlas en `aliases_db.json` habría inflado el gazetteer con entradas que no son entidades licitantes independientes (son subunidades del ICBF nacional). El gazetteer ya tenía 509 entidades y crecer con 32 regionales ICBF + 89 entidades SENA rompía el principio de "una entidad, una entrada".

**Decisión:** `icbf_regionals.json` vive fuera del gazetteer, en `app/data/`, con su propio resolver puro (`icbf_resolver.py`). El resolver NO hace llamadas SECOP ni genera SoQL — solo retorna un `EcosystemResolution` con `resolution_type`, `department`, y `official_names`. El bridge (entity_ecosystem_router.py) se encarga de traducir la resolución a filtros SoQL.

Relación 1:1 regional/depto significa que "icbf atlántico" resuelve exactamente a una regional, sin ambigüedad. Esto simplifica el alias matching vs SENA que requiere `contains_all`.

ICBF fue primero porque era estructuralmente más sencillo — sirvió como prueba de concepto para el patrón ecosystem.

**Alternativas descartadas:**
- Meter las 32 regionales en `aliases_db.json` (infla gazetteer, rompe principio de entidad independiente).
- Hacer el resolver con llamadas SECOP directas (latencia innecesaria, datos mutables).
- Catálogo plano sin resolver dedicado (el scrub institucional y la lógica de `contains_all` requerían un punto de extensión).

**Consecuencias:** El patrón ecosystem se validó con ICBF y se replicó a SENA. Todos los ecosistemas viven en `app/data/` como JSON separados, con resolver puro en `app/core/`.

**Métricas al cierre:**
- Tests: tests/test_icbf_regionals.py (47 tests) — todos PASS
- Suite: 205 passed, 13 xfailed (ecosystem + gazetteer)
- Corpus: 89/89 PASS
- Lint: limpio

---

## ADR-019: SENA Entity Ecosystem — audit-first vs catálogo plano

**Fecha:** 2026-05-26  
**Commit:** `a73e741`  
**Contexto:** SENA es estructuralmente más complejo que ICBF. Un audit contra SECOP II live reveló 89 entidades únicas SENA en 7 tipos distintos: `national_secretaria_general` (1), `national_direccion_general` (2), `regional_parent` (28), `regional_admin_group` (54), `training_center` (1), `intercentros` (2), `needs_review` (1). Volumen total: 616,210 procesos/contratos.

Hallazgo clave del audit: los `regional_admin_group` (~395K) tienen **más del doble de volumen** que los `regional_parent` (~179K). La mayoría de registros SENA están bajo grupos administrativos, no bajo la entidad regional principal. Esto invalidaba el enfoque de catálogo 1:1 tipo ICBF.

**Decisión:**
1. **Audit-first:** ejecutar `scripts/audit_sena_entities.py` contra SECOP II live antes de diseñar el catálogo. Esto evitó diseñar sobre supuestos incorrectos.
2. **Taxonomía:** 7 tipos con semántica distinta, documentados en `docs/gazetteer/SENA_ENTITY_AUDIT.md`.
3. **Patrón `contains_all`:** en vez de resolver a una entidad específica (imposible cuando hay 54 admin groups y 28 parents por depto), el resolver devuelve filtros LIKE compuestos: `UPPER(entidad) LIKE '%SENA%' AND UPPER(entidad) LIKE '%MAGDALENA%'`. El consumidor downstream (SoQL builder) aplica el filtro compuesto.
4. **Catálogo en `sena_units.json`:** no en `aliases_db.json`. Sigue el mismo patrón que ICBF (ecosistema fuera del gazetteer).

**Alternativas descartadas:**
- Catálogo plano 1:1 (no funciona — SENA no es ICBF, 54 admin groups no mapean 1:1 por depto).
- Resolver a entidad específica con fuzzy matching (imposible: 54 grupos por ~28 deptos no tienen nombre departamental consistente).
- Ignorar admin groups (pierde ~64% del volumen — los 395K registros).

**Consecuencias:** SENA es el caso más complejo de ecosystem hasta ahora. El patrón `contains_all` se convirtió en el estándar para ecosistemas institucionales multi-entidad.

**Métricas al cierre:**
- Audit: 89 entidades únicas, 616K volumen total
- Tests: tests/test_sena_resolver.py (41 tests)
- Docs: SENA_ENTITY_AUDIT.md, SENA_ENTITY_MODEL.md

---

## ADR-020: Entity Ecosystem Bridge — capa fuera de query_router

**Fecha:** 2026-05-26  
**Commits:** `d78ebf3`, `9bd7dda`, `85b07cf`  
**Contexto:** ICBF y SENA son ecosistemas institucionales que no encajan ni en el gazetteer (aliases_db.json) ni en el EntityResolver (que resuelve entidades únicas). Necesitaban un punto de integración en el pipeline que operara después del parser pero antes del SoQL builder. La alternativa era meter la lógica en `query_router.py`, que ya tenía 800+ líneas y el plan explícitamente prohibía tocar.

**Decisión:** Crear `entity_ecosystem_router.py` como capa independiente que vive conceptualmente "después del parser y antes del resolver". El flujo completo:

```
raw_query → normalize_institutional_acronyms → resolve_entity_ecosystem
→ build_entity_ecosystem_filter → scrub_institutional_tokens_from_objeto
→ QueryPlan → SoQLBuilder
```

**Reglas críticas del bridge:**
1. **No override:** `departamento_resolved` ni `entidad_resolved` existentes no se sobrescriben.
2. **Filtros sustanciales:** `entidad_contains_all` e `entidad_in_candidates` cuentan como filtros válidos (evitan WHERE 1=1).
3. **Normalización institucional:** `INSTITUTIONAL_ACRONYM_FIXES` con `\b` word boundary — reemplaza "icfb" → "icbf", no toca "bicfbtest".
4. **Frontera léxica SENA:** `_is_sena_mention()` usa regex `\bsena\b` — rechaza "senado"/"senador".
5. **Scrub institucional:** Cuando un ecosistema se activa, remueve vocabulario institucional (icbf, sena, bienestar, etc.) de `params["objeto"]` para que no contamine el `LIKE`.
6. **Tipos ICFB (G7C.1):** `9bd7dda` — "icfb magdalena" → "icbf magdalena". Esto es un typo real documentado en SECOP queries. El fix demostró el valor del bridge: un solo punto de normalización para todos los ecosistemas.
7. **Hardening post-ICFB (G7C.2):** `85b07cf` — `\b` en normalize + `\bsena\b` en is_sena + corpus evalúa ecosystem solo cuando entry lo declara.

**Alternativas descartadas:**
- Meter lógica en `query_router.py` (anti-monolito, prohibido por plan).
- Fusionar ecosystems en `entity_resolver.py` (el resolver no sabe de ecosistemas multi-entidad).
- Resolver ecosistemas en SoQL builder (el builder no debe tener lógica de negocio).

**Consecuencias:**
- `query_router.py` quedó intacto — zero cambios.
- Cada ecosistema nuevo solo agrega un resolver puro + datos JSON, sin tocar el core.
- Corpus runner ahora evalúa ecosystem como capa intermedia (`parser_plus_ecosystem`).
- Regresión CORPUS-SM011 (corpus SMOKE) captura el flujo completo como prueba viva.

**Métricas al cierre:**
- Tests: tests/test_entity_ecosystem_router.py + tests/test_entity_ecosystem_query_bridge.py (59 tests)
- Suite ecosystem + gazetteer: 205 passed, 13 xfailed
- Corpus: 89/89 PASS
- Lint: limpio

---

## ADR-021: Format de moneda doble — exacto en cards, compacto en chat

**Fecha:** 2026-05-26  
**Commit:** `30b6c7a` (parte del streamlit/UX commit)  
**Contexto:** SECOPPAL mostraba valores monetarios con el mismo formato en tarjetas (cards) y en la narración del chat. En valores grandes (>1B), el formato exacto ($1,234,567,890) ocupaba demasiado espacio en las cards sin aportar legibilidad. En la narración, al revés: la notación compacta ($1,234M) perdía precisión para comparaciones.

**Decisión:**
1. **`format_cop()`** — formato exacto con separador de miles, usado en cards (detalle). No pierde precisión.
2. **`format_money()`** — formato compacto usado en la narración del chat. Notación colombiana:
   - M (10^6): valores entre 1M y 999M → "$500M"
   - MM (10^9): valores entre 1MM y 999MM → "$1.2MM"
   - B (10^12): valores ≥ 1B → "$2.5B"
   - Por debajo de 1M: formato exacto "$800,000"
3. **`_format_cop_compact()`** — función interna que implementa la compactación con umbrales alineados al ADR.

**Alternativas descartadas:**
- Un solo formato para ambos contextos (cards ilegibles en valores grandes).
- Notación internacional (K/M/B): en Colombia se usa MM para miles de millones.
- Ocultar valores pequeños en compacto: formato exacto es más informativo para montos <1M.

**Consecuencias:** Cards legibles incluso con valores >1B. Chat con narración compacta sin perder precisión en detalle.

---

## ADR-022: Chips contextuales adaptativos

**Fecha:** 2026-05-26  
**Commit:** `30b6c7a` (streamlit/UX commit)  
**Contexto:** Los chips de refinamiento (estado, fecha, monto, entidad, ciudad) existían desde el dogfooding fix (DOGFOOD-REFINE-004) pero tenían bugs críticos:
1. Threshold de aparición en 20 resultados — muy alto para refinamientos sucesivos.
2. Generación de montos con escalas fijas (1M, 5M, 10M, 50M, 100M, 500M, 1B) sin adaptarse al result set real.
3. `uuid.uuid4()` en widget keys de Streamlit — cada render generaba keys nuevas, perdiendo el estado del click.
4. No había sidebar sticky ni "Ver más" para chips que ocupaban mucho espacio.

**Decisión:**
1. **Thresholds adaptativos por percentil:** los montos se calculan de los valores reales del result set usando p33/p66/p90, redondeados a escala {1,2,3,5,7}×10^N con floor. Esto garantiza que los chips reflejen la distribución real de valores.
2. **Threshold de aparición:** bajado de 20 a 5 resultados. Esto permite refinamientos sucesivos (cada chip reduce el universo, pero mientras haya ≥5 resultados, se muestran chips para seguir refinando).
3. **Widget keys estables:** reemplazar `uuid.uuid4()` por identificadores basados en `msg_idx` + categoría. Esto preserva el estado del click entre renders.
4. **Sidebar sticky + "Ver más":** los chips se despliegan en la sidebar con posición sticky. Cuando hay más de 8 chips, se colapsan bajo un botón "Ver más" que reusa el patrón `is_pagination_phrase`.
5. **Flujo del click:** `_pending_refinement` → `apply_refinement()` → `seed_resolved_params` → `_run_refinement()` (bypass del parser). El bypass evita re-parsear la query natural; los params refinados se inyectan directamente al builder.

**Alternativas descartadas:**
- Escalas fijas sin percentiles (no reflejan la realidad del result set; un universo de 50-100M no necesita chips de 1B).
- Mantener uuid como key (los clicks nunca funcionaban — bug histórico).
- Sin sidebar sticky (los chips ocupaban demasiado espacio en el área principal).
- Re-parsear la query natural en el click (pierde contexto del refinamiento previo).

**Consecuencias:** El refinamiento por chips ahora funciona correctamente incluso en cadenas de refinamientos sucesivos. La sidebar sticky mejora la usabilidad en pantallas grandes. Los montos adaptativos evitan sugerir rangos irrelevantes.

|**Métricas al cierre:**
|- Tests: suite completa post-merge
|- Corpus: 89/89 PASS
|- Chips verificados funcionales en UI (no más uuid rotos)

---

## ADR-023: Orchestrator Split — de god module a fachada delgada con workflow/

**Fecha:** 2026-05-27  
**Commits:** `6fa8971` (foundation), `9ea83a8` (actions), `f537315` (refinement), `47c4a87` (handlers)  
**Contexto:** `app/core/orchestrator.py` llegó a 1549 líneas con responsabilidades mezcladas: acciones Burr, construcción del grafo, handlers de casos especiales (reset, paginación, sugerencia, lookup directo, refinamiento), flujo normal Burr, persistencia, y helpers. Cada feature nueva tocaba orchestrator porque concentraba lógica que debería estar distribuida.

**Decisión (en 8 pasos atómicos):**

1. **Paso 0 — Golden safety net:** 9 tests de contrato que capturan el comportamiento observable de `run_query()` con mocks. Sirven como red de seguridad para todo el refactor.
2. **Paso 1 — Helpers puros:** extraer `_relax_params`, `_best_date_for_row`, `_format_value`, `_safe_url`, etc. a `workflow/helpers.py` (177 LOC).
3. **Paso 2 — WorkflowDeps + HandlerContext:** inyectar dependencias vía `WorkflowDeps` (property, no atributo) y pasar contexto inmutable vía `HandlerContext` (frozen dataclass).
4. **Paso 3 — results.py + persistence.py (staged):** preparar `empty_result()`, `tabular_result()`, `persist_trace_and_turn()` sin consumirlos todavía.
5. **Paso 4 — graph.py:** mover `ApplicationBuilder` y la definición del grafo Burr a `workflow/graph.py` (148 LOC).
6. **Paso 5 — 12 acciones Burr a workflow/actions/:** parsing, planning, execution, rendering. Cada acción vive en su propio módulo. `graph.py` importa actions directamente desde `workflow/actions/`, no desde orchestrator.
7. **Paso 6 — Refinement handler:** extraer `_run_refinement` a `workflow/handlers/refinement.py` (136 LOC).
8. **Paso 7 — 5 handlers + colapso de run_query:** reset, pagination, suggestion, direct_lookup, normal_flow. `run_query()` se colapsa a chain-of-responsibility: prueba handlers en orden, el primero que retorna un resultado gana.

**Estructura final:**

```
app/core/
├── orchestrator.py              196 LOC  (fachada — solo run_query + init + shim)
└── workflow/
    ├── graph.py                 148 LOC  (Burr ApplicationBuilder)
    ├── helpers.py               177 LOC  (helpers puros)
    ├── deps.py                   56 LOC  (WorkflowDeps)
    ├── context.py                42 LOC  (HandlerContext)
    ├── persistence.py            87 LOC  (persist_trace_and_turn)
    ├── results.py               211 LOC  (empty_result, tabular_result)
    ├── actions/
    │   ├── parsing.py            92 LOC
    │   ├── planning.py          164 LOC
    │   ├── execution.py         274 LOC
    │   └── rendering.py         126 LOC
    └── handlers/
        ├── reset.py              47 LOC
        ├── pagination.py        109 LOC
        ├── suggestion.py        171 LOC
        ├── direct_lookup.py     236 LOC
        ├── refinement.py        136 LOC
        └── normal_flow.py       236 LOC
```

**Reglas arquitectónicas (4 tests de lock-in):**
1. `graph.py` NO importa de `orchestrator.py`
2. `workflow/actions/*.py` NO importan de `orchestrator.py`
3. `workflow/handlers/*.py` NO importan de `orchestrator.py`
4. Los handlers solo reciben `WorkflowDeps` + `HandlerContext` (no `self` de `SecopalWorkflow`)

**Shim legacy:** orchestrator.py mantiene re-exports de actions y helpers para 3 tests legacy (`test_degrade_query`, `test_guard_where_1_1_equivalence`, `test_count_lazy`). Marcado con `TODO(refactor)` explícito. El runtime no lo usa.

**Alternativas descartadas:**
- Partir en un solo commit (imposible — 1549 líneas, alto riesgo de regresión).
- Mover todo a un paquete `actions/` plano sin `handlers/` (los handlers de casos especiales necesitan su propia capa para no mezclarse con acciones Burr).
- Preservar `_run_refinement` como wrapper en orchestrator (añade ruido sin valor — ningún caller externo lo usaba).

**Consecuencias:**
- orchestrator.py: 1549 → 196 LOC (−87%).
- Cada handler y action vive en su propio archivo con responsabilidad única.
- Golden tests aseguran que el comportamiento observable no cambió.
- Nuevo feature puede agregarse como handler nuevo sin tocar orchestrator.

**Métricas al cierre:**
- Golden: 9/9 PASS
- Architectural lock-in: 4/4 PASS
- Suite: 1212 passed, 23 xfailed
- Corpus: 89/89 PASS
- lint-core: limpio

**Deuda consciente:**
- 3 tests legacy importan desde orchestrator (shim temporal).
- `persist_trace_and_turn` no se adoptó en `normal_flow` ni `suggestion` porque requería extender la firma con `suggestions=` (cambio de comportamiento, no mecánico).
|- `tabular_result` no se adoptó en el camino exitoso de `suggestion` porque el código legacy mezcla `formatted_response`/`formatted_rows` (Burr-internal) con `response`/`rows` (canónico). Requiere golden test propio.

---

## 2026-05-27 — Dogfooding: tres bugs hallados en sesión de pruebas manuales

Tres bugs identificados durante dogfooding post-refactor. Ninguno introducido por el refactor — todos preexistentes, ahora documentados para priorización.

---

### DOGFOOD-FOLLOWUP-007: Follow-up poisoning sin invalidación cruzada

**Observado:** El `followup_engine` hereda params del turno anterior sin validar compatibilidad entre ellos.

**Caso (a) — cambio de departamento:**
- Turno 1: "procesos del ICBF en Cundinamarca" → `departamento='Cundinamarca'`, `entidad_ecosystem=['ICBF', 'CUNDINAMARCA']`
- Turno 2: "y en Cesar" → `departamento='Cesar'` (nuevo), pero `entidad_ecosystem` se hereda con `['ICBF', 'CUNDINAMARCA']`
- Resultado: `WHERE departamento='Cesar' AND nombre_entidad LIKE '%CUNDINAMARCA%'` → 0 resultados lógicamente imposibles

**Caso (b) — cambio de dataset:**
- Turno 1: "procesos abiertos" → `dataset='procesos'`, `intent_type='opportunity_search'`, `estado_family='oferta_abierta'`
- Turno 2: "muéstrame los contratos" → `dataset='contratos'` (nuevo), pero `intent_type` y `estado_family` se heredan
- Resultado: se aplica `estado_family='oferta_abierta'` (válido solo para procesos) sobre dataset contratos → WHERE con estados inexistentes en contratos

**Fix propuesto:** `invalidate_incompatible_inheritance()` en `followup_engine.py` que dropee claves dependientes cuando cambia el scope/dataset/período:
- Si cambia `departamento` → invalidar `entidad_ecosystem`, `entidad_contains_all`, `entidad_in_candidates`, `ciudad`
- Si cambia `dataset` → invalidar `intent_type` (si es opportunity_search), `estado_family` (si es oferta_abierta), `estado_de_apertura_del_proceso`
- Si cambia `objeto` sustancialmente (nuevos tokens, no refinamiento) → invalidar `valor_min`, `valor_max` heredados

**Prioridad:** Alta — afecta follow-ups geográficos y de dataset switch, los más frecuentes.

---

### DOGFOOD-RANGE-007: Parser incompleto en rangos compuestos "X y menos de Y"

**Observado:** `"más de 350 millones y menos de 700"` captura solo `valor_min=350_000_000`. El upper bound (`menos de 700`) se ignora.

**Análisis:** `query_router.py:_extract_amounts()` cubre:
- `"entre X y Y"` → valor_min + valor_max (DOGFOOD-RANGE-005)
- `"más de X"` → valor_min
- `"menos de Y"` → valor_max

Pero no cubre la secuencia `"más de X [conjunción] menos de Y"` donde ambos límites aparecen en la misma query.

**Fix propuesto:** Extender `_extract_amounts()` para detectar patrones secuenciales:
- `"más de N1 [y/e] menos de N2"` → valor_min=N1, valor_max=N2
- `"mayor a N1 [y/e] menor a N2"` → valor_min=N1, valor_max=N2
- `"por encima de N1 [y/e] por debajo de N2"` → valor_min=N1, valor_max=N2
- `"al menos N1 [y/e] a lo sumo N2"` → valor_min=N1, valor_max=N2

La conjunción debe estar dentro de una ventana de ~10 tokens del primer límite para evitar falsos positivos con queries tipo "más de 350 millones en Bogotá y menos en Cali" (que no es un rango).

**Prioridad:** Media-Alta — el usuario asume que el rango funciona cuando dice "y menos de".

**Cerrado 2026-05-27:** Implementado en `ecf94de`. Tres fixes: (1) range span agregado a consumed_spans para evitar leak de "desde"/"hasta"/"entre" a objeto, (2) rangos con guión sin prefijo ("1000-3000 millones") ahora detectados, (3) rangos compuestos de polaridad inversa ("más de X y menos de Y", "mayor a X y menor a Y", "por encima de X y por debajo de Y", "al menos X y a lo sumo Y") detectados con herencia de unidad. 21 tests en test_value_ranges.py. Corpus 89/89 verde.

---

### DOGFOOD-MORPHO-007: Expansión morfológica demasiado ancha (software → tecnología)

**Observado:** `morphological_variants.py` expande `"software"` a la raíz `"tecnología"`. Esto genera:
- Query: "contratos de software en Atlántico"
- Expansión: `objeto LIKE '%tecnología%'`
- Resultados incluyen: "TECNOLOGIA HAMILTON" (equipo médico), "equipos biomédicos", "alcantarillado", etc.
- El usuario buscaba software informático; recibe contratos de tecnología médica e infraestructura

**Análisis:** No es substring contamination (como el caso Policarpa — "policía" matcheaba "Policarpa"). Es expansión semántica sin precisión: "software" es un subtipo de "tecnología", pero no toda "tecnología" es "software".

**Fix propuesto:**
1. **Opcional A:** Remover el mapeo `software → tecnología` de `morphological_variants.py`. Mantener "software" como token raro sin expandir.
2. **Opcional B:** Crear una subcategoría `software_informatico` con aliases específicos ("software", "programa", "aplicación", "sistema informático") que NO expandan a "tecnología" genérica.
3. **Validación:** Antes de aplicar cualquier fix, medir cuántos contratos matchean "software" exacto vs "tecnología" en SECOP II para confirmar que la pérdida de recall es aceptable.

**Nota:** El mismo patrón puede afectar otros mapeos en `morphological_variants.py` donde la raíz es más genérica que los tokens hijos.

**Prioridad:** Media — afecta precisión en queries técnicas, no es un crash.

---

## ADR-024: QueryPlan invariants consolidation

**Fecha:** 2026-06-03
**Estado:** Accepted

**Decisión:** Consolidar las invariantes jurídicas/semánticas de contratación en
`app/core/contract_invariants.py` y aplicarlas desde `QueryPlan` como P10-P13,
manteniendo `QueryPlan` puro y bajo el límite G8 de 300 LOC.

**Invariantes consolidadas:**
- `firmado`/`celebrado`/`suscrito` seleccionan `dataset=contratos`, pero no son
  valores válidos de `estado_contrato`.
- `Firmado` y `Celebrado` se purgan como estados inválidos sin borrar estados
  reales como `En ejecución`, `Modificado`, `Prorrogado` o `Cerrado`.
- Intención de ofertar/presentarse fuerza búsqueda de oportunidades:
  `dataset=procesos`, `intent_type=opportunity_search`,
  `estado_family=oferta_abierta`.
- `oferta_abierta` no aplica a contratos.
- El check pasivo P13 usa `SCOPE_TOPIC_KEYS` para detectar candidatos globales
  sin bloquear en `QueryPlan`.

**Corrección de alcance INV-6:** `SCOPE_TOPIC_KEYS` y `NONEMPTY_WHERE_KEYS`
responden preguntas distintas y se mantienen separadas:
- `SCOPE_TOPIC_KEYS` pregunta si hay base útil para ejecutar
  (entidad/lugar/tema/contratista). Estado y modalidad no cuentan.
- `NONEMPTY_WHERE_KEYS` pregunta si el builder puede emitir un `WHERE` no
  tautológico. Estado y modalidad sí cuentan porque evitan `WHERE 1=1`.

Por eso `"contratos en ejecución"` puede construir SoQL con
`estado_contrato = 'En ejecución'`, pero `execute_query` todavía pide un filtro
de entidad, lugar, tema o contratista. Ese comportamiento queda congelado por
test y P13 queda pasivo; activar el bloqueo en `QueryPlan` queda como plan
futuro `QP-SAFETY-ACTIVATION`.

**Guardrails agregados:**
- `tests/test_contract_invariants.py`: unidad pura de invariantes.
- `tests/test_invariants_characterization.py`: caracterización del pipeline
  offline `parse -> llm_policy opcional -> compute_query_plan`.
- `tests/test_invariants_lock.py`: falla si se reintroducen listas/frases
  consolidadas fuera de `contract_invariants.py`.
- `tests/test_plan_query_node.py` congela la diferencia intencional entre
  builder anti-tautología y guard de ejecución.

**Deudas conscientes descubiertas:**
- El mapeo parser-level `firmado -> Aprobado` en `app/data/estados.py` se
  preserva por 0 regresiones, pero no se declara verdad de dominio. Requiere
  experimento de producto para medir cuánto oculta otros estados firmados.
- P13 sigue pasivo porque `execute_query` ya bloquea correctamente las búsquedas
  sin base útil. Activarlo en `QueryPlan` cambia razón/ruta/mensaje y requiere
  prueba de equivalencia de respuesta.

**Métricas al cierre:**
- Suite: 1306 passed, 23 xfailed
- `query_plan.py`: 275 LOC
- `make lint-core`: limpio
- Grep de duplicados (`_INVALID_CONTRACT_STATES`, `_CLOSED_STATES`,
  `_BIDDER_PHRASES`, `_SUBSTANTIAL_FILTER_KEYS`): vacío

---

## ADR-025: Soporte de fechas relativas semana/día en DialoguePolicy + QueryPlan

**Fecha:** 2026-06-04
**Estado:** Accepted

**Contexto:** La capa temporal solo soportaba mes/año (`temporal_month` vía P5).
Expresiones cotidianas como "esta semana", "ayer", "últimos 30 días" no producían
rango de fechas. Peor: los tokens temporales ("semana", "dias") se colaban al
objeto contractual, contaminando la búsqueda con un AND que mataba el recall.

**Decisión:** Extender la capa temporal existente con un nuevo tipo de señal
(`temporal_range`) que convive con `temporal_month` sin tocarlo:

1. **Detección** (`dialogue_policy.py`): nueva función `_detect_temporal_range()`
   con 5 regex R4b (hoy, ayer, esta semana, semana pasada, últimos N días).
   Emite señales `kind="temporal_range"` con payload `{desde, hasta, phrase}`.
   Se llama desde `classify()` justo después de `_detect_temporal_month`.

2. **Aplicación** (`query_plan.py`): nueva regla P5b (espejo de P5) que consume
   `temporal_range` y aplica `desde`/`hasta` a `working_params`. Mismo guard que
   P5: si ya hay `fecha_desde`/`fecha_hasta` → `temporal_conflict`, no sobreescribe.

3. **Scrub** (`query_router.py`): +7 tokens a STOPWORDS (`semana, dia, dias, día,
   días, ayer, pasada`). "hoy" ya estaba. La detección lee el query crudo, no el
   objeto → scrub y detección son independientes.

**Semántica exacta** (usa el `today: Date` que `classify` recibe del contexto):
| Expresión | desde | hasta |
|-----------|-------|-------|
| "hoy" | today | today |
| "ayer" | today − 1 | today − 1 |
| "esta semana" | lunes ISO | today |
| "la semana pasada" / "semana pasada" | lunes previo | domingo previo |
| "últimos N días" (1-365) | today − N | today |

**Alternativas descartadas:** Meter la lógica en `temporal_month` existente
(mezcla semánticas distintas: mes necesita `calendar.monthrange`, semana/día
necesita `timedelta` + `isocalendar`). Usar `datetime.now()` en vez del `today`
del contexto (rompe testabilidad, zona horaria inconsistente).

**Guardrails:**
- P5 (`temporal_month`) intacta — no se tocó ni una línea.
- Detección sobre query crudo, no sobre objeto → inmune al scrub de STOPWORDS.
- Conflicto con fechas preexistentes → `temporal_conflict` flag.
- Zona horaria: usa el `today` que el contexto provee, sin `datetime.now()` nuevo.

**Métricas al cierre:**
- Suite: 1334 passed, 23 xfailed (1324 base + 10 tests Claude Code gap #2)
- Tests propios: 15 en `tests/test_temporal_relative_characterization.py`
- `query_plan.py`: 293 LOC (+18, bajo el límite G8 de 300)
- `dialogue_policy.py`: +88 LOC (detección + regex + tipo literal)
- `make lint-core`: limpio
- Commit: `4ac717c`

---

## ADR-026: "Firmado" no es un estado — semántica correcta vía fecha_de_firma

**Fecha:** 2026-06-04
**Estado:** Accepted

**Contexto:** `ESTADO_CONTRATO_SYNONYMS` en `estados.py` mapeaba
`firmado/firmada/firmados → 'Aprobado'`. Esto violaba el invariante documentado
en `estado_families.py:64` e `intent_vocabulary.py:69` ("firmado = sin filtro de
estado, solo fuerza dataset=contratos"). El bug era grave: `estado=Aprobado`
restringía a solo 184K contratos de ~5.6M realmente firmados, tirando ~97% del
recall.

**Validación empírica contra SECOP real** (dataset `jbjy-vk9h`, ~5.2M contratos):

| estado_contrato | total | con fecha_de_firma | % firmado |
|---|---|---|---|
| En ejecución | 1,680,032 | 1,680,032 | 100% |
| Cerrado | 1,567,172 | 1,567,172 | 100% |
| Modificado | 1,031,720 | 1,031,720 | 100% |
| terminado | 730,535 | 730,535 | 100% |
| Aprobado | 184,033 | 184,031 | 100% |
| cedido | 28,044 | 28,044 | 100% |
| Suspendido | 6,428 | 6,428 | 100% |
| Prorrogado | 121 | 121 | 100% |
| Borrador | 234,448 | 437 | 0.2% |
| Cancelado | 107,313 | 80 | 0.1% |
| enviado Proveedor | 41,562 | 109 | 0.3% |
| En aprobación | 21,659 | 84 | 0.4% |

8 estados tienen ~100% de firma; 4 tienen <0.5%. "Firmado" NO es un valor de
`estado_contrato` — es `fecha_de_firma IS NOT NULL`. Enumerar estados es frágil
(¿qué pasa si SECOP agrega uno nuevo?); el filtro por `fecha_de_firma` es exacto
y a prueba de futuro.

**Decisión:** 
1. Quitar `firmado/firmada/firmados → 'Aprobado'` de `ESTADO_CONTRATO_SYNONYMS`
   en `estados.py`. Dejar nota explicando la semántica `fecha_de_firma`.
2. Agregar `firmada/firmadas` a STOPWORDS (ya estaban `firmado/firmados`).
3. Mantener la capa de `intent_vocabulary.py` que fuerza `dataset=contratos`
   para "firmado" (eso sí es correcto).

**Alternativas descartadas:** Enumerar explícitamente los 8 estados con firma
(frágil — SECOP puede agregar/quitar estados). Mapear "firmado" a
`estado_family=con_contrato` (incluye Borrador/En aprobación que NO están
firmados).

**Tests:**
- Volteado `test_signed_contracts_current_baseline_keeps_legacy_aprobado` →
  `test_signed_contracts_apply_no_state_filter` en `test_invariants_characterization.py`
- Nuevo `tests/test_firmado_signed_semantics.py` (15 tests): caso real de la sesión
  ("contratos de adulto mayor firmados en mayo" → sin estado, fecha_de_firma en mayo)
  + regresión de estados explícitos (En ejecución, Cerrado, terminado, Aprobado legítimo)

**Métricas al cierre:**
- Suite: 1338 passed, 23 xfailed
- Commit: `a745ac8`
- `make lint-core`: limpio

---

## ADR-027: Procesos sin URL pública — señalizar, no omitir; paridad entre canales

**Fecha:** 2026-06-10
**Estado:** Accepted

**Contexto:** Un usuario reportó "en Streamlit salen los links de los contratos,
en Telegram no". La hipótesis natural (Telegram pierde/rompe URLs) era falsa. La
construcción de URL es **compartida** por todos los canales vía
`Formatter._safe_url` → `to_rows`, así que el campo `url` es idéntico en Streamlit,
Telegram y WhatsApp. Medido sobre la query real "oportunidades en adulto mayor":
de 25 resultados, **solo 6 tenían URL**. Las otras 19 son procesos con
`id_del_proceso = CO1.REQ.*` y `urlproceso` vacío.

`CO1.REQ.*` = requisiciones pre-publicación / régimen especial (ESAL). Su página
`OpportunityDetail` devuelve "argument out of range" → un fix previo
(FIX-URL-FREEFORM-001 / DOGFOOD-URL-005) ya los suprime a propósito en `_safe_url`
(no se muestra un link roto). La diferencia real entre canales NO estaba en los
datos sino en la **señalización**:
- **Streamlit** (`streamlit_app.py:139`) mostraba la nota
  `🔒 Vista pública no disponible (proceso pre-publicación)`.
- **Telegram / WhatsApp** **omitían la línea en silencio** → parecía que "faltan
  los links" cuando en realidad el proceso no es consultable públicamente.

**Decisión:**
1. **No omitir en silencio: señalizar.** Cuando una fila no tiene `url`, los tres
   canales muestran la misma nota de cortesía
   `🔒 Vista pública no disponible (proceso pre-publicación)`. Se añadió la rama
   `else` en `format_telegram` y `format_whatsapp` (`formatter.py`), igualando lo
   que Streamlit ya hacía.
2. **La política de resolución de URL sigue centralizada en `_safe_url`** (fuente
   única; los formatters por canal solo deciden cómo *presentar* el resultado, no
   lo recalculan). Streamlit no se modificó.

**Alternativas descartadas:**
- *Fabricar un link igual para CO1.REQ* (p.ej. `OpportunityDetail?noticeUID=CO1.REQ…`):
  da error en SECOP; mostrar un link roto es peor que señalizar la ausencia.
- *Filtrar/ocultar los procesos sin URL*: pierden visibilidad oportunidades reales
  (varios ESAL "Abierto" de alto valor); el usuario debe verlos aunque no sean
  consultables online.
- *Resolver la nota por canal con texto distinto en cada formatter*: rompería la
  paridad; se reusó la redacción exacta de Streamlit.

**Tests:**
- Nuevo `tests/test_url_fallback_channel_parity.py` (5 tests): CO1.REQ → nota en
  Telegram y WhatsApp; CO1.NTC → link real; conteo de notas = filas sin url;
  blindaje de que `_safe_url` deja `url=""` para CO1.REQ y URL válida para CO1.NTC
  (Streamlit intacto).

**Consecuencias:** Los tres canales explican por qué un proceso no tiene link, en
vez de dejar un hueco confuso. Útil para Radar (futuro): un proceso pre-publicación
es señal de oportunidad temprana y ahora el sistema lo distingue explícitamente.
Nota de producto pendiente (no es bug): muchos resultados "Abierto" de adulto mayor
son ESAL sin página pública — evaluar si etiquetarlos o priorizarlos distinto.

**Métricas al cierre:**
- Suite: 1415 passed, 23 xfailed (incluye +5 de `test_url_fallback_channel_parity.py`)
- Fix en `app/core/formatter.py` (`format_telegram`, `format_whatsapp`)
- Streamlit (`streamlit_app.py`) NO modificado

---

## ADR-028: Búsqueda por regiones geográficas (Caribe, Pacífico, Llanos...)

**Fecha:** 2026-06-10
**Estado:** Accepted

**Contexto:** Los usuarios preguntan naturalmente por zonas: "contratos en la costa
caribe", "obras en el pacífico", "oportunidades en los llanos". El parser solo
entendía departamentos individuales. "Caribe" no matchea con ningún departamento
y la búsqueda fallaba o devolvía resultados irrelevantes.

**Decisión:**
1. Crear `app/data/colombia_geography.py` con:
   - `DEPARTAMENTOS_BY_REGION`: 6 regiones mapeadas a sus departamentos oficiales
     SECOP (con tilde correcta: Atlántico, Bolívar, Cesar, Córdoba, La Guajira,
     Magdalena, Sucre para Caribe; Cauca, Chocó, Nariño, Valle del Cauca para
     Pacífico; etc.)
   - `REGION_ALIASES`: aliases coloquiales → región canónica ("costa caribe" →
     "Caribe", "pacífico colombiano" → "Pacífico", "llanos orientales" →
     "Llanos", etc.)
2. Agregar `scan_region()` en `EntityResolver` — simétrico a `scan_departamento`,
   con índice de primer-token para detección O(1).
3. Rama 6.4 en `QueryRouter`: detectar región SOLO si no hay departamento
   específico (precedencia: "bolívar" explícito gana sobre región Caribe aunque
   Bolívar pertenezca al Caribe). Setea `region` + `departamentos_in`.
4. `SoQLBuilder`: nueva rama `departamento IN (...)` cuando hay `departamentos_in`.
   Se agregó a `NONEMPTY_WHERE_KEYS` para que el guard anti-global reconozca el
   scope regional como filtro válido.

**Trampas de diseño cubiertas:**
- **Colisión Amazonas/Amazonía:** "amazonas" (departamento) vs "amazonia" (región).
  El scan de departamento corre primero; si matchea "Amazonas", no se busca región.
  "Amazonia" solo matchea como región.
- **Precedencia departamento > región:** un departamento explícito ("bolívar")
  gana sobre la región aunque pertenezca a una.
- **Timeout en regiones grandes:** Andina = 11 departamentos genera `IN` con 11
  valores. El path count-first existente absorbe el riesgo (mismo comportamiento
  que un departamento grande hoy).

**Tests:** `tests/test_regiones.py` (10 tests): cobertura de datos, detección,
builder, colisión Amazonas/Amazonía, precedencia departamento > región.

**Fuera de alcance (deferido):** hint en respuesta ("región Caribe: 7
departamentos"), relajación dirigida por región en `degrade_query`, subregiones
finas (Costa Atlántica vs. Caribe continental).

**Métricas al cierre:**
- Suite: 1425 passed, 23 xfailed
- Commit: `95dbf59`

---

## ADR-029: SECOPPAL Radar v1.0 — Búsquedas guardadas con feed de movimientos

**Fecha:** 2026-06-11
**Estado:** Accepted (commiteado en `main`, pendiente de merge a feature branch)

**Contexto:** Los usuarios repiten consultas periódicas ("¿qué hay de nuevo en
mantenimiento vial en Cundinamarca?"). Volver a formular la misma búsqueda cada
vez es tedioso y no hay forma de saber qué cambió desde la última consulta.

**Decisión:** Construir Radar como sistema de búsquedas guardadas + diff entre
ejecuciones, en dos sprints:

**Sprint 1 — Backend puro (`app/core/radar/`):**
- `store.py`: `RadarStore` con 3 tablas SQLite (`radars`, `radar_runs`,
  `radar_seen_results`). Auditoría completa: soql, params_json, execution_ms,
  created_from_trace_id, relevance_label.
- `keys.py`: clave estable (`radar_key`) + snapshot hash de resultados,
  leyendo los campos de `SoQLBuilder.SPECS`.
- `diff.py`: clasifica resultados como nuevos / cambio de estado / cambio de
  valor / desaparecidos + operador `nuevo` que omite los ya vistos.
- `runner.py`: ventana `limit=100`, mide `execution_ms`, persiste lo visto,
  registra corrida.
- `digest.py`: lectura asesora con `risk_flag` "Radar amplio" cuando el
  universo supera la ventana.
- **Bisagra:** `helpers.py` — `execute_resolved_params` extraído
  conversacionalmente puro (sin `append_turn`, sin feedback, sin mutar
  contexto). `refinement.py` lo reusa; el golden pasa idéntico.

**Sprint 2 — Integración:**
- `commands.py` + `handlers/radar.py`: comandos guardar/listar/ejecutar radar,
  registrados en la cadena antes de `handle_normal_flow` (orchestrator.py).
- `streamlit_app.py`: pestaña 📡 Radares con tarjetas clicables → timeline de
  corridas con digests expandibles + botón "Ejecutar ahora".

**Alternativas descartadas:**
- *Cron/scheduler automático*: diferido a v1.1. La v1.0 es manual (el usuario
  dispara la ejecución).
- *Polling server-side*: requeriría proceso persistente; SQLite local es
  suficiente para el MVP manual.
- *Almacenar resultados completos*: solo se guarda snapshot hash + diff;
  ligero y rápido.

**Consecuencias:** El usuario puede guardar una búsqueda, volver días después,
ejecutar el radar y ver exactamente qué procesos/servicios/contratos son nuevos,
cuáles cambiaron de estado y cuáles cambiaron de valor. La pestaña Streamlit da
visibilidad completa del timeline.

**Tests:** `tests/test_radar_commands.py` (29 casos) + suite radar/golden (72
passed en criterio de cierre Sprint 2).

**Métricas al cierre:**
- Suite completa: 1425 passed, 23 xfailed, 1 failure (ambiental:
  test_corpus_v1_unaffected — venv path hardcodeado en worktree)
- Commits: `9c1e9c0` a `1a7e1df` (main)

---

## ADR-030: Analytics — Paquete modular con registry de analyzers

**Fecha:** 2026-06-09
**Estado:** Accepted

**Contexto:** La primera ruta analítica (AQ-001A aggregate_sum, ADR sin número)
estaba hardcodeada en el orchestrator como un if-else. Agregar más analyzers
(top_entities, top_contractors, distribuciones, comparativas) requería un
mecanismo extensible, no más ramas condicionales.

**Decisión:**
1. Crear `app/core/analytics/` como paquete Python con:
   - `registry.py`: registro central de analyzers con `@register_analyzer`.
     Cada analyzer declara su `intent`, `guard` y `execute`.
   - `base.py`: `BaseAnalyzer` con interfaz común.
   - `aggregate_sum.py`: primer analyzer migrado desde orchestrator.
   - `contratado_pagado.py`: analyzer de ejecución presupuestal (comparativa
     valor contratado vs. pagado).
2. `AnalyticsEngine` en orchestrator: itera el registry, ejecuta el primer
   analyzer cuyo guard se active. Si ningún guard activa, cae a búsqueda normal.
3. El analyzer retorna `AnalyticalResult` con `response_text` y metadatos; el
   orchestrator lo propaga sin que `degrade_query` ni `format_response` lo
   sobrescriban.

**Alternativas descartadas:**
- *Seguir con if-else en orchestrator*: no escala; cada analyzer nuevo requería
  tocar el orchestrator.
- *LLM para clasificar intención analítica*: overkill para queries deterministas
  como "cuánto se contrató".

**Consecuencias:** Agregar un analyzer nuevo es una clase + decorator, sin tocar
orchestrator. El registry permite descubribilidad y testing aislado.

**Tests:** `tests/test_analytical_queries.py` (existente, extendido con
contratado/pagado).

**Métricas al cierre:**
- Suite: 1338 passed, 23 xfailed
- Commit: `3150cbf`

---

## ADR-031: Adaptador OpenWA + robustez Telegram y paridad de links

**Fecha:** 2026-06-10
**Estado:** Accepted

**Contexto:** SECOPPAL usaba solo el adaptador Telegram nativo. Se necesitaba
soportar WhatsApp vía OpenWA (API abierta) sin duplicar lógica de formatting.
Además, se detectó que Telegram/WhatsApp omitían en silencio los procesos sin
URL pública (CO1.REQ.*), creando la falsa impresión de que "faltaban links".

**Decisión:**
1. **OpenWA adapter** (`app/channels/openwa_adapter.py`): implementa la interfaz
   de canal existente, reusa `Formatter.format_whatsapp()` para el formatting
   (cero duplicación). Soporta envío de mensajes y recepción de comandos vía
   webhook.
2. **Paridad de señalización** (ADR-027): los tres canales (Telegram, WhatsApp,
   Streamlit) ahora muestran `🔒 Vista pública no disponible (proceso
   pre-publicación)` para procesos sin URL, en vez de omitir la línea en
   silencio.
3. **Robustez Telegram**: reintentos con backoff, manejo de floods, mensajes
   largos fragmentados automáticamente.

**Alternativas descartadas:**
- *Adaptador separado con lógica duplicada*: cada canal tendría su propio
  formatter; divergencia inevitable.
- *Usar Twilio/MessageBird*: costo recurrente; OpenWA es self-hosted y gratuito.

**Tests:** `tests/test_url_fallback_channel_parity.py` (5 tests) para la
señalización; tests de integración del adaptador en `test_openwa_adapter.py`.

**Métricas al cierre:**
- Suite: 1415 passed, 23 xfailed
- Commit: `bcf4a2d`


## ADR-032: Clasificación de etapa de oportunidad en SECOP II (OPP-STAGE-001)

> Numeración: en la rama principal existen ADR-028 a ADR-031 sin mergear; este se
> numera ADR-032 para evitar colisión. Reconciliar al merge.

**Contexto.** SECOP parte el ciclo de vida en DOS ejes desalineados:
`estado_del_procedimiento` (grueso) y `fase` (etapa real). Un mismo `Publicado`
abarca varias fases. Además el dataset de procesos mezcla **requisiciones internas
en borrador** (id `CO1.REQ.*`, sin publicar) con procesos publicados, y el usuario
lo interpreta todo como "procesos para presentarme". Se deriva una noción propia
de accionabilidad.

**Decisiones confirmadas (con evidencia).**
1. `estado`, `apertura` y `fase` son dimensiones distintas; NO se colapsan en
   `ESTADO_FAMILIES`. `OpportunityStage` vive en módulo puro
   (`app/core/opportunity_stage.py`), hermano de `opportunity_scope_policy.py`.
2. **No existe campo de fecha de vigencia poblado.** Auditadas las 11 columnas de
   fecha: solo las de publicación (~99%); recepción/apertura/adjudicación ~0%. → el
   clasificador NO recibe fecha y NO afirma vigencia.
3. **No afirmar "puede ofertar ahora"** → `offer_phase_indicated`, no "accionable".
4. **La URL no determina publicación** → etiqueta "Sin enlace público disponible
   en datos abiertos" (antes "proceso pre-publicación").
5. El clasificador opera en **shadow** (Fase 2B): trace `opportunity_stage_shadow`
   con `computed_from=current_result_set`, sin cambiar orden/filtro/respuesta/Radar.
   Flag `OPPORTUNITY_STAGE_SHADOW_ENABLED`.
6. Seis etapas: `offer_phase_indicated`, `published_pre_offer_signal`,
   `unpublished_draft_signal`, `post_offer_signal`, `conflicting_signals`,
   `unknown`. El sub-estado del borrador (Borrador/En aprobación/Aprobado) se
   conserva como razón (no valen igual).
7. `confidence` del acuerdo de señales (high=alineadas, medium=parcial,
   unknown=low); nunca "low" sobre una etapa fuerte.

**Hallazgos empíricos (shadow).** 125 filas de 5 consultas (funnel correcto):
`offer_phase_indicated` ~29%, `unpublished_draft_signal` ~69%,
`published_pre_offer_signal` ~2%. **La muestra está sesgada por el orden actual**
(`fecha_de_publicacion_del DESC`), que sube los registros con fecha NULL —los
borradores— al inicio. Prueba que *la primera página está contaminada por
borradores no publicados*, NO que el 69% del universo lo sea.

**Hipótesis NO demostradas (no afirmar como hechos).**
- que los borradores nunca se publiquen;
- que reaparezcan con otro identificador;
- que todo `Aprobado` sin URL sea ruido;
- que la distribución observada represente el universo;
- que un proceso sin enlace no tenga versión pública relacionada;
- que un `offer_phase_indicated` no haya producido ya un contrato en otro dataset.

**Decisión diferida (Fase 3).** No se decide aún entre filtro / boost / consultas
separadas / sección colapsada / Radar. Antes se requiere: distribución de
`unpublished_draft_signal` por sub-estado y por profundidad (top 25/100/500), en
más sectores y regiones, para distinguir problema de ordenamiento de NULL vs funnel
amplio vs señal temprana útil. Apuesta probable: dos carriles (búsqueda pública =
offer + published_pre_offer; inteligencia temprana separada). NO un simple boost
sobre 50 (los públicos relevantes pueden quedar fuera del lote por el orden actual).

**Errores corregidos en el proceso.** El primer experimento shadow fue INVÁLIDO:
llamó a `SoQLBuilder.build()` con `estado_family` crudo sin expandir → SoQL sin
filtro de estado → se colaron estados post-cierre. Deuda de tooling: helper canónico
de params para diagnósticos (tarea aparte).

**Hallazgo de recall — HUECO DE COBERTURA (corregido tras diagnóstico).** El caso
"semáforos en Galapa 2026" → 0 resultados se atribuyó primero a dos bugs; el dato
los refutó:
- "Entidad con punto final" REFUTADO: las 4.083 filas de Galapa Y el gazetteer
  tienen ambos "MUNICIPIO DE GALAPA." → coinciden, el `LIKE` no descarta nada.
- Filtro de año sobre `fecha_de_publicacion_del` NULL: bug latente real, pero NO
  fue la causa aquí (la fila no está).
- **Causa real, confirmada por NIT (890102472, clave estable):** el proceso 2026
  (`SA-006-2026` / `CO1.NTC.10384092`, Published, fase observaciones) **NO está en
  `datos.gov.co` p6dx-8zbt** (solo 4 filas Galapa+semáforo, todas 2025), pese a
  estar vivo en `community.secop.gov.co` y a que el dataset está fresco (máx
  publicación 2026-06-14).

**Limitación conocida.** El techo de recall de SECOPPAL = lo exportado a
datos.gov.co. Un proceso vivo y publicado en el portal transaccional puede estar
ausente del open-data; ningún filtro/ranking lo recupera. Mitigación futura:
consultar la API transaccional de `community.secop.gov.co` y/o datasets SECOP
adicionales como fuente complementaria para procesos actuales (feature, no fix).
Bug latente independiente aún en cola: filtro de año sobre fecha NULL.

**Tests:** `tests/test_opportunity_stage.py` + `tests/test_opportunity_stage_shadow.py`.
Suite al cierre: 1611+ passed, solo el fallo de entorno preexistente.

---

## ADR-033: Gestión de usuarios — autenticación email+contraseña en SQLite, roles y sesión persistente

**Fecha:** 2026-06-18
**Contexto:** La UI Streamlit no tenía autenticación: la "identidad" era un
`user_email`/`user_name` de texto libre en el sidebar (`_current_user_identity()`) más un
id anónimo en la URL. No había forma de distinguir un administrador del resto, ni de
ocultar nada. Se necesitaba: (1) login real, (2) un rol admin que pueda todo —incluido
dar de alta cuentas— mientras los demás conservan toda la búsqueda, (3) ocultar el bloque
"Métricas" (estadísticas de acierto) a los no-admin, (4) registrar el uso por usuario,
(5) desplegar en un VPS.

**Decisión:** Nuevo módulo `app/core/auth.py` con tabla `app_accounts` en la misma SQLite
del feedback (`data/secoppal.db`), separada de la tabla analítica `users` y enlazada por
email. Hashing **PBKDF2-HMAC-SHA256 de la stdlib** (salt por cuenta, `hmac.compare_digest`)
para no añadir `bcrypt`. Dos roles (`admin`/`user`). El admin inicial se siembra desde
env vars (`SECOPPAL_ADMIN_EMAIL`/`_PASSWORD`/`_NAME`) vía `ensure_admin_seed()` idempotente;
las altas posteriores solo las hace el admin desde un panel en la UI. `streamlit_app.py`
gana un gate de login (`_require_login()` + `st.stop()`); el bloque "Métricas" y el panel
"👤 Gestión de usuarios" quedan tras `if _is_admin`. La identidad autenticada alimenta el
cableado existente → cada trace queda atada al `user_id` real (la tabla `traces` ES el
registro de uso; se expone `get_usage_by_user()`). **Sesión persistente** ("recordarme")
con cookie `secoppal_session` (token opaco) respaldada por tabla `app_sessions` (TTL 30
días, revocable), usando `extra-streamlit-components`. Despliegue objetivo: **Contabo VPS**
(systemd + nginx HTTPS + backup de `secoppal.db`).

**Alternativas descartadas:** OIDC/Google `st.login` (exige registrar app OAuth + IdP +
dominio; sobredimensionado para un equipo pequeño). Magic link passwordless (requiere SMTP).
`bcrypt`/`streamlit-authenticator` (dependencia evitable; PBKDF2 stdlib basta). Registro
abierto o por código de invitación (se prefirió control total: solo el admin crea cuentas).
**Netlify / serverless** (no corre un servidor Streamlit persistente ni SQLite en disco
persistente). Token de sesión firmado sin tabla (no permitiría revocación al cerrar sesión
ni al desactivar cuentas).

**Consecuencias:** La cookie de sesión viaja en claro sobre HTTP → el deploy **debe** ser
HTTPS. El "recordarme" resuelve la pérdida de sesión en F5 (Streamlit crea sesión nueva por
refresco); sin marcarlo, se reingresa tras refresh. Se protege contra desactivar/degradar al
último admin activo. Nueva dependencia `extra-streamlit-components>=0.1.71` en
`requirements.txt`. `SECOPPAL_AUTH_ENABLED=false` desactiva el gate para dev/tests
(conserva el comportamiento legacy de identidad por texto libre).

**Tests:** `tests/test_auth.py` (17 passed) — hashing, create/authenticate, unicidad,
cuenta inactiva, reset, roles, protección del último admin, seed idempotente y
sesiones (crear/resolver/revocar/expirar/cuenta desactivada).

---

## ADR-034: Scheduler de radares — systemd timer + script (pull programado), refresh-only

**Fecha:** 2026-06-19
**Contexto:** El Radar v1.0 era pull **manual**: `run_radar` solo se dispara cuando el
usuario pulsa "Ejecutar/Actualizar" en la UI. El campo `frequency` (`manual`/`cada_6h`/
`diaria`/`semanal`) se guardaba pero nada lo consumía. Consecuencia: "volver días después"
muestra el feed de la última corrida manual, no las novedades reales. Se necesitaba que los
radares activos corrieran solos según su frecuencia. Disparador: revisión de robustez del
Radar tras añadir usuarios autenticados (el `owner` ahora es `email:` estable, así que un
scheduler multi-usuario es viable y seguro).

**Decisión:** Núcleo reusable en `app/core/radar/scheduler.py` — `is_due(radar, now)`
(mapa `FREQUENCY_INTERVALS`; `manual`/desconocida nunca corre; sin `last_run_at` siempre;
tolerancia de 10 min para el jitter del timer) y `run_due_radars(deps, store, *, force=False)`
que itera `RadarStore.list_all_enabled_radars()` (nuevo) y ejecuta los que toquen vía el
`run_radar` existente, **aislando el error por radar** (uno que falle no tumba la pasada).
Entrypoint `scripts/run_radar_scheduler.py` (reusa `get_workflow()`, que es `lru_cache` y por
tanto headless; flags `--dry-run`/`--force`). Disparo por **systemd timer** (`deploy/
secoppal-radar.{service,timer}`): cada 6 h con `Persistent=true` (recupera corridas perdidas
tras apagón). **Alcance v1.1 = solo refrescar el feed**, sin notificación.

**Alternativas descartadas:** APScheduler/hilo dentro del proceso Streamlit (atado al ciclo
de vida del proceso, compite con los reruns, reloj se reinicia con la app). Cron crudo del
sistema (systemd timer da `Persistent=true`, logs en journald y reinicio gestionado). Notificar
ahora por Telegram/WhatsApp (el `owner` es un email; no hay mapeo web→chat id — se difiere).

**Consecuencias:** `frequency` pasa de decorativo a funcional. El scheduler corre como proceso
aparte (no necesita la app Streamlit arriba). **Costura para notificación futura:**
`run_due_radars` devuelve por radar `{owner, digest, new_count, changed_count}`; un futuro
`notifier` + tabla `notification_prefs(owner, canal, dirección, on/off)` permitirán WhatsApp/
email a elección del usuario sin refactor. Cadencia fija de 6 h → un radar `diaria` puede
correr hasta ~5 h tarde (irrelevante para SECOP). Ventana de 100 resultados de `run_radar`
sigue vigente (radares muy amplios pueden no ver novedades fuera del top-100).

**Tests:** `tests/test_radar_scheduler.py` (12 passed) — `is_due` (manual, sin run, intervalos,
tolerancia, timestamp naive) y `run_due_radars` (corre solo los due, salta manual/no-due/
deshabilitado, `force` corre todos, un fallo no detiene a los demás, `status="error"` cuenta).

---

## ADR-035: "Seguir proceso" — watchlist individual sobre un radar sintético

**Fecha:** 2026-06-20
**Contexto:** El Radar rastrea **búsquedas** (query → resultados que matchean). Faltaba poder
seguir un **proceso concreto** desde su tarjeta para vigilar SUS cambios de estado/valor, aunque
no haga match con ninguna búsqueda. Decisión de producto: una **lista única "📌 Procesos
seguidos"** por usuario, con los cambios visibles en el mismo feed del radar. Restricción
técnica hallada: las filas de tarjeta del formatter (`_to_card_rows`) eran **solo de display**
(sin `id_del_proceso`/`referencia`), por lo que no había clave estable para seguir.

**Decisión:** Modelar la watchlist como un **radar sintético** por usuario (`frequency='follows'`,
nombre `📌 Procesos seguidos`) en vez de tablas nuevas — reutiliza intactos `radar_seen_results`
(los seguidos), `radar_events` (feed, FK a `radars` satisfecha), `keys.py` (clave + hash) y el
render del feed. Nuevo módulo `app/core/radar/follows.py` (`follow`/`unfollow`/`is_following`/
`list_follows`/`refresh_follows` + `get_or_create_follows_radar`). El formatter ahora **enriquece**
cada fila de tarjeta con `id_del_proceso`/`id_contrato`, `referencia_*` y `dataset_id` (campos
aditivos). Al seguir, el snapshot queda vacío → la **1ª** corrida de `refresh_follows` fija la
línea base en silencio; después, cambios de estado/valor emiten `changed_state`/`changed_value`.
El scheduler (ADR-034) **salta** el radar follows en el lado query (`is_due` False para 'follows')
y lo refresca aparte en cada pasada. UI: botón **👁 Seguir/✓ Siguiendo** por tarjeta + sección
"Procesos seguidos" en la pestaña Radares + `store.delete_seen()` para dejar de seguir.

**Alternativas descartadas:** Tablas dedicadas `followed_results`/`followed_events` (duplicaría
feed y diff ya existentes). Hacer `radar_id` nullable en `radar_events` para colgar eventos sin
radar (migración de columna NOT NULL en SQLite, frágil; el radar sintético satisface la FK sin
tocar el esquema). Re-consultar SECOP al pulsar "Seguir" (lento; se prefiere seguir instantáneo
con los datos de la tarjeta y fijar baseline en el primer refresh).

**Consecuencias:** El radar sintético aparece en `list_radars(owner)`; se **filtra** del listado
de búsquedas y de `_refresh_all_radares` por `frequency=='follows'`. El valor mostrado del seguido
pasa de formateado (tarjeta) a crudo (SECOP) tras el primer refresh → la UI lo formatea con
`_fmt_money_feed`. Un cambio ocurrido entre seguir y el primer refresh no se reporta (baseline).
Procesos sin id ni referencia no se pueden seguir (`follow` devuelve False).

**Tests:** `tests/test_radar_follows.py` (10 passed) — follow idempotente/sin-clave/aislado por
owner, unfollow, refresh (baseline silenciosa, cambio de estado, cambio de valor, sin cambios,
missing) y que el scheduler no corre el radar follows como búsqueda.

## ADR-036: Phrase-aware retrieval — recuperación amplia, validación local y ranking previo al render

**Fecha:** 2026-06-26 · **Estado:** Accepted · **Tag:** `semantic-retrieval-v1-2026-06`

**Contexto:** Para "elaboración o actualización de **manuales de contratación** en 2024" el
sistema mostraba 138 resultados de los que **solo 5 eran del tema** (precisión ~3.6%) y perdía
95 de 100 contratos relevantes conocidos (recall ~5%). El diagnóstico por capas reveló que el
problema NO era comprensión sino aguas abajo, en cinco puntos encadenados: (1) el extractor
partía "manual de contratación" en tokens y descartaba "contratación", buscando `%MANUALES%`
plural suelto → perdía todos los singulares y traía cualquier "manuales de [otra cosa]"; (2) el
`WHERE` estricto eliminaba documentos pertinentes antes de que ninguna capa los viera —
**el ranking no rescata filas que nunca llegaron**; (3) `LIMIT 50` con `ORDER BY fecha DESC`
ocultaba 52 de 100 positivos conocidos (medido live: 48/100 → 100/100 al subir a 200); (4) el
BM25 histórico corría **después** de que el formatter ya había congelado las filas, así que
nunca afectaba el display (`RELEVANCE-RANKING-AFTER-FORMAT-001`) — esto explica por qué la
democión AND→OR, que confiaba en un BM25 posterior, nunca recuperó precisión visible; (5) el
texto de SECOP es sucio (saltos de línea, puntuación) y la frase exacta literal perdía positivos
por formato, no por semántica. Validado además con A/B de juez en sprints previos: el extractor
LLM NO sube precisión → el cuello no es comprensión. Ver [[project_secoppal_arquitectura_y_eval]].

**Decisión:** Una capa **phrase-aware** donde el significado sobrevive de la consulta al display:
`parse → preservar PhraseSpec (objeto_phrases) → WHERE recall-first → candidate pool → validación
local → ranking formal → display`. Reglas que la componen, cada una protegida por tests:
- **Frase como unidad:** `phrase_catalog` (datos) + `detect_phrases` consume la frase ANTES del
  scrub de tokens y la emite como `objeto_phrases` (JSON-safe); el `WHERE` la consume.
- **Recall-first con anclas en el MISMO campo:** `(obj LIKE %a1% AND obj LIKE %a2%) OR (desc …)`;
  substring (cubre singular/plural), subsume la frase exacta sin rama redundante. Nunca cruza
  objeto con descripción.
- **`candidate_limit` (200) ≠ `display_limit` (25):** el pool de fetch se amplía solo con frase;
  el display recorta aguas abajo, jamás se filtra al builder.
- **Validación local tolerante** (`phrase_validator`): normaliza (tildes/`\n`/puntuación) y
  clasifica `direct/probable/weak/none` con gate de mismo-campo y orden.
- **Ranking formal ANTES del render** (`rank_results`, acción del grafo): orden = nivel
  (direct>probable>weak) → BM25 **dentro** del nivel → orden original. `rendering.py` solo
  presenta y corta a 25.
- **`none` en cuarentena**, no borrado (auditable; falso negativo no desaparece).
- **Conceptos nuevos = una entrada de datos** (`PhraseSpec`), cero `if` especial (probado con
  "aire acondicionado" / SIP-049, live 25/25 direct).
- **Header determinístico** (no LLM) que distingue total_count / candidate_count / validated_count.

**Alternativas descartadas:** (a) Solo `LIKE` de frase exacta — pierde "manual interno de
contratación" y los rotos por `\n`. (b) Tokenizar y unir con AND — el bug original. (c) Arreglarlo
solo con morfología (`morphological_variants`) — eso es forma superficial, no detección de unidad;
además ese archivo ya mezcla mal morfología + semántica de dominio. (d) Mejorar el ranking antes
que el recall — inútil: el ranking no rescata filas ausentes. (e) LLM como solución principal —
A/B con juez mostró que no sube precisión; +4s latencia. (f) Borrar definitivamente los `none` —
perdía 4 positivos en silencio. (g) Un solo `limit` para consulta y pantalla — `LIMIT 50` mataba
52 positivos.

**Consecuencias:** Positivas: recall 5/100→100/100 (pool), validación 96/100, Precision@25
52%→100% (dev); arquitectura **reutilizable** (un concepto = un PhraseSpec); explicación auditable;
**punto formal único** para futuros rerankers (BGE/spaCy) que SÍ llegarán a pantalla. Costos: pool
descargado mayor (200 vs 50); validación local por fila; **4% de falsos negativos v1** (4 positivos
en cuarentena, congelados para corpus v2, NO se tunean mirando holdout v1); conceptos con jerga/
siglas (PAE, POT) requieren enriquecimiento curado (aliases/exclusiones), fuera de PhraseSpec;
el pool de 200 puede saturarse en conceptos de baja selectividad; métricas deben reportarse
**separadas por etapa** (entrar al pool ≠ sobrevivir validación ≠ aparecer en página 1).

**Invariantes que no deben romperse** (este ADR es la protección contra futuras "simplificaciones"
que vuelvan a poner `LIMIT 50` o el ranking dentro del formatter): `candidate_count = validated +
unvalidated`; `validated = direct + probable + weak`; `displayed ≤ validated`; `candidate_limit ≠
display_limit`; el ranking ocurre ANTES del formatting; anclas solo en el mismo campo; `none` no
significa irrelevante definitivo.

**Guard de precedencia (no ∅):** `phrase_catalog` es dueño de sus frases por orden de ejecución
(detecta y consume antes que `KNOWN_BIGRAMS`, y el `WHERE` consume `objeto_phrases`). El guard
pasó de "no solapar" a "el catálogo detecta y consume" — permite migrar frases legacy.

**Tests:** 11 archivos nuevos (`test_phrase_spec`, `_retrieval_text`, `_objeto_phrases`,
`_phrase_validator`, `_soql_builder_phrase`, `_candidate_limit_wiring`, `_phrase_ranking`,
`_rank_results`, `_phrase_catalog_collisions`, `_sip049_aire_acondicionado`, `_phrase_presentation`)
+ benchmark offline reproducible (`scripts/exp_phrase_retrieval.py`, corpus silver con split
dev/holdout). Suite 1714 passed. Informe completo: `docs/SEMANTIC_RETRIEVAL_SPRINT_2026-06.md`.
Para extender: `docs/PHRASE_RETRIEVAL_RUNBOOK.md`.

## ADR-037: Los parámetros explícitos del turno prevalecen sobre estado/framing heredado (FRAME-LEAK-ESTADO)

**Fecha:** 2026-06-26 · **Estado:** Accepted

> Numeración: rescatado del prototipo paralelo `claude/trusting-heisenberg` (que lo numeró
> ADR-036, en colisión con el ADR-036 phrase-aware ya mergeado y tagueado en main). Se
> renumera **ADR-037** y se reescribe para documentar SOLO lo que se portó. La rama prototipo
> NO se fusionó; se usó como documentación ejecutable (su canary + algoritmo). El phrase-first
> alternativo de esa rama se DESCARTÓ por redundante con `semantic-retrieval-v1` (ADR-036).

**Contexto:** En un follow-up que pivota el estado ("procesos abiertos…" → "y los cerrados?"),
el filtro anterior sobrevivía por la puerta de atrás. Doble causa: (1) el estado se representa
con **varias claves hermanas** (`estado`, `estado_field`, `estado_family`,
`estado_contrato`, `estado_de_apertura_del_proceso`, `estado_del_procedimiento`); el turno nuevo
trae solo un subconjunto, y el `{**prev, **curr}` ciego dejaba en pie las claves viejas, que
ganan por precedencia en el builder. (2) El framing `intent_type=opportunity_search` heredado
sobrevivía y la invariante de oportunidad aguas abajo **re-imponía** `oferta_abierta`, purgando
el estado pedido y devolviendo en silencio a "abiertos". Es la "Causa 2" (merge ciego del frame)
del documento técnico y el **Paso 1 (sellar fuga del frame)** del roadmap. El sello previo en
main (`_merge_raw` con expiración de `_RAW_CLASSIFICATION_KEYS` al cambiar `intent_type`) era
**parcial**: no trataba el estado como cluster ni soltaba el framing de oportunidad.

**Decisión:** **Los parámetros expresados en el turno actual prevalecen sobre clasificaciones,
familias y filtros heredados incompatibles** — por clusters atómicos, no por borrado
indiscriminado. Dos mecanismos en `followup_engine.py`:
1. **`_merge_modifiers`** (cluster atómico de estado): si el turno actual trae CUALQUIER clave de
   `_ESTADO_KEYS`, el estado heredado se reemplaza por completo (las hermanas obsoletas no
   coexisten). El resto de modifiers conserva el merge legacy. Aplicado en las políticas
   `refine_filter`, `change_year`, `change_scope`, `change_dataset`, `contextual_requery`.
2. **Sello en `detect_and_merge`**: si el turno fija un estado explícito que NO es
   `estado_family=oferta_abierta`, se sueltan `intent_type=opportunity_search`, `estado_family`,
   `opportunity_mode`, `filter_active_only` del merge.
Se conservan tema, entidad, lugar, fechas y valores compatibles (el cluster solo toca estado).

**Alternativas descartadas:** Borrado indiscriminado de todo lo heredado al pivotar (rompería la
continuidad legítima: cambiar año no debe borrar tema ni estado). Resolver solo en el builder
(el frame ya llega contaminado; hay que sellarlo en el merge). Fusionar la rama prototipo entera
(traería un segundo motor de phrase-first).

**Consecuencias:** Correctitud conversacional: "y los cerrados?" ya no revierte a abiertos;
ningún pivote (entidad/ciudad/fecha/dataset/estado) fuga el valor anterior. Es invariante
transversal (afecta cualquier búsqueda, no solo dual-dataset) → cimiento para construir
dual-dataset sobre un merge confiable. Deuda residual: las uniones en `query_frame.py` y la
paginación siguen siendo merge legacy; cubiertas por el canary como deuda observable, no refactor
preventivo.

**Tests:** `tests/test_frame_leak_canary.py` (5 vectores: entidad/ciudad/fecha/dataset/estado;
el de estado sella esta decisión). Suite 1719 passed. Sin cambios a phrase-retrieval/ranking/
dual-dataset.
