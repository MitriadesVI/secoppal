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

   > **Este punto 1 quedó SUPERCEDIDO PARCIALMENTE POR ADR-071 (2026-08-31).** Se
   > conserva la redacción original de mayo de 2026. El comportamiento —no
   > construir la URL desde un id `CO1.REQ.*`— sigue en pie, pero su causa era
   > falsa: el censo de `p6dx-8zbt` muestra que el 100% de las filas lleva ese
   > prefijo y que el 98,97% sí tiene vista pública. El motivo demostrado es otro:
   > ninguna fila usa su propio id como `noticeUID`. La nota de UI que nombraba
   > una etapa ya no existe; la presentación vigente es
   > `app/core/secop_access.py`.

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
**Estado:** Accepted — **SUPERCEDIDO PARCIALMENTE POR ADR-071 (2026-08-31)**

> **Léase con esta advertencia.** Lo que sigue conserva el contexto tal como se
> entendía en junio de 2026, sin reescribirlo con lo que se midió en agosto.
> Dos partes envejecieron de forma distinta:
>
> - **Sigue vigente la DECISIÓN**: no omitir en silencio una fila sin enlace, y
>   mantener la política de URL centralizada con paridad entre canales.
> - **Queda REVOCADA la INTERPRETACIÓN** "`CO1.REQ.*` = requisición
>   pre-publicación / ESAL sin link consultable". El censo completo de
>   `p6dx-8zbt` la falsifica: los 9.096.877 procesos llevan ese prefijo —el
>   100%— y el 98,97% trae vista pública.
>
> La autoridad vigente sobre la navegación a SECOP es **ADR-071** y
> `app/core/secop_access.py`. La nota de UI que este ADR igualó entre canales
> ("Vista pública no disponible (proceso pre-publicación)") ya no existe: nombraba
> una etapa que los datos no sostienen y fue reemplazada por una acción —el
> buscador público oficial con la referencia observada—. El detalle de la
> corrección está al final de esta entrada.

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
  produce un enlace roto. La razón original ("da error en SECOP") NO se pudo
  reverificar en 2026-08-31 —community.secop.gov.co responde 403/reCAPTCHA a
  cliente no-navegador— pero la conclusión se sostiene sobre evidencia más
  fuerte: ninguna fila del dataset usa su propio id como `noticeUID`
  (0 de 9.096.877). Mostrar un link roto sigue siendo peor que no mostrarlo.
- *Filtrar/ocultar los procesos sin URL*: pierden visibilidad oportunidades
  reales; el usuario debe verlos aunque no tengan ficha pública. La
  justificación original citaba "varios ESAL 'Abierto' de alto valor"; esa
  atribución se ELIMINA por venir de la premisa falsificada y no de una medición
  independiente. El censo del 2026-08-31 la contradice además de frente: la tasa
  de ausencia en `Contratación régimen especial` es 34.163/3.563.571 = 0,96%,
  indistinguible de la tasa base del dataset (1,03%). La decisión no dependía de
  ese dato y no cambia.
- *Resolver la nota por canal con texto distinto en cada formatter*: rompería la
  paridad; se reusó la redacción exacta de Streamlit.

**Tests:**
- `tests/test_url_fallback_channel_parity.py`. Reescrito en 2026-08-31: el
  PRINCIPIO de este ADR (los tres canales dicen lo mismo; ninguno omite en
  silencio) se conserva y se refuerza —ahora hay un test que exige la misma
  decisión en los tres para la misma fila—, pero lo señalizado dejó de ser una
  nota que nombraba una etapa y pasó a ser una acción: el buscador público
  oficial con la referencia observada.

**Consecuencias:** Los tres canales explican por qué un proceso no tiene link, en
vez de dejar un hueco confuso. Útil para Radar (futuro): un proceso pre-publicación
es señal de oportunidad temprana y ahora el sistema lo distingue explícitamente.

> **Corrección posterior (2026-08-31, ADR-071 / SECOP Access V2).** La DECISIÓN de
> este ADR (señalizar en vez de omitir; política de URL centralizada) sigue en
> pie. Su CAUSA no: "`CO1.REQ.*` = requisición pre-publicación sin link" quedó
> falsificada. Censo completo de `p6dx-8zbt`: los 9.096.877 procesos llevan id
> `CO1.REQ.*` —el 100%— y el 98,97% trae vista pública entregada por SECOP; el
> predictor real de la ausencia es `estado_del_procedimiento='Borrador'` (98,3%
> sin vista), no el prefijo ni el régimen especial (0,96%, la tasa base). Lo que
> sí quedó demostrado es que ninguna fila usa su propio id como `noticeUID`
> (0 de 9.096.877), así que el guard que impide construir la URL se conserva
> por ESE motivo. La redacción pública ya no infiere etapa (ADR-032 #4).
> Evidencia: `docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md`.
> Autoridad vigente: `docs/decisions/2026-08-31_ADR-071_secop-access-authority.md`.
>
> **Deuda aceptada: `TRUTH_LAYER_SEARCH_URL_CLASSIFICATION_DEBT`.** SECOP
> publica el buscador público de avisos
> (`/Public/Tendering/ContractNoticeManagement/Index?...&Page=login&...`) en
> `urlproceso` para los 4.130 contratos de `jbjy-vk9h` sin ficha pública. El
> `_classify_url` de `truth_layer/visibility_resolver.py` solo busca `/login` en
> el PATH, así que clasifica esa pantalla como `public` y la fila resuelve a
> `public_view_available` —una vista pública que no muestra ningún contrato—.
> NO se corrige aquí y la decisión es deliberada: el contrato semántico de T1B
> está congelado por sus documentos vinculantes, corregirlo cambiaría un
> veredicto de evidencia (no una etiqueta de UI) y merece su propia
> adjudicación. Impacto en runtime de SECOP Access V2: **ninguno**. Las cuatro
> superficies web y los tres canales de texto deciden con
> `app/core/secop_access.py`, que rechaza esa página por su ruta y no por el
> parámetro `Page=login`; ninguna de ellas consulta el Truth Layer para decidir
> la vía a SECOP. Regresión que lo fija: `tests/test_secop_access.py`
> (`test_the_contract_management_page_never_becomes_a_direct_link`) y
> `tests/test_url_fallback_channel_parity.py`
> (`test_the_contract_search_page_is_never_presented_as_a_process_view`).

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

## ADR-038: Ejecución agrupada sobre ambos datasets (procesos + contratos)

**Fecha:** 2026-06-26 · **Estado:** Accepted

> Rescatado del prototipo `claude/trusting-heisenberg` (ver ADR-037 para el contexto de la
> colisión de ADRs). Solo se portó dual-dataset; su phrase-first se descartó por redundante con
> `semantic-retrieval-v1` (ADR-036). Reimplementado sobre la arquitectura actual de main, no
> fusionado. Construido en 4 commits por etapas (`4f15a04` parsing, `7e2da30` ejecución,
> `c2b6086` presentación, `542ac72` conversación).

**Contexto:** "procesos **o** contratos de automatización" colapsaba a UN solo dataset (contratos)
y perdía la mitad del pedido — el "o" no existía en el modelo. Procesos (`p6dx-8zbt`) y contratos
(`jbjy-vk9h`) son recursos de Socrata SEPARADOS, con esquemas distintos (estados, fechas, campos):
no se pueden `UNION` en un SoQL, y son **momentos jurídicos distintos** (un proceso es una
convocatoria; un contrato es una adjudicación) que no deben mezclarse en un ranking único.

**Decisión:** **Una consulta explícita sobre ambos datasets se ejecuta como dos búsquedas
HERMANAS, con validación, ranking, conteos y presentación independientes.**
- **Parsing:** una conjunción/cobertura ("procesos o contratos", "ambos") marca
  `dataset_scope=["procesos","contratos"]`; `dataset` sigue escalar (primario=procesos) por
  compatibilidad. `dataset_scope` es señal de orquestación, NO llega al `SoQLBuilder`.
- **Ejecución:** `rank_candidate_pool` se extrajo como núcleo de ranking ÚNICO (un algoritmo, dos
  consumidores: la acción `rank_results` para el primario y `execute_dataset_child` para el
  secundario). `params_for_dataset` purga los campos de estado exclusivos del otro esquema (dual
  frame-leak). El primario sale del estado del pipeline (NO se re-ejecuta); el secundario corre por
  un helper PURO (sin persistir, sin follow-up, sin formatear, sin recursión del workflow).
- **Presentación:** `Formatter.format_grouped` expresa cada grupo por separado con los campos de SU
  dataset; SIN suma global de conteos; sugerencias una sola vez. `empty` ≠ `error`; fallo parcial
  conserva el grupo exitoso.
- **Conversación:** `dataset_scope` se hereda en follow-ups compatibles (año/orden conservan
  "ambos"); un turno con dataset explícito lo suelta ("solo procesos"/"ahora contratos"), con
  **dual-collapse** para heredar el tema; "más" tras dual pregunta cuál grupo.

**Alternativas descartadas:** Convertir `dataset` en lista en todo el sistema (invasivo, rompería
el builder y mil sitios). Mezclar filas en un único ranking (procesos y contratos no son
comparables). Ejecutar recursivamente el workflow completo para el secundario (doble persistencia/
follow-up/observer, traces incoherentes). Tratar "ambos" como contratos y sugerir cambiar a procesos
(el bug original). Fallar toda la respuesta si un solo dataset hace timeout (all-or-nothing).

**Consecuencias:** El usuario por fin puede pedir ambos universos en un turno. Cada grupo tiene sus
conteos honestos (entran ≠ validan ≠ muestran), sin sumar universos heterogéneos. Costo: una
consulta dual hace 2 fetches (hasta ~400 filas con `candidate_limit=200` por hijo) — NO reducir por
intuición; medir `latencia_*`/`timeout` por grupo primero. Deuda: paginación real por grupo ("más
procesos"/"más contratos") es follow-on; hoy "más" solo pregunta cuál. La herencia de `dataset_scope`
es legacy-raw; cubierta por el seal inverso + dual-collapse + tests, no refactor del frame.

**Tests:** `test_dual_dataset_scope` (parsing/round-trip/herencia), `test_dual_dataset_execution`
(ejecución/sanitización/fallo parcial/conteos/cero pérdida), `test_dual_dataset_presentation`
(4 casos de salida + cero re-ejecución del primario), `test_dual_dataset_conversation` (collapse/
herencia/más). Suite 1744 passed. Cero cambios a `semantic-retrieval-v1`.

## ADR-039: Latencia acotada por un deadline de turno + recuperación degradada (Sprint A / Etapa 6)

**Fecha:** 2026-06-29 · **Estado:** Accepted

> Sprint A de remediación (rama `sprint-a-dogfood-remediation`). Plan en
> `docs/planes/PLAN_DOGFOOD_REMEDIACION_2026-06-27.md`; perfiles en
> `docs/PERFIL_LATENCIA_2026-06-28.md`; diagnóstico de fallback en
> `docs/H4_0_TAXONOMIA_TIMEOUTS_2026-06-28.md`. Hipótesis H3/H5 DEFERRED (evidencia no las
> justifica). Construido por etapas: H1 `bea06ff`, H2 `3dd647d`, H2.1 `9665971`, H4 `3a2b3ad`+
> `be8fb37`, direct_lookup `6a3792d`.

**Contexto:** la cola de latencia (p90 45–60s, max 120s, ~25–31% de turnos >15s) NO era
comprensión lenta sino **operaciones externas que secuestraban el turno entero**: el `count`
bloqueaba 22–60s, y un `fetch`/`query` con `ORDER BY` costoso sobre un universo amplio hacía
timeout de 60s. El p50 (~7.5s) era sano: el daño estaba en la cola.

**Decisión:** **un turno tiene un presupuesto monotónico compartido (15s) y NINGUNA llamada
externa puede exceder el tiempo restante menos una reserva de cierre.** Propagado por
`ContextVar` (sin cambiar firmas), reloj `perf_counter` inyectable para tests.
- **H1 — count no bloqueante:** las filas mandan; el `count` solo corre con presupuesto
  restante y nunca fabrica 0/EMPTY (filas + count fallido → `PARTIAL`/`COUNT_TIMEOUT`, total "X+").
- **H2 — fetch acotado:** `fetch_budget = min(timeout_fuente, restante − reserva)`; si no hay
  presupuesto útil, la llamada NO se inicia (corte honesto, no se cuelga).
- **H2.1 — seguridad concurrente:** el singleton `SecopClient` (`@lru_cache`) bajo ThreadPool no
  se muta in-place; `derived_for_timeout` devuelve un cliente derivado por llamada (preserva mocks
  de instancia, no abre transporte real en tests).
- **H4 — recuperación degradada sin ORDER BY:** ante timeout RECUPERABLE y orden PRESENTACIONAL,
  UN fallback que retira el `ORDER BY` costoso (LIMIT≤50) y reordena la página localmente; reparto
  **8/3/4** (principal/fallback/reserva). Si el orden es SEMÁNTICO ("más caros", "últimos" →
  `ordering_signal`) NO se degrada (no afirmar ranking sobre una muestra). Orden y conteo son ejes
  independientes (`result_order_guarantee` ∈ exact_order/unordered_sample/unavailable). La respuesta
  visible no afirma orden sobre una muestra — **dos generadores** (`formatter` Y
  `response_policy.build_advisor_response`) debieron parchearse.
- **direct_lookup:** el path de búsqueda por referencia probaba ~12 variantes en serie SIN deadline
  (un turno llegó a 67s); ahora cada probe se acota al presupuesto y la cadena serial se corta
  honestamente al agotarlo.

**Alternativas descartadas:** timeout por llamada (no acota la SUMA de un turno con múltiples
fetches); subir el techo a 18–20s (reabre la cola); relajar filtros sustantivos en el fallback
(`_relax_params` — responde otra pregunta); forzar `total` aproximado en el fallback (el conteo es
independiente del orden); LLM-first (ADR previo: el extractor no es la palanca).

**Consecuencias:** p90/max 60s→11.6s/15.5s, turnos>15s ~28%→1 (explicado: turno con fallback apila
primary+fallback+count), 0 falsos EMPTY, 0 rankings falsos, recall de referencias intacto.
inconclusas sin filas 12→4–7 (recuperación real pero VARIABLE con la velocidad de Socrata).
El único turno >15s y la variabilidad del recall quedan como observaciones honestas, no como cierre
falso. Guía operativa: `docs/LATENCY_DEADLINE_RUNBOOK.md`.

## ADR-040: Resolución geo TIPADA — territorio vs comprador (Etapa 7)

**Fecha:** 2026-06-29 · **Estado:** Accepted · commits `85a4fa9`+`f07cd57`

**Contexto:** el gazetteer COLAPSABA territorio y comprador: "del departamento del Atlántico"
(territorio = todas las entidades del depto) y "de la Gobernación del Atlántico" (comprador = la
gobernación) resolvían ambos a `entidad_resolved='DEPARTAMENTO DEL ATLANTICO**'` → mismo WHERE.
La causa: frases territoriales figuran como ALIAS del comprador en `aliases_db.json`.

**Decisión:** la resolución lleva un **`kind` semántico** y patrones sintácticos mapean a campos
distintos. "departamento de X" (con X = departamento reconocido) → `kind=territorial_department`
→ campo `departamento`; toda entidad resuelta → `kind=buyer_entity` → campo `entidad`/
`nombre_entidad`. Un paso sintáctico (query_router 4.55) intercepta "departamento de X" ANTES del
scan de entidades y lo enruta a territorio, consumiendo el span. **INV-7: "Gobernación de X" ≠
"departamento de X" → WHERE distintos.**

**Hallazgo de datos (manda el dato, no la expectativa):** SECOP nombra las gobernaciones de forma
inconsistente — Atlántico se registra como `DEPARTAMENTO DEL ATLANTICO**`, Nariño como
`GOBERNACION DE NARIÑO`. El invariante correcto NO es "el valor contiene GOBERNACION" (la aserción
vieja del fixture era falsa para Atlántico → corregida) sino que comprador y territorio generan
campos/WHERE distintos. El comprador resuelve a su valor REAL en SECOP aunque difiera de la frase
humana.

**Alternativas descartadas:** forzar que "Gobernación de X" contenga "GOBERNACION" (no matchea el
comprador real de Atlántico → pierde resultados); limpiar aliases territoriales del gazetteer (559
entradas, riesgo amplio). **Fuera de alcance (anotado):** "Departamento de Policía del Atlántico"→
territorial es PREEXISTENTE (scan de departamento, cobertura de gazetteer), no del patrón E7.

## ADR-041: Paginación canónica dividida en E8A (hecha) y E8B (pendiente)

**Fecha:** 2026-06-29 · **Estado:** Accepted (proceso) · E8A `54710e7`, spec E8B `a4a3dda`

**Contexto:** el OFFSET de Socrata ≠ offset de los resultados validados/rankeados; y la página 1
(`normal_flow`→`rank_results`+validación de frases) y "más" (`pagination`→`rank_and_dedupe`) NO
pasan por la MISMA ruta de ranking → con PhraseSpec/exclusiones la página 2 puede tener distinta
calidad. El rework completo es stateful (estado conversacional, TTL, memoria).

**Decisión:** **dividir Etapa 8.** *E8A (hecha):* orden fuente estable con desempate por id
(`ORDER BY … , id DESC`) + fingerprint guard (no continuar una paginación bajo consulta/contrato
incompatible; `query_fingerprint` = `sha256(canonical_json)`, versionable vía `extra`). *E8B
(pendiente, spec en `docs/ESPEC_ETAPA8B_BUFFER_CANONICO.md`):* buffer canónico
(candidate/validated_ranked) + cursores display/source + dedup + TTL + 3 totales separados. **No se
declara Etapa 8 cerrada hasta E8B.**

**Precisión de diseño para E8B (registrada para no perderla):** el buffer es la CONSECUENCIA, no el
arreglo — primero **unificar** la ruta de validación/ranking en `handle_pagination` (la misma de
página 1), recién luego CACHEARLA; el gate es **P1≡PN** (la concatenación de páginas == la
secuencia validada completa, no solo "no se repiten"); dual = buffer por `(fingerprint, grupo)`.

**Por qué dividir:** una etapa stateful con varios invariantes simultáneos no se implementa al final
de una sesión larga; un error produce síntomas sutiles (duplicados, filas faltantes, páginas de otra
consulta, totales contradictorios), peores que un crash. Mejor base estable + frontera clara que
medio buffer.

## ADR-051: Contrato de entrega de notificaciones del Radar (P1B-0)

**Fecha:** 2026-07-15  
**Estado:** Proposed — número provisional  
**Rama:** `radar/mvp-manual` · **HEAD de diseño:** `02ffcfd`

> Antes de integrar, verificar `ADR-051` contra todas las refs remotas. Las
> colisiones históricas de numeración se resolverán mediante un detector
> automático separado; no se reescriben ADR anteriores en este bloque.

> **Nota de integración (2026-07-26).** La verificación de numeración pedida
> arriba se ejecutó: `ADR-051` no aparece en `main` ni en ninguna rama local, de
> modo que el número deja de ser provisional por colisión. El bloque se
> incorpora a `main` con su texto original, sin reescribir la decisión. Hasta
> esta fecha existió únicamente como cambio sin commitear en el worktree de
> `radar/mvp-manual`, tal como registra
> `docs/radar/2026-07-26_RADAR_TECHNICAL_INTEGRATION_CLOSEOUT.md`.

### Contexto

P0/P1A corrigieron verdad del diff, refresh de vistos, validación canónica,
outcomes/backoff y exclusión mutua de `run_radar`. La auditoría P1B-0 comprobó
que aún no existe un subsistema de notificación del Radar y que conectarlo al
retorno en memoria del scheduler sería incorrecto:

- `radars.owner` no es una identidad canónica y puede caer en un `"anon"`
  compartido;
- `radar_events` no tiene cursor monotónico público y todos los eventos de una
  corrida comparten `ts`;
- `seen`, `runs`, `events` y estado operativo se materializan hoy en conexiones
  autocommit separadas, por lo que un crash puede perder una novedad;
- `follows.refresh_follows` consulta y escribe fuera del lease de P1A;
- evento, intención de entrega e intento de proveedor tienen ciclos de vida y
  riesgos diferentes;
- el borde de envío existente colapsa timeout, 400, 429 y 500 en un booleano.

### Decisión

1. **Verdad detectada, materializada y entregada son fronteras distintas.** La
   materialización de una corrida se vuelve transaccional antes de construir
   outbox: tras terminar toda red, `BEGIN IMMEDIATE`, renovación/fencing del
   lease como primera operación DML, `run + seen + events + estado`, `COMMIT`.
   Si el token no sigue vigente o cualquier paso falla, `ROLLBACK` total.

2. **Tres preflights son obligatorios antes del outbox:**
   - Preflight 1: materialización atómica y fault injection;
   - Preflight 2: identidad/lease/idempotencia de `follows` y ciclo durable de
     refresh;
   - Preflight 3: `needs_attention` solo se limpia mediante acción explícita;
     una corrida OK no reactiva ni borra la necesidad de revisión.

3. **Identidad canónica ≠ canal.** Se crean `principals`,
   `principal_owner_keys`, `notification_endpoints` y preferencias por
   `(radar_id, endpoint_id)`. Un principal solo nace de una `app_account`
   existente y activa. Un owner anónimo/no vinculado no puede tener endpoints.
   La creación de preferencia exige que el principal resuelto del radar coincida
   con `endpoint.principal_id`; el DDL protege también batch↔endpoint con FK
   compuesta `(endpoint_id, principal_id)`.

4. **Verificación no es un campo editable.** Crear o editar un endpoint deja
   `verified_at=NULL`. Solo un `ChannelVerifier`, después de prueba de posesión,
   puede verificarlo. P1B-1 implementa estado pendiente y restricciones; P1B-5
   implementa el challenge real de Telegram.

5. **Evento ≠ entrega.** Se separan seis responsabilidades: principal,
   endpoint, preferencia, batch/outbox, asociación batch-evento e intento. Todo
   mensaje se remonta a eventos concretos y ningún error almacenado contiene
   secretos o direcciones.

6. **Cursor explícito por `event_seq`.** `ts` solo es inseguro porque una corrida
   comparte timestamp; `(ts,id)` sería técnicamente válido, pero se adopta
   `event_seq INTEGER` por legibilidad y consumo incremental. La reserva de
   secuencia y la inserción del evento ocurren en la misma conexión y dentro de
   `materialize_run`. Eventos previos a la migración permanecen `NULL` y no se
   usan como backfill.

7. **El cursor es un watermark de examen.** `last_examined_seq` avanza al máximo
   evento examinado únicamente en la misma transacción que crea batches e
   inserta `batch_events`. Una ventana sin elegibles también avanza. Si algo
   falla, cursor y outbox revierten juntos. `max_items` limita el render, no la
   trazabilidad: todos los eventos representados quedan asociados.

8. **Opt-in sin backfill.** La transición de preferencia `0→1` —y cualquier
   reactivación posterior— fija `last_examined_seq = MAX(event_seq)` y habilita
   la preferencia en la misma transacción. Los eventos del periodo previo o
   silenciado no se envían por defecto; batches `dead/opted_out` no reviven.

9. **Frecuencia MVP: solo `per_run`.** Un batch por endpoint, radar y
   `source_cycle_id`, con todos sus eventos elegibles. Para un radar ordinario
   el ciclo es `radar_run_id`; Preflight 2 define el equivalente durable de
   `follows`. `daily` y `weekly` se rechazan hasta contar con contrato propio.

10. **Idempotencia basada en el conjunto exacto.** La clave es:

    `sha256(policy_version | endpoint_id | radar_id | event_seq únicos ordenados)`.

    No depende de texto, `digest_hash` ni rango `min/max`. El mismo evento no
    puede aparecer en dos batches del mismo endpoint:
    `UNIQUE(endpoint_id,event_id)`, además de la unicidad dentro del batch.

11. **Claim recuperable y fenced.** El worker reclama con un único `UPDATE` que
    admite `pending` o `claimed` vencido, respeta `not_before` y decide por
    `rowcount`:

    ```sql
    UPDATE radar_notification_batches
       SET status='claimed', claim_token=:token, claim_until=:until
     WHERE notification_id=:id
       AND (
            status='pending'
            OR (status='claimed' AND claim_until<=:now)
       )
       AND (not_before IS NULL OR not_before<=:now)
    ```

    Un claim vencido sin intento `dispatching` puede continuar. Si existe un
    `dispatching` huérfano, se cierra a `finished/ambiguous`, el batch queda
    `ambiguous` y no se reintenta automáticamente.

12. **Attempt write-ahead.** Tras claim y revalidación, se inserta
    `DeliveryAttempt(phase='dispatching', outcome=NULL, claim_token=...)` y se
    hace `COMMIT` antes de llamar al proveedor. Solo después se finalizan attempt
    y batch. Todo intento `finished` tiene outcome; un `dispatching` abandonado
    se reconcilia a `finished/ambiguous`.

13. **Fencing al finalizar.** `finalize_attempt` comprueba en una transacción que
    el batch sigue `claimed`, conserva el mismo token y no ha vencido. Un worker
    viejo no puede marcar `sent` después de que un sucesor haya reconciliado el
    batch a `ambiguous`.

14. **Semántica effectively-once, no exactly-once.** La idempotencia local evita
    duplicar decisiones propias, pero no puede hacer commit atómico con el
    proveedor. En las fuentes oficiales revisadas el 2026-07-15 no se encontró
    una clave de idempotencia documentada para los endpoints salientes
    considerados, ni un endpoint de reconciliación en esas referencias; no se
    verificó exhaustivamente su ausencia y debe revalidarse antes de P1B-5.
    `ambiguous` es outcome de primera clase y no se reintenta solo.

15. **Elegibilidad estricta.** Solo `new`, `changed_state`, `changed_value` y
    `disappeared` confirmado son elegibles. `baseline`, `followed`,
    `window_churn`, `radar_empty`, outcomes de error y estados de control nunca
    producen push comercial. `needs_attention` y `next_run_after` no invalidan
    eventos ya materializados; sí frenan opt-out, endpoint inválido/revocado,
    principal suspendido, radar archivado o batch vencido.

16. **Radares se archivan y restauran.** `archive_radar`/`restore_radar` usan
    `archived_at`; no hay `DELETE` destructivo. El nombre permanece reservado
    por `UNIQUE(owner,name)` mientras esté archivado y la historia sobrevive.

17. **Payload congelado, texto renderizado al enviar.** El batch conserva datos
    y asociaciones; el formatter del canal produce un único mensaje. El exceso
    se resume con `truncated_count`; no se trocea un batch en varios mensajes.

18. **Privacidad y trazabilidad.** Tokens quedan fuera de tablas; endpoints se
    minimizan; logs usan IDs enmascarados; `error_message_safe` no contiene
    secretos, emails, teléfonos ni payload crudo. Revocación/archivo preservan
    ledger histórico; retención y anonimización son explícitas.

19. **Shadow-first.** Outbox y worker se prueban con fake provider y fault
    injection, luego shadow sin red y finalmente Telegram detrás de flag
    `False` por defecto. No hay activación automática.

### Alternativas descartadas

- notificar directamente desde `SchedulerSummary` en RAM;
- una tabla monolítica `owner/channel/destination/preference`;
- idempotencia por texto, `digest_hash` o `min_seq/max_seq`;
- cursor solo por timestamp o `rowid`;
- marcar `sent` antes de enviar;
- reintentar automáticamente un estado ambiguo;
- usar `needs_attention` o `next_run_after` para invalidar eventos históricos;
- borrar radares, endpoints o principals con historia;
- prometer exactly-once sin capacidad externa demostrada;
- declarar `daily/weekly` antes de definir sus ventanas y concurrencia.

### Consecuencias

El roadmap queda: Preflight 1 → Preflight 2 → Preflight 3 → P1B-1 identidad,
endpoints y preferencias → P1B-2 outbox/ledger → P1B-3 selector y render shadow
→ P1B-4 worker fake/fault injection → P1B-5 Telegram deshabilitado → P1B-6
shadow y activación controlada.

**Detalle:** contrato en `docs/radar/P1_NOTIFICATION_AUDIT_ADDENDUM.md` y plan en
`docs/planes/radar-p1b-notificaciones.md`. Sustituye E5–E7 del plan post-P0, que
permanece como registro histórico.

## ADR-054: Contrato conversacional de modalidad con autoridad determinística (M3)

**Fecha:** 2026-07-26  
**Estado:** Accepted  
**Rama de integración:** `integration/modality-contract-v1-main` · **HEAD:** `c5f857e`

### Contexto

El resolutor de modalidad (M2) resolvía el turno inicial, pero el segundo turno
no tenía semántica: `además contratación directa`, `quita contratación directa`
y `permite contratación directa` son tres operaciones distintas sobre el mismo
filtro previo y ninguna estaba nombrada. Además, las señales de control se
confundían con lenguaje descriptivo (`contratación directa que incluye
suministro`) y jurídico (el `sin` interno de un nombre de modalidad); el LLM
podía reescribir parámetros estructurales que el pipeline determinístico ya
había resuelto; y una mutación parcial podía contaminar dataset, estados, topic
y persistencia, ejecutando SoQL sobre un estado a medias.

### Decisión

M2 resuelve modalidad exactamente una vez por turno y sus `consumed_spans`
vinculan el control con targets reales. Las operaciones conversacionales forman
un enum cerrado —`REPLACE`, `ADD_INCLUDE`, `ADD_EXCLUDE`, `REMOVE_MEMBER`,
`REMOVE_EXCLUDE`, `RESTORE_INCLUDE`, `CLEAR`— aplicado por un motor puro sobre
IDs canónicos. `RESTORE_INCLUDE` en universo abierto elimina la exclusión sin
crear whitelist. Status M2 bloqueado, target ausente o conflicto de control
producen rollback completo, cero SoQL y cero ejecución: no hay mutación parcial.
Solo el builder proyecta procedure IDs a raws del dataset final. El LLM opera
bajo allowlist monotónica (solo llena huecos permitidos aún vacíos: una
representación de topic, entidad, ciudad o departamento) y tiene cerrados
dataset, modalidad, estados, monto, fecha, orden, intent y todo campo futuro.
La metadata de control es efímera y no entra a frame, fingerprint, persistencia
ni SoQL. Las construcciones cerradas pueden enmascarar la vista que ve M2 sin
alterar el texto original ni sus offsets. La telemetría es privada, agregada,
opt-in y fail-open. **El sidecar de aclaración (M3C-D) queda DEFERRED** hasta
que la compuerta se cumpla con muestra real (`sample_is_real=true`,
`turns_total >= 100`, y además `blocked_total >= 10` o bien
`blocked_by_m2_status_total >= 5` con ratio `>= 0.05`); `target_missing` y
`control_conflict` no cuentan como bloqueo jurídico M2.

### Consecuencias

Comportamiento determinístico y auditable, composición segura con topic, scope,
monto, fecha y dataset, y campos nuevos cerrados por defecto para el LLM. El
costo es mayor superficie de tests, un vocabulario de control estrictamente
gobernado, casos ambiguos (`sin modalidad`, `agrega y quita X`) que exigen
aclaración explícita, y una telemetría que debe acumular uso real antes de que
M3C-D pueda evaluarse.

Alternativas descartadas: dejar que el LLM reescriba todos los params; proteger
campos por lista negra creciente; reejecutar M2 después del merge; inferir el
target jurídico desde el lenguaje de control; construir ya el sidecar de
aclaración; aplicar mutaciones parciales cuando un target queda bloqueado.

### Métricas

```text
lint core           app/core/ limpio
focal modalidad     526 passed, 1 xfailed
suite completa      5381 passed, 2 skipped, 24 xfailed
query corpus        89/89 PASS
dialogue corpus     60/61 PASS
dialogue E2E        29/29 secuencias PASS
```

Fallo basal aceptado, ajeno a modalidad y con firma idéntica al baseline:
`GOB-DEPTO-001` (`entidad_resolved`: expected `GOBERNACIÓN DE BOYACÁ`, got
`None`).

### Commits

Once commits integrados en orden; hash en `main` ← hash de origen en
`search/modality-contract-v1`:

`af55772` ← `e2e3db5` (catálogo canónico), `53812d1` ← `82b17c8` (resolver
composicional), `361fd6d` ← `2bae6cb` (alternativas coordinadas), `8e23e43` ←
`e1009df` (contratos de integración), `5b75e67` ← `f94ffbb` (pipeline canónico),
`d397cdf` ← `33bbdd3` (álgebra pura), `2e3c889` ← `16da02e` (R3 target-span),
`eaa051f` ← `4096a2c` (wiring + allowlist), `63d22d2` ← `ec55a81` (telemetría /
product gate), `84a9518` ← `3ea5dc6` (ER1A polaridad), `c5f857e` ← `3cc81cb`
(cierre M3C-E).

### Trazabilidad

El trabajo se desarrolló en `search/modality-contract-v1`, cuya ascendencia
incluía trabajo ajeno a modalidad. Sobre `main` se integró únicamente el
conjunto de modalidad; el contenido de la decisión es el mismo en ambas líneas y
conserva la identidad `ADR-054` que ya tenía en la rama de origen.

La numeración global conserva reservas de ramas concurrentes; los saltos no
implican documentos faltantes en esta rama.

**Cierre de ramas (2026-07-26).** `search/modality-contract-v1` e
`integration/modality-contract-v1-main` se eliminaron tras comprobar que `main`
conserva su contenido íntegro. Los *hashes de origen* de la lista anterior
quedan sin referencia y son recolectables por `git gc`: valen como identidad
histórica, no como punteros resolubles. Para inspeccionar el trabajo, usar los
hashes de `main`. Sobre el árbol final —modalidad y Radar juntos— la suite
completa mide `6592 passed, 2 skipped, 24 xfailed`, que es la cifra que el
closeout había registrado en la rama de origen; el `5381` de arriba corresponde
a `main` antes de integrar Radar.

### Enlaces

- ADR completo: `docs/decisions/2026-07-26_ADR-054_conversational-modality-contract.md`
- Closeout as-built (autoridad): `docs/planes/2026-07-25_M3C_modality_conversation_closeout.md`
- Guía operativa: `docs/guides/MODALITY_CONVERSATION_GUIDE.md`

---

> **Nota de reconciliación (2026-07-26).** ADR-052 y ADR-053 se incorporan a
> `main` después de ADR-054 desde una rama concurrente. La numeración ADR es
> global y conserva la identidad original; el orden de aparición en este log no
> altera la fecha ni la precedencia histórica de las decisiones. En su rama de
> origen ambas existían únicamente como entradas de este registro —no como
> archivos ADR separados—, y así se conservan aquí: el texto es el commiteado en
> `radar/mvp-manual`, sin renumerar ni reescribir. Sus commits de código
> (`87fe8af`, `2516f69`) se trasladaron íntegros; solo su cambio sobre este
> archivo se difirió hasta este punto.

## ADR-052: Precedencia en la frontera de entrega — consentimiento antes que expiración (R2B-1C-A)

**Fecha:** 2026-07-16  
**Estado:** Accepted  
**Rama:** `radar/mvp-manual` · **HEAD:** `f9a77e4`

### Contexto

`authorize_and_begin_dispatch` evaluaba `expires_at` **antes** de leer la matriz
de consentimiento. La justificación original (R2A) era razonable: ambas causas
son terminales y ninguna envía, así que la precedencia "solo elige la etiqueta",
y evaluar el reloj primero ahorraba un join dentro del lock de escritor.

El argumento tiene un hueco: la etiqueta *es* el registro. `dead_reason` es lo
único que queda para explicar por qué el sistema no habría podido entregar, y
un batch de alguien que se dio de baja marcado `expired` afirma que el mensaje
caducó, cuando la verdad es que esa persona ya no lo quería. La razón más
verdadera pierde frente a la más rápida de calcular.

### Decisión

Orden obligatorio dentro de la frontera de dispatch, bajo un solo
`BEGIN IMMEDIATE` y con el reloj capturado después del lock:

1. **Claim válido.** `status='claimed'`, token exacto y `claim_until > now`. Va
   primero: un intento abierto cuyo claim ya venció es un *huérfano* —el caso
   normal que `claim_for_reconciliation` resuelve—, no una inconsistencia. Un
   worker que llega con el claim perdido debe oír `claim_lost`, no una alarma.
2. **Intento `phase='dispatching'` abierto.** Con el claim *vigente*, encontrar
   uno es el estado imposible: `WorkerStoreConsistencyError`, sin tocar nada.
   Precede a la expiración y al consentimiento.
3. Seguridad, aislamiento y consentimiento mediante la matriz compartida
   (`authorization.py`).
4. Solo si la matriz autoriza, se evalúa la expiración.
5. Después se evalúan quiet hours.

### Consecuencias

Razones terminales de consentimiento: ganan sobre la expiración y **quedan
persistidas** en `dead_reason`.

- `preference_disabled + expired` → `dead_reason='opted_out'`
- `endpoint_revoked + expired` → `dead_reason='endpoint_revoked'`
- `radar_archived + expired` → `dead_reason='radar_archived'`
- `authorized + expired` → `dead_reason='expired'`

Precedencia del intento abierto:

- claim vencido + intento abierto → `claim_lost` y ruta de reconciliación
- claim vigente + intento abierto → `WorkerStoreConsistencyError`

`principal_mismatch + expired` → la autorización devuelve `security_blocked`,
pero **solo como resultado de esa llamada**: `security_blocked` no es hoy un
`dead_reason` persistente. El batch queda `pending` con `not_before = expires_at`
y el sweep termina registrándolo como `expired` al llegar su TTL. Es decir: el
registro final de un fallo de aislamiento sigue siendo menos verdadero de lo que
este ADR exige para el resto de los casos. Corregirlo requiere un `dead_reason`
nuevo y migración de schema, y queda fuera de R2B-1C-A a propósito — se anota
aquí para que la deuda esté escrita y no se descubra leyendo el sweep.

Reemplaza deliberadamente la precedencia fijada en R2A por
`test_61f_expiry_wins_over_consent_causes_and_both_refuse_to_send`, reescrito en
este bloque para fijar la política nueva. El coste aceptado es un join adicional
dentro del lock en la rama de batches vencidos.

## ADR-053: Vocabulario auditable del ledger y migración fail-closed (R2B-1C-B / R2B-2)

**Fecha:** 2026-07-16  
**Estado:** Accepted  
**Rama:** `radar/mvp-manual` · **HEAD:** `87fe8af`

### Contexto

El worker necesita cerrar tres desenlaces que el vocabulario existente de
`radar_notification_batches.dead_reason` y `radar_delivery_attempts.error_class`
no podía nombrar sin mentir:

- un payload que no se deja decodificar, versionar o renderizar — el fallo es
  nuestro y ocurre antes de cualquier llamada al proveedor;
- una política de reintentos agotada — lo que murió es la paciencia, no el
  consentimiento ni el TTL del batch;
- un canal sin proveedor registrado, y su fallo permanente equivalente.

Cerrar cualquiera de los tres con `dead_reason='expired'` o `'invalid_address'`
—los únicos valores disponibles— afirmaría algo falso sobre por qué el mensaje
no se entregó, exactamente el mismo problema que ADR-052 ya había señalado para
`security_blocked`.

### Decisión

1. **Vocabulario ampliado**, generado por una sola fuente en `schema.py`
   (`DEAD_REASONS`, `ERROR_CLASSES`) y consumido por `ledger_store.py` sin
   copia paralela:
   - `dead_reason`: `render_error`, `retries_exhausted`,
     `permanent_provider_error`.
   - `error_class`: `render_error`, `provider_unavailable`.
   - `invalid_address` sigue reservado exclusivamente para una dirección
     inválida; los fallos nuestros o de configuración usan las clases nuevas.

2. **`ProviderResult.error_class` comparte el vocabulario persistente.**
   `provider.py` importa `ERROR_CLASSES` de `schema.py` (sin ciclo: `schema.py`
   no importa `provider.py`) y rechaza cualquier clase que no pertenezca a él
   al construir el resultado. Sin este acoplamiento, un proveedor podía producir
   un `ProviderResult` válido para el contrato de Python pero que la base
   rechazaría después de haber llamado al proveedor: un "no hubo efecto,
   reintentable" barato se habría convertido en un intento que revienta a
   mitad de cerrarse, queda `dispatching`, y la siguiente corrida lo reconcilia
   como `ambiguous` por una razón ajena a si el mensaje salió.

3. **Migración fail-closed por DDL completo, no por fragmento.** El preflight
   de `schema.py` compara el texto **entero** normalizado de
   `radar_notification_batches` y `radar_delivery_attempts` contra las cuatro
   formas canónicas (current/legacy de cada tabla), generadas por los mismos
   `_batches_ddl`/`_attempts_ddl` que producen el DDL real. Reconocer solo el
   fragmento del vocabulario habría permitido clasificar como vigente una tabla
   que conservara `dead_reason`/`error_class` correctos pero hubiera perdido
   cualquier otra invariante — un `UNIQUE`, un `ON DELETE RESTRICT`, el `CHECK`
   compuesto de `phase`/`outcome` — y `CREATE TABLE IF NOT EXISTS` no repara lo
   que no reconoce. La normalización absorbe tres diferencias no semánticas
   entre el DDL que se ejecuta y el que SQLite persiste en `sqlite_master.sql`:
   ausencia de `IF NOT EXISTS`, ausencia del `;` final, y las comillas dobles
   que `ALTER TABLE ... RENAME TO` añade alrededor del nombre de tabla.
   Cualquier otro DDL —desconocido o parcialmente parecido— para en seco con
   `SchemaIncompatibleError`.

4. **La migración es atómica.** Ambas tablas se reconstruyen en una sola
   transacción aunque solo una lo necesite —evita un estado intermedio que
   ningún preflight posterior sabría nombrar—, preserva cada fila, FK e índice,
   y corre `foreign_key_check` + `integrity_check` antes del `COMMIT`. Un fallo
   en cualquier fase revierte por completo; una base ya vigente no se toca.

### Consecuencias

- `data/secoppal.db` **no** fue migrada durante el desarrollo de este bloque:
  no contiene las tablas del ledger. La migración solo se ejercitó contra
  bases temporales fabricadas en las pruebas.
- El vocabulario de Python y el `CHECK` de la base no pueden desincronizarse
  en silencio: ambos se derivan de `DEAD_REASONS`/`ERROR_CLASSES`, y
  `provider.py` valida contra la misma lista antes de que un resultado exista.
- Detectar el schema heredado por DDL completo es más estricto que por
  fragmento a propósito: una base que conserve el vocabulario pero pierda otra
  restricción ahora falla cerrado en vez de aceptarse como vigente.

---

## ADR-056: Recuperación de oportunidades vigentes por fecha civil colombiana (O1B / O1B.1)

**Fecha:** 2026-08-17  
**Estado:** Accepted  
**Rama:** `feature/opportunity-retrieval-v1` (mergeada ff-only) · **HEAD:** `2d4e24a`

### Contexto

La búsqueda de oportunidades devolvía un universo dominado por procesos que el
usuario no puede accionar. Medición pareada sobre 22 consultas reales contra
`p6dx-8zbt` (2026-08-17), antes del cambio:

| Métrica | Baseline |
|---|---:|
| Filas en prepublicación (`Borrador`/`En aprobación`/`Aprobado`) | 722 de 1.050 (68,8%) |
| Filas con plazo de recepción ya vencido | 75 (7,1%) |
| Filas con plazo futuro conocido | 28 |

O1A ya había falsificado la salida fácil: filtrar después de `LIMIT 50` pierde
candidatos válidos, porque el recorte ocurre antes de que el filtro vea el
universo. El filtro grueso tiene que vivir en el `WHERE`.

O1A.1 añadió el matiz que impide endurecer de más: la cobertura de
`fecha_de_recepcion_de` cambia mucho por modalidad, y su ausencia **no** es
evidencia de que el proceso esté cerrado.

### Decisión

Política pura y determinista en `app/core/opportunity_retrieval_policy.py`, sin
I/O, sin backend, sin LLM y sin reloj propio. Se aplica **solo** cuando
`dataset = procesos AND intent_type = opportunity_search`, sin tiempo explícito
y sin estado explícito del usuario.

1. **Estados por defecto:** `estado_del_procedimiento IN ('Publicado','Abierto')`,
   conservando intacta `STATE-PRIORITY-001`
   (`estado_de_apertura_del_proceso = 'Abierto'`).

2. **Regla temporal**, expresada como un único predicado Socrata:

   ```sql
   case(fecha_de_recepcion_de IS NULL,
        fecha_de_publicacion_del >= 'D-90',
        true, fecha_de_recepcion_de >= 'D')
   ```

   `D` es la **fecha civil `America/Bogota`**, derivada vía `ZoneInfo` de un
   instante UTC capturado **una sola vez** en `plan_query` y transportado como
   metadata interna JSON-safe (`_opportunity_reference_instant`).
   Los 90 días son una ventana de descubrimiento cuando el plazo es desconocido,
   **no** una vida máxima de la oportunidad.

3. **Precedencia de presentación (O1B sobre M2):** `Recepción de ofertas` exige
   plazo futuro observado. Plazo nulo → `Publicado · Vigencia por verificar`;
   plazo vencido → `Plazo de recepción reportado vencido`. Las etapas canónicas
   `selected`, `evaluation`, `cancelled` y `suspended` conservan su autoridad: el
   eje temporal nunca las rebaja.

4. **Estabilidad temporal:** la paginación reutiliza la referencia original desde
   `PaginationBuffer.query_params` (verificado también en el camino de refill);
   un follow-up semántico la descarta y recaptura, para que una conversación
   larga no siga consultando con el reloj de ayer.

### Alternativas descartadas

- **`OR` literal entre las dos columnas de fecha.** Lógicamente correcto, pero
  el planner de Socrata lo penaliza severamente: mediana 2,03 s y p90 8,41 s,
  por encima del gate. El `case(...)` produce el mismo conjunto y el mismo orden.
- **Comparar contra el instante UTC exacto** (implementación original de O1B).
  Excluía el proceso durante todo su último día — 38 procesos vivos medidos — y
  entre las 19:00 y 24:00 COT adelantaba un día calendario, dejando fuera 637
  filas que aún no vencían. Corregido en O1B.1: la frontera de día se decide en
  `America/Bogota`, nunca en UTC ni en el huso del host (verificado idéntico con
  `TZ` en UTC, Asia/Tokyo, America/Los_Angeles y Pacific/Kiritimati).
- **Freshness de 90 días universal.** Habría descartado plazos futuros de
  procesos publicados hace mucho: 22 filas reales verificadas en vivo.
- **Tratar `fecha_de_recepcion_de IS NULL` como cerrado.** Contradice O1A.1 y
  eliminaría la mayor parte del universo accionable.
- **Post-filtrado local tras recuperar 50 filas.** Es exactamente el defecto que
  O1A falsificó.

### Consecuencias

- Prepublicación y plazos vencidos salen del default: **0% en ambas** sobre la
  muestra pareada, sin regresión de timeouts (1 vs 1, atribuible a red).
- Se acepta el **falso positivo plausible con plazo nulo**. La promesa de
  producto es priorizar las mejores señales disponibles de vigencia, no
  certificar que el proceso siga abierto; Radar cubre la detección temprana.
- Una consulta de oportunidad ejecuta **una sola** recuperación productiva: sin
  Tier 2 automático, sin segunda consulta ante cero filas, sin oversampling. El
  fallback técnico H4 ante timeout conserva todos los filtros sustantivos.
- Deuda aceptada: `DEPLOY-TZ-001` — `ZoneInfo("America/Bogota")` se resuelve al
  importar el módulo y depende de la base de zonas horarias del sistema. Una
  imagen Linux mínima sin `tzdata` haría fallar el arranque. Se trata como
  microbloque de deployment/hardening; **no** se resuelve tocando la lógica de
  oportunidades.
- Diferido: `STATE_DATE_CONFLICT_DEFERRED` (Evaluación con plazo futuro),
  `OPPORTUNITY_MODALITY_SEMANTICS_DEFERRED` (contratación directa),
  `WAREHOUSE_RECEPTION_DATE_TRANSPORT_DEFERRED` (solo si Search gana backend local).

### Tests

`tests/test_current_opportunity_retrieval.py` (22 tests, incluidos los guardianes
de frontera de calendario, cruce 05:00Z, referencia de paginación y no-fuga de la
metadata interna).

Validación de cierre: suite **7466 passed, 2 skipped, 24 xfailed** ·
query corpus **89/89** · dialogue corpus **60/61** (`GOB-DEPTO-001` basal) ·
torture matrix **12/12 invariantes** · `make lint-core` limpio ·
paridad fuera de alcance **0 divergencias** sobre 22 consultas (procesos con año,
históricos, estados explícitos, contratos, entidad, modalidad, valor, direct lookup).

**Documentación:** `docs/audits/2026-08-17_O1B_CURRENT_OPPORTUNITY_RETRIEVAL.md`.

---

## ADR-057: Fundación dormida del Truth Layer integrada a main (M1)

**Fecha:** 2026-08-14  
**Estado:** Accepted  
**Rama:** trasplante selectivo desde `exp/generic-enrichment-runtime-v1` · **HEAD:** `ad1bb28`

### Contexto

La investigación del Truth Layer quedó cerrada (`TRUTH_LAYER_PROVEN_FOUNDATION_CLOSE_WORKSTREAM`)
sobre una rama experimental de 27 commits descendiente lineal de main. Mantenerla
viva fuera de main la condenaba a divergir; fusionarla entera habría traído 46
archivos exclusivos del experimento a la ruta productiva.

### Decisión

Trasplante **selectivo** —no merge ni cherry-pick de la secuencia experimental—
con contrato `VISIBLE_PRODUCT_CHANGE = NO`. De un inventario de 103 elementos:
28 `FOUNDATION_KEEP`, 3 `NEUTRAL_SEAM_EXTRACT`, 2 `DORMANT_LIBRARY_KEEP`,
2 `DEFER`, 46 `EXPERIMENT_ONLY_DROP`, 22 obsoletos. Resultado: 36 archivos nuevos
y **0 modificados**. La fundación entra dormida: nadie la consume todavía, y el
siguiente consumidor queda declarado (`PROCESS_OPERATIONAL_STATE`).

### Alternativas descartadas

- **Merge completo de la rama experimental:** arrastra el andamiaje de
  investigación a producción.
- **Dejar la fundación fuera de main:** divergencia garantizada y un consumidor
  (M2) bloqueado sin base.

### Consecuencias

Main gana la capa de observación sin cambio visible de producto. La
investigación queda cerrada como línea de trabajo; lo que sigue es consumo, no
más investigación.

### Tests

Suite en el momento del cierre: `6592 passed, 2 skipped, 24 xfailed` ·
query `89/89` · dialogue `60/61` (`GOB-DEPTO-001` basal). Oráculos públicos
capturados antes del trasplante (formatter, ranking, SoQL, paginación,
constructores canónicos y hashes de archivos runtime públicos).

**Documentación:** `docs/audits/2026-08-14_TRUTH_LAYER_MAIN_FOUNDATION_INTEGRATION.md`.

---

## ADR-058: La tarjeta deja de mostrar `estado_de_apertura_del_proceso` como estado (M2)

**Fecha:** 2026-08-17  
**Estado:** Accepted  
**Rama:** `feature/process-operational-state` · **HEAD:** `81993dd`

### Contexto

Medición en vivo contra `p6dx-8zbt` (9.018.738 filas, 2026-08-17):
`estado_de_apertura_del_proceso = 'Abierto'` cubre 7.888.926 filas (**87,5%**),
y de esas el **65,7% no está en recepción de ofertas** — 60,1% `Seleccionado`,
4,1% `Evaluación`, 0,84% `Cancelado`. La tarjeta mostraba ese campo como si
fuera el estado del proceso: un proceso ya adjudicado se leía como "Abierto".

### Decisión

Costura pequeña (`app/core/process_operational_state.py`) entre el Truth Layer
(M1) y el producto. La autoridad del estado es `process_lifecycle`, resuelto por
`observe_process_truth_axes`; aquí solo se rotula. El campo crudo no se oculta:
se degrada a corroboración y se conserva visible cuando **contradice** la etapa
operativa. El eje temporal es soporte, nunca motor: puede retirarle el verde a
una ventana de ofertas, jamás crear o cambiar una etapa.

### Alternativas descartadas

- **Seguir mostrando el campo crudo:** afirma disponibilidad falsa en dos de
  cada tres tarjetas.
- **Ocultar el campo crudo:** deja la tarjeta muda cuando es el único dato.
- **Reimplementar la interpretación en la capa de presentación:** duplicaría el
  lifecycle fuera del Truth Layer.

### Consecuencias

Lo que la tarjeta afirma queda respaldado por evidencia. Evidencia insuficiente
nunca se convierte en categoría: cae a un fallback conservador
(`Estado por verificar`). No consulta contratos, así que un proceso publicado ya
contratado en otro dataset no es detectable aquí (`OPP-CROSS-DATASET-001`).

### Tests

`tests/test_process_operational_state.py` (33) · `tests/test_truth_layer_public_parity.py` (10),
que acota la divergencia visible autorizada: quitar los campos nuevos restaura
byte a byte los oráculos previos.

**Documentación:** `docs/audits/2026-08-17_M2_PROCESS_OPERATIONAL_STATE.md`.

---

## ADR-059: Autoridad monetaria determinística y vigencia SMMLV (Money v2)

**Fecha:** 2026-08-18  
**Estado:** Accepted  
**Rama:** `feature/money-v2`

> **Reconciliación de registro (2026-08-22).** Esta decisión se versionó primero
> como archivo autónomo —`docs/decisions/2026-08-18_ADR-059_money-v2-deterministic-authority.md`—
> porque en ese momento `decisions_log.md` tenía cambios locales ajenos al patch.
> Esta entrada la incorpora al registro canónico. El archivo autónomo se conserva
> como documentación de detalle; la autoridad *as-built* sigue siendo
> `docs/audits/2026-08-18_MONEY_V2_CLOSEOUT.md`.

### Contexto

SECOPPAL debía interpretar montos colombianos sin confundirlos con NIT, años o
referencias, y proyectarlos al campo económico correcto de procesos o contratos.
SMMLV añade una restricción propia: el equivalente en COP depende de una
**vigencia**, y no puede sobrevivir sin revisión cuando un follow-up cambia el
alcance temporal.

La auditoría posterior al arco inicial dejó tres fronteras que debían quedar
explícitas bajo la misma autoridad: un umbral SMMLV heredado frente a un año
nuevo, el cardinal verbal `mil` ya escalado por la gramática, y un monto como
`2000 millones` que tiene forma de año.

### Decisión

1. `app/core/money_resolution.py` resuelve la representación numérica con
   `Decimal`; `app/utils/money.py` aplica unidades y escalas COP.
2. En cardinales verbales, `mil` **forma parte** del número resuelto
   (`dos mil` = 2.000). Solo un literal numérico seguido de la unidad `mil`
   aplica un factor adicional (`500 mil` = 500 × 1.000).
3. Un entero `20xx` seguido de unidad monetaria **no** es señal temporal para el
   clasificador de follow-up: `2000 millones` refina valor; `solo 2025` cambia la
   vigencia.
4. `money_metadata` conserva la **cantidad SMMLV fuente**. Si un follow-up cambia
   fechas, el merge revalida la metadata heredada antes de ejecutar:
   - una sola vigencia catalogada **recalcula** `valor_min`/`valor_max` desde la
     cantidad fuente;
   - un alcance **multianual** elimina el umbral COP heredado y produce una
     aclaración bloqueante;
   - una vigencia **no catalogada** también bloquea, sin extrapolar.
5. `QueryPlan` **P16** es la última compuerta: una aclaración SMMLV impide
   construir o enviar una consulta SECOP.
6. El contrato público sigue usando **enteros COP** en `valor_min`/`valor_max`.
   La polaridad no cambia y `DatasetSpec` conserva la proyección a `precio_base`
   o `valor_del_contrato`.
7. La metadata sigue siendo un diccionario mínimo y opcional. **No** se
   introduce un motor monetario paralelo, ni LLM, ni dependencia lingüística
   externa, ni semántica SMMLV multianual aproximada.

### Consecuencias

- El umbral SMMLV siempre corresponde al **año efectivo del frame final**.
- Los rangos de varias vigencias **fallan cerrados** y hacen **cero** llamadas a
  SECOP.
- Las cantidades cardinales no monetarias por debajo del umbral del router ya no
  se inflan 1.000 veces hasta convertirse en filtros económicos falsos.
- El invariante `1 billón = 1000 × mil millones` permanece intacto.
- El catálogo anual sigue exigiendo actualización **explícita y respaldada por
  fuente oficial** para cada vigencia nueva.

### Alcance diferido

Este ADR **no** autoriza limpieza general del parser. Siguen diferidos `MV2-06`,
`MV2-07`, `MV2-08`, `MV2-09`, `MV2-11`, `MV2-12` y `MV2-13`, según el re-audit.

**Documentación:** decisión versionada en detalle en
`docs/decisions/2026-08-18_ADR-059_money-v2-deterministic-authority.md`;
autoridad *as-built* en `docs/audits/2026-08-18_MONEY_V2_CLOSEOUT.md`.

---

## ADR-060: Actor Profile V1 — identidad tipada, resolución de proveedor y huella contractual

**Fecha:** 2026-08-22  
**Estado:** Accepted  
**Rama:** `feature/actor-profile-v1` (mergeada ff-only) · **HEAD de cierre:** `7f24517`  
**Vigente en main:** `8acb21a`

> **Nota de numeración.** El máximo global comprometido sobre los ~21 worktrees
> era **ADR-059** (autoridad monetaria determinística), que se versionó primero
> como archivo autónomo y quedó reconciliado en este registro el 2026-08-22.
> El **ADR-042** provisional de la propuesta de junio
> (`docs/planes/2026-06-29_perfil-contratista-dossier.md`) **no se reutiliza**:
> ese número quedó asignado a otra decisión y reciclarlo rompería el orden
> temporal del log.

### Contexto

SECOPPAL no sabía responder *quién es este proveedor*. Las preguntas reales del
usuario —"qué contratos tiene X", "qué ha hecho X en 2026", "contratos de X con
Y", por nombre, por NIT o por cédula— caían en el camino temático: el nombre del
actor entraba como `objeto` contractual y la consulta buscaba esa cadena dentro
del objeto del contrato.

Ese degradado es peor que un error visible. AP0 lo midió: *"qué ha celebrado la
persona 1140835135 en 2025"* producía una búsqueda de la palabra `persona` en
2025, y esa lista se presentaba como el historial de esa persona. No falla, no
avisa, y devuelve una respuesta con la forma correcta.

Cuando lo que se afirma es sobre un tercero identificable, **una atribución
equivocada es peor que no responder**. Esa asimetría fija toda la política del
frente.

### Decisión

#### 1. Identidad tipada como spine (INV-ID-TYPE)

La llave canónica es `IdentityKey(document_type, base_number)`
(`app/core/actor_identity.py`). Tipos autoritativos en V1: **`NIT`** y **`CC`**.

La llave es lo que **une** fuentes; la procedencia es lo que se **audita**. Si la
llave llevara `source`, `raw_value`, `check_digit` o `confidence`, dos registros
del mismo actor vistos desde fuentes distintas dejarían de comparar iguales
(INV-PROVENANCE-SEPARATION). Por eso `IdentityMatch` conserva aparte el crudo
verbatim, el origen, el DV cuando se pudo separar y por qué se decidió el tipo.

Consecuencias directas:

- **El mismo número bajo dos tipos declarados distintos no es automáticamente el
  mismo actor.**
- **La longitud jamás decide el tipo** (INV-NO-LENGTH): cédulas y NIT se solapan
  en dígitos, y clasificar por longitud produce atribuciones falsas con
  apariencia de certeza.
- El **dígito de verificación es evidencia de calidad, no autoridad de tipo**: un
  número cualquiera satisface el módulo 11 con probabilidad ≈1/11, así que un DV
  válido nunca degrada una `CC` declarada a `NIT`. Un DV inconsistente se anota
  (`CHECK_DIGIT_MISMATCH`) sin reescribir el número que la fuente publicó.

#### 2. La fuente de la huella es SECOP II Contratos

El perfil contractual se construye sobre **SECOP II Contratos electrónicos,
`jbjy-vk9h`**, proyectando los campos reales de proveedor y documento. El
almacén histórico de procesos **no** es verdad de Actor Profile: no publica
autoridad equivalente de documento del proveedor por el mismo camino.

El tipo de la fuente se usa para **excluir lo contradictorio, no para exigir lo
idéntico** (`app/core/actor_document_types.py`): `"No Definido"` y `"Otro"` se
conservan siempre —son indeterminados, no contradictorios—, porque un filtro de
igualdad `tipodocproveedor = 'NIT'` borraría uniones temporales clasificadas como
`"Otro"` (cuatro de los 4.230 contratos del NIT 860.002.400, y 10.073 filas con
documento utilizable y tipo indeterminado en el universo completo).

Cuando la combinación actor/dataset **no** se puede proyectar, la consulta **no
se ejecuta**: se pide la aclaración correspondiente (`actor_scope_policy.py`,
INV-NO-SILENT-DROP). El actor nunca se descarta para seguir adelante con los
filtros restantes.

#### 3. Directorio local de aliases de actores

La resolución por nombre se apoya en un directorio SQLite local construido desde
`jbjy-vk9h` (`scripts/build_actor_alias_index.py` →
`app/data/actor_alias_index.py`, consultado por `app/data/actor_alias_resolver.py`).
Build completo aceptado (AP3-E / AP3-E1):

| Métrica | Valor |
|---|---:|
| Grupos de origen considerados | 1.235.591 |
| Suma de ocurrencias de origen | 5.784.468 |
| Identidades indexadas | 1.214.351 |
| Cobertura de identidad | 98,84% |
| Cobertura de actividad contractual | 98,13% |
| Aliases exactos ambiguos | 14.305 (1,18%) |
| Exclusiones por suciedad | 15.936 (1,30%) |

El índice es **infraestructura de recuperación de candidatos**, no verdad
registral: no es RUES, no es RUP, no prueba existencia legal y no certifica nada.
El SQLite generado está ignorado y **no** se commitea.

#### 4. Estado ≠ método: dos dimensiones separadas

`app/core/actor_resolution.py` mantiene separado *qué ocurrió* de *por qué una
identidad es autoritativa*.

Estados: `RESOLVED`, `AMBIGUOUS`, `CANDIDATES`, `UNRESOLVED`, `INDEX_UNAVAILABLE`.

Métodos autoritativos (`AUTHORITATIVE_ACTOR_METHODS`):

- `explicit_document`
- `exact_unique_alias`
- `exact_legal_form_variant`
- `user_selected_candidate`

**La recuperación difusa no es autoridad.** El trigram/BM25 de FTS5 recupera
candidatos y jamás auto-selecciona identidad (INV-NAME-NO-GUESS); un candidato
solo se vuelve autoritativo por **selección explícita del usuario**. Un nombre
sin resolver termina en `name_unresolved`: la consulta queda clasificada, la
identidad queda declarada pendiente, y el perfil no se ejecuta.

#### 5. Mención completa y precedencia de geografía desnuda

`INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION` — un fragmento más corto de una
mención natural más larga no adquiere autoridad solo porque ese fragmento sea un
alias exacto del directorio.

`INV-BARE-GEOGRAPHY-PRECEDENCE` — *"contratos de Barranquilla"* sigue siendo
interpretación geográfica/de búsqueda cuando el contexto lo sostiene. Un alias de
actor llamado Barranquilla no puede secuestrar esa consulta. El lenguaje
explícito de actor (*"proveedor"*, *"contratista"*, *"qué ha contratado"*) sí
puede sobreponerse a esa precedencia.

#### 6. Variantes de forma jurídica, acotadas en lookup

`app/core/actor_legal_forms.py` genera equivalencias deterministas de **cola**:
`S.A.S. ↔ SAS`, `S.A. ↔ SA`, `E.S.E. ↔ ESE`. **N2 no se reconstruyó globalmente**;
la capa está guardada en tiempo de consulta y no reescribe tokens intermedios ni
elimina la forma jurídica.

Auditoría de la variante (`scripts/audit_actor_legal_form_variants.py`):
1.187.243 aliases · 36.965 clases de equivalencia · **77 equivalencias nuevamente
ambiguas** · **0 identidades equivocadas**. Las ambiguas **fallan cerrado**.
Método resultante: `exact_legal_form_variant`.

#### 7. El producto es una huella observada, no un dossier

`ActorProfileResult` (`app/core/actor_profile/`) devuelve la **huella contractual
observada en SECOP II**: nombre observado, documento tipado, `queried_at` con
zona, conteo de contratos, valor contractual reportado, rango de actividad,
actividad por año, principales compradores, modalidades, territorios, contratos
recientes y enlaces a SECOP.

No es un dossier legal, ni certificación RUP/RUES, ni informe de riesgo, ni
certificado de antecedentes. El render declara la fuente, la fecha y —sobre
todo— **qué no se consultó** (INV-SCOPE-DECLARED), y no emite juicios agregados
de riesgo ni ordena actores en ninguna escala (INV-NO-VERDICT).

Un agregado secundario que falla **no** destruye el perfil, y un perfil incompleto
**no** se presenta como completo (INV-PARTIAL): el vocabulario de `outcome.py`
distingue el vacío confirmado por la fuente del fallo de la fuente, para que un
timeout no se lea como "este actor no tiene contratos en Antioquia".

#### 8. Conversación natural de actor

`app/core/actor_conversation.py` reconoce **operaciones y roles, nunca
identidades**: `SET_ACTOR`, `REPLACE_ACTOR`, `SELECT_ACTOR_CANDIDATE`,
`SET_BUYER`, `SET_TERRITORY`. El `QueryFrame` existente sigue gobernando los
filtros ortogonales (tiempo, valor, modalidad, orden).

Con actor activo: `CON <entidad>` → comprador; `EN <lugar>` → territorio. Ejemplo:
`FUNSOCOM` → `qué ha hecho en 2026` → `con Córdoba` significa actor FUNSOCOM, año
2026, comprador Departamento de Córdoba.

#### 9. Documento completo en la UX de candidatos

En la lista de candidatos, **NIT y CC se muestran completos**
(`format_actor_identity_for_user`): el documento **es** la evidencia de
desambiguación, y enmascararlo destruiría exactamente la información que el
usuario necesita para elegir entre `JORGE VERBEL MARTELO — CC 92550735` y
`JORGE LUIS VERBEL GARCIA — CC 1020727022`. `mask_identity_key` sigue intacto
para reprs, logs, excepciones y superficies diagnósticas.

#### 10. Selección clicable, transporte-independiente

Streamlit renderiza controles nativos sobre `actor_candidate_options(...)`. El
clic y el texto (`2`, `opción 2`, `el segundo`) llegan a la **misma** operación
de backend `SELECT_ACTOR_CANDIDATE`, servida por
`SecopalWorkflow.select_actor_candidate(candidate_rank, pending_token)` y resuelta
en `handle_actor_selection`. La UI **no puede inyectar un `IdentityKey`
arbitrario**: solo transporta un rango y un token opaco que ata la selección a la
lista pendiente vigente. Procedencia resultante: `user_selected_candidate`. Los
botones de candidato en Telegram **no** están entregados todavía; la operación ya
es apta para ese callback.

#### 11. Flag apagado por defecto ≠ feature incompleta

`SECOPPAL_ACTOR_PROFILE_ENABLED=false` es el **default seguro del código**
(`app/config.py`), verificado por `tests/test_actor_flag_off.py`. Activar es una
decisión de entorno aprovisionado, no un efecto secundario del import.

Para activar hacen falta **dos** cosas: `SECOPPAL_ACTOR_PROFILE_ENABLED=true` y
`SECOP_ACTOR_ALIAS_INDEX_PATH` apuntando a un `actor_alias_index.sqlite` completo
y de esquema compatible. Sin artefacto válido el resolver devuelve
`INDEX_UNAVAILABLE` sin fallback — nunca adivina.

### Alternativas descartadas

- **Tratar el nombre del actor como `objeto` contractual.** Es el defecto que el
  frente existe para cerrar: produce falsa atribución con forma de acierto.
- **Inferir el tipo documental por longitud del número.** Cédulas y NIT se
  solapan; la inferencia fabrica certeza donde no la hay.
- **Dejar que el DV reclasifique una `CC` declarada como `NIT`.** Sería fabricar
  una identidad que ninguna fuente entregó.
- **Auto-resolver el mejor candidato difuso.** Un BM25 alto ordena; no identifica.
- **Filtrar `tipodocproveedor` por igualdad.** Cambia falsas atribuciones por
  omisiones silenciosas.
- **Reconstruir N2 globalmente para absorber formas jurídicas.** Coste alto y
  riesgo de colisión sobre 1,2 M de identidades, cuando la equivalencia acotada
  en lookup resuelve el caso con 0 identidades equivocadas.
- **Enmascarar el documento en la lista de candidatos.** Destruye la evidencia de
  desambiguación en la única pantalla donde el usuario debe decidir.
- **Descartar el actor y ejecutar el resto de filtros cuando el dataset no lo
  soporta.** Degradado silencioso: la peor de las opciones.

### Consecuencias

- SECOPPAL responde por proveedor con identidad tipada y procedencia auditable,
  o **no responde** — nunca atribuye por conveniencia.
- El directorio local añade un artefacto de aprovisionamiento a la operación: sin
  índice compatible, el frente queda inerte por diseño.
- **Fuera de alcance de V1** (siguen siendo trabajo futuro): relaciones UT /
  consorcios, RUP, RUES, representante legal, SIRI, enriquecimiento de
  responsabilidad fiscal, `ActorId` como grafo entre fuentes, score de riesgo,
  Radar por actor, grafo profundo / Neo4j.
- **Deuda aceptada:** latencia trigram sobre el índice completo **287–857 ms**
  según estado de caché (p95 caliente / p95 en frío); pico de memoria del builder
  **3,02 GB**; 77 equivalencias de forma jurídica ambiguas que fallan cerrado.
  Ninguna bloquea el cierre.
- **Siguiente frente de actor:** Actor Relationships V1 — UT / Consorcios.

### Tests y evidencia de cierre

Suite en el cierre de Actor Profile (`7f24517`): **8175 passed, 2 skipped,
24 xfailed** · corpus de actor **41/41** · corpus natural de actor **6/6** ·
mutaciones AP3-G de UX de candidatos **15/15 detectadas**.

Auditoría adversarial congelada — **1.150 casos**,
`FROZEN_CORPUS_SHA256 = a3db00d39cc8ed54811ac3ebfea2c05c6c1e275711171295e34015b5a8df7518`
(`tests/fixtures/actor_profile_adversarial/SHA256SUMS`):

```text
P0 0 · P1 0 · identidades equivocadas 0 · secuestros temáticos 0
pérdidas de actor 0 · auto-resoluciones difusas 0
```

**Documentación:** guía técnica `docs/architecture/ACTOR_PROFILE_V1.md`; informes
en `docs/audits/` (`2026-08-18_ACTOR_PROFILE_DIAGNOSTIC.md`,
`2026-08-18_ACTOR_NAME_RESOLUTION_BENCHMARK.md`,
`2026-08-19_ACTOR_PROFILE_V1_ADVERSARIAL_AUDIT.md`,
`2026-08-20_AP3E_PRODUCTION_ACTOR_DIRECTORY.md`,
`2026-08-20_AP3E1_FULL_INDEX_REMEDIATION.md`).

---

## ADR-061: Dimensiones semánticas estructuradas y mutaciones conversacionales componibles (SEARCH-SH1)

**Fecha:** 2026-08-22  
**Estado:** Accepted  
**Rama:** `fix/search-semantic-hardening-v1` (mergeada ff-only) · **HEAD / main:** `8acb21a`

### Contexto

Dos fallas sistémicas encontradas en dogfood manual, con una raíz común: el
sistema no distinguía **a qué dimensión estructural pertenece** un tramo del
lenguaje del usuario, ni permitía que dos dimensiones distintas cambiaran en el
mismo turno.

**A. `convenios` se convertía en tema.**

```text
convenios de alcaldia de barranquilla en 2026
```

`convenios` no era ni modalidad reconocida ni tema real, así que caía en el
extractor de objeto y terminaba como búsqueda literal de texto.

**B. El refinamiento monetario se perdía al repetir el año.**

```text
base:      contratos adulto mayor gobernacion de atlantico en 2026
follow-up: por mas de 100 millones en 2026
```

El parser **sí** emitía `valor_min=100000000`. La pérdida ocurría después:

```text
MONEY_LOSS_STAGE:
  FollowupClassifier(change_year)
    -> followup_mutations.gate_delta(change_year allowed_set)
```

El año repetido clasificaba el turno como `change_year`, y el conjunto permitido
de esa intención **descartaba `valor_min`** como `field_not_allowed_for_intent`.
Como el campo desaparecía antes de serializar el frame, el fingerprint de la
consulta seguía representando el universo viejo y el sidecar de paginación
anterior permanecía elegible: *"más resultados"* seguía paginando resultados sin
filtrar.

### Decisión

#### 1. Dimensiones semánticas explícitas

`app/core/semantic_dimensions.py` declara un **vocabulario, no una ontología**:

```text
TOPIC · MODALITY · CONTRACT_TYPE_OR_NATURE · ACTOR · BUYER
GEOGRAPHY · TIME · VALUE · STATE · CONTROL_LANGUAGE
```

Sirve para que un resolutor estructurado declare **qué consumió**, de modo que
ese tramo no pueda volver a inferirse como término temático.

**`INV-STRUCTURED-DIMENSION-NOT-TOPIC`** — una vez que un tramo se consume con
confianza como dimensión estructurada, no puede además filtrarse a `objeto`,
salvo que la semántica exija explícitamente el uso dual (el caso `COMPOSITE`, que
requiere evidencia textual controlada *además* del campo estructurado).

Parse conceptual de referencia:

```text
convenios de régimen especial de alcaldía de barranquilla en 2026
  → naturaleza contractual = convenio
  → modalidad             = régimen especial
  → comprador             = Barranquilla
  → año                   = 2026
  → objeto                = []
```

#### 2. `convenio` es COMPOSITE, no un alias de modalidad

Auditoría de fuente en vivo sobre `p6dx-8zbt` (procesos) y `jbjy-vk9h`
(contratos), 2026-08-22. Campos estructurados relevantes: `tipo_de_contrato`
junto a `modalidad_de_contratacion` y el campo de justificación de modalidad.

**No existe** un valor de fuente `tipo_de_contrato = 'Convenio'`. La familia
interadministrativa —135.932 filas de procesos y 123.823 de contratos— está
mayoritariamente almacenada como tipo `Otro`, lo que prueba que
`tipo_de_contrato` por sí solo **no puede** representar el lenguaje natural
`convenio`. Familia solidaria: 2.208 procesos / 8.900 contratos. Tipo exacto
`Acuerdo de cooperación`: 27 / 22.

Por eso `app/core/contract_type_resolver.py` clasifica en tres niveles
—`STRUCTURED_EXACT`, `STRUCTURED_FAMILY`, `COMPOSITE`— y
`app/core/contract_type_projection.py` proyecta cada clase con la evidencia que
la fuente sí sostiene, por dataset (los nombres de campo de justificación
difieren). Donde el valor de fuente confunde contratos y convenios, se exige
**además** evidencia textual controlada; el `%convenio%` crudo no es equivalente
a naturaleza contractual (verificado con falsos positivos reales: contratos de
prestación de servicios, obra e interventoría que solo *referencian* un convenio).

**Documentar `convenio == régimen especial` sería arquitectura falsa.** Son ejes
independientes que pueden coexistir.

#### 3. La arquitectura de modalidad se preserva intacta

`ModalityResolver`, su catálogo y su léxico **no se modificaron**. La autoridad
sobre la modalidad de contratación sigue siendo suya. Familias verificadas en la
auditoría: selección abreviada, régimen especial, mínima cuantía, licitación
pública, concurso de méritos, acuerdo marco, contratación directa y enajenación
—todas consumidas sin residual y sin fuga a `objeto`.

`M2 cambiado: NO`. Los IDs de procedimiento de modalidad **nunca** se usan como
IDs de tipo contractual. `naturaleza contractual ≠ modalidad`.

#### 4. Los deltas ortogonales componen

**`INV-ORTHOGONAL-DELTA-NOT-LOST-BY-NOOP`** — un no-op o una repetición en una
dimensión no puede borrar un cambio explícito en otra.

`app/core/followup_engine.py` deja de clasificar como `change_year` (o
`change_order`) un turno que además trae un filtro ortogonal, y
`app/core/followup_mutations.py` admite `MONEY_FIELDS` dentro de los conjuntos
permitidos de `change_year`, `change_order`, `change_scope` y `change_dataset`.
El caso base:

```text
año previo = 2026
follow-up  = por mas de 100 millones en 2026
resultado  = año 2026 + valor_min 100000000
```

Reglas de límites monetarios: un rango completo reemplaza ambos extremos; un
mínimo nuevo solo limpia un máximo heredado cuando los límites son
incompatibles, y simétricamente.

#### 5. Proyección monetaria por capacidad de dataset

La restricción monetaria del usuario es **una sola** a nivel de producto y se
proyecta según el dataset: `valor_del_contrato` en contratos, `precio_base` en
procesos. Fuera del contrato de producto vigente esos campos de fuente **no** se
declaran semánticamente equivalentes.

#### 6. Toda mutación semántica invalida la paginación

**`INV-FILTER-MUTATION-INVALIDATES-PAGINATION`** — si cambia el filtro, cambia el
universo de resultados; el estado de paginación deja de ser válido.

`ConversationStore.invalidate_pagination_buffers(chat_id)` elimina el sidecar
completo, y `handle_normal_flow` lo invoca **antes** de parsear y ejecutar
cualquier turno que no sea de paginación. Toda mutación aceptada limpia además
`offset`, `_display_offset` y `_shown_total`. Aplica a valor, tiempo, tema,
comprador, territorio, modalidad, tipo/naturaleza contractual y actor.

El sidecar es solo una caché reproducible de paginación: el historial de
conversación queda intacto. Borrarlo entero también impide que un grupo hermano
rancio de una consulta dual-dataset sobreviva a un refinamiento posterior de un
solo dataset, y cubre los caminos de cero resultados, aclaración y timeout.

### Alternativas descartadas

- **Añadir `convenio` al léxico de modalidad.** Falso: no es una modalidad, y
  contaminaría la autoridad de `ModalityResolver`.
- **Proyectar `convenio` como `LIKE '%convenio%'` sobre el objeto.** Trae
  contratos de obra, interventoría y prestación de servicios que solo mencionan
  un convenio ajeno.
- **Ampliar el conjunto permitido de `change_year` sin tocar la clasificación.**
  Trata el síntoma: el turno seguiría siendo "cambio de año" cuando en realidad
  es un refinamiento.
- **Invalidar la paginación solo cuando cambia el fingerprint.** Fue exactamente
  el acoplamiento que dejó el sidecar rancio vivo mientras el delta monetario se
  perdía antes de serializarse.

### Consecuencias

- El lenguaje natural de naturaleza contractual deja de degradarse a búsqueda de
  texto, y `objeto` deja de ser el vertedero de todo token no-stopword.
- Un refinamiento conversacional se comporta como refinamiento: acota el
  universo y la paginación siguiente pagina **ese** universo.
- **Cero es un resultado válido** cuando la intersección está respaldada por la
  fuente y bien formada; no toda consulta debe devolver filas.
- Añadir una clase contractual nueva exige evidencia de fuente para su
  proyección, no una entrada de léxico.

### Evidencia de dogfood aceptada (SECOP en vivo, 2026-08-22)

| Caso | Dataset | Filas | Verificación |
|---|---|---:|---|
| Gobernación de Atlántico · adulto mayor · 2026 (base) | contratos | 46 | tema, comprador y año preservados |
| ↳ `por mas de 100 millones en 2026` | contratos | 4 | `valor_min=100000000`; mínimo crudo $348.747.143; filas bajo el umbral **0** |
| ↳ `más resultados` | contratos | 4, agotado | conserva `valor_min`; sin filas ni IDs rancios |
| convenios · Barranquilla · 2026 | procesos | 2 | `contract_type_include=[convenio]`; sin fuga a `objeto` |
| convenio + régimen especial + Barranquilla + `>2B` · 2026 | procesos | 0 | intersección válida respaldada por fuente; ambas dimensiones presentes |
| selección abreviada · Barranquilla · 2026 | contratos | 19 | familia exacta de tres hijos preservada |
| adulto mayor · Cartagena · `>1B` · 2025 | contratos | 5 | `adulto_mayor` temático preservado |

### Tests

Suite final: **8250 passed, 2 skipped, 24 xfailed** · query corpus **89/89** ·
dialogue corpus **60/61** (`GOB-DEPTO-001` basal, en `query_plan` no tocado) ·
modalidad/diálogo/E2E **3.327 passed** · dinero/follow-up/paginación focalizados
**364 passed** · centinelas de Actor Profile 8 passed · campaña de mutación
**15/15 detectadas**, fuentes restauradas byte a byte
(`scripts/run_search_sh1_mutations.py`) · `make lint-core` limpio y Ruff limpio
sobre todos los archivos nuevos de SEARCH-SH1.

Archivos de prueba: `tests/test_search_sh1_dogfood_regressions.py`,
`tests/test_contract_type_resolver.py`,
`tests/test_contractual_dimension_pipeline.py`,
`tests/test_search_sh1_modality_audit.py`,
`tests/test_search_sh1_money_followups.py`,
`tests/test_pagination_buffer_lifecycle.py`.

**Documentación:** guía técnica `docs/architecture/SEMANTIC_QUERY_DIMENSIONS.md`;
evidencia detallada de fuente y proyección en
`docs/audits/2026-08-22_SEARCH_SH1_SEMANTIC_MONEY_HARDENING.md`.

---

## ADR-062: Cierre del spike Semantic Market Pack — NO-GO como autoridad runtime

**Fecha:** 2026-08-23  
**Estado:** Accepted  
**Rama:** `exp/semantic-radar-adulto-mayor-final` (**no mergeada**; archivo de evidencia)

```text
AS_BUILT_AUTHORITY:
  docs/audits/2026-08-23_SEMANTIC_MARKET_PACK_SPIKE_CLOSURE.md
  docs/audits/2026-08-23_ADULTO_MAYOR_SEMANTIC_RADAR_FINAL.md
```

### Contexto

El spike preguntaba si un **Market Pack** ejecutado sobre el **Semantic Registry
v0** podía ser autoridad de runtime en dos frentes: sustituir el retrieval
clásico en Search y clasificar pertenencia a mercado en Radar mejor que una
definición curada lexical. El protocolo pre-registrado autorizaba transferir la
prueba a un segundo mercado (Infraestructura Vial) **solo** ante `GO` o
`KEEP_WITH_LIMITS` en el primero.

Brazos: `A_CLASSIC` (producción), `B2_STRONG_CURATED_OR` (definición curada:
términos positivos, frases, exclusiones y filtros estructurados) y
`D_SEMANTIC_REGISTRY_HYBRID` (Market Pack sobre el Registry, con abstención
conservadora y evidencia de fila requerida).

### Experimentos

MP1 (spike) → MP1-R (reparación de metodología) → MP1-D (labels diagnósticos
ciegos) → MP2 (B2 + protocolo de cold pre-registrado) → MP2-FINAL (sonda de
retrieval en vivo) → SRADAR-2 (veredicto de Radar sobre muestra ciega de 240
filas con labels congelados antes de abrir el MAP).

### Evidencia

**Search — `FINAL_EXPERIMENT_TRANSPORT_INCONCLUSIVE`.** Run `20260823T033843Z`:
36 casos × 3 rondas = 108 slots por brazo, seis permutaciones balanceadas,
semilla `20260822`, `timeout=15s`, sin reintento oculto. `paired_query_count = 9`
frente a `minimum_paired_queries = 24` → transporte insuficiente y **sin
veredicto comparativo**. Dos hechos independientes del transporte: el componente
semántico de D registró **108/108 abstenciones**, por lo que sus 42 rondas
exitosas fueron **passthrough** del brazo clásico; y las 108 trazas cerraron con
cero violaciones de seguridad — el diseño resultó seguro pero **inerte**.

**Radar — `ADULTO_MAYOR_RADAR_SEMANTIC_NO_GO`.**

| Métrica | B2 | D |
|---|---:|---:|
| Precision | 0,913793 | 1,000000 |
| Recall | 0,913793 | 0,724138 |
| F1 | 0,913793 | 0,840000 |
| Abstention rate | 0,000000 | 0,808333 |

D corrige 5 falsos positivos y 1 hard negative, recupera 2 misses por evidencia
de fila, pierde 13 positivos y produce **11 notificaciones verdaderas netas
menos**. Los 5 falsos positivos corregidos y los 13 positivos perdidos se
atribuyen a `ABSTENTION_POLICY`, no a causalidad semántica: solo hay 2
`SEMANTIC_CAUSAL_WINS` y 1 `SEMANTIC_CAUSAL_LOSS`.

Gates: `R1 PASS`, `R2 PASS`, `R3 PASS`, **`R4 FAIL`** (costo de recall), `R5 PASS`.

### Decisión

```text
SEMANTIC_MARKET_PACK_RUNTIME: NO_GO
```

1. El Market Pack **no** se adopta como autoridad de runtime, ni en Search ni en
   Radar.
2. La prueba de transferencia a **Infraestructura Vial queda cancelada**: el
   primer mercado no superó el gate de utilidad operativa.
3. El **Semantic Registry se conserva**, reclasificado a rol
   `ADVISORY / GOVERNANCE / AUTHORING`: relaciones semánticas, provenance,
   documentación, auditoría y sugerencia de vocabulario al curar definiciones de
   mercado. No se convierte en autoridad de runtime sin evidencia independiente
   nueva.
4. Se mantiene la separación de autoridades `MORPHOLOGY` / `DOMAIN SEMANTICS` /
   `QUERY SEMANTICS`.
5. Baseline recomendado para Radar: **definición de mercado curada** (términos
   positivos, frases, exclusiones y filtros estructurados de región, entidad,
   valor, modalidad y fechas) → broad retrieval → dedup/diff de candidatos →
   notificación. Se adopta el patrón, **no** el artefacto B2 de Adulto Mayor.
6. Search se mantiene sin cambios. SODA3/transporte, recall lexical, reranking,
   embeddings y clasificadores post-retrieval son prioridades independientes que
   **no** forman parte de este cierre.
7. Ninguna rama experimental se mergea y ninguna evidencia se borra.

### Qué NO significa

No se concluye que la semántica de dominio carezca de valor, que el Registry
deba eliminarse, que embeddings o rerankers no puedan funcionar, ni que el Radar
no pueda manejar mercados. Search quedó **inconcluyente por transporte**, no
refutado. La conclusión es acotada: la hipótesis e implementación actuales de
Market Pack + Semantic Registry no justifican su complejidad como autoridad
runtime.

### Consecuencias

- Radar conserva su arquitectura actual; el patrón de definición curada queda
  registrado como baseline recomendado para cuando se implemente soporte de
  mercados.
- El Registry v0 (`app/core/semantic_registry.py`,
  `app/data/semantic_registry_v0.json`) permanece **dormido y fuera de main**:
  ningún módulo de runtime lo importa.
- Todo el harness experimental (`app/experiments/market_packs/`,
  `scripts/experiments/`, `artifacts/experiments/`) queda fuera de producción.
- Se preserva la evidencia completa: cold v2 (36 casos, hash canónico
  `aa0f4c32…`), labels adjudicados, manifiestos, métricas y auditorías.
- Hipótesis futura registrada, **sin implementación**:
  `BROAD RETRIEVAL → RERANK / CLASSIFY` en lugar de
  `SEMANTIC PRE-RETRIEVAL GATING`, porque la abstención conservadora demostró
  costar cobertura.

### Criterios de reapertura

La clasificación semántica de mercado en runtime solo se reabre si existe una
técnica o hipótesis nueva que cumpla **las cuatro** condiciones:

1. **no** dependa de retunear el cold de Adulto Mayor ya usado;
2. ataque directamente el tradeoff precision/recall observado — es decir,
   proponga un mecanismo que recupere cobertura sin depender de la política de
   abstención;
3. traiga una evaluación independiente nueva, con muestra y labels congelados
   antes de medir;
4. **no** sea simplemente añadir más vocabulario al Registry.

Añadir conceptos, relaciones o qualifiers al Registry **no** constituye por sí
mismo un criterio de reapertura.

**Documentación:** cierre y matriz de integración de commits en
`docs/audits/2026-08-23_SEMANTIC_MARKET_PACK_SPIKE_CLOSURE.md`; veredicto de
Radar en `docs/audits/2026-08-23_ADULTO_MAYOR_SEMANTIC_RADAR_FINAL.md`.

---

## ADR-063: La normalización preserva intención direccional y extremos abiertos

**Fecha:** 2026-08-24
**Estado:** Accepted

### Contexto

HCA-R2 confirmó un patrón común antes de `QueryPlan`:
`NORMALIZATION_ERASES_USER_INTENT_BEFORE_POLICY`. El extractor monetario
reconocía correctamente un límite inferior y uno superior, pero ordenaba sus
valores con `min()`/`max()` aun cuando el usuario había expresado roles
direccionales contradictorios. P8 recibía así un rango aparentemente válido.
Este es su estado histórico exacto:

```text
P8 historical status:
BLINDED_BY_NORMALIZATION_FROM_BIRTH
```

En tiempo, `_extract_dates()` emitía correctamente `fecha_desde=2025-01-01` sin
`fecha_hasta` para `desde 2025`; el fallback genérico de año completaba después
el extremo ausente y volvía esa consulta indistinguible de `en 2025`.

### Decisión

**`INV-NORMALIZATION-PRESERVES-INTENT`** — normalizar puede convertir unidades,
canonicalizar representación y ordenar extremos solo cuando la expresión es
semánticamente simétrica. No puede destruir dirección, contradicción,
apertura/cierre, granularidad ni provenance requerida por una policy posterior.

La representación existente ya basta para HCA-R3:

- `valor_min` conserva el límite inferior expresado y `valor_max` el superior;
- `fecha_desde`/`fecha_hasta` conservan la presencia o ausencia de cada extremo;
- los rangos monetarios simétricos (`entre X y Y`) pueden seguir ordenándose;
- los rangos de polaridad (`más de X` y `menos de Y`) no se intercambian;
- un fallback de año solo crea un año cerrado si el extractor temporal no emitió
  ninguna estructura;
- P8 valida los límites conservados y `SoQLBuilder` proyecta solo los extremos
  presentes.

No se añade un tipo arquitectónico, un AST paralelo ni inteligencia nueva en
policy. `QueryFrame` ya transporta estos cuatro campos sin reinterpretarlos.

### Alcance y deuda explícita

Esta decisión no es Time V2. Permanecen fuera de alcance los rangos enero-marzo,
los rangos con días, el día exacto, `de junio`, `mes pasado`, `año pasado`,
negación, control-language y actor hygiene.

---

## ADR-064: La negación preserva la dimensión antes que la ejecutabilidad

**Fecha:** 2026-08-24
**Estado:** Accepted — `DIMENSIONAL_NEGATION_V1: CLOSED`; infraestructura R4-S1,
OBJECT R4-S2, safe policies R4-S3 y lifecycle R4-S4 implementados

### IMPLEMENTATION STATUS (R4-S4)

R4-S1 centralizó el vocabulario en `app/core/negation_markers.py` e introdujo
`NegationRequest` en `app/core/negation_binding.py`. El binding ocurre antes de
los extractores positivos y sus spans se enmascaran conservando offsets. La
metadata viaja como estado efímero del turno, fuera de `QueryFrame`; las
dimensiones todavía no ejecutables cierran de forma segura con aclaración.
R4-S2 convirtió únicamente OBJECT en ejecutable mediante la autoridad existente
`objeto_exclude`, corrigió la proyección a `NOT (coalesce(LIKE, false) OR …)` y
añadió aclaración fail-closed ante overlap positivo/negativo proyectado. La
modalidad mantiene su política existente. R4-S3 cerró CITY, TIME, BUYER, ACTOR,
CONTRACT TYPE y STATE como `CLARIFY_ONLY`; el plan bloqueado no llama SECOP, el
resolvedor de actor ni el fallback LLM. La aclaración en follow-up conserva y
persiste una copia exacta del frame confirmado y no invalida su snapshot de
paginación. DEPARTMENT sólo ejecuta al restar un valor canónico de un
`departamentos_in` finito ya materializado; sin ese conjunto aclara y no genera
`NOT IN`. Evidencia:
`docs/audits/2026-08-25_HCA_R4_S3_SAFE_NEGATION_POLICIES.md`.

R4-S4 cerró el lifecycle conversacional sobre las autoridades ya existentes.
OBJECT soporta crear, preservar, retirar, re-incluir, añadir y reemplazar
exclusiones con traza previa/solicitada/resultante e idempotencia. MODALITY
conserva su motor especializado; `ahora incluye X` restaura X retirándolo del
conjunto negativo, sin forzar una whitelist. DEPARTMENT sólo re-incluye cuando
la procedencia `region` permite reconstruir el conjunto canónico; de lo
contrario aclara sin mutar. No se añadió historial universal de eventos ni se
abrieron las dimensiones `CLARIFY_ONLY`. Evidencia:
`docs/audits/2026-08-25_HCA_R4_S4_NEGATION_LIFECYCLE.md`.

### CONTEXT

HCA-1 reportó tres P1 de negación transversal. HCA-R4 los reprodujo sobre el
runtime de R3 (`9ecf3be`) con el runner offline y con sondeos directos a
`QueryRouter.parse` y `followup_engine.detect_and_merge`:

```text
"contratos de aseo que no sean de barranquilla"
  → ciudad=Barranquilla + entidad_resolved=DISTRITO … DE BARRANQUILLA

"…de la gobernacion de sucre sin alimentacion"
  → objeto=[alimentacion]

T1 "aseo en valledupar" / T2 "menos los de 2025"
  → fecha_desde=2025-01-01 + fecha_hasta=2025-12-31
```

La reproducción amplió el daño más allá de lo reportado:

- `en atlantico menos barranquilla` → `departamento=Atlántico` **+**
  `ciudad=Barranquilla`: el complemento exacto de lo pedido;
- T2 `menos barranquilla` sobre `Atlántico` → `intent=change_scope` que **borra
  Atlántico** y deja el valor rechazado como alcance;
- T2 `sin alimentacion` sobre tema `alimentacion` → frame idéntico al previo:
  **no-op silencioso**;
- `donde el proveedor no sea X` → params `{dataset}`: **drop total**;
- `objeto` + `objeto_exclude` con el mismo valor → SoQL insatisfacible y **cero
  filas fabricado**, indistinguible de un cero real.

SECOPPAL sí ejecuta negación: la modalidad tiene exclusión completa
(`modalidad_exclude_procedure_ids` → `NOT IN`, verificado PASS). Es el único
subsistema que entiende que un marcador negativo pertenece a una dimensión.

### PROBLEM

Los extractores estructurados corren antes que cualquier lectura de polaridad y
reclaman el valor sin mirar qué lo gobierna. La arbitración que salva a la
modalidad es **accidental**: su span se consume en el paso 1.c del parser, antes
de que la regex de exclusión corra en el paso 11. El mismo mecanismo rompe la
geografía, porque la ciudad también consume su span (paso 10.5) y deja la cola
de `excepto` vacía.

Mismo patrón raíz que ADR-063 —
`NORMALIZATION_ERASES_USER_INTENT_BEFORE_POLICY` — en otra dimensión: allí la
normalización monetaria borraba dirección; aquí la extracción borra polaridad.

### DIMENSIONAL_NEGATION_MODEL

Un marcador de exclusión se resuelve a una petición tipada
`NegationRequest(dimension, value_span, scope_anchor, marker, verdict)` **antes**
de decidir si es ejecutable. Siete clases, no una:

| clase | ejemplo | dimensiones |
|---|---|---|
| `VALUE_EXCLUSION` (vocabulario abierto) | `sin alimentación` | OBJECT |
| `SET_EXCLUSION` (vocabulario cerrado) | `excepto contratación directa` | MODALITY, DEPARTMENT, CONTRACT TYPE, STATE |
| `SCOPE_EXCLUSION` (resta territorial) | `que no sean de Barranquilla` | CITY, DEPARTMENT, BUYER |
| `TEMPORAL_EXCLUSION` (hueco) | `menos los de 2025` | TIME |
| `CONTRADICTORY_NEGATION` | `alimentación pero sin alimentación` | cualquiera |
| `UNSUPPORTED_NEGATION` | proyección insegura | cualquiera |
| `POLARITY_INVERSION` (**no es exclusión**) | `no mayores de 500 millones` | MONEY |

Separar `POLARITY_INVERSION` es requisito de seguridad, no taxonomía: el
marcador `menos` es compartido entre `por menos de 500 millones` (comparador,
correcto hoy) y `menos los de 2025` (exclusión).

La resolución de dimensión reutiliza la disciplina de la modalidad: los
marcadores se evalúan **solo en el texto fuera de spans ya resueltos**
(`has_external_negative_operator`), con lista de anti-negación. Por eso
`contratación directa sin ofertas` —alias real— resuelve a inclusión. OBJECT es
el destino por defecto del **residuo**, no de la negación.

### SUPPORTED_V1

1. **OBJECT exclusion.** La cadena `objeto_exclude` ya existe completa —parser →
   frame → `exclude_topic` → `MutationPolicy` → `NOT (…)` en SoQL— y está
   verificada extremo a extremo. Falta solo el disparador lingüístico: la regex
   acepta `excluye|excluir|excepto` anclada al final y no acepta `sin`,
   `que no sean`, `salvo`, `excluyendo` ni el subjuntivo (`excluyan` fuga a
   `objeto`).
2. **MODALITY exclusion.** Existente, comportamiento intacto. Única intervención
   admisible: extraer `_EXTERNAL_NEGATIVE_OPERATORS` / `_ANTI_NEGATION_PATTERNS`
   a un módulo compartido de una sola responsabilidad, con pruebas de
   equivalencia exacta. Duplicar la lista garantizaría deriva entre dimensiones.
3. **DEPARTMENT set-exclusion con alcance positivo explícito.** Se resta de
   `departamentos_in` **antes** de construir el SoQL. Sin predicado negativo,
   sin semántica de NULL, sin tocar `SoQLBuilder`.

### DEFERRED

- **CITY exclusion** (con o sin ancla) → CLARIFY. Precondición: gazetteer
  municipal + `ciudad_resolved` con proyección por igualdad + medición de
  NULL/centinela en `ciudad`.
- **TEMPORAL exclusion** → `TEMPORAL_NEGATION_V1 = CLARIFY_UNSUPPORTED`.
- **Geografía negativa sin alcance positivo** (complemento nacional) → CLARIFY.
- **BUYER** y **ACTOR** exclusion → CLARIFY. Identidad no resuelta: ecosistemas
  de entidad y integrantes de UT (Riesgo D, ADR-042).
- **CONTRACT TYPE** y **STATE** → sin demanda reportada; baratos después.

Se aparta de la frontera sugerida en el encargo (*city exclusion cuando existe
alcance mayor*): el alcance positivo resuelve la **amplitud** pero no la
**proyección**. `ciudad` se proyecta por subcadena
(`unaccent(UPPER(ciudad)) LIKE '%x%'`) sobre una columna sin canonicalizar y sin
gazetteer, así que `NOT LIKE '%cali%'` borra Calima y `NOT LIKE '%san andres%'`
borra San Andrés de Sotavento y de Cuerquia. Un LIKE positivo demasiado ancho
devuelve filas de más y el usuario las descarta; un `NOT LIKE` demasiado ancho
**borra filas correctas de forma invisible**. `DEPARTMENT set-exclusion` entrega
la misma clase de utilidad con riesgo nulo.

### CONFLICT_POLICY

Hay contradicción cuando el **mismo valor**, en la **misma dimensión**, aparece
a la vez en el conjunto positivo y en el negativo tras normalización —incluida
la expansión morfológica—. Política V1: **CLARIFY, fail-closed**, tanto en el
mismo turno como por herencia entre turnos. No se adivina cuál de las dos
lecturas quiso el usuario; su respuesta a la aclaración es la telemetría que
permitirá decidirlo después con datos.

Replica el precedente de `ModalityFilter.__post_init__`, que lanza `ValueError`
ante `include ∩ exclude ≠ ∅`. La diferencia entre modalidad y objeto no puede
ser el azar de qué subsistema se escribió con más cuidado. El orden de los
parsers no decide quién gana.

### NULL_SEMANTICS

HCA-R4.1 midió la fuente en vivo el 2026-08-24. En los campos CITY, NULL y
vacío fueron 0 en ambos datasets; el desconocido se codifica exactamente como
`No Definido`: 2 154 643/9 055 609 filas de `ciudad_entidad` en `p6dx-8zbt`
(**23,79 %**) y 1 286 689/5 980 312 de `ciudad` en `jbjy-vk9h`
(**21,52 %**), sin otras variantes de centinela observadas. En DEPARTMENT, el
mismo snapshot dio 582 818 (**6,44 %**) y 120 469 (**2,01 %**): la cifra de
diseño 6,61 % / 2,01 % ya no se reproduce para Procesos y sí se reproduce, al
redondear, para Contratos. Las consecuencias siguen siendo opuestas: NULL bajo
lógica de tres valores hace que `NOT (x LIKE …)` **elimine** la fila;
`No Definido` es una cadena real y la **incluye**.

Política: una exclusión debe declarar su tratamiento del desconocido, y el
default correcto es **incluirlo y decirlo**. Excluir el desconocido convierte la
resta de una ciudad en la resta de una ciudad más el 21–24 % del universo
observado; 2–7 % es el rango de DEPARTMENT. Ese es el drop silencioso que el
invariante prohíbe.

Defecto latente detectado en el mecanismo **ya en producción**: la cláusula
`NOT (f₁ LIKE … OR f₂ LIKE …)` de `objeto_exclude` no es null-safe. La
medición encontró 5 NULL en `nombre_del_procedimiento` y 1 en
`descripci_n_del_procedimiento` de Procesos, sin solape; ambos campos de
Contratos tuvieron 0 NULL. Con un término sintético ausente, la forma actual
eliminó las 6 filas de Procesos y la forma
`COALESCE(… LIKE …, false)` preservó 6/6; con controles que sí contenían el
término, ambas formas conservaron la exclusión. La corrección es semántica,
aunque el impacto vivo medido sea hoy de seis filas.

R4-S2 cerró este defecto para OBJECT. La forma NULL-safe fue verificada de nuevo
en vivo sobre Procesos y Contratos; las seis filas NULL de Procesos sobrevivieron
y las filas con el término excluido continuaron fuera del resultado.

### FOLLOWUP_POLICY

Regla estructural: **un valor gobernado por un marcador negativo nunca puede
servir de evidencia para clasificar la intención del turno.** La clasificación
corre sobre el residuo positivo; si está vacío, el turno es de exclusión y no de
cambio de alcance.

- Una política de mutación de exclusión solo puede tocar su campo negativo
  (`MutationPolicy({objeto_exclude})` ya lo hace). Esa restricción es lo que
  impide estructuralmente que `menos barranquilla` reemplace el alcance.
- Una exclusión no ejecutable en seguimiento **no es un no-op**: produce un turno
  de aclaración y deja el frame anterior intacto.
- Una exclusión sin alcance positivo previo ni en el turno es demasiado amplia
  (precedente: P15 bloquea oportunidades sin alcance).
- Las exclusiones deben poder retirarse. R4-S4 añadió el slot acotado
  `objeto_exclude` a `requested_clear_fields` y una autoridad específica de
  mutación; sólo las construcciones explícitas autorizadas lo activan.
- La expansión morfológica se conserva en la exclusión (simetría con el
  positivo) pero **se declara** en traza y respuesta: `sin alimentación` también
  excluye `comedor`, `nutrición`, `alimentos`.

### INVARIANTS

**`INV-NEGATION-PRESERVES-DIMENSION`** — una negación conserva qué dimensión se
niega, qué valor se niega, dentro de qué alcance, y si la exclusión puede
ejecutarse con seguridad. Ninguna dimensión puede recibir un valor **positivo**
derivado de un span gobernado por un marcador negativo.

**`INV-NO-SILENT-NEGATION-DROP`** — si el usuario usa un marcador inequívoco de
exclusión sobre un span resoluble y SECOPPAL no puede representarlo, aclara o
avisa. Nunca lo convierte en positivo; nunca lo ignora en silencio. Ejecutar la
parte positiva ignorando la exclusión **no cumple**: devuelve un superconjunto
que contiene precisamente las filas rechazadas.

**`INV-UNSUPPORTED-NEGATION-DOES-NOT-MUTATE-QUERY`** — una negación bindeada
sin capacidad segura no altera filtros positivos, no activa resolución de
actor/entidad y no ejecuta SECOP. Produce una aclaración que reconoce la
exclusión entendida.

**`INV-NEGATION-CLARIFY-IS-TRANSACTIONAL`** — una aclaración negativa en
follow-up persiste exactamente el frame confirmado anterior; no conserva un
delta parcial, no crea `actor_pending` y no invalida el universo paginable de
esa búsqueda.

**`INV-NEGATION-LIFECYCLE-SYMMETRIC`** — una exclusión OBJECT creada por el
sistema puede retirarse, re-incluirse o reemplazarse mediante lenguaje acotado
sin crear otra representación y conservando el resto del frame confirmado.

**`INV-NEGATION-REMOVAL-IS-DIMENSIONAL`** — retirar una exclusión sólo muta el
campo negativo de su dimensión; no borra el tema positivo, el alcance ni otros
filtros.

### TEST_REQUIREMENTS

24 casos pre-registrados **antes** del código en
`tests/fixtures/audits/negation_focal_v1.yaml`, ejecutados explícitamente con el
runner HCA (`--corpus`) y mantenidos byte a byte. Cubren los 12 exigidos más
siete derivados de la reproducción
(anti-negación ESAL, alias con `sin`, subjuntivo, span de ciudad consumido,
buyer, actor, control monetario) y dos anti-metamórficas:

- **N5 ≢ N6** — `de alimentación sin transporte` y `sin alimentación ni
  transporte` tienen significados opuestos y hoy producen el mismo frame;
- **N2 ≢ N12** — una consulta y su negación no pueden producir el mismo frame.

Innegociables del bloque de implementación: `NEG-N4` y `NEG-N15` (modalidad) y
`NEG-N13` (dinero) son controles anti-regresión; el corpus congelado
`conversation_holistic_v1.yaml` (`sha256 262c225c…99ffba`) no se edita.

Las tres mediciones preimplementación quedaron cerradas por HCA-R4.1: tasa de
NULL de los cuatro campos reales usados por `objeto_exclude`; NULL, vacío y
centinela de `ciudad`/`ciudad_entidad`; y benchmark alternado de la disyunción
de fechas en `p6dx-8zbt`. El transporte temporal fue SAFE en el sondeo (sin
timeouts; p90 fetch 0,4582 s, count 2,5275 s), pero la comparación es
fuente-a-fuente y no activa soporte TIME. Evidencia:
`docs/audits/2026-08-24_HCA_R4_1_NEGATION_LIVE_SOURCE_PRECHECK.md`.

Resultado R4-S4 del fixture focal: **22 PASS / 2 FAIL / 0 errores**. Los dos
restantes se clasificaron sin cambiar código, fixture ni runner:

- `NEG-N15`: `VALID_DEFERRED`; la inclusión de modalidad es correcta, pero el
  plan exige tema o alcance adicional para ejecutar una consulta sólo-modalidad.
- `NEG-ANTI-002`: `WRONG_ORACLE_CANDIDATE`; la consulta negativa difiere en
  `ciudad`, pero el oráculo exige además diferencia individual en
  `entidad_resolved`, que correctamente es `None` en ambos frames desde S3.

La regresión completa cerró con **8436 passed, 2 skipped, 23 xfailed**;
`query_corpus` con **89/89**, diálogo E2E con **44/44**, fixture HCA congelado
con **164 PASS / 31 FAIL / 0 errores** y cero cambios de estado, frame o detalle
frente al baseline. `HCA-ACTOR-011` permanece FAIL: el arreglo general de Actor
Pending se mantuvo fuera del alcance.

### Alcance y deuda explícita

Esta decisión no soporta toda la negación del español. No cubre negación de
constituyentes coordinados complejos, cuantificadores ni doble negación
(`no sin`). El objetivo es **dejar de ejecutar lo contrario de lo que pidió el
usuario**.

Recomendación independiente, de una línea y sin depender de que se implemente
exclusión alguna: **desacoplar `_CITY_TO_ENTITY` de la detección de ciudad
cuando hay marcador negativo**. Ese mapeo convierte una exclusión geográfica en
una restricción de comprador y duplica el daño de la inversión.

Guía técnica: `docs/architecture/DIMENSIONAL_NEGATION_SEMANTICS.md`.

---

## ADR-065: Una autoridad temporal determinística y un reloj civil inyectable

**Fecha:** 2026-08-25
**Estado:** Accepted — `TIME_SEMANTICS_V2: CLOSED` para las capacidades R5

### Contexto

La semántica temporal estaba dividida entre dos capas. `QueryRouter._extract_dates()`
proyectaba años y un único patrón mes+año, sin reloj. `dialogue_policy` ya
reconocía `este mes`, `mes pasado`, semanas y últimos N días usando un reloj
contextual, pero `QueryPlan` sólo proyectaba esas señales si el parser no había
creado otro extremo. En el workflow real varias señales se perdían y los
extractores genéricos podían reinterpretar sus tokens como objeto contractual.

Los rangos específicos tampoco tenían precedencia: `entre enero y marzo de
2026` terminaba como marzo porque el último match genérico sobrescribía al
primero. Un día exacto colapsaba al mes y una fecha civil imposible se ejecutaba
como el mes completo. El merge conversacional heredaba por separado
`fecha_desde` y `fecha_hasta`, por lo que un scope nuevo abierto podía conservar
un extremo viejo.

### Decisión

1. **`temporal_semantics.resolve_temporal_scope()` es la única autoridad
   lingüística temporal.** Vive en su propio módulo —no dentro de
   `dialogue_policy`— porque es una responsabilidad separada y ambos
   consumidores la necesitan sin arrastrar el clasificador. Evalúa una gramática
   determinística en orden de especificidad y devuelve una sola
   `TemporalResolution`. `QueryRouter` la consume y sólo proyecta a
   `fecha_desde`/`fecha_hasta`; `dialogue_policy` la adapta al contrato histórico
   de señales y `QueryPlan` delega el veredicto de ejecución en
   `temporal_gate.temporal_plan_block()`.
2. **`civil_today()` es el único punto de lectura del reloj civil.** El valor
   viaja por `HandlerContext` y el estado del workflow hasta parser y policy.
   `SecopalWorkflow.run_query(today=...)` y el runner HCA permiten congelarlo.
3. **Mes —y día inequívoco— sin año usa el año civil actual.** Esta política A
   se aplica incluso en enero ante `diciembre`: puede señalar diciembre futuro.
   No se inventa “el más reciente no futuro”. Los rangos sin año aclaran porque
   heredar o inventar el año cambiaría materialmente la intención.
4. **La precedencia es semántica, no una secuencia de mutaciones:** rangos de
   día/mes/año → día exacto → relativos → abiertos → mes → año. Un rango
   invertido o una fecha imposible aclara; nunca se ordena ni se degrada a mes.
5. **La mutación temporal de follow-up es atómica.** La señal efímera
   `_temporal_replace_scope` permite limpiar ambos extremos anteriores antes de
   aplicar el scope nuevo, incluida la ausencia intencional de un extremo. La
   señal se elimina antes de SoQL, persistencia y fingerprint.
6. **Todo span temporal reconocido se enmascara antes de actor, entidad, estado,
   dinero y objeto.** La protección léxica impide además la corrección
   `dias → vias`.

### Semántica cerrada en R5

- día, mes y año cerrados; mes sin año según la política anterior;
- rangos explícitos de días, meses y años, incluido cross-year con ambos años;
- `desde`/`hasta` para año o mes;
- `este/mes pasado`, `este/año pasado`, hoy, ayer;
- semana civil ISO actual/anterior y últimas N semanas como ventana trailing
  `today - N*7 .. today`;
- últimos N días conserva el contrato previo `today - N .. today`;
- fecha inválida, rango invertido, rango sin año y marcador inequívoco no
  soportado terminan en aclaración determinística, nunca `needs_llm`.

### Consecuencias y límites

No se crea AST temporal ni se modifica `SoQLBuilder`: la representación sigue
siendo `fecha_desde`/`fecha_hasta`. Continúan fuera de alcance la negación
temporal ejecutable, huecos/NOT BETWEEN, quarters, semestres, años fiscales,
recurrencia, días hábiles, deadlines y estado contractual. `menos los de 2025`
conserva la política de ADR-064: TIME/CLARIFY y cero proyección positiva.

### Evidencia de aceptación

- HCA congelado: **164/195 → 171/195** sobre el mismo corpus
  (sha `262c225c…`, sin editar). La línea base se remidió de forma
  independiente en un worktree limpio de `main` y el diferencial se comparó
  caso por caso, no por el neto: corregidos `HCA-TIME-003/004/008/010/012/014/015`,
  **regresiones: ninguna**. Ambas corridas se hicieron con el extractor LLM
  caído (HTTP 402, saldo agotado) y por tanto en modo regex-only; la comparación
  es válida porque la condición fue idéntica en ambos lados, y R5 es
  precisamente trabajo determinístico sin LLM;
- los 15 `HCA-TIME-*`: **15/15**;
- `time_v2_focal.yaml`: **32/32** con reloj por caso relativo;
- fuente Socrata: 80 filas en día/mes/rango/abierto, **0 violaciones**;
- fixture de negación intacto: **22/24** basal, 2 deudas de oráculo conocidas.

Informe reproducible:
`docs/audits/2026-08-25_HCA_R5_TIME_V2.md`.

---

## ADR-066: El lenguaje de control no es contenido de búsqueda

**Fecha:** 2026-08-25
**Estado:** Accepted — `CONTROL_LANGUAGE: CLOSED`

### Contexto

Una consulta mezcla dos lenguajes en una sola frase. **CONTENIDO** nombra lo que
se busca (`obra`, `mantenimiento`, `Cali`) y pertenece al `WHERE`. **CONTROL**
dice cómo ordenar, paginar o enmarcar ese contenido (`ordenados por mayor
valor`, `los más antiguos primero`) y qué clase de pregunta se está haciendo
(`qué está contratando`, `procesos que cierran`).

La separación existía solo por accidente. La regex de orden consumía el
*criterio* (`mayor valor`) pero no el *comando* a su alrededor (`ordenados`), y
el residual iba entero al extractor temático. `contratos de obra en cali
ordenados por mayor valor` viajaba a SECOP con `LIKE '%ordenados%'` y devolvía
**0 filas** sobre un universo que tiene 50. El sistema convertía una consulta
respondible en un vacío, sin decirlo.

Dos autoridades desalineadas agravaban el efecto en conversación: el parser
reconocía un vocabulario de orden y el clasificador de follow-up otro, más
estrecho. `los más antiguos primero` no estaba en ninguno, así que se leía como
tema nuevo, cambiaba el universo y no ordenaba.

### Decisión

1. `app/core/control_language.py` es la autoridad única del lenguaje de control.
   Recibe el residual positivo y devuelve un `ControlRequest` con la señal de
   orden y el residual sin los spans consumidos. **No es una lista de palabras
   prohibidas**: el reconocimiento es por span y contextual, igual que el
   binding dimensional de ADR-064.
2. Un span reconocido como control nunca sobrevive al residual —
   `INV-CONTROL-LANGUAGE-NOT-CONTENT` — y por tanto no puede volverse `objeto`,
   actor, geografía ni disparador de ecosistema de entidad.
3. El span cubre el comando completo, no solo el criterio. Se ensancha hacia la
   izquierda sobre pegamento gramatical, y sobre artículos y sustantivos de
   relleno **solo si alcanza un imperativo real de «ordenar»** o un ancla
   `primero`. Un criterio sin comando alrededor no se traga contenido vecino —
   `INV-CONTROL-SPAN-DOES-NOT-CLASSIFY-CONTENT`.
4. Las parejas direccionales explícitas se leen antes que cualquier criterio de
   un solo lado. `de menor a mayor valor` contiene `mayor valor`; leerlo de un
   lado invierte la intención — el patrón `NORMALIZATION_ERASES_USER_INTENT` de
   HCA-R2. `ordering_signal` gana `valor_asc` y `fecha_asc`, y `soql_builder`
   los proyecta a `ASC` con el desempate por identificador en la misma
   dirección.
5. La morfología imperativa de «ordenar» es control con criterio o sin él: un
   imperativo incompleto se consume pero **no infiere dirección** — el orden
   presentacional por defecto se mantiene antes que adivinar uno.
6. Los verbos que fijan el modo de la pregunta (`está contratando`, `piensa
   contratar`, `cierran`) son control y conservan su complemento temático:
   `está contratando mantenimiento` mantiene `mantenimiento` —
   `CONTROL_VERB_CANNOT_BECOME_TOPIC`.
7. Un follow-up de solo orden cambia el `ORDER BY` y nada más:
   `INV-ORDERING-FOLLOWUP-PRESERVES-UNIVERSE`, verificado comparando el `WHERE`
   generado antes y después, no solo el frame.
8. Un comando es frontera del objetivo de una negación, y solo cuando va
   explícitamente encabezado por comando. `sin alimentación ordenados por mayor
   valor` excluye el objeto y ordena; `sin obras nuevas` conserva su
   comportamiento previo intacto.
9. La etiqueta de orden en cabecera vive en `order_semantics.ordering_label`.
   Estaba inlined en tres consumidores como un condicional de dos ramas; con
   dos direcciones nuevas, tres copias habrían anunciado «de mayor valor» sobre
   una página de menor a mayor.

### Consecuencias y límites

`params["limit"]` **no** es cardinalidad de display sino tamaño del pool de
candidatos que se valida y rankea. Un `top 10` que lo fijara encogería el pool
antes de validar y cambiaría *qué* filas sobreviven. R6 reconoce y consume el
span de `top N` —no puede contaminar— y aplica el orden que lo acompaña, pero
**no** la cardinalidad: `DEFERRED_CONTROL_LIMIT`.

Sobre `cierran`, la decisión garantiza únicamente que no es un tema contractual.
La semántica de deadline queda exactamente como estaba; no se abrió trabajo de
deadlines. `menor cuantía` se excluye del vocabulario ascendente porque es una
**modalidad** de contratación, no una dirección de orden.

`app/core/query_plan.py` no se tocó: sigue en 299 LoC y la `NEXT_TOUCH_RULE`
no se invocó.

### Evidencia de aceptación

- HCA congelado: **171/195 → 175/195** sobre el mismo corpus (sha
  `262c225c…`, sin editar; sha idéntico antes y después). Diferencial caso por
  caso, no por el neto: corregidos `HCA-INTENT-007`, `HCA-INTENT-008`,
  `HCA-STATE-006`, `HCA-ORDER-004`; **rotos: 0**. Ambas corridas en modo
  regex-only por el extractor LLM caído (HTTP 402), condición idéntica en ambos
  lados y trabajo determinístico sin LLM;
- corpus focal `control_language_v1.yaml`: **42/42**, pre-registrado
  (`make audit-control-language-v1`);
- adversarial generado: **516 comprobaciones**, 0 hallazgos en las once clases
  de fuga buscadas;
- fuente Socrata, caso cero histórico: `b661a09` **0 filas** con
  `LIKE '%ordenados%'` → R6 **50 filas** ordenadas por valor descendente, sin
  ese predicado. `fecha_asc` verificado ascendente estricto contra la fuente;
- no-regresión: suite 8554 passed / 2 skipped / 24 xfailed · query corpus 89/89 ·
  dialogue corpus 60/61 (único FAIL `GOB-DEPTO-001`, basal) ·
  `make audit-negation-v1` 22/24 idéntico a `b661a09` ·
  `make audit-time-v2` 32/32.

Informe reproducible:
`docs/audits/2026-08-25_HCA_R6_CONTROL_LANGUAGE.md`.

---

## ADR-067: Estado léxico exacto frente a familia semántica

**Fecha:** 2026-08-26
**Estado:** Accepted — `EXACT_VS_FAMILY_STATE: CLOSED`

### Contexto

`estado_families.py` traducía todo token conocido a una familia y proyectaba
**todos** sus valores. Esa uniformidad producía dos errores opuestos.

`procesos publicados de aseo en cesar` devolvía `estado_del_procedimiento IN
('Publicado','Borrador','En aprobación','Aprobado','Abierto')`: el usuario nombró
un valor literal del dataset y recibió borradores. En el otro extremo,
`contratos en ejecución de la alcaldía de Medellín` declaraba
`estado_family = contrato_activo` en el frame y llegaba a la fuente como
`estado_contrato = 'En ejecución'`, perdiendo `Modificado` y `Prorrogado`, que
ADR-003 declara parte de la misma familia.

La causa común no es el catálogo sino la ausencia de una distinción: **nada
garantizaba que la forma de resolver un estado y la forma de proyectarlo
coincidieran**. Una familia podía declararse sin materializarse —la ruta de
`intent_vocabulary` no conoce el dataset y nunca escribió filtros— y un literal
podía materializarse como familia.

### Decisión

Un token de estado se resuelve por **uno de dos contratos**, y la proyección a
la fuente corresponde siempre al contrato con que se resolvió.

1. **Léxico exacto** — el token nombra un valor literal del dataset y proyecta
   ese valor y solo ese. `LEXICAL_EXACT_STATE` tiene precedencia sobre
   `NATURAL_TO_FAMILY` y deriva la ubicación del literal de `ESTADO_FAMILIES` en
   vez de duplicarla. **INV-EXACT-STATE-DOES-NOT-WIDEN-TO-FAMILY**.
2. **Familia semántica** — el token describe una etapa del ciclo de vida sin
   nombrar un valor, y proyecta la familia completa.
   **INV-STATE-FAMILY-PROJECTS-COMPLETE**, aplicado en
   `app/core/estado_projection.py` sobre los params ya resueltos, de modo que
   cubre cualquier ruta que declare familia. **Rellena y nunca ensancha**: un
   campo de estado con valores es una decisión previa y se respeta.

El catálogo exacto es deliberadamente corto: un token entra solo si nombra un
literal real, su familia tiene más de un valor para ese dataset y no es a la vez
vocabulario que describe la familia entera. Por lo tercero quedan fuera
`abiertos` y `vigentes` —lenguaje natural de la oferta abierta— y `en ejecución`,
al que ADR-003 asigna los tres estados de contrato activo.

### Consecuencias y límites

La policy de oportunidad vigente (O1B: `Publicado` + `Abierto` más ventana de
descubrimiento) no cambia. El invariante de proyección corrige además un caso
que nadie había reportado: `procesos desiertos` declaraba `no_procede` y
consultaba **sin filtro de estado alguno**.

Esta decisión **no** toca el alcance de liquidación. Qué significa *liquidado*
en SECOP (HCA-STATE-003, HCA-SOURCE-002) sigue sin autoridad suficiente.

`app/core/query_plan.py` no se tocó: sigue en 299 LoC y la `NEXT_TOUCH_RULE` no
se invocó.

### Evidencia de aceptación

- HCA congelado, corpus sha `262c225c…` sin editar: `HCA-STATE-005` y
  `HCA-STATE-007` FAIL → PASS, **0 rotos**;
- corpus focal `hca_final_p1_fixes.yaml` 40/40 (`make audit-hca-final-p1`);
- fuente Socrata: `Publicado` y `Borrador` son valores reales y disjuntos; los
  tres estados de `contrato_activo` existen por separado — el ensanchamiento
  traía filas no pedidas y el truncamiento ocultaba filas legítimas;
- diferencial de SoQL sobre 50 consultas adversariales contra `eb8cfe1`: los
  únicos cambios de filtro de estado son los previstos.

Detalle: `docs/decisions/2026-08-26_ADR-067_exact-state-vs-state-family.md`.
Informe: `docs/audits/2026-08-26_HCA_FINAL_P1_RUNTIME_FIXES.md`.

---

## ADR-068: El estado derivado de ejecución no persiste como estado del usuario

**Fecha:** 2026-08-26
**Estado:** Accepted — `DERIVED_EXECUTION_STATE: CLOSED`

### Contexto

`resolved_params` mezclaba dos cosas de naturaleza distinta: el **estado
semántico del usuario** —tema, territorio, comprador, dinero, tiempo, estado,
modalidad— y el **estado derivado de ejecución**, que el motor calcula *después*
de consultar la fuente para enriquecer la respuesta.

`entidad_ecosystem` es del segundo tipo. `normal_flow` lo inyecta tras ejecutar,
reconstruye la SoQL para la respuesta asesora y **no vuelve a consultar**: las
filas que el usuario vio nunca lo aplicaron. Como se persistía en el turno, el
siguiente follow-up lo heredaba como filtro solicitado. Tras una búsqueda en
Cali, pedir `los más antiguos primero` añadía al `WHERE` un `OR` con diez
compradores del ecosistema municipal: el usuario pidió reordenar y recibió otro
universo.

Tres capas ya se defendían por su cuenta, cada una con su lista —buffer de
paginación, repesca de página 2, guardado de radares—. Ninguna formalizaba la
categoría, y faltaba justamente la cuarta: la persistencia conversacional, que
es por donde se propaga.

### Decisión

1. `app/core/derived_execution_state.py` declara la categoría una sola vez.
   `entidad_ecosystem` es su primer miembro.
2. **El historial conversacional guarda estado semántico del usuario, nunca
   estado derivado de ejecución.** El filtro se aplica en
   `ConversationStore.append_turn` y no en los cuatro handlers que escriben
   turnos: el invariante es del historial — lo que entra ahí es exactamente lo
   que el próximo follow-up hereda.
3. **INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE**: para un follow-up que solo
   cambia el orden, `FILTERS(WHERE(T2)) == FILTERS(WHERE(T1))`. Las únicas
   diferencias admisibles son `ORDER BY` y el cursor de paginación.

El gate se pone sobre las **source calls reales**, no sobre el `QueryFrame`: el
defecto vivía justo en el hueco entre ambos.

### Consecuencias y límites

Purgar la clave derivada no se lleva un universo legítimo. Un ecosistema que el
usuario declara no viaja por `entidad_ecosystem` —`contratos del ICBF en
Antioquia` llega como `entidad_resolved`/`entidad_like`— y `should_expand_ecosystem`
ni se activa cuando hay entidad: la expansión post-query solo ocurre en ausencia
de comprador, que es cuando nadie la pidió.

No se introduce event sourcing ni procedencia por campo: la distinción se
resuelve con una lista corta porque la clave que la necesitaba es una sola. Si
aparece otro enriquecimiento post-query persistido, su lugar es esa lista y no
una cuarta deny-list local.

### Evidencia de aceptación

- `tests/test_ordering_followup_source_universe.py`, 10 casos que comparan las
  source calls de T1 y T2 (territorio municipal y departamental, comprador
  explícito, dinero, tiempo, estado, negación, modalidad). **Verificado por
  mutación**: sin el fix, 4 fallan;
- HCA-FOLLOWUP-016 re-adjudicado: `WHERE` idéntico entre turnos, orden correcto,
  `obra` preservada como `contract_type_include`. Sigue rojo en el HCA bruto
  **solo** por `objeto_has:[obra]` — deuda de oráculo, no de runtime, que pasa
  al Oracle Patch;
- HCA congelado **0 rotos**.

Detalle: `docs/decisions/2026-08-26_ADR-068_derived-execution-state.md`.
Informe: `docs/audits/2026-08-26_HCA_FINAL_P1_RUNTIME_FIXES.md`.

---

## ADR-069: Una forma lingüística que una autoridad no resuelve no se degrada en silencio

**Fecha:** 2026-08-26
**Estado:** Accepted — `HCA_FRESH_SYSTEMIC_REMEDIATION: PASS`

### Contexto

La adjudicación causal de los hallazgos fresh demostró que las siete causas raíz
declaradas eran siete síntomas de un solo mecanismo:
`ENUMERATED_SURFACE_FORM_VOCABULARY_WITH_FAIL_OPEN_DEFAULT`. Cada dimensión
semántica se reconoce con una lista de formas superficiales mantenida a mano, y
un token ausente de su lista no produce error ni aclaración: cae al destino por
defecto o desaparece, y el sistema ejecuta una consulta bien formada y
semánticamente equivocada.

Lo decisivo no es que hubiera huecos —siempre los habrá— sino qué pasa al
encontrar uno. El comportamiento por defecto era continuar. Por eso el mismo
lema con distinta flexión daba comportamientos opuestos (`compra` sí, `compró`
no; `participar` sí, `licitar` no; `millones` sí, `mil` no), y por eso la peor
manifestación no fue perder una restricción sino invertirla: `procesos que no
estén abiertos` devolvía exactamente y solo los abiertos.

### Decisión

**INV-NO-SILENT-SEMANTIC-FALLBACK.** Una forma lingüística materialmente
estructural que una autoridad no puede resolver no puede degradarse en silencio
a otra dimensión: no puede volverse objeto, desaparecer, invertir su polaridad,
activar una dimensión fabricada ni producir un filtro parcial.

El invariante es común; las autoridades siguen separadas — no se crea un gestor
semántico global. Reconocimiento léxico y semántica quedan en capas distintas y
el reconocimiento se **construye** desde los catálogos semánticos. La polaridad
se resuelve antes que cualquier extractor positivo. Un rango explícito se aplica
entero o pide precisión. Una corrección ortográfica no puede crear una dimensión
semántica, y por eso el lenguaje funcional se protege **antes** del corrector.
Las familias lingüísticas se declaran una vez y todas sus ramas se construyen
desde ellas. `F601` entra al gate: la clave duplicada que proyectaba el
complemento del estado pedido era detectable por el linter.

### Consecuencias y límites

Fallar cerrado cuesta cobertura a propósito: `entre 500 mil y 3 millones` antes
ejecutaba con un techo y ahora pide precisión. El invariante no promete cerrar
todos los huecos de vocabulario —promete que un hueco produzca una pregunta y no
una consulta equivocada—. La vigencia deja de ser un estado y pasa a ser una
familia con proyección por dataset. No se introduce LLM-as-parser, embeddings ni
dependencia NLP: el paradigma copulativo es morfología cerrada y las gramáticas
de control se componen de sus partes. Queda fuera, declarado, la exclusión
ejecutable de estado: sigue no soportada y aclara.

### Evidencia de aceptación

- Gate predictivo **12/12**, con la línea base congelada verificada por SHA y
  sus expectativas sin tocar;
- Gate anti-vocabulario sobre las familias completas —no las formas observadas—,
  que encontró una fuga que los nueve escenarios nunca mostraron;
- Canario vivo contra SECOP: `semantic_contract_violations = 0`,
  `hard_filter_violations = 0`;
- HCA canónico **188/195, 0 rotos**, corpus byte-idéntico;
- Plan fresh congelado **30 → 37 de 52**, sin regresiones;
- `F601`: 3 hallazgos antes, 0 después, ya dentro del gate.

Detalle: `docs/decisions/2026-08-26_ADR-069_no-silent-semantic-fallback.md`.
Informe: `docs/audits/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION.md`.

## ADR-070: SODA3 `/query` como transporte Socrata productivo por defecto

**Fecha:** 2026-08-28
**Estado:** Accepted — `SODA3_TRANSPORT_V1_INTEGRATED`

### Contexto

SECOPPAL hablaba con Socrata por un único camino: `GET /resource/{dataset}.json`
vía `sodapy`, con el timeout horneado en el objeto `Socrata` y una sola llamada en
línea dentro de `SecopClient`. El discovery de `/query` midió ese camino contra el
endpoint moderno y encontró mejora de rendimiento sostenida con paridad estricta de
filas en los pares comparables.

La migración no era un cambio de URL. El camino clásico no tiene el eje
`orderingSpecifier`, y el discovery descubrió que imponerlo indiscriminadamente
rompía justo la ruta donde el runtime retira el `ORDER BY` a propósito para
abaratar la consulta —el fallback H4—. El problema real era decidir qué declararle
al backend y desde dónde; el transporte HTTP era la parte fácil.

### Decisión

**SODA3 `/query` es el transporte productivo por defecto**; `classic` permanece
como rollback explícito, no como red de seguridad automática.

La selección se hace con `SECOPPAL_SOCRATA_TRANSPORT`, vive en
`SecopalWorkflow.__init__` y solo ahí, y **falla cerrado**: ausencia de
configuración resuelve a `soda3`, `classic` activa el camino legacy y cualquier
otro valor aborta la construcción del workflow. Cambiar de transporte exige
reconstruir o reiniciar el workflow: no es un interruptor caliente. **No existe
fallback automático SODA3 → classic**: son dos transportes seleccionables, no una
cascada.

`orderingSpecifier` es **intención de ejecución declarada por la ruta**, nunca una
lectura del texto del SoQL —la ausencia de `ORDER BY` también es legítima en
`count`, `aggregate` y direct lookup, que no son H4—:

```text
query normal → total    count → total    aggregate → total
fallback H4 query → no_preserve          fallback H4 count → total
```

SECOPPAL sigue siendo autoridad única sobre el SoQL: el transporte lo emite
literal, sin reescribirlo ni normalizarlo, contra
`POST /api/v3/views/{dataset}/query.json`. Errores HTTP, JSON o de forma levantan
excepción tipada y **no se degradan a `[]`** —eso convertiría un 400 de SoQL en un
«no hay resultados» indistinguible, el fallo que ADR-069 prohibió en el plano
semántico, aquí en el del transporte—.

### Consecuencias y límites

`sodapy` permanece instalado: sin él el rollback sería teórico; retirarlo no es
parte de esta decisión. El timeout deja de estar horneado en el transporte y viaja
por llamada, lo que permite compartir la instancia SODA3 y su pool entre clientes
derivados; el camino clásico conserva su semántica previa. La mejora de latencia es
un efecto medido, **no una promesa contractual**: lo que se congela es la
corrección del transporte y la política de ordering. Quedan **fuera de alcance**,
declarados: `orderingSpecifier=discard`, `/export`, SODA3 `page`, `SEARCH`,
`:updated_at`, la retirada de `sodapy`, el fallback automático y deterministic
TOP-N.

### Evidencia de aceptación

- Discovery V0 `GO_SODA3_QUERY`; runtime canary V1 `NO_GO_DIVERGENCE` y
  adjudicación V1.1 `V1_1_BLOCKERS_CONFIRMED`, con las divergencias resueltas una
  por una hasta `GO_INTEGRATION`;
- Microprobe H4 `ORDERING_SPECIFIER_CAUSAL_SIGNAL`: sobre el mismo SoQL, `total`
  0/5 contra `no_preserve` 5/5; replay H4 `H4_NO_PRESERVE_POLICY = PASS` con
  **0 regresiones SODA3-only alcanzables**;
- Productive live smoke `PRODUCTIVE_LIVE_SMOKE_GO`;
- Full runtime dogfood `GO`: **19 turnos, 41 llamadas remotas**, ambos datasets,
  con búsqueda, follow-ups, paginación, direct lookup, Actor Profile,
  analytics/observer, H4 y Radar en `PASS`; `HTTP_CONTRACT_ERRORS = 0`,
  `SODA3_ONLY_REPRODUCIBLE_FAILURES = 0`, regresiones semánticas 0;
- Cutover `dd51ccd`: default live smoke `PASS`, rollback classic `PASS`, suite
  completa **9461 passed, 2 skipped, 24 xfailed, 0 failed**.

Detalle: `docs/decisions/2026-08-28_ADR-070_soda3-default-transport.md`.
Informes: `docs/audits/2026-08-27_SODA3_QUERY_FINAL_ADJUDICATION.md`,
`docs/audits/2026-08-28_SODA3_PRODUCTIVE_LIVE_SMOKE.md`,
`docs/audits/2026-08-28_SODA3_FULL_RUNTIME_DOGFOOD.md`.

## ADR-071: Acceso a SECOP como autoridad separada de visibilidad y etapa

**Fecha:** 2026-08-31
**Estado:** Accepted — `SECOP_ACCESS_V2_CLOSED`

### Contexto

Desde DOGFOOD-URL-005 y ADR-027 el sistema operaba sobre una premisa heredada
nunca medida: `CO1.REQ.*` = requisición / prepublicación → ausencia esperable de
vista pública. De ahí salían un guard que impedía construir la URL desde el id y
una nota de UI que AFIRMABA una etapa ("proceso pre-publicación") en los tres
canales.

El censo completo de `p6dx-8zbt` al 2026-08-31 la falsifica: los **9.096.877**
procesos llevan id `CO1.REQ.*` —el **100%**— y **9.002.823 (98,97%)** traen vista
pública `OpportunityDetail` con `noticeUID=CO1.NTC.*`; las filas con
`noticeUID=CO1.REQ.*` son **0 (0%)**. El prefijo lo llevan todas las filas: no
tiene ningún poder predictivo sobre la publicación. El predictor real es otro,
`estado_del_procedimiento='Borrador'` (**42.580 / 43.304 = 98,3%** sin vista
pública), mientras que `Contratación régimen especial` —la causa que ADR-027
nombraba— da **34.163 / 3.563.571 = 0,96%**, indistinguible de la tasa base del
dataset (1,03%).

Lo único que el censo sí demuestra a favor del guard es distinto y más fuerte:
**cero** filas usan su propio id como `noticeUID`, así que construir la URL desde
`id_del_proceso` daría un enlace roto en el 100% de los casos. El comportamiento
se conserva y el motivo escrito se corrige.

### Decisión

`app/core/secop_access.py` es la **única autoridad sobre cómo llegar a la fuente
oficial**, y responde una sola pregunta: qué vía segura puede ofrecer SECOPPAL
para llegar a SECOP. **No** responde —ni debe empezar a responder— si el proceso
está publicado, abierto, si se puede ofertar, si es prepublicación o qué etapa
tiene.

**Un enlace directo se observa, nunca se construye.** Para ser aceptado exige
esquema `http(s)`, host de la allow-list de SECOP y representar una VISTA del
proceso: se rechazan la pantalla de autenticación (`/login`, 94.054 filas), el
buscador de avisos (`/ContractNoticeManagement`), los hosts externos, los
esquemas inseguros y la URL malformada. El buscador se reconoce por su RUTA y no
por el parámetro `Page=login`, que dejaría pasar la misma pantalla sin él o con
otra capitalización.

**Sin ficha directa siempre queda una acción**, no un diagnóstico: el buscador
público oficial más la referencia observada. `SECOP_PUBLIC_SEARCH_URL` no es una
ruta deducida: es el literal que el propio SECOP publica en `urlproceso` para los
4.130 contratos de `jbjy-vk9h` sin ficha pública. Tres clases: `direct`,
`search_with_reference`, `search`.

**La falta de enlace directo no elimina la oportunidad, no reduce su ranking, no
implica prepublicación y no implica borrador.** El usuario conserva siempre una
vía hacia SECOP. Es el principio de ADR-027 —señalizar, no omitir— conservado y
reforzado: lo señalizado dejó de nombrar una etapa y pasó a ser ejecutable.

**Guardar sigue siendo local**: el Workspace no hace lookup remoto al guardar,
copia el snapshot observado; los ítems legacy sin `secop_url` usan su
`reference`, sin migración ni reescritura del histórico.

### Autoridades

```text
SECOP Access            → navegación a la fuente
Truth Layer visibility  → evidencia sobre visibilidad
Opportunity Stage       → etapa del proceso
Operational State       → lectura operativa
Radar                   → detección / monitoreo
```

Ninguna capa infiere en silencio la respuesta de otra.

### Consecuencias

La decisión de acceso existe en un solo lugar: host, `/login`, ruta de la página,
referencia y URL del buscador dejaron de ser reglas dispersas dentro de
`Formatter` y de las vistas. Feed y detalle de Radar, Workspace, tarjetas de
búsqueda, detalle de direct lookup, procesos seguidos, tarjetas agrupadas,
Telegram y WhatsApp comparten la misma autoridad y, para la misma fila, toman la
misma decisión — hay un test que lo exige.

### Fuera de alcance

Corregir `_classify_url` del Truth Layer; introducir el eje de provenance de URL;
cualquier resolución REQ → NTC, propia o de terceros; cualquier inferencia de
etapa a partir de la ausencia de enlace.

### Deudas aceptadas

- **`TRUTH_LAYER_SEARCH_URL_CLASSIFICATION_DEBT` — `DEFERRED`, impacto en SECOP
  Access UI `NONE`.** `_classify_url` de `truth_layer/visibility_resolver.py`
  solo mira `/login` en el PATH, así que clasifica el buscador de avisos como
  `public` y la fila resuelve a `public_view_available`: una vista pública que no
  muestra ningún contrato. No se corrige aquí a propósito — el contrato semántico
  del Truth Layer está congelado y corregirlo mueve un veredicto de evidencia, no
  una etiqueta de UI, así que merece adjudicación propia. Ninguna superficie
  consulta el Truth Layer para decidir la vía a SECOP. Regresiones que lo fijan:
  `tests/test_secop_access.py`
  (`test_the_contract_management_page_never_becomes_a_direct_link`) y
  `tests/test_url_fallback_channel_parity.py`
  (`test_the_contract_search_page_is_never_presented_as_a_process_view`).
- **`SECOP_SYNTHETIC_URL_PROVENANCE_DEBT` — registrada, no corregida.**
  `Formatter._safe_url` y `SecopClient._normalize_row` todavía pueden fabricar
  ciertas URLs desde ids y referencias, y `secop_access` recibe después un string
  sin saber si fue `observed`, `normalized` o `synthetic`. Contradice
  conceptualmente el contrato "el enlace directo se observa, no se construye",
  aunque hoy no produzca una regresión P0/P1 conocida: para `p6dx-8zbt` las ramas
  constructoras son inalcanzables (el 100% de los ids es `CO1.REQ.*`). Diseño
  candidato —no implementado— `raw_url · observed_url · normalized_url ·
  synthetic_url · url_provenance`.

### Evidencia

- Censo completo de `p6dx-8zbt` (9.096.877 filas) y de `jbjy-vk9h` (5.981.689);
  cobertura de host: **0 filas** con `urlproceso` fuera de
  `community.secop.gov.co` en ambos datasets.
- `CO1_REQ_DIRECT_RESOLUTION = NO_GO`: muestra congelada de 31 filas en 10
  estratos; las 18 sin vista pública dan **0/18** resueltas y **0/18** ambiguas
  por `referencia_del_proceso`+`nit_entidad` y por `id_del_portafolio` —en los 18
  casos la consulta devuelve exactamente la propia fila—. `id_del_portafolio`
  además no es clave determinista (2 de 13 filas con vista comparten portafolio
  con 3 URLs públicas distintas). Ninguna columna oficial relaciona `CO1.REQ` con
  `CO1.NTC`.
- Oráculo HTTP **NO DISPONIBLE** (403 / reCAPTCHA; no se resolvieron CAPTCHAs).
  El veredicto descansa íntegramente en la evidencia Socrata.
- Regresiones: `tests/test_secop_access.py`,
  `tests/test_url_fallback_channel_parity.py`, `tests/test_url_repair.py`.

### Commits

```text
feature:          fccbe4ddcfdfee2231736f6ac73e30ed7563b5f6
parity closeout:  9a5cceea759bf882c19bfeeb968c0a568146f579
```

Detalle: `docs/decisions/2026-08-31_ADR-071_secop-access-authority.md`.
Informe: `docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md`.

## ADR-072: Ownership exacto y elegibilidad de scan son autoridades distintas

**Fecha:** 2026-09-01
**Estado:** Accepted — `SCAN_ALIAS_ELIGIBILITY_R2D3`

R2D.2 acreditó que nueve aliases válidos o tolerados por Tier 1 capturaban 17
compradores físicos cuando la misma lista alimentaba `_scan_index`. Se separan
las autoridades con metadata declarativa del gazetteer:
`scan_disabled_aliases` conserva ownership exacto sin scan y
`resolution_blocked_aliases` impide que fuzzy resucite un alias retirado. La
política pura no contiene nombres ni heurística global; los registros sin
metadata no cambian y D6 permanece intacta.

Detalle:
`docs/decisions/2026-09-01_ADR-072_exact-alias-vs-scan-eligibility.md`.
