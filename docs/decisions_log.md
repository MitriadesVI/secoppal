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
