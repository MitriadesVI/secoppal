# Decisions Log — SECOPPAL

Registro de Decisiones Arquitectónicas (ADR). Append-only. Una entrada por decisión de fondo que cambia el comportamiento del sistema.

---

## ADR-001: FollowupEngine como cerebro único del seguimiento conversacional

**Fecha:** 2026-05-16  
**Contexto:** La lógica de follow-up estaba repartida entre `conversation_store.py` (merge_params), `query_frame.py` (classify_turn + merge_frames) y `orchestrator.py` (apply_context). Cada módulo hacía una clasificación distinta con categorías incompatibles. Esto causaba bugs como "muéstrame más contratos de adulto mayor" tratado como paginación pura con offset heredado.

**Decisión:** Crear `app/core/followup_engine.py` con clasificador explícito de 11 intenciones (`pagination_more`, `refine_filter`, `change_year`, `change_order`, `change_scope`, `change_dataset`, `contextual_requery`, `suggestion_selection`, `explain_result`, `new_search`, `unclear`) y políticas de merge distintas por tipo. `apply_context` en orchestrator.py usa `detect_and_merge()` como punto único.

**Alternativas descartadas:** Mantener merge_params con if-else dispersos (escala mal). Seguir con query_frame.classify_turn (categorías muy gruesas, no distingue cambio_de_año vs cambio_de_orden).

**Consecuencias:** QueryFrame queda como estructura de datos legacy. ConversationStore queda como memoria transaccional (no clasificador). Se expone `followup_intent_type` en el resultado de `run_query()`.

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
**Contexto:** Cuando existía `estado_contrato` como lista (ej: `["En ejecución", "Modificado"]`) Y `estado` como scalar legacy (ej: "En ejecución"), el SoQLBuilder escribía `estado_contrato = 'En ejecución'` y saltaba la lista por el `continue` del bloque legacy. El filtro IN nunca se generaba.

**Decisión:** En `_build_where()`, si `params[field]` existe como lista Y `estado_field == field`, la lista tiene prioridad. El scalar solo se escribe si no hay lista para ese field.

**Alternativas descartadas:** Eliminar el scalar `estado` por completo (rompe backward compatibility con tests legacy). Forzar limpieza de `estado` en query_router cuando hay lista (más cambios, más riesgo).

---

## ADR-005: Stopwords auxiliares subjuntivas

**Fecha:** 2026-05-16  
**Contexto:** "esten" (de "estén") se colaba como objeto contractual porque no estaba en STOPWORDS. Esto producía consultas SoQL con `LIKE '%esten%'` que devolvían resultados sin relación con el tema real del usuario.

**Decisión:** Agregar `este`, `esten`, `sea`, `sean`, `encuentre`, `encuentren` a STOPWORDS en query_router.py. Complementa el set existente de verbos auxiliares ("esta", "estan", "hay", "tiene").

**Alternativas descartadas:** Crear un filtro gramatical Post-Parse (el fix es más barato en STOPWORDS). Usar POS tagging (overkill para un puñado de tokens).

---

## ADR-006: Archivado de archivos v2/v3 legacy

**Fecha:** 2026-05-16  
**Contexto:** `app/core/` contenía 5 archivos con espacios y sufijos de versión (`soql_builder v2.py`, `query_router v2.py`, `orchestrator v2.py`, `entity_resolver v2.py`, `entity_resolver v3.py`). Esto es veneno lento: confunde al agente (agarra el archivo equivocado), rompe imports, contamina tests y requiere sync manual constante entre copias.

**Decisión:** Archivar todos en `archive/cleanup_2026-05-16/`. SHA256 pre/post guardados en `docs/auditoria_2026-05-16/`. Tests y accuracy_test.py actualizados para cargar desde canonical o archive según corresponda. Regla: **nunca versiones paralelas dentro de app/core**.

**Alternativas descartadas:** Mantener sync manual (ya demostró ser frágil con el bug del Chocó). Dejarlos (siguen siendo veneno lento).

---

## ADR-007: Guard anti-WHERE 1=1 específico (solo scope/topic)

**Fecha:** 2026-05-15  
**Contexto:** El mensaje del guard ofrecía "fecha o valor" como filtros base, pero fecha y valor solos no pasan el guard — solo scope/topic (entidad, lugar, tema, contratista). El mensaje engañaba al usuario.

**Decisión:** Cambiar mensaje a "Necesito al menos un filtro de entidad, lugar, tema o contratista para buscar." El guard en execute_query sigue siendo el mismo (`resolved_params` debe tener al menos un key en `_SCOPE_TOPIC_KEYS`).

---
