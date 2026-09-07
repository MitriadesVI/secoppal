# ENTITY_PROFILE_FRAME_V1_2_A_CORE

Fecha: 2026-09-07. Estado: **CORE IMPLEMENTADO, SIN ACTIVACIÓN PRODUCTIVA**.
Estado productivo de Entity Profile: **NO_GO vigente**. Este documento no lo
levanta y no puede levantarlo: el veredicto máximo de esta tanda es
`CORE_GO_FOR_V1_2_B`.

| Referencia | SHA / ubicación |
|---|---|
| Main observado | `7868664757b28bed16c583ea50327d669f1501e3` |
| Base del carril (runtime V1.1) | `62ec330a04f054cbd8ff6028bda3a67d08ce3a28` |
| Auditoría independiente de la que salen los contraejemplos | `e024753a7a3e26a80aa0c07359078544f45f4ee5` |
| Diseño congelado (autoridad) | `02d65ddfdc221f74a8fac75352931dcf6673a171` |
| Rama | `feature/entity-profile-frame-v1-2-a-core` |
| Worktree | `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a` |

El commit documental `02d65dd` contiene **exclusivamente** el documento de
arquitectura (`git show --stat`: 1 archivo, 807 inserciones), así que se
cherry-pickeó sobre esta rama para que el diseño viaje con su implementación.
No se integró la rama de auditoría ni ninguno de sus artefactos rojos.

## 1. Qué está implementado

Trece módulos nuevos en `app/core/entity_profile/`, ninguno importado por una
ruta productiva:

| Módulo | Responsabilidad |
|---|---|
| `text_offsets.py` | Mapa fiel del texto normalizado al original. Sin él, un span "sobre el original" señala otra palabra. |
| `frame_values.py` | Los valores tipados: fuente, periodo, árbol temático, estado, selección, monto. |
| `claims.py` | Ocurrencias con span, valor tipado, autoridad y relaciones; validación del conjunto. |
| `claim_filters.py` | Claims de las dimensiones que el parser ya resolvió y que no disputan ocurrencias. |
| `claim_adapters.py` | Las autoridades existentes publican ocurrencias **vinculadas**. |
| `frame.py` | `EntityProfileFrame`: la única autoridad del significado confirmado. |
| `patch.py` | KEEP / SET / CLEAR tipados, sin merge de diccionarios. |
| `claim_composition.py` | Claims → parche, sin reasignar sujetos. |
| `frame_migration.py` | Lector estricto de contexto V1/V1.1. Puro: lee y devuelve. |
| `frame_predicates.py` | El WHERE del perfil desde predicados acreditados. |
| `frame_compiler.py` | `compile_profile`: READY / NEEDS_CLARIFICATION / INVALID_CONTEXT. |
| `frame_evidence.py` | Métricas acreditadas atadas al plan que las autorizó. |
| `frame_render.py` | Alcance visible derivado del plan compilado. |

Ninguno supera los topes de LoC; el mayor es `claim_adapters.py` (405 líneas
con comentarios, 6 responsabilidades de adaptación de cláusula). `query_plan.py`
no se tocó: sigue en 299/300.

## 2. Inventario de autoridades: quién lee hoy y qué lo reemplaza

| Pieza actual | Quién la lee hoy | Reemplazo V1.2 | ¿Activa en A? | ¿Se retira en B? |
|---|---|---|---|---|
| `semantic_request.EntityProfileSemanticRequest` | `turn.py`, `execution_plan.py`, `source_plan.py`, `assembler.py` | `frame.EntityProfileFrame` | Sí (V1.1) | Sí |
| `semantic_request.inherit_request` | `turn._composed_request` | `patch.apply_patch` | Sí (V1.1) | Sí |
| `semantic_request.event_for` | `source_plan.project_source` | relación `HAS_SUBJECT` del claim | Sí (V1.1) | Sí |
| `source_request.resolve_requested_sources` | `semantic_request.build_semantic_request` | `claim_adapters._source_claims` | Sí (V1.1) | Sí |
| `source_request._clause_head_sources` | ídem | `claim_adapters._clause_head` (cabeza real, no primera aparición) | Sí (V1.1) | Sí |
| `temporal_event.resolve_temporal_events` | `semantic_request` | `claim_adapters._event_claims` (sin deduplicar) | Sí (V1.1) | Sí |
| `temporal_event` (vocabulario `_PUBLICATION`/`_SIGNATURE`/`_UNSUPPORTED_EVENTS`/`_CURRENT_MARKER`) | `temporal_event`, ahora también `claim_adapters` | **se conserva**: es autoridad de vocabulario | Sí | No |
| `clauses.split_clauses` | `source_request`, `temporal_event`, ahora `claim_adapters` | **se conserva**: es autoridad de estructura | Sí | No |
| `material_filters.MaterialFilters.from_params` | `effective_scope` | `frame_render._filter_lines` sobre `SourcePredicates` | Sí (V1.1) | Sí |
| `material_filters.TopicExpression` (4 tuplas planas) | `effective_scope` | `frame_values.TopicNode` (árbol) | Sí (V1.1) | Sí |
| `source_plan.project_source` (copia y purga params) | `execution_plan` | `frame_compiler._compile_source` | Sí (V1.1) | Sí |
| `execution_plan.build_execution_plan` | `turn.py`, `assembler.py` | `frame_compiler.compile_profile` | Sí (V1.1) | Sí |
| `execution_plan.FOCUS_CAPABILITY` / `SECTION_CALLS` / `FOCUS_SECTIONS` | `execution_plan` | tablas propias en `frame_compiler`, con guard de paridad | Sí (V1.1) | Sí |
| `execution_plan.executed_datasets` | `effective_scope`, `render` | `ProfilePlan.planned_sources` + `frame_evidence` | Sí (V1.1) | Sí |
| `effective_scope.build_effective_scope` | `render.py` | `frame_render.build_compiled_scope` | Sí (V1.1) | Sí |
| `conversation_state` (`confirmed`/`pending` sobre requests) | `turn.py` | `ProfileConversationState` sobre Frame+Patch | Sí (V1.1) | Sí (en B) |
| `turn.classify_entity_profile_turn` | workflow productivo | admisión superior Burr + acciones de perfil | Sí (V1.1) | Sí (en B) |
| `turn._FOCUS`, `turn._PROFILE_NOISE` | `turn.py`, ahora `claim_adapters` | **se conserva**: vocabulario del perfil | Sí | No |
| `turn.profile_query_params` | `turn.py`, y el harness de tests | adaptador de admisión de B | Sí | Se decide en B |
| `scope.resolve_profile_scope` | `turn.py`, y el harness | **se conserva**: autoridad de comprador | Sí | No |
| `query_runner.jobs_for_focus` | `execution_plan` | `frame_compiler._job_catalog` | Sí (V1.1) | Sí |
| `aspects/*` (forma del SELECT y parsers) | `query_runner`, ahora `frame_compiler` | **se conservan** | Sí | No |
| `soql_builder._build_where` | ruta tabular y `aspects/common.build_where` | ruta tabular la conserva; el perfil usa `frame_predicates` | Sí | No (para tabular) |

**Lectores que no deben sobrevivir al cutover:** `semantic_request`,
`material_filters.from_params`, `source_plan`, `execution_plan`,
`effective_scope`, y el `_entity_profile_request` de `conversation_state` como
fuente heredable.

## 3. El límite con SoQLBuilder

`_build_where(params)` no se invoca desde el perfil nuevo. Se reutilizan sus
emisores —`_entity_clause`, `_exact_entity_clause`, `_object_term_conditions`,
`_phrase_recall_conditions`, `_add_estado_clause`, `_modality_clause`,
`project_modality_filter`, `project_contract_types`— sin su capa de decisiones
de recuperación (`_should_demote_objeto`, `_split_objeto_by_rarity`, la política
de oportunidad, el fallback de `estado` legacy).

Los constructores de aspecto siguen siendo los acreditados. `_FrozenWhereBuilder`
delega todo en el builder real y sólo intercepta el punto donde preguntarían por
el WHERE, que en el perfil ya está decidido; `params` les llega vacío, así que no
queda nada que un emisor pueda reinterpretar. La alternativa —reescribir la forma
de cada SELECT— habría creado dos copias de la misma consulta.

No se creó un segundo catálogo de modalidades, aliases, estados ni resolución de
comprador.

## 4. Los 35 adversariales: CORE vs INTEGRATION_B

| Función `test_…` del reaudit | Casos | Clasificación | Estado en A |
|---|---:|---|---|
| coordination_does_not_invent_or_discard_sources | 4 | CORE | PASA |
| source_and_occurrence_own_their_temporal_predicate | 5 | CORE | PASA |
| each_event_owns_its_period_or_clarifies | 2 | CORE | PASA |
| real_parser_or_is_not_rendered_as_and | 1 | CORE | PASA |
| negative_and_legacy_filters_are_honored_and_declared | 3 | CORE | PASA |
| explicit_origin_still_requires_complete_answer_after_reload | 2 | INTEGRATION_B | contrato preparado |
| explicit_pending_choice_preserves_the_requested_focus | 1 | INTEGRATION_B | contrato preparado |
| displayed_inherited_period_is_in_every_executed_query | 2 | INTEGRATION_B | contrato preparado |
| pending_period_does_not_leak_into_confirmed_execution | 1 | INTEGRATION_B | contrato preparado |
| explicit_source_switch_does_not_inherit_incompatible_event | 1 | CORE | PASA |
| remove_period_is_not_silently_restored | 1 | CORE | PASA |
| property_current_marker_does_not_erase_relative_historical_event | 1 | CORE | PASA |
| contract_count_preserves_decimal_truth | 3 | CORE | PASA |
| tiny_positive_invalid_count_does_not_render_zero | 1 | CORE | PASA |
| temporal_value_rejects_nonfinite_source_metric | 3 | CORE | PASA |
| v1_unsupported_event_is_not_lost_on_compatibility_reload | 1 | CORE | PASA |
| malformed_claim_span_is_discarded_without_crashing | 1 | CORE | PASA |
| profile_does_not_seed_a_tabular_pagination_result | 1 | INTEGRATION_B | contrato preparado |
| property_underflow_never_accredits_zero_count | 1 | CORE | PASA |
| **Total** | **35** | **28 CORE / 7 INTEGRATION_B** | |

Ningún rojo se eliminó ni se debilitó. Los siete INTEGRATION_B exigen
publicación versionada, recarga en otra instancia o admisión antes del
dispatcher; en `tests/test_entity_profile_frame_integration_contract.py` se
congela el esquema que B necesitará —round-trip íntegro, compilación estable
tras recarga, periodo que no se reinterpreta, plan atado a una revisión— y se
declaran los ocho casos sin fingir un verde de comportamiento.

Dos matices de honestidad sobre los ports:

- **R2 «hoy los contratos terminados en agosto»** bloquea por
  `AMBIGUOUS_PERIOD_ANCHOR`, no por `UNSUPPORTED_EVENT`. Cuando un marcador de
  actualidad consume el término de ciclo de vida, el periodo queda sin
  referente: podría acotar la terminación —que no tiene fecha publicada— o la
  firma. El contraejemplo exige que el hecho histórico se conserve y no se
  ejecute, y eso es lo que se verifica.
- **R3 preserva la fuente, no el complemento completo.** El parser existente ya
  descarta «procesos» del tema en «gestión de procesos industriales». Esa
  pérdida es previa al core y no la introduce ni la arregla esta tanda.

## 5. Cambios que no estaban planeados y por qué se hicieron

1. **`_clause_head` sustituye a «la primera palabra de fuente de la cláusula».**
   La regla de `source_request` basta para «procesos y contratos» pero no para
   «gestión de procesos industriales», donde la primera aparición ya vive en el
   complemento. La cabeza exige que todo lo anterior sea lenguaje de petición,
   reutilizando `_PROFILE_NOISE` en vez de un vocabulario nuevo.
2. **El compilador captura `RuntimeError` al construir los SQL.**
   `UnsafeGlobalQueryError` y `BuyerCandidateSetExceedsSafeLimitError` no son
   `ValueError`: un SQL imposible de construir reventaba la llamada en vez de
   bloquear el plan. Lo descubrió una mutación.
3. **La aserción de exclusión de modalidad mira su propia columna.**
   `NOT IN` a secas también lo emite el filtro de documentos de proveedor, así
   que el test pasaba por la razón equivocada. Ahora comprueba
   `modalidad_de_contratacion NOT IN`, y la mutación que omite la exclusión la
   mata.

Ninguno cambia la arquitectura congelada, así que no se numera ADR.

## 6. Validación ejecutada

| Puerta | Resultado |
|---|---|
| Core adversariales (`test_entity_profile_frame_core.py`) | 26 passed |
| Migración (`..._frame_migration.py`) | 9 passed |
| Evidencia (`..._frame_evidence.py`) | 11 passed |
| Contrato de integración (`..._frame_integration_contract.py`) | 13 passed |
| Propiedades Hypothesis (`..._frame_properties.py`) | 14 passed |
| Mutaciones (`..._frame_mutations.py`) | 10 passed, 10 mutaciones muertas |
| Paridad de catálogos (`..._frame_catalog_parity.py`) | 4 passed |
| Focales existentes de Entity Profile (17 archivos) | 603 passed |
| Corpus semántico de Entity Profile | 276 passed |
| Paridad externa: SoQLBuilder, QueryRouter, Actor (7 archivos) | 289 passed, 2 xfailed (preexistentes) |
| Corpus de consultas `query_corpus_v1.yaml` (89 casos) vía `test_money_v2_invariants` | 64 passed |
| Ruff (`app/ tests/ scripts/`) | All checks passed! |
| Semgrep (`.semgrep/secoppal-laws.yml`) | 10 findings; baseline 10, nuevos 0, ninguno en archivos propios |
| `make lint-core` | app/core/ limpio |
| `tests/test_module_size_limits.py` | 2 passed |
| `git diff --check` | limpio |

**No se ejecutó la suite global.** Se reserva al cutover de V1.2-B, como pide el
encargo. Corrí lo anterior, no la suite.

## 7. Paridad productiva

El runtime público de V1.1 se comporta igual en esta rama. No se modificó ningún
archivo existente de `app/`: los trece módulos son nuevos y no los importa
ninguna ruta productiva. `run_query`, `handle_normal_flow`, `execute_query`,
`turn.py`, el dispatcher, la paginación y `ConversationStore` quedan intactos.
Los únicos consumidores del core nuevo son tests.

## 8. Diferido a V1.2-B

`ContinuationContract` productivo · admisión superior en Burr · cutover del
dispatcher · integración de paginación · cutover de `ConversationStore` ·
handlers compartidos · `ProfileConversationState` con pending conversacional ·
retirada de los lectores de la sección 2.

## 9. Fuera del carril, observado y no tocado

- Los diez hallazgos Semgrep del baseline, todos en `formatter.py`,
  `response_policy.py` y `workflow/handlers/*`.
- `docs/architecture/ENTITY_ANALYSIS_V1.md` sigue afirmando que los siete P1
  están cerrados; la revisión independiente prevalece. No se reescribe aquí.
- El parser de objeto descarta las palabras de fuente del tema (R3), lo que
  impide preservar el complemento completo. Pertenece a `query_router`.
- `resolve_temporal_scope` resuelve una sola ventana por texto; el core la
  invoca por cláusula para obtener periodos por fuente, sin modificarla.
