# M3_CONSUMER_MAP — mapa de consumidores para dimensionar M3

Base: `main` 4ec99bc1 (`/Users/rodrigoortiz/Documents/secoppal`). IR v2: `[c12]` = `/Users/rodrigoortiz/Documents/secoppal-m1-c12/app/core/` (HEAD 67c0badb). Diseño: `[C0]` = `.../secoppal-m1/docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md`, `[A4]` = `A4_CAPABILITY_BOUNDARIES.md` (verificado contra el código, no copiado). Los paths sin prefijo son `app/core/...` de main salvo `app/config.py`, `streamlit_app.py`.
Sólo lectura. Único código ejecutado: `ast.parse` para tamaños de función (`python -B`, sin importar `app`) y un experimento mínimo de Burr (ver H1). Sin pytest, sin `Settings()`.

Convenciones: RUTA = decide "es mío"; VALOR = produce un parámetro ejecutable; VETO = sólo aclara/bloquea; PRES = presentación. Tamaños = líneas de la función (cota superior: muchas mezclan lectura y fusión). "Propios" excluye el tramo compartido de §1. Conteos por función nombrada; helpers internos no enumerados.

## 0. Vocabulario de destino (c12) — lo que existe y lo que no
- `Operation`: SEARCH, COUNT, SUM_VALUE, GROUP_BY, RANK_SUPPLIERS, PROFILE_BUYER, PROFILE_ACTOR, FOLLOW, EXPORT, FOLLOWUP_DELTA, OUT_OF_DOMAIN, PROTOCOL ([c12] foundation_operations.py:18). `Aspect`: TOTAL, BY_MODALITY, BY_YEAR, BY_BUYER, BY_SUPPLIER, BY_STATE, BY_TYPE, OTHER (:33). `Subject` (processes/contracts/unspecified, :44) es enum, no `ExecutableValue`.
- `ExecutableValue` = 11 tipos ([c12] foundation_authority_values.py:545): TopicValue(:169, frase plana+token_mode), BuyerValue(:198, unit|group+membership), ActorValue(:246, document/kind/display), GeoValue(:273), TimeValue(:317, intervalo semiabierto, cota abierta permitida), MoneyValue(:357, un comparador), StateValue(:378, literales por dataset), ModalityValue(:440, procedure_ids|family_policy), ContractTypeValue(:468), OrderValue(:499), CardinalityValue(:523, 1..`MAX_TOP_N`=100, :33).
- NO existe tipo para: dataset/universo ejecutable, foco/aspecto de perfil, métrica de ranking, evento temporal, identificador de lookup, nombre/id de radar, ni para `Aspect` de COUNT/GROUP_BY como valor. `decode_value` sólo lo consume `foundation_authority_contract.py` (grep en c12).
- Tabla de dueños [C0] §5.3.2 (:270-283): SEARCH→FOUNDATION; COUNT/SUM_VALUE/GROUP_BY→{contract_activity, analytics_*}; PROFILE_BUYER→{entity_profile}; PROFILE_ACTOR→{actor_profile, actor_resolution}; RANK_SUPPLIERS→{entity_profile.suppliers} sólo vía binding/perfil activo; FOLLOW→{radar} (el comando literal ya corrió); EXPORT→sin dueño (UNSUPPORTED+guía); FOLLOWUP_DELTA→LEGACY_FOLLOWUP {tabular, actor, entity_profile}; PROTOCOL→NO_ACTION. En main no existen aún `foundation_operations`/`foundation_authority_*` (ls `app/core`); `foundation_projection.py:416` y `foundation_admission.py:62` rechazan toda operación ≠ SEARCH con `UNSUPPORTED_OPERATION`.
- Embrión de manifiesto: `accreditation/spec.py:32` `Dimension` = BUYER TIME ACTOR RANKING SOURCE CARDINALITY POLARITY MONEY MODALITY GEOGRAPHY TOPIC (11); `registry.py:14-47` una sola spec (suppliers). Divergencia con c12: la spec tiene RANKING/SOURCE/POLARITY (sin `ExecutableValue`) y carece de STATE/CONTRACT_TYPE/ORDER (que c12 sí tipa).

## 1. Tramo compartido del grafo legacy (lo cruzan Actor, Entity Profile V1.1, Analytics, Contract Activity, Suppliers legacy y SEARCH)
Despacho (`orchestrator.py:392-452`): refinement(seed) → reset/actor_selection/radar/pagination/suggestion/direct_lookup (:411-416) → Natural Alpha → `capability_for` → `handle_bound_follow` → `run_foundation_search` (:444) → `handle_normal_flow`. Grafo: `workflow/graph.py` (parse_query → apply_context → llm_parse → resolve_actor_name → plan_query → resolve_entities → build_query → execute_query → degrade → observe → suggester → rank → format).

| # | Lector (tipo) | Ubicación | Tamaño |
|---|---|---|---|
| S1 | `QueryRouter.parse` (RUTA+VALOR+VETO: actor, entidad, fechas, dinero, estado, modalidad, tema, recibo `semantic_evidence`) | query_router.py:326-1460 | 1135 |
| S2 | `modality_wiring.classify_control` (VALOR) | parsing.py:123,799 → modality_followup_wiring.py:174 | — |
| S3 | `llm_parse` (VALOR, condicional a `needs_llm`) | parsing.py:250 | 30 |
| S4 | `resolve_actor_params` + `_parse_exact_boundary_modifier_suffix` (RUTA+VALOR; re-parsea el sufijo con `QueryRouter().parse`) | actor_resolution.py:109, :56 | 235+51 |
| S5 | `apply_context` (+`is_followup` conversation_store.py:626, `detect_and_merge` followup_engine.py:954, `is_complete_buyer_search` actor_arbitration.py:79, `classify_actor_pending_turn` actor_conversation.py:181) (RUTA+VALOR+VETO) | parsing.py:367 (llamadas :490,:503,:753) | 473+79+15 |
| S6 | `compute_query_plan`: `dialogue_policy.classify` (5 detectores de texto: command_incomplete, implicit_opportunity, discourse_filler, temporal, city_mention; `_detect_number_phrase_scrub` sólo lee params), P9 (marcadores en `raw_query`, query_plan.py:157), `apply_signed_dataset_invariant`, `apply_opportunity_invariant` (VALOR/VETO). `evaluate_opportunity_scope` NO lee texto (cuerpo leído, opportunity_scope_policy.py:120-150) | query_plan.py:48; dialogue_policy.py:130-372; contract_invariants.py:131,145 | ≈250 |
| S7 | `resolve_entities`: `resolve_entity_ecosystem(raw)` (VALOR: inyecta filtros de ecosistema desde el crudo) y `contract_universe_intent.request(user_query)` (RUTA/VALOR universo) | planning.py:210, :179 | 189 |
| S8 | pre-tabular de `execute_query`: `classify_entity_profile_turn(user_query)` :97, `is_supplier_followup` :109, `detect` ×5 de analytics :109 | execution.py:67-190 | — |
Total S ≈ 25 funciones, ≈ 2.450 líneas (cota). Las etapas posteriores al ejecutor (`rank_results` ranking.py:25, `format_response`, `build_advisor_response`) leen params; `user_query` sólo llega al narrador (PRES, response_policy.py:99).

## 2. Capacidades

### 2.1 Actor Profile
1. **Op**: PROFILE_ACTOR ([C0] :276). Flag: `SECOPPAL_ACTOR_PROFILE_ENABLED` default True (app/config.py:70; `actor_profile_enabled()` :243).
2. **Ejecutor**: `maybe_handle_actor_profile(params, dataset_id, secop_client, soql_builder)` actor_profile/turn.py:84 → `build_actor_profile(params, dataset_id, secop_client, soql_builder)` assembler.py:99 (aspectos en `aspects.py`, SoQL vía `soql_builder._build_where`, `scope_from_resolved_params` scope_context.py:15).
3. **Texto**: ejecutor y admisión propia (`is_actor_profile_turn` :40, `_asks_for_a_listing` :62) leen sólo params (cuerpos leídos): 0 lectores. Entre admisión y ejecución, lectores propios: RUTA 3 (`actor_claims` foundation_route_owner.py:20; `classify_actor_pending_turn`; `handle_actor_selection` actor_selection.py:89, 136 líneas), VALOR 4 (`resolve_actor_params`; `_parse_exact_boundary_modifier_suffix`; `parse_actor_candidate_selection` actor_conversation.py:146; `_document_selection` actor_selection.py:227). Módulos de texto de actor dentro de S1: actor_intent.py (763), actor_arbitration.py (400), actor_conversation.py (439).
4. **Entradas**: ACTOR (`contratista` documento + `actor_scope{role,resolution_method,document_type,identity_confidence,observed_name}`), más BUYER/GEO/TIME/VALUE/STATE/MODALITY/CONTRACT_TYPE/TOPIC como filtros (misma WHERE) y ORDER/TOPIC como veto de listado (:62-81); todo como dict legacy. c12: ActorValue cubre documento+kind+display; **falta** rol y método/procedencia de resolución (el ejecutor/guard `is_authoritative_actor_resolution` actor_resolution.py:108 los exige). Resto cubierto por Buyer/Geo/Time/Money/State/Modality/ContractType/Topic/OrderValue con conversión (Time semiabierto vs `fecha_desde/hasta`; Money un comparador vs `valor_min/max`). **Falta** tipo de universo/dataset (`dataset`, `contract_universe`).
5. **Veredicto**: EJECUTOR_YA_TIPADO (sobre dict legacy). Los insumos nacen en lectores VALOR compartidos (S1,S4). Legacy a borrar propio medido ≈ 445 líneas (S4 286 + actor_selection 157).
6. **Estado**: `actor_pending{version,origin,original_query,original_params,actor_name,resolution_status,candidates,selection_token,...}` dentro de `Turn.parsed_params` (escribe `project_actor_resolution` actor_resolution.py:272 → `append_turn` normal_flow.py:454; lee `handle_actor_selection` :115, `apply_context` parsing.py:502, followup_engine.py:302). TTL 30 min (actor_selection.py:37). Con Frame ON además `DelegatedRef` en `ContinuationContract` (continuation.py:164, turn_publication.py:94-110).
7. **UI sin texto**: `select_actor_candidate(rank, pending_token)` orchestrator.py:326 → `ctx.actor_candidate_selection` → `handle_actor_selection` :89 (streamlit_app.py:1969). Re-ejecuta `handle_normal_flow` con `_SelectedFrameRouter` (actor_selection.py:39-53) pero con `original_query` crudo (:214-222): S6/S7 siguen leyendo texto.

### 2.2 Entity Profile V1.1
1. **Op**: PROFILE_BUYER. Sin flag (corre siempre: execution.py:97).
2. **Ejecutor**: `run_entity_profile_turn(turn: EntityProfileTurn, *, secop_client, soql_builder)` actions/entity_profile.py:21 → `build_entity_profile(*, scope, identity, params, focus, secop_client, soql_builder, user_query="", plan)` entity_profile/assembler.py:27 (`plan` siempre viene de `turn.plan`; `user_query` queda "" en ruta productiva).
3. **Texto** (propios): RUTA 4 (`detect_entity_profile_turn` turn.py:115, 21 l.; `is_bare_affirmative` :147; `is_supplier_followup` followup.py:24; `is_explicit_entity_profile_search` :346, llamada desde followup_engine.py:298), VALOR 4 (`build_semantic_request` semantic_request.py:186 → `resolve_requested_sources` source_request.py:126, `resolve_temporal_events` temporal_event.py:140, `current_state_survives_events` :177), VETO 1 (`territory_only_name` :195; `territory_clarification` :209 es PRES). `classify_entity_profile_turn` :228 (91 l.) los orquesta.
4. **Entradas**: `EntityProfileScope`(buyer_units, buyer_group_id, time_from/to, modality, topic; models.py:39) + `EntityProfileIdentity` + `EntityProfileExecutionPlan`; foco `overview|suppliers|modalities|values|recent|temporal`; fuente PROCESSES/CONTRACTS/BOTH; evento temporal. c12: BuyerValue≈buyer_units/group; TimeValue≈time_from/to (**falta evento**); TopicValue plano vs `topic: tuple[str]`. **Faltan**: foco, fuente (`Subject` no es valor), evento temporal.
5. **Veredicto**: MIXTO (ejecutor tipado; plan se construye desde texto). Legacy a borrar ≈ 211 l. (funciones medidas) + source_request 153 + temporal_event 214.
6. **Estado**: dentro de params: `_entity_profile_active/_request/_sources/_state/_focus` (turn.py:30-31; conversation_state.py:25,100 `state_context`); escribe turn.py:281-308; lee turn.py:124,238,281,332 y followup.py:25-33; `migrate_current_legacy` profile_migration.py:11.
7. **UI sin texto**: `select_entity_profile_action(action_id, reference)` orchestrator.py:355 → `ctx.profile_action_selection`; sólo con Frame ON (turn_coordination.py:14-24). Dentro, `admission.py:191-192` convierte la acción en `action.trusted.phrase` y la re-parsea con `QueryRouter.parse`.

### 2.3 Entity Profile Frame V1.2 (flag OFF)
1. **Op**: PROFILE_BUYER. Flag `SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED` default False (config.py:83; `entity_profile_frame_enabled()` :145). Enciende admisión superior + sobre versionado (turn_coordination.py:25-35).
2. **Ejecutor**: `execute_plan(plan: ProfilePlan, secop_client) -> ProfileEvidence` frame_runner.py:99; `compile_profile(frame, *, soql_builder, supplier_ranking)` frame_compiler.py:301. Entrada de ruta: `run_profile_route(deps, ctx, *, params, state, turn_id, bound_focus)` profile_route.py:36 → `run_profile_turn(*, user_query, params, state, turn_id, soql_builder, secop_client, today, bound_focus)` frame_turn.py:163.
3. **Texto** (propios): RUTA 3 (`is_profile_turn` frame_turn.py:125; `asks_to_clear_period` claim_adapters.py:136; `detect_entity_profile_turn` reutilizado :142,:160) + `external_owner` external_admission.py:8 (RUTA, 29 l.) y `admission_rules.classify` :125; VALOR ≈ 15 (`collect_claims` claim_adapters.py:157 y 9 helpers: `_mask_buyer _buyer_claim _buyer_span _source_claims _event_claims _period_claims _state_claims _legacy_state_claims _intent_claims`; `consume_clear_period_terms` :110; `complete_reformulation` pending_resolution.py:198; `resolve_pending/_select/_by_ordinal/_by_own_words` :70-141; `_ranking_request` frame_turn.py:479); VETO 3 (`_territory_block` :441; `territory_only_name`; `_orphan_period_issue` claim_adapters.py:400). ≈ 450 l. en claim_adapters (110-565) + pending_resolution ≈ 120 + frame_turn ≈ 190.
4. **Entradas**: `EntityProfileFrame`(BuyerScope, focus, SourceSelection, `SourcePredicates{temporal: TemporalPredicate(Period+event), current_state: StateExpression, topic: TopicNode(árbol and/or/not), modality/contract_type: Selection, value: ValueConstraint(Decimal lower/upper), contract_universe}`; frame.py:61-271, frame_values.py:89-340). **Sistema de tipos paralelo** a `ExecutableValue`: equivalencias aproximadas BuyerScope↔BuyerValue, Period↔TimeValue, StateExpression↔StateValue, Selection↔Modality/ContractTypeValue, ValueConstraint↔MoneyValue (min+max Decimal vs un comparador int). Sin equivalente en c12: TopicNode (árbol; TopicValue es plano), SourceSelection, focus, TemporalEventKind, contract_universe.
5. **Veredicto**: MIXTO (ejecutor `execute_plan` ya tipado; la entrada LEE_TEXTO_PARA_VALOR). Legacy a borrar ≈ 760 l. medidas.
6. **Estado propio**: `ProfileConversationState{confirmed_frame, pending: PendingProfile{patch,question,base_frame_ref,issues,alternatives}}` (profile_state.py:149,95) en `ConversationEnvelope.profile_state` (clave `PROFILE_STATE_KEY` :36; envelope en conversation_envelope.py:44); `ContinuationContract{confirmed_profile, pending_profile,...}` (continuation.py:164). Escribe `publish` turn_publication.py:114-137 (vía `conv_store.publish_turn` conversation_store.py:372); lee `admit`/`_restore` admission.py:349 y `run_profile_turn(state=)`. No escribe `Turn.parsed_params` (turn_coordination.py:201-212).
7. **UI sin texto**: `select_entity_profile_action` (ver 2.2); acciones publicadas `_attach_published_actions` turn_coordination.py:344.

### 2.4 Suppliers / A2
1. **Op**: RANK_SUPPLIERS (sólo vía binding/perfil activo, [C0] :277). Flags: `SECOPPAL_NATURAL_ALPHA_MODE` (default "off", config.py:99) + `SECOPPAL_A2_SUPPLIERS_BINDING_ENABLED` (False, :107); sidecar de polaridad `SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED` (False, :191); con Frame ON, ruta de acreditación `accreditation/` (admission.py:267 `_accredited_pending`).
2. **Ejecutor**: el mismo de Entity Profile (foco `suppliers`): legacy `bound_profile_turn(capability, user_query, params)` workflow_hook.py:28 → `classify_entity_profile_turn(..., bound_focus="suppliers")` → `run_entity_profile_turn`; Frame: `run_profile_route(..., bound_focus)` → `execute_plan`. Sin binding: `is_supplier_followup` (followup.py:24, sólo con perfil activo).
3. **Texto** (propios): RUTA 3 (`assess_supplier_request` supplier_request_authority.py:145 = testigo de mención; `ambiguous_cardinal_surface` supplier_dogfood_guard.py; compuerta `decide` supplier_contract.py:163, 194 l.), VALOR 5 (`resolve_supplier_ranking_request` supplier_ranking_request.py:101; `ranking_metric(user_query)`; `account` residual.py; `quantity_refs/mention_refs` supplier_semantics.py; `polarity_veto` LLM sidecar supplier_semantics_provider.py) y re-lectura por `classify_entity_profile_turn`. Frame ON: `assemble` accreditation/assembly.py:198 con ≈10 autoridades/guards sobre el crudo (authorities.py 308, guards.py 172, residual_verifier.py 322). Tamaño de módulos (no de funciones): capability_binding/supplier_*+residual+ranking_criterion = 2.122 l.; accreditation lectores ≈ 1.200 l.
4. **Entradas**: BUYER (requerido), TIME (opcional), cardinalidad+métrica (`_supplier_ranking_limit/_metric` supplier_ranking_request.py:154-160, 1..25, `MAX_SUPPLIER_LIMIT` :29), fuente CONTRACTS fija. c12: BuyerValue; TimeValue (conversión); CardinalityValue cubre el número pero su rango es 1..100 (no impone el tope 25 del ejecutor). **Faltan**: métrica (count|value|both, :38-42) y dimensión RANKING (existe en `spec.Dimension`, no en `ExecutableValue`).
5. **Veredicto**: MIXTO (dominante LEE_TEXTO_PARA_VALOR). Legacy a borrar ≈ 1.4k l. (módulos de lectura de capability_binding) + ≈ 1.2k de accreditation si Frame ON.
6. **Estado**: ninguno propio; el request viaja en params (`_supplier_ranking_*`, lee execution_plan.py:237). Si persisten en `Turn.parsed_params`: NO VERIFICADO. Telemetría `dogfood_telemetry` (efímera).
7. **UI sin texto**: ninguna.

### 2.5 Contract Activity
1. **Op**: COUNT (TOTAL) y GROUP_BY (BY_MODALITY, BY_STATE↔`BY_REPORTED_STATE`, equivalencia NO VERIFICADA); SUM_VALUE y BY_YEAR/BY_BUYER/BY_SUPPLIER/BY_TYPE → `HANDOFF` (`_operation` admission.py:217-254). Flag `SECOPPAL_CONTRACT_ACTIVITY_BINDING_ENABLED` (False, config.py:112) sólo gobierna la vía Alpha (capability_binding/registry.py:55). Ver H1 sobre la vía "directa".
2. **Ejecutor**: `build_plan(request: ContractActivityRequest) -> ContractActivityPlan` planning.py:17 y `execute_plan(plan, secop_client) -> ContractActivityResult` runtime.py:23. Entrada: `maybe_handle_analytical_query(user_query, params, dataset_id, secop_client, soql_builder, channel, *, capability_binding, today, semantic_evidence)` analytics/registry.py:94 → `handle_contract_activity(user_query, params, secop_client, *, binding, today, semantic_evidence)` contract_activity/__init__.py:26.
3. **Texto**: ejecutor 0 (cuerpos leídos; `raw` sólo viaja en el request). Admisión `admit_contract_activity(raw, params, *, binding, today, semantic_evidence)` admission.py:109 (106 l.): RUTA 1 (`_operation` :217, 38 l.), VALOR 4 sin binding (`_period` :269, `_money` :291, `_modalities` :318 vía `resolve_modality(raw)`, `_states` :342 vía `detect_estado_family`) o 2 con binding (`accredit_filters` filters.py, 244 l.; `accredit_contract_unit` contract_activity_binding.py), VETO 2 (`_buyer_is_present` :368; `_has_unowned_topic` :377). Opera sobre el recibo tipado `semantic_evidence` (spans de `analytic_control.py` 369 l., `material_residual.py` 228 l.) fabricado en S1.
4. **Entradas** (`ContractActivityRequest` models.py:44): operation, buyer_units + source_buyer_names (proyección `project_buyer_names`, admission.py:358), date_from/date_to (par cerrado e inclusivo; `PARTIAL_SIGNATURE_PERIOD` models.py:60), modality_values (valores crudos de la fuente), reported_states (`estado_contrato` crudo), `MoneyConstraint(operator, amount>0)`. c12: MoneyValue≈MoneyConstraint (amount≥0 vs >0); TimeValue admite cotas abiertas (CA no); BuyerValue.members≠source names (necesita proyección); ModalityValue(procedure_ids)/StateValue requieren proyección a valores crudos (`project_modality_filter` ya existe). **Faltan**: `Aspect` como valor del job (sí existe como enum), universo contratos fijo.
5. **Veredicto**: LEE_TEXTO_PARA_VALOR (ejecutor EJECUTOR_YA_TIPADO). Legacy a borrar ≈ 259 l. (admission, sin bound) + 244 (filters, bound).
6. **Estado**: ninguno; recibo `SemanticTurnEvidence` en Burr state (no persistido).
7. **UI sin texto**: ninguna.

### 2.6 Analytics legacy (Execution, ValueStats, Ranking, Temporal, AggregateSum)
1. **Op**: SUM_VALUE (AggregateSum), GROUP_BY/BY_YEAR (Temporal), GROUP_BY/BY_BUYER (Ranking) — mapeo inferido; **Execution (contratado vs pagado) y ValueStats (promedio/mín/máx) no tienen operación ni aspecto en c12** ([C0] sin AVG/MIN/MAX/PAID; grep). Sin flag propio (registry.py:30-36 `REGISTRY`).
2. **Ejecutor**: `_run(analyzer, params, dataset_id, secop_client, soql_builder)` registry.py:50; builders: `build_aggregate_sum_query(soql_builder, dataset_id, params)` aggregate_sum.py:28, `soql_builder.build_value_stats` soql_builder.py:762, `build_top_entities` :754 (limit 5), `build_temporal_dist` :825, SQL inline en execution.py:~56.
3. **Texto**: RUTA 5 (`detect(query, params)`: execution.py:48, value_stats.py:38, ranking.py:44, temporal.py:38, aggregate_sum.py:70; cada una un regex sobre el crudo, ignoran `params`), VALOR 0, VETO 0. build/format leen params (cuerpos leídos). ≈ 5 regex + 5 métodos ≈ 70 l.
4. **Entradas**: dict legacy vía `soql_builder._build_where` (≥ 33 claves directas en soql_builder.py:261-753 + delegadas: buyer_group, modalidad_*_procedure_ids, contract_universe*, actor_scope). `has_analytics_scope` (base.py:15) exige `SCOPE_TOPIC_KEYS`. Valor cubierto por los 11 tipos con conversión; faltan agregación (avg/min/max/paid) y aspecto.
5. **Veredicto**: LEE_TEXTO_PARA_RUTA (5 lectores, 0 de valor). Legacy a borrar ≈ 70 l. + la rama pre-tabular (execution.py:109).
6. **Estado**: ninguno.
7. **UI sin texto**: ninguna.

### 2.7 Radar / Follow
1. **Op**: FOLLOW → RADAR ([C0] :279). Flag `SECOPPAL_NATURAL_FOLLOW_BINDING_ENABLED` (False, config.py:117) sólo para `FOLLOW·LIST/RUN` vía Alpha (capability_binding/registry.py:36-39,60); `FOLLOW·SAVE` deliberadamente sin binding (registry.py:11-19 doc).
2. **Ejecutores**: `handle_radar_command(deps, ctx)` handlers/radar.py:31 → `_do_list(store, owner)` :133, `_do_save(deps, ctx, store, owner, name)` :73 → `save_radar_from_params(store, *, owner, name, params, channel, frequency, created_from_trace_id)` radar/builder.py:52, `_do_run(deps, store, owner, name)` :178 → `run_radar(deps, store, radar, *, run_trigger)` runner.py:543 → `_execute_radar_window(deps, params)` :266. Follows: `follow(store, owner, row)` follows.py:187, `unfollow` :253, `refresh_follows(deps, store, owner)` :437. Vía bound: `handle_bound_follow` handlers/natural_follow.py:34.
3. **Texto**: RUTA 3 (`parse_radar_command` commands.py:77, regex `_LIST_RE/_SAVE_RE/_RUN_RE`; `external_owner` external_admission.py:8 [Frame ON]; `turn_coordination.py:25,109` re-parsean el comando), VALOR 2 (nombre del radar en `parse_radar_command`/`_clean_name`; `radars_named_in` name_mentions.py:25 [bound]). `run_radar` y `list` no leen texto. `_do_save` recibe el dict de un turno previo (`_last_search_params` :54) o `ctx.export_search_params` (Frame ON). ≈ 98+33+29 = 160 l.
4. **Entradas**: SAVE: params legacy → `sanitize_for_save` (deny: `entidad_ecosystem, ordering_signal, offset, _display_offset, _shown_total, *_resolution`, builder.py:28-41) → `stamp` → `frame_from_params` → `QueryFrame` JSON (store). RUN: `Radar` del store → `params_from_radar` runner.py:218 → `_apply_window_policy` (limit 100, sin orden). c12: la búsqueda salvada es un set de condiciones (los 11 tipos), pero lo persistido es `QueryFrame` legacy (sistema de tipos tercero, query_frame.py:19). **Faltan**: nombre/id de radar, frecuencia, y el puente IR→`QueryFrame`.
5. **Veredicto**: MIXTO (RUN/LIST: ejecutor tipado, texto sólo ruta+nombre; SAVE: VALOR vía dict de `Turn`). Un turno Foundation guarda `{}` (H2) → SAVE no tiene qué guardar.
6. **Estado**: `RadarStore` SQLite (radars, radar_runs, radar_seen_results, radar_events; store.py 1927 l.), radar sintético `frequency='follows'` (follows.py:5-20); lease (lease.py).
7. **UI sin texto**: streamlit_app.py:270-272 (`follow/unfollow`), :1021,:1530,:1561 (`run_radar`), :1133-1145 (`save_radar_from_params`+`run_radar`), :1535 (`refresh_follows`); el formulario de creación usa `run_query(base)` y lee `result["resolved_params"]` (:1113-1121).

### 2.8 Export
1. **Op**: EXPORT sin dueño en M1 ([C0] :280). Sin flag.
2. **Ejecutor**: `build_results_xlsx(*, rows, query, resolved_params, dataset_id, total_count, generated_at)` export_xlsx.py:69; UI `st.download_button` streamlit_app.py:557-569. No pasa por `run_query`.
3. **Texto**: 0 lectores (`query` sólo PRES: `_write_search_sheet` hoja "Búsqueda"). Guarda `compatible(params)` (contract_universe.py:79).
4. **Entradas**: filas ya mostradas + `resolved_params` con 20 claves `_FILTER_KEYS` (export_xlsx.py:28-49: dataset, contract_universe(_version), intent_type, objeto, objeto_or, objeto_phrases, entidad_resolved, departamento_resolved, ciudad, contratista, actor_identity, modalidad, estado_family, estado, fecha_desde/hasta, valor_min/max, ordering_signal). Un turno Foundation publica sólo `export_params = stamp({"dataset":"contratos"})` (foundation_presentation.py:32-38): la hoja de filtros no lista condiciones. No hay tipo para "filtro mostrado".
5. **Veredicto**: EJECUTOR_YA_TIPADO; 0 lectores.
6. **Estado**: `ContinuationContract.export_ref` = `turn_id` del último turno tabular (continuation.py:169; turn_publication.py:97-103; lee `export_params` tabular_capability.py:62-70 sobre `Turn.parsed_params`) [Frame ON]; en Streamlit, `debug.resolved_params`/`export_params` del ítem de historial (streamlit_app.py:553-560).
7. **UI sin texto**: el botón completo.

### 2.9 Direct lookup (ID SECOP / URL / referencia)
1. **Op**: ninguna en c12; [A4] §2.7 lo lista junto a PROTOCOL; [C0] no lo menciona (grep). Corre antes de Foundation (orchestrator.py:411-416). Sin flag.
2. **Ejecutor**: `_handle_entity_ref(deps, ctx, request: DirectLookupRequest)` handlers/direct_lookup.py:318; `_handle_id_or_url(deps, ctx, lookup_value, lookup_kind)` :410. `DirectLookupRequest{reference_raw, reference_normalized, entity_qualifier_raw, uid, url, match_mode}` direct_lookup.py:66 ("el handler no vuelve a analizar el texto").
3. **Texto**: `detect_direct_lookup(raw_query)` direct_lookup.py:100 → `parse_direct_lookup_request` :114 (RUTA y VALOR en la misma función, 48 l.) + helpers `_extract_url_ids` :433, `_parse_notice_uid` :447, `_extract_entity_refs` :473, `_extract_entity_qualifier` :208, `normalize_entity_ref` :181 ≈ 179 l. totales. En el handler, `ctx.user_query` sólo aparece en :77 (trace) y :86 (detección).
4. **Entradas**: id/URL/referencia + qualifier. Sin `ExecutableValue`; necesitaría tipo propio.
5. **Veredicto**: EJECUTOR_YA_TIPADO (1 lector RUTA+VALOR + 5 helpers; ≈ 179 l.).
6. **Estado**: ninguno (no hace `append_turn`, :70; el trace guarda `parsed_params={"direct_lookup_ref": ...}` sólo en trace).
7. **UI sin texto**: ninguna.

### 2.10 SEARCH tabular legacy (sólo el ejecutor)
1. **Op**: SEARCH (dueño FOUNDATION; legacy sólo si `SECOPPAL_FOUNDATION_MODE=legacy`, default, config.py:94) y FOLLOWUP_DELTA→LEGACY_FOLLOWUP.
2. **Ejecutor**: cola de `execute_query(state, secop_client, soql_builder, capability_binding)` execution.py:67 (guard :192, consulta :213, fallback H4 :264) sobre `SoQLBuilder.build(dataset_id, params, ...)` soql_builder.py:261 / `build_count` :354 / `_build_where` :367 y `_query_first_then_count(secop_client, dataset_id, count_soql, soql, *, limit, ...)` helpers.py:177. Mismo ejecutor para refinement (`execute_resolved_params(deps, params, *, channel)` helpers.py:62), paginación (3 llamadas), sugerencias (2), radar (2), `dual_dataset` (2) y **Foundation** (`project_accepted` compila condiciones a un dict legacy y llama `builder.build`, foundation_projection.py:216,440-455; `foundation_topic_terms` soql_builder.py:71,529).
3. **Texto**: el ejecutor 0. Los previos son §1 (≈25 funciones, ≈2.450 l.).
4. **Entradas**: ≥ 33 claves en `_build_where` (intent_type, estado, estado_*, fecha_desde/hasta/_hasta_exclusive, ciudad, departamento_resolved, departamentos_in, municipal_territory, objeto, objeto_or, objeto_phrases, objeto_exclude, foundation_topic_terms, entidad_resolved/_like/_in_candidates/_contains_all/_ecosystem, contratista, actor_scope, valor_min/max, ordering_signal, limit, offset, contract_type_include, estado_field, estado_family) + delegadas (buyer_group, modalidad_*, contract_universe*). Cada una tiene análogo c12 salvo: `limit/offset` (paginación, no es dimensión), `intent_type`/`dataset`/`contract_universe` (universo), `entidad_ecosystem/_in_candidates/_contains_all` (BuyerValue GROUP cubre `in_candidates` pero no el ecosistema derivado del texto).
5. **Veredicto**: EJECUTOR_YA_TIPADO (el dict); productor de params LEE_TEXTO_PARA_VALOR (§1).
6. **Estado**: `PaginationBuffer` (pagination_buffer.py:86 `create`, sidecar `<chat>.pagination.json`; escribe normal_flow.py:60-87 → `save_pagination_buffer` conversation_store.py:289; invalida :334; lee pagination.py:191-259) y `TabularGroup` en `ContinuationContract` (continuation.py:57). `Turn.suggestions`.
7. **UI sin texto**: chips de refinamiento → `seed_resolved_params` (streamlit_app.py:1936-1942, `apply_refinement` contextual_suggestions.py:252) → `handle_refinement` orchestrator.py:403/refinement.py:36; "Ver más" envía el texto "más resultados" (streamlit_app.py:1952-1958), o sea sí pasa por texto.

## 3. Tabla transversal — `Turn.parsed_params` entre turnos

**Escritores** (todo pasa por `ConversationStore.append_turn` conversation_store.py:201, que sólo filtra `entidad_ecosystem` (:227; derived_execution_state.py:39))

| # | Escritor | Forma que escribe |
|---|---|---|
| W1 | normal_flow.py:454-468 | `state["resolved_params"]` completo (post-plan/resolve; incluye `limit`, meta-claves, `_entity_profile_*`, `actor_pending`) + `suggestions[].modified_params` + `result_ids`, `query_fingerprint` |
| W2 | refinement.py:113 | `params` del seed (sin `offset`, con `limit`) |
| W3 | suggestion.py:159 | `sug_params` = `modified_params` de la sugerencia |
| W4 | persistence.py:78 (llamado por pagination.py:162,173,308,361) | `result["parsed_params"]`; la página lleva `_display_offset`, `_shown_total` (pagination.py:329-333) |
| W5 | foundation_runtime.py:228-233 | `{}` + `outcome_reason="foundation_v3"` (nunca params) |
| W6 | actor_selection.py:67-69 | delega en W1 con `user_query` reemplazada |
| — | direct_lookup.py:70 | no escribe turno |
| — | Frame ON, ruta perfil | no escribe `Turn`; publica sobre (turn_coordination.py:201-229) |
Además `PaginationBuffer.query_params` guarda otra copia del dict (normal_flow.py:63-87).

**Lectores directos del dict del turno anterior**

| # | Lector | Dónde | Para qué |
|---|---|---|---|
| R1 | `apply_context` (+`frame_from_params`, `detect_and_merge`) | normal_flow.py:94 → parsing.py:383,388,753 | fusión de follow-up, precedencia de actor, modalidad, negación |
| R2 | guarda de universo en `resolve_entities` | planning.py:176-179 | `compatible(previous)` |
| R3 | telemetría de modalidad | normal_flow.py:477 → modality_followup_telemetry.py:134,155 | ¿había filtro previo? |
| R4 | `refinement_delta(last_ok.parsed_params, resolved)` | normal_flow.py:410; refinement_recovery.py:29 | diff genérico + 17 etiquetas |
| R5 | radar `_last_search_params` + `save_radar_from_params` | radar.py:54-66; builder.py:36-76 | guardar búsqueda (guarda `SCOPE_TOPIC_KEYS` 11 claves, `_followup_constants.py:54`) |
| R6 | `handle_pagination` | pagination.py:155-175,222 | siguiente página (sin `tabular_referent`) |
| R7 | `handle_actor_selection` | actor_selection.py:115 | selección de candidato |
| R8 | `migrate_current_legacy` | profile_migration.py:11-23 (llamada admission.py:222) [Frame ON] | migrar perfil legacy a Frame |
| R9 | `export_params` | tabular_capability.py:62-70 [Frame ON] | export/guardar radar |
| R10 | `handle_suggestion_selection` | suggestion.py:56-100 (lee `Turn.suggestions`) | ejecutar `modified_params` |
Indirectos (consumen claves ya heredadas por R1 en el mismo turno): R11 Entity Profile V1.1 (turn.py:124,238,281,332), R12 Actor Profile (`is_actor_profile_turn` :40), R13 `supplier_contract._unsupported/_actor_subject` (supplier_contract.py:81,112,117). Otros lectores de `last_turn`: `is_followup` (conversation_store.py:626, sólo timestamp), `ContextDetector` (followup_engine.py:98-119, `result_ids`), `last_foundation_turn` (foundation_route_owner.py:86-90, `outcome_reason`) — no leen params.
Lectores a nivel de resultado, no de `Turn`: Streamlit lee `debug.resolved_params` del historial (streamlit_app.py:1936-1942, :553-563); `feedback.log_trace` (feedback.py:228-236).

**Claves leídas entre turnos y nº de lectores directos** (R1–R10; "G" = además los 3 genéricos R4/R5/R6 que leen todo el dict)
- Por nombre en `QueryFrame` (query_frame.py:46-69), sólo R1 salvo indicación: 13 de alcance (`entidad, entidad_resolved, entidad_resolution, buyer_group, departamento, departamento_resolved, departamento_resolution, departamentos_in, ciudad, ciudad_resolved, region, region_resolution, municipal_territory`) → R1 (+R8 para `entidad`, `entidad_resolved`; +R5 para 6 vía guarda) ; `objeto, objeto_exclude` → R1; `fecha_desde, fecha_hasta` → R1, R8; `valor_min, valor_max, money_metadata` → R1 (rebase SMMLV followup_engine.py:642-717); `estado, estado_field, estado_family, estado_contrato, estado_contrato_adicional, estado_de_apertura_del_proceso, estado_del_procedimiento` → R1 (+R8 para 3); `modalidad_include_procedure_ids, modalidad_exclude_procedure_ids` → R1, R3; `contract_type_include, ordering_signal, _temporal_replace_scope` → R1; `contratista` → R1, R5.
- `contract_universe, contract_universe_version` → **6** (R1, R2, R5, R6, R8, R9). `dataset` → **6** (R1, R2, R5, R6, R9, R10). `dataset_scope` → 2 (R1 followup_engine.py:984, R6 :171). `dataset_explicit` → 1.
- `actor_pending` → 2 (R1 parsing.py:502, R7) (+R11/R12 indirecto); `actor_scope, actor_resolution(_method/_status), actor_intent_conflict, actor_selection_rank, actor_conversation_*` (10, `_ACTOR_STATE_KEYS` actor_conversation.py:429) → R1.
- `_entity_profile_active/_request/_state` → R8 (+R11 indirecto); `_entity_profile_sources/_focus` → sólo R11 indirecto.
- Cursor: `offset, limit` → R1 (followup_engine.py:722; query_frame.py:121), R6; `_display_offset, _shown_total, _page_fingerprint` → R6 (+R1 las descarta query_frame.py:121-123).
- `intent_type` → R1 (followup_engine.py:619), R10. `entidad_ecosystem` → denegada en R5 (builder.py:28) y R6 (pagination.py:61), descartada al escribir.
- `_supplier_ranking_limit/_metric` → ningún lector entre turnos verificado.
Conteo: R1 nombra 37 claves por pertenencia a `QueryFrame` (13+20+4); ≈ 57 claves nombradas en total entre todos los lectores directos (conteo manual); el resto viaja por `raw_params` sin lectura por nombre.

## 4. Hallazgos laterales (hechos)
- **H1 — CA "directo" inalcanzable en el grafo de producción**: `registry.py:115` sólo corre CA sin binding si `capability_binding is LEGACY_CONTRACT_ACTIVITY_DIRECT`; `graph.py:79` hace `.bind(capability_binding=getattr(deps,"capability_binding",None))`, `deps.py:62` default `None`, y `observed_deps` (natural_alpha.py:77-83) sólo lo reemplaza por el binding de `capability_for`. Experimento mínimo (venv, sin importar `app`): `action.bind(cb=None)` entrega `None`, no el default `object()`. Los tests de CA invocan `execute_query` directo con el default (test_contract_activity_narrow_results.py:136). Contradice [A4] §2.2 ("el camino directo corre siempre que haya semantic_evidence"). Pese a ello `specialized_owner` sigue reclamando CONTRACT_ACTIVITY (foundation_route_owner.py:53-57). **Inferencia**: con Alpha off, un conteo de CA cae a REGISTRY/tabular.
- **H2**: Foundation escribe `{}` como params de turno (foundation_runtime.py:228-233) → FOLLOWUP, Radar-SAVE y la hoja de filtros de Export no tienen insumos tras un turno Foundation.
- **H3**: una acción de UI "sin texto" se materializa como frase y se re-parsea (admission.py:191-192).
- **H4**: `followup_engine` lee texto vía Entity Profile (`is_explicit_entity_profile_search` :298; `is_supplier_followup` :300): el motor de follow-up es lector de RUTA de perfiles.
- **H5**: tres sistemas de tipos para el mismo universo ejecutable: dict legacy (`SoQLBuilder`), `Frame`/`frame_values` (perfil V1.2), `ExecutableValue` (c12) + `QueryFrame` (radar).

## 5. Tabla resumen

| Capacidad | Operación IR v2 | Veredicto | RUTA (propios) | VALOR (propios) | Tipos c12 que faltan | Estado propio |
|---|---|---|---|---|---|---|
| Actor Profile | PROFILE_ACTOR | EJECUTOR_YA_TIPADO | 3 | 4 | universo/dataset; rol y procedencia de ActorValue | `actor_pending` en params (+DelegatedRef Frame ON) |
| Entity Profile V1.1 | PROFILE_BUYER | MIXTO | 4 | 4 | foco, fuente, evento temporal | `_entity_profile_*` en params |
| Entity Profile Frame V1.2 (OFF) | PROFILE_BUYER | MIXTO (ejecutor tipado) | 3+2 | ≈15 | TopicNode(árbol), SourceSelection, focus, evento, universo | `ProfileConversationState` + `ContinuationContract` en envelope |
| Suppliers / A2 | RANK_SUPPLIERS | MIXTO (VALOR dominante) | 3 | 5 (+≈10 Frame ON) | métrica de ranking, tope 25, RANKING | ninguno (params `_supplier_ranking_*`) |
| Contract Activity | COUNT, GROUP_BY (BY_MODALITY/BY_STATE); SUM_VALUE→HANDOFF | LEE_TEXTO_PARA_VALOR (ejecutor tipado) | 1 | 4 (2 con binding) | proyecciones modality/estado/buyer; par de fechas cerrado | ninguno (recibo en Burr state) |
| Analytics legacy ×5 | SUM_VALUE, GROUP_BY/BY_YEAR, BY_BUYER; Execution y ValueStats sin op | LEE_TEXTO_PARA_RUTA | 5 | 0 | agregación avg/min/max/paid | ninguno |
| Radar / Follow | FOLLOW | MIXTO | 3 | 2 | nombre/id de radar, frecuencia, puente IR→QueryFrame | `RadarStore` SQLite, radar `follows` |
| Export | EXPORT (sin dueño) | EJECUTOR_YA_TIPADO | 0 | 0 | "filtro mostrado" | `export_ref` (Frame ON) |
| Direct lookup | ninguna (≈PROTOCOL) | EJECUTOR_YA_TIPADO | 1 | 1 (misma función) | identificador de lookup | ninguno |
| SEARCH tabular (ejecutor) | SEARCH / FOLLOWUP_DELTA | EJECUTOR_YA_TIPADO; productor LEE_TEXTO_PARA_VALOR | 0 (+≈25 compartidas) | 0 (+compartidas) | universo, ecosistema derivado | `PaginationBuffer`, `TabularGroup`, `Turn.suggestions` |

## 6. NO VERIFICADO
- Equivalencia semántica `BY_REPORTED_STATE` (CA) ↔ `Aspect.BY_STATE` (c12) y mapeo de Ranking→BY_BUYER, Temporal→BY_YEAR: inferidos del nombre, no de [C0].
- Si `_supplier_ranking_*` persisten en `Turn.parsed_params` y alguien los hereda.
- Tamaños de helpers no enumerados (`actor_intent`, `actor_arbitration`, `contract_conditions`, `source_occurrence`, `estado_families`) y qué fracción de cada módulo es lectura pura.
- Comportamiento end-to-end de H1 (sólo se verificó la semántica de `bind` y la lectura del código; no se ejecutó el workflow).
- Conteos de lectores de §3 por clave: manual, sin garantía de exhaustividad para claves que sólo pasan por `raw_params`.
- Con Frame ON, qué hace `profile_contract` con `export_ref`/`delegated` en todos los resultados (leído sólo turn_publication.py:94-110).
- Cualquier comportamiento con Natural Alpha en modo `alpha` (no medible offline).
