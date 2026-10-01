# A4 — Inventario de fronteras de capacidad (M1 Phase A)

Fecha: 2026-09-30. Base: worktree `secoppal-m1` sobre `main` @ `4ec99bc1` (+ contrato M1.0 v1.2).
Lane: A4, etiquetador `sonnet-A4`. No modifica `app/` ni `tests/`.

Entregables de este carril:

| Archivo | Qué es |
|---|---|
| `gold/neighbor.jsonl` | 111 casos sintéticos, 71 pares de frontera, `gold_schema.py` → `errors=0` |
| `scripts/audits/m1_search/boundary_probe.py` | sonda OFFLINE (puerta de Foundation + despacho real con productor de Foundation sustituido) |
| `a4_boundary_probe.json` | salida de la sonda: por caso, resumen, sensibilidad con actor resoluble y sondeos extra |

Todo lo medido aquí es **comportamiento de hoy con Natural Alpha apagado**. Lo que depende de la salida
de un LLM (Natural Alpha, bindings A2 / Contract Activity / FOLLOW, sidecar de polaridad) no se puede
medir offline y se declara como tal en cada fila (§7).

## 0. Resumen

1. **Hoy la puerta léxica protege a los vecinos casi por accidente.** De 58 casos NO-SEARCH puntuables,
   Foundation SEARCH se queda con **6** (10 %): dos preguntas conceptuales («qué es una licitación
   pública», «qué es un contrato de obra»), un FOLLOW («quiero que me avises cuando salgan
   licitaciones…»), un EXPORT («quiero exportar a Excel los contratos del SENA…»), un RANK_SUPPLIERS
   («necesito los 5 proveedores con más contratos del SENA») y un MULTI SEARCH+FOLLOW («busca contratos
   de aseo en Cali y avísame cuando salgan nuevos», donde el FOLLOW se pierde en silencio).
2. **Sin la puerta léxica (M1.5 la borra) los secuestros suben a 25/58.** Por vecino: Suppliers 7/8,
   Radar 6/8 (los comandos literales «mis radares», «ejecuta el radar X» siguen protegidos por su handler),
   Export 3/3, conceptuales 3/3, PROFILE_ACTOR sin marcador 2/9 («quién es X», «información de X»),
   Contract Activity 2/16 (conteos sobre PROCESOS), Entity Profile 1/8 («cuéntame sobre el SENA»).
   **Suppliers, FOLLOW en lenguaje natural y Export no tienen ningún detector de texto** en el camino
   sin Natural Alpha: la única barrera hoy es la regex de la puerta.
3. **Vetos a SEARCH legítimo: 13/47** (más 6 rechazados por la regex léxica; unión = 16/47 no llegan a
   Foundation). Por dueño: ACTOR 11, ANALYTICS 1, ENTITY_PROFILE 1. Sólo 4 son aceptables por contrato (único
   filtro ACTOR, §6), 5 son condicionales (ACTOR + otra dimensión: depende de que Actor Profile la honre) y 4 no
   lo son (N007 veto falso de ACTOR sobre una búsqueda temática, N012 ANALYTICS, N051 ENTITY_PROFILE, N073).
4. **Los solapes más peligrosos** (§4): (a) la sonda de actor desnudo `contratos de X` + los marcadores
   `la corporación/la fundación/la empresa` reclaman compradores reales y temas; (b) `por año`/`el contrato
   más grande` los reclaman los analyzers legacy; (c) `_OVERVIEW` de Entity Profile («analiza») lo reclama
   aunque no haya comprador; (d) cuando el actor SÍ se resuelve, «contratos de X» deja de ser un listado y
   pasa a ser un perfil.
5. **Vocabulario de sustantivos de universo duplicado en 14 módulos** (§5), con conjuntos distintos
   (la puerta no conoce `convocatoria`, `convenio`; `source_occurrence` sí `procedimiento`).

## 1. Cómo se cede el turno hoy (cadena real)

`SecopalWorkflow._dispatch` (`app/core/orchestrator.py:392`), en este orden, y el primero que devuelve algo gana:

| # | Paso | Línea | Notas |
|---|---|---|---|
| 1 | `handle_refinement` (sólo con `seed_resolved_params`) | 404 | TABULAR |
| 2 | `handle_reset` · `handle_actor_selection` · `handle_radar_command` · `handle_pagination` · `handle_suggestion_selection` · `handle_direct_lookup` | 411-416 | Radar exige `chat_id` + `radar_store` (`handlers/radar.py:32`) |
| 3 | Natural Alpha `observe_turn` | 428 | `SECOPPAL_NATURAL_ALPHA_MODE` default `off` → `None` |
| 4 | `capability_for(observation, settings)` → binding | 435 | A2 / CA / FOLLOW LIST·RUN, cada uno tras su flag |
| 5 | `handle_bound_follow` | 436 | sólo `radar.list` / `radar.run` |
| 6 | **`run_foundation_search`** | 442-444 | `foundation_runtime.py:250` |
| 7 | `handle_normal_flow` (grafo Burr legacy) | 450 | |

Dentro de Foundation (`foundation_runtime.run_foundation_search`, :250-266):

1. `secoppal_foundation_mode == "foundation_v3"` (default `legacy`).
2. `continues_foundation_turn` (`foundation_route_owner.py:93`) → aclaración de continuación.
3. **Puerta léxica** `first_search_ineligibility` (:30). Sin historial y sin seed; y la regex
   `(busca(r)|muestra(me)|quiero|necesito|que|algun[oa]?|<sustantivo>)\b` al INICIO **y** un sustantivo
   `(procesos?|contratos?|oportunidades?|licitacion(es)?)` en cualquier lugar (:38-43), más la excepción
   «cuenta … suma».
4. **Cesión a dueños** `decline_reason` (`foundation_route_owner.py:74`): primero
   `natural_alpha_owner` (:63: `CAPABILITY_BINDING` si hay binding; `NATURAL_ALPHA_JOB` si un único job
   `MATCHED` no es SEARCH — **aunque no tenga binding**, y entonces corre legacy), luego
   `specialized_owner` (:41), en este orden y devolviendo el primero que reclama:
   **ACTOR** (`actor_claims` :20) → **ENTITY_PROFILE** (`detect_entity_profile_turn`, sólo *detectar*) →
   **CONTRACT_ACTIVITY** (`admit_contract_activity(...).status is not HANDOFF`, es decir, también cuando
   *aclara*) → **ANALYTICS** (cualquier `REGISTRY[i].detect`).

Orden de ejecución **dentro** del grafo legacy (`workflow/actions/execution.py`): perfil de entidad
(`classify_entity_profile_turn`, :97) → analytics (CA directo si hay `semantic_evidence`, luego
`REGISTRY`: Execution, ValueStats, Ranking, Temporal, AggregateSum, :109) → `run_entity_profile_turn`
(:148) → `maybe_handle_actor_profile` (:166) → tabular.

## 2. Inventario por capacidad vecina

Flags (defaults de código en `app/config.py`): `SECOPPAL_FOUNDATION_MODE=legacy` (:94),
`SECOPPAL_NATURAL_ALPHA_MODE=off` (:99), `SECOPPAL_A2_SUPPLIERS_BINDING_ENABLED=false` (:107),
`SECOPPAL_CONTRACT_ACTIVITY_BINDING_ENABLED=false` (:112), `SECOPPAL_NATURAL_FOLLOW_BINDING_ENABLED=false`
(:117), `SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED=false` (:191), `SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED=false`
(:83), `SECOPPAL_ACTOR_PROFILE_ENABLED=true` (:70; kill-switch), `SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED=false`
(:88). Radar y Contract Activity «directo» no tienen flag: están siempre activos.

### 2.1 Suppliers (RANK_SUPPLIERS, A2)

| Aspecto | Hallazgo |
|---|---|
| Detectores | **Ninguno de texto para un primer turno.** (a) Natural Alpha job `(ANALYZE, SUPPLIERS)` → `capability_binding/registry.py:35` `_BINDINGS` (requiere modo `alpha` + flag A2). (b) Seguimiento dentro de un perfil activo: `entity_profile/followup.py:10-22` (`_SUPPLIER_QUESTION`, `_WHO_CONTRACTS`, `_BY_VALUE`) con `_entity_profile_active`. (c) Foco `suppliers` de Entity Profile (`entity_profile/turn.py:41`), también sólo con perfil activo. (d) CA devuelve `HANDOFF/SUPPLIERS_NEIGHBOR` (`contract_activity/admission.py:56,243-245`) a un vecino que no tiene detector. |
| Formas | `proveedores?`, `contratistas?`; `principales/mayores/primeros/grandes` (`supplier_request_grammar.py:27`); «cuáles/quiénes son», «quién(es) tiene(n) más contratos / contrata(n) más»; cardinales («los 5…») por gramática propia de A2; «por valor / número de contratos». |
| Flags | binding A2 + sidecar de polaridad, ambos `false`. |
| Cesión de Foundation | sólo `natural_alpha_owner` → `CAPABILITY_BINDING`. Ninguna cesión por texto. |
| IR/params del ejecutor | `classify_entity_profile_turn(bound_focus="suppliers")` → `EntityProfileSemanticRequest` (comprador único resuelto, `TIME`, cardinalidad, valor; **prohibido ACTOR** según el manifiesto del roadmap); ejecutor `entity_profile.aspects.suppliers`. |
| Medido | 7/8 casos RANK_SUPPLIERS terminan hoy en `LEGACY_TABULAR` (el octavo, N109, lo toma Foundation): la pregunta se convierte en una búsqueda de contratos con palabras de control como tema (`objeto=['quienes','principales']`, `['contratista','facturo']`…). Sin puerta, Foundation capturaría 7/8 (N010 «proveedores de aseo» es ambiguo y SEARCH es un dueño aceptable). |

### 2.2 Contract Activity y analytics legacy (COUNT / SUM_VALUE / GROUP_BY)

| Aspecto | Hallazgo |
|---|---|
| Detectores CA | `admit_contract_activity` (`contract_activity/admission.py:109`) sobre el recibo `semantic_evidence` del QueryRouter: controles analíticos de `analytic_control.py` (`_COUNT_CUES` :194 — `cuántos/cuántas`, `cuéntame…`, `número/total/cantidad de`; `_GROUP_VERB` :134 — `desglosa`, `distribuye`, `agrupa`; `_GROUP_NOUN` :137). `_operation` (:217) entrega `HANDOFF` por: `procesos` sin `contratos` (`PROCESS_NEIGHBOR`), verbos `lista/muestra/enumera` (`SEARCH_NEIGHBOR`), `valor total / cuánto suman` (`TOTAL_VALUE_NEIGHBOR`), `proveedores` (`SUPPLIERS_NEIGHBOR`). |
| Detectores legacy | `REGISTRY` (`analytics/registry.py:30`): `ExecutionAnalyzer` (`execution.py:22`: «cuánto se ha pagado/ejecutado», «nivel de ejecución»), `ValueStatsAnalyzer` (`value_stats.py:15`: «valor promedio/mínimo/máximo», **«contrato más grande/caro/costoso»**), `RankingAnalyzer` (`ranking.py:14`: «qué entidades contratan más», «top entidades»), `TemporalAnalyzer` (`temporal.py:14`: **«por año», «evolución», «tendencia», «histórico»**, «contratación anual»), `AggregateSumAnalyzer` (`aggregate_sum.py:15`: «cuánto se contrató/suman», «total contratado»). |
| Operaciones que CA sabe | sólo `TOTAL`, `BY_MODALITY`, `BY_REPORTED_STATE` (`admission.py:97` `_HOST_OPERATION`), un único comprador (`BUYER_SCOPE_REQUIRED`), sólo contratos, sin tema (`UNSUPPORTED_MATERIAL_FILTER`) y sin actor (`BUYER_ONLY_SCOPE`). `BY_YEAR`, `BY_TYPE`, `BY_BUYER`, `AVG/MIN/MAX` y «pagado» no existen en CA; los cubren (mal) los analyzers legacy. |
| Flags | binding CA `false` (sólo gobierna el camino vía Natural Alpha). El camino **directo** corre siempre que haya `semantic_evidence` (`registry.py:116-121`). |
| Cesión de Foundation | `specialized_owner` devuelve `CONTRACT_ACTIVITY` cuando el estado **no es HANDOFF** — incluye `CLARIFY` (p. ej. `BUYER_SOURCE_PROJECTION_REQUIRED`, `MULTIPLE_OPERATIONS`); y `ANALYTICS` si cualquier `detect` casa. |
| IR/params del ejecutor | `ContractActivityRequest`: `operation`, `buyer_units`, `source_buyer_names`, `date_from/to`, `modality_values`, `reported_states`, `money` (`models.py`). Analyzers legacy: `params` planos + `soql_builder._build_where`. |
| Medido | 8 casos CA reclamados por el dueño en la puerta; 8 ANALYTICS. Ninguno de los 16 casos COUNT/GROUP/SUM acaba hoy con una respuesta certificada offline (sin red → mensaje «No tengo un conteo certificado», `analytics_*_error`); lo relevante es que **no** llegan a Foundation. |

### 2.3 Entity Profile (PROFILE_BUYER)

| Aspecto | Hallazgo |
|---|---|
| Detector | `detect_entity_profile_turn` (`entity_profile/turn.py:115`): `_OVERVIEW` (:36) = `analiza(me)`, `como (ha) contrata(do)`, `que sabes de`, `perfil de/del`, `radiografia de/del`. Con perfil activo además: foco (`_FOCUS` :40: proveedores, modalidad, valores/montos, recientes/últimos, evolución/por año), `_TIME_FOLLOWUP` (:51), `_SOURCE_FOLLOWUP` (:59), «sí/claro/dale» con pendiente. La **clasificación** real es `classify_entity_profile_turn` (:228) y exige comprador (`resolve_profile_scope`). |
| Flags | `SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED=false` (admisión Frame, `turn_coordination.py:19-39`); `frame_turn.is_profile_turn` (:123) reutiliza el mismo detector. |
| Cesión de Foundation | usa `detect` y **no** `classify`: reclama aunque el perfil después no ejecute (comprador ausente, territorio ambiguo). |
| IR/params | `EntityProfileSemanticRequest` + `EntityProfileScope/Identity` + `EntityProfileExecutionPlan`; foco `overview|suppliers|modalities|values|recent|temporal`. El Frame tiene estado propio (56 archivos, `app/core/entity_profile/`), que M2 debe absorber. |
| Medido | 7/8 PROFILE_BUYER llegan a `ENTITY_PROFILE`; `cuéntame sobre el SENA` no lo reconoce ningún regex (cae a tabular con `objeto=['cuentame']`). El detector reclama además un SEARCH, `analiza contratos de pavimentación en Córdoba` (N051), y un PROFILE_ACTOR, `perfil de UNION TEMPORAL PUENTES 2024` (N067: el perfil de entidad lo detecta, luego no lo ejecuta por falta de comprador, y Actor no lo reclama sin marcador; cae a tabular). |

### 2.4 Actor y Actor Profile (PROFILE_ACTOR / ACTOR como filtro)

| Aspecto | Hallazgo |
|---|---|
| Detectores | Sintaxis fuerte en el parse: `actor_intent.py` — marcadores de rol `_ROLE_MARKERS` (:118: proveedor [adjudicado], contratista, adjudicatario), marcadores con determinante `_DETERMINED_ROLE_MARKERS` (:121: **empresa, firma, persona, sociedad, corporación, fundación**), documento `NIT/CC` (:103-110), construcciones verbales `_VERB_PATTERNS` (:128-139: «qué ha contratado», «contratos adjudicados a», «historial contractual de», «contratos celebrados con»…). Sonda desnuda `actor_arbitration._BARE_ACTOR_RE` (:37) `contratos? de (.+)$` + `detect_bare_actor_probe` (:112), o nombre desnudo sin sustantivo de universo. Resolución: `resolve_actor_params` (`workflow/actions/actor_resolution.py`, usada tanto por el grafo como por `actor_claims`). Rechazo de sustantivos de universo: `actor_conversation._NON_ACTOR_SEARCH_LEADS` (:267). |
| Qué reclama el perfil | `is_actor_profile_turn` (`actor_profile/turn.py:40`): identidad autoritativa + `contratista` + dataset que soporta actor + **no** `_asks_for_a_listing` (:62: sólo un TEMA o un ORDEN explícito). Es decir: **todo lo demás con actor resuelto es un perfil, no un listado**. |
| Flag | `SECOPPAL_ACTOR_PROFILE_ENABLED=true` (kill-switch). |
| Cesión de Foundation | `actor_claims` (`foundation_route_owner.py:20`) = flags del parse **o** la proyección de `resolve_actor_params`; ACTOR es el **primero** en `specialized_owner`. |
| IR/params | `contratista` (identidad de documento), `actor_scope{role, resolution_method, observed_name, requires_resolution}`, `actor_pending`, `actor_intent_conflict`, `actor_resolution_method`. El perfil honra buyer/geografía/fechas/valor/estado/modalidad/tipo/tema vía `actor_profile/scope.py:build_actor_profile_scope` (misma WHERE). |
| Vocabulario Foundation | **ACTOR no existe en el prompt v4.2** (contrato §10): si Foundation toma un turno con actor, el nombre del actor sólo puede viajar como tema. |
| Medido | ver §6.3 (sensibilidad con actor resoluble). |

### 2.5 Radar / FOLLOW

| Aspecto | Hallazgo |
|---|---|
| Detectores | `radar/commands.py`: `_LIST_RE` (:46), `_SAVE_RE` (:55: `guard*/cre*… radar <nombre>`), `_RUN_RE` (:62: `ejecut*/revis*/corre*/actualiz*/run radar <nombre>`); todos anclados a la palabra **«radar»**. `handle_radar_command` (`handlers/radar.py:31`) exige `chat_id`. `external_admission.external_owner` usa el mismo parser. |
| Formas NO reconocidas | «avísame cuando…», «notifícame si…», «sígueme…», «ponme una alerta…», «quiero que me avises…». |
| Flags | binding FOLLOW `false`; `registry.py:36-39` sólo enlaza `FOLLOW·LIST` y `FOLLOW·RUN`; `FOLLOW·SAVE` **no** se enlaza a propósito (:15-17: el almacén exige un nombre que la persona no dio). |
| Cesión de Foundation | el handler de Radar corre **antes** (:413), pero sólo protege el comando literal. Sin Natural Alpha el FOLLOW en lenguaje natural no tiene dueño en ningún estado de flags. |
| IR/params | Radar guarda `resolved_params` de la última búsqueda tabular (`handlers/radar.py:54-61`) → un FOLLOW sobre una búsqueda de Foundation (`append_turn(..., {}, ...)`, `foundation_runtime.py:228-233`) **no tiene params que guardar**. |
| Medido | 3/3 comandos literales → `RADAR`; 5/5 FOLLOW naturales → `LEGACY_TABULAR` con el verbo de alerta como tema (`objeto=['avisame','salgan','vias']`); 1 FOLLOW con arranque «quiero…licitaciones» lo toma Foundation. |

### 2.6 Natural Alpha (jobs)

`natural_alpha/catalog.py`: `ACTION_ASPECTS` = SEARCH(4) · ANALYZE(13: OVERVIEW, VALUES, VALUE_STATS, TOTAL_VALUE,
SUPPLIERS, MODALITIES, RECENT, TEMPORAL, EXECUTION, TOP_ENTITIES, CONTRACT_COUNT, CONTRACTS_BY_MODALITY,
CONTRACTS_BY_STATE) · FOLLOW(SAVE, LIST, RUN) · RESULT_ACTION(PAGINATE, EXPORT_XLSX) · LOOKUP · OTHER.
Con modo `alpha` y ≥2 jobs → `MULTI_JOB_BLOCKED` y respuesta terminal «hazlas por pasos»
(`handlers/natural_alpha.py:multi_job_result`): hoy MULTI no ejecuta nada en ninguna ruta. Sólo se ligan a
una capacidad `ANALYZE·SUPPLIERS`, los `CONTRACT_COUNT/…BY_*` de CA y `FOLLOW·LIST/RUN`. **No medible offline.**

### 2.7 Export y protocolo (para completar la frontera)

- **Export**: ningún detector de texto. Existe `export_xlsx.py` y `foundation_presentation.export_fields`
  (botón); Natural Alpha lo llama `RESULT_ACTION·EXPORT_XLSX` y no está enlazado. Un «exporta a Excel los
  contratos del SENA…» de primer turno → legacy tabular con `exporta`, `excel` como tema.
- **Protocolo**: `is_pagination_phrase` (`conversation_store.py:610`), ordinales, `is_reset_command`,
  `detect_direct_lookup`, `admission_rules.input_shape`. Todos exigen historial; sin él «ver más» y
  «opción 2» caen a la puerta léxica de Foundation (y la rechaza por regex, no por semántica).

## 3. Resultados de la medición (gate de hoy)

Fuente: `a4_boundary_probe.json` (`summary`). 111 casos: 64 NO-SEARCH (58 puntuables + 6 `NEEDS_HISTORY`
que no se cuentan: sólo significan algo con un turno previo) y 47 SEARCH. Sin red, sin Natural Alpha,
`foundation_mode=foundation_v3`, un `chat_id` fresco por caso, persistencia en scratch.

Descomposición de `specialized_owner` por caso: `None` 68 · ACTOR 17 · ENTITY_PROFILE 10 · ANALYTICS 8 ·
CONTRACT_ACTIVITY 8. Puerta léxica y despacho real coinciden en los 111 casos
(`gate_vs_dispatch_disagreements = []`).

### 3.1 NO-SEARCH que Foundation SEARCH captura (secuestros)

**Hoy (con puerta): 6/58**

| id | Texto | Debería ser | Por qué pasa |
|---|---|---|---|
| N091 | qué es una licitación pública | OUT_OF_DOMAIN (conceptual) | arranca con «qué» (`que\b`) y trae «licitación» |
| N092 | qué es un contrato de obra | OUT_OF_DOMAIN (conceptual) | ídem con «contrato» |
| N099 | busca contratos de aseo en Cali y avísame cuando salgan nuevos | MULTI [SEARCH, FOLLOW] | arranca con «busca»; el FOLLOW se pierde sin aviso |
| N105 | quiero que me avises cuando salgan licitaciones de vías en Antioquia | FOLLOW | «quiero» + «licitaciones» |
| N107 | quiero exportar a Excel los contratos del SENA en 2024 | EXPORT | «quiero» + «contratos» |
| N109 | necesito los 5 proveedores con más contratos del SENA | RANK_SUPPLIERS | «necesito» + «contratos»; CA devuelve `SUPPLIERS_NEIGHBOR` y nadie lo recoge |

Control: N104 «quiero saber cuántos contratos tiene la Alcaldía de Soledad» pasa la puerta léxica y **lo salva
únicamente CA** (`CONTRACT_ACTIVITY`).

**Con la puerta borrada (M1.5): 25/58** — los 6 anteriores más:

| Vecino | Casos | Ids |
|---|---|---|
| Suppliers | 6 más (7/8 en total) | N000 N002 N004 N006 N008 N011 |
| Radar (lenguaje natural) | 5 más | N074 N080 N082 N084 N086 |
| Export | 2 más | N087 N090 |
| Conceptual / fuera de dominio | 1 más | N093 |
| Contract Activity (COUNT sobre PROCESOS, `PROCESS_NEIGHBOR`) | 2 | N018 N041 |
| Entity Profile (perfil sin regex) | 1 | N050 «cuéntame sobre el SENA» |
| Actor Profile (perfil sin marcador) | 2 | N057 «quién es X», N060 «información de X» |

Los comandos literales de Radar (N078/N079) y `ver más` siguen protegidos por sus handlers; los 6 casos
`NEEDS_HISTORY` (N013 N076 N089 N095 N096 N101) no se puntúan.

### 3.2 SEARCH vetados a un vecino (13/47) y rechazados por la puerta léxica (6/47)

| id | Texto | Vetado por | ¿Aceptable por contrato? |
|---|---|---|---|
| N061 | contratos de SERVICIOS INTEGRALES DEL CARIBE LTDA | ACTOR | **sí** (único filtro ACTOR, §6) |
| N063 | contratos del NIT 900.000.001-1 | ACTOR | **sí** |
| N070 | contratos adjudicados a CONSORCIO PAE ORIENTE | ACTOR | **sí** |
| N072 | contratos del contratista Juan Ejemplo Prueba | ACTOR | **sí** |
| N001 | contratos del proveedor X con la Gobernación del Huila | ACTOR | condicional: ACTOR + BUYER, sólo si honra el comprador |
| N058 | contratos de CONSORCIO VIAL DEL NORTE en Córdoba | ACTOR | condicional (ACTOR + GEO) |
| N066 | contratos de UNION TEMPORAL PUENTES 2024 con el INVIAS | ACTOR | condicional (ACTOR + BUYER) |
| N068 | contratos de la empresa X mayores a 100 millones | ACTOR | condicional (ACTOR + VALUE) |
| N071 | contratos de construcciones andinas sas en huila | ACTOR | condicional (ACTOR + GEO) |
| N073 | procesos del proveedor CONSORCIO PAE ORIENTE | ACTOR | no: actor sobre PROCESOS |
| N007 | los 10 contratos de obras más caros en Antioquia | ACTOR | **no — veto falso**: la sonda de actor desnudo toma «obras más caros en antioquia» y pregunta «¿obras como tema o proveedor?» |
| N012 | el contrato más grande de la Alcaldía de Cali | ANALYTICS (ValueStats) | **no — veto falso** |
| N051 | analiza contratos de pavimentación en Córdoba | ENTITY_PROFILE | **no — veto falso**: `_OVERVIEW` casa «analiza» sin comprador |

Rechazados por la regex léxica (no por vecino): N007 («los…»), N012 («el…»), N021 («los contratos más caros
de la Alcaldía de Medellín…»), N039 («lista los contratos…»), N051 («analiza…»), N083 («quiero ver
convocatorias…»: `convocatorias` no está en la lista de sustantivos). **31/47 SEARCH llegan hoy a Foundation.**

## 4. Solapes: formas que dos dueños reclaman

| # | Forma | Dueños que la reclaman | Efecto | Evidencia |
|---|---|---|---|---|
| S1 | `contratos de <tema>` | sonda de actor desnudo (`actor_arbitration.py:37`) **vs** SEARCH temático | el tema se trata como candidato a proveedor y se pregunta; Foundation lo cede | N007 |
| S2 | `la corporación / la fundación / la empresa / la sociedad <nombre>` | marcadores con determinante (`actor_intent.py:121`) **vs** comprador con ese nombre | compradores reales (CAR/CVC, universidades privadas) se leen como actor | sondeos: «fundación universitaria de Cali», «corporación autónoma regional del Valle» → ACTOR |
| S3 | `por año`, `evolución`, `tendencia`, `histórico` | TemporalAnalyzer **vs** SEARCH con TIME / PV1 `HISTORICAL_PROCESSES` / GROUP_BY·BY_YEAR | un SEARCH con «por año» lo reclama ANALYTICS | sondeo «busca contratos de aseo por año en Cali» → ANALYTICS |
| S4 | `contrato más grande/caro/costoso` | ValueStats **vs** SEARCH + ORDER value_desc límite 1 | veto + entidad no resuelta | N012 |
| S5 | `analiza <tema> en <lugar>` | `_OVERVIEW` de Entity Profile **vs** SEARCH temático | el perfil detecta, no clasifica (sin comprador) | N051 |
| S6 | `perfil de <razón social sin marcador>` | Entity Profile (detecta) **vs** Actor Profile (no reclama sin marcador) | nadie lo ejecuta: cae a tabular con el nombre como tema | N067 |
| S7 | `proveedor / contratista` | marcador de rol del filtro ACTOR (`actor_intent.py:118`) **vs** sujeto del ranking Suppliers | «contratos del proveedor X» → ACTOR; «los mayores proveedores del SENA» → nadie | N001 vs N000/N002 |
| S8 | `modalidad(es)` | foco de Entity Profile **vs** `_GROUP_VERB` de CA **vs** filtro MODALITY de SEARCH | tres lecturas de una palabra | N014 vs N015 |
| S9 | `valor(es) / monto(s)` | foco `values` del perfil **vs** AggregateSum **vs** VALUE **vs** ORDER (`control_language.py:83`) | cuatro dueños | N020/N022/N023 |
| S10 | `recientes / últimos` | foco `recent` del perfil **vs** ORDER `date_desc` (`control_language.py:136,147`) | ambos | N027 |
| S11 | `cuántos … tiene <actor>` | CA (`BUYER_ONLY_SCOPE`) **vs** Actor Profile | el nombre del actor acaba como `objeto` (§6.3) | N040 |
| S12 | arranque `quiero/necesito/busca` + sustantivo | puerta léxica **vs** FOLLOW / EXPORT / RANK / conceptual | secuestro | N099 N105 N107 N109 |
| S13 | `quién es X`, `información de X`, `qué tal es X` | ninguno / Actor (sólo con marcador `la empresa`) | huérfano | N057 N060 vs N069 |
| S14 | ACTOR resuelto + `contratos de X` | Actor Profile (perfil) **vs** SEARCH (listado) | el usuario pide registros y recibe un perfil | §6.3 |

## 5. Vocabulario duplicado

Sustantivos de universo (proceso/contrato/licitación/convocatoria/oportunidad…) — **14 módulos**, conjuntos
distintos:

| Módulo : línea | Conjunto |
|---|---|
| `foundation_runtime.py:38` (puerta) | procesos, contratos, oportunidades, licitación(es) — **sin** convocatoria/convenio |
| `foundation_basic_fallback.py:25-26` | contrato(s), proceso(s), licitación(es), oportunidad(es), convocatoria(s) |
| `foundation_universe_policy.py:31-33` | OPPORTUNITY_HEADS / CALL_HEADS / TENDER_HEADS |
| `query_router.py:143, 471, 1483` | `_OPPORTUNITY_SOURCE_WORDS`; lista de palabras clave; heurística de dataset |
| `source_occurrence.py:23-42` | procesos, licitaciones, convocatorias, **procedimientos**, contratos (+ postpuesto) |
| `actor_conversation.py:267-274` | + compra(s), contratación(es), selección abreviada, mínima cuantía, concurso de méritos |
| `actor_arbitration.py:38` | contratos, procesos, **convenios** |
| `entity_profile/turn.py:58` | procesos, contratos, licitaciones, convocatorias |
| `entity_profile/claim_adapters.py:87-88` | idem + procedimientos |
| `contract_activity/admission.py:226-235` | contratos?, procesos?, contrataciones? (inline en `_operation`) |
| `followup_engine.py:214, 222` | cuántos contratos/procesos; contratos, procesos, licitaciones, convocatorias |
| `dialogue_policy.py:116-119` | `_SECOP_NOUNS` (oferta, licitación, convocatoria, …) |
| `process_state_request.py:82` | `OPPORTUNITY_HEAD_SURFACES` |
| `intent_vocabulary.py:236` | «qué procesos en convocatoria/publicados/abiertos/vigentes» |

Palabras de rol/pregunta de proveedor: `actor_intent.py:118-122`, `actor_arbitration.py:40`
(`_ACTOR_SYNTAX_TOPIC_TERMS`), `entity_profile/followup.py:10-22`, `entity_profile/turn.py:41,66`
(`_FOCUS`, `_PROFILE_NOISE`), `contract_activity/admission.py:56`, `supplier_request_grammar.py:27`,
`source_occurrence.py` (`_HEAD_FILLERS`), `query_router.py:189,474,1468,2179`. Ejes temporales
(«por año», «recientes», «últimos») repetidos en `analytics/temporal.py`, `entity_profile/turn.py:40-56`,
`control_language.py` y `contract_activity/filters.py`.

## 6. Medición adicional

### 6.1 Natural Alpha / bindings: lo que NO se midió

No hay proveedor LLM offline, así que `natural_alpha_owner` (`CAPABILITY_BINDING`, `NATURAL_ALPHA_JOB`) y los tres
bindings no se ejercen. Consecuencia para M1: en producción con Alpha, **un único job no-SEARCH `MATCHED`
hace que Foundation decline aunque ninguna capacidad lo ligue**, y entonces corre el grafo legacy
(`foundation_route_owner.py:68-71`). Mide esa ruta en el A/B con un proveedor inyectado.

### 6.2 Actores inventados vs resolubles

Los actores del Gold son inventados y **no** existen en el directorio (`UNRESOLVED`): el dueño reclama el turno,
pero el resultado es «No encontré una coincidencia exacta…». Para medir qué pasa con identidad resuelta, la
sonda repite 14 formas con un alias genérico que sí está en el índice (`CENTRAL DE SUMINISTROS SAS`).

### 6.3 Sensibilidad: actor resuelto (`sensitivity_resolvable_actor`)

| Forma | Atiende hoy | Condiciones que llegan a la ejecución |
|---|---|---|
| `contratos de {X}` | **Actor Profile** (perfil, no listado) | actor |
| `contratos del proveedor {X} con la Gobernación del Huila` | Actor Profile | actor + comprador |
| `contratos de {X} en 2024` | Actor Profile | actor + fechas |
| `contratos de {X} mayores a 100 millones` | Actor Profile | actor + `valor_min` |
| `contratos de {X} en Córdoba` | resolución de actor / tabular | actor + departamento |
| `contratos de aseo de {X}` | resolución fuzzy (`fts5_trigram_bm25`) | **pierde actor y tema** |
| `contratos de {X} por contratación directa` | resolución fuzzy | **pierde actor y modalidad** |
| `contratos terminados de {X}` | **Foundation** (nadie lo reclama) | Foundation no tiene ACTOR (§10) |
| `los contratos más caros de {X}` | tabular | **actor como tema** (`objeto=['central','suministro']`) + orden |
| `perfil del proveedor {X}` / `{X}` desnudo | Actor Profile | actor |
| `quién es {X}` | resolución fuzzy | pierde el actor |
| `información de {X}` | tabular, **dataset procesos** | actor como tema |
| `cuántos contratos tiene {X}` | tabular | actor como tema |

Lectura: con ACTOR resuelto, §6 («Actor Profile sólo si honra todas las otras condiciones») se cumple para
BUYER/TIME/VALUE/GEO, **pero entrega un perfil, no una lista**; falla para TOPIC, MODALITY, ORDER, STATE y para
todo lo que no sea `contratos de X`/marcador. El borde se rompe en la ortografía de la frase, no en la semántica.

### 6.4 Sondeos fuera del Gold (`spot_checks`)

| Texto | Puerta | Dueño hoy |
|---|---|---|
| busca contratos de aseo por año en Cali | ANALYTICS | analytics temporal |
| busca contratos de la fundación universitaria de Cali en 2025 | ACTOR | resolución de actor |
| quiero ver contratos de la corporación autónoma regional del Valle | ACTOR | resolución de actor |
| busca contratos de alimentación escolar, y dime cuántos hay | CONTRACT_ACTIVITY (`MULTIPLE_OPERATIONS`) | tabular |
| necesito los contratos del NIT 899999001 con la Alcaldía de Soledad | ACTOR | Actor Profile |
| busca contratos de la empresa de acueducto de Bogotá · … sociedad de activos especiales · … del proveedor de aseo del SENA | ninguno | Foundation |

## 7. Casos frontera que el contrato M1.0 no resuelve (CONTRACT_GAPS)

Marcados con `contract_gap` en el Gold donde aplica.

1. **«proveedores de <tema>» sin comprador** (§2 lo manda a la cola; N010). Hoy ningún dueño lo atiende.
2. **«qué ha contratado X» / «qué contratos tiene X»** (N062): B2 fija el universo pero no la operación
   (¿SEARCH o PROFILE_ACTOR?). Actor Profile ya lo atiende como perfil.
3. **«contratos de X» con X institución** («la corporación autónoma regional…», «fundación universitaria…»):
   D5 resuelve por *operación* pero no por *tipo de entidad*; el contrato no dice cómo separar BUYER de ACTOR
   cuando el nombre empieza como una razón social (S2).
4. **Operaciones analíticas ausentes**: promedio/mínimo/máximo (N032), ejecución contratado-vs-pagado (N036),
   `BY_BUYER` global («qué entidades contratan más», N034) y `BY_YEAR`/`BY_TYPE` existen en `aspect` pero CA no
   las implementa; no hay dueño de destino claro para M3.
5. **«analiza <tema> en <lugar>»** (N051): B3 sólo cubre comprador/actor.
6. **«por año»** como eje de desglose vs filtro temporal vs ORDER: la regla de frontera §2 sólo ilustra
   «por contratación directa»; «por año / por entidad / por tipo» no están zanjados.
7. **EXPORT con consulta propia** (N087, N107): ¿EXPORT·requests o MULTI[SEARCH, EXPORT]? El esquema permite
   ambas; el validador no distingue.
8. **MULTI**: los `requests` de jobs no-SEARCH no se validan (sólo SEARCH); «busca X y avísame» (N099) es MULTI
   en el Gold pero hoy no tiene ejecutor en ninguna ruta (Alpha lo bloquea con «por pasos»).
9. **Cardinalidad de ranking** («los 10 mayores proveedores»): `limit_n` sólo está definido para `ORDER` de
   SEARCH; en RANK_SUPPLIERS no tiene dónde ir.
10. **ACTOR sobre PROCESOS** (N073): no se verificó que el dataset lo soporte.
11. **COUNT/GROUP sobre un ACTOR** (N040): CA lo rechaza (`BUYER_ONLY_SCOPE`); destino M3 sin dueño.
12. **Preguntas conceptuales** («qué es una licitación pública»): ¿OUT_OF_DOMAIN o una operación `EXPLAIN`?
13. **Municipio desnudo** («perfil de Montería», N053): alcaldía vs territorio; el contrato no tiene una
    disposición para «comprador ambiguo» (se etiquetó CLARIFY/`BUYER_VS_TERRITORY`).

## 8. Recomendaciones

### Para M1 (SEARCH)

1. **No borrar la puerta léxica antes de que cada vecino tenga su detector de rechazo**, o Foundation capturará
   25/58 de lo no-SEARCH. Orden seguro: primero Suppliers/FOLLOW/EXPORT/«preguntas conceptuales» obtienen un
   rechazo determinista *anterior* a Foundation (gramáticas cerradas, no regex heredadas), luego se retira la puerta.
2. **Foundation debe poder rechazar por operación**: el intérprete v5 necesita `operation` en su vocabulario
   para devolver «esto es COUNT / RANK / FOLLOW / EXPORT / PROFILE» y que el host dirija al ejecutor vecino o
   aclare. Sin eso, SEARCH seguirá dependiendo de que ningún regex reclame el turno.
3. **ACTOR entra a M1 como dimensión de SEARCH** (ya está en el roadmap) y debe cubrir, con paridad medida,
   exactamente las formas del §6.3 que hoy pierden el actor (TOPIC, MODALITY, ORDER, STATE) — son las que
   Foundation tomará en cuanto no haya `contratos de X` literal.
4. **Resolver el veto falso de ACTOR (S1/S2) antes de la paridad**: es el único veto que quita búsquedas
   válidas con certeza (N007). Opciones: pasar el span del actor por la autoridad de directorio *antes* de
   preguntar «¿tema o proveedor?», y no usar `la corporación/la fundación` como marcador sin evidencia de
   directorio.
5. **Cambiar `detect_entity_profile_turn` por `classify_entity_profile_turn` en `specialized_owner`** (o
   cualquier cesión por *capacidad real*, no por *forma*): hoy Foundation cede ante perfiles que no ejecutan
   (N051, N067).
6. **CA «claims» con `CLARIFY`**: Foundation cede también cuando CA sólo aclara (`BUYER_SOURCE_PROJECTION_REQUIRED`).
   Decidir si la aclaración de CA es mejor que un listado SEARCH en esos casos.
7. **Unificar el léxico de sustantivos de universo** (14 módulos, §5) en una sola función pura que sirva a puerta,
   fallback y políticas, antes de añadir `convocatoria`/`convenio`/`selección abreviada` en un 15º sitio.

### Para M3 (vecinos)

1. **Manifiestos con `forbidden: [ACTOR]`** (Suppliers) y `required: [BUYER]` (CA, Entity Profile) son la forma
   de resolver S7 y S11: la validación de dimensiones por operación en el host.
2. **Radar necesita params que guardar de una búsqueda Foundation**: hoy `append_turn(…, {}, …)` no deja
   `resolved_params`; un FOLLOW/«guardar como radar» sobre un resultado de Foundation fallará (`radar_save_no_context`).
3. **Actor Profile**: separar explícitamente «listado» de «perfil» por operación, no por la presencia de tema/orden
   (`_asks_for_a_listing`); hoy «contratos de X» devuelve un perfil.
4. **Cerrar los huecos de §7** antes de escribir manifiestos: AVG/MIN/MAX, pagado, BY_BUYER/BY_YEAR/BY_TYPE, EXPORT
   con consulta, MULTI no-SEARCH.
5. **Medir Alpha**: repetir esta sonda con un proveedor inyectado para cuantificar `NATURAL_ALPHA_JOB`.

## 9. Reproducción y límites

```
cd /Users/rodrigoortiz/Documents/secoppal-m1
PYTHONPATH=. /Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/m1_search/boundary_probe.py --scratch <dir>
python3 scripts/audits/m1_search/gold_schema.py docs/audits/m1_search_foundation/gold/neighbor.jsonl
```

- Sockets bloqueados; `burr.telemetry` apagado; `ConversationStore`, feedback y radar apuntan a `--scratch`.
- El productor de Foundation se sustituye por un stub que sólo lanza `Claimed`: el hecho de que corra es la
  evidencia de que la puerta admitió. Los datos de legacy son irrelevantes (sin red), no su ruta.
- `SECOPPAL_*` se leen de los defaults de código (`Settings(_env_file=None)`); Natural Alpha `off`.
- Actores del Gold inventados (no resolubles); el alias resoluble de §6.3 es genérico y sólo se usa en la sonda,
  no en el Gold. Una consulta de usuario real con el actor en el directorio se comporta como §6.3, no como el
  Gold.
- 6 casos `NEEDS_HISTORY` se miden pero no se puntúan (no hay turno previo).
- Etiquetas con `ok_owners` / `conditional_owners` se leen del campo `neighbor` del Gold (extensión de A4; los
  validadores ignoran campos extra).
