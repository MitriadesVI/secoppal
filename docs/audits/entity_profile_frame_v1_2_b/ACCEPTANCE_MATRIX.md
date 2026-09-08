# V1.2-B — Matriz de aceptación

Honestidad sobre el orden de trabajo: `INVENTORY.md` se escribió **antes** de
tocar código, como exige la orden. Esta matriz se redactó **junto con** las
pruebas que la implementan, no como un artefacto previo separado. Cada fila
corresponde a una prueba real, nombrada, que se ejecuta y se puede reproducir.

Columnas: entrada real → estado previo → ruta admitida → Frame → jobs/SQL →
evidencia → respuesta → estado tras recarga.

## 1. Los ocho escenarios INTEGRATION_B

| # | Entrada | Estado previo | Ruta | Frame | Jobs / SQL | Evidencia | Respuesta | Tras recarga | Prueba |
|---|---|---|---|---|---|---|---|---|---|
| R5a | «analiza contratos firmados de … en agosto» → «proveedores» | ninguno → confirmado overview/CONTRACTS/agosto | PROFILE (`profile_turn`) | foco suppliers, agosto conservado | 2 jobs, ambos con `fecha_de_firma` y `2026-08-01` | 2 llamadas intentadas, 2 SUCCESS | dos TOP5 con encabezado «Agosto de 2026, según fecha de firma» | `confirmed.focus == suppliers`, periodo agosto | `test_r5a_inherited_period_is_in_every_query_after_a_real_reload` |
| R5b | «analiza procesos y contratos … en agosto» → «proveedores» → «solo contratos» | BOTH EXPLICIT | PROFILE | pendiente; luego CONTRACTS EXPLICIT + suppliers | 0 jobs en T2; 2 jobs con agosto en T3 | 0 llamadas en T2 | aclaración con alternativa; luego proveedores | confirmado suppliers/CONTRACTS, sin pendiente | `test_r5b_explicit_completeness_survives_the_reload` |
| R5c | «analiza procesos … en agosto» → «proveedores» → «solo contratos» | PROCESSES EXPLICIT | PROFILE | pendiente con alternativa tipada; luego CONTRACTS | 0 jobs; luego 2 jobs sólo sobre `jbjy-vk9h` | 0 llamadas en T2 | la alternativa DICE «fecha de firma» y «conservando agosto de 2026» | foco suppliers, agosto | `test_r5c_a_pending_selection_crosses_turns_and_keeps_its_focus` |
| R5c′ | … → «1» | pendiente con una alternativa | PROFILE (`profile_pending_selection`) | igual que R5c | 2 jobs de contratos | — | proveedores | idem | `test_r5c_an_ordinal_selects_the_published_alternative` |
| R5c″ | … → «sí» | pendiente | PROFILE | sin cambio | 0 jobs | 0 llamadas | se repite la pregunta | pendiente intacto | `test_a_bare_yes_never_chooses_the_alternative_for_the_person` |
| R5d | «… en julio» → «proveedores en agosto» → «recientes» → «cancela ese cambio» → «recientes» | PROCESSES/julio | PROFILE | confirmado nunca cambia de julio | 0, 0, 0 jobs; luego 1 job con `2026-07-01` | 0 llamadas en T2 y T3 | dos aclaraciones, cancelación, y recientes de julio | julio, sin pendiente | `test_r5d_a_pending_period_never_reaches_a_confirmed_execution` |
| R6 | «analiza procesos y contratos … en agosto» → «proveedores» / «montos» | BOTH EXPLICIT | PROFILE | origen EXPLICIT persistente | 0 jobs en ambos | 0 llamadas | aclaración de completitud | origen sigue EXPLICIT, foco sigue overview | `test_r6_explicit_origin_survives_persistence` |
| R6 control | «analiza … en agosto» (DEFAULT) → «proveedores» | BOTH DEFAULT | PROFILE | requested sigue BOTH/DEFAULT | 2 jobs sólo de contratos | 2 llamadas | responde y **declara** la limitación | universo sin recortar | `test_r6_control_default_origin_narrows_and_declares_it` |
| R7a | «analiza contratos … en agosto» → «mas» | perfil confirmado | CLARIFY (`profile_is_not_paginable`) | sin cambio | 0 jobs, ningún OFFSET | 0 llamadas | pide en qué profundizar | perfil intacto | `test_r7a_a_profile_does_not_seed_tabular_pagination` |
| R7b | «analiza contratos firmados … en agosto» → «sin filtro de fecha» → «recientes» | CONTRACTS/agosto | PROFILE | periodo CLEAR y durable | 4 y 1 jobs, ninguno con agosto | 4 y 1 llamadas | «Todo el histórico disponible» | periodo `UNBOUNDED` | `test_r7b_clear_survives_another_reload` |
| R7b′ | idem | — | — | tema sin negación inventada | ningún SQL con «filtro» | — | sin línea «Tema:» | — | `test_r7b_clearing_a_period_does_not_invent_a_negative_topic` |
| R7c | búsqueda tabular → perfil → «mas» | tabular publicado, luego perfil | CLARIFY | sin cambio | 0 jobs | 0 llamadas | cursor tabular inerte | perfil sigue siendo el dueño | `test_r7c_a_tabular_cursor_is_inert_after_a_profile` |

## 2. Tabla de transiciones (sección 10 del diseño)

Cada fila se prueba sobre el clasificador puro y, cuando exige efectos, de
extremo a extremo. El producto completo —6 formas de entrada × 5 dueños × 3
estados de sobre = 90 combinaciones— se recorre para comprobar que la decisión
es total y de valor único.

| Estado previo | Entrada | Decisión esperada | Prueba |
|---|---|---|---|
| Sin historial / contrato inválido | «más» | CLARIFY `no_referent_to_continue`, cero consultas | `test_more_without_a_referent_clarifies_instead_of_inventing_an_offset` |
| Tabular con grupos | «más» | DELEGATED a `handle_pagination` | `test_more_over_a_tabular_result_keeps_its_handler` |
| Cola corrupta | «más», ordinal, «sí», chip | CLARIFY `unreadable_continuation` | `test_an_unreadable_queue_never_reactivates_an_old_cursor` |
| Tabular → Entity Profile | «más» | CLARIFY `profile_is_not_paginable` | `test_r7c_a_tabular_cursor_is_inert_after_a_profile` |
| Perfil confirmado | foco nuevo | PROFILE | `test_r5a_…` |
| Perfil confirmado | cambio/limpieza temporal | PROFILE | `test_r7b_…`, `test_a_temporal_change_over_a_profile_executes` |
| Perfil pendiente | selección válida | PROFILE | `test_r5c_…` |
| Perfil pendiente | «sí» ambiguo | CLARIFY, se repiten opciones | `test_a_bare_yes_never_chooses_the_alternative_for_the_person` |
| Perfil pendiente | cambio no relacionado | CLARIFY, cero consultas | `test_r5d_…` |
| Cualquier pendiente | cancelación explícita | se cancela; confirmado sobrevive | `test_r5d_…` |
| Perfil o pendiente | nueva consulta inequívoca | PROFILE con NEW | `test_a_new_buyer_starts_its_own_universe` |
| Actor con candidatos | número/token | DELEGATED al handler Actor | `test_a_number_with_actor_candidates_is_delegated_to_its_handler` |
| Perfil confirmado | «1» sin opciones | CLARIFY `profile_has_no_options` | `test_a_number_over_a_profile_without_options_does_not_hunt_old_suggestions` |
| Perfil | refinamiento desde UI | CLARIFY `seed_over_profile`, cero consultas | `test_a_ui_refinement_over_a_profile_runs_no_query` |
| Tabular | refinamiento desde UI con referencia vigente | DELEGATED | `test_a_seed_that_names_the_visible_result_keeps_its_route` |
| Tabular | refinamiento con referencia vieja | CLARIFY `stale_seed_reference` | `test_a_seed_that_names_an_old_result_is_refused` |
| Cualquiera | listar/ejecutar radar, lookup, sugerencias | DELEGATED, con sus permisos | `test_every_combination_yields_exactly_one_route` + suite adyacente |
| Cualquiera | reset explícito | DELEGATED; la continuidad se invalida | `test_an_explicit_reset_invalidates_the_published_continuation` |
| Perfil con fallo total | «proveedores» | PROFILE; se recompila y se declara el fallo | `test_a_total_source_failure_keeps_the_confirmed_intent` |

## 3. Puerta de utilidad

`tests/test_entity_profile_frame_b_utility.py` — 15 casos que **deben ejecutar**:
comprador reconocido, los seis focos, BOTH por defecto, periodos distintos
inequívocos, AND, OR, exclusión de modalidad, cambio temporal, selección
pendiente y CLEAR. Cada uno comprueba además `llamadas == jobs planificados` y
`jobs ≤ PROFILE_QUERY_BUDGET`. Un sistema que aclarara siempre fallaría los 15.
