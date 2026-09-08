# V1.2-B — Los 35 adversariales y los ocho escenarios B

## 1. Dónde están los archivos congelados

Los dos archivos que la sección 14 del diseño nombra —
`tests/test_entity_profile_v1_1_reaudit.py` y `…_reaudit_evidence.py`— viven en
el árbol de auditoría `e024753a7a3e26a80aa0c07359078544f45f4ee5` y **no forman
parte de esta línea**: la base congelada de B (`4c86d5b`) desciende de `main`
`7868664`, no de la rama de auditoría.

Esto no es un hallazgo de B: es el estado en que A2 entregó, documentado en
[ENTITY_PROFILE_FRAME_V1_2_A_CORE.md](../ENTITY_PROFILE_FRAME_V1_2_A_CORE.md)
§4, donde A2 clasificó los 35 casos y portó sus expectativas al contrato nuevo
mediante `tests/entity_profile_frame_harness.py`. **B no debilita ni reescribe
ninguno**: conserva los ports de A2 intactos y cierra los siete que A2 dejó
como `INTEGRATION_B`.

Se comprobó con `grep` que ninguno de los 19 nombres de función del reaudit
aparece en este árbol; el port vive bajo otros nombres en los archivos de A2.

## 2. Clasificación y estado en B

| Función `test_…` del reaudit | Casos | Clasificación A2 | Estado en B |
|---|---:|---|---|
| coordination_does_not_invent_or_discard_sources | 4 | CORE | verde (port A2 sin tocar) |
| source_and_occurrence_own_their_temporal_predicate | 5 | CORE | verde |
| each_event_owns_its_period_or_clarifies | 2 | CORE | verde; además E2E en `test_two_sources_with_different_periods_are_not_globalised` |
| real_parser_or_is_not_rendered_as_and | 1 | CORE | verde; además E2E en `test_or_and_and_are_not_rendered_or_queried_alike` |
| negative_and_legacy_filters_are_honored_and_declared | 3 | CORE | verde; además E2E en `test_a_modality_exclusion_stays_in_the_sql_and_is_declared` |
| explicit_origin_still_requires_complete_answer_after_reload | 2 | **INTEGRATION_B** | **CERRADO** — `test_r6_explicit_origin_survives_persistence` |
| explicit_pending_choice_preserves_the_requested_focus | 1 | **INTEGRATION_B** | **CERRADO** — `test_r5c_a_pending_selection_crosses_turns_and_keeps_its_focus` |
| displayed_inherited_period_is_in_every_executed_query | 2 | **INTEGRATION_B** | **CERRADO** — `test_r5a_inherited_period_is_in_every_query_after_a_real_reload`, `test_r5b_explicit_completeness_survives_the_reload` |
| pending_period_does_not_leak_into_confirmed_execution | 1 | **INTEGRATION_B** | **CERRADO** — `test_r5d_a_pending_period_never_reaches_a_confirmed_execution` |
| explicit_source_switch_does_not_inherit_incompatible_event | 1 | CORE | verde; además E2E en `test_a_source_switch_does_not_inherit_the_other_event` |
| remove_period_is_not_silently_restored | 1 | CORE | verde; además E2E en `test_r7b_clear_survives_another_reload` |
| property_current_marker_does_not_erase_relative_historical_event | 1 | CORE | verde |
| contract_count_preserves_decimal_truth | 3 | CORE | verde |
| tiny_positive_invalid_count_does_not_render_zero | 1 | CORE | verde |
| temporal_value_rejects_nonfinite_source_metric | 3 | CORE | verde; además E2E en `test_a_non_finite_metric_is_never_accredited` |
| v1_unsupported_event_is_not_lost_on_compatibility_reload | 1 | CORE | verde |
| malformed_claim_span_is_discarded_without_crashing | 1 | CORE | verde |
| profile_does_not_seed_a_tabular_pagination_result | 1 | **INTEGRATION_B** | **CERRADO** — `test_r7a_…` y `test_r7c_…` |
| property_underflow_never_accredits_zero_count | 1 | CORE | verde |
| **Total** | **35** | 28 CORE / 7 INTEGRATION_B | **28 verdes + 7 cerrados** |

Los siete `INTEGRATION_B` corresponden a los ocho escenarios de
`INTEGRATION_B_REQUIRED` (R5b aporta dos casos a
`displayed_inherited_period_is_in_every_executed_query`).
`test_every_declared_b_scenario_has_a_real_test` comprueba que la lista
declarada y la de pruebas reales coinciden exactamente; ninguno queda cubierto
por una aserción de pertenencia a una tupla.

## 3. Los siete P1 del diseño (sección 13)

| ID | Estado en B | Dónde |
|---|---|---|
| R1a | aclara, cero jobs | `test_an_incompatible_subject_and_event_clarifies_without_swapping_them` |
| R1b | ejecuta, seis jobs con fechas propias, invariante al orden | `test_two_sources_with_different_periods_are_not_globalised`, `test_the_order_of_two_unambiguous_clauses_does_not_change_the_universe` |
| R2a / R2b | port CORE de A2, sin cambios | archivos de A2 |
| R3a / R3b / R3c | port CORE de A2 y R3d, sin cambios | archivos de A2/R3d |
| R4a | ANY y ALL producen SQL y alcance distintos | `test_or_and_and_are_not_rendered_or_queried_alike` |
| R4b | NOT IN conservado y exclusión declarada | `test_a_modality_exclusion_stays_in_the_sql_and_is_declared` |
| R5a–R5d | cerrados | tabla §2 |
| R6 | cerrado, con su control DEFAULT | tabla §2 |
| R7a–R7c | cerrados | tabla §2 |

## 4. Ningún oráculo debilitado

- No se añadió ningún `skip`, `xfail` o `deselect` nuevo.
- Los dos `xfail` que aparecen en la corrida adyacente pertenecen a
  `tests/test_actor_hardening_v2_1_remediation.py`, existen desde antes de esta
  entrega y su motivo lo documenta el propio archivo (preexistente en BASE
  `f9cb8c9`).
- El único cambio a un archivo de test ajeno es **ninguno**: los archivos de
  A2/R3d y `test_v12a1_independent_counterexamples.py` no se tocaron.
