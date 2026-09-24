# INVARIANT_REGISTRY_V1 — revalidación F3

**Base productiva:** `04426d19`. **Carril F2:** `59363501`. **Total:** 28 leyes (10 P0, 18 P1).
**Ejecución F3:** 28 referencias primarias actuales, 56 casos verdes; 5 leyes tienen además escenario READY con mutante rojo. La existencia de un nodeid por sí sola no se contabilizó como defensa.

Las 28 leyes siguen vigentes bajo los owners registrados. El estado actual es 27 `DEFENDED`, 1 `PARTIAL`, 0 `UNDEFENDED` y 0 `SUPERSEDED`. `DEFENDED` denota al menos una prueba conductual vigente ejecutada en este candidate; sólo las cinco filas marcadas READY tienen prueba de mutación F3. No se autoriza retirar suites antiguas con este registro.

| Ley | Owner | Estado | Defensa ejecutada | Mutación F3 |
|---|---|---|---|---|
| `INV-ACTOR-NAME-NO-GUESS` | actor | DEFENDED | `tests/test_actor_routing.py::test_inv_name_no_guess_a_named_actor_is_declared_never_resolved` | PC-AUTH-003 |
| `INV-ACTOR-ID-TYPE` | actor | DEFENDED | `tests/test_actor_identity.py::test_inv_id_type_same_number_with_different_types_are_different_actors` | pendiente |
| `INV-ACTOR-SINGLE-IDENTITY` | actor | DEFENDED | `tests/test_actor_dogfood_hardening_v2.py::test_split_brain_frame_fails_closed_before_soql` | pendiente |
| `INV-BUYER-NOT-ACTOR` | authority_arbitration | PARTIAL | `tests/test_buyer_actor_role_controls_v1.py::test_new_buyer_after_legitimate_actor` | pendiente |
| `INV-SEM-NO-SILENT-FALLBACK` | semantics | DEFENDED | `tests/test_qualified_buyer_fail_closed.py::test_missing_governorship_del_fails_closed` | pendiente |
| `INV-MONEY-POLARITY-PRESERVES-INTENT` | money | DEFENDED | `tests/test_money_polarity_v2.py::test_upper_cannot_produce_lower` | PC-MONEY-001 |
| `INV-ENTITY-RENDERED-SCOPE-EQUALS-EXECUTED` | entity_profile | DEFENDED | `tests/test_entity_profile_semantic_properties.py::test_p11_the_rendered_scope_equals_the_executed_scope` | pendiente |
| `INV-SOURCE-FAILURE-IS-NOT-ABSENCE` | source_semantics | DEFENDED | `tests/test_timeout_transparency.py::test_timeout_does_not_claim_zero_results` | PC-FAIL-001 |
| `INV-PERSIST-TESTS-NEVER-WRITE-PROJECT-DATA` | architecture | DEFENDED | `tests/test_feedback_isolation.py::test_workflow_uses_configured_feedback_path_not_real_data_file` | pendiente |
| `INV-HARNESS-PRODUCT-CONTRACT-OFFLINE` | test_architecture | DEFENDED | `tests/product_contract/test_product_harness_safety.py::test_a_swallowed_connection_attempt_still_fails_the_ledger` | pendiente |
| `INV-ACTOR-COMPLETE-MENTION` | actor | DEFENDED | `tests/test_actor_mention_completeness.py::test_a_truncated_fragment_never_takes_the_identity_of_another_actor` | pendiente |
| `INV-ACTOR-DATASET-CAPABILITY-NO-SILENT-DROP` | actor | DEFENDED | `tests/test_actor_routing.py::test_inv_no_silent_drop_the_actor_survives_a_dataset_that_cannot_serve_it` | pendiente |
| `INV-ACTOR-PROFILE-DECLARES-LIMITS` | actor_profile | DEFENDED | `tests/test_actor_profile_render.py::test_inv_scope_declared_the_profile_always_says_what_it_did_not_consult` | pendiente |
| `INV-GEO-BARE-GEOGRAPHY-PRECEDENCE` | geography | DEFENDED | `tests/test_actor_geography_precedence.py::test_bare_canonical_geography_never_becomes_actor_intent` | pendiente |
| `INV-GEO-TERRITORY-IS-NOT-BUYER` | geography | DEFENDED | `tests/test_geo_vs_entity_typed.py::test_inv7_buyer_and_territory_produce_distinct_where` | pendiente |
| `INV-NEG-NO-SILENT-DROP` | negation | DEFENDED | `tests/test_negation_workflow.py::test_first_turn_unsupported_negation_blocks_before_execution` | pendiente |
| `INV-SEM-STRUCTURED-DIMENSION-NOT-TOPIC` | semantics | DEFENDED | `tests/test_contractual_dimension_pipeline.py::test_convenio_modality_money_buyer_and_time_all_compose` | PC-MOD-001 |
| `INV-MONEY-RANGE-ATOMIC` | money | DEFENDED | `tests/test_money_range_atomicity.py::test_partial_range_blocks_instead_of_degrading` | pendiente |
| `INV-STATE-FIRMADO-IS-NOT-A-STATE` | state | DEFENDED | `tests/test_firmado_signed_semantics.py::test_firmados_en_mayo_no_aplica_estado_filtra_fecha` | pendiente |
| `INV-STATE-EXACT-DOES-NOT-WIDEN-TO-FAMILY` | state | DEFENDED | `tests/test_suspended_contract_semantics.py::test_a_suspended_contract_query_never_widens_to_cancelado` | pendiente |
| `INV-CTRL-PURE-ORDERING-PRESERVES-UNIVERSE` | control_language | DEFENDED | `tests/test_control_language.py::test_ordering_followup_preserves_the_universe` | pendiente |
| `INV-CTRL-LANGUAGE-IS-NOT-CONTENT` | control_language | DEFENDED | `tests/test_control_language.py::test_ordering_never_reaches_objeto` | pendiente |
| `INV-CONV-EXPLICIT-TURN-PARAMS-PREVAIL` | conversation | DEFENDED | `tests/test_frame_leak_canary.py::test_canary_estado_pivot` | pendiente |
| `INV-PAG-FILTER-MUTATION-INVALIDATES-PAGINATION` | pagination | DEFENDED | `tests/test_search_sh1_dogfood_regressions.py::test_r4_pagination_after_money_uses_the_filtered_universe` | pendiente |
| `INV-TIME-OPPORTUNITY-VIGENCIA-NOT-CERTIFICATION` | opportunity | DEFENDED | `tests/test_current_opportunity_retrieval.py::test_red_5_known_expired_deadline_is_excluded` | pendiente |
| `INV-DUAL-DATASET-SEPARATE-UNIVERSES` | dual_dataset | DEFENDED | `tests/test_dual_dataset_execution.py::test_build_groups_runs_both_independently` | PC-DUAL-001 |
| `INV-LOOKUP-DIRECT-ID-IS-NOT-TOPIC-SEARCH` | direct_lookup | DEFENDED | `tests/test_direct_lookup.py::TestDetection::test_detect_id_co1_pccntr` | pendiente |
| `INV-STATE-CONTRACT-UNIVERSE-CELEBRATED` | contract_universe | DEFENDED | `tests/test_contract_universe_product.py::test_seeds_before_limit` | pendiente |

## Adjudicaciones que cambian F2

- `INV-STATE-CONTRACT-UNIVERSE-CELEBRATED`: `DEFENDED`. El primario de producto `test_seeds_before_limit` verifica filas, límite y universo celebrado; R5 agrega exclusiones y continuidad de consumidores. Los 951 nodeids históricos no son el argumento de defensa.
- `INV-BUYER-NOT-ACTOR`: `PARTIAL`. `test_new_buyer_after_legitimate_actor` pasa y conserva comprador tras actor legítimo; falta un READY de Entity Profile con índice que colisiona. El antiguo `test_seeds` falla en 20 parametrizaciones porque su dialecto SQLite no registra `date_extract_y`; la traza llega a SoQL con comprador y monto correctos y falla antes de producir filas. Se registró `HD-BUYER-001`; no se declara bug de runtime con esa evidencia.
- Las otras 26 leyes conservan owner y defensa primaria de F2. Se ejecutó una referencia primaria por ley; esto confirma el camino conductual vigente, mientras que la sensibilidad por mutación queda probada sólo para las cinco leyes de READY. Ninguna ley nueva de `PROCESS_STATE_REQUEST_INTEGRITY_V1` se integra: `INV-PROCESS-STATE-NO-SILENT-LOSS` es `CANDIDATE_INVARIANT_PENDING_MAIN`.

## Brechas y límites

Las entradas `OPG-*` previas fueron reejecutadas: 15 xfails y 16 casos que pasan en los nodeids seleccionados; ninguno de los gaps se promovió. El nuevo `HD-BUYER-001` queda fuera del scope de edición F3 porque su test vive fuera de `tests/product_contract/**`. Los estados y referencias verificables viven en `tests/contracts/invariants.yaml`; el registro no crea autoridad de producto nueva.
