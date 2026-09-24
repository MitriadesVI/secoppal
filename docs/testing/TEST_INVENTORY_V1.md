# TEST_INVENTORY_V1 — inventario automático

> Generado por `scripts/audits/test_architecture_inventory.py render`. **Heurística, no autoridad.**
> `BASELINE_HEAD = 04426d1` · 506 archivos · 16187 nodeids.
> La adjudicación humana/agente vive en `TEST_ARCHITECTURE_V1.md`; nada aquí autoriza retiros.

## Clases por nodeid

| Clase | nodeids | archivos (clase primaria) |
|---|---:|---:|
| ARCHITECTURE | 228 | 14 |
| AUDIT_HISTORICAL | 2214 | 88 |
| DOMAIN_INVARIANT | 12320 | 336 |
| PRODUCT | 1194 | 53 |
| PROPERTY | 186 | 10 |
| UNKNOWN | 45 | 5 |

Confianza: high=1435, low=2166, medium=12541, none=45

## Familias

| Familia | archivos | nodeids | workflow nodeids | tiempo hist. (s) |
|---|---:|---:|---:|---:|
| modality | 16 | 3371 | 82 | 0.0 |
| buyer_entity_resolution | 37 | 1280 | 8 | 0.0 |
| other | 46 | 1174 | 96 | 0.0 |
| actor | 37 | 1065 | 112 | 0.0 |
| query_frame_parsing | 36 | 994 | 29 | 0.0 |
| contract_universe | 26 | 951 | 505 | 0.0 |
| entity_profile | 33 | 862 | 77 | 0.0 |
| notify | 18 | 832 | 0 | 0.0 |
| truth_layer | 20 | 791 | 7 | 0.0 |
| ranking_results_presentation | 35 | 683 | 75 | 0.0 |
| entity_profile_frame | 35 | 626 | 269 | 0.0 |
| radar | 14 | 536 | 0 | 0.0 |
| geo_territory | 23 | 535 | 8 | 0.0 |
| money_value | 17 | 473 | 25 | 0.0 |
| soql_source_execution | 29 | 459 | 21 | 0.0 |
| negation_control | 12 | 429 | 55 | 0.0 |
| followup_conversation | 27 | 322 | 52 | 0.0 |
| buyer_actor_arbitration | 10 | 260 | 216 | 0.0 |
| channels_auth | 8 | 239 | 14 | 0.0 |
| time_state_opportunity | 17 | 222 | 14 | 0.0 |
| architecture_meta | 7 | 42 | 3 | 0.0 |
| product_contract | 3 | 41 | 18 | 0.0 |

## Top candidatos de duplicación (score estructural, sin adjudicar)

| score | A | B | queries compartidas | clases | workflow A/B |
|---:|---|---|---:|---|---|
| 1.0 | `test_contract_universe_independent_consumers.py` | `test_contract_universe_remediation_consumers.py` | 11 | AUDIT_HISTORICAL/AUDIT_HISTORICAL | no/no |
| 0.972 | `test_contract_universe_independent.py` | `test_contract_universe_remediation.py` | 15 | AUDIT_HISTORICAL/AUDIT_HISTORICAL | no/no |
| 0.958 | `test_contract_universe_independent_legacy.py` | `test_contract_universe_remediation_legacy.py` | 2 | AUDIT_HISTORICAL/AUDIT_HISTORICAL | no/no |
| 0.812 | `test_openwa_webhook.py` | `test_telegram_webhook.py` | 1 | DOMAIN_INVARIANT/PRODUCT | sí/sí |
| 0.625 | `test_entity_multi_regional.py` | `test_entity_multi_regional_edges.py` | 1 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.603 | `test_demo_proponente.py` | `test_demo_veedor.py` | 2 | PRODUCT/PRODUCT | sí/sí |
| 0.589 | `test_followup_delta_queries.py` | `test_followup_post_merge.py` | 11 | DOMAIN_INVARIANT/PRODUCT | sí/sí |
| 0.579 | `test_demo_alcalde.py` | `test_demo_proponente.py` | 2 | PRODUCT/PRODUCT | sí/sí |
| 0.547 | `test_truth_layer_contract_performance.py` | `test_truth_layer_contract_signature.py` | 2 | AUDIT_HISTORICAL/DOMAIN_INVARIANT | no/no |
| 0.544 | `test_demo_alcalde.py` | `test_demo_veedor.py` | 1 | PRODUCT/PRODUCT | sí/sí |
| 0.509 | `test_truth_layer_contract_liquidation.py` | `test_truth_layer_contract_performance.py` | 1 | DOMAIN_INVARIANT/AUDIT_HISTORICAL | no/no |
| 0.507 | `test_observability_traces.py` | `test_parallel_secop_execution.py` | 2 | PRODUCT/PRODUCT | sí/sí |
| 0.501 | `test_truth_layer_contract_liquidation.py` | `test_truth_layer_contract_signature.py` | 1 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.492 | `test_bidder_intent.py` | `test_bidder_intent_commercial.py` | 0 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.49 | `test_demo_proponente.py` | `test_sprint6_integration.py` | 1 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.487 | `test_plan_query_scrub_and_temporal.py` | `test_sprint2a_canonical_smoke.py` | 2 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.483 | `test_plan_query_clarification.py` | `test_plan_query_implicit_opportunity.py` | 0 | PRODUCT/PRODUCT | sí/sí |
| 0.482 | `test_demo_alcalde.py` | `test_sprint6_integration.py` | 1 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.479 | `test_contract_universe_remediation_legacy.py` | `test_cu_v11_audit_persistence.py` | 1 | AUDIT_HISTORICAL/AUDIT_HISTORICAL | no/no |
| 0.476 | `test_demo_veedor.py` | `test_sprint6_integration.py` | 0 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.474 | `test_truth_layer_contract_changes.py` | `test_truth_layer_contract_liquidation.py` | 1 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.467 | `test_plan_query_city_extraction.py` | `test_sprint2a_canonical_smoke.py` | 2 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.466 | `test_e2e_followup_context.py` | `test_followup_post_merge.py` | 3 | PRODUCT/PRODUCT | sí/sí |
| 0.464 | `test_beta_entity_actor_conversation.py` | `test_contract_universe_r5_profile_compatibility.py` | 11 | PRODUCT/PRODUCT | sí/no |
| 0.464 | `test_contract_universe_independent_legacy.py` | `test_cu_v11_audit_persistence.py` | 1 | AUDIT_HISTORICAL/AUDIT_HISTORICAL | no/no |
| 0.459 | `test_plan_query_clarification.py` | `test_plan_query_scrub_and_temporal.py` | 0 | PRODUCT/PRODUCT | sí/sí |
| 0.457 | `test_public_university_completion_r2fb.py` | `test_public_university_pilot_r2fa.py` | 4 | AUDIT_HISTORICAL/AUDIT_HISTORICAL | no/no |
| 0.454 | `test_observer.py` | `test_observer_partial.py` | 1 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.454 | `test_plan_query_city_extraction.py` | `test_plan_query_clarification.py` | 1 | PRODUCT/PRODUCT | sí/sí |
| 0.453 | `test_timeout_handling.py` | `test_timeout_transparency.py` | 2 | PRODUCT/PRODUCT | sí/sí |
| 0.45 | `test_e2e_followup_context.py` | `test_followup_delta_queries.py` | 4 | PRODUCT/DOMAIN_INVARIANT | sí/sí |
| 0.449 | `test_truth_layer_contract_changes.py` | `test_truth_layer_contract_performance.py` | 1 | DOMAIN_INVARIANT/AUDIT_HISTORICAL | no/no |
| 0.446 | `test_truth_layer_contract_changes.py` | `test_truth_layer_contract_signature.py` | 1 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.444 | `test_entity_profile_frame_coordinator_clock.py` | `test_entity_profile_frame_integration_contract.py` | 1 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.443 | `test_plan_query_city_extraction.py` | `test_plan_query_implicit_opportunity.py` | 0 | PRODUCT/PRODUCT | sí/sí |
| 0.441 | `test_plan_query_implicit_opportunity.py` | `test_sprint2a_canonical_smoke.py` | 0 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.438 | `test_plan_query_implicit_opportunity.py` | `test_plan_query_scrub_and_temporal.py` | 0 | PRODUCT/PRODUCT | sí/sí |
| 0.433 | `test_entity_trailing_token_robustness.py` | `test_firmado_signed_semantics.py` | 0 | DOMAIN_INVARIANT/DOMAIN_INVARIANT | no/no |
| 0.417 | `test_plan_query_clarification.py` | `test_sprint2a_canonical_smoke.py` | 0 | PRODUCT/AUDIT_HISTORICAL | sí/sí |
| 0.414 | `test_followup_post_merge.py` | `test_query_frame.py` | 5 | PRODUCT/DOMAIN_INVARIANT | sí/sí |

## Consultas literales repetidas en ≥4 archivos

| archivos | consulta | familias |
|---:|---|---|
| 51 | `en ejecución` | actor, architecture_meta, buyer_actor_arbitration, contract_universe, followup_conversation, modality, money_value, negation_control, query_frame_parsing, radar, ranking_results_presentation, soql_source_execution, time_state_opportunity, truth_layer |
| 23 | `analiza contratos de` | entity_profile, entity_profile_frame, other |
| 23 | `contratación directa` | actor, buyer_actor_arbitration, contract_universe, entity_profile, followup_conversation, modality, other, query_frame_parsing, radar |
| 22 | `licitación pública` | entity_profile, followup_conversation, modality, notify, radar, soql_source_execution, truth_layer |
| 17 | `alcaldia municipal de baranoa` | buyer_actor_arbitration, entity_profile, entity_profile_frame, geo_territory |
| 17 | `contratos de` | actor, buyer_actor_arbitration, buyer_entity_resolution, entity_profile, entity_profile_frame, money_value, other |
| 16 | `alcaldia de barranquilla` | actor, buyer_actor_arbitration, followup_conversation, geo_territory, negation_control, query_frame_parsing, radar, ranking_results_presentation, soql_source_execution |
| 15 | `alcaldia de baranoa` | buyer_actor_arbitration, entity_profile, entity_profile_frame, geo_territory, negation_control, other |
| 15 | `licitacion publica` | followup_conversation, modality, other, ranking_results_presentation, soql_source_execution |
| 15 | `muestrame mas` | buyer_entity_resolution, entity_profile_frame, followup_conversation, modality, negation_control, other, query_frame_parsing, ranking_results_presentation |
| 15 | `valle del cauca` | actor, buyer_entity_resolution, entity_profile, geo_territory, modality, negation_control, query_frame_parsing |
| 14 | `cédula de ciudadanía` | actor, entity_profile |
| 14 | `en aprobación` | contract_universe, soql_source_execution, time_state_opportunity, truth_layer |
| 13 | `no definido` | actor, contract_universe, entity_profile, money_value, soql_source_execution, truth_layer |
| 12 | `analiza contratos firmados de` | entity_profile, entity_profile_frame |
| 12 | `distrito especial industrial y portuario de barranquilla` | actor, buyer_actor_arbitration, buyer_entity_resolution, followup_conversation, money_value, ranking_results_presentation, soql_source_execution |
| 12 | `group by modalidad_de_contratacion` | actor, entity_profile, other |
| 12 | `mantenimiento vial` | architecture_meta, channels_auth, followup_conversation, query_frame_parsing, radar, ranking_results_presentation, soql_source_execution |
| 12 | `muéstrame más` | buyer_actor_arbitration, contract_universe, entity_profile_frame, followup_conversation, modality, product_contract |
| 11 | `contratos de mantenimiento` | actor, contract_universe, followup_conversation, query_frame_parsing, ranking_results_presentation, soql_source_execution, time_state_opportunity |
| 11 | `manual de contratacion` | query_frame_parsing, radar, ranking_results_presentation, soql_source_execution |
| 10 | `contratos del proveedor` | actor, buyer_actor_arbitration, other |
| 10 | `distrito capital de bogotá` | actor, architecture_meta, buyer_actor_arbitration, buyer_entity_resolution, geo_territory |
| 9 | `analiza procesos de` | entity_profile, entity_profile_frame |
| 9 | `contratos del proveedor fundacaribe` | actor, buyer_actor_arbitration, other |
| 9 | `read timed out` | actor, negation_control, soql_source_execution, time_state_opportunity |
| 9 | `selección abreviada` | entity_profile, modality, notify, radar |
| 8 | `contratos de obra en baranoa` | entity_profile_frame, truth_layer |
| 8 | `enviado proveedor` | contract_universe, soql_source_execution, truth_layer |
| 8 | `mis radares` | entity_profile_frame, other, radar |

## Por archivo

| archivo | familia | clase primaria | mezcla | nodeids | param | hyp fn | wf | skip/xfail | t hist (s) | evidencia |
|---|---|---|---|---:|---:|---:|---:|---|---:|---|
| `test_actor_alias_index.py` | actor | DOMAIN_INVARIANT | DOMA=12 | 12 | 0 | 0 | 0 | 0/0 | 0.0 | AP3-B/B1 — índice local reproducible y política de fuente estable. |
| `test_actor_alias_source.py` | actor | DOMAIN_INVARIANT | DOMA=14 | 14 | 4 | 0 | 0 | 0/0 | 0.0 | La adquisición AP3-B es acotada, por lotes y agregada en servidor. |
| `test_actor_candidate_ux.py` | actor | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | AP3-G visible identities and safe Streamlit candidate controls. |
| `test_actor_contracted_action_ownership.py` | actor | DOMAIN_INVARIANT | DOMA=21 | 21 | 20 | 0 | 0 | 0/0 | 0.0 | INV-ACTOR-MENTION-ATOMICITY under the contracted-action intent. |
| `test_actor_conversation.py` | actor | DOMAIN_INVARIANT | DOMA=45, PROD=29 | 74 | 52 | 0 | 29 | 0/0 | 0.0 | AP3-F systemic actor-conversation operations and invariants. |
| `test_actor_conversation_corpus.py` | actor | DOMAIN_INVARIANT | DOMA=5, PROD=1 | 6 | 0 | 0 | 1 | 0/0 | 0.0 | Executable contract for the AP3-F natural-language actor corpus. |
| `test_actor_conversation_dogfood.py` | actor | AUDIT_HISTORICAL | AUDI=8 | 8 | 4 | 0 | 2 | 0/0 | 0.0 | AP3-F frozen dogfood transcripts for natural actor conversation. |
| `test_actor_document_negation_boundary.py` | actor | DOMAIN_INVARIANT | DOMA=24, PROD=9, PROP=2 | 35 | 27 | 2 | 9 | 0/0 | 0.0 | Actor documents are structural boundaries, never negation identity evidence. |
| `test_actor_dogfood_hardening_v2.py` | actor | AUDIT_HISTORICAL | AUDI=37 | 37 | 17 | 0 | 14 | 0/0 | 0.0 | Dogfood hardening V2 — four correctness defects found in real Actor Profile use. |
| `test_actor_dogfood_hardening_v2_properties.py` | actor | AUDIT_HISTORICAL | AUDI=11 | 11 | 0 | 11 | 0 | 0/0 | 0.0 | Combinatorial invariants behind the dogfood hardening fixes. |
| `test_actor_exact_prefix.py` | actor | AUDIT_HISTORICAL | AUDI=35 | 35 | 25 | 0 | 0 | 0/0 | 0.0 | Exact structural CC ambiguity: synthetic, offline, red-first authority gates. |
| `test_actor_exact_prefix_properties.py` | actor | PROPERTY | PROP=8 | 8 | 0 | 8 | 0 | 0/0 | 0.0 | Real combinatorial authority invariants with deterministic offline examples. |
| `test_actor_exact_prefix_workflow.py` | actor | DOMAIN_INVARIANT | DOMA=12, PROD=11 | 23 | 16 | 0 | 11 | 0/0 | 0.0 | Fail-closed structural ambiguity through workflow, UI, and explicit selection. |
| `test_actor_flag_off.py` | actor | DOMAIN_INVARIANT | DOMA=16 | 16 | 7 | 0 | 0 | 0/0 | 0.0 | INV-FLAG — con ACTOR_PROFILE apagado, el sistema es el de antes de AP2. |
| `test_actor_geography_precedence.py` | actor | DOMAIN_INVARIANT | DOMA=17 | 17 | 16 | 0 | 0 | 0/0 | 0.0 | INV-BARE-GEOGRAPHY-PRECEDENCE — AP3-E.1 §8. |
| `test_actor_hardening_v2_1_properties.py` | actor | PROPERTY | PROP=10 | 10 | 0 | 10 | 0 | 0/0 | 0.0 | Invariants behind the V2.1 remediation. |
| `test_actor_hardening_v2_1_remediation.py` | actor | AUDIT_HISTORICAL | AUDI=46 | 46 | 30 | 0 | 15 | 0/2 | 0.0 | Counterexamples from the independent review of Actor Dogfood Hardening V2. |
| `test_actor_identity.py` | actor | DOMAIN_INVARIANT | ARCH=1, DOMA=129 | 130 | 89 | 0 | 0 | 0/0 | 0.0 | AP1 — identidad tipada de actor: invariantes y casos reales. |
| `test_actor_identity_sentinels.py` | actor | DOMAIN_INVARIANT | DOMA=18, PROP=1 | 19 | 15 | 1 | 0 | 0/0 | 0.0 |  |
| `test_actor_index_isolation.py` | actor | ARCHITECTURE | ARCH=6 | 6 | 0 | 0 | 0 | 1/0 | 0.0 | §6 — la suite no puede depender del directorio local de la máquina. |
| `test_actor_mention_completeness.py` | actor | DOMAIN_INVARIANT | DOMA=21 | 21 | 15 | 0 | 0 | 0/0 | 0.0 | INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION — AP3-E.1. |
| `test_actor_name_properties.py` | actor | PROPERTY | DOMA=1, PROP=5 | 6 | 0 | 5 | 0 | 0/0 | 0.0 | Propiedades de `normalize_actor_name`, verificadas con Hypothesis. |
| `test_actor_name_resolver.py` | actor | DOMAIN_INVARIANT | ARCH=1, DOMA=25 | 26 | 10 | 0 | 0 | 0/0 | 0.0 | AP3-B/B2 — resolver local precision-first y sus mutantes adversariales. |
| `test_actor_negation_composition.py` | actor | DOMAIN_INVARIANT | DOMA=42, PROD=1, PROP=1 | 44 | 24 | 1 | 1 | 0/7 | 0.0 | Un marcador negativo gobierna una COLECCIÓN de actores, no uno solo. |
| `test_actor_ownership_supplier_contract.py` | actor | DOMAIN_INVARIANT | DOMA=5 | 5 | 5 | 0 | 0 | 0/0 | 0.0 | Ownership de turno: un actor positivo acreditado no es asunto de A2. |
| `test_actor_pending_document_selection.py` | actor | DOMAIN_INVARIANT | DOMA=14, PROD=10, PROP=3 | 27 | 10 | 3 | 10 | 0/0 | 0.0 | An explicit document answering a pending actor list selects, never restarts. |
| `test_actor_pending_system_metadata.py` | actor | DOMAIN_INVARIANT | DOMA=19, PROP=15 | 34 | 32 | 2 | 0 | 0/0 | 0.0 | INV-SYSTEM-METADATA-DOES-NOT-CHANGE-SPEECH-ACT. |
| `test_actor_prefix_adjudicator.py` | actor | AUDIT_HISTORICAL | AUDI=17 | 17 | 3 | 0 | 0 | 0/0 | 0.0 | Tests for the ADR-094 performance re-adjudication rule. |
| `test_actor_profile.py` | actor | DOMAIN_INVARIANT | DOMA=28 | 28 | 5 | 0 | 0 | 0/0 | 0.0 | AP2-C — la huella contractual observada: métricas, parcialidad y límites. |
| `test_actor_profile_render.py` | actor | DOMAIN_INVARIANT | DOMA=23, PROD=8 | 31 | 4 | 0 | 8 | 0/0 | 0.0 | AP2-D — el perfil que ve el usuario, y lo que se niega a decirle. |
| `test_actor_profile_scope.py` | actor | DOMAIN_INVARIANT | DOMA=73, PROP=2 | 75 | 66 | 2 | 0 | 0/0 | 0.0 | Applied scope is presentation only; SQL oracle frozen at f6fa849. |
| `test_actor_profile_scope_conversation.py` | actor | PRODUCT | PROD=11 | 11 | 6 | 0 | 11 | 0/0 | 0.0 | Dogfood shapes with synthetic identities and deterministic source EMPTY. |
| `test_actor_resolution_benchmark.py` | actor | AUDIT_HISTORICAL | AUDI=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | AP3-A — benchmark no-runtime de nombre de actor. |
| `test_actor_resolution_orchestration.py` | actor | DOMAIN_INVARIANT | DOMA=81, PROD=1 | 82 | 61 | 0 | 1 | 0/0 | 0.0 | AP3-C — two-door arbitration, authority and contextual selection. |
| `test_actor_routing.py` | actor | DOMAIN_INVARIANT | DOMA=74 | 74 | 53 | 0 | 0 | 0/0 | 0.0 | AP2-B — el routing de un actor contractual, y sus invariantes. |
| `test_actor_source_contract.py` | actor | DOMAIN_INVARIANT | DOMA=23 | 23 | 7 | 0 | 0 | 0/0 | 0.0 | AP2-A — el contrato de la fuente contractual, congelado y atado al código. |
| `test_actor_typed_projection.py` | actor | DOMAIN_INVARIANT | DOMA=21 | 21 | 9 | 0 | 0 | 0/0 | 0.0 | INV-TYPED-PROJECTION — la llave de identidad llega entera al WHERE. |
| `test_audit_runner_isolation.py` | architecture_meta | AUDIT_HISTORICAL | AUDI=11 | 11 | 4 | 0 | 0 | 0/0 | 0.0 | INV-AUDIT-RUNNER-NO-PRODUCTION-WRITES. |
| `test_invariants_characterization.py` | architecture_meta | AUDIT_HISTORICAL | AUDI=11 | 11 | 8 | 0 | 0 | 0/0 | 0.0 | Characterization tests for QP-INV before consolidating QueryPlan invariants. |
| `test_invariants_lock.py` | architecture_meta | ARCHITECTURE | ARCH=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 | Lock-in tests for QP-INV invariant consolidation. |
| `test_llm_integration_coverage.py` | architecture_meta | DOMAIN_INVARIANT | DOMA=3, PROD=3 | 6 | 0 | 0 | 3 | 0/0 | 0.0 | Tests de cobertura H7 para el camino LLM integrado. |
| `test_module_size_limits.py` | architecture_meta | ARCHITECTURE | ARCH=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 | G8 discipline — LoC caps for architectural modules. |
| `test_stress_corpus_structure.py` | architecture_meta | ARCHITECTURE | ARCH=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Structural validation of the broad-query stress corpus. |
| `test_workflow_architecture.py` | architecture_meta | ARCHITECTURE | ARCH=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | Architectural lock-in for the workflow refactor. |
| `test_beta_conversation_properties.py` | buyer_actor_arbitration | DOMAIN_INVARIANT | DOMA=7, PROP=3 | 10 | 7 | 3 | 0 | 0/0 | 0.0 | Metamorphic conversation and admission invariants, deterministic and offline. |
| `test_beta_entity_actor_conversation.py` | buyer_actor_arbitration | PRODUCT | PROD=18 | 18 | 12 | 0 | 18 | 0/0 | 0.0 | Dogfood regressions through the real parser, Burr graph and persisted turns. |
| `test_buyer_actor_harness_compatibility.py` | buyer_actor_arbitration | DOMAIN_INVARIANT | DOMA=17, PROP=1 | 18 | 16 | 1 | 0 | 0/0 | 0.0 | R2B: field-owned oracle, dialect fidelity, and unchanged synthetic controls. |
| `test_buyer_actor_independent_boundaries.py` | buyer_actor_arbitration | AUDIT_HISTORICAL | AUDI=31 | 31 | 30 | 1 | 30 | 0/0 | 0.0 | Independent nearby grammar, provenance, negation and property checks. |
| `test_buyer_actor_independent_local.py` | buyer_actor_arbitration | AUDIT_HISTORICAL | AUDI=4 | 4 | 4 | 0 | 4 | 0/0+rt | 0.0 | Opt-in local index: only two authorized mentions, no identity export. |
| `test_buyer_actor_independent_product.py` | buyer_actor_arbitration | AUDIT_HISTORICAL | AUDI=64 | 64 | 64 | 0 | 64 | 0/0 | 0.0 | Positive product expectations independent of evaluated parser output. |
| `test_buyer_actor_role_arbitration_v1.py` | buyer_actor_arbitration | PRODUCT | PROD=20 | 20 | 20 | 0 | 20 | 0/0 | 0.0 | Buyer role regression with real parser, directory, workflow and SQL compiler. |
| `test_buyer_actor_role_controls_v1.py` | buyer_actor_arbitration | PRODUCT | DOMA=4, PROD=76 | 80 | 80 | 0 | 76 | 0/0 | 0.0 | Mention/role contrasts and persisted continuity for Buyer-Actor V1. |
| `test_buyer_actor_role_local_v1.py` | buyer_actor_arbitration | PRODUCT | PROD=4 | 4 | 4 | 0 | 4 | 0/0+rt | 0.0 | Opt-in read-only verification of the two reported local mentions only. |
| `test_buyer_actor_role_properties_v1.py` | buyer_actor_arbitration | DOMAIN_INVARIANT | DOMA=10, PROP=1 | 11 | 10 | 1 | 0 | 0/0 | 0.0 | Deterministic role invariance under changes to synthetic alias data. |
| `test_accredited_name_rewrite_spans.py` | buyer_entity_resolution | DOMAIN_INVARIANT | ARCH=2, DOMA=3 | 5 | 5 | 0 | 0 | 0/0 | 0.0 | Cross-interaction: a new shorter buyer cannot steal an accredited longer name. |
| `test_buyer_candidate_execution_limit.py` | buyer_entity_resolution | PRODUCT | DOMA=2, PROD=4 | 6 | 2 | 0 | 4 | 0/0 | 0.0 | Canarios del cierre temporal para grupos de compradores grandes. |
| `test_buyer_execution_truth.py` | buyer_entity_resolution | PRODUCT | PROD=1 | 1 | 0 | 0 | 1 | 0/0 | 0.0 | El contexto de respuesta sólo describe los filtros realmente ejecutados. |
| `test_buyer_group_analytics_truth.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Aggregate rows are physical counts, and the trace is the executed aggregate. |
| `test_buyer_group_capacity_evidence.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=9 | 9 | 8 | 0 | 0 | 0/0 | 0.0 | The execution GO is bounded by actual complete, ordered source probes. |
| `test_buyer_intent_sdis.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=4 | 4 | 2 | 0 | 0 | 0/0 | 0.0 | La mención acreditada de SDIS conserva su intención antes del objeto. |
| `test_buyer_rewrite_fuzzy_confidence.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=5 | 5 | 4 | 0 | 0 | 0/0 | 0.0 | Global fuzzy rewrite parity is separate from A2 buyer admission. |
| `test_buyer_territory_tail.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=14 | 14 | 9 | 0 | 0 | 0/0 | 0.0 | La cola del territorio de un comprador no es un filtro territorial. |
| `test_buyer_unit_integrity_p02.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=37 | 37 | 35 | 0 | 0 | 0/0 | 0.0 | Canarios congelados de BUYER_UNIT_INTEGRITY (Etapa 1, ADR-074). |
| `test_car_core_r2d.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=250 | 250 | 247 | 0 | 0 | 0/0 | 0.0 | R2D — núcleo oficial de 33 CAR/CDS con identidad SECOP II verificada. |
| `test_compound_buyer_capture_r2a1.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=68 | 68 | 44 | 0 | 0 | 0/0 | 0.0 | R2A.1 — un nombre institucional no se corta por su propia juntura. |
| `test_compound_identity_guard.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=32 | 32 | 28 | 0 | 0 | 0/0 | 0.0 | M1A.2 predictions frozen before the fix, against both complete catalogs. |
| `test_control_national_r2e.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=72 | 72 | 54 | 0 | 0 | 0/0 | 0.0 | R2E — cobertura segura del universo de control nacional. |
| `test_d6_department_suffix_r2d1.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=41 | 41 | 32 | 0 | 0 | 0/0 | 0.0 | D6-R2D1 — geografía vs institución que termina en geografía. |
| `test_declared_buyer_groups.py` | buyer_entity_resolution | DOMAIN_INVARIANT | ARCH=1, DOMA=17, PROD=3 | 21 | 15 | 0 | 3 | 0/0 | 0.0 | Declared groups preserve membership, explicit units and physical truth. |
| `test_dogfood_buyer_coverage_adr073.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=15 | 15 | 12 | 0 | 0 | 0/0 | 0.0 | Los cuatro bindings del dogfood valen lo que vale su artefacto (ADR-073). |
| `test_entity_candidates_no_silent_collapse.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=12 | 12 | 12 | 0 | 0 | 0/0 | 0.0 | Un grupo de compradores nunca se ejecuta como uno solo, en silencio. |
| `test_entity_multi_regional.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Tests ENTITY-MULTI-REGIONAL-001 — multi-regional entity resolution. |
| `test_entity_multi_regional_edges.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Test ENTITY-MULTI-REGIONAL idempotency and edge cases. |
| `test_entity_resolver.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=23 | 23 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_entity_resolver_fallback.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_entity_trailing_token_robustness.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=10 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | ENTITY-TRAILING-001 — resolución de entidad robusta a tokens trailing. |
| `test_fuzzy_identity_multiword_r2e1a.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=45 | 45 | 35 | 0 | 0 | 0/0 | 0.0 | R2E.1A — el territorio pedido se compara como frase, no como tokens sueltos. |
| `test_generic_buyer_punctuation.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=14 | 14 | 14 | 0 | 0 | 0/0 | 0.0 | El ruido físico de SECOP no convierte una clase en BuyerUnit. |
| `test_governorate_core_r2a.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=39 | 39 | 33 | 0 | 0 | 0/0 | 0.0 | R2A — cobertura de compradores raíz departamentales verificada en registro. |
| `test_icbf_regionals.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=47 | 47 | 41 | 0 | 0 | 0/0 | 0.0 | Tests for ICBF Regional Ecosystem Resolver. |
| `test_institutional_family_compatibility.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=23 | 23 | 16 | 0 | 0 | 0/0 | 0.0 | Institutional-family compatibility for strict buyer rewrites. |
| `test_institutional_fuzzy_safety_r2e1.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=93 | 93 | 81 | 0 | 0 | 0/0 | 0.0 | R2E.1 — seguridad del fuzzy institucional. |
| `test_national_core_r2b.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=66 | 66 | 64 | 0 | 0 | 0/0 | 0.0 | R2B — cobertura nacional verificada de ministerios y departamentos administrativ |
| `test_public_university_completion_r2fb.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=52 | 52 | 43 | 0 | 0 | 0/0 | 0.0 | R2F-B — cierre de las ocho universidades publicas que R2F-A dejo cerradas. |
| `test_public_university_pilot_r2fa.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=21 | 21 | 11 | 0 | 0 | 0/0 | 0.0 | R2F-A — piloto supervisado de ocho universidades publicas. |
| `test_qualified_buyer_fail_closed.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=20 | 20 | 15 | 0 | 0 | 0/0 | 0.0 | R1A — un comprador nombrado explícitamente falla CERRADO si no existe. |
| `test_root_buyer_evidence_r1d.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=19 | 19 | 16 | 0 | 0 | 0/0 | 0.0 | R1D — una peticion departamental raiz exige evidencia de ownership raiz. |
| `test_scan_alias_eligibility_r2d3.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=57 | 57 | 48 | 0 | 0 | 0/0 | 0.0 | R2D.3 — exact alias ownership is not text-scan authority. |
| `test_secop1_entity_aliases.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=17 | 17 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para el crosswalk de entidades SECOP I — C1A + C1A.1 hardening. |
| `test_sena_resolver.py` | buyer_entity_resolution | DOMAIN_INVARIANT | DOMA=41 | 41 | 32 | 0 | 0 | 0/0 | 0.0 | Tests for SENA Entity Ecosystem Resolver. |
| `test_source_name_projection_v1.py` | buyer_entity_resolution | AUDIT_HISTORICAL | AUDI=85 | 85 | 60 | 0 | 0 | 0/0 | 0.0 | SOURCE_NAME_PROJECTION_V1 — ADR-073. |
| `test_adr073_fiscal_authority_r1.py` | channels_auth | AUDIT_HISTORICAL | AUDI=14, PROP=2 | 16 | 8 | 2 | 0 | 0/0 | 0.0 | Regression for Schema 2 fiscal authority preserved by Schema 3. |
| `test_auth.py` | channels_auth | DOMAIN_INVARIANT | DOMA=26 | 26 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_contract_activity_unit_authority_v2.py` | channels_auth | DOMAIN_INVARIANT | DOMA=6, PROP=1 | 7 | 0 | 1 | 0 | 0/0 | 0.0 |  |
| `test_openwa_webhook.py` | channels_auth | DOMAIN_INVARIANT | DOMA=8, PROD=7 | 15 | 0 | 0 | 7 | 0/0 | 0.0 | Pruebas del adaptador OpenWA (webhook de WhatsApp Web). |
| `test_supplier_request_authority.py` | channels_auth | DOMAIN_INVARIANT | ARCH=1, DOMA=120, PROP=6 | 127 | 108 | 6 | 0 | 0/0 | 0.0 | Structural and adversarial contract for Supplier Request Authority V1. |
| `test_telegram_pilot.py` | channels_auth | AUDIT_HISTORICAL | AUDI=28 | 28 | 0 | 0 | 0 | 0/0 | 0.0 | Pruebas sin red de la herramienta local del piloto Telegram. |
| `test_telegram_webhook.py` | channels_auth | PRODUCT | DOMA=2, PROD=7 | 9 | 0 | 0 | 7 | 0/0 | 0.0 | Pruebas del webhook de Telegram tras robustecer: chunking a 4096, |
| `test_web_session.py` | channels_auth | DOMAIN_INVARIANT | DOMA=11 | 11 | 0 | 0 | 0 | 0/0 | 0.0 | Frontera de sesión web: identidad de conversación y vaciado de estado. |
| `test_contract_invariants.py` | contract_universe | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | Unit tests for pure SECOP contract invariants. |
| `test_contract_universe_consumers.py` | contract_universe | PRODUCT | ARCH=1, DOMA=6, PROD=8 | 15 | 0 | 0 | 8 | 0/0 | 0.0 | Execute actual consumer builders against the common synthetic database. |
| `test_contract_universe_f2_1_completeness.py` | contract_universe | PRODUCT | DOMA=1, PROD=247 | 248 | 246 | 0 | 247 | 0/0 | 0.0 | F2.1 — representable=True obliga a conservar TODAS las condiciones. |
| `test_contract_universe_f2_1_symmetry.py` | contract_universe | PRODUCT | DOMA=18, PROD=26, PROP=2 | 46 | 44 | 2 | 26 | 0/0 | 0.0 | F2.1 — la respuesta no puede depender del orden de las condiciones. |
| `test_contract_universe_f2_2_materialization.py` | contract_universe | PRODUCT | DOMA=3, PROD=29, PROP=2 | 34 | 30 | 2 | 31 | 0/0 | 0.0 | F2.2: every composed condition needs an effective producer and actual SQL. |
| `test_contract_universe_independent.py` | contract_universe | AUDIT_HISTORICAL | AUDI=21 | 21 | 16 | 1 | 15 | 0/0 | 0.0 | Policy oracles are authored independently; failures remain red for review. |
| `test_contract_universe_independent_consumers.py` | contract_universe | AUDIT_HISTORICAL | AUDI=11 | 11 | 10 | 0 | 8 | 0/0 | 0.0 | Additional consumer boundary contrasts including full dual response. |
| `test_contract_universe_independent_legacy.py` | contract_universe | AUDIT_HISTORICAL | AUDI=4 | 4 | 0 | 0 | 1 | 0/0 | 0.0 | Read artifacts emitted by the real frozen BASE serializers, never strip keys. |
| `test_contract_universe_independent_safety.py` | contract_universe | AUDIT_HISTORICAL | AUDI=5 | 5 | 4 | 0 | 0 | 0/0 | 0.0 | A swallowed external attempt is a failing safety gate, even outside workflow. |
| `test_contract_universe_product.py` | contract_universe | PRODUCT | PROD=16 | 16 | 10 | 0 | 16 | 0/0 | 0.0 | Acceptance observes SQL results and published response, not only params. |
| `test_contract_universe_properties.py` | contract_universe | PROPERTY | PROP=3 | 3 | 0 | 3 | 0 | 0/0 | 0.0 | SQL and pure eligibility parity; excluded insertions cannot perturb metrics. |
| `test_contract_universe_r3_sql_oracles.py` | contract_universe | DOMAIN_INVARIANT | DOMA=5, PROP=3 | 8 | 3 | 3 | 0 | 0/0 | 0.0 | Counterexamples and generated invariants for the adjudicated R3 oracles. |
| `test_contract_universe_r4_state_metadata.py` | contract_universe | DOMAIN_INVARIANT | DOMA=13, PROD=2, PROP=1 | 16 | 12 | 1 | 2 | 0/0 | 0.0 | R4: universe, explicit state and provenance have separate authorities. |
| `test_contract_universe_r5_profile_compatibility.py` | contract_universe | PRODUCT | PROD=14 | 14 | 6 | 0 | 14 | 0/0 | 0.0 | Counter-proofs and persistence compatibility for Entity Profile under CU V1.1. |
| `test_contract_universe_remediation.py` | contract_universe | AUDIT_HISTORICAL | AUDI=21 | 21 | 16 | 1 | 15 | 0/0 | 0.0 | Owned remediation ports of frozen independent scenarios. |
| `test_contract_universe_remediation_consumers.py` | contract_universe | AUDIT_HISTORICAL | AUDI=11 | 11 | 10 | 0 | 8 | 0/0 | 0.0 | Additional consumer boundary contrasts including full dual response. |
| `test_contract_universe_remediation_legacy.py` | contract_universe | AUDIT_HISTORICAL | AUDI=6 | 6 | 0 | 0 | 2 | 0/0 | 0.0 | Read artifacts emitted by the real frozen BASE serializers, never strip keys. |
| `test_contract_universe_remediation_properties.py` | contract_universe | AUDIT_HISTORICAL | AUDI=47 | 47 | 41 | 2 | 32 | 0/0 | 0.0 | V1.1 adjudication and bounded occurrence invariants; no external I/O. |
| `test_cu_f21_final_delta.py` | contract_universe | AUDIT_HISTORICAL | AUDI=19 | 19 | 15 | 1 | 16 | 0/0 | 0.0 | Independent F2.1 final delta: executed conditions, residuals and catalog parity. |
| `test_cu_v11_audit_aprobado.py` | contract_universe | AUDIT_HISTORICAL | AUDI=302 | 302 | 297 | 0 | 0 | 0/0 | 0.0 | Independent Aprobado V1.1 matrix: Python eligible() vs the SQL predicate. |
| `test_cu_v11_audit_f1_f5.py` | contract_universe | AUDIT_HISTORICAL | AUDI=48 | 48 | 44 | 0 | 37 | 0/0 | 0.0 | Independent F1-F5 oracles for Contract Universe V1.1. |
| `test_cu_v11_audit_findings.py` | contract_universe | AUDIT_HISTORICAL | AUDI=8 | 8 | 8 | 0 | 6 | 0/0 | 0.0 | Reproduced non-compliances of Contract Universe V1.1. These stay RED. |
| `test_cu_v11_audit_persistence.py` | contract_universe | AUDIT_HISTORICAL | AUDI=11 | 11 | 0 | 0 | 1 | 0/0 | 0.0 | Independent persistence/version checks against a REAL V1 artifact. |
| `test_cu_v11_audit_ports.py` | contract_universe | AUDIT_HISTORICAL | AUDI=11 | 11 | 8 | 0 | 8 | 0/0 | 0.0 | Independent ports of the ten frozen reds. Oracles authored here, not copied |
| `test_cu_v11_audit_properties.py` | contract_universe | AUDIT_HISTORICAL | AUDI=5 | 5 | 0 | 5 | 0 | 0/0 | 0.0 | Hypothesis properties authored for this review. |
| `test_cu_v11_audit_regressions.py` | contract_universe | AUDIT_HISTORICAL | AUDI=13 | 13 | 10 | 0 | 12 | 0/0 | 0.0 | Minimum regression surface for Contract Universe V1.1, independently authored. |
| `test_entity_ecosystem.py` | entity_profile | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | Tests ENTITY-ECOSYSTEM-001 — entity ecosystem expansion. |
| `test_entity_ecosystem_query_bridge.py` | entity_profile | DOMAIN_INVARIANT | DOMA=20 | 20 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for Entity Ecosystem integration into the query pipeline. |
| `test_entity_ecosystem_router.py` | entity_profile | DOMAIN_INVARIANT | DOMA=55 | 55 | 29 | 0 | 0 | 0/0 | 0.0 | Tests for Entity Ecosystem Router. |
| `test_entity_profile_action_admission.py` | entity_profile | PRODUCT | DOMA=1, PROD=22 | 23 | 12 | 0 | 22 | 0/0 | 0.0 |  |
| `test_entity_profile_action_properties.py` | entity_profile | PROPERTY | PROP=2 | 2 | 0 | 2 | 1 | 0/0 | 0.0 |  |
| `test_entity_profile_action_ui_boundary.py` | entity_profile | ARCHITECTURE | ARCH=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_entity_profile_conversation_scope.py` | entity_profile | DOMAIN_INVARIANT | DOMA=20 | 20 | 6 | 0 | 0 | 0/0 | 0.0 | La conversación profundiza en el MISMO análisis, y el texto dice lo ejecutado. |
| `test_entity_profile_conversation_transaction.py` | entity_profile | PRODUCT | DOMA=1, PROD=8 | 9 | 1 | 0 | 8 | 0/0 | 0.0 | La conversación del perfil, con disco real y un workflow nuevo por turno. |
| `test_entity_profile_evidence.py` | entity_profile | DOMAIN_INVARIANT | DOMA=14, PROP=2 | 16 | 11 | 2 | 0 | 0/0 | 0.0 | Evidence invariants: failed, malformed or contradictory calls are not zero. |
| `test_entity_profile_execution_plan.py` | entity_profile | DOMAIN_INVARIANT | DOMA=39 | 39 | 30 | 0 | 0 | 0/0 | 0.0 | Matriz foco × fuentes y compuertas de llamada: una sola autoridad decide. |
| `test_entity_profile_executive_actions.py` | entity_profile | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_entity_profile_executive_ux.py` | entity_profile | DOMAIN_INVARIANT | ARCH=1, DOMA=12, PROD=7, PROP=2 | 22 | 7 | 2 | 7 | 0/0 | 0.0 |  |
| `test_entity_profile_executive_ux_boundary.py` | entity_profile | UNKNOWN | ARCH=2, UNKN=5 | 7 | 4 | 0 | 0 | 0/0 | 0.0 | Bound the explicitly authorized profile and Alpha UI additions by exact bytes. |
| `test_entity_profile_independent_evidence.py` | entity_profile | AUDIT_HISTORICAL | AUDI=22 | 22 | 19 | 1 | 7 | 0/0 | 0.0 | Independent evidence and boundary falsification with synthetic source replies. |
| `test_entity_profile_independent_review.py` | entity_profile | AUDIT_HISTORICAL | AUDI=31 | 31 | 28 | 1 | 5 | 0/0 | 0.0 | Independent audit: desired contracts, deliberately red on candidate 991a02f. |
| `test_entity_profile_metric_evidence.py` | entity_profile | DOMAIN_INVARIANT | DOMA=20, PROP=1 | 21 | 7 | 1 | 0 | 0/0 | 0.0 | Verdad por métrica: transporte exitoso no es lo mismo que hecho acreditado. |
| `test_entity_profile_queries.py` | entity_profile | DOMAIN_INVARIANT | DOMA=15 | 15 | 6 | 0 | 0 | 0/0 | 0.0 |  |
| `test_entity_profile_r3d_postposed_source.py` | entity_profile | DOMAIN_INVARIANT | DOMA=14, PROP=2 | 16 | 10 | 2 | 0 | 0/0 | 0.0 | R3d: one occurrence authority for postposed source and topic ownership. |
| `test_entity_profile_remediation_properties.py` | entity_profile | AUDIT_HISTORICAL | AUDI=28 | 28 | 18 | 10 | 10 | 0/0 | 0.0 | P13–P23: las propiedades que la revisión independiente echó en falta. |
| `test_entity_profile_scope.py` | entity_profile | DOMAIN_INVARIANT | DOMA=8 | 8 | 4 | 0 | 0 | 0/0 | 0.0 |  |
| `test_entity_profile_semantic_corpus.py` | entity_profile | DOMAIN_INVARIANT | DOMA=276 | 276 | 276 | 0 | 0 | 0/0 | 0.0 | Corpus sistémico de la petición semántica del perfil institucional. |
| `test_entity_profile_semantic_plan_freeze.py` | entity_profile | AUDIT_HISTORICAL | AUDI=22 | 22 | 0 | 0 | 0 | 0/0 | 0.0 | Contratos congelados del incidente P1 y de la fuga semántica que lo produce. |
| `test_entity_profile_semantic_properties.py` | entity_profile | PROPERTY | PROP=12 | 12 | 0 | 12 | 0 | 0/0 | 0.0 | P1–P12: las doce propiedades del plan semántico, sobre entradas generadas. |
| `test_entity_profile_semantic_request.py` | entity_profile | DOMAIN_INVARIANT | DOMA=23 | 23 | 8 | 0 | 0 | 0/0 | 0.0 | La petición semántica: qué pidió el usuario, antes de decidir qué ejecutar. |
| `test_entity_profile_source_plan.py` | entity_profile | DOMAIN_INVARIANT | DOMA=18 | 18 | 4 | 0 | 0 | 0/0 | 0.0 | Proyección por fuente: ningún predicado sobrevive al esquema equivocado. |
| `test_entity_profile_supplier_empty_and_budget.py` | entity_profile | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 | A focused supplier view never answers with a bare header, and never blames the s |
| `test_entity_profile_supplier_placeholders.py` | entity_profile | DOMAIN_INVARIANT | DOMA=25, PROP=1 | 26 | 21 | 1 | 0 | 0/0 | 0.0 | Execute the generated aggregation in SQLite: WHERE precedes GROUP BY/LIMIT. |
| `test_entity_profile_workflow.py` | entity_profile | PRODUCT | PROD=17 | 17 | 5 | 0 | 17 | 0/0 | 0.0 |  |
| `test_procurement_action.py` | entity_profile | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Tests PROCUREMENT-ACTION-001 — scrub verbos contractuales del objeto. |
| `test_procurement_foundation_frozen_delivery.py` | entity_profile | AUDIT_HISTORICAL | AUDI=14 | 14 | 11 | 1 | 0 | 0/0 | 0.0 | Historical provenance is independent of later checkout bytes and Git failures. |
| `test_procurement_layers_model.py` | entity_profile | DOMAIN_INVARIANT | DOMA=28, PROP=3 | 31 | 21 | 3 | 0 | 0/0 | 0.0 | Deterministic domain invariants: identity, evidence, cardinality and navigation. |
| `test_procurement_layers_sources.py` | entity_profile | UNKNOWN | UNKN=13 | 13 | 8 | 0 | 0 | 0/0 | 0.0 | Receipt integrity and frozen-cohort integration, including negative evidence. |
| `test_procurement_relationships.py` | entity_profile | AUDIT_HISTORICAL | AUDI=28, PROP=6 | 34 | 22 | 4 | 0 | 0/0 | 0.0 | Offline authority, ambiguity, isolation and byte-replay invariants. |
| `test_entity_profile_frame_b_admission.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=106, PROD=3 | 109 | 90 | 0 | 3 | 0/0 | 0.0 | La tabla de transiciones de la sección 10, y que sus predicados no compiten. |
| `test_entity_profile_frame_b_integration.py` | entity_profile_frame | PRODUCT | DOMA=9, PROD=12 | 21 | 8 | 0 | 12 | 0/0 | 0.0 | Los ocho escenarios `INTEGRATION_B_REQUIRED`, como pruebas REALES. |
| `test_entity_profile_frame_b_properties.py` | entity_profile_frame | PROPERTY | PROP=9 | 9 | 0 | 9 | 0 | 0/0 | 0.0 | Propiedades de lo que V1.2-B añade: estado durable, contrato y publicación. |
| `test_entity_profile_frame_b_publication.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=11, PROD=10 | 21 | 0 | 0 | 10 | 0/0 | 0.0 | Publicación, recarga y rollback: lo que sobrevive y lo que se invalida. |
| `test_entity_profile_frame_b_semantics.py` | entity_profile_frame | PRODUCT | PROD=19 | 19 | 0 | 0 | 19 | 0/0 | 0.0 | La semántica conversacional obligatoria, de extremo a extremo. |
| `test_entity_profile_frame_b_utility.py` | entity_profile_frame | PRODUCT | PROD=15 | 15 | 11 | 0 | 15 | 0/0 | 0.0 | La puerta de UTILIDAD: lo soportado tiene que ejecutar y responder. |
| `test_entity_profile_frame_catalog_parity.py` | entity_profile_frame | ARCHITECTURE | ARCH=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | Mientras convivan los dos catálogos, deben decir lo mismo. |
| `test_entity_profile_frame_coordinator_clock.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=2 | 2 | 2 | 0 | 0 | 0/0 | 0.0 | The helper's civil reference is injected, including after a later reload. |
| `test_entity_profile_frame_core.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=28, PROP=1 | 29 | 14 | 1 | 0 | 0/0 | 0.0 | Contraejemplos CORE del diseño congelado, portados al Frame V1.2-A. |
| `test_entity_profile_frame_evidence.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=17 | 17 | 12 | 0 | 0 | 0/0 | 0.0 | P2-C: una métrica no finita nunca acredita un monto, ni invalida el conteo. |
| `test_entity_profile_frame_independent_contracts.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=18 | 18 | 12 | 1 | 0 | 0/0 | 0.0 | Independent positive contract checks and original CORE numeric ports. |
| `test_entity_profile_frame_independent_review.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=29 | 29 | 26 | 0 | 0 | 0/0 | 0.0 | Independent V1.2-A audit: intended invariants, with unhidden red tests. |
| `test_entity_profile_frame_integration_contract.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=12 | 12 | 8 | 0 | 0 | 0/0 | 0.0 | Los adversariales que V1.2-A NO puede resolver: contrato preparado, sin E2E. |
| `test_entity_profile_frame_migration.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=18, PROP=1 | 19 | 9 | 1 | 0 | 0/0 | 0.0 | P2-A/P2-B: leer contexto legacy sin preferir en silencio una de dos copias. |
| `test_entity_profile_frame_mutations.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=16 | 16 | 2 | 0 | 0 | 0/0 | 0.0 | Cada mutación debe MATAR una aserción conductual, no un import. |
| `test_entity_profile_frame_properties.py` | entity_profile_frame | PROPERTY | DOMA=2, PROP=14 | 16 | 0 | 14 | 0 | 0/0 | 0.0 | Propiedades del core: invariantes que ningún ejemplo enumerado cubre. |
| `test_entity_profile_frame_v1_2_a2.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=30, PROP=3 | 33 | 26 | 3 | 0 | 0/0 | 0.0 | A2 counterexamples: immutable plans, R3 ownership, and legacy state. |
| `test_frame_leak_canary.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=5 | 5 | 0 | 0 | 5 | 0/0 | 0.0 | G0 — Frame-leak canary (5 vectores). |
| `test_v12a1_independent_counterexamples.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=13 | 13 | 10 | 0 | 0 | 0/0 | 0.0 | Independent counterexamples for SECOPPAL Frame V1.2-A.1. |
| `test_v12b1_actor_port.py` | entity_profile_frame | PRODUCT | PROD=4 | 4 | 4 | 0 | 4 | 0/0 | 0.0 | Authorized Actor port: independent direct/API states and consumed-token rejectio |
| `test_v12b1_independent_controls.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=13 | 13 | 12 | 0 | 13 | 0/0 | 0.0 | Positive continuity controls and independent token/owner boundaries. |
| `test_v12b1_independent_mutation_oracles.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=2 | 2 | 2 | 0 | 2 | 0/0 | 0.0 | Product invariants separate from diagnostic route names for M2/M3. |
| `test_v12b1_independent_records.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=28 | 28 | 27 | 1 | 27 | 0/0 | 0.0 | Independent corruption and valid-publication controls on real reload. |
| `test_v12b1_independent_storage.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=7 | 7 | 6 | 0 | 7 | 0/0 | 0.0 | Publication failures at actual file boundaries, after private writes exist. |
| `test_v12b1_independent_supplier_overflow.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=6 | 6 | 6 | 0 | 6 | 0/0 | 0.0 | Bounded supplier defect reproduction, runnable on pre-B.1 exports too. |
| `test_v12b1_process_ports.py` | entity_profile_frame | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 | Ports A/B with independent processes and pipe/event handshakes. |
| `test_v12b1_remediation_acceptance.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=25 | 25 | 16 | 2 | 23 | 0/0 | 0.0 | B.1 acceptance: real workflow/reload, only external effects are synthetic. |
| `test_v12b2_integrity.py` | entity_profile_frame | PRODUCT | PROD=12, PROP=1 | 13 | 10 | 1 | 12 | 0/0 | 0.0 | Structural buyer authority and cardinality, without resolving on reload. |
| `test_v12b2_storage.py` | entity_profile_frame | PRODUCT | DOMA=2, PROD=12 | 14 | 12 | 0 | 12 | 0/0 | 0.0 | Real I/O faults on the new barrier, including errors AFTER visibility. |
| `test_v12b2_suppliers.py` | entity_profile_frame | PRODUCT | PROD=24, PROP=2 | 26 | 24 | 2 | 24 | 0/0 | 0.0 | Amounts never erase supplier identity/count, source facts or publication. |
| `test_v12b_independent_base_comparison.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=5 | 5 | 5 | 0 | 5 | 0/0 | 0.0 | Identical observational probes runnable on BASE and candidate (no B imports). |
| `test_v12b_independent_boundaries.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=23 | 23 | 17 | 1 | 22 | 0/0 | 0.0 | Real continuation, compatibility and delegated authority boundaries. |
| `test_v12b_independent_continuity.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=36 | 36 | 35 | 0 | 36 | 0/0 | 0.0 | Independent workflow counterexamples; assertions are the frozen contract. |
| `test_v12b_independent_execution.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=10 | 10 | 8 | 0 | 10 | 0/0 | 0.0 | Independent positive execution controls and a mismatched Frame boundary. |
| `test_v12b_independent_publication.py` | entity_profile_frame | AUDIT_HISTORICAL | AUDI=5 | 5 | 2 | 0 | 2 | 0/0 | 0.0 | Controlled interleavings at actual reservation, execution and publish boundaries |
| `test_contextual_suggestions.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=39 | 39 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for app.core.contextual_suggestions. |
| `test_conversation_snapshot.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=7 | 7 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 4 — el Turn como ExecutionSnapshot: outcome persistido + last_successful_t |
| `test_conversation_store.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=22 | 22 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para ConversationStore — 2.1 |
| `test_dialogue_e2e_invariants.py` | followup_conversation | PRODUCT | DOMA=1, PROD=29 | 30 | 29 | 0 | 29 | 0/0 | 0.0 | E2E multi-turn dialogue invariants — anti "tailor's syndrome". |
| `test_dialogue_policy.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=7 | 7 | 7 | 0 | 0 | 0/0 | 0.0 | B2 — DISCOURSE-FILLER: verbos interrogativos scrubeados. |
| `test_dialogue_policy_corpus_min.py` | followup_conversation | ARCHITECTURE | ARCH=7 | 7 | 0 | 0 | 0 | 0/0 | 0.0 | Validate structure of dialogue_policy_corpus_min.yaml — no runner yet. |
| `test_dogfood_regressions.py` | followup_conversation | AUDIT_HISTORICAL | AUDI=11 | 11 | 10 | 0 | 0 | 2/0 | 0.0 | Etapa 0 — Regresiones deterministas de la corrida dogfood_20260627. |
| `test_dual_dataset_conversation.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=5, PROD=2 | 7 | 0 | 0 | 2 | 0/0 | 0.0 | Dual-dataset Commit 4: conversación dual. |
| `test_e2e_1_2.py` | followup_conversation | PRODUCT | PROD=22 | 22 | 0 | 0 | 0 | 0/0 | 0.0 | Tests E2E tarea 1.2.e — traza completa query→params para los casos |
| `test_e2e_followup_context.py` | followup_conversation | PRODUCT | PROD=2 | 2 | 0 | 0 | 2 | 0/0 | 0.0 | Test E2E: follow-up contextual preserva dataset y objeto. |
| `test_followup_contratista_purge.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 | B4 — FOLLOWUP-NIT-LEAK-001: purga herencia en contratista-only pivot. |
| `test_followup_delta_queries.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=8, PROD=4 | 12 | 0 | 0 | 4 | 0/0 | 0.0 | Tests E2E para follow-up con queries delta (refinamiento puro). |
| `test_followup_engine.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=30 | 30 | 0 | 0 | 0 | 0/0 | 0.0 | Tests del follow-up engine — subsistema conversacional de SECOPPAL. |
| `test_followup_engine_new_search_classification.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=7 | 7 | 6 | 0 | 0 | 0/0 | 0.0 | Tests de equivalencia de responsabilidad H3/H8. |
| `test_followup_merge_policy.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=7 | 7 | 0 | 0 | 0 | 0/0 | 0.0 | Explicit per-field raw_params merge policy (anti follow-up poisoning). |
| `test_followup_mutations.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=19 | 19 | 5 | 0 | 0 | 0/0 | 0.0 | Etapa 2: mutaciones conversacionales tipadas y trazables. |
| `test_followup_post_merge.py` | followup_conversation | PRODUCT | PROD=3 | 3 | 0 | 0 | 3 | 0/0 | 0.0 | Tests E2E: follow-up robusto post-merge — delta queries conservan contexto. |
| `test_followup_purge.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_hca_fresh_outcome_e2e.py` | followup_conversation | PRODUCT | PROD=1 | 1 | 0 | 0 | 1 | 0/0 | 0.0 | Workflow tripwire for clarification decided before query planning. |
| `test_newsearch_pagination.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=20, PROD=6 | 26 | 0 | 0 | 6 | 0/0 | 0.0 | Tests: new_search completo + paginación robusta + anti-WHERE 1=1. |
| `test_refinement_recovery.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 4 — recuperación de refinamientos vacíos (delta + UX). |
| `test_search_sh1_dogfood_regressions.py` | followup_conversation | AUDIT_HISTORICAL | AUDI=4 | 4 | 0 | 0 | 1 | 0/0 | 0.0 | SEARCH-SH1: frozen dogfood regressions for semantic dimensions and money. |
| `test_slash_commands.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=8 | 8 | 6 | 0 | 0 | 0/0 | 0.0 | B1 — SLASH-COMMAND-NOT-DETECTED-001: variantes con slash como reset. |
| `test_sprint_anaphora_mayor.py` | followup_conversation | AUDIT_HISTORICAL | AUDI=10 | 10 | 0 | 0 | 4 | 0/0 | 0.0 | Tests: protected phrases + pagination + temporal anaphora + ESE guard + degrade  |
| `test_suggester.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=14 | 14 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para suggester.py — generación de sugerencias accionables. |
| `test_suggester_contextual.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | Tests Suggester contextual por intent_type (Sprint 3 fase 2). |
| `test_suggestion_selection.py` | followup_conversation | DOMAIN_INVARIANT | DOMA=9 | 9 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para sugerencias ejecutables — selección numérica (1, 2, 3). |
| `test_city_detection.py` | geo_territory | DOMAIN_INVARIANT | DOMA=7, PROD=1 | 8 | 0 | 0 | 1 | 0/0 | 0.0 | Tests: city/municipio detection — evitar que ciudades entren en objeto. |
| `test_gazetteer_alias_ownership_r1c.py` | geo_territory | DOMAIN_INVARIANT | DOMA=44 | 44 | 40 | 0 | 0 | 0/0 | 0.0 | GAZETTEER-ALIAS-OWNERSHIP-R1C — canarios de propiedad de aliases. |
| `test_gazetteer_lote_01.py` | geo_territory | DOMAIN_INVARIANT | DOMA=23 | 23 | 19 | 0 | 0 | 0/3 | 0.0 | Tests for Lote 01 gazetteer expansion — verifies new entities resolve correctly |
| `test_gazetteer_manager.py` | geo_territory | DOMAIN_INVARIANT | DOMA=32 | 32 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for the Gazetteer Manager (scripts/gazetteer_manager.py). |
| `test_gazetteer_ownership_gate_r2a.py` | geo_territory | DOMAIN_INVARIANT | DOMA=9 | 9 | 0 | 0 | 0 | 0/0 | 0.0 | R2A — la puerta de ownership múltiple mira el CONJUNTO, no el cardinal. |
| `test_gazetteer_source_binding_audit.py` | geo_territory | AUDIT_HISTORICAL | AUDI=40 | 40 | 0 | 0 | 0 | 4/0 | 0.0 | Auditoría ADR-073 gazetteer-wide: los contratos que el auditor NO puede romper. |
| `test_gazetteer_source_binding_audit_regressions.py` | geo_territory | AUDIT_HISTORICAL | AUDI=34 | 34 | 8 | 0 | 0 | 0/0+rt | 0.0 | ADR-073 gazetteer-wide — regresiones de la REMEDIACIÓN (revisión independiente). |
| `test_geo_mention_resolution.py` | geo_territory | DOMAIN_INVARIANT | DOMA=13 | 13 | 4 | 0 | 0 | 0/0 | 0.0 | Tests for typed and untyped pure geography gazetteer lookups. |
| `test_geo_vs_entity_typed.py` | geo_territory | DOMAIN_INVARIANT | DOMA=14 | 14 | 6 | 0 | 0 | 0/0 | 0.0 | Etapa 7 — resolución TIPADA: territorio (departamento) vs comprador (entidad). |
| `test_municipal_language_capture.py` | geo_territory | DOMAIN_INVARIANT | DOMA=47 | 47 | 46 | 0 | 0 | 0/0 | 0.0 | M1A.1 predictive capture cases, frozen before changing runtime. |
| `test_municipal_m0_evidence.py` | geo_territory | AUDIT_HISTORICAL | AUDI=6 | 6 | 2 | 0 | 0 | 0/0 | 0.0 | The municipal census never turns an unresolved residue into full coverage. |
| `test_municipal_m1a.py` | geo_territory | AUDIT_HISTORICAL | AUDI=19 | 19 | 13 | 0 | 0 | 0/0 | 0.0 | Predictive M1A gates run against a temporary catalog before production writes. |
| `test_municipal_m1a2a.py` | geo_territory | AUDIT_HISTORICAL | AUDI=13 | 13 | 10 | 0 | 0 | 0/0 | 0.0 | M1A.2A: no affiliation from department/NIT; exact Turbaco physical projection. |
| `test_municipal_m1b.py` | geo_territory | AUDIT_HISTORICAL | AUDI=13 | 13 | 4 | 0 | 0 | 0/0 | 0.0 | M1B must explain the fixed cohort without turning geography/NIT into identity. |
| `test_municipal_m2a.py` | geo_territory | AUDIT_HISTORICAL | AUDI=21 | 21 | 2 | 0 | 0 | 0/0 | 0.0 | Falsify fiscal identity shortcuts and verify offline discovery accounting. |
| `test_municipal_m2b.py` | geo_territory | AUDIT_HISTORICAL | AUDI=35 | 35 | 18 | 0 | 0 | 0/0 | 0.0 | Falsify institutional shortcuts and verify the frozen M2B conservation ledger. |
| `test_municipal_m2b1.py` | geo_territory | AUDIT_HISTORICAL | AUDI=33 | 33 | 17 | 0 | 0 | 0/0 | 0.0 | Adversarial checks for every M2B.1 identity relaxation and frozen accounting. |
| `test_municipal_territory.py` | geo_territory | AUDIT_HISTORICAL | AUDI=33 | 33 | 30 | 0 | 0 | 0/0 | 0.0 | DIVIPOLA geography cannot promote a buyer or guess a homonymous municipality. |
| `test_plan_query_city_extraction.py` | geo_territory | PRODUCT | PROD=7 | 7 | 5 | 0 | 7 | 0/0 | 0.0 | B4.1 — E2E tests for P3 (city extraction) + P9 (entity autopromotion purge). |
| `test_regiones.py` | geo_territory | DOMAIN_INVARIANT | DOMA=14 | 14 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for region-based search (búsqueda por zonas/regiones). |
| `test_territorial_import_guard.py` | geo_territory | ARCHITECTURE | ARCH=60 | 60 | 55 | 3 | 0 | 0/0 | 0.0 | Syntax and mutation regressions for the single territorial runtime guard. |
| `test_territorial_knowledge.py` | geo_territory | DOMAIN_INVARIANT | ARCH=1, DOMA=10 | 11 | 0 | 0 | 0 | 0/0 | 0.0 | Contract tests for the stable M1A -> M2B.1 territorial knowledge shadow. |
| `test_turbaco_m1a2a.py` | geo_territory | DOMAIN_INVARIANT | DOMA=6 | 6 | 6 | 0 | 0 | 0/0 | 0.0 | Regression: human Turbaco aliases must query its actual SECOP BuyerUnit. |
| `test_analyze_modality_followup_gate.py` | modality | DOMAIN_INVARIANT | DOMA=44 | 44 | 0 | 0 | 0 | 0/0 | 0.0 | Tests focales del analizador de la compuerta de producto M3C-C -> M3C-D. |
| `test_modality_catalog_v1.py` | modality | DOMAIN_INVARIANT | DOMA=94 | 94 | 62 | 0 | 0 | 0/0 | 0.0 | MOD-M1 — tests del catálogo canónico de modalidad V1. |
| `test_modality_dataset_projection.py` | modality | DOMAIN_INVARIANT | DOMA=12 | 12 | 3 | 0 | 0 | 0/0 | 0.0 |  |
| `test_modality_execution_policy.py` | modality | DOMAIN_INVARIANT | DOMA=11 | 11 | 8 | 0 | 0 | 0/0 | 0.0 |  |
| `test_modality_filter.py` | modality | DOMAIN_INVARIANT | DOMA=34 | 34 | 17 | 0 | 0 | 0/0 | 0.0 | Tests del value object ejecutable ModalityFilter (M3B-1). |
| `test_modality_followup.py` | modality | DOMAIN_INVARIANT | DOMA=164 | 164 | 97 | 0 | 0 | 0/0 | 0.0 | Tests del motor puro de mutaciones conversacionales de modalidad (M3C-B / R1). |
| `test_modality_followup_pipeline.py` | modality | DOMAIN_INVARIANT | DOMA=105, PROD=64 | 169 | 112 | 0 | 64 | 0/0 | 0.0 | E2E de pipeline para operaciones conversacionales de modalidad (M3C-C). |
| `test_modality_followup_telemetry.py` | modality | DOMAIN_INVARIANT | DOMA=43, PROD=13 | 56 | 5 | 0 | 13 | 0/0 | 0.0 | Tests for app/core/modality_followup_telemetry.py. |
| `test_modality_followup_wiring.py` | modality | DOMAIN_INVARIANT | DOMA=101 | 101 | 55 | 0 | 0 | 0/0 | 0.0 | Tests del adaptador de wiring de modalidad (M3C-C). |
| `test_modality_frame_adapter.py` | modality | DOMAIN_INVARIANT | DOMA=22 | 22 | 7 | 0 | 0 | 0/0 | 0.0 | Tests del adapter puro M2 → ModalityInterpretation (M3B-1). |
| `test_modality_lexicon.py` | modality | DOMAIN_INVARIANT | DOMA=78 | 78 | 41 | 0 | 0 | 0/0 | 0.0 | Loader fail-closed, matriz semántica y normalización del lexicón M2-R1.1. |
| `test_modality_parser_integration.py` | modality | DOMAIN_INVARIANT | ARCH=1, DOMA=7 | 8 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_modality_pipeline_integration.py` | modality | DOMAIN_INVARIANT | ARCH=2, DOMA=30, PROD=5 | 37 | 23 | 0 | 5 | 0/0 | 0.0 |  |
| `test_modality_precedence.py` | modality | DOMAIN_INVARIANT | ARCH=1, DOMA=25 | 26 | 22 | 0 | 0 | 0/0 | 0.0 |  |
| `test_modality_resolver.py` | modality | DOMAIN_INVARIANT | ARCH=1, DOMA=2499 | 2500 | 2465 | 0 | 0 | 0/0 | 0.0 | Corpus adversarial y propiedades del resolver modular M2-R1.5. |
| `test_search_sh1_modality_audit.py` | modality | AUDIT_HISTORICAL | AUDI=15 | 15 | 14 | 0 | 0 | 0/0 | 0.0 | SEARCH-SH1 audit corpus for the existing modality dimension. |
| `test_format_money.py` | money_value | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para format_money — convención Colombia (MM = mil millones, B = billones). |
| `test_money.py` | money_value | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for money_to_cop — Colombian monetary expression parser. |
| `test_money_identifier_arbitration.py` | money_value | DOMAIN_INVARIANT | DOMA=10 | 10 | 8 | 0 | 0 | 0/0 | 0.0 | Money V2 / M3 — evidencia semántica para NIT frente a dinero. |
| `test_money_polarity_e2e.py` | money_value | PRODUCT | PROD=36 | 36 | 33 | 0 | 15 | 0/0 | 0.0 | MPV2 parser → planning → SECOP spies → faithful presentation. |
| `test_money_polarity_v2.py` | money_value | DOMAIN_INVARIANT | DOMA=106, PROP=8 | 114 | 105 | 8 | 0 | 0/0 | 0.0 | Frozen human oracle for MPV2; expectations precede runtime changes. |
| `test_money_presentation.py` | money_value | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Money V2 / M7 — transparencia contextual del filtro económico. |
| `test_money_range_atomicity.py` | money_value | DOMAIN_INVARIANT | DOMA=21 | 21 | 17 | 0 | 0 | 0/0 | 0.0 | INV-MONEY-RANGE-ATOMIC — un rango de dos extremos no se aplica a medias. |
| `test_money_resolution.py` | money_value | DOMAIN_INVARIANT | DOMA=68 | 68 | 59 | 0 | 0 | 0/0 | 0.0 | Money V2 / M2 — tests de la autoridad de representación numérica monetaria. |
| `test_money_smmlv.py` | money_value | DOMAIN_INVARIANT | DOMA=19 | 19 | 8 | 0 | 0 | 0/0 | 0.0 | Money V2 / M6 — SMMLV con autoridad anual explícita. |
| `test_money_v2_canary.py` | money_value | AUDIT_HISTORICAL | AUDI=10 | 10 | 5 | 0 | 10 | 0/0 | 0.0 | Money V2 / M9 — canario determinista por el workflow normal, sin red. |
| `test_money_v2_invariants.py` | money_value | DOMAIN_INVARIANT | DOMA=64 | 64 | 48 | 0 | 0 | 0/0 | 0.0 | Money V2 / M1 — congelación del comportamiento monetario antes de tocar runtime. |
| `test_money_verbal_numbers.py` | money_value | DOMAIN_INVARIANT | DOMA=14 | 14 | 11 | 0 | 0 | 0/0 | 0.0 | Money V2 / M4 — cardinales monetarios españoles prácticos. |
| `test_search_sh1_money_followups.py` | money_value | DOMAIN_INVARIANT | DOMA=21 | 21 | 15 | 0 | 0 | 0/0 | 0.0 | SEARCH-SH1 monetary follow-up composition and mutation invariants. |
| `test_value_polarity.py` | money_value | DOMAIN_INVARIANT | DOMA=22 | 22 | 22 | 0 | 0 | 0/0 | 0.0 |  |
| `test_value_ranges.py` | money_value | DOMAIN_INVARIANT | DOMA=21 | 21 | 17 | 0 | 0 | 0/0 | 0.0 | Tests for monetary range parsing (VALUE-RANGE-001 + DOGFOOD-RANGE-007). |
| `test_value_sanity.py` | money_value | DOMAIN_INVARIANT | DOMA=12 | 12 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for VALUE-SANITY-002A — Value anomaly detection for SECOP contracts. |
| `test_value_sanity_reachability.py` | money_value | DOMAIN_INVARIANT | DOMA=23, PROP=1 | 24 | 15 | 1 | 0 | 0/0 | 0.0 | ADR-005 reachability from projected source strings, with no external I/O. |
| `test_control_language.py` | negation_control | DOMAIN_INVARIANT | DOMA=73 | 73 | 53 | 0 | 0 | 0/1 | 0.0 | HCA-R6 — control language is not search content. |
| `test_negation_binding.py` | negation_control | DOMAIN_INVARIANT | ARCH=1, DOMA=36 | 37 | 20 | 0 | 0 | 0/0 | 0.0 | HCA-R4-S1: dimensional negation binding and positive-span isolation. |
| `test_negation_lifecycle.py` | negation_control | PRODUCT | PROD=24 | 24 | 13 | 0 | 24 | 0/0 | 0.0 | HCA-R4-S4: symmetric lifecycle for persisted dimensional exclusions. |
| `test_negation_workflow.py` | negation_control | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Workflow guards for INV-NO-SILENT-NEGATION-DROP. |
| `test_object_negation.py` | negation_control | DOMAIN_INVARIANT | DOMA=9 | 9 | 0 | 0 | 0 | 0/0 | 0.0 | HCA-R4-S2: executable OBJECT negation without polarity loss. |
| `test_object_negation_soql.py` | negation_control | DOMAIN_INVARIANT | DOMA=9 | 9 | 8 | 0 | 0 | 0/0 | 0.0 | Specification tests for the null-safe OBJECT exclusion predicate. |
| `test_order_semantics.py` | negation_control | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | H4 — gate semántico del ORDER BY (presentacional vs semántico). |
| `test_ordering_followup_source_universe.py` | negation_control | PRODUCT | PROD=10 | 10 | 8 | 0 | 10 | 0/0 | 0.0 | INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE (HCA-FOLLOWUP-016). |
| `test_ordering_proper_name_spans.py` | negation_control | DOMAIN_INVARIANT | DOMA=19 | 19 | 16 | 0 | 0 | 0/0 | 0.0 | Ordering must respect accredited names without deleting its vocabulary. |
| `test_safe_negation_policies.py` | negation_control | DOMAIN_INVARIANT | DOMA=32, PROD=21 | 53 | 44 | 0 | 21 | 0/0 | 0.0 | HCA-R4-S3: fail-closed policies for unsupported bound negations. |
| `test_soda3_h4_ordering_wiring.py` | negation_control | DOMAIN_INVARIANT | ARCH=1, DOMA=9 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | T1B — la política de ordenamiento viaja desde la ruta, nunca desde el SoQL. |
| `test_state_polarity_safety.py` | negation_control | DOMAIN_INVARIANT | ARCH=1, DOMA=170 | 171 | 166 | 0 | 0 | 0/0 | 0.0 | INV-NO-SILENT-SEMANTIC-FALLBACK aplicado a la polaridad de estado. |
| `test_notify_authorization.py` | notify | DOMAIN_INVARIANT | DOMA=44 | 44 | 10 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/authorization.py — la matriz única de "¿se puede |
| `test_notify_claim.py` | notify | DOMAIN_INVARIANT | DOMA=14 | 14 | 3 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/claim.py — exclusión mutua efímera sobre un |
| `test_notify_endpoints.py` | notify | DOMAIN_INVARIANT | DOMA=41 | 41 | 2 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/endpoint_store.py y masking.py — a dónde y si se puede. |
| `test_notify_fake_provider.py` | notify | DOMAIN_INVARIANT | DOMA=17 | 17 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/fake_provider.py (R2C). |
| `test_notify_grouping.py` | notify | DOMAIN_INVARIANT | DOMA=21 | 21 | 2 | 0 | 0 | 0/0 | 0.0 |  |
| `test_notify_identity.py` | notify | DOMAIN_INVARIANT | DOMA=14 | 14 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/identity_store.py — quién es la persona. |
| `test_notify_ledger.py` | notify | DOMAIN_INVARIANT | DOMA=25 | 25 | 7 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/ledger_store.py — intentos de entrega, |
| `test_notify_outbox.py` | notify | DOMAIN_INVARIANT | DOMA=24 | 24 | 8 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/outbox_store.py — outbox, idempotencia y |
| `test_notify_preferences.py` | notify | DOMAIN_INVARIANT | ARCH=1, DOMA=62 | 63 | 17 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/preference_store.py — qué aceptó recibir la persona. |
| `test_notify_provider.py` | notify | DOMAIN_INVARIANT | DOMA=22 | 22 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/provider.py — el contrato con el mundo exterior (R2C). |
| `test_notify_quiet_hours.py` | notify | DOMAIN_INVARIANT | DOMA=38 | 38 | 10 | 0 | 0 | 0/0 | 0.0 |  |
| `test_notify_render.py` | notify | DOMAIN_INVARIANT | DOMA=70 | 70 | 16 | 0 | 0 | 0/0 | 0.0 |  |
| `test_notify_retry.py` | notify | DOMAIN_INVARIANT | DOMA=61 | 61 | 37 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/retry.py — la aritmética pura del tiempo del claim. |
| `test_notify_schema.py` | notify | DOMAIN_INVARIANT | DOMA=56 | 56 | 9 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/schema.py — el preflight de bases heredadas. |
| `test_notify_selector.py` | notify | DOMAIN_INVARIANT | ARCH=1, DOMA=71 | 72 | 40 | 0 | 0 | 0/0 | 0.0 |  |
| `test_notify_telegram_provider.py` | notify | DOMAIN_INVARIANT | ARCH=1, DOMA=22 | 23 | 10 | 0 | 0 | 0/0 | 0.0 | Pruebas sin red del TelegramProvider real del piloto. |
| `test_notify_worker.py` | notify | DOMAIN_INVARIANT | ARCH=1, DOMA=57 | 58 | 7 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/worker.py — el ciclo completo de entrega (P1B-4, R2C). |
| `test_notify_worker_store.py` | notify | DOMAIN_INVARIANT | ARCH=2, DOMA=167 | 169 | 51 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/notify/worker_store.py — las primitivas transaccionales del |
| `test_a2_suppliers_dogfood_promotion.py` | other | AUDIT_HISTORICAL | AUDI=75 | 75 | 43 | 0 | 19 | 0/0 | 0.0 | Gates for promoting A2 SUPPLIERS to Rodrigo's personal dogfood. |
| `test_accreditation_assembly.py` | other | DOMAIN_INVARIANT | DOMA=21 | 21 | 6 | 0 | 0 | 0/0 | 0.0 | C4A/C4B: authorities over the raw turn, accredited params, residual and overreac |
| `test_accreditation_contract.py` | other | DOMAIN_INVARIANT | DOMA=47, PROP=2 | 49 | 36 | 2 | 0 | 0/0 | 0.0 | Accreditation contracts: credential identity, declarative registry, neutral path |
| `test_adr073_schema3_product.py` | other | AUDIT_HISTORICAL | AUDI=13, PROP=1 | 14 | 0 | 1 | 0 | 0/0 | 0.0 | Schema 3 product contract against the frozen Schema 2 projection oracle. |
| `test_c4a2_harness.py` | other | UNKNOWN | UNKN=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | C4A.2 harness checks: the adjudication is consistent and the frozen files did no |
| `test_c4a2a_harness_integrity.py` | other | DOMAIN_INVARIANT | DOMA=12 | 12 | 5 | 0 | 0 | 0/0 | 0.0 | C4A.2a — harness integrity: per-run scoring, computed gates, honest metrics. |
| `test_capability_binding_suppliers_contract.py` | other | DOMAIN_INVARIANT | DOMA=50, PROP=5 | 55 | 44 | 5 | 0 | 0/0 | 0.0 | A2 Driver Remediation V1 — SUPPLIERS input contract. |
| `test_capability_binding_suppliers_workflow.py` | other | PRODUCT | PROD=32 | 32 | 20 | 0 | 32 | 0/0 | 0.0 | End-to-end SUPPLIERS binding through the real workflow, with a recording source. |
| `test_contract_activity_admission_v2.py` | other | PRODUCT | PROD=11 | 11 | 8 | 0 | 11 | 0/0 | 0.0 |  |
| `test_contract_activity_alpha_replay_v2.py` | other | DOMAIN_INVARIANT | DOMA=7, PROD=1 | 8 | 3 | 0 | 1 | 0/0 | 0.0 |  |
| `test_contract_activity_filters_v2.py` | other | DOMAIN_INVARIANT | DOMA=9, PROP=1 | 10 | 0 | 1 | 0 | 0/0 | 0.0 |  |
| `test_contract_activity_narrow_corpus.py` | other | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_contract_activity_narrow_properties.py` | other | PROPERTY | PROP=3 | 3 | 0 | 3 | 0 | 0/0 | 0.0 |  |
| `test_contract_activity_narrow_results.py` | other | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_contract_activity_narrow_sql.py` | other | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_dogfood_001_execution_budget.py` | other | AUDIT_HISTORICAL | AUDI=23 | 23 | 9 | 0 | 4 | 0/0 | 0.0 | DOGFOOD-001 — el carril PROFILE recibe el presupuesto que legacy ya tenía. |
| `test_natural_alpha_contract.py` | other | DOMAIN_INVARIANT | DOMA=41, PROP=1 | 42 | 34 | 1 | 0 | 0/0 | 0.0 |  |
| `test_natural_alpha_coverage_report.py` | other | DOMAIN_INVARIANT | DOMA=35, PROP=2 | 37 | 30 | 2 | 0 | 0/0 | 0.0 | Executed SQLite reporting and pure partition properties for coverage. |
| `test_natural_alpha_dogfood.py` | other | AUDIT_HISTORICAL | AUDI=38 | 38 | 17 | 2 | 0 | 0/0+rt | 0.0 | Synthetic SQLite coverage oracle, with scope decoys and write witnesses. |
| `test_natural_alpha_evaluation.py` | other | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_natural_alpha_identity.py` | other | DOMAIN_INVARIANT | DOMA=21 | 21 | 17 | 0 | 0 | 0/0 | 0.0 | Only the current authenticated Streamlit account can mint Alpha admission. |
| `test_natural_alpha_prelegacy_seam.py` | other | DOMAIN_INVARIANT | DOMA=17 | 17 | 12 | 0 | 0 | 0/0 | 0.0 | C2: one observation before admission, consumed by the seam and never repeated. |
| `test_natural_alpha_presentation.py` | other | DOMAIN_INVARIANT | ARCH=1, DOMA=3 | 4 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_natural_alpha_runtime.py` | other | PRODUCT | PROD=18 | 18 | 18 | 0 | 18 | 0/0 | 0.0 | Full legacy execution against a SQL-evaluated source with decoy rows. |
| `test_natural_alpha_terminal_publication.py` | other | DOMAIN_INVARIANT | DOMA=8, PROD=3 | 11 | 7 | 0 | 3 | 0/0 | 0.0 | Persist terminal NONE ownership without exposing a reusable public reference. |
| `test_natural_alpha_turn_oracle.py` | other | PRODUCT | PROD=1 | 1 | 0 | 0 | 1 | 0/0 | 0.0 | External oracle: N eligible run_query turns == N classified SQLite traces. |
| `test_natural_alpha_turn_receipt.py` | other | DOMAIN_INVARIANT | DOMA=35, PROD=7 | 42 | 31 | 0 | 7 | 0/0 | 0.0 | One opaque receipt per turn, consumed by the real SQLite sink, never by text. |
| `test_natural_alpha_uniqueness.py` | other | DOMAIN_INVARIANT | DOMA=10, PROP=2 | 12 | 9 | 2 | 0 | 0/0 | 0.0 | Post-ground integrity: reject the complete proposal, never repair the jobs. |
| `test_natural_job_accredited_admission.py` | other | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | C4A: the accredited assembly inside the real turn, still with zero source calls. |
| `test_natural_job_c4b_suppliers_runtime.py` | other | DOMAIN_INVARIANT | DOMA=17 | 17 | 8 | 0 | 0 | 0/0 | 0.0 | C4B — the accredited SUPPLIERS lane inside the real turn. |
| `test_natural_job_credentialed_admission.py` | other | DOMAIN_INVARIANT | DOMA=13 | 13 | 7 | 0 | 0 | 0/0 | 0.0 | C3: a valid Observation claims Admission and ends the turn without legacy. |
| `test_residual_verifier_contract.py` | other | DOMAIN_INVARIANT | DOMA=33 | 33 | 15 | 0 | 0 | 0/0 | 0.0 | C4B — the productive residual-role contract cannot drift from the frozen winner. |
| `test_semantic_turn_evidence_r2.py` | other | DOMAIN_INVARIANT | DOMA=10, PROP=1 | 11 | 4 | 1 | 0 | 0/0 | 0.0 | Atomic semantic evidence admission and product transport regressions. |
| `test_supplier_cardinal_admission.py` | other | DOMAIN_INVARIANT | DOMA=49 | 49 | 48 | 0 | 0 | 0/1 | 0.0 | Positive admission of the cardinal span's immediate context. |
| `test_supplier_semantics.py` | other | DOMAIN_INVARIANT | DOMA=49 | 49 | 34 | 0 | 0 | 0/0 | 0.0 | A2 SUPPLIERS polarity sidecar: a lock that can only stay shut. |
| `test_supplier_semantics_replay.py` | other | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | Replay of experiment a2-polarity-sidecar-x1 through the runtime adjudicator. |
| `test_supplier_semantics_settings.py` | other | DOMAIN_INVARIANT | DOMA=18 | 18 | 6 | 0 | 0 | 0/0 | 0.0 |  |
| `test_supplier_semantics_wiring.py` | other | DOMAIN_INVARIANT | DOMA=10 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | The seam between the flag, the credential and the pure contract. |
| `test_suppliers_request_flex_v1.py` | other | DOMAIN_INVARIANT | DOMA=96, PROP=9 | 105 | 90 | 9 | 0 | 0/0 | 0.0 | SUPPLIERS 1.1 — el request explícito, el corpus y las propiedades (§15-16). |
| `test_suppliers_request_flex_v1_integration.py` | other | ARCHITECTURE | ARCH=25 | 25 | 18 | 0 | 0 | 0/0 | 0.0 | SUPPLIERS 1.1 — la ruta real de primer turno (§17). |
| `test_text_lineage.py` | other | DOMAIN_INVARIANT | DOMA=51 | 51 | 37 | 0 | 0 | 0/0 | 0.0 |  |
| `test_text_lineage_integration.py` | other | DOMAIN_INVARIANT | DOMA=26 | 26 | 17 | 0 | 0 | 0/0 | 0.0 |  |
| `test_text_lineage_properties.py` | other | PROPERTY | PROP=9 | 9 | 0 | 9 | 0 | 0/0 | 0.0 |  |
| `test_text_lineage_v1_2_contract.py` | other | DOMAIN_INVARIANT | DOMA=137 | 137 | 114 | 0 | 0 | 0/0 | 0.0 |  |
| `test_zeus_adr073_adversarial.py` | other | AUDIT_HISTORICAL | AUDI=26 | 26 | 13 | 0 | 0 | 0/0 | 0.0 | ZEUS — pruebas adversariales independientes sobre la auditoría ADR-073. |
| `test_zeus_adr073_recheck.py` | other | AUDIT_HISTORICAL | AUDI=1 | 1 | 0 | 0 | 0 | 0/0 | 0.0 | ZEUS recheck — ataques al mecanismo de integridad introducido en la remediación. |
| `contracts/test_invariant_registry.py` | product_contract | ARCHITECTURE | ARCH=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | F2 — el registro canónico de invariantes es válido y sus defensas existen. |
| `product_contract/test_product_contract_v1.py` | product_contract | PRODUCT | PROD=13 | 13 | 13 | 0 | 13 | 0/0 | 0.0 | F3 gate: only accredited READY scenarios, each with a killed seam mutant. |
| `product_contract/test_product_harness_safety.py` | product_contract | ARCHITECTURE | ARCH=23 | 23 | 3 | 1 | 5 | 0/0 | 0.0 | F2.5 — Product Harness Safety: el harness demuestra sus garantías antes de F3. |
| `test_accent_normalization.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=19 | 19 | 12 | 0 | 0 | 0/0 | 0.0 | Tests ACCENT-NORMALIZATION-001 — unaccent() en SoQL + strip_accents Python. |
| `test_bidder_intent.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | BIDDER-INTENT-002 tests: para poder presentarme + mantenimiento vial. |
| `test_bidder_intent_commercial.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | BIDDER-INTENT-003: commercial verbs must not pollute objeto in opportunity queri |
| `test_bloque1_guard_ordering.py` | query_frame_parsing | AUDIT_HISTORICAL | AUDI=15 | 15 | 0 | 0 | 8 | 0/0 | 0.0 | Tests: Bloque 1 — Guard anti-WHERE 1=1 + valor_desc vs fecha_desc + suggestions  |
| `test_contract_type_resolver.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=13 | 13 | 11 | 0 | 0 | 0/0 | 0.0 | Contractual type/nature resolver stays separate from modality authority. |
| `test_contractual_dimension_pipeline.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=19 | 19 | 14 | 0 | 0 | 0/0 | 0.0 | Natural contractual-class corpus through parser, frame and SoQL. |
| `test_degrade_query.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=10 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | Tests tarea 1.4.5 — degradacion elegante (relax_params + degrade_query action). |
| `test_guard_where_1_1_equivalence.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=19 | 19 | 16 | 0 | 0 | 0/0 | 0.0 | Tests de equivalencia H9: execute_query y FollowupGuards usan la misma |
| `test_linguistic_family_coverage.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=158 | 158 | 156 | 0 | 0 | 0/0 | 0.0 | Gate anti-vocabulario: la familia entera, no las formas observadas. |
| `test_llm_expansion_or.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_morpho_expansion_consistency.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Tests MORPHO-EXPANSION-INCONSISTENT-001 — ambas rutas expanden igual. |
| `test_morpho_uneven.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Tests MORPHO-UNEVEN-001 — cobertura morfológica ≥3 variantes por rubro. |
| `test_multi_and_demotion.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_normalization_contract.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=3, PROD=3 | 6 | 0 | 0 | 3 | 0/0 | 0.0 | HCA-R3: normalization preserves directional and open-bound intent. |
| `test_objeto_phrases.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=10 | 10 | 3 | 0 | 0 | 0/0 | 0.0 | Commit 3: la frase sobrevive desde la consulta hasta los params (objeto_phrases) |
| `test_p15_recovery_seam.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | P15 ↔ follow-up recovery seam (OPP-SCOPE-001 + B5b). |
| `test_phrase_catalog_collisions.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=2 | 2 | 2 | 0 | 0 | 0/0 | 0.0 | Precedencia entre phrase_catalog y morphological_variants (KNOWN_BIGRAMS). |
| `test_phrase_presentation.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | Commit 6C-UX: header determinístico honesto + trace completo. |
| `test_phrase_ranking.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Commit 6B: ranking de candidatos por validación de frase. |
| `test_phrase_spec.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | Contrato de datos de PhraseSpec (Commit 2): inmutable, serializable, genérico. |
| `test_phrase_validator.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=11 | 11 | 4 | 0 | 0 | 0/0 | 0.0 | Commit 4: validador puro de frases sobre filas (PhraseMatch). |
| `test_plan_query_clarification.py` | query_frame_parsing | PRODUCT | PROD=4 | 4 | 0 | 0 | 4 | 0/0 | 0.0 | B4.4 — E2E tests for clarification path (P1 command_incomplete is the key E2E ca |
| `test_plan_query_node.py` | query_frame_parsing | PRODUCT | DOMA=1, PROD=4 | 5 | 0 | 0 | 4 | 0/0 | 0.0 | Tests for B3 — plan_query node inserted as no-op pass-through. |
| `test_plan_query_scrub_and_temporal.py` | query_frame_parsing | PRODUCT | PROD=4 | 4 | 0 | 0 | 4 | 0/0 | 0.0 | B4.3 — E2E tests for P5 (temporal), P6 (number scrub), P7 (discourse filler). |
| `test_polysemic.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=9 | 9 | 0 | 0 | 0 | 0/0 | 0.0 | Tests POLYSEMIC-TOPIC-001 — polysemic topic resolution. |
| `test_query_fingerprint.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 4 — huella estable de consulta (sha256, no hash()). |
| `test_query_frame.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=12, PROD=6 | 18 | 0 | 0 | 6 | 0/0 | 0.0 | Tests: QueryFrame — política global de conversación v1.2. |
| `test_query_plan.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=32 | 32 | 10 | 0 | 0 | 0/0 | 0.0 | Tests for QueryPlan — compute_query_plan with 9 rules P1..P9. |
| `test_query_plan_or_groups.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | B0 — QUERY-PLAN-LIST-NESTED-001: tolerar grupos OR (listas anidadas) en objeto. |
| `test_query_router.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=70 | 70 | 11 | 0 | 0 | 0/0 | 0.0 | Tests for query_router v2.py (gazetteer-first, production stack). |
| `test_query_router_source_occurrence.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=9 | 9 | 9 | 0 | 0 | 0/0 | 0.0 | Global parity for occurrence-owned procurement source words. |
| `test_query_text_consumption.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=35 | 35 | 20 | 0 | 0 | 0/0 | 0.0 | Tests del consumo exacto de texto por offsets (M3B-1). |
| `test_semantic_sanity.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=9 | 9 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for the passive semantic sanity layer (P14 / semantic_sanity.py). |
| `test_sip049_aire_acondicionado.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | SIP-049: prueba de generalidad — 'aire acondicionado' por sola entrada de datos. |
| `test_spell_correction.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=68 | 68 | 33 | 0 | 0 | 0/0 | 0.0 | Tests for the spell correction module. |
| `test_spell_semantic_safety.py` | query_frame_parsing | DOMAIN_INVARIANT | DOMA=397 | 397 | 394 | 0 | 0 | 0/0 | 0.0 | INV-SPELL-CORRECTION-CANNOT-CREATE-SEMANTIC-DIMENSION. |
| `test_radar_builder.py` | radar | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/builder.py — sanitización y guardado compartido. |
| `test_radar_commands.py` | radar | DOMAIN_INVARIANT | DOMA=38 | 38 | 19 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/commands.py y el handler handle_radar_command. |
| `test_radar_detail.py` | radar | DOMAIN_INVARIANT | ARCH=2, DOMA=43 | 45 | 9 | 0 | 0 | 0/0 | 0.0 | R1 — la oportunidad que un movimiento del Radar representa. |
| `test_radar_diff.py` | radar | DOMAIN_INVARIANT | DOMA=10 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/diff.py — clasificación de novedades. |
| `test_radar_follows.py` | radar | DOMAIN_INVARIANT | DOMA=47 | 47 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/follows.py — watchlist de procesos individuales. |
| `test_radar_inbox.py` | radar | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | R5A — la bandeja es una proyección; el feed histórico no se muta. |
| `test_radar_keys.py` | radar | DOMAIN_INVARIANT | DOMA=27 | 27 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/keys.py — identidad y snapshot de resultados. |
| `test_radar_receipts.py` | radar | DOMAIN_INVARIANT | ARCH=1, DOMA=14 | 15 | 8 | 0 | 0 | 0/0 | 0.0 | R5A — receipts append-only de Inbox y feedback de relevancia. |
| `test_radar_runner.py` | radar | DOMAIN_INVARIANT | ARCH=1, DOMA=84 | 85 | 17 | 0 | 0 | 0/0 | 0.0 | Tests e2e de app/core/radar/runner.py con un SecopClient fake. |
| `test_radar_scheduler.py` | radar | DOMAIN_INVARIANT | DOMA=20 | 20 | 2 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/scheduler.py — lógica de 'due' y orquestación. |
| `test_radar_store.py` | radar | DOMAIN_INVARIANT | ARCH=5, DOMA=178 | 183 | 29 | 0 | 0 | 0/0 | 0.0 | Tests de app/core/radar/store.py — persistencia del Radar. |
| `test_radar_ui_truth.py` | radar | DOMAIN_INVARIANT | ARCH=2, DOMA=17 | 19 | 0 | 0 | 0 | 0/0 | 0.0 | Verdad de presentación Radar: zona horaria, timeline y heartbeat. |
| `test_radar_workspace.py` | radar | DOMAIN_INVARIANT | ARCH=1, DOMA=14 | 15 | 1 | 0 | 0 | 0/0 | 0.0 | R2 — el puente Radar → Workspace. |
| `test_workspace_store.py` | radar | DOMAIN_INVARIANT | DOMA=22 | 22 | 10 | 0 | 0 | 0/0 | 0.0 | WorkspaceStore — lo que el usuario decidió conservar (R2). |
| `test_analytic_control_upstream_v1.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=44, PROD=4, PROP=3 | 51 | 33 | 3 | 4 | 0/0 | 0.0 | ANALYTIC_CONTROL_UPSTREAM_V1 — analytic instructions are control, not content. |
| `test_analytic_control_upstream_v1_r1.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=48, PROP=4 | 52 | 40 | 4 | 0 | 0/0 | 0.0 | Independent R1 review regressions, frozen before runtime remediation. |
| `test_analytic_control_upstream_v1_r2_1.py` | ranking_results_presentation | AUDIT_HISTORICAL | AUDI=76 | 76 | 70 | 0 | 0 | 0/0 | 0.0 | Frozen R2.1 sobre/entity oracle and parser integrity regressions. |
| `test_analytical_queries.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=6, PROD=6 | 12 | 0 | 0 | 6 | 0/0 | 0.0 | Tests AQ-001A — aggregate_sum analytical queries (versión reforzada). |
| `test_analytics_execution.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=10 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | Tests del ExecutionAnalyzer — ejecución contratado vs pagado. |
| `test_analytics_orphans.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=18 | 18 | 13 | 0 | 0 | 0/0 | 0.0 | Tests de los analyzers cableados desde builders huérfanos: ranking, temporal, |
| `test_demo_alcalde.py` | ranking_results_presentation | PRODUCT | PROD=9 | 9 | 0 | 0 | 9 | 0/2 | 0.0 | Demo A — Perfil Alcalde (mock E2E, Sprint 3 fase 2). |
| `test_demo_proponente.py` | ranking_results_presentation | PRODUCT | PROD=7 | 7 | 0 | 0 | 7 | 0/2 | 0.0 | Demo B — Perfil Proponente (mock E2E, Sprint 3 fase 2). |
| `test_demo_veedor.py` | ranking_results_presentation | PRODUCT | PROD=7 | 7 | 0 | 0 | 7 | 0/2 | 0.0 | Demo C — Perfil Veedor (mock E2E, Sprint 3 fase 2). |
| `test_dual_dataset_execution.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Dual-dataset Commit 2: ejecución de dos hijos independientes (sin presentar). |
| `test_dual_dataset_presentation.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | Dual-dataset Commit 3: presentación agrupada (pura). |
| `test_dual_dataset_scope.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=8 | 8 | 4 | 0 | 0 | 0/0 | 0.0 | Dual-dataset Commit 1: representación y parsing de dataset_scope. |
| `test_export_xlsx.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_feedback_isolation.py` | ranking_results_presentation | ARCHITECTURE | ARCH=3 | 3 | 0 | 0 | 1 | 0/0 | 0.0 |  |
| `test_feedback_sqlite.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=14 | 14 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_formatter_no_results.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=19 | 19 | 0 | 0 | 0 | 0/0 | 0.0 | Tests tarea 1.4 — mensaje 'sin resultados' amigable con sugerencias. |
| `test_narrator.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=39 | 39 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para app/core/narrator.py |
| `test_orchestrator.py` | ranking_results_presentation | PRODUCT | PROD=8 | 8 | 0 | 0 | 8 | 0/0 | 0.0 |  |
| `test_orchestrator_golden.py` | ranking_results_presentation | PRODUCT | PROD=9 | 9 | 0 | 0 | 9 | 0/0 | 0.0 | Golden contract tests for SecopalWorkflow.run_query. |
| `test_pagination_buffer.py` | ranking_results_presentation | PRODUCT | DOMA=5, PROD=10 | 15 | 0 | 0 | 10 | 0/0 | 0.0 | Paginación A-buffer: cero solapamiento y cero skip entre páginas. |
| `test_pagination_buffer_lifecycle.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=7 | 7 | 0 | 0 | 0 | 0/0 | 0.0 | E8B.2: contrato persistido y ciclo de vida de PaginationBuffer. |
| `test_pagination_transparency.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_pilot_real.py` | ranking_results_presentation | AUDIT_HISTORICAL | AUDI=50 | 50 | 0 | 0 | 0 | 0/0 | 0.0 | Pruebas end-to-end offline del piloto real Adulto Mayor (TG-P2D1, endurecido). |
| `test_rank_results.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Commit 6D: etapa formal de ranking ANTES de format_response. |
| `test_rated_eval.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 | Guardia de regresión sobre queries REALES calificadas por usuarios. |
| `test_relevance.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=10 | 10 | 0 | 0 | 0 | 0/0 | 0.0 | Tests RELEVANCE-PHRASE-001 — BM25 + shingle bigram scoring. |
| `test_response_policy.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=19 | 19 | 0 | 0 | 0 | 0/0 | 0.0 | Tests: Bloque 3 — Política de respuesta asesora. |
| `test_result_grouping.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=49 | 49 | 7 | 0 | 0 | 0/0 | 0.0 | Bloque G — contrato de la agrupación neutral de resultados. |
| `test_result_identity.py` | ranking_results_presentation | DOMAIN_INVARIANT | ARCH=1, DOMA=73 | 74 | 56 | 0 | 0 | 0/0 | 0.0 | RG-1 — contrato de la autoridad neutral y dormida de identidad. |
| `test_retrieval_text.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=8 | 8 | 2 | 0 | 0 | 0/0 | 0.0 | Invariantes puras de normalize_retrieval_text (Commit 2). |
| `test_sprint2a_canonical_smoke.py` | ranking_results_presentation | AUDIT_HISTORICAL | AUDI=6 | 6 | 5 | 0 | 6 | 0/0 | 0.0 | B4.5 — Canonical smoke test covering the 5 HIGH backlog bugs E2E. |
| `test_sprint6_integration.py` | ranking_results_presentation | AUDIT_HISTORICAL | AUDI=4 | 4 | 0 | 0 | 4 | 0/0 | 0.0 | Integration tests E2E — Sprint 6 features: polysemic, ecosystem, BM25. |
| `test_torture_queries.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=34, PROD=4 | 38 | 3 | 0 | 4 | 0/0 | 0.0 | Torture Matrix QA-001 — tests parametrizados para bugs semánticos y conversacion |
| `test_workflow_persistence.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=13 | 13 | 0 | 0 | 0 | 0/0 | 0.0 | Unit tests for app.core.workflow.persistence. |
| `test_workflow_results.py` | ranking_results_presentation | DOMAIN_INVARIANT | DOMA=22 | 22 | 9 | 0 | 0 | 0/0 | 0.0 | Unit tests for app.core.workflow.results. |
| `test_candidate_limit_wiring.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Commit 6A: candidate_limit (fetch) separado de display_limit (mostrado=25). |
| `test_count_lazy.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | Tests COUNT-QUERY-LATENCY-001 — query-first, count-lazy behaviour. |
| `test_count_nonblocking.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=11 | 11 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 6 H1 — el count no bloquea la entrega de filas; nunca fabrica EMPTY/0. |
| `test_direct_lookup.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=78 | 78 | 30 | 0 | 0 | 0/0 | 0.0 | Tests for app.core.direct_lookup — Feature A1: direct process/contract lookup by |
| `test_direct_lookup_entity_scope.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=47 | 47 | 26 | 0 | 0 | 0/0 | 0.0 | Etapa 1: lookup por referencia acotado por qualifier de entidad. |
| `test_h4_fallback.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=13, PROD=3 | 16 | 0 | 0 | 3 | 0/0 | 0.0 | H4 — fallback degradado sin ORDER BY (gate semántico + presupuesto 8/3/4). |
| `test_instrumentation.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=10, PROD=2 | 12 | 0 | 0 | 2 | 0/0 | 0.0 | Etapa 5 — telemetría observacional (timings + HTTP), sin cambiar comportamiento. |
| `test_observability_traces.py` | soql_source_execution | PRODUCT | DOMA=1, PROD=1 | 2 | 0 | 0 | 1 | 0/0 | 0.0 |  |
| `test_observer.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=22 | 22 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para observer.py — Tarea 2.6. |
| `test_observer_partial.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=3 | 3 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para resiliencia parcial del observer (B5). |
| `test_observer_signals.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=9 | 9 | 0 | 0 | 0 | 0/0 | 0.0 | Tests Observer 2.0 — nuevos signals enriquecidos. |
| `test_outcome.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=20 | 20 | 10 | 0 | 0 | 0/0 | 0.0 | Etapa 4 — modelo bidimensional outcome + outcome_reason. |
| `test_parallel_secop_execution.py` | soql_source_execution | PRODUCT | PROD=2 | 2 | 0 | 0 | 2 | 0/0 | 0.0 |  |
| `test_recall_audit.py` | soql_source_execution | AUDIT_HISTORICAL | AUDI=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 3 — auditoría diferencial de recall (predicate waterfall por existencia). |
| `test_safe_soql.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=21 | 21 | 6 | 0 | 0 | 0/0 | 0.0 | SAFE-SOQL-001 — SoQLBuilder must refuse to emit accidental global queries. |
| `test_secop1_procesos_field_mapping.py` | soql_source_execution | UNKNOWN | UNKN=11 | 11 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de field mapping para SECOP I Procesos (f789-7hwg) → NormalizedHistoricalP |
| `test_secop_access.py` | soql_source_execution | DOMAIN_INVARIANT | ARCH=1, DOMA=37 | 38 | 22 | 0 | 0 | 0/0 | 0.0 | RADAR-SECOP-ACCESS-V2 — una oportunidad nunca queda sin vía a la fuente. |
| `test_secop_integrado_field_mapping.py` | soql_source_execution | UNKNOWN | UNKN=13 | 13 | 0 | 0 | 0 | 0/0 | 0.0 | Tests de field mapping para SECOP Integrado (rpmr-utcd) → NormalizedContractReco |
| `test_socrata_retry.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | Tests para retry/backoff en SecopClient (B5). |
| `test_socrata_transport_classic.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=12 | 12 | 3 | 0 | 0 | 0/0 | 0.0 | T1A — la costura de transporte no cambia el comportamiento clásico. |
| `test_socrata_transport_soda3.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=24 | 24 | 9 | 0 | 0 | 0/0 | 0.0 | T1B — contrato HTTP del transporte SODA3 y sus errores tipados. |
| `test_soda3_transport_selection.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=13, PROD=9 | 22 | 4 | 0 | 9 | 0/0 | 0.0 | Selección de transporte por configuración tras el cutover SODA3. |
| `test_soql_builder.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=30 | 30 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_soql_builder_phrase.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=5 | 5 | 0 | 0 | 0 | 0/0 | 0.0 | Commit 5: el builder consume objeto_phrases con un WHERE recall-first. |
| `test_soql_count_select_consistency.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_timeout_handling.py` | soql_source_execution | PRODUCT | PROD=3 | 3 | 0 | 0 | 3 | 0/0 | 0.0 | Tests para manejo de timeout en consultas SECOP pesadas. |
| `test_timeout_transparency.py` | soql_source_execution | PRODUCT | PROD=1 | 1 | 0 | 0 | 1 | 0/0 | 0.0 |  |
| `test_url_fallback_channel_parity.py` | soql_source_execution | DOMAIN_INVARIANT | ARCH=2, DOMA=26 | 28 | 20 | 0 | 0 | 0/0 | 0.0 | ADR-027 — paridad de canales para la vía a SECOP. |
| `test_url_repair.py` | soql_source_execution | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 | Tests for secop_client._normalize_row URL repair (FIX-URL-FREEFORM-001). |
| `test_current_opportunity_retrieval.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=22 | 22 | 0 | 0 | 0 | 0/0 | 0.0 | O1B — current opportunity retrieval policy (tests RED first). |
| `test_deadline.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=12 | 12 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 6 H2 — deadline total del turno + fetch acotado por presupuesto restante. |
| `test_e2e_estado_followup_bug.py` | time_state_opportunity | PRODUCT | PROD=1 | 1 | 0 | 0 | 1 | 0/0 | 0.0 | Test E2E: Bug follow-up estado + objeto contaminado. |
| `test_estado_families_integrity.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=2 | 2 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_llm_handler_state_policy.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=8 | 8 | 0 | 0 | 0 | 0/0 | 0.0 |  |
| `test_natural_alpha_deadline.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=7 | 7 | 6 | 0 | 0 | 0/0 | 0.0 |  |
| `test_opportunity_queries.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=16, PROD=2 | 18 | 0 | 0 | 2 | 0/3 | 0.0 | OPP-002: opportunity_search policy tests (real functions, will be red until impl |
| `test_opportunity_scope_policy.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=24 | 24 | 6 | 0 | 0 | 0/0 | 0.0 | Unit tests for the pure opportunity scope policy (OPP-SCOPE-001). |
| `test_opportunity_stage.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=24 | 24 | 0 | 0 | 0 | 0/0 | 0.0 | Tests del clasificador puro OpportunityStage (OPP-STAGE-001, Fase 2A). |
| `test_opportunity_stage_shadow.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=10, PROD=5 | 15 | 0 | 0 | 5 | 0/0 | 0.0 | Fase 2B — shadow del clasificador de etapa de oportunidad. |
| `test_plan_query_implicit_opportunity.py` | time_state_opportunity | PRODUCT | PROD=6 | 6 | 5 | 0 | 6 | 0/0 | 0.0 | B4.2 — E2E tests for P4 (implicit_opportunity → opportunity_search). |
| `test_query_plan_opportunity_scope.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=13 | 13 | 0 | 0 | 0 | 0/0 | 0.0 | Integration tests for P15 (opportunity scope) inside compute_query_plan. |
| `test_reorder_active_first.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | DOGFOOD-SOQL-006: boost 'activos primero' en Python tras quitar CASE WHEN del So |
| `test_state_catalog.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=6 | 6 | 0 | 0 | 0 | 0/0 | 0.0 | Tests STATE-CATALOG-INCOMPLETE-001 — catalog expansion + query matching. |
| `test_state_priority_001.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | B3 — STATE-PRIORITY-001: conjugate both state dimensions for procesos. |
| `test_temporal_relative_characterization.py` | time_state_opportunity | AUDIT_HISTORICAL | AUDI=15 | 15 | 0 | 0 | 0 | 0/0 | 0.0 | Etapa 4 — Verificación: fechas relativas semana/día producen rangos correctos. |
| `test_time_semantics_v2.py` | time_state_opportunity | DOMAIN_INVARIANT | DOMA=39 | 39 | 31 | 0 | 0 | 0/0 | 0.0 |  |
| `test_firmado_signed_semantics.py` | truth_layer | DOMAIN_INVARIANT | DOMA=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | FIRMADO-NO-ESTADO — "firmado" ⇔ fecha_de_firma, no un estado_contrato. |
| `test_process_operational_state.py` | truth_layer | DOMAIN_INVARIANT | DOMA=33 | 33 | 0 | 0 | 0 | 0/0 | 0.0 | M2/PROCESS-OP-STATE-001 — estado operativo del proceso en la tarjeta. |
| `test_published_temporal_value.py` | truth_layer | DOMAIN_INVARIANT | ARCH=1, DOMA=22 | 23 | 21 | 0 | 0 | 0/0 | 0.0 | The public temporal boundary, and the two runtime consumers that use it. |
| `test_streamlit_operational_state.py` | truth_layer | DOMAIN_INVARIANT | DOMA=7 | 7 | 0 | 0 | 0 | 0/0 | 0.0 | M2.1: el renderer Streamlit consume el resumen operativo ya formateado. |
| `test_suspended_contract_semantics.py` | truth_layer | DOMAIN_INVARIANT | DOMA=15, PROD=2 | 17 | 11 | 0 | 2 | 0/0 | 0.0 | INV-STATE-SUSPENDED-IS-DATASET-AWARE. |
| `test_truth_layer_contract_changes.py` | truth_layer | DOMAIN_INVARIANT | DOMA=11 | 11 | 3 | 0 | 0 | 0/0 | 0.0 | T1B-3 contract change tests. |
| `test_truth_layer_contract_lifecycle.py` | truth_layer | DOMAIN_INVARIANT | ARCH=4, DOMA=9 | 13 | 0 | 0 | 0 | 0/0 | 0.0 | T1B-3 contract lifecycle aggregation and transverse tests. |
| `test_truth_layer_contract_liquidation.py` | truth_layer | DOMAIN_INVARIANT | DOMA=14 | 14 | 3 | 0 | 0 | 0/0 | 0.0 | T1B-3 contract liquidation tests. |
| `test_truth_layer_contract_performance.py` | truth_layer | AUDIT_HISTORICAL | AUDI=17 | 17 | 11 | 0 | 0 | 0/0 | 0.0 | T1B-3 contract performance tests. |
| `test_truth_layer_contract_signature.py` | truth_layer | DOMAIN_INVARIANT | DOMA=21 | 21 | 13 | 0 | 0 | 0/0 | 0.0 | T1B-3 contract signature tests. |
| `test_truth_layer_evidence.py` | truth_layer | DOMAIN_INVARIANT | ARCH=6, DOMA=90 | 96 | 23 | 0 | 0 | 0/0 | 0.0 | T1B-1 — Evidence / conflicts / quality tests. |
| `test_truth_layer_frame_guard_controls.py` | truth_layer | DOMAIN_INVARIANT | DOMA=9, PROD=5 | 14 | 13 | 0 | 5 | 0/0 | 0.0 | Bound the two B/B.1 hunks before authorizing their parity digests. |
| `test_truth_layer_import_boundary.py` | truth_layer | ARCHITECTURE | ARCH=4 | 4 | 0 | 0 | 0 | 0/0 | 0.0 | Import-safety and architectural boundary gates for dormant Truth Layer M1. |
| `test_truth_layer_models.py` | truth_layer | DOMAIN_INVARIANT | ARCH=1, DOMA=46 | 47 | 25 | 0 | 0 | 0/0 | 0.0 |  |
| `test_truth_layer_observers.py` | truth_layer | DOMAIN_INVARIANT | ARCH=1, DOMA=6 | 7 | 0 | 0 | 0 | 0/0 | 0.0 | Product-foundation tests for the two pure, dormant observer seams. |
| `test_truth_layer_process_lifecycle.py` | truth_layer | DOMAIN_INVARIANT | ARCH=5, DOMA=235 | 240 | 126 | 0 | 0 | 0/0 | 0.0 | T1B-2 — Process stage + resolution tests. |
| `test_truth_layer_public_parity.py` | truth_layer | DOMAIN_INVARIANT | DOMA=14 | 14 | 0 | 0 | 0 | 0/0 | 0.0 | Exact public oracles captured from main before dormant M1 integration. |
| `test_truth_layer_sentinels.py` | truth_layer | DOMAIN_INVARIANT | ARCH=4, DOMA=72 | 76 | 6 | 0 | 0 | 0/0 | 0.0 | T1B-0A — Field Sentinel Semantics tests. |
| `test_truth_layer_temporal.py` | truth_layer | DOMAIN_INVARIANT | ARCH=7, DOMA=84 | 91 | 11 | 0 | 0 | 0/0 | 0.0 | T1B-0C/T1B-4 — temporal validity, relations and the wall-time boundary. |
| `test_truth_layer_visibility.py` | truth_layer | DOMAIN_INVARIANT | ARCH=3, DOMA=39 | 42 | 7 | 0 | 0 | 0/0 | 0.0 | T1B-5 — public visibility resolution. |
