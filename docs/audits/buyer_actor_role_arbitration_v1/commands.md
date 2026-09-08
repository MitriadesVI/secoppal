# Comandos y reproducción

Todos desde /Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-arbitration-v1.
El worktree no contiene .venv; se usa el entorno canónico conservando cwd aislado.

```sh
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off --json
make lint-core
git diff --check
```

Focal final (manifiesto completo, sin filtrado por test):

```sh
SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED=false /Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.buyer_actor_offline_guard tests/test_actor_alias_index.py tests/test_actor_alias_source.py tests/test_actor_candidate_ux.py tests/test_actor_conversation.py tests/test_actor_conversation_corpus.py tests/test_actor_dogfood_hardening_v2.py tests/test_actor_dogfood_hardening_v2_properties.py tests/test_actor_exact_prefix.py tests/test_actor_exact_prefix_properties.py tests/test_actor_exact_prefix_workflow.py tests/test_actor_flag_off.py tests/test_actor_geography_precedence.py tests/test_actor_hardening_v2_1_properties.py tests/test_actor_hardening_v2_1_remediation.py tests/test_actor_identity.py tests/test_actor_identity_sentinels.py tests/test_actor_index_isolation.py tests/test_actor_mention_completeness.py tests/test_actor_name_properties.py tests/test_actor_name_resolver.py tests/test_actor_pending_document_selection.py tests/test_actor_prefix_adjudicator.py tests/test_actor_profile.py tests/test_actor_profile_render.py tests/test_actor_profile_scope.py tests/test_actor_profile_scope_conversation.py tests/test_actor_resolution_benchmark.py tests/test_actor_resolution_orchestration.py tests/test_actor_routing.py tests/test_actor_source_contract.py tests/test_actor_typed_projection.py tests/test_buyer_actor_role_arbitration_v1.py tests/test_buyer_actor_role_controls_v1.py tests/test_buyer_actor_role_local_v1.py tests/test_buyer_actor_role_properties_v1.py tests/test_buyer_candidate_execution_limit.py tests/test_buyer_execution_truth.py tests/test_buyer_group_analytics_truth.py tests/test_buyer_group_capacity_evidence.py tests/test_buyer_intent_sdis.py tests/test_buyer_territory_tail.py tests/test_buyer_unit_integrity_p02.py tests/test_compound_buyer_capture_r2a1.py tests/test_declared_buyer_groups.py tests/test_entity_profile_frame_b_admission.py tests/test_entity_profile_frame_b_integration.py tests/test_entity_profile_frame_b_properties.py tests/test_entity_profile_frame_b_publication.py tests/test_entity_profile_frame_b_semantics.py tests/test_entity_profile_frame_b_utility.py tests/test_entity_resolver.py tests/test_entity_resolver_fallback.py tests/test_generic_buyer_punctuation.py tests/test_module_size_limits.py tests/test_money_v2_invariants.py tests/test_negation_binding.py tests/test_negation_lifecycle.py tests/test_negation_workflow.py tests/test_qualified_buyer_fail_closed.py tests/test_query_router.py tests/test_query_router_source_occurrence.py tests/test_root_buyer_evidence_r1d.py tests/test_v12b1_actor_port.py tests/test_workflow_architecture.py tests/test_workflow_persistence.py tests/test_workflow_results.py -q --tb=short -ra
```

Frame ON + evidencia + lectura local expresamente acotada:

```sh
SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED=true BUYER_ACTOR_EVIDENCE=docs/audits/buyer_actor_role_arbitration_v1/after BUYER_ACTOR_LOCAL_INDEX=/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite /Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.buyer_actor_offline_guard tests/test_actor_resolution_orchestration.py tests/test_actor_conversation.py tests/test_actor_pending_document_selection.py tests/test_actor_exact_prefix_workflow.py tests/test_actor_candidate_ux.py tests/test_actor_dogfood_hardening_v2.py tests/test_actor_hardening_v2_1_remediation.py tests/test_buyer_actor_role_arbitration_v1.py tests/test_buyer_actor_role_controls_v1.py tests/test_buyer_actor_role_properties_v1.py tests/test_buyer_actor_role_local_v1.py tests/test_entity_profile_frame_b_admission.py tests/test_entity_profile_frame_b_utility.py -q --tb=short -ra
```

El corpus se ejecutó con scripts/run_query_corpus.py --mode all, Actor true, Frame false e índice ausente; SECOP_FEEDBACK_PATH, SECOP_FEEDBACK_DB_PATH y CONVERSATION_STORE_DIR apuntaron a un TemporaryDirectory propio. El runner usa parser/merger reales, no transporte de fuentes.

Histórico: git archive de cada SHA hacia un TemporaryDirectory independiente (rutas en base-export.txt / pre-frame-export.txt). Se copió únicamente el harness de las dos semillas, con getattr para reportar Frame ausente; no runtime ni bases SQLite. El builder de cada versión construyó su esquema de filas sintéticas. Se ejecutó el test_seeds con la fecha 2026-09-08. BASE: 20 combinaciones. Pre-Frame: las 10 combinaciones OFF, seleccionadas explícitamente con -k off porque la versión no tiene Frame. Los resultados rojos son la reproducción, no un gate verde.

El único protocolo de fuente simulado es SECOP; también se deshabilita narración externa. QueryRouter, EntityResolver, admisión, acción Actor, builder SQLite de aliases, resolver y SoQLBuilder son reales. El plugin tests.buyer_actor_offline_guard falla si algún test intenta red externa.
