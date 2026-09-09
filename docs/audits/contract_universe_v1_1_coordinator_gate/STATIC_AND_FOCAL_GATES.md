# Gates focales y estáticos

Comandos y stdout/stderr íntegros: `focal_static_literal.json`. Los `.txt` eliminan solo espacios de fin de línea para diff-check. Totales por bloque; no sumar suites que se solapan.

## frozen_after

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_independent.py tests/test_contract_universe_independent_consumers.py tests/test_contract_universe_independent_legacy.py tests/test_contract_universe_independent_safety.py tests/test_module_size_limits.py -v --tb=short
exit_code=0
============================= test session starts ==============================
platform darwin -- Python 3.14.7, pytest-9.0.2, pluggy-1.6.0 -- /Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
cachedir: .pytest_cache
hypothesis profile 'default'
rootdir: /Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-coordinator-gate
configfile: pytest.ini
plugins: cov-7.1.0, hypothesis-6.165.10, anyio-4.13.0
collecting ... collected 43 items

tests/test_contract_universe_independent.py::test_independent_sql_matrix[False-seed0] PASSED [  2%]
tests/test_contract_universe_independent.py::test_independent_sql_matrix[False-seed1] PASSED [  4%]
tests/test_contract_universe_independent.py::test_independent_sql_matrix[True-seed0] PASSED [  6%]
tests/test_contract_universe_independent.py::test_independent_sql_matrix[True-seed1] PASSED [  9%]
tests/test_contract_universe_independent.py::test_object_borradores_is_not_state PASSED [ 11%]
tests/test_contract_universe_independent.py::test_two_states_not_silently_partial[registros cancelados y borradores de la Alcald\xeda de Cartagena] PASSED [ 13%]
tests/test_contract_universe_independent.py::test_two_states_not_silently_partial[registros borradores y cancelados de la Alcald\xeda de Cartagena] PASSED [ 16%]
tests/test_contract_universe_independent.py::test_negated_records_do_not_widen[contratos de la Alcald\xeda de Cartagena sin registros cancelados] PASSED [ 18%]
tests/test_contract_universe_independent.py::test_negated_records_do_not_widen[contratos de la Alcald\xeda de Cartagena no quiero borradores] PASSED [ 20%]
tests/test_contract_universe_independent.py::test_negated_records_do_not_widen[contratos de la Alcald\xeda de Cartagena excluyendo los registros cancelados] PASSED [ 23%]
tests/test_contract_universe_independent.py::test_materialized_fields_are_not_consent[params0] PASSED [ 25%]
tests/test_contract_universe_independent.py::test_materialized_fields_are_not_consent[params1] PASSED [ 27%]
tests/test_contract_universe_independent.py::test_materialized_fields_are_not_consent[params2] PASSED [ 30%]
tests/test_contract_universe_independent.py::test_published_description_not_rewritten[False] PASSED [ 32%]
tests/test_contract_universe_independent.py::test_published_description_not_rewritten[True] PASSED [ 34%]
tests/test_contract_universe_independent.py::test_complete_continuity_and_export[False] PASSED [ 37%]
tests/test_contract_universe_independent.py::test_complete_continuity_and_export[True] PASSED [ 39%]
tests/test_contract_universe_independent.py::test_frame_fingerprint_scope_changes PASSED [ 41%]
tests/test_contract_universe_independent.py::test_direct_record_does_not_leak_to_search PASSED [ 44%]
tests/test_contract_universe_independent.py::test_radar_cancellation_event_and_digest PASSED [ 46%]
tests/test_contract_universe_independent.py::test_admitted_state_independent_of_date PASSED [ 48%]
tests/test_contract_universe_independent_consumers.py::test_explicit_control_with_independent_rows[registros cancelados de la alcaldia de cartagena-Cancelado] PASSED [ 51%]
tests/test_contract_universe_independent_consumers.py::test_explicit_control_with_independent_rows[borradores de la alcaldia de cartagena-Borrador] PASSED [ 53%]
tests/test_contract_universe_independent_consumers.py::test_explicit_control_with_independent_rows[registros enviados al proveedor de la alcaldia de cartagena-enviado Proveedor] PASSED [ 55%]
tests/test_contract_universe_independent_consumers.py::test_explicit_control_with_independent_rows[contratos suspendidos de la alcaldia de cartagena-Suspendido] PASSED [ 58%]
tests/test_contract_universe_independent_consumers.py::test_actor_name_state_word_has_no_universe_authority[False] PASSED [ 60%]
tests/test_contract_universe_independent_consumers.py::test_actor_name_state_word_has_no_universe_authority[True] PASSED [ 62%]
tests/test_contract_universe_independent_consumers.py::test_materialized_dual_children_and_source_names[celebrated-22] PASSED [ 65%]
tests/test_contract_universe_independent_consumers.py::test_materialized_dual_children_and_source_names[source_records-94] PASSED [ 67%]
tests/test_contract_universe_independent_consumers.py::test_dual_raw_explicit_records_not_silently_dropped[False] PASSED [ 69%]
tests/test_contract_universe_independent_consumers.py::test_dual_raw_explicit_records_not_silently_dropped[True] PASSED [ 72%]
tests/test_contract_universe_independent_consumers.py::test_invalid_date_is_not_published_as_a_date PASSED [ 74%]
tests/test_contract_universe_independent_legacy.py::test_real_base_conversation_requires_review PASSED [ 76%]
tests/test_contract_universe_independent_legacy.py::test_real_base_buffer_requires_review PASSED [ 79%]
tests/test_contract_universe_independent_legacy.py::test_real_base_frame_and_authorized_plan_require_review PASSED [ 81%]
tests/test_contract_universe_independent_legacy.py::test_real_base_radar_preserved_and_blocked PASSED [ 83%]
tests/test_contract_universe_independent_safety.py::test_actual_derived_client_keeps_injected_transport[with_timeout] PASSED [ 86%]
tests/test_contract_universe_independent_safety.py::test_actual_derived_client_keeps_injected_transport[derived_for_timeout] PASSED [ 88%]
tests/test_contract_universe_independent_safety.py::test_ledger_rejects_swallowed_aggregate_attempt_outside_workflow[with_timeout] PASSED [ 90%]
tests/test_contract_universe_independent_safety.py::test_ledger_rejects_swallowed_aggregate_attempt_outside_workflow[derived_for_timeout] PASSED [ 93%]
tests/test_contract_universe_independent_safety.py::test_repeated_mutations_require_green_and_product_difference PASSED [ 95%]
tests/test_module_size_limits.py::test_query_plan_under_300_loc PASSED   [ 97%]
tests/test_module_size_limits.py::test_dialogue_policy_under_400_loc PASSED [100%]

============================== 43 passed in 4.98s ==============================
```

## auditor_final_19

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_cu_f21_final_delta.py -q --tb=short
exit_code=0
...................                                                      [100%]
19 passed in 3.88s
```

## f2_2_product_properties

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_f2_2_materialization.py -q --tb=short
exit_code=0
..................................                                       [100%]
34 passed in 12.34s
```

## f2_1_symmetry

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_f2_1_symmetry.py -q --tb=short
exit_code=0
..............................................                           [100%]
46 passed in 4.97s
```

## f2_1_completeness

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_f2_1_completeness.py -q --tb=short
exit_code=0
........................................................................ [ 29%]
........................................................................ [ 58%]
........................................................................ [ 87%]
................................                                         [100%]
248 passed in 30.82s
```

## auditor_findings_now_green

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_cu_v11_audit_findings.py -q --tb=short
exit_code=0
........                                                                 [100%]
8 passed in 1.49s
```

## auditor_rest

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_cu_v11_audit_ports.py tests/test_cu_v11_audit_f1_f5.py tests/test_cu_v11_audit_aprobado.py tests/test_cu_v11_audit_persistence.py tests/test_cu_v11_audit_regressions.py tests/test_cu_v11_audit_properties.py -q --tb=short
exit_code=0
........................................................................ [ 18%]
........................................................................ [ 36%]
........................................................................ [ 55%]
........................................................................ [ 73%]
........................................................................ [ 92%]
..............................                                           [100%]
390 passed in 9.53s
```

## remediation_85

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_remediation.py tests/test_contract_universe_remediation_consumers.py tests/test_contract_universe_remediation_properties.py tests/test_contract_universe_remediation_legacy.py -q --tb=short
exit_code=0
........................................................................ [ 84%]
.............                                                            [100%]
85 passed in 8.83s
```

## author_targets

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_product.py tests/test_contract_universe_consumers.py tests/test_contract_universe_properties.py tests/test_soql_builder.py tests/test_soql_builder_phrase.py tests/test_observer.py tests/test_observer_partial.py tests/test_observer_signals.py tests/test_analytical_queries.py tests/test_actor_profile.py tests/test_actor_profile_scope.py tests/test_actor_profile_render.py tests/test_actor_profile_scope_conversation.py tests/test_contract_invariants.py tests/test_truth_layer_contract_signature.py tests/test_entity_profile_frame_b_semantics.py tests/test_entity_profile_frame_b_admission.py tests/test_entity_profile_frame_catalog_parity.py tests/test_module_size_limits.py tests/test_entity_profile_frame_core.py tests/test_entity_profile_frame_migration.py tests/test_entity_profile_frame_properties.py tests/test_entity_profile_semantic_corpus.py tests/test_buyer_actor_independent_product.py tests/test_buyer_actor_independent_boundaries.py tests/test_entity_profile_source_plan.py tests/test_radar_builder.py tests/test_radar_runner.py tests/test_query_router.py tests/test_estado_families_integrity.py tests/test_pagination_buffer.py tests/test_pagination_buffer_lifecycle.py tests/test_export_xlsx.py tests/test_truth_layer_public_parity.py tests/test_entity_profile_frame_b_integration.py tests/test_entity_profile_frame_b_publication.py tests/test_entity_profile_frame_evidence.py tests/test_entity_profile_frame_b_properties.py tests/test_entity_profile_frame_independent_contracts.py tests/test_entity_profile_workflow.py tests/test_entity_profile_conversation_scope.py tests/test_entity_profile_queries.py -q --tb=short
exit_code=0
........................................................................ [  5%]
........................................................................ [ 11%]
........................................................................ [ 17%]
........................................................................ [ 23%]
........................................................................ [ 29%]
........................................................................ [ 35%]
........................................................................ [ 41%]
........................................................................ [ 47%]
........................................................................ [ 53%]
........................................................................ [ 59%]
........................................................................ [ 65%]
........................................................................ [ 70%]
........................................................................ [ 76%]
........................................................................ [ 82%]
........................................................................ [ 88%]
........................................................................ [ 94%]
.................................................................        [100%]
1217 passed in 67.96s (0:01:07)
```

## corpus

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/run_query_corpus.py --mode all
exit_code=0

Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
```

## ruff

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
exit_code=0
All checks passed!
```

## semgrep

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
exit_code=0


┌──────────────────┐
│ 10 Code Findings │
└──────────────────┘

    app/core/formatter.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          173┆ try:
          174┆     from app.core.relevance import RelevanceScorer
          175┆     field = "objeto_del_contrato" if dataset_id == "jbjy-vk9h" else
               "nombre_del_procedimiento"
          176┆     scored = RelevanceScorer().score_rows(topic_flat, out, field=field)
          177┆     out = [r for r, _ in scored]
          178┆ except Exception:
          179┆     pass  # best-effort, igual que en normal_flow
            ⋮┆----------------------------------------
          718┆ try:
          719┆     median_str = f" (la mediana es {format_cop(vs['median'])})"
          720┆ except Exception:
          721┆     pass

    app/core/response_policy.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          508┆ try:
          509┆     result = narrator.narrate(
          510┆         rows=rows, query=query, channel=channel,
          511┆         strict=True, universe_insights=insights, suggestions=None,
          512┆     )
          513┆     if result and not _contains_numeric_claim(result):
          514┆         return result
          515┆ except Exception:
          516┆     pass
            ⋮┆----------------------------------------
          577┆ try:
          578┆     median_str = f" (la mediana es {format_cop(vs['median'])})"
          579┆ except Exception:
          580┆     pass

    app/core/workflow/actions/execution.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          365┆ try:
          366┆     soql = soql_builder.build(state["dataset_id"], relaxed)
          367┆     count_soql = soql_builder.build_count(state["dataset_id"], relaxed)
          368┆     total, results, _timings, _approx = _query_first_then_count(
          369┆         secop_client, state["dataset_id"], count_soql, soql
          370┆     )
          371┆     if results:
          372┆         return _with_timing(
          373┆             state,
          374┆             "degrade_ms",
             [hid 9 additional lines, adjust with --max-lines-per-finding]

    app/core/workflow/handlers/direct_lookup.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          371┆ try:
          372┆     cards = deps.formatter.to_rows(
          373┆         [dict(r)], ds,
          374┆         {"intent_type": "direct_lookup", "route_reason": "entity_ref_disambiguation"},
          375┆         reference_instant=reference_instant_utc,
          376┆     )
          377┆     if cards:
          378┆         estado = _estado_visible(cards[0]) or estado
          379┆         nota_estado = _estado_raw_nota(cards[0])
          380┆ except Exception:
             [hid 3 additional lines, adjust with --max-lines-per-finding]

    app/core/workflow/handlers/normal_flow.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          171┆ try:
          172┆     from app.core.contextual_suggestions import compute_contextual_filters
          173┆     computed = compute_contextual_filters(
          174┆         raw_results,
          175┆         result["dataset_id"],
          176┆         result.get("resolved_params", {}),
          177┆     )
          178┆     result["contextual_filters"] = computed
          179┆ except Exception:
          180┆     pass  # best-effort, never block the main flow

    app/core/workflow/handlers/pagination.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          186┆ try:
          187┆     display_count = max(1, int(deps.formatter.max_results or 25))
          188┆     if admitted_buffer is not None:
          189┆         buffer = admitted_buffer
          190┆         group = buffer.group
          191┆         fetch_params = dict(buffer.query_params)
          192┆         current_fp = buffer.query_fingerprint
          193┆     elif requested_group:
          194┆         group = requested_group
          195┆         buffer = deps.conv_store.get_pagination_buffer(ctx.chat_id, group, None)
             [hid 171 additional lines, adjust with --max-lines-per-finding]

    app/core/workflow/handlers/radar.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

          114┆ try:
          115┆     total = run_radar(deps, store, radar).total
          116┆ except Exception:
          117┆     pass

    app/core/workflow/handlers/refinement.py
   ❯❯❱ semgrep.except-exception-que-silencia
          ❰❰ Blocking ❱❱
          Un except que se traga la excepcion sin registrar nada. En un sistema cuyo defecto central es
          devolver resultados incorrectos en silencio, esto es justo el patron que impide diagnosticar.
          Registra o re-lanza.

           93┆ try:
           94┆     from app.core.contextual_suggestions import compute_contextual_filters
           95┆     result["contextual_filters"] = compute_contextual_filters(
           96┆         results, ds_id, params,
           97┆     )
           98┆ except Exception:
           99┆     pass



┌─────────────┐
│ Scan Status │
└─────────────┘
  Scanning 348 files tracked by git with 3 Code rules:
  Scanning 331 files with 3 python rules.


┌──────────────┐
│ Scan Summary │
└──────────────┘
✅ Scan completed successfully.
 • Findings: 10 (10 blocking)
 • Rules run: 3
 • Targets scanned: 331
 • Parsed lines: ~100.0%
 • Scan skipped:
   ◦ Files larger than  files 1.0 MB: 1
 • Scan was limited to files tracked by git
 • For a detailed list of skipped files and lines, run semgrep with the --verbose flag
Ran 3 rules on 331 files: 10 findings.
```

## lint_core

```text
make lint-core
exit_code=0
app/core/ limpio
```

## size

```text
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest tests/test_module_size_limits.py -q
exit_code=0
..                                                                       [100%]
2 passed in 0.11s
```

## diff_check

```text
git diff --check
exit_code=0
```
