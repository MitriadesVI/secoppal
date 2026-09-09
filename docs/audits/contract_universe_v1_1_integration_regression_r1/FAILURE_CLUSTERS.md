# Residuo de los 116 fallos

Resultado definitivo con el mismo adaptador de fechas del coordinator: **18 passed / 98 failed**.

26 clusters por archivo/función parametrizada. Se ejecutó un representante idéntico por cluster en main c52d7ed y en R1: **26 passed en main / 26 failed en R1**. test_identity.json confirma igualdad byte a byte de los archivos de test. No se ejecutó global sobre main.

Clasificación A según §11: PASS main / FAIL candidato. No significa 26 bugs productivos distintos: varios son goldens o fixtures que necesitan adjudicación. No hay representantes B; la causa raíz de algunas diferencias permanece sin adjudicar, aunque la comparación A es concluyente.

Hay mecanismos independientes: representación/duplicación de estados, guards de consumidores temporales, goldens de SQL/fingerprint y admisión de exportaciones. Se detiene la ampliación conforme a §11. Ninguno se repara, repinea ni exime aquí.

La primera ejecución sin adaptador se conserva con prefijo without_date_plugin_; no es la autoridad del reporte final. La repetición con adaptador produjo los mismos conteos. El plugin sólo registra funciones SQLite de fechas y no cambia política ni expectativas.

## C01 — Actor / nombre con documento (1 fallos)

Archivo/función: `tests.test_actor_routing::test_inv_actor_mention_atomicity_name_plus_document_cannot_leak`.

Representante: `tests/test_actor_routing.py::test_inv_actor_mention_atomicity_name_plus_document_cannot_leak[qu\xe9 ha contratado ASILO DE SAN ANTONIO con NIT 890.102.896-1]`.

**A — main PASS / R1 FAIL.** objeto contiene asilo; viola atomicidad de mención. No se adjudica ni repara aquí.

Mensaje observado:
```text
tests/test_actor_routing.py:186: in test_inv_actor_mention_atomicity_name_plus_document_cannot_leak
    assert params.get("objeto") is None
E   AssertionError: assert ['asilo'] is None
E    +  where ['asilo'] = <built-in method get of dict object at 0x1134a6500>('objeto')
E    +    where <built-in method get of dict object at 0x1134a6500> = {'actor_conversation_construction': 'what_x_did_with_y', 'actor_conversation_operation': 'SET_ACTOR', 'actor_resolutio...erted_name': 'asilo de san antonio', 'base_number': '890102896', 'check_digit': '1', 'document_type': 'NIT', ...}, ...}.get
```

## C02 — Actor / nombre sin resolver (1 fallos)

Archivo/función: `tests.test_actor_routing::test_inv_no_topic_fallback_an_unresolved_name_does_not_fall_into_objeto`.

Representante: `tests/test_actor_routing.py::test_inv_no_topic_fallback_an_unresolved_name_does_not_fall_into_objeto[qu\xe9 ha contratado FUNSOCOM]`.

**A — main PASS / R1 FAIL.** objeto contiene funsocom; falla no-topic-fallback. No se adjudica ni repara aquí.

Mensaje observado:
```text
tests/test_actor_routing.py:426: in test_inv_no_topic_fallback_an_unresolved_name_does_not_fall_into_objeto
    assert not (tokens & {"fundacaribe", "funsocom"})
E   AssertionError: assert not ({'funsocom'} & {'fundacaribe', 'funsocom'})
```

## C03 — Perfil comprador / resultados (1 fallos)

Archivo/función: `tests.test_beta_entity_actor_conversation::test_entity_profile_galapa_does_not_report_false_zero`.

Representante: `tests/test_beta_entity_actor_conversation.py::test_entity_profile_galapa_does_not_report_false_zero`.

**A — main PASS / R1 FAIL.** No se observa el total esperado en respuesta; fixture deja estado sin poblar. Compatibilidad del fixture requiere decisión, no inferir fallo de SECOP real.

Mensaje observado:
```text
tests/test_beta_entity_actor_conversation.py:80: in test_entity_profile_galapa_does_not_report_false_zero
    assert "Se observan 0 contratos" not in result["response"]
E   AssertionError: assert 'Se observan 0 contratos' not in 'MUNICIPIO D...s recientes.'
E
E     'Se observan 0 contratos' is contained here:
E       ocesos]
E       • Se observan 0 contratos por $0. [SECOP II Contratos]
E       • Sus modalidades de proceso más frecuentes fueron Contratación directa (100%). [SECOP II Procesos]
E       • Proceso reciente: OBRA PÚBLICA. [SECOP II Procesos]
E
E       Puedes pedirme sus proveedores, modalidades, evolución o contratos recientes.
```

## C04 — Perfil comprador / continuidad (4 fallos)

Archivo/función: `tests.test_beta_entity_actor_conversation::test_entity_profile_supplier_followup_keeps_buyer`.

Representante: `tests/test_beta_entity_actor_conversation.py::test_entity_profile_supplier_followup_keeps_buyer[cuales son sus mayores proveedores?-contratos]`.

**A — main PASS / R1 FAIL.** route_reason heuristic_only en vez de entity_profile_suppliers; no se atribuye automáticamente a pending Actor.

Mensaje observado:
```text
tests/test_beta_entity_actor_conversation.py:108: in test_entity_profile_supplier_followup_keeps_buyer
    assert result["route_reason"] == "entity_profile_suppliers"
E   AssertionError: assert 'heuristic_only' == 'entity_profile_suppliers'
E
E     - entity_profile_suppliers
E     + heuristic_only
```

## C05 — Buyer / SQL ejecutado (20 fallos)

Archivo/función: `tests.test_buyer_actor_role_arbitration_v1::test_seeds`.

Representante: `tests/test_buyer_actor_role_arbitration_v1.py::test_seeds[cartagena-0-off]`.

**A — main PASS / R1 FAIL.** source_rows ausente. Fixture tiene controles textuales sobre SQL y estado Firmado; no neutralizar política para recuperar filas.

Mensaje observado:
```text
tests/test_buyer_actor_role_arbitration_v1.py:163: in test_seeds
    assert not trace.get("violations"), trace
E   AssertionError: {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...9a49d7c95b01c944adf4081bd9dc5f4d"}
E     ', ''], 'query': 'contratos de alcaldia de cartagena por mas de 500 millones', ...}
E   assert not ['missing amount']
E    +  where ['missing amount'] = <built-in method get of dict object at 0x11ce666c0>('violations')
E    +    where <built-in method get of dict object at 0x11ce666c0> = {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...9a49d7c95b01c944adf4081bd9dc5f4d"}\n', ''], 'query': 'contratos de alcaldia de cartagena por mas de 500 millones', ...}.get
```

## C06 — Buyer / recarga y año (4 fallos)

Archivo/función: `tests.test_buyer_actor_role_controls_v1::test_reload_buyer_year_more`.

Representante: `tests/test_buyer_actor_role_controls_v1.py::test_reload_buyer_year_more[cartagena-False]`.

**A — main PASS / R1 FAIL.** Respuesta no conserva buyer visible. Depende del fixture de SQL del carril Buyer; requiere adjudicación independiente.

Mensaje observado:
```text
tests/test_buyer_actor_role_controls_v1.py:29: in test_reload_buyer_year_more
    assert not trace.get("violations"), trace
E   AssertionError: {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...BULAR', 'analytical_intent': '', 'analytical_response': '', 'clarification_reason': '', ...}, 'source_rows': [[]], ...}
E   assert not ['missing amount']
E    +  where ['missing amount'] = <built-in method get of dict object at 0x11c76d680>('violations')
E    +    where <built-in method get of dict object at 0x11c76d680> = {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...BULAR', 'analytical_intent': '', 'analytical_response': '', 'clarification_reason': '', ...}, 'source_rows': [[]], ...}.get
```

## C07 — Buyer / variantes (8 fallos)

Archivo/función: `tests.test_buyer_actor_role_controls_v1::test_orthogonal_variants`.

Representante: `tests/test_buyer_actor_role_controls_v1.py::test_orthogonal_variants[CONTRATOS DE LA ALCALD\xcdA DE CARTAGENA por mas de 500 millones-False]`.

**A — main PASS / R1 FAIL.** source_rows ausente; misma superficie de fixture y controles del carril Buyer.

Mensaje observado:
```text
tests/test_buyer_actor_role_controls_v1.py:85: in test_orthogonal_variants
    assert not trace.get("violations"), trace
E   AssertionError: {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...BULAR', 'analytical_intent': '', 'analytical_response': '', 'clarification_reason': '', ...}, 'source_rows': [[]], ...}
E   assert not ['missing amount']
E    +  where ['missing amount'] = <built-in method get of dict object at 0x11c5025c0>('violations')
E    +    where <built-in method get of dict object at 0x11c5025c0> = {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...BULAR', 'analytical_intent': '', 'analytical_response': '', 'clarification_reason': '', ...}, 'source_rows': [[]], ...}.get
```

## C08 — Buyer / catálogo (12 fallos)

Archivo/función: `tests.test_buyer_actor_role_controls_v1::test_existing_catalog_families`.

Representante: `tests/test_buyer_actor_role_controls_v1.py::test_existing_catalog_families[False-contratos de -alcaldia de galapa]`.

**A — main PASS / R1 FAIL.** source_rows ausente; misma superficie de fixture y controles del carril Buyer.

Mensaje observado:
```text
tests/test_buyer_actor_role_controls_v1.py:108: in test_existing_catalog_families
    assert not trace.get("violations"), trace
E   AssertionError: {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...et_explicit': True, ...}, 'query': 'contratos de alcaldia de galapa por mas de 500 millones', 'source_rows': [[]], ...}
E   assert not ['missing amount']
E    +  where ['missing amount'] = <built-in method get of dict object at 0x1133fe000>('violations')
E    +    where <built-in method get of dict object at 0x1133fe000> = {'effective_config': {'actor': True, 'frame': False, 'index': '/private/var/folders/md/4k9ytrcn3tnb63chr970lyb00000gn/...et_explicit': True, ...}, 'query': 'contratos de alcaldia de galapa por mas de 500 millones', 'source_rows': [[]], ...}.get
```

## C09 — SQL / proyección exacta (5 fallos)

Archivo/función: `tests.test_buyer_unit_integrity_p02::test_verified_buyer_unit_soql_uses_exact_source_name`.

Representante: `tests/test_buyer_unit_integrity_p02.py::test_verified_buyer_unit_soql_uses_exact_source_name[jbjy-vk9h-FONDO FINANCIERO DISTRITAL DE SALUD]`.

**A — main PASS / R1 FAIL.** Golden de SQL completo no contiene predicado de universo. Igualdad de buyer no demuestra por sí sola regresión productiva.

Mensaje observado:
```text
tests/test_buyer_unit_integrity_p02.py:316: in test_verified_buyer_unit_soql_uses_exact_source_name
    assert query == f"SELECT count(*) WHERE {field} = '{expected}'"
E   assert "SELECT count...AL DE SALUD.'" == "SELECT count...AL DE SALUD.'"
E
E     - SELECT count(*) WHERE nombre_entidad = 'FONDO FINANCIERO DISTRITAL DE SALUD.'
E     + SELECT count(*) WHERE (upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', 'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) AND nombre_entidad = 'FONDO FINANCIERO DISTRITAL DE SALUD.'
```

## C10 — Perfil / alcance declarado (6 fallos)

Archivo/función: `tests.test_entity_profile_remediation_properties::test_p17_every_material_filter_in_sql_is_declared_in_the_scope`.

Representante: `tests/test_entity_profile_remediation_properties.py::test_p17_every_material_filter_in_sql_is_declared_in_the_scope[contratos de mantenimiento excepto interventoria-None]`.

**A — main PASS / R1 FAIL.** El SQL contiene estado_contrato pero scope declarado no dice Estado actual. Requiere adjudicación de cómo declarar el universo por defecto.

Mensaje observado:
```text
tests/test_entity_profile_remediation_properties.py:214: in test_p17_every_material_filter_in_sql_is_declared_in_the_scope
    assert visible.casefold() in declared.casefold(), (marker, declared)
E   AssertionError: ('estado_contrato', 'Tema: mantenimiento, excepto interventoria.')
E   assert 'estado actual' in 'tema: mantenimiento, excepto interventoria.'
E    +  where 'estado actual' = <built-in method casefold of str object at 0x1126c3cf0>()
E    +    where <built-in method casefold of str object at 0x1126c3cf0> = 'Estado actual'.casefold
E    +  and   'tema: mantenimiento, excepto interventoria.' = <built-in method casefold of str object at 0x11ca2e9d0>()
E    +    where <built-in method casefold of str object at 0x11ca2e9d0> = 'Tema: mantenimiento, excepto interventoria.'.casefold
```

## C11 — Estado / representación (1 fallos)

Archivo/función: `tests.test_firmado_signed_semantics::test_firmados_mas_estado_explicito_usa_el_estado`.

Representante: `tests/test_firmado_signed_semantics.py::test_firmados_mas_estado_explicito_usa_el_estado`.

**A — main PASS / R1 FAIL.** estado=Cerrado esperado; apply_request transporta estado_contrato y elimina estado. Contrato de representación diferente.

Mensaje observado:
```text
tests/test_firmado_signed_semantics.py:59: in test_firmados_mas_estado_explicito_usa_el_estado
    assert cerrados.get("estado") == "Cerrado"
E   AssertionError: assert None == 'Cerrado'
E    +  where None = <built-in method get of dict object at 0x112661c40>('estado')
E    +    where <built-in method get of dict object at 0x112661c40> = {'contract_universe': 'celebrated', 'contract_universe_version': 'contract-universe-1.1', 'dataset': 'contratos', 'dataset_explicit': True, ...}.get
```

## C12 — Followup / trace (1 fallos)

Archivo/función: `tests.test_followup_mutations::test_change_dataset_cleans_incompatible_fields_before_apply`.

Representante: `tests/test_followup_mutations.py::test_change_dataset_cleans_incompatible_fields_before_apply`.

**A — main PASS / R1 FAIL.** accepted_fields incluye metadata de universo. No se adjudica la neutralidad de trazas en este arreglo.

Mensaje observado:
```text
tests/test_followup_mutations.py:98: in test_change_dataset_cleans_incompatible_fields_before_apply
    assert trace["accepted_fields"] == ["dataset", "dataset_explicit"]
E   AssertionError: assert ['contract_un...set_explicit'] == ['dataset', '...set_explicit']
E
E     At index 0 diff: 'contract_universe' != 'dataset'
E     Left contains 2 more items, first extra item: 'dataset'
E     Use -v to get more diff
```

## C13 — Estado / frame (2 fallos)

Archivo/función: `tests.test_invariants_characterization::test_signed_closed_contracts_preserve_real_closed_state`.

Representante: `tests/test_invariants_characterization.py::test_signed_closed_contracts_preserve_real_closed_state[False]`.

**A — main PASS / R1 FAIL.** estado_family ausente tras plan. Mecanismo de normalización/representación de estado diferente al clasificador documental.

Mensaje observado:
```text
tests/test_invariants_characterization.py:76: in test_signed_closed_contracts_preserve_real_closed_state
    assert snap["after_plan"]["estado_family"] == "contrato_cerrado"
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   KeyError: 'estado_family'
```

## C14 — SQL / conjunto vacío (1 fallos)

Archivo/función: `tests.test_modality_pipeline_integration::test_dual_resolution_keeps_id_and_each_child_projects_independently`.

Representante: `tests/test_modality_pipeline_integration.py::test_dual_resolution_keeps_id_and_each_child_projects_independently`.

**A — main PASS / R1 FAIL.** Golden espera 1=0 literal, ahora tiene predicado de universo AND 1=0.

Mensaje observado:
```text
tests/test_modality_pipeline_integration.py:348: in test_dual_resolution_keeps_id_and_each_child_projects_independently
    assert contract_where == "1=0"
E   assert "(upper(unacc...DO')) AND 1=0" == '1=0'
E
E     - 1=0
E     + (upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', 'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) AND 1=0
```

## C15 — SQL / conjunto vacío (1 fallos)

Archivo/función: `tests.test_modality_pipeline_integration::test_soql_canonical_clauses_zero_set_unknown_id_and_global_guard`.

Representante: `tests/test_modality_pipeline_integration.py::test_soql_canonical_clauses_zero_set_unknown_id_and_global_guard`.

**A — main PASS / R1 FAIL.** Golden espera WHERE 1=0 literal dentro del SQL.

Mensaje observado:
```text
tests/test_modality_pipeline_integration.py:394: in test_soql_canonical_clauses_zero_set_unknown_id_and_global_guard
    assert " WHERE 1=0 " in zero
E   assert ' WHERE 1=0 ' in "SELECT id_contrato, referencia_del_contrato, nombre_entidad, departamento, ciudad, objeto_del_contrato, descripcion_d... 'SUSPENDIDO', 'PRORROGADO')) AND 1=0 ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50"
```

## C16 — Fingerprint / goldens (9 fallos)

Archivo/función: `tests.test_modality_pipeline_integration::test_no_modality_pre_post_golden_parity`.

Representante: `tests/test_modality_pipeline_integration.py::test_no_modality_pre_post_golden_parity[contratos de alimentaci\xf3n-golden0]`.

**A — main PASS / R1 FAIL.** El universo versionado cambia fingerprint. No repinear goldens sin adjudicación.

Mensaje observado:
```text
tests/test_modality_pipeline_integration.py:572: in test_no_modality_pre_post_golden_parity
    assert query_fingerprint(parsed.params) == expected_fingerprint
E   AssertionError: assert '55480b721e4b...1e0f75dc1a7a2' == '1084437227dc...3febc6ba87f4f'
E
E     - 1084437227dc22cc853225828a8a7996bb8f46ba7a1ce2eba5b3febc6ba87f4f
E     + 55480b721e4badd24a04e560e90e0ff44a22ed4e3384219dc3f1e0f75dc1a7a2
```

## C17 — SQL / negación (1 fallos)

Archivo/función: `tests.test_object_negation_soql::test_generated_object_exclusion_is_null_safe_for_real_dataset_fields`.

Representante: `tests/test_object_negation_soql.py::test_generated_object_exclusion_is_null_safe_for_real_dataset_fields[jbjy-vk9h-objeto_del_contrato-descripcion_del_proceso]`.

**A — main PASS / R1 FAIL.** Golden exige NOT al inicio; el predicado de universo lo precede.

Mensaje observado:
```text
tests/test_object_negation_soql.py:44: in test_generated_object_exclusion_is_null_safe_for_real_dataset_fields
    assert where.startswith("NOT (")
E   assert False
E    +  where False = <built-in method startswith of str object at 0x11c5f87b0>('NOT (')
E    +    where <built-in method startswith of str object at 0x11c5f87b0> = "(upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent...znegtest%')), false) OR coalesce(unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%zznegtest%')), false))".startswith
```

## C18 — SQL / universidad (2 fallos)

Archivo/función: `tests.test_public_university_completion_r2fb::test_source_projection_is_exact_equality_never_like`.

Representante: `tests/test_public_university_completion_r2fb.py::test_source_projection_is_exact_equality_never_like[UNIVERSIDAD DEL PAC\xcdFICO-by_dataset1]`.

**A — main PASS / R1 FAIL.** Prohíbe unaccent en todo SQL; el predicado de estado lo usa aunque buyer conserva igualdad exacta.

Mensaje observado:
```text
tests/test_public_university_completion_r2fb.py:181: in test_source_projection_is_exact_equality_never_like
    assert "unaccent" not in soql
E   assert 'unaccent' not in "SELECT coun... PACIFICO//'"
E
E     'unaccent' is contained here:
E       SELECT count(*) WHERE (upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', 'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) AND nombre_entidad = 'UNIVERSIDAD DEL PACIFICO//'
E     ?                              ++++++++
```

## C19 — SQL / universidad (4 fallos)

Archivo/función: `tests.test_public_university_pilot_r2fa::test_source_projection_is_exact_and_preserves_canonical`.

Representante: `tests/test_public_university_pilot_r2fa.py::test_source_projection_is_exact_and_preserves_canonical[UNIATLANTICO-by_dataset0]`.

**A — main PASS / R1 FAIL.** Prohíbe unaccent en todo SQL; el predicado de estado lo usa aunque buyer conserva igualdad exacta.

Mensaje observado:
```text
tests/test_public_university_pilot_r2fa.py:169: in test_source_projection_is_exact_and_preserves_canonical
    assert "unaccent" not in soql
E   assert 'unaccent' not in "SELECT coun...l Atlántico'"
E
E     'unaccent' is contained here:
E       SELECT count(*) WHERE (upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', 'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) AND nombre_entidad = 'Universidad del Atlántico'
E     ?                              ++++++++
```

## C20 — SQL / global autorizado (1 fallos)

Archivo/función: `tests.test_safe_soql::test_allow_global_opt_in_bypasses_guard`.

Representante: `tests/test_safe_soql.py::test_allow_global_opt_in_bypasses_guard`.

**A — main PASS / R1 FAIL.** Golden exige WHERE 1=1; ahora universo contractual obligatorio.

Mensaje observado:
```text
tests/test_safe_soql.py:107: in test_allow_global_opt_in_bypasses_guard
    assert " WHERE 1=1 " in soql
E   assert ' WHERE 1=1 ' in "SELECT id_contrato, referencia_del_contrato, nombre_entidad, departamento, ciudad, objeto_del_contrato, descripcion_d...CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50"
```

## C21 — SQL / conjunto vacío (1 fallos)

Archivo/función: `tests.test_safe_soql::test_fully_inapplicable_include_is_unsatisfiable`.

Representante: `tests/test_safe_soql.py::test_fully_inapplicable_include_is_unsatisfiable`.

**A — main PASS / R1 FAIL.** Golden exige WHERE 1=0; ahora universo contractual AND 1=0.

Mensaje observado:
```text
tests/test_safe_soql.py:177: in test_fully_inapplicable_include_is_unsatisfiable
    assert " WHERE 1=0 " in soql
E   assert ' WHERE 1=0 ' in "SELECT id_contrato, referencia_del_contrato, nombre_entidad, departamento, ciudad, objeto_del_contrato, descripcion_d... 'SUSPENDIDO', 'PRORROGADO')) AND 1=0 ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50"
```

## C22 — SQL / source name (8 fallos)

Archivo/función: `tests.test_source_name_projection_v1::test_unbound_identity_is_byte_identical_to_pre`.

Representante: `tests/test_source_name_projection_v1.py::test_unbound_identity_is_byte_identical_to_pre[jbjy-vk9h-nombre_entidad-CONTRALOR\xcdA GENERAL DE LA REP\xdaBLICA]`.

**A — main PASS / R1 FAIL.** Paridad byte a byte del SQL completo anterior al universo contractual.

Mensaje observado:
```text
tests/test_source_name_projection_v1.py:196: in test_unbound_identity_is_byte_identical_to_pre
    assert soql == expected
E   assert "SELECT count...REPÚBLICA%'))" == "SELECT count...REPÚBLICA%'))"
E
E     - SELECT count(*) WHERE unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%CONTRALORÍA GENERAL DE LA REPÚBLICA%'))
E     + SELECT count(*) WHERE (upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', 'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) AND unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%CONTRALORÍA GENERAL DE LA REPÚBLICA%'))
```

## C23 — SQL / DAPRE (1 fallos)

Archivo/función: `tests.test_source_name_projection_v1::test_dapre_projects_physical_name_in_both_datasets`.

Representante: `tests/test_source_name_projection_v1.py::test_dapre_projects_physical_name_in_both_datasets`.

**A — main PASS / R1 FAIL.** Paridad literal de SQL completo anterior al universo contractual. No cambiar alias.

Mensaje observado:
```text
tests/test_source_name_projection_v1.py:249: in test_dapre_projects_physical_name_in_both_datasets
    assert builder.build_count(CONTRATOS, {"entidad_resolved": DAPRE_CANONICAL}) == (
E   assert "SELECT count...dad = 'DAPRE'" == "SELECT count...dad = 'DAPRE'"
E
E     - SELECT count(*) WHERE nombre_entidad = 'DAPRE'
E     + SELECT count(*) WHERE (upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL OR upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', 'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')) AND nombre_entidad = 'DAPRE'
```

## C24 — Catálogo / integridad (1 fallos)

Archivo/función: `tests.test_state_catalog::test_catalog_no_unexpected_duplicates`.

Representante: `tests/test_state_catalog.py::test_catalog_no_unexpected_duplicates`.

**A — main PASS / R1 FAIL.** Suspendido duplicado entre contrato_suspendido y no_procede; se añadió la primera familia en candidato. Independiente del speech act.

Mensaje observado:
```text
tests/test_state_catalog.py:95: in test_catalog_no_unexpected_duplicates
    assert key in _KNOWN_OVERLAPS, (
E   AssertionError: Unexpected duplicate contratos:estado_contrato:Suspendido in families contrato_suspendido and no_procede
E   assert 'contratos:estado_contrato:Suspendido' in {'procesos:estado_del_procedimiento:Aprobado', 'procesos:estado_del_procedimiento:En aprobación', 'procesos:estado_del_procedimiento:Seleccionado'}
```

## C25 — Arquitectura / temporal (1 fallos)

Archivo/función: `tests.test_truth_layer_temporal::test_temporal_modules_have_no_runtime_consumers`.

Representante: `tests/test_truth_layer_temporal.py::test_temporal_modules_have_no_runtime_consumers`.

**A — main PASS / R1 FAIL.** Dos consumidores runtime nuevos detectados por guard temporal: contract_universe.py y formatter.py. Independiente del speech act.

Mensaje observado:
```text
tests/test_truth_layer_temporal.py:1314: in test_temporal_modules_have_no_runtime_consumers
    assert consumers == []
E   AssertionError: assert ['app/core/co...formatter.py'] == []
E
E     Left contains 2 more items, first extra item: 'app/core/contract_universe.py'
E     Use -v to get more diff
```

## C26 — Exportación / versión (1 fallos)

Archivo/función: `tests.test_value_sanity_reachability::test_export_preserves_source_amount_and_existing_columns`.

Representante: `tests/test_value_sanity_reachability.py::test_export_preserves_source_amount_and_existing_columns`.

**A — main PASS / R1 FAIL.** build_results_xlsx rechaza universo no acreditado/versionado del fixture. No quitar la protección de exportación.

Mensaje observado:
```text
tests/test_value_sanity_reachability.py:195: in test_export_preserves_source_amount_and_existing_columns
    payload = build_results_xlsx(
app/core/export_xlsx.py:81: in build_results_xlsx
    raise ValueError("El universo de esta exportación requiere revisión. Repite la búsqueda.")
E   ValueError: El universo de esta exportación requiere revisión. Repite la búsqueda.
```
