# M1-D-STATE/MOD: deuda que legacy conserva hasta el CUTOVER

Dueño: CUTOVER. Decisión de congelamiento: el código y los datos alcanzables desde legacy son byte-idénticos a
`main` (88653752) hasta el CUTOVER; `app/core/estado_families.py` (alcanzable vía `query_router`, `llm_handler`,
`contract_universe`…) NO se modifica en este carril (sha `366d5465…`, igual a main).

## D-LEG-STATE-1 (A2 STATE-015/016/017)

Legacy (`estado_families.resolve_estado`, `process_state_literal`, `read_process_state_mentions`) conserva:

1. **Sin plegado de acentos**: baja a minúsculas pero no pliega tildes, así que «en ejecución» / «en evaluación»
   (como las escribe el usuario) no resuelven, aunque cada clave del catálogo está plegada (STATE-015/016).
2. **«adjudicado» → familia de evaluación**: sobre `procesos`, «adjudicados» ensancha a Evaluación + Seleccionado +
   Aprobado + En aprobación, aunque `ESTADO_PROCEDIMIENTO_SYNONYMS` lo declara sinónimo exacto de «Seleccionado»
   (STATE-017).

La autoridad STATE (`app/core/host_authorities/state.py` + `state_catalog.py`) NO hereda ninguna de las dos: pliega con
su propio tokenizador (`window_tokens`) y su catálogo plegado, y lee «adjudicado» como el literal exacto. Lo fijan
`test_the_authority_folds_accents_and_case_by_itself_state_015_016` y
`test_adjudicado_is_the_exact_literal_on_procesos_never_the_evaluation_family_state_017`
(`tests/test_host_authority_state.py`), con los mutantes E01–E03 de `scripts/audits/m1_d_state_mod/mutants.py`.
La divergencia con legacy queda declarada y fijada en `LEGACY_DIVERGENCES` del mismo archivo.

**Acción en el CUTOVER**: aplicar los dos arreglos a `estado_families` (fueron probados y medidos en el commit
`3bf50d3c`, 13 de 172 celdas de legacy cambian; tres tests de consumidores —`test_process_state_integrity_close_v1`,
`test_process_state_request_integrity_v1`— fijan hoy el ensanchamiento) y retirar la entrada de `LEGACY_DIVERGENCES`.
