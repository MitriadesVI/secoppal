# C1.2 — enmienda 4 de ADR-103 aplicada: frontera legacy y tope top-N (P8)

Rama `mission/m1-c12-ir-v2`, parte de `99bce8fe`. Aplica `C1_2_FRONTIER_SPEC.md` (v2) y `C1_2_TOPN_SPEC.md` (v2) más las
correcciones N1, N2 y N6 de ZEUS. Autoridad: ADR-103 enmienda 4, D1(b), D1(d), D1(e), D9 y D13.

## 1. Frontera de vocabulario Foundation / legacy

| Archivo | Cambio |
|---|---|
| `app/core/foundation_guard.py` | Hoja de frontera de fuente: `_GUARD`, `_RAW_ROWS`, `execution_guard`, `verify_effective_source_call`, `foundation_source_active`, `raw_source_rows`, `source_rows_raw`, `validate_foundation_count_total` con el mismo cuerpo. Sólo importa `contextlib`, `contextvars` y `source_call_receipts`. `_GUARD` es `ContextVar[object \| None]`. |
| `app/core/foundation_execution_guard.py` (nuevo) | `FoundationGuardError`, `_without_order`, `RequestExecution`, `ExecutionGuard`, movidos sin cambio. La hoja no los reexporta. |
| `app/core/foundation_runtime.py:18-19`, `tests/test_foundation_runtime.py`, `tests/test_foundation_token_runtime.py` | Sólo rutas de import. |
| `app/core/secop_client.py` | **Sin cambio**: sha `a34f2642…97c9` igual al de `88653752`. |
| `scripts/audits/m1_search/legacy_closure_manifest.py` (nuevo) | Genera el manifiesto `{ruta: sha256}` en la base con `git cat-file`. |
| `tests/fixtures/legacy_closure_manifest_88653752.json` (nuevo) | 708 entradas: `app/**/*.py`, `streamlit_app.py`, `scripts/**/*.py`, todo `app/data/**` y los 7 archivos `artifacts/audits/municipal_*` que nombra `territorial_knowledge` (N1). Regenerar exige motivo en el commit y ZEUS. |
| `tests/test_m1_wave1_parity_pins.py` | Pines nuevos (sección «generated freeze»), listados abajo. |

Pines nuevos (todos en `tests/test_m1_wave1_parity_pins.py`):

1. `test_every_legacy_closure_module_matches_main`: cada módulo del cierre (386) tiene el sha del manifiesto. `ALLOWED_DIVERGENCE` =
   `semantic_dimensions` (pines 4 y 5) y `foundation_guard`. Un módulo del cierre que no está en el manifiesto es violación.
2. N2: `foundation_guard` no queda abierto por nombre. Sus bytes están fijados en `LEAF_SHA256` (`f9e965bd…949e`). Cualquier
   edición de la hoja falla ahí.
3. `test_legacy_data_is_frozen`: sha de cada archivo de `app/data/**` y de los 7 artifacts; sin archivos nuevos ni faltantes en
   `app/data/`; ningún módulo del cierre contiene `host_data`; cada literal `.json/.yaml/.yml/.csv/.parquet` de un módulo del
   cierre se resuelve por RUTA (directorio del módulo y ancestros, o `app/data`) a un archivo congelado del manifiesto, nunca
   por nombre suelto. Exenciones explícitas por par (módulo, literal): `pagination.json` y `.pagination.json`
   (`conversation_store`, `continuity_staging`, estado de runtime) y `/query.json` (`socrata_transport.soda3`, URL).
   No-vacuidad: se ven `aliases_db.json` (`query_router`) y `buyer_groups.json` (`buyer_groups`).
4. `test_semantic_dimensions_is_main_plus_new_members`: el archivo menos el bloque final después de `CONTROL_LANGUAGE` es
   byte-idéntico a main; el bloque sólo tiene líneas en blanco, comentarios y `NOMBRE = "NOMBRE"` con nombres fuera de
   `MAIN_MEMBERS`.
5. `test_semantic_dimension_use_in_legacy_is_whitelisted`: en el cierre, `SemanticDimension` sólo aparece como alias de import,
   como valor de un `Attribute` con `attr ∈ MAIN_MEMBERS` o dentro de una anotación (incluye `contract_type_resolver.py:48`).
   Se siguen los alias `as`.
6. `test_foundation_guard_is_a_source_boundary_leaf`: las aristas de la hoja son un subconjunto de
   `{app, app.core, app.core.source_call_receipts}`.

### Mutantes de frontera (aplicados sobre disco, corridos con `tests/test_m1_wave1_parity_pins.py`, restaurados; todos MUEREN)

| # | Mutación | Pines que fallan |
|---|---|---|
| F1 | la hoja importa `foundation_projection` | 2, 6 y los de cierre/aristas |
| F2 | `secop_client` importa `foundation_execution_guard` | 2 y los de cierre/aristas |
| F3 | import perezoso de `interpretation_contract` en la hoja | 2, 6 |
| F4 | `contract_type_resolver` hace `for d in SemanticDimension` | 5 y 2 |
| F5 | `def helper()` al final de `semantic_dimensions` | 4 |
| F6 | un byte de `foundation_text_predicates.py` | 2 |
| F7 | un byte de `app/data/aliases_db.json` | 3 |
| F8 | `query_router` lee `app/host_data/buyer/aliases.json` | 3 y 2 |
| F9 | la hoja reexporta `ExecutionGuard` | 2, 6 |
| N1 | un byte de `artifacts/audits/municipal_m2a/logical_units.json` y de `municipal_m2b_1/gates.json` | 3 |
| N2 | `source_rows_raw` devuelve siempre `True` en la hoja (y `foundation_source_active` siempre `False`) | 2 (sha de la hoja) |
| P-trans | `money_resolution` (legacy permitido) importa `interpretation_contract` | 2 y los de cierre |
| P-new | archivo nuevo `app/data/zz_new_file.json` | 3 |
| P-new2 | módulo nuevo `app/data/zz_new_mod.py` importado desde `query_router` | 3 y 2 |

## 2. P8: `cardinality@2`

`app/core/foundation_authority_values.py`: `TAG = "cardinality@2"`, campos `n`, `basis`, `n_requested`; `MAX_REQUESTED_TOP_N =
10**18 - 1` (sólo representación); comentario de `MAX_TOP_N` y docstring del módulo corregidos; propiedad `capped`
(`n_requested > n`); `encode()` = `("cardinality@2","n=<n>","basis=<b>","n_requested=<k>")`; `_decode` con control ASCII y de dígitos
para las dos cifras. Orden de validación fijo: 1 `n` (`INVALID_CARDINALITY_N`), 2 `basis`, 3 `n_requested`
(`INVALID_CARDINALITY_N_REQUESTED`), 4 singular (`INVALID_CARDINALITY_SINGULAR`), 5 `n == min(n_requested, MAX_TOP_N)`
(`INVALID_CARDINALITY_CAP`). `foundation_authority_contract.py` no se tocó.

Kit (`tests/foundation_authority_kit.py`, `_check_shape`): INV-C1.2-CAP (N6). Con tope exige los tres (NARROWING, `topn-cap@1`,
regla `topn.capped`); sin tope exige ninguno de los tres. Se calcula con `n_requested > n`, no con `capped`, para que un
`capped` mutado no ciegue al kit.

Tests (`tests/test_foundation_authority_contract.py`, `tests/test_foundation_authority_kit.py`): `cardinality_values()` con cifras
hasta `MAX_REQUESTED_TOP_N`; los inválidos de CARDINALITY pasan a un `parametrize` de pares `(build, code)` con
`pytest.raises(ContractError, match=rf"^{code}$")` (anclado, N6); las etiquetas alternativas del TAG se derivan de la
etiqueta real; pin de `encode` y de `capped`; `@1` falla con `^UNSUPPORTED_VALUE_TAG$`; seis autoridades juguete del kit
(`CapsAndDeclares` conforme; `CapsSilently`, `CapsWithoutTheRule`, `CapsWithoutThePolicy`, `DeclaresWithoutCap`, `RuleWithoutCap`,
`PolicyWithoutCap` fallan con `INV-C1.2-CAP`).

### Mutantes TOPN (aplicados sobre disco, corridos con los dos archivos del contrato y del kit, restaurados; todos MUEREN)

| # | Mutación | Test que la mata |
|---|---|---|
| T1 | sin paso 5 | `an_invalid_cardinality_fails_with_its_own_code` |
| T2 | `MAX_TOP_N = 101` | pin de `MAX_TOP_N` y `capped` |
| T3 | `encode` sin `n_requested` | pin de `encode`, ida y vuelta (Hypothesis) |
| T4 | `_decode` acepta `@1` | `^UNSUPPORTED_VALUE_TAG$` y etiquetas derivadas |
| T5 | sin paso 4 | `(2,S,2)` |
| T6 | `capped` pasa a `>=` | `(10,F,10).capped is False` |
| T7a | se quita INV-C1.2-CAP del kit | `the_kit_rejects_every_non_conforming_authority` |
| T7b | INV-C1.2-CAP invertido | idem y `a_conforming_cap_is_announced...` |
| T8 | paso 5 antes que 4 | `(1,S,2)` da CAP en vez de SINGULAR |
| T9 | `encode` escribe `basis=figure` para un singular | ida y vuelta de `(1,S,1)` |
| T10 (N6) | pasos 1 y 3 invertidos | `^INVALID_CARDINALITY_N$` anclado |
| T11 (N6) | el kit vuelve a «capped != all(tres)» (no exige ninguno sin tope) | `RuleWithoutCap`, `PolicyWithoutCap` |
| T12 | el kit no exige la regla | `CapsWithoutTheRule`, `a_conforming_cap...` |

## 3. Gates ejecutados (literales)

- `pytest tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py`: 80 passed (los dos tests que
  fallaban antes, en verde).
- `pytest tests/test_foundation_*.py tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py`:
  4502 passed, 1 skipped. Cubre el kit C1.1 (`test_foundation_authority_kit.py`), IR v2, `lift_v1`, `load_acceptance` y las
  fixtures de paridad. No es la suite global.
- 141 tests que mencionan `secop_client` (fuera de los anteriores): 47 failed, 3698 passed, 8 skipped, 31 xfailed, 2 errors.
  Los 47 rojos y los errores son `date_extract_y` (fixture sqlite sin la función; el workflow cae a red real), preexistentes:
  los mismos 47 nodeids fallan en una exportación (`git archive`) de `88653752` en un directorio de scratch, `comm -3` vacío;
  el error de `test_actor_conversation_corpus` también existe en la base.
- `secop_client.py`: sha256 `a34f264292fc1b90d02f32ec515b76b2bd27759267b04e1c9de0da0bf75797c9` en el árbol y en `88653752`.
- Paridad legacy: `QueryRouter.parse` sobre `parity_texts.json` (5471 textos): 0 diferencias, salidas byte-idénticas (2 errores
  idénticos en ambos lados). «main» = exportación de `88653752` (HEAD de main).
- `ruff check` sobre los 11 archivos tocados: All checks passed.
- `olympus-semgrep-delta --base 99bce8fe --config .semgrep/secoppal-laws.yml`: TOTAL=42 BASELINE=42 NEW_DELTA=0 FIXED=0.
- `rg -n "clarified, never clamped|por encima, aclarar" app docs/audits/m1_search_foundation`: `app/` limpio; queda
  `docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md:699` (fila P8), que corrige P16 de `C0_PATCHES` (documental, fuera de este commit).

## 4. Notas de aplicación

- Las 7 rutas `artifacts/audits/municipal_*` de `territorial_knowledge.EXPECTED_SOURCES` son etiquetas de procedencia (no se
  abren), pero N1 pide congelarlas y quedan en el manifiesto. Son 7 archivos, no 8.
- La resolución de literales de datos es por ruta (directorio del módulo y ancestros, o `app/data`); un nombre suelto que no
  resuelve a un archivo del manifiesto es violación.
- `MODE_FRONTIER` y su comentario no cambian aquí (C1.6).

## 5. Corrección por `C1_2_FRONTIER_SPEC` v3 (aprobada en `45457425`) y paridad de `foundation_v3` para ORDER

Un ZEUS dio NO_GO porque la implementación seguía la v2. Esta sección cierra la brecha con la v3 y la decisión de paridad.

### 5.1 Cambios

| Pin | Qué se añadió |
|---|---|
| 7 `test_query_router_use_inside_foundation_is_declared` | F = `foundation_*` salvo la hoja y los dos auxiliares (`foundation_text_predicates`, `foundation_trace_view`), `host_authorities.*` e `interpretation_contract`. Todo `.query_router` (atributo) o la cadena `"query_router"` (`getattr`) en F sólo en `QUERY_ROUTER_USERS_IN_F = {foundation_route_owner, foundation_shadow_hook, foundation_basic_fallback}`. No-vacuidad: los tres usan el atributo, F no está vacío, un override en `foundation_admission` (atributo y `getattr`) es violación. |
| 6(c) AST de la hoja | `tests/fixtures/foundation_guard_main_88653752.py.txt` (fuente de `88653752`, su sha es el del manifiesto). Las 6 funciones y `_GUARD`/`_RAW_ROWS` tienen el mismo `ast.dump` que en main, sin anotaciones. Diferencias de anotación que existen y se admiten: `_GUARD` (`ContextVar[object \| None]`) y el parámetro `guard: object` de `execution_guard` (la hoja no puede importar `ExecutionGuard`); el cuerpo es idéntico. Una función ausente es violación. |
| 3(b) completo | El manifiesto pasa a **todo archivo versionado** de `88653752` (6815 entradas, `git ls-tree -r`, sin `__pycache__`; antes 708). El escáner recoge todo literal que termine en una extensión de datos: toda extensión de un archivo versionado no `.py` del manifiesto (`.json .jsonl .yaml .yml .csv .txt .md .gz .log .xml ...`) más `.parquet .sqlite .db .xlsx .tsv`, y todo literal de ruta multisegmento que sea un directorio versionado. Resolución por ruta: relativa a la raíz, relativa al módulo (directorio y ancestros, o `app/data`), sufijo único; más de una coincidencia = ambiguo = violación; sin resolver y sin exención = violación. Cada archivo resuelto (o todo el directorio) conserva el sha del manifiesto. |
| 3(c) exenciones | `NON_DATA_LITERALS`, con motivo por entrada y probadas como reales (el literal existe en el módulo) y necesarias (no resuelven a un archivo versionado). Las dos de la v3 (`pagination.json`, `/query.json`) más las que el escáner ampliado encontró: estado de runtime no versionado (`history.jsonl`, `.jsonl`, `.envelopes.jsonl`, `.pagination.json`, `data/feedback.jsonl`, `data/secoppal.db`, `secoppal.db`, `data/actor_alias_index.sqlite`, directorio `data/conversations`) y el sufijo `.xlsx` de `export_xlsx`. |
| Script | `scripts/audits/m1_search/legacy_closure_manifest.py` ya no tiene lista de rutas ni `EXTRA_FROZEN_DATA`: vuelca todo blob. **Motivo de la regeneración del fixture**: v3 exige el listado completo para decidir ambigüedad por sufijo. Las 708 entradas anteriores conservan su sha (comprobado). |

### 5.2 Paridad de `foundation_v3` para ORDER y CARDINALITY

Reproducer `zeus9/orderprobe.py`: un payload wire v4.2 con dimensión `ORDER` pasaba de `INVALID_PROPOSAL MALFORMED_PROPOSAL:ValueError`
(base) a `NEEDS_CLARIFICATION UNRESOLVED_HOST_DIMENSION:ORDER`. **Causa**: `SemanticDimension` ganó `ORDER` y `CARDINALITY`, de modo que
`SemanticDimension("ORDER")` dejó de lanzar `ValueError` en el decoder.

**Ninguna decisión vigente exige el cambio.** Lo que hay apunta en sentido contrario: C0 §5.2 «`lift_v1` no pierde nada» e INV-C0-2
(`C0_SPINE_V5_DESIGN.md:98`, :243 «sin cambio semántico»); `C1_2_CANDIDATE.md` D3 (:49, «lo que el v4.2 no declara no se eleva: se rechaza»);
`M1_0_SEARCH_CONTRACT.md:209` («ACTOR, ORDER … no existen en [el vocabulario de v4.2]»; son vocabulario del productor v5). La
única mención de ORDER/CARDINALITY y el wire v4.2 en el candidato (`C1_2_CANDIDATE.md:65`) los describe como «excepciones por dimensiones
inexistentes» en la base, es decir, como casos que ya eran `INVALID_PROPOSAL`. El ADR-103 (:41, :64-71) decide qué hace el **productor v5**
con superlativos y cifras, no el v4.2.

**Arreglo**: `foundation_lift_v1.v1_dimension(value)` (única puerta de dimensión del wire v4.2) lanza el mismo `ValueError` que lanzaba
main para `ORDER` y `CARDINALITY`; lo usan el decoder (condiciones y omisiones) y `lift_v1` de registros v1 guardados (`INVALID_DIMENSION`).
La IR v2 en memoria no cambia (los tests de IR v2 con ORDER/CARDINALITY siguen verdes). Verificado con el reproducer: ahora
`INVALID_PROPOSAL MALFORMED_PROPOSAL:ValueError` y el mismo mensaje de usuario que en main, para `ORDER`, `CARDINALITY` y ambas.

### 5.3 Mutantes (aplicados sobre disco, restaurados; todos MUEREN)

| # | Mutación | Murió por |
|---|---|---|
| F10(c) | `source_rows_raw` devuelve `True` **y** `LEAF_SHA256` regenerado | `foundation_guard_is_a_source_boundary_leaf`, `the_leaf_functions_are_mains_by_ast…` (el pin 6(b) pasa; lo mata el AST) |
| F10(b) | igual sin regenerar el sha | pines 2 y 6(b), 6(c) |
| F12 | `foundation_admission` llama `workflow.deps.query_router.parse` | `query_router_use_inside_foundation_is_declared`, `…pin_is_not_vacuous` |
| F13 | se quita la exención de `pagination.json` (conversation_store y continuity_staging) | `legacy_data_is_frozen`, `…must_resolve_unambiguously_or_be_exempt`, `removing_an_exemption…` |
| F13b | se quita la exención `.pagination.json` | los mismos |
| F11 | un byte de `artifacts/audits/municipal_m2b/adjudicated_candidates.json` | `legacy_data_is_frozen` |
| F14 | el cierre lee `baseline/B0_corpus.txt` (`.txt` versionado) y cambia | `legacy_data_is_frozen` |
| F15 | el cierre lee un `.txt` que no existe | `legacy_data_is_frozen` (sin resolver) |
| F16 | el cierre lee `gates.json` (9 coincidencias) | `legacy_data_is_frozen` (ambiguo) |
| O1 | `v1_dimension` acepta ORDER/CARDINALITY | 5 tests de `test_foundation_ir_v2.py` |
| O2 | el decoder de condiciones no usa `v1_dimension` | `a_v42_wire_with_order_or_cardinality_is_invalid_exactly_as_in_main` |
| O3 | el decoder de omisiones no usa `v1_dimension` | `the_v42_decoder_names_only_the_dimensions_main_had` |
| O4 | `lift_v1` de registros guardados no usa `v1_dimension` | `a_stored_v1_record_with_order_is_not_lifted…` |
| O5 | sólo ORDER bloqueado, CARDINALITY pasa | los dos tests de paridad |

Re-corridos los de frontera anteriores (`frontier_mutants.py`, 16 mutantes F1-F9, N1, N1b, N2, N2b, P-trans, P-new, P-new2): los 16 MUEREN.
Un primer borrador del test de omisiones pasaba por la razón equivocada (`ContractError` es un `ValueError`); se endureció con
`type(error) is ValueError` y el positivo de control, y O3 pasó de sobrevivir a morir.

### 5.4 Gates ejecutados (literales)

- `pytest tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py tests/test_foundation_*.py tests/test_invariants_lock.py`:
  **4516 passed, 1 skipped** (144 s). No es la suite global.
- `pytest tests/test_m1_wave1_parity_pins.py` solo: 85 passed.
- Paridad legacy: `QueryRouter.parse` sobre `parity_texts.json` (5471 textos), `88653752` exportado vs este árbol: **0 diferencias**
  (2 errores idénticos en ambos lados).
- `ruff check` sobre los 5 archivos Python tocados: All checks passed.
- `olympus-semgrep-delta --base 4a8d2594 --config .semgrep/secoppal-laws.yml`: TOTAL=42 BASELINE=42 NEW_DELTA=0 FIXED=0.
