# M1-C1.2 — IR v2 en memoria con `lift_v1` sin pérdida

Rama `mission/m1-c12-ir-v2` (worktree `secoppal-m1-c12`), base `7bd671e6` (ola 1: main + Fase A + H0 + C1.1 + D-MONEY + D-GEO + D-TIME).
Autoridad: `C0_SPINE_V5_DESIGN.md` §5.1, §5.2, §3 (INV-C0-1/2), §6 fila C1.2, enmienda §5.5 por `C1_1_METIS_REVIEW.md`,
ADR-103 (enmiendas 1–3), contrato M1.0 v1.5. El contrato de autoridades C1.1 (`foundation_authority_contract.py`,
`foundation_authority_values.py`, `foundation_geo_level.py`) **no se tocó**; `GeoLevel` se importa de donde C1.1 lo dejó.

Candidato = el commit que contiene este archivo. El código de `app/` es el de `f5bd08c0`; los commits posteriores sólo
añaden pruebas y este documento. Cualquier cambio posterior invalida la certificación.

Commits: `c7d1790d` (vocabulario), `f5bd08c0` (IR v2 + lift + loader + adaptación), `test(foundation): pin the IR v2
survivors…` (cierre de supervivientes), y el de este documento.

## 0. Tesis

El wire v4.2 se decodifica como siempre y se **eleva** a la IR v2 con una función pura (`lift_v1`); el host consume el job y el
`Subject` en vez de `request.operation`/`request.subject` y **no cambia de comportamiento**. INV-C0-2 queda demostrado:
las 620 propuestas reales del productor y el corpus de regresión dan el mismo estado, razón, resoluciones, políticas, SoQL
(query y count), cláusulas y disposiciones antes y después. Cambian sólo los hashes (ver §3.3).

## 1. Cambios por archivo

| Archivo | Cambio |
|---|---|
| `app/core/foundation_operations.py` (nuevo) | `Operation` (12), `Aspect` (8), `Subject` (`processes`/`contracts`/`unspecified`), `ASPECT_OPERATIONS`, `job_shape_error(...)` → código cerrado (`:56`). Vocabulario puro. |
| `app/core/foundation_operator_table.py` (nuevo) | `OPERATORS` (tabla cerrada, `:20`), `operator_allowed` (`:36`). UNSUPPORTED no está atado a la tabla. |
| `app/core/semantic_dimensions.py:24-26` | `ORDER`, `CARDINALITY` (aditivo). |
| `app/core/legacy_shadow.py:96-99` | `DIMENSION_ORDER` incluye `CARDINALITY` (ya incluía `ORDER`). |
| `app/core/interpretation_contract.py` | `SCHEMA_VERSION = 2` (`:26`); `ProposedJob` (`:171`); `ProposedRequest` sin `operation`, `subject: Subject`, regla del universo por sujeto (`:206-243`); `ProposedCondition.level` + tabla de operadores + nivel (`:246-292`); `InterpretationProposal.jobs`, `job_of`/`operation_of`/`is_multi` (`:390-401`), `_validate_jobs` (`:403`), `_validate_order_and_cardinality` (`:425`), evidencia de jobs en `validate_raw` (`:489`). |
| `app/core/foundation_lift_v1.py` (nuevo) | `lift_v1` (registro guardado v1 → v2, `:88`), `lift_operation`, `lift_subject`, `lift_level`, `v1_job_specs` (compartidos con el decoder). |
| `app/core/foundation_acceptance_loader.py` (nuevo) | `load_acceptance` (`:134`), `load_proposal` (`:113`): versión leída del registro (1 → `lift_v1`; 2; otra cosa → `UNSUPPORTED_SCHEMA_VERSION`), claves cerradas, cada registro reconstruido por su constructor. |
| `app/core/foundation_producers.py:95-125` | El decoder v4.2 construye `Subject`, `level` (vía `lift_level`) y los jobs (vía `v1_job_specs`); mismo orden de lectura de evidencia. |
| `app/core/foundation_admission.py:57,64-69` | `request.operation != "SEARCH"` → `proposal.operation_of(...)`; `subject` por enum; `UNSPECIFIED` → `UNSUPPORTED_UNIVERSE` (la PV no está integrada); se borra la rama `UNGROUNDED_REQUEST_UNIVERSE` (el contrato ya la garantiza). |
| `app/core/foundation_projection.py:178-179,406-416` | Igual: operación por job, universo por `Subject` (se retiran los sinónimos españoles que ningún camino alcanzaba). |
| `app/core/foundation_universe_policy.py:91,107,150,161` | H7 lee el enum; `RequestUniverseDecision.subject` sigue siendo `str` (`.value`): el `to_dict` del ledger es idéntico. |
| `app/core/foundation_buyer_choices.py:58` | Tabla de datasets por `Subject`. |

Tests: `tests/test_foundation_ir_v2.py` (nuevo, 67), `tests/foundation_ir.py` (builders de jobs), `tests/foundation_ir_parity.py` +
`tests/fixtures/foundation_ir_v1_parity_corpus.json` (resultado del corpus **producido por el código IR v1** de la base);
adaptados `test_interpretation_contract.py`, `test_foundation_projection.py`, `test_foundation_universe_policy.py`
(constructores a mano con `Subject`/jobs/`level`), `test_foundation_h0_time_coverage.py` (ver D3), `test_foundation_authority_contract.py` (ver D8).

## 2. Decisiones donde el diseño dejaba margen

| Id | Decisión | Por qué |
|---|---|---|
| D1 | **`jobs` puede ser vacío sólo en la interpretación vacía** (cero peticiones ⇒ `NO_ACTION/NO_REQUEST`). Con peticiones, `MISSING_JOBS`. | 73/620 respuestas reales del productor tienen `requests: []`. El diseño pedía `jobs` no vacío, pero `lift_v1` no puede inventar un job (sin evidencia) y no hay que convertir `NO_ACTION` en `INVALID_PROPOSAL`. El decoder v5 (C1.3) puede exigir ≥1 job en el wire sin cambiar esto. **Pide visto bueno de METIS.** |
| D2 | `ProposedJob` vive en `interpretation_contract.py`; `foundation_operations.py` guarda vocabulario + `job_shape_error`. | `ProposedJob` necesita `RawSpan`/`ContractError`; ponerlo en `foundation_operations` crea un ciclo. |
| D3 | **Lo que el v4.2 no declara no se eleva: se rechaza** (`UNSUPPORTED_V1_OPERATION`, `UNSUPPORTED_V1_SUBJECT`) y un par (dimensión, operador) fuera de la tabla es `OPERATOR_OUTSIDE_DIMENSION` (`INVALID_PROPOSAL`, sin ejecutar). | La tabla cerrada es el diseño (mutante «operador fuera de tabla»). Delta declarado en §3.1. El caso H0 `TIME/before` usa ahora `between_dates` (en tabla, la intención —«otros operadores TIME siguen sin resolver»— se conserva) y `test_a_v1_operator_outside_the_table…` fija el rechazo. |
| D4 | Tabla: `CONTRACT_TYPE_OR_NATURE` conserva `family` (el host la ejecuta hoy, «reservado» del diseño); `CONTROL_LANGUAGE` = `∅` (el prompt no la ofrece); `ACTOR` = `is`. | No perder nada que el host ya ejecuta. |
| D5 | `lift_v1`: un job por operación (orden de primera aparición, `job-<op>`); SEARCH sin evidencia propia; `COUNT`/`SUM_VALUE` con `aspect=TOTAL` y la evidencia de sus peticiones (sin duplicados). Dos SEARCH con scope compartido = un job. | `DUPLICATE_JOB_OPERATION` del diseño. |
| D6 | `UNSPECIFIED` llega al host y éste devuelve `UNSUPPORTED_UNIVERSE` (fail-closed) hasta que exista la PV. | Nada produce `unspecified` en C1.2 (el wire v5 es C1.3). |
| D7 | No se añaden `AcceptanceStatus.HANDOFF` ni `view_decisions` (§5.1.2). | Son de C1.4 y de la PV (F). |
| D8 | `test_c1_1_modules_are_not_imported_by_any_runtime_module` **estaba en rojo en la base**: buscaba los nombres de módulo como texto y `geo_authority.py` (D-GEO) los nombra en un **comentario** (`:341`), no los importa. Se corrige leyendo el código sin comentarios (`tokenize`); se probó que un `import` real sigue en rojo y un comentario no. Además `foundation_geo_level` ahora lo importan la IR v2 y C1.1, y un test nuevo lo fija (`test_only_the_ir_v2_and_the_authority_contract_import_the_geographic_level`). | C1.2 es la integración de `GeoLevel` que C1.1 anticipó. No se relajó ninguna comprobación sobre el contrato. |
| D9 | `NONCONJUNCTIVE_ORDER` sólo para ORDER (como dice el diseño); CARDINALITY bajo OR ya lo bloquea la proyección (`_conjunctive`). | Cerrar sólo lo que el diseño cierra. |
| D10 | `InterpretationProposal.jobs` es un campo con valor por defecto `()` entre `omissions` y `schema_version` (el orden posicional de lo demás no cambia); `ProposedRequest` pierde `operation` (cambio de firma, declarado). | — |
| D11 | Atribución de commits: `Co-Authored-By: Claude Sonnet 5.5` (el modelo real que escribe) en vez del `Opus 5.5` del encargo. | Exactitud de la atribución. |

## 3. Deltas de comportamiento declarados

### 3.1 Respuestas v4.2 que el contrato ya no admite (todas dejan de ejecutar; ninguna ejecutaba)
- operación fuera de `SEARCH|COUNT|SUM_VALUE`, sujeto fuera de `processes|contracts`, par (dimensión, operador) fuera de la tabla: antes
  `UNSUPPORTED`/`NEEDS_CLARIFICATION` según el caso, ahora `INVALID_PROPOSAL` con razón cerrada. **Medición: 0/620 reales, 0/14 del
  corpus.** Fuzz diferencial salvaje (3000 payloads v4.2 con vocabulario y operadores arbitrarios): 2191 difieren y **las 2191** terminan en
  `INVALID_PROPOSAL` con una de esas tres razones (más 670 que en la base eran excepciones por dimensiones `ORDER/CARDINALITY`
  inexistentes).
- Riesgo producto: una respuesta del productor tipo `TIME/before` (el prompt 4.2 permite «an explicit unsupported operation») pasa de
  «¿me aclaras?» a «no pude interpretar con seguridad». El productor real usa `UNSUPPORTED` para eso (130+37 casos en las 620). Con v5 el wire
  cierra la tabla desde el prompt.

### 3.2 Idéntico
- Fuzz diferencial **estricto** (4000 payloads con vocabulario v4.2 y pares en tabla): 4000/4000 idénticos (80 READY, 71 `INVALID_PROPOSAL` con la misma razón).

### 3.3 Hashes
`interpretation_hash` cambia en todo turno (8/8 en el corpus con manifiesto) porque `to_dict` cambia. **`plan_hash` no cambia** (0/8): sólo
depende de dataset/SoQL/cláusulas/políticas. El diseño dice que cambian los dos; sólo cambia el primero. Ninguna prueba fija un hash
literal (`grep` de 64-hex junto a `interpretation_hash|plan_hash` en `tests/`: vacío).

## 4. Gates (comandos y resultados literales)

`PYTHONPATH=<worktree>`, `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` (3.14.7). Corrí los archivos que cito, **no la suite global**.

| Gate | Comando | Resultado |
|---|---|---|
| INV-C0-2 (a) corpus | `replay_host.py` (base `git archive 7bd671e6` vs rama; decode→H7→admisión→proyección, sin red) y `test_the_regression_corpus_gives_the_outcome_the_ir_v1_gave` (fixture generado por el código v1) | `corpus.jsonl: n=14 identical=14 different=0`; test verde |
| INV-C0-2 (b) 620 reales | mismo script sobre `captures/producer_raw.jsonl` (texto por `gold_id` de `inputs/*.jsonl`) | `producer.jsonl: n=620 identical=620 different=0` (READY 152, UNSUPPORTED 216, NEEDS_CLARIFICATION 179, NO_ACTION 73). Campos comparados: estado, razón, H7 (decisiones), resoluciones, políticas, manifiesto (query+count: SoQL, cláusulas, políticas, filtro local) y disposiciones. 306 manifiestos y 762 disposiciones en los READY. |
| Foundation y vecinos (paridad por nodeid) | `pytest tests/test_foundation_*.py tests/test_interpretation_contract.py tests/test_legacy_shadow.py tests/test_turn_ledger.py` | BASELINE (`7bd671e6`): `1 failed, 4172 passed, 1 skipped` (el rojo = `test_c1_1_modules_are_not_imported…`). CANDIDATE: `4493 passed, 1 skipped`. DELTA por nodeid: 0 desaparecidos, +320 (67 de `test_foundation_ir_v2`, 250 de `test_foundation_messages` y 2 del kit que parametrizan sobre las dimensiones nuevas, 1 test nuevo de `GeoLevel`), y el rojo de base pasa. Incluye H0, C1.1 y autoridades. |
| Expandido | `pytest` sobre lo anterior + `tests/product_contract` + los 16 archivos de `tests/` que mencionan foundation/interpretation/legacy_shadow | `5366 passed, 1 skipped, 7 xfailed` en 176 s |
| Guardas de bytes | incluidas arriba (`test_foundation_prompts.py`, `test_foundation_product_gates.py`, manifiesto natural_alpha, streamlit_app) | verdes; `git diff --name-status 7bd671e6..HEAD` no toca `foundation_prompts.py`, `streamlit_app.py`, ni archivos de carril |
| Test «un solo perfil» | `test_foundation_is_off_by_default_and_has_one_profile` | intacto y verde |
| Ruff | `python -m ruff check` sobre los 20 archivos tocados/nuevos | `All checks passed!` |
| Semgrep | `olympus-semgrep-delta --base 7bd671e6 --json <20 archivos>` | `TOTAL 0, BASELINE 0, NEW_DELTA 0, FIXED 0` |
| Mutantes | `c12_mutants.py` (fuera del repo; backup/restauración por archivo; `git status` limpio después) | ver §5 |

Nota sobre el baseline: la copia `git archive` no es un repo git, así que `test_procurement_*` (que lee blobs de git) da 13 rojos allí por artefacto;
el baseline que cuento es el del worktree en `7bd671e6` antes de editar.

## 5. Mutantes (aplicar, correr 6 archivos de prueba con `-x`, restaurar)

64 mutantes: los 6 del diseño (D1–D6) + 58. **63 muertos, 1 equivalente** (X62).

- Diseño §6 C1.2: job huérfano aceptado (D1), UNSPECIFIED con evidencia (D2), GEOGRAPHY sin `level` (D3), operador fuera de tabla (D4),
  `lift_v1` que pierde COUNT (D5), ORDER bajo OR (D6): **6/6 muertos**.
- Contrato (X01–X19): `MISSING_JOBS`, petición en dos jobs, dos jobs de una operación, job sobre petición ajena, sujeto sin universo,
  `level` fuera de GEOGRAPHY, dos ORDER, dos CARDINALITY, CARDINALITY sin ORDER, la excepción de RANK_SUPPLIERS, evidencia de job y de referente sin
  atar al texto, tipo de `level`/`subject` sin comprobar, `job_id` duplicado, `SCHEMA_VERSION=1`, forma del job, `is_multi`, `operation_of`.
- Forma del job (X20–X25): aspecto de COUNT, GROUP_BY sin aspecto, aspecto prohibido, job no-SEARCH sin evidencia, SEARCH sin petición, etiqueta de aspecto.
- Tabla (X30–X34): `before` en TIME, UNSUPPORTED atado, `family` perdido, dimensión ausente que acepta todo, CONTROL_LANGUAGE con `is`.
- `lift_v1` y decoder (X40–X53): SUM_VALUE perdido, COUNT sin TOTAL/sin evidencia, nivel perdido/en todas/municipio, un job por petición, sujeto u operación
  desconocidos elevados, versión no comprobada, jobs perdidos, todo elevado a SEARCH.
- Loader (X60–X67): versión «sniffeada», v1 sin elevar, claves ajenas/obligatorias, política y razón perdidas, enum indulgente.
- Adaptación del host (X70–X75): COUNT ejecutado como SEARCH, MULTI sin mirar la operación (admisión y proyección), UNSPECIFIED como contratos,
  proyección que ejecuta un job no-SEARCH, dataset de `processes` mal mapeado.
- Primera pasada: 6 supervivientes (X33, X49, X62, X71, X73, X74). Se añadieron 5 pruebas (`test_a_dimension_missing_from_the_table…`,
  `test_lift_v1_only_reads_a_version_one_record`, `test_a_search_sharing_its_scope_with_a_count…`, `test_the_projection_never_compiles…`) y se re-ejecutaron: muertos.
  **X62** (`True` como versión): equivalente por defensa en profundidad (`lift_v1` y el constructor de la propuesta vuelven a rechazar el `bool`).

## 6. Rondas de reparación

1 ronda de reparación (clase A/B: supervivientes de mutantes → pruebas). Ningún hallazgo C–F. Sin STOP §10: INV-C0-2 no falló.

## 7. Riesgos y pendientes

- **D1** (jobs vacío sólo en la interpretación vacía) y **D3** (rechazo de lo no declarado) piden visto bueno de METIS antes de congelar C1.2/C1.3.
- `scripts/audits/m1_search/capture_foundation_legacy.py` (`describe`, `canonical`, por ende `host_replay`) y `authority_probes.host_chain` usan el constructor
  v1 (`ProposedRequest(..., "SEARCH", subject, ...)`, `r.operation`) y **no corren** contra la IR v2. Son de HERA (diseño §5.1.6: el adaptador vive en
  `scripts/audits/m1_search/`); no se tocaron. El replay de este candidato usa un script propio fuera del repo.
- Las 620 respuestas son del prompt v4.2 con la base previa al candidato: prueban INV-C0-2, no el comportamiento de un productor v5.
- `lift_v1` rechaza (no eleva) lo que v4.2 no declara; si un dogfood muestra respuestas `TIME/before` frecuentes, hay que decidir entre rechazo y
  elevación a `UNSUPPORTED` (cambio de una línea en `lift_level`/decoder, no de contrato).
- `ProposedRequest` cambió de firma: cualquier script/tool externo que lo construya a mano debe adaptarse (`Subject` + jobs).
- `foundation_trace_view` no muestra los jobs (antes tampoco mostraba la operación): diagnóstico pendiente si se quiere.
- Pendiente de C1.3/C1.4: decoder v2 y perfiles, `decide_operation`, `HANDOFF`, borrar los dos `UNSUPPORTED_OPERATION` de admisión/proyección.
