# HCA-R7 — Integration review y promoción a main

Fecha: 2026-08-26. **HCA_R7_INTEGRATION_REVIEW: PASS**.
**ACTOR_HYGIENE_V1: CLOSED / INTEGRATED**. Sin nuevas capacidades, cambios de
runtime, tests ni oráculos durante esta revisión. No Actor Profile V2.

## Historia comprobada y promoción

| Referencia | Commit |
|---|---|
| Main antes / merge-base | `39234f873fb345b9de72ded7092afacadfdd646c` |
| Feature head / main inmediatamente después del ff-only | `23f307cf1c33cec029213d1707df91488f34f3bb` |
| Integración funcional R6 | `8bd5467aa7aa203d55605a758eedc0bf18ca33b5` |

`git show --stat`, padres reales y `git log 8bd5467..39234f8` confirman que
`39234f8` es **docs(bitacora): register HCA-R6 integration closeout**: modifica
solo la bitácora, 46 inserciones y 28 eliminaciones; su padre directo es
`8bd5467`. **RUNTIME_CHANGED: NO; EXPECTED_IN_MAIN: YES**. La bitácora de ese
commit autoriza expresamente el cierre documental como base de R7.

`git merge-base main 23f307c` dio `39234f8`; `--is-ancestor` terminó en 0.
**MAIN_RELATION: FAST_FORWARD_AVAILABLE** antes de integrar. No hay divergencia
entre R6 y R7, ni commits adicionales ocultos en la cadena a integrar.

| COMMIT | PURPOSE | RUNTIME/TEST/DOC/EVIDENCE | EXPECTED_IN_MAIN |
|---|---|---|---|
| `39234f8` (ya estaba en main) | Cierre canónico de integración R6 | DOC solamente | YES |
| `23f307c` (único integrado) | Routing Actor, cesión de pending y ranking de candidatos | RUNTIME, TEST, DOC, FIXTURE y arnés de EVIDENCE | YES |

Después del veredicto PASS se ejecutó `git switch main` y
`git merge --ff-only feature/hca-r7-actor-hygiene`. El HEAD resultante se
verificó exactamente como `23f307cf1c33cec029213d1707df91488f34f3bb`.
**FAST_FORWARD: YES; MERGE_COMMIT: NO; PUSH: NO**. Rama conservada.
Este informe y la bitácora forman el cierre **docs-only posterior**, separado
del commit funcional. No se usaron reset, clean, rebase ni cherry-pick.

## Diff acumulado

**ACCUMULATED_DIFF: CLEAN; UNRELATED: 0**. `39234f8..23f307c`: 19 archivos,
1321 inserciones, 69 eliminaciones. Clasificación exclusiva por archivo:

| Clase | Cantidad | Archivos |
|---|---:|---|
| RUNTIME | 8 | `actor_arbitration.py`, `actor_conversation.py`, `actor_intent.py`, `followup_engine.py`, `workflow/actions/actor_resolution.py`, `workflow/actions/parsing.py`, `workflow/handlers/actor_selection.py`, `data/actor_alias_resolver.py` (bajo `app/`) |
| TEST | 5 | `test_actor_conversation.py`, `test_actor_conversation_dogfood.py`, `test_actor_name_resolver.py`, `test_actor_resolution_orchestration.py`, `test_audit_runner_isolation.py` |
| FIXTURE | 1 | Nueva `tests/fixtures/audits/actor_hygiene_v1.yaml` |
| DOC | 2 | Informe original R7 y bitácora |
| EVIDENCE / arnés | 3 | `Makefile`, `scripts/audits/generate_actor_hygiene_variants.py`, `scripts/audits/run_conversation_holistic_audit.py` |
| UNRELATED | 0 | Ninguno |

No hay implementación nueva de Actor Relationships ni buyer relationships,
Semantic Registry, Market Packs, limpieza de `morphological_variants`, Time,
SODA3, Warehouse, Radar, state family o formatter general. El código de
relaciones contextual ya existente no cambia. Control Language se consume
para reconocer controles y no capturarlos como nombres; no se modifica su
autoridad. El cambio monetario solo consume la preposición de la cola del
nombre Actor; no modifica Money V2.

El arnés R7 añade stubs de aggregate y desactiva explícitamente el LLM, además
de comprobaciones para la nueva fixture. Los oráculos existentes permanecen
idénticos. La revisión no añade tests ni modifica fixtures.

## Autoridades y sentinels

| Etapa | Autoridad después de R7 |
|---|---|
| Intent/routing | Evidencia semántica, rol/documento/alias exacto y nombre desnudo acotado; casing no concede intención Actor |
| Exact resolution | N2 + alias único o variante jurídica determinista; ambigüedad exacta falla cerrado |
| Fuzzy/trigram | Recuperación y ranking; pool 100 identidades, máximo 10 candidatos; no identidad |
| Ambiguity | Lista pendiente y aclaración; identidad solo tras selección explícita válida |

`actor_name.py`, `actor_identity.py`, `actor_legal_forms.py`, el predicado
`is_authoritative_actor_resolution`, la proyección de autoridad y el documento
Actor Profile V1 no cambiaron. `resolve` retorna por las puertas exactas antes
de llegar a FTS; después de FTS solo construye `CANDIDATES`, sin `identity`.

```text
EXACT_IDENTITY_AUTHORITY: UNCHANGED
FUZZY_IDENTITY_AUTHORITY: NONE
FUZZY_CANDIDATE_AUTHORITY: YES
AUTO_RESOLUTION_FROM_FUZZY: 0
ACTOR_AUTHORITY: PRESERVED
```

- **UPPERCASE:** ambas formas de `contratos de aseo en barranquilla por más de
  100 millones` son NORMAL SEARCH: dataset `contratos`, topic `aseo`, ciudad
  `Barranquilla`, `valor_min=100000000`, sin actor, candidatos ni pending.
  Se compararon campos materiales, sin exigir igualdad del raw query.
- **Bare actors:** `FUNSOCOM` y `funsocom` resuelven NIT `900334689`.
  `UNION TEMPORAL ASEO SCJ` tiene evidencia exacta en el índice actual y sigue
  Actor flow. Los controles de actor con ciudad, año, monto y modalidad pasan.
- **Pending:** `jorge verbel` devuelve candidatos; la búsqueda siguiente
  `contratos de aseo en barranquilla en 2025` produce el mismo frame que una
  búsqueda fresca. Sin replay, sin actor ni pending residual en la respuesta
  **ni en el turno persistido**. Callback con nonce anterior:
  `actor_selection_stale`. Reemplazo atómico comprobado.
- **Selection:** `1` y `el segundo` seleccionan respectivamente los candidatos
  1 y 2 de la lista vigente, método `user_selected_candidate`; eliminan pending.
- **Exact vs fuzzy:** `FUNSOCOM` y `LMP INGENIERIA SAS` retornan exactamente el
  mismo estado, método e identidad antes/después. `lmp ingenieria` sin sufijo
  y `lmp ingieneria` siguen CANDIDATES, nunca RESOLVED.
- **Prueba explícita TOP_FUZZY_SCORE ≠ IDENTITY_AUTHORITY:** el chequeo de
  integración eleva artificialmente el score del candidato superior a
  `-1e100` (menor BM25 es mejor); proyectarlo sigue sin `actor_scope`, sin
  `contratista` y sin autoridad. Los tests existentes también verifican que
  exacto y ambigüedad exacta no invocan fuzzy. **FUZZY_AUTO_RESOLUTIONS: 0**.
- **Negative Actor:** `contratos donde el proveedor no sea X` y el control
  con `FUNSOCOM` bindean ACTOR/CLARIFY y responden SAFE_CLARIFY honesto.
  `resolve` y `resolve_exact` instrumentados para fallar si se invocan no
  fueron llamados. Sin candidatos, resolución positiva ni pending.
  **NEGATIVE_ACTOR_POSITIVE_LEAKS: 0**.
- **Index unavailable:** 7 sentinels existentes de ausencia, corrupción,
  incompatibilidad y bloqueo de workflow pasan; sentinel adicional devuelve
  `INDEX_UNAVAILABLE / INDEX_MISSING`, sin identidad ni candidatos. Sin red,
  fallback fuzzy inseguro ni adivinación LLM.

## Typo y rendimiento

Índice actual: 662409216 bytes, SHA256
`464add1dbb479e616ee80a079b11c256db0d67dc6b979e4f285612233bdb8690`;
hash comprobado idéntico antes/después. Resolver abre
`mode=ro&immutable=1`, `query_only=ON`; no se reconstruyó el índice.

| Consulta | Estado antes → después | Rank LMP antes → después |
|---|---|---|
| `lmp ingenieria` | CANDIDATES → CANDIDATES | 1 → 1 |
| `lmp ingieneria` | CANDIDATES → CANDIDATES | **7 → 1** |
| `LMP INGENIERIA SAS` | RESOLVED → RESOLVED, NIT 900739049 | No aplica |

**HISTORICAL_TOTAL_DISAPPEARANCE: NOT_REPRODUCED_CURRENT_INDEX**.
**CONFIRMED_CURRENT_DEFECT: POOR_CANDIDATE_RANKING / RECALL QUALITY**.
**R7_FIX: rank 7 → rank 1**. Esto distingue el relato histórico de la
reproducción actual; no afirma que la desaparición histórica nunca ocurriera.
Los JSON de benchmark conservan los diez candidatos, rankings y scores de
ambas versiones para las dos consultas.

Medición secuencial en la misma máquina y con el mismo índice, una vez
terminadas las suites; 5 warmups y 40 muestras por consulta, p95 por rango
más cercano. Incluye apertura y validación del índice. Milisegundos:

| Consulta | Antes p50 | Después p50 | Antes p95 | Después p95 |
|---|---:|---:|---:|---:|
| FUNSOCOM exact | 0,160 | 0,164 | 0,229 | 0,227 |
| LMP INGENIERIA SAS exact | 0,160 | 0,158 | 0,188 | 0,166 |
| lmp ingenieria | 28,529 | 29,841 | 29,452 | 30,965 |
| lmp ingieneria | 9,070 | 8,986 | 9,744 | 9,684 |

**MATERIAL_PERFORMANCE_REGRESSION: NO**. Normal aumenta ~4,6% en p50
(1,31 ms), en el rango ~30 ms reportado por R7; exacto y typo conservan
sus órdenes de magnitud y p95. No es una prueba de carga de producción.

## Reruns, HCA diferencial y deudas

Main previo se exportó desde `39234f8` con `git archive`; runtime y fixtures
son los de ese commit. Se copió **solo el runner R7** a esa exportación para
usar el mismo aislamiento, aggregate stub y LLM desactivado en ambas versiones.
Los nuevos operadores del runner no aparecen en el corpus HCA congelado.
SECOP query/count/aggregate y narrador son stubs: **no es validación de fuente
viva**. Se bloquearon sockets externos con `sandbox-exec`.

| Suite, ejecución actual | Resultado |
|---|---|
| Full pytest | **8641 passed, 2 skipped, 24 xfailed**, 228,70 s |
| Focal ampliada actor/profile, control, money, modality, followup, negation, temporal, límites | **4419 passed, 1 xfailed**, 103,85 s; subconjunto del full |
| Índice ausente/corrupto/incompatible | **7 passed**, 101 deselected |
| `make audit-actor-hygiene-v1` | **36/36**, 0 ERROR |
| Adversarial R7 generado | **242/242**, 0 ERROR |
| `make audit-control-language-v1` | **42/42**; baseline repetida 42/42 |
| `make audit-negation-v1` | **22/24**, mismos NEG-N15 y NEG-ANTI-002; baseline repetida 22/24 |
| `make audit-time-v2` | **32/32**; baseline repetida 32/32 |
| Query corpus, antes/después | **89/89 → 89/89** |
| Dialogue corpus, antes/después | **60/61 → 60/61**, único FAIL `GOB-DEPTO-001` |
| HCA holistic | **175/20/0 → 177/18/0**, 195 casos |
| `make lint-core` | PASS |
| Ruff Python cambiado, `--select E,W,F --ignore E501` | PASS, 0 hallazgos |
| `git diff --check` acumulado y working tree | PASS |

Los runners de auditoría pueden terminar con exit 0 aunque existan FAIL:
los gates anteriores se evaluaron leyendo `verdicts.json`, no solo exit codes.

Comparación caso por caso de estado, errores, checks y frame material:

| Caso | Cambio | Causa |
|---|---|---|
| HCA-ACTOR-011 | FAIL → PASS | Nueva búsqueda reemplaza pending; recupera aseo y elimina candidatos |
| HCA-META-002B | FAIL → PASS | Casing ya no secuestra búsqueda; preserva ciudad y tema |

**FIXED: esos dos; BROKEN: 0**. Ningún otro veredicto, check fallido ni frame
material final cambia. La comparación adicional de listas detecta solo el
reordenamiento de `HCA-ACTOR-004` y la eliminación de listas en los dos casos
corregidos. Nonces aleatorios no se tratan como cambios semánticos.

HCA conserva los 18 FAIL: HCA-TOPIC-007, HCA-TOPIC-012, HCA-MODALITY-002,
HCA-MODALITY-012, HCA-ENTITY-001, HCA-ENTITY-004, HCA-ENTITY-005,
HCA-MONEY-020, HCA-MONEY-026, HCA-ACTOR-004, HCA-STATE-003, HCA-STATE-005,
HCA-STATE-007, HCA-TYPO-003, HCA-FOLLOWUP-007, HCA-FOLLOWUP-016,
HCA-META-001B y HCA-SOURCE-002.

| Oracle pendiente | Clasificación factual | Evidencia actual |
|---|---|---|
| HCA-ACTOR-004 | **CONFIRMED_ORACLE_DEBT** | Notas piden candidates; único check fallido exige `actor_scope` antes de selección. Frame conserva contratos/2025, sin topic LMP; estado CANDIDATES, 10 candidatos, LMP rank 1, sin identidad. Actor Profile V1 §8 prohíbe identidad por fuzzy. |
| HCA-FOLLOWUP-016 | **POTENTIAL_ORACLE_DEBT**, conservada | Único fallo: `objeto_has: obra`; runtime conserva monto 300M, Cali, `contract_type_include=[obra]`, `ordering_signal=fecha_asc`. |
| NEG-N15 | **VALID_DEFERRED**, conservada | Modalidad positiva correcta; guard de alcance aclara una consulta solo de modalidad. |
| NEG-ANTI-002 | **WRONG_ORACLE_CANDIDATE**, conservada | Ciudad difiere del control; el oracle además exige que `entidad_resolved` difiera aunque es None en ambos. |

Ninguno se corrigió aquí. HCA SHA256
`262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba` intacto;
fixture de negación también idéntica. Regresiones nuevas observadas en control,
negación, tiempo, dinero, modalidad y followup: **0**; no implica que sus
fallos basales de HCA estén resueltos.

Precisión sobre HCA-ACTOR-004: `has: [actor_scope]` exige presencia, no demuestra
por sí solo identidad; otras rutas pueden conservar un scope no autoritativo.
La deuda confirmada consiste en exigir esa representación a una ruta cuyo
estado válido es `CANDIDATES + actor_pending`, sin scope. No se debe convertir
el candidato superior en RESOLVED para satisfacer esa aserción. El contrato
de proyección existente admite expresamente candidatos sin `actor_scope`.

`QUERY_PLAN: 299/300` usando el contador del test de límites (excluye líneas
vacías y comentarios; 371 líneas físicas). Archivo y test idénticos a main
previo; **NEXT_TOUCH_RULE intacta**, sin refactor ni aumento del límite.

## Evidencia reproducible y estado final

Artefactos de esta revisión: `/tmp/secoppal-r7-integration.BNZmE3/`:
`hca-before/`, `hca-after/`, `actor-hygiene-v1/`, `control-language-v1/`,
`negation-v1/`, `time-v2/`, `adversarial/`, baselines focales, logs completos
pytest/query/dialogue/lint, `summary.json`, `sentinels.json`,
`benchmark-before.json`, `benchmark-after.json`, `review_checks.py`,
`summarize.py`, snapshots de Git y hashes antes/después. Diez auditorías
reportan `outside_sandbox=[]`; stores efímeros eliminados por el runner.

Comandos principales (todos ejecutados, resultados arriba):

```bash
DEEPSEEK_API_KEY='' PYTHONPATH=/Users/rodrigoortiz/Documents/secoppal \
  sandbox-exec -p '(version 1) (allow default) (deny network*)' \
  .venv/bin/python3 -m pytest tests/ -q
make audit-actor-hygiene-v1 AUDIT_OUT=<out>
make audit-control-language-v1 AUDIT_OUT=<out>
make audit-negation-v1 AUDIT_OUT=<out>
make audit-time-v2 AUDIT_OUT=<out>
.venv/bin/python3 scripts/run_query_corpus.py --mode all
.venv/bin/python3 scripts/run_dialogue_corpus.py --mode all --json
.venv/bin/python3 scripts/audits/run_conversation_holistic_audit.py --out <out>
.venv/bin/python3 scripts/audits/generate_actor_hygiene_variants.py --out <variants.yaml>
make lint-core
git diff --check 39234f8..23f307c
```

Los comandos de auditoría y corpus también se ejecutaron con el mismo bloqueo
de red. Los untracked conocidos se preservaron: snapshots NUL-delimited de
status idénticos antes y después de las pruebas; no se limpiaron datos ni ramas.
El cierre documental se stagea mediante lista explícita, sin incluir untracked.

```text
HCA_R7: INTEGRATED
ACTOR_HYGIENE_V1: CLOSED / INTEGRATED
ACTOR_RELATIONSHIPS: NOT PART OF R7
PENDING: ORACLE_REVIEW, FINAL_HCA, CONTROL_LIMIT_SEMANTICS,
         SEMANTIC_AUTHORITY_CLEANUP, QUERY_PLAN_AT_LIMIT
NEXT: READY_FOR_HCA_ORACLE_REVIEW
```
