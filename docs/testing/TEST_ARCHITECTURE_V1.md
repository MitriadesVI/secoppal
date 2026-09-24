# TEST_ARCHITECTURE_V1 — Fase 0 + Fase 1

> **Actualización F3 sobre main `04426d19`:** este documento conserva las
> decisiones y mediciones F0–F2.5 como historia. La portabilidad, el inventario
> fresco, los escenarios READY y los gaps actuales están en
> [TEST_ARCHITECTURE_V1_F3.md](TEST_ARCHITECTURE_V1_F3.md). La condición antigua
> de espera por Text Lineage ya se levantó en esta base.

**Estado:** F0 + F1 GO (asesor, 2026-09-12). F2 + F2.5 entregadas (§10). F3 en **WAIT** hasta que Text Lineage V1.2 esté en `main`.
**Plan:** [`docs/planes/TEST_ARCHITECTURE_V1_IMPLEMENTATION_PLAN.md`](../planes/TEST_ARCHITECTURE_V1_IMPLEMENTATION_PLAN.md)
**Carril:** B, en paralelo a Text Lineage V1.2 (carril A), sin compartir ownership.
**Rama / worktree:** `chore/test-architecture-v1-f0-f1` · `../secoppal-test-architecture-v1-f0-f1`
**`BASELINE_HEAD`:** `9a60ff02cc0ea5ac268a22a023f48fd20a5cdded` (`main`)

Este documento es la adjudicación. El inventario mecánico está en
[`TEST_INVENTORY_V1.md`](TEST_INVENTORY_V1.md), que se regenera; los datos,
en `artifacts/audits/test_architecture_v1/`.

> **Límites de esta entrega.** No se tocó runtime, no se tocó ningún test, no
> se borró nada, no se añadieron markers ni targets. No se ejecutó la suite:
> F0/F1 sólo usan `pytest --collect-only` y análisis estático, para no competir
> por CPU/IO con los benchmarks y gates de Text Lineage. Toda clase asignada es
> **heurística**; donde digo "confirmado" es porque leí el código o hice `diff`.

---

## 1. Fase 0 — Baseline

| Campo | Valor | Fuente |
|---|---|---|
| `BASELINE_HEAD` | `9a60ff0` | `git rev-parse HEAD` |
| status rastreado | limpio (sólo los tres paths nuevos de este carril) | `git status --short` |
| intérprete | Python 3.14.7 · pytest 9.0.2 · Hypothesis 6.165.10 | `.venv` de main |
| `TOTAL_TEST_FILES` | **442** `tests/test_*.py`, todos en la raíz de `tests/` | collect |
| `TOTAL_NODEIDS` | **14.439** | `pytest --collect-only` (11 s, 0 errores de colección) |
| funciones de test | **≈6.206**; el 63 % de los nodeids sale de `parametrize` | AST |
| `SKIPS` estáticos | **3**: 2 `skip` y 1 `skipif` | evaluación de markers al recolectar |
| `SKIPS` dinámicos (históricos) | **19** en la última corrida global; los 19 dependen del entorno o son opt-in (§4 H2) | JUnit `ded6ae4` |
| `XFAILS` | **15**, 5 `strict`; en la corrida histórica los 15 siguen fallando y no hay XPASS silencioso | collect + JUnit |
| duración full suite | **no medida en `9a60ff0`**; la última: `14349 passed, 19 skipped, 15 xfailed in 796.04s (0:13:16)` | `docs/audits/contract_universe_v1_1_final_closeout/FINAL.md` @ `ded6ae4` |
| tiempos por nodeid | 14.383 / 14.439 nodeids con tiempo histórico; los 56 nuevos, sin tiempo | `global.xml` @ `ded6ae4` |
| `make lint-core` | PASS (`app/core/ limpio`) | ejecutado |

### 1.1 `CURRENT_MARKERS`

`pytest.ini` sólo declara `testpaths = tests`. **No hay ningún marker propio
registrado** (`product`, `slow`, `live`, `audit`… no existen). Uso real, por
nodeid: `parametrize` 9.201 · `hypothesis` 174 · `xfail` 15 · `skip` 2 · `skipif` 1.
Hoy es imposible seleccionar la suite por propósito sin listar archivos.

### 1.2 `CURRENT_GATES`

| Gate | Qué ejecuta | Observación |
|---|---|---|
| `make test` | `source .venv/bin/activate && pytest tests/ -q` | necesita `.venv` en el cwd, así que no funciona en un worktree |
| `make lint` | `ruff check app/ tests/ scripts/ --select E,W,F --ignore E501` | no forma parte de CI |
| `make lint-core` | busca archivos prohibidos en `app/core` | no es un linter |
| `make smoke` / `smoke-fast` | `scripts/smoke_test.py` contra SECOP en vivo | |
| `make audit-{negation-v1,time-v2,control-language-v1,actor-hygiene-v1,hca-final-p1}` | runner holístico sobre `tests/fixtures/audits/*.yaml` | auditoría, no gate por commit |
| CI `unit-tests` | `pytest tests/ -q --tb=short` en **Python 3.12**, en push a main/dev y en PR | toda la evidencia local es de 3.14.7 (§4 H13) |
| CI `smoke` | semanal o por dispatch, SECOP en vivo | |
| En la suite | `test_module_size_limits` (NEXT_TOUCH_RULE), `test_truth_layer_import_boundary`, `test_territorial_import_guard`, fixtures autouse de `tests/conftest.py` (persistencia en `tmp_path`, Actor flag ON, índice de actores aislado) | |
| Manual/agente | Semgrep `.semgrep/secoppal-laws.yml`, corrida global del coordinador (OLYMPUS_POLICY) | no está en CI |

### 1.3 `CURRENT_CORPORA`

| Corpus | Tamaño | Qué alimenta |
|---|---:|---|
| `query_corpus_v1.yaml` | 89 queries | parser / `QueryFrame` |
| `dialogue_e2e_corpus.yaml` | 29 secuencias | `SecopalWorkflow.run_query` con filas sintéticas genéricas |
| `dialogue_policy_corpus.yaml` / `_min.yaml` | 61 / 30 casos | DialoguePolicy (`_min`: sólo estructura, "no runner yet") |
| `rated_eval_v1.yaml` | 62 queries | eval calificada |
| `stress_broad_queries.yaml` | 25 casos | estructura y estrés |
| `modality_resolver_corpus_v1.yaml` | 301 casos | modalidad (M3) |
| `money_polarity_corpus_v1.yaml` | 105 casos | Money polarity V2 |
| `actor_profile_corpus_v1.yaml` | 41 queries | Actor Profile |
| `entity_profile_corpus_v1.yaml` | 41 de un turno + 20 conversaciones | Entity Profile |
| `actor_conversation/natural_actor_v1.yaml` | 10 actores reales + 15 familias + 16 dogfood exactos | conversación Actor |
| `actor_profile_adversarial/` | 1.150 frozen + 38 secuencias + 385 evidencias | adversarial Actor |
| `actor_resolution/` | 5.629 muestras benchmark + 105 review pack | resolución de nombres |
| `actor_full_index/p0_repros.jsonl` | 3 | P0 del índice completo |
| `dogfood_followup_regressions.json` | 10 | follow-up |
| `audits/conversation_holistic_v1.yaml` (HCA) | 195 | runner holístico |
| `audits/{negation_focal_v1,time_v2_focal,control_language_v1,actor_hygiene_v1,hca_final_p1_fixes}.yaml` | 24 / 32 / 42 / 36 / 40 | targets `make audit-*` |
| `soda3_probe_cases.json` / `soda3_runtime_canary_queries.json` | 16 / 26 | transporte SODA3 |
| `phrase_retrieval/manual_contratacion_2024_v1.jsonl` | 146 | phrase retrieval |
| muestras SECOP (`secop_integrado*`, `secop1_procesos`) | 20–50 filas | field mapping |

Lo que no existe es un corpus que componga consulta real + workflow +
directorio de actores con colisiones + persistencia + resultado visible. Esa
es la brecha que motiva el Product Contract (plan §1.2), y el inventario la
confirma (§4 H1).

---

## 2. Fase 1 — Inventario

Herramienta: `scripts/audits/test_architecture_inventory.py` (`collect` →
`build` → `render`). Es determinista: `build` y `render` producen bytes idénticos
con distinto `PYTHONHASHSEED`. Salidas:

- `artifacts/audits/test_architecture_v1/baseline.json` y `baseline.md`
- `artifacts/audits/test_architecture_v1/collection.json.gz`: volcado crudo de la colección
- `artifacts/audits/test_architecture_v1/inventory.json`: resumen, archivos, duplicación y menciones de `INV-*`
- `artifacts/audits/test_architecture_v1/inventory_items.jsonl.gz`: un registro por nodeid con path, dominio, markers, fixtures, parametrización, Hypothesis, imports `app.*`, uso de workflow, dependencia de red, clase, confianza, evidencia y duración histórica

### 2.1 Clasificación preliminar

| Clase | nodeids | % | archivos (clase primaria) | tiempo hist. |
|---|---:|---:|---:|---:|
| DOMAIN_INVARIANT | 11.143 | 77,2 % | 294 | 188 s (24 %) |
| AUDIT_HISTORICAL | 1.860 | 12,9 % | 77 | **269 s (34 %)** |
| PRODUCT | 1.094 | 7,6 % | 48 | 226 s (29 %) |
| ARCHITECTURE | 173 | 1,2 % | 12 | 6 s (1 %) |
| PROPERTY | 131 | 0,9 % | 8 | 97 s (12 %) |
| UNKNOWN | 38 | 0,3 % | 3 | 2 s |

Confianza: alta 1.265 · media 11.364 · baja 1.772 · ninguna 38.

**Cómo leer la tabla.** `DOMAIN_INVARIANT` es la clase por defecto de
cualquier test que importa `app.*` sin otra señal, así que es un techo, no un
recuento acreditado. `PRODUCT` significa que el nodeid llega a
`SecopalWorkflow.run_query` (directamente o por helper o fixture); **no**
significa que use datos reales ni que esté aislado de la red (H16).
`AUDIT_HISTORICAL` se asigna por nombre (`independent`, `remediation`,
`audit`…), por importar `scripts.audits` o por leer `artifacts/audits` con
token de versión.

Precedencia de reglas: nombre histórico fuerte → nombre de arquitectura →
nombre histórico débil → `@given` → la función lee código de `app/` →
`scripts.audits` → versión + artefactos → workflow → artefactos sin `app` →
importa `app` → sólo fixtures → UNKNOWN.

### 2.2 `UNKNOWN`, adjudicados a mano (38 nodeids)

| Archivo | nodeids | Adjudicación |
|---|---:|---|
| `test_secop1_procesos_field_mapping.py` | 11 | **Autorreferente.** Define `NormalizedHistoricalProcessRecord` dentro del test y prueba ese modelo contra una muestra JSON. No importa `app`: **no defiende runtime**. Candidato a `MOVE_TO_AUDIT` en F6, con proof. |
| `test_secop_integrado_field_mapping.py` | 13 | Igual: `NormalizedContractRecord` definido en el test. Autorreferente. |
| `test_procurement_layers_sources.py` | 14 | Prueba `scripts/explore_procurement_layers.py` (exploración) con `requests` bloqueado. Es tooling de auditoría, no producto. |

Hay otros tres del mismo tipo que el heurístico sí clasificó:
`test_dialogue_policy_corpus_min.py` (7; su docstring dice "no runner yet":
sólo cuenta casos del YAML), `test_buyer_group_capacity_evidence.py` (9; valida
JSON de `artifacts/audits`, no comportamiento) y
`test_gazetteer_ownership_gate_r2a.py` (9; gate sobre un baseline congelado).

### 2.3 Familias

| Familia | archivos | nodeids | nodeids workflow | AUDIT_HIST | tiempo hist. |
|---|---:|---:|---:|---:|---:|
| modality | 16 | 3.371 | 82 | 15 | 68 s |
| buyer_entity_resolution | 35 | 1.261 | 8 | 298 | **123 s** |
| query_frame_parsing | 36 | 983 | 29 | 15 | 22 s |
| actor | 34 | 981 | 102 | 162 | 87 s |
| contract_universe | 26 | 951 | **505** | **543** | 89 s |
| entity_profile | 32 | 856 | 77 | 145 | 42 s |
| notify | 18 | 832 | 0 | 0 | 21 s |
| truth_layer | 20 | 791 | 7 | 17 | 13 s |
| entity_profile_frame | 35 | 626 | 269 | 225 | 97 s |
| radar | 14 | 536 | 0 | 0 | 8 s |
| ranking_results_presentation | 32 | 501 | 71 | 60 | 16 s |
| money_value | 17 | 473 | 25 | 10 | 52 s |
| geo_territory | 21 | 461 | 8 | 173 | 36 s |
| soql_source_execution | 29 | 459 | 21 | 8 | 10 s |
| negation_control | 12 | 429 | 55 | 0 | 17 s |
| followup_conversation | 27 | 322 | 52 | 25 | 12 s |
| buyer_actor_arbitration | 10 | 260 | 216 | 99 | 58 s |
| time_state_opportunity | 16 | 215 | 14 | 15 | 9 s |
| channels_auth | 5 | 89 | 14 | 28 | 1 s |
| architecture_meta | 7 | 42 | 3 | 22 | 5 s |

La asignación de familia es por nombre de archivo; la tabla completa por
archivo está en `TEST_INVENTORY_V1.md`.

---

## 3. Qué dice el mapa, en una línea por familia crítica

- **Modality:** 3.371 nodeids, pero 2.500 son una sola tabla
  (`test_modality_resolver.py`, 60 funciones × corpus). Su volumen es
  parametrización, no redundancia.
- **Contract Universe:** 26 archivos creados en **dos días**
  (2026-09-08/09). El 57 % de sus nodeids son historia de auditoría o
  remediación (`independent`, `remediation`, `r3`, `r4`, `r5`, `f2_1`, `f2_2`,
  `cu_v11_audit_*`, `f21_final_delta`). Aquí están los duplicados confirmados
  (H3).
- **Entity Profile Frame:** 35 archivos, de los cuales 46 `entity_profile_*`/`v12*`
  se crearon el **mismo día** (2026-09-07). 225 de 626 nodeids son
  `v12b*_independent`/`remediation`.
- **Actor:** 34 archivos con fechas de alta repartidas en **8 días** (del
  08-18 al 09-09): AP1, conversación AP3, prefijo exacto (ADR-094), Hardening
  V2, V2.1, pending document y contracted action. A eso se suman 10 archivos
  `buyer_actor_*`/`beta_*` del 09-06 al 09-09. Ninguna generación compone la
  forma Entity Profile de Barranquilla con un directorio que colisione (H1).
- **Notify / Radar:** 1.368 nodeids, cero workflow y cero historia. Son
  dominio puro y el candidato más claro a `test-core`.

---

## 4. Hallazgos

Numerados para citarlos en F2/F3. Ninguno se corrigió: todos quedan fuera del
scope de F0/F1.

### H1 — La composición Barranquilla en Entity Profile no está en el gate (P1, confirmado estáticamente)

- **Cubierto** (offline, workflow real, SQL evaluado en SQLite):
  `test_buyer_actor_role_arbitration_v1.py::test_seeds[barranquilla-*]`,
  reproducción de `47e46a1`. Usa la frase *"que ha contratado alcaldia de
  barranquilla por mas de 2000 millones"* (ruta de búsqueda + monto) y un
  directorio cuyos proveedores **se llaman igual que la mención** (cardinalidad
  0, 1 o 3, índice ausente o incompatible, Frame off/on).
- **No cubierto:** la ruta Entity Profile (*"analiza la alcaldía de
  barranquilla"*) con un directorio de actores que colisione. Las pruebas que
  combinan Entity Profile con un índice de actores usan filas que no colisionan
  con el comprador (`ACTOR ALFA` sobre Galapa, `FUNSOCOM`, `RODRIGO ORTIZ`), y
  la golden de Entity Profile es Baranoa: `"alcaldia municipal de baranoa"`
  aparece en 17 archivos. `"alcaldia de barranquilla"` aparece en 16 archivos de
  9 familias y **en ninguno** de `entity_profile*`.
- Tampoco hay colisiones de forma real (una persona cuyo nombre contenga o
  prefije tokens del comprador) fuera de los opt-in que nunca corren (H2). La
  única fila de directorio con forma institucional es `P0-3` (Alcaldía de
  Puerto Salgar como **proveedor**).
- **Límite:** no ejecuté el runtime. No afirmo que hoy falle; afirmo que
  **ningún test del gate falla si falla**. F3 debe demostrarlo con
  `mutation → FAIL / current → PASS` (plan §14.2).

### H2 — Los tests contra índice real nunca corren en un gate automático (P1)

Los 19 skips de la última corrida global dependen todos del entorno:

- 8 de `test_actor_conversation_dogfood.py` ("production actor index is not
  available");
- 4 de `test_buyer_actor_role_local_v1.py` y 4 de
  `test_buyer_actor_independent_local.py`, que requieren
  `BUYER_ACTOR_LOCAL_INDEX`, y son **justamente los casos Cartagena y
  Barranquilla con el índice real**;
- 1 de `test_actor_index_isolation.py`;
- 2 de recall vivo en `test_dogfood_regressions.py`.

`test_actor_conversation_dogfood.py` resuelve `data/actor_alias_index.sqlite`
**relativo al cwd**. Corre en el checkout de `main` si el artefacto de 662 MB
existe y se salta en worktrees y en CI. Es exactamente el resultado dependiente
del entorno que `test_actor_index_isolation.py` §6 declara inaceptable. La
evidencia real de Barranquilla existe pero vive fuera de todo gate.

### H3 — Duplicados confirmados en Contract Universe (MERGE, no retirar)

| A | B | nodeids | `diff` |
|---|---|---:|---|
| `test_contract_universe_independent.py` | `test_contract_universe_remediation.py` | 21 + 21 | sólo docstring y un comentario; mismas aserciones |
| `test_contract_universe_independent_consumers.py` | `test_contract_universe_remediation_consumers.py` | 11 + 11 | sólo docstring y comentario |
| `test_contract_universe_independent_legacy.py` | `test_contract_universe_remediation_legacy.py` | 4 + 6 | **no** son duplicados: fijan contratos opuestos (`schema_version` 1 frente a 2, predicado de estado ausente frente a presente) sobre artefactos distintos, y remediation añade 2 tests |

Hay 32 nodeids que se ejecutan dos veces sin aportar defensa nueva. Aun así,
el plan exige `TEST_RETIREMENT_PROOF` en F6, así que aquí sólo se marcan.

### H4 — Seis xfail no estrictos de los demos llevan tiempo muertos

`test_demo_{alcalde,proponente,veedor}.py` tienen 6 `xfail(strict=False)` con
el motivo "Observer 2.0 signals requieren mock de aggregate() adicional". No
marcan un bug de producto: es deuda del harness desde ADR-009. Por ser no
estrictos, si empezaran a pasar nadie se enteraría. Además, los tres demos y
`test_sprint6_integration.py` comparten andamiaje (scores estructurales de 0,48
a 0,60).

### H5 — Bugs de producto congelados como xfail que tocan el Product Contract

- `test_actor_hardening_v2_1_remediation.py::test_actor_region_and_buyer_together_still_resolve`
  (2, no estrictos): *actor + región explícita + comprador explícito* termina en
  CANDIDATES en vez de resolver. Es preexistente desde `f9cb8c9` y es **la
  familia de composición de Barranquilla**.
- `test_control_language.py::test_scope_change_carrying_a_new_topic_keeps_that_topic`:
  un `change_scope` que trae un tema nuevo lo pierde.
- `test_gazetteer_lote_01.py`: `sena choco` y `sena casanare` resuelven a SENA
  SECRETARÍA GENERAL (estrictos); en `hospital municipal de acacias` un alias
  roba la resolución (no estricto).
- `test_opportunity_queries.py`: OPP-003 y REFERENCE-001, pendientes.

### H6 — El costo está en historia y propiedades, no en el dominio

AUDIT_HISTORICAL es el 12,9 % de los nodeids pero el **34 % del tiempo** (269
de 788 s); PROPERTY es el 0,9 % de los nodeids y el 12 % del tiempo. Un solo
archivo, `test_compound_identity_guard.py`, se lleva **103 s (13 % de la
suite)** con 32 nodeids: son predicciones M1A.2 congeladas que cargan dos veces
el catálogo completo (baseline + candidate) y la matriz DIVIPOLA desde
`scripts.audits`. Le siguen `test_money_polarity_v2.py` (44 s) y
`test_contract_universe_f2_1_completeness.py` (34 s).

### H7 — "14.000 tests" son unas 6.200 funciones

El 63 % de los nodeids viene de `parametrize`. Cinco archivos suman 3.725
nodeids: `modality_resolver` 2.500, `spell_semantic_safety` 397,
`cu_v11_audit_aprobado` 302, `entity_profile_semantic_corpus` 276 y
`car_core_r2d` 250. El número total de tests no sirve como métrica (plan §21).

### H8 — El gate depende del tooling de auditoría

- **23 archivos (568 nodeids)** importan `scripts.*`, en su mayoría `scripts.audits`.
- `tests/conftest.py` (fixture `frozen_municipal_baseline_router`) importa
  `scripts.audits.promote_municipal_m1a` y lee
  `artifacts/audits/municipal_m1a/before_aliases_db.json.gz`.
- 15 archivos leen `artifacts/audits` y 17 leen `docs/audits`.

Limpiar o mover scripts o artefactos de auditoría rompe la suite. F6 debe
tratarlos como dependencias del gate.

### H9 — El registro de invariantes está disperso y con IDs ambiguos

- Los `INV-*` viven en docs y ADRs. En tests, el más citado aparece nominalmente en **4 archivos**.
- `INV-STRUCTURED-DIMENSION-NOT-TOPIC`, `INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE`
  e `INV-NORMALIZATION-PRESERVES-INTENT` no aparecen por nombre en ningún test
  (pueden estar defendidos sin nombrarlos; F2 debe mapearlos).
- Conviven espacios de nombres locales que colisionan: `INV-1`, `INV-001`,
  `INV-005`, `INV-7` (29 menciones), `INV-W9`…`INV-W28`. F2 necesita un espacio
  global.

### H10 — La mayoría de los tests PRODUCT no bloquea la red

De los **72 archivos** que construyen `SecopalWorkflow`, **66 no instalan ningún
guard** de sockets ni de `requests` (grep literal; es un techo, porque puede
haber guards indirectos). Sustituyen métodos de `secop_client`. El propio
`test_buyer_actor_role_arbitration_v1.py` documenta que `with_timeout(...)`
construye un cliente nuevo que **sale a la red de verdad** si no se ata a mano.
Un test PRODUCT puede estar verde habiendo consultado SECOP en vivo. El harness
de Product Contract debe imponer offline por construcción (plan P3), no por
convención.

### H11 — CI y la evidencia local no miden lo mismo

- CI corre `pytest tests/` en Python 3.12; todas las corridas globales
  documentadas son de 3.14.7.
- CI no ejecuta Ruff ni Semgrep.
- `make test` exige `.venv` en el cwd, así que no funciona desde un worktree,
  que es donde trabajan los agentes.

### H12 — El catálogo de ADR está partido

`docs/decisions/` tiene 21 archivos, mientras la bitácora llega a ADR-095
(con ADR-092/093 intercalados antes que ADR-081). La extracción de invariantes
de F2 tiene que leer `docs/decisions_log.md`, no sólo `docs/decisions/`.

---

## 5. `TOP_DUPLICATION_CANDIDATES`

El score estructural combina 0,45 × Jaccard de consultas literales, 0,35 ×
Jaccard de identificadores y 0,20 × Jaccard de imports `app`. **Nada se borra
por este score.**

| # | A | B | score | Adjudicación preliminar |
|---:|---|---|---:|---|
| 1 | `contract_universe_independent_consumers` | `contract_universe_remediation_consumers` | 1,00 | **DUPLICADO CONFIRMADO** (diff) → MERGE en F6 |
| 2 | `contract_universe_independent` | `contract_universe_remediation` | 0,97 | **DUPLICADO CONFIRMADO** (diff) → MERGE en F6 |
| 3 | `contract_universe_independent_legacy` | `contract_universe_remediation_legacy` | 0,96 | **NO duplicado**: contratos de versión opuestos |
| 4 | `openwa_webhook` | `telegram_webhook` | 0,81 | PLAUSIBLE: paridad de canal (ADR-027/031); probablemente KEEP y parametrizar |
| 5 | `demo_proponente` / `demo_veedor` / `demo_alcalde` (+ `sprint6_integration`) | — | 0,48–0,60 | PLAUSIBLE: mismo andamiaje con personas distintas; además cargan H4 |
| 6 | `followup_delta_queries` | `followup_post_merge` | 0,59 | PLAUSIBLE: 11 consultas compartidas; uno es DOMAIN y el otro PRODUCT. **No tocar antes de R6** (plan §20) |
| 7 | `entity_multi_regional` | `entity_multi_regional_edges` | 0,63 | PLAUSIBLE: candidato natural a parametrizar |
| 8 | `truth_layer_contract_{performance,signature,liquidation,changes}` | — | 0,47–0,55 | andamiaje compartido; `performance` es AUDIT y los otros DOMAIN |
| 9 | `plan_query_{clarification,implicit_opportunity,scrub_and_temporal,city_extraction}` + `sprint2a_canonical_smoke` | — | 0,45–0,49 | PLAUSIBLE: familia de activación Sprint 2A |
| 10 | `public_university_pilot_r2fa` | `public_university_completion_r2fb` | 0,46 | pilot → completion; el pilot probablemente sea MOVE_TO_AUDIT |
| 11 | `bidder_intent` / `bidder_intent_commercial`, `observer` / `observer_partial`, `timeout_handling` / `timeout_transparency` | — | 0,45–0,49 | PLAUSIBLE: partir los pares "base + variante" |
| 12 | `beta_entity_actor_conversation` | `contract_universe_r5_profile_compatibility` | 0,46 | 11 consultas compartidas, pero **uno usa el workflow real y el otro no** → probable KEEP ambos |

Hay algo que no es duplicación pero pesa en la consolidación: literales como
`"en ejecución"` (51 archivos), `"licitación pública"` (22) o `"contratación
directa"` (20) están copiados por toda la suite como vocabulario de dominio
suelto. Cualquier cambio de catálogo de estados o modalidades obliga a tocar
50 archivos.

---

## 6. `P0_P1_INVARIANTS_DISCOVERED`

Es una propuesta de criticidad para F2, no un registro. "Nominal" significa
que el ID aparece literalmente en un test.

| Invariante | Crit. propuesta | Autoridad | Defensa observada | Hueco |
|---|---|---|---|---|
| INV-BUYER-NOT-ACTOR (nombre del plan) | P0 | ADR-074, Buyer-Actor V1 (`47e46a1`) | `buyer_actor_role_*_v1`, `buyer_actor_independent_*` | ruta Entity Profile; índice real sólo opt-in (H1, H2) |
| INV-NAME-NO-GUESS | P0 | ADR-060 | nominal en 2 archivos; `actor_name_resolver`, `actor_name_properties` | sin Product Contract |
| INV-ID-TYPE | P0 | ADR-060 | nominal en 4 archivos; `actor_identity` | — |
| INV-ACTOR-SINGLE-IDENTITY | P0 | Hardening V2 (A) | nominal en 1; `actor_dogfood_hardening_v2*` (clase histórica) | la única defensa vive en una suite marcada como auditoría |
| INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION | P1 | ADR-094 | nominal en 2; `actor_exact_prefix*`, `actor_mention_completeness` | — |
| INV-BARE-GEOGRAPHY-PRECEDENCE | P1 | `e1dc3e1` | nominal en 1; `actor_geography_precedence` | la composición actor + región + comprador está en xfail (H5) |
| INV-NO-SILENT-SEMANTIC-FALLBACK | P0 | ADR-069 | nominal en 4 | — |
| INV-NO-SILENT-NEGATION-DROP | P1 | ADR-064 | `negation_*`, `safe_negation_policies` | sin nominal |
| INV-STRUCTURED-DIMENSION-NOT-TOPIC | P1 | ADR-061 | `contractual_dimension_pipeline` (presunta) | **0 nominal** |
| INV-NORMALIZATION-PRESERVES-INTENT + polaridad fail-closed | P0 | ADR-063, ADR-093 | `money_polarity_*`, `money_range_atomicity`, corpus de 105 casos | **0 nominal** |
| INV-STATE-FAMILY-PROJECTS-COMPLETE | P1 | ADR-067 | nominal en 2 | — |
| "Firmado" no es estado | P1 | ADR-026 | `firmado_signed_semantics` | — |
| INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE | P1 | ADR-066 | nominal en 2; `ordering_followup_source_universe` | — |
| Los params explícitos del turno prevalecen sobre el framing heredado | P1 | ADR-037 | `frame_leak_canary`, `followup_*` | cambio de scope con tema nuevo en xfail (H5) |
| Geo tipada: territorio frente a comprador | P1 | ADR-040 | `geo_vs_entity_typed` | — |
| INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE | P0 | ADR-095 | Entity Profile semantic/frame | **0 nominal** |
| INV-SCOPE-DECLARED / INV-PARTIAL / timeout ≠ cero | P1 | Actor Profile, ADR-039, `outcome.py` | `timeout_*`, `observer_partial`, `outcome` | sin Product Contract |
| Vigencia priorizada, sin certificar apertura | P1 | ADR-056/057/058 | `current_opportunity_retrieval`, `opportunity_*`, `truth_layer_*` | — |
| Contract Universe V1.1 (Aprobado con fecha usable, etc.) | P1 | closeout V1.1 (sin archivo ADR en `docs/decisions/`) | 951 nodeids, 57 % históricos | defensa vigente mezclada con historia (H3) |
| Persistencia de tests fuera del proyecto | P0 (arquitectura) | AGENTS.md | `conftest.isolate_runtime_persistence`, `feedback_isolation` | — |

---

## 7. `FIRST_20_PRODUCT_CONTRACT_PROPOSAL`

Cubre los 20 casos obligatorios del plan §14.1, anclados a evidencia existente.
**Los valores canónicos deben tomarse del runtime acreditado en F3**; donde
cito uno es porque ya aparece como oráculo en un test del gate, y aun así hay
que reverificarlo.

| ID | Obligatorio §14.1 | Turnos (propuesta) | Oráculos clave | Fuente | Minar de |
|---|---|---|---|---|---|
| PC-AUTH-001 | 1 incidente Barranquilla | "analiza la alcaldía de barranquilla" + pack de actores con personas que colisionan (forma a extraer de la evidencia del incidente) | `route: entity_profile`; `entidad_resolved` canónico (en tests: `DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA`); `absent: actor_scope, actor_pending, contratista`; la respuesta no ofrece candidatos | incident | `47e46a1` `before/*.json`, `test_buyer_actor_role_local_v1` |
| PC-AUTH-002 | 2 buyer frente a actor | "que ha contratado alcaldia de barranquilla por mas de 2000 millones" con proveedores homónimos de la mención (×3) | `dataset: contratos`; `valor_min: 2000000000`; sin params de actor; el resolver de actores no se invoca | audit | `test_buyer_actor_role_arbitration_v1::test_seeds` |
| PC-AUTH-003 | 3 actor ambiguo | "rodrigo ortiz" con dos CC distintas | `outcome: block/clarify`; candidatos ≥ 2; ninguna identidad autoseleccionada; `source_calls: 0` antes de elegir | dogfood | `test_beta_entity_actor_conversation` (líneas 155–161) |
| PC-AUTH-004 | 4 fuzzy no autoritativo | nombre mal escrito cuyo mejor candidato LMP no es la persona | el top candidate nunca es autoridad; hace falta selección explícita | dogfood | `test_actor_conversation_dogfood::test_r7_*` (hoy siempre en skip) |
| PC-FU-001 | 5 follow-up que cambia actor | selección de candidato → documento nuevo → turno con comprador | `contratista == actor_scope` (una sola identidad); comprador conservado | audit | Hardening V2 (A), `test_actor_pending_document_selection` |
| PC-FU-002 | 6 follow-up que cambia buyer | "analiza la alcaldia de galapa" → otra alcaldía en el mismo chat | el perfil cambia; sin predicado heredado de actor ni de comprador anterior | dogfood | `test_beta_entity_actor_conversation` (`SqlProfileClient`) |
| PC-MONEY-001 | 7 polaridad | "contratos de más de 500 millones" | `valor_min` presente; **no aparece `valor_max`** (bug histórico de fallback) | feedback | `money_polarity_corpus_v1`, ADR-093 |
| PC-MONEY-002 | 8 rango | "entre 100 y 150 millones", y la contradictoria "más de 500 y menos de 100 millones" | ambos extremos atómicos; la contradicción se bloquea o aclara, no se ejecuta | audit | `test_money_range_atomicity`, ADR-063 |
| PC-DS-001 | 9 pivote de dataset | "analiza la alcaldia de galapa" → "solo contratos" | `dataset` cambia; entidad y tema se conservan | dogfood | `test_beta_entity_actor_conversation` (mapa procesos/contratos) |
| PC-EP-001 | 10 follow-up de proveedores | perfil de entidad → pregunta por proveedores | proveedores acotados al comprador; los placeholders no cuentan como proveedores | dogfood | `entity_profile_supplier_placeholders.json`, V1.2-B suppliers |
| PC-MOD-001 | 11 incluir modalidad | búsqueda con "licitación pública" | la modalidad entra como dimensión estructurada, no como tema | audit | `modality_resolver_corpus_v1`, ADR-054 |
| PC-MOD-002 | 12 excluir modalidad | "… que no sean contratación directa" | exclusión null-safe; "No Definido" no se trata como NULL | audit | ADR-064, `test_safe_negation_policies` |
| PC-NEG-001 | 13 negación | tema u objeto negado | la dimensión negada se conserva y no se descarta en silencio | audit | `negation_focal_v1`, `test_object_negation*` |
| PC-FAIL-001 | 14 timeout ≠ cero | fuente con timeout | `outcome: partial/error`; nunca "0 contratos" | incident | `test_timeout_transparency`, ADR-039 |
| PC-FAIL-002 | 15 fuente caída ≠ vacío | fuente 503 / no disponible | respuesta de indisponibilidad, no "sin resultados" | feedback | `outcome.py`, `test_observer_partial` |
| PC-OPP-001 | 16 oportunidad activa | "procesos abiertos de …" con reloj inyectado | vigencia por fecha civil CO; sin afirmar apertura certificada | audit | ADR-056/058, `test_current_opportunity_retrieval` |
| PC-TIME-001 | 17 histórico | "contratos firmados en 2023 por …" | rango por `fecha_de_firma`; "firmado" no se filtra como estado | audit | ADR-026/065, `test_time_semantics_v2` |
| PC-PAG-001 | 18 paginación tras refinamiento | búsqueda → refinamiento → "muéstrame más" | la página 2 pertenece al universo **refinado** | dogfood | `test_newsearch_pagination`, ADR-041 |
| PC-DUAL-001 | 19 dual dataset | consulta sobre procesos + contratos | universos separados; sumas sin mezclar | audit | ADR-038, `test_dual_dataset_*` |
| PC-LOOKUP-001 | 20 lookup directo | referencia de proceso o contrato | ruta de lookup directo; sin búsqueda por tema; sin inferir publicación desde CO1.REQ | audit | `test_direct_lookup*`, ADR-071 |

Tres condiciones para que F3 lo acepte:

1. PC-AUTH-001 y PC-AUTH-004 deben probar su capacidad de detección con
   `mutation → FAIL / current → PASS`.
2. El harness debe bloquear sockets por construcción (H10).
3. PC-AUTH-001 necesita un pack mínimo de actores **derivado** de la evidencia
   real (hoy sólo opt-in, H2), minimizado o anonimizado según el plan §9.3.

---

## 8. `NEXT_RECOMMENDATION`

1. **Mantener WAIT en F2/F3** hasta que Text Lineage V1.2 cierre y se conozca el
   nuevo `main`.
2. Luego, **REFRESH INVENTORY** contra el nuevo `main` (≈15 s + render):

   ```bash
   .venv/bin/python scripts/audits/test_architecture_inventory.py collect
   .venv/bin/python scripts/audits/test_architecture_inventory.py build --junit <global.xml del nuevo main> --junit-head <sha>
   .venv/bin/python scripts/audits/test_architecture_inventory.py render
   ```

   Si el coordinador corre la global del nuevo `main` con `--junit-xml`, el
   refresh también cierra la duración medida, que F0 dejó pendiente.
3. **Revisión humana de §7** (los primeros 20) antes de autorizar F3.
4. Decisiones que F2/F5 necesitan del usuario (no son de este carril):
   - **H2:** ¿el índice real se vuelve un gate explícito (p. ej. `make test-local-index`, fuera de CI) o se congela un pack mínimo derivado?
   - **H10:** el harness de Product Contract nace con guard de sockets obligatorio.
   - **H4/H5:** los xfail no estrictos de los demos pasan a estrictos o se reparan; los bugs congelados de H5 entran al Product Contract como "rojo conocido" o se abren como remediación.
   - **H11:** alinear la versión de Python de CI con la de desarrollo.
   - **H6:** `test_compound_identity_guard` es el primer candidato a `slow`/`audit` en F5.

---

## 9. Gates de esta entrega

| Comando | Resultado |
|---|---|
| `git status --short` | sólo `artifacts/audits/test_architecture_v1/`, `docs/testing/`, `scripts/audits/test_architecture_inventory.py` |
| `.venv/bin/python scripts/audits/test_architecture_inventory.py collect` (`pytest --collect-only`) | `14439 tests collected`, 0 errores |
| `PYTHONHASHSEED=1 … build` vs `PYTHONHASHSEED=987 … build` | `inventory.json` e `inventory_items.jsonl.gz` idénticos (`cmp`) |
| `… render` (dos semillas) | PASS, `TEST_INVENTORY_V1.md` idéntico |
| `make lint-core` | PASS |
| `ruff check scripts/audits/test_architecture_inventory.py` | PASS |
| `semgrep --config .semgrep/secoppal-laws.yml scripts/audits/test_architecture_inventory.py` | 0 hallazgos (archivo nuevo: baseline = 0, delta = 0) |

**Corrí `--collect-only` y análisis estático, no la suite.** No se ejecutó
ningún test ni Hypothesis: esta entrega no cambia runtime ni tests.

---

## 10. F2 + F2.5 (rama `chore/test-architecture-v1-f2-f2-5`, base `fab030d`)

### 10.1 Decisiones del asesor tras F0/F1

| Tema | Decisión |
|---|---|
| Índice real de actores (H2) | Ambos, con funciones distintas: **pack congelado de colisiones** como gate diario y `make test-product-full-index` (662 MB) como certificación pre-release, fuera de CI. SECOP en vivo sólo para canary y dogfood. |
| Red (H10) | Prohibición obligatoria: ningún socket externo durante un Product Contract; `live` nunca pertenece a `test-product`. |
| xfail (H4/H5) | `HARNESS_DEBT` ≠ `OPEN_PRODUCT_GAP`. La deuda del harness se repara o se mueve a audit. Los defectos de producto van a cuarentena con reproducción RED y **nunca** cuentan como verde. |
| Plan | Nueva fase **F2.5 Product Harness Safety** entre F2 y F3. Consolidación: ni un test hasta F6. |

### 10.2 Entregado

- **F2:** [`INVARIANT_REGISTRY_V1.md`](INVARIANT_REGISTRY_V1.md). 28 invariantes P0/P1 con ID canónico, dueño, primarios verificados por AST, casos de producto y mapeo de IDs heredados.
- **F2.5:** [`PRODUCT_HARNESS_SAFETY_V1.md`](PRODUCT_HARNESS_SAFETY_V1.md).
  - Guard de red con ledger.
  - Fuente congelada sobre un `SecopClient` real.
  - Pack de colisiones sobre el builder real.
  - Persistencia confinada.
  - Motor de oráculos.
  - Esquema del corpus.
  - Los 20 escenarios en DESIGN.
  - Cuarentena (6 `OPEN_PRODUCT_GAP`, 2 `HARNESS_DEBT`).
- Archivos nuevos solamente. `app/**`, los tests existentes, `pytest.ini` y el Makefile quedan intactos.

### 10.3 Correcciones y hallazgos nuevos

- **H9 corregido:** `INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE` e `INV-STRUCTURED-DIMENSION-NOT-TOPIC` sí están defendidos, aunque sin nombrarlos (P11, P7 y `contractual_dimension_pipeline`).
- **H10 concretado:** el workflow productivo sin fuente congelada intenta `create_connection(('www.datos.gov.co', 443))`, y el guard lo registra aunque el runtime se trague el error.
- **H13 (nuevo, P1 de privacidad/red):** la telemetría de Burr está encendida por defecto y nada en el repo la apaga. Un turno hizo 3 intentos a `app.posthog.com:443`. Hay que decidir si producción la desactiva (`BURR_TELEMETRY_ENABLED=false`).
- **H14 (nuevo):** el plan `docs/planes/TEST_ARCHITECTURE_V1_IMPLEMENTATION_PLAN.md` **no está versionado**: sólo existe sin rastrear en el checkout de `main`, así que no se enmendó. La fase F2.5 queda registrada aquí y en `PRODUCT_HARNESS_SAFETY_V1.md`.
- **H15 (nuevo):** Semgrep no escanea `tests/` (ignore por defecto), así que las leyes del repo no acreditan nada sobre tests ni harnesses.
- **INV-ACTOR-SINGLE-IDENTITY (P0):** sus dos primarios viven en suites históricas y quedó marcado `re_home_before_f6`.

### 10.4 Gates de F2/F2.5

| Comando | Resultado |
|---|---|
| `pytest tests/product_contract tests/contracts -q` | 23 passed |
| mutante "la fuente ignora el WHERE" | 4 tests caen |
| mutante "el ledger no registra" | 3 tests caen |
| mutante "confinamiento ciego" | 1 test cae |
| `pytest --collect-only -q tests` | 14.462 nodeids (14.439 + 23), 0 errores; el `conftest.py` anidado no rompe la importación |
| `ruff check tests/product_contract tests/contracts scripts/audits/test_architecture_inventory.py` | PASS |
| `semgrep --config .semgrep/secoppal-laws.yml` | no aplica: 0 archivos escaneados (H15) |

**Corrí los 23 tests nuevos, los mutantes y `--collect-only`, no la suite.**
