# HCA Final P1 — Runtime Integration Review

**Fecha:** 2026-08-26. **Veredicto:** PASS. **Integración:** fast-forward confirmado.
No se implementaron features, correcciones runtime ni cambios de fixtures/oráculos durante el review.

## 1. Preflight y cadena real

- Main antes / merge-base: `eb8cfe1ed72ba5c9df0ab9c0ead5089ff5d09368`.
- Feature y main tras fast-forward: `dec589a21bf36844d595db80093e7824ea2a0189`.
- `MAIN_RELATION: FAST_FORWARD_AVAILABLE`; main era ancestro y no había avanzado.
- Rama conservada: `feature/hca-final-p1-runtime-fixes`. Sin merge commit y sin push.
- 191 rutas untracked preexistentes preservadas. Ningún cambio tracked al comenzar.
- Dos worktrees detached aislaron base y feature. Los procesos del usuario no se tocaron.
- `git diff --check` limpio; `query_plan.py` byte-idéntico, 299 LoC según el contador del repo
  (371 líneas físicas). `NEXT_TOUCH_RULE: PRESERVED`.

| COMMIT | CLUSTER | RUNTIME / TEST / DOC | PURPOSE | EXPECTED_IN_MAIN |
|---|---|---|---|---|
| `f85f209` | MONEY | RUNTIME / FIXTURE / EVIDENCE | Admisión explícita; singular millón; conversión sin reemplazo | YES |
| `d63fa26` | STATE | RUNTIME / TEST | Selector exacto y proyección de familia completa | YES |
| `a8aa645` | GENERIC BUYER | RUNTIME | Comprador incompleto aclara; no es tema ni entidad arbitraria | YES |
| `9169b8e` | FOLLOWUP UNIVERSE | RUNTIME / TEST / FIXTURE | No persistir expansión derivada como intención | YES |
| `dc1bf79` | ADR-067 / ADR-068 / audit | DOC / TEST / MAKEFILE | ADRs, informe, golden STATE y target reproducible | YES |
| `6719f2d` | Audit findings | DOC | Registrar dos hallazgos basales fuera de alcance | YES |
| `dec589a` | Final evidence | DOC | Cerrar cifras finales de suite del sprint | YES |

Los tres últimos commits tienen prefijo documental, pero `dc1bf79` también modifica tests y Makefile.
Se revisó su contenido real. `UNRELATED_COMMITS = 0`.

## 2. Diff acumulado

20 archivos; 1855 inserciones, 39 eliminaciones. Runtime 9, tests 4, fixture 1,
docs 4, Makefile 1, herramienta de evidencia 1, unrelated 0.

| Clase | Archivo |
|---|---|
| RUNTIME | `app/core/conversation_store.py` |
| RUNTIME | `app/core/derived_execution_state.py` |
| RUNTIME | `app/core/entity_resolver.py` |
| RUNTIME | `app/core/estado_families.py` |
| RUNTIME | `app/core/estado_projection.py` |
| RUNTIME | `app/core/generic_buyer.py` |
| RUNTIME | `app/core/money_admission.py` |
| RUNTIME | `app/core/query_router.py` |
| RUNTIME | `app/utils/money.py` |
| TESTS | `tests/test_modality_pipeline_integration.py` |
| TESTS | `tests/test_ordering_followup_source_universe.py` |
| TESTS | `tests/test_pagination_buffer.py` |
| TESTS | `tests/test_plan_query_node.py` |
| FIXTURES | `tests/fixtures/audits/hca_final_p1_fixes.yaml` |
| DOCS | `docs/audits/2026-08-26_HCA_FINAL_P1_RUNTIME_FIXES.md` |
| DOCS | `docs/decisions/2026-08-26_ADR-067_exact-state-vs-state-family.md` |
| DOCS | `docs/decisions/2026-08-26_ADR-068_derived-execution-state.md` |
| DOCS | `docs/decisions_log.md` |
| MAKEFILE | `Makefile` |
| EVIDENCE | `scripts/audits/probe_query.py` |

**Sin cambios contractuales ni de autoridad** en Actor Profile identity authority,
Actor Relationships, Control Limit, only-modality scope policy, liquidation semantics,
buyer-vs-territory unresolved policy, source-capability liquidation, Semantic Registry runtime,
Market Packs, Warehouse, Radar, SODA3 o morphological semantic cleanup.
Los módulos y catálogos específicos de esas superficies no están en el diff. El router compartido
sólo recibe las integraciones MONEY, STATE y GENERIC BUYER; el store sólo filtra la clave derivada.

## 3. Autoridades nuevas

| Módulo | Responsabilidad | Callers | Efectos | Autoridad duplicada / alcance |
|---|---|---|---|---|
| money_admission | Admitir evidencia monetaria explícita o aplicar piso anti-ruido | QueryRouter._extract_amounts | Ninguno; puro, sin I/O | No convierte ni decide polaridad; consume gramática de app/utils/money.py |
| estado_projection | Materializar familia declarada para dataset, salvo filtro ya acotado | QueryRouter.parse | Muta el dict privado del parser; sin I/O | Consume ESTADO_FAMILIES; no cambia membership ni política de oportunidad |
| generic_buyer | Detectar mención institucional incompleta | QueryRouter y EntityResolver | Ninguno; devuelve decisión de aclaración | Unifica vocabulario; no fuzzy, lookup, identidad ni nuevo entity resolver |
| derived_execution_state | Quitar entidad_ecosystem del estado persistible | ConversationStore.append_turn | Devuelve copia; el store existente realiza I/O | No cambia el followup engine; listas locales previas de paginación/radar siguen intactas |

`EACH_MODULE_ONE_RESPONSIBILITY = YES`. Las listas anteriores que limpian la misma clave
en otras fronteras no se refactorizaron ni se presentan como eliminadas por este sprint.
ADR-067 se limita a exacto/familia, no declara resuelta liquidación ni todo el catálogo SECOP.
ADR-068 coincide con el filtro en append_turn; no introduce event sourcing ni reescribe ConversationStore.
Invariante verificado: `INV-DERIVED-EXECUTION-STATE-NOT-PERSISTED-AS-USER-INTENT`.

## 4. Revisión crítica de tests

**plan_query_node: JUSTIFIED.** Cambia exactamente el escalar `estado_contrato = 'En ejecución'`
por `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')`.
ADR-003 ya asignaba esa familia a “en ejecución”; ADR-067 materializa ese contrato y
ESTADO_FAMILIES conserva idénticos miembros. **5 asserts antes / 5 después**;
los guards que lanzan si se consulta la fuente, la aclaración y el risk flag permanecen.

**modality_golden: JUSTIFIED. MODALITY_STRUCTURAL_DELTA = 0.**
La función de test conserva AST idéntico y **7/7 asserts**. Sólo una entrada de golden cambia:
`393770fc…` → `efe365c8…`, y su marcador STATE escalar → familia completa.
Se compararon 12 entradas golden y 4 consultas adicionales con modalidad explícita.
Interpretation completo, included/excluded IDs, clarification policy, route y needs_llm son idénticos.
Con `PYTHONHASHSEED=0`, sustituir únicamente la proyección STATE autorizada en el SoQL de feature
reproduce byte por byte el SoQL base. No se normalizaron filtros para ocultar cambios.

**pagination_nonvacuous: YES.** **5 → 6 asserts**: el espía confirma que la expansión post-query
ocurrió, y el historial no contiene el derivado. Página 2 sale del buffer y la fuente original
no contiene el comprador inyectado. Sin el fix de persistencia falla este test en la aserción
de historial: no puede pasar por ausencia de expansión.

**Mutación reproducida en proceso aislado**, sin editar archivos runtime: reemplazar sólo
`strip_derived_execution_state` por `dict` produce **6 fallos / 4 PASS** en los 10 tests de
ordering, más **1 fallo** en paginación: **7 FAIL / 4 PASS** en conjunto.
La cifra histórica “4 failures” no describe la suite final actual; la dependencia causal sí queda probada.
La ejecución sin mutación pasa los **82 tests** focales.
`UNSAFE_TEST_CHANGES = 0`.

## 5. Seis defectos y sentinels

| Caso | Base | Feature | Evidencia |
|---|---|---|---|
| HCA-ENTITY-005 | FAIL; alcaldía como tema | PASS | Aclaración, cero query/count/aggregate, sin LIKE de alcaldía |
| HCA-MONEY-020 | FAIL; monto omitido | PASS | valor_min=100000 y valor_del_contrato >= 100000 |
| HCA-MONEY-026 | FAIL; singular omitido | PASS | valor_min=1000000 y predicado correspondiente |
| HCA-STATE-005 | FAIL; funnel ensanchado | PASS | estado_del_procedimiento = 'Publicado' |
| HCA-STATE-007 | FAIL; escalar truncado | PASS | contrato_activo; En ejecución + Modificado + Prorrogado |
| HCA-FOLLOWUP-016 | Runtime WHERE drift; raw FAIL | Runtime cerrado; raw FAIL | WHERE idéntico; único fallo restante objeto_has:[obra] |

**MONEY.** “1 millón”, “2 millones” y “500 millones” producen 1M/2M/500M sin regresión.
El piso de 1M permanece en `MIN_BARE_AMOUNT_COP` para números sin evidencia; no se borraron
indiscriminadamente todos los guards. Año 2025, NIT 900739049, 100 procesos y 100 desnudo
no producen dinero. La referencia CO1.PCCNTR.1234567 mantiene su defecto basal valor_max=1234567,
sin arreglarlo. “más de un millón y medio de pesos”, aislado y dentro de una consulta de papelería,
aclara, no produce cota parcial y realiza **0 source calls**:
`SAFE_UNSUPPORTED_OR_DEFERRED_GRAMMAR`; `PARTIAL_MONEY_EXECUTION = 0`.

**STATE.** “publicados” no agrega Borrador/En aprobación/Aprobado; `EXACT_STATE_WIDENING = 0`.
“oportunidades de aseo en cesar” conserva byte por byte el predicado fuente base:
Publicado + Abierto y ventana O1B, no Publicado-only.
“procesos desiertos” ya declaraba no_procede en base y ahora proyecta Cancelado + Suspendido:
`CAUSAL_SAME_ROOT_FIX`. Catálogo idéntico en base y feature; propiedad ejecutada sobre los
**11 pares familia/dataset** declarados, omitiendo campos vacíos, y los seis selectores exactos
permanecen acotados. HCA-STATE-003 y HCA-SOURCE-002 conservan frames/source behavior,
incluido dataset contratos y ausencia de estado de liquidación: **NO CONTRACTUAL CHANGE**.

**BUYER.** Medellín, alcaldía local de Teusaquillo y aseo + alcaldía de Medellín resuelven
comprador y ejecutan; el tercero mantiene tema. El caso “contratos del proveedor LMP Ingeniería
con la alcaldía” conserva comportamiento basal, incluida la falta de separación correcta:
`IDENTICAL_PREEXISTING`, sin invadir Actor Relationships. También se mantiene el caso de
Fondo Nacional de Vivienda; no se incorporó un alias o nueva capacidad.

## 6. FOLLOWUP-016: source universe y persistencia

T1: `contratos de obra en cali por mas de 300 millones`.
T2: `los mas antiguos primero`.

```sql
-- WHERE1
unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Cali%')) AND valor_del_contrato >= 300000000 AND (tipo_de_contrato = 'Obra')
-- WHERE2
unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Cali%')) AND valor_del_contrato >= 300000000 AND (tipo_de_contrato = 'Obra')
```

Sólo cambia ORDER BY de fecha/valor/id DESC a ASC; LIMIT sigue en 50 y no hay cursor en este caso.
La comparación retira exclusivamente ORDER BY/paginación, no compradores ni filtros.
El OR de diez compradores que aparecía en T2 base desaparece. Dinero, Cali y tipo Obra permanecen.
`FILTERS(WHERE1) == FILTERS(WHERE2)`, `WHERE_DRIFT = 0`.
ICBF/Antioquia y SENA/Antioquia sobreviven a ordering-only con los mismos predicados explícitos.
`EXPLICIT_ECOSYSTEM_PRESERVED = YES`.

La expansión derivada aún puede enriquecer la respuesta actual; append_turn no la guarda como
intención del usuario. El frame persistido de T1 base tenía entidad_ecosystem; feature no.
El ecosistema solicitado viaja en entidad_resolved/entidad_like y no se elimina.

Re-adjudicación: `PRIMARY_RUNTIME_BUG: CLOSED`; `SECONDARY_ORACLE_DEBT: CONFIRMED`.
El raw HCA sigue FAIL **únicamente** por `objeto_has:[obra]`; obra está estructuralmente presente
en contract_type_include y en tipo_de_contrato. Ningún fixture se editó durante este review.

## 7. Corpus, cobertura y alcance

| Corpus | Base PASS | Feature PASS | PASS→FAIL |
|---|---|---|---|
| hca | 177/195 | 182/195 | 0 |
| hca-final-p1 | 26/40 | 40/40 | 0 |
| negation-v1 | 22/24 | 22/24 | 0 |
| time-v2 | 32/32 | 32/32 | 0 |
| control-language-v1 | 42/42 | 42/42 | 0 |
| actor-hygiene-v1 | 36/36 | 36/36 | 0 |

HCA: **177 PASS / 18 FAIL / 0 ERROR → 182 PASS / 13 FAIL / 0 ERROR**.
Se compararon los **195 IDs**, no sólo el net delta; **BROKEN = 0**.
SHA256 original intacto:
`262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`.
Se instrumentaron **251 turnos por revisión** (corpus completo más sentinels), capturando
query/count/aggregate y el historial persistido. Los agregados de Actor se compararon como
multiconjunto porque se ejecutan concurrentemente; no cambiaron consultas ni autoridad de identidad.
Al comparar frames sólo se excluyó el nonce aleatorio selection_token, no semántica del actor.

Los diez turnos HCA con delta real en ejecución pertenecen a los mismos fixes:
ENTITY-005, MONEY-020/026, STATE-005/007, INTENT-012 (publicados), STATE-008 (borrador exacto),
FOLLOWUP-003/004/016 (eliminar ecosistema derivado heredado). No se encontraron cambios de
source contract fuera de esas causas.

**Focal versionado.** La revisión inicial en `f85f209` reproduce **25/40** sobre eb8cfe1.
El fixture final reproduce **26/40** sobre la misma base y **40/40** sobre feature.
`9169b8e` corrige dos `soql_not:[nombre_entidad]` que rechazaban la columna en SELECT,
reemplazándolos por el predicado OR de comprador; P1-FOLLOWUP-002 deja de ser falso fallo basal.
Es un ajuste del nuevo focal dentro del sprint, no una edición del HCA congelado.

**Sentinels no readjudicados:** CONFIRMED_ORACLE_DEBT ACTOR-004, TOPIC-007, ENTITY-001,
FOLLOWUP-007 y META-001B; SAFE TOPIC-012; DEFERRED MODALITY-002/012 y TYPO-003;
INSUFFICIENT ENTITY-004, STATE-003 y SOURCE-002. Mantienen frames/source behavior;
las diferencias de wording de error no se usaron para cambiar clasificación.

Query corpus **89/89** en base y feature. Dialogue **60/61**, JSON idéntico en ambos;
único fallo **GOB-DEPTO-001**, basal. Negación conserva **NEG-N15 y NEG-ANTI-002**;
ningún fixture previo cambió de SHA.

## 8. Suite final, lint y aislamiento

- Suite autoritativa desde el HEAD final: **8657 passed, 2 skipped, 24 xfailed, 0 failed**.
  Worktree feature byte-idéntico en archivos versionados, con copia local de sólo lectura del
  índice de actores. No se instaló nada ni se modificó el artefacto del usuario.
- La primera corrida sin ese recurso fue **8648 passed, 11 skipped, 24 xfailed**;
  nueve skips de índice se eliminaron en la repetición completa, no se sumaron corridas parciales.
- Focal de tests de integración, aislamiento, estado y repins: **82 passed**.
- `make lint-core`: PASS. Ruff sobre los 14 Python cambiados: PASS con ruff.toml del repo.
- `git diff --check eb8cfe1..dec589a` y working tree: PASS.
- `SEMGREP: TOOL_UNAVAILABLE`: no ejecutable en PATH/venv ni paquete instalado.
  No se instaló. Inspección manual equivalente de persistencia: la nueva sonda construye
  `Settings(**stores.settings_overrides, deepseek_api_key=None)` dentro de isolated_runtime_stores,
  antes de importar app; el workflow recibe esos settings. Tests excluidos por la ley y aislados
  por conftest. No se afirma semgrep PASS ni ejecución de sus demás reglas.
- Todos los isolation.json declaran **outside_sandbox=[]**. El test black-box de runner
  preserva el data/ centinela. Ventana real de **1.493 segundos**,
  inicio `2026-08-26T14:36:05.214961+00:00`, channel telegram / chat_id hca1-HCA-FOLLOWUP-016:
  **0 archivos de producción cambiados; 0 escrituras atribuibles**. No se usó un hash global
  de horas ni se tocó Streamlit/uvicorn.

## 9. Veredicto previo y fast-forward

El veredicto PASS se emitió al usuario antes de cambiar main y quedó preservado en
`integration-preverdict.json`. Se revalidaron refs, ancestry y ausencia de cambios tracked.
`git switch main` seguido de `git merge --ff-only feature/hca-final-p1-runtime-fixes`
terminó exactamente en `dec589a21bf36844d595db80093e7824ea2a0189`. **7 commits integrados; 0 merge commits; 0 push**.

Post-merge: `make audit-hca-final-p1` **40/40** y **82 tests** de dinero/estado/comprador/universo
(el focal corpus cubre dinero y buyer; pytest cubre estado, repins, paginación y followups).
No se repitió full pytest después: fast-forward byte-idéntico a la revisión autoritativa.
Los untracked previos se preservaron; la rama feature no se borró.
El cierre de bitácora y este informe/evidencia quedan como cambios documentales locales,
**sin commit adicional**, para conservar main exactamente en el HEAD solicitado.

## 10. Cierre canónico y Oracle Patch Plan posterior

```text
HCA_FINAL_P1_RUNTIME: CLOSED / INTEGRATED
REAL_RUNTIME_BUGS: 6 / 6 CLOSED
RAW_HCA: 182 / 195
SEMANTICALLY_SUPPORTED: 183 / 195 → 189 / 195
189/195 != runner score
REMAINING: 3 KNOWN_DEFERRED_PRODUCT_DEBT + 3 INSUFFICIENT_EVIDENCE
NEXT: READY_FOR_HCA_ORACLE_PATCH
```

189 = 182 PASS + 5 oracle debts confirmadas previas + 1 SAFE + 1 deuda secundaria FOLLOWUP-016.
Los 13 raw FAIL son 6 de oracle debt + 1 SAFE + 3 diferidos + 3 sin evidencia suficiente.
No se declara FINAL_HCA completo.

**Addendum al Oracle Patch Plan de 2026-08-26:** se habilita **HCA-FOLLOWUP-016** una vez
cerrado su defecto runtime. En el patch posterior, sustituir la exigencia `objeto_has:[obra]`
por el contrato estructural `contract_type_include:[obra]`, con source predicate tipo_de_contrato=Obra,
dinero 300M, Cali y ordering fecha_asc conservados. Mantener el test de igualdad de WHERE como
evidencia independiente; no relajar la frontera de universo. Las cinco deudas confirmadas previas
siguen en el plan original. SAFE no se reclasifica por conveniencia; DEFERRED e INSUFFICIENT
no reciben patch aquí. **Este review no aplica ningún cambio a fixtures**.

## Evidencia reproducible

Artefactos en `docs/audits/evidence/2026-08-26_HCA_FINAL_P1_RUNTIME_INTEGRATION_REVIEW/`: comparison.json, static-audit.json,
source-evidence base/feature, verdicts de todos los corpus, baseline focal inicial,
logs de mutación, suites, query/dialogue, aislamiento, veredicto previo y promoción.
`review_probe.py` reproduce runtime real con cliente SECOP/narrador simulados y stores aislados;
ejecutar desde cada revisión con `PYTHONHASHSEED=0`. **No se presentan filas sintéticas como
validación de datos vivos SECOP**; la auditoría prueba el contrato de las llamadas a la fuente.
