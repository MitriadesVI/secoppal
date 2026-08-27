# SECOPPAL — HCA Final Re-Audit Post Systemic Remediation Campaign Closeout V2

**Fecha:** 2026-08-27

**Rama:** `audit/hca-final-closeout-v2`

**Main real auditado:** `1a11f0c659ce3ad0b5ff418b45b7c7ae82313051`

**Modo:** audit-only

**Veredicto:** `HCA_FINAL_REAUDIT: BLOCKED_FOR_NEW_RUNTIME_BUG`

Este documento **supercede** el intento de closeout del 2026-08-26. No lo
borra: el audit anterior quedó bloqueado, la remediación sistémica fue
integrada, y esta reauditoría vuelve a bloquear por hallazgos realmente nuevos
del lote sorpresa V2.

```text
previous final audit: BLOCKED_FOR_NEW_RUNTIME_BUG
systemic remediation: INTEGRATED
final reaudit: BLOCKED_FOR_NEW_RUNTIME_BUG
HCA_CAMPAIGN: OPEN
```

## 1. Resumen ejecutivo

Los gates congelados sí son estables. El HCA canónico reproduce exactamente
**188 PASS / 7 FAIL / 0 ERROR**, sin un solo PASS→FAIL y con los mismos siete
casos adjudicados. El predictivo endurecido da **12/12**, sus **7/7** sentinels
y detecta las **5/5** mutaciones. El fresh original reproduce **37/52**, sin
P0/P1 de runtime: quedan las mismas doce expectativas P3, dos fronteras P2 y
`FRESH-CONTROL-005` reducido a singularización de oracle. El canario vivo es
conclusivo y la suite Python 3.12 termina **9393 passed, 2 skipped, 24 xfailed,
0 failed**.

Eso no basta para cerrar. El lote Fresh Surprise V2 fue escrito y hasheado
antes de ejecutarse, no comparte ninguna consulta con HCA, fresh original ni
predictive, y contiene 36 escenarios / 43 turnos. Su resultado crudo fue
21 PASS / 15 FAIL; la adjudicación semántica encuentra **2 escenarios P0 y 12
P1 de runtime**. Un PASS crudo adicional escondía fuga estructural al tema. La
política de cierre exige cero P0/P1 nuevos, así que el veredicto obligado es
`BLOCKED_FOR_NEW_RUNTIME_BUG`. Este bloque no corrige ninguno.

La evidencia completa está en
[2026-08-27_HCA_FINAL_CLOSEOUT_V2](evidence/2026-08-27_HCA_FINAL_CLOSEOUT_V2/).

## 2. Preflight y alcance

`main` real resolvió a `1a11f0c659ce…`, exactamente la base declarada. La rama
`audit/hca-final-closeout-v2` nació desde ese commit. Había cero cambios
tracked y 178 rutas untracked preexistentes; todas se preservaron. No se hizo
reset, clean, merge, rebase ni push.

| Gate de alcance | Resultado |
|---|---:|
| `app/**` modificado por auditoría | 0 |
| Fixtures modificados | NO |
| Tests productivos modificados | 0 |
| Fresh original modificado | NO |
| Predictive expectations modificadas | 0 |
| Push | NO |

Los runners estructurales usaron el workflow real, fuente SECOP simulada y
stores temporales. El canario fue read-only, `LIMIT 5`, sin persistir filas.
La suite completa usó Python 3.12.12 con feedback, SQLite y conversaciones
redirigidos a `/private/tmp`.

## 3. Inputs congelados

| Input | SHA-256 | Gate |
|---|---|---:|
| HCA | `a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e` | MATCH |
| Fresh original | `72a0c95f8a30019f77fbb3f6e9c82dfc851ccf3cb1d050607313086ce850ecce` | MATCH |
| Predictive baseline | `68179d0bcbcfbfac5aa7a925f8ec78c261dd151daca22f66d3794866c914f414` | MATCH |
| Predictive expectations | `9d18eae36a82c4af4585c6920aa9ab3df3356b9f48a31597067842d8274148a8` | MATCH; cambios 0 |
| Fresh V2 blind plan | `151b1e4f6fdabbdd43340a38145acd37109f482e70902e2273f759e4721f36e1` | escrito pre-ejecución |

## 4. HCA canónico

| Métrica | Resultado |
|---|---:|
| Casos | 195 |
| PASS | 188 |
| FAIL | 7 |
| ERROR | 0 |
| Cambios case-by-case | 0 |
| PASS→FAIL | 0 |
| Bugs de runtime HCA | 0 |
| Oracle debt HCA | 0 |

Los siete FAIL son exactamente:

| Caso | Clasificación actual |
|---|---|
| `HCA-TOPIC-012` | `SAFE_UNSUPPORTED` |
| `HCA-MODALITY-002` | `DEFERRED` |
| `HCA-MODALITY-012` | `DEFERRED` |
| `HCA-TYPO-003` | `DEFERRED` |
| `HCA-ENTITY-004` | `INSUFFICIENT` |
| `HCA-STATE-003` | `INSUFFICIENT` |
| `HCA-SOURCE-002` | `INSUFFICIENT` |

`SEMANTICALLY_SUPPORTED` sigue siendo **189/195**, definido únicamente como
188 PASS del oracle canónico más un caso seguro que falla cerrado
(`HCA-TOPIC-012`). No es score del runner, porcentaje de accuracy ni cobertura
completa del español.

## 5. Predictive hard gate y no vacuidad

```text
PREDICTIVE:              12 / 12
SENTINELS:                7 / 7
EXPECTATION_CHANGES:      0
MUTATIONS_DETECTED:       5 / 5
INSTRUMENTED_SOURCE_I/O:  query, count, aggregate
BLOCKED_SOURCE_CALLS:     0
```

Las mutaciones `STATE`, `MONEY`, `SPELL`, `BUYER` y `CONTROL` fueron detectadas
por su propia familia. Los sentinels de estado negado:

- `procesos que no estén abiertos`;
- `procesos que no estén en convocatoria`;

aclaran, no proyectan estado positivo y hacen cero llamadas. No se observó
inversión, drop ni bypass de texto crudo en esas formas congeladas.

Dinero congelado también cumple:

- `entre 700 mil y 900 mil pesos` → `700000 / 900000`;
- `entre 700 mil y 900 mil` → aclaración, cero llamadas;
- `entre 500 mil y 3 millones` → aclaración, ninguna cota parcial.

`otra` no se convierte en `obra`, y los typos reales siguen corrigiéndose. Las
familias soportadas `participar`, `licitar`, `ofertar`, `presentarme` preservan
intención de oportunidad sin fugas al objeto. El test E2E de reset conserva a
la vez `needs_clarification=true`, `outcome=clarification`,
`reason=clarification_requested`, cero llamadas y ausencia del mensaje falso de
zero-after-refinement.

## 6. Fresh original

La corrida exacta de 52 escenarios / 58 turnos reproduce **37/52**, sin cambios
de estado respecto del post-remediation.

```text
NEW_RUNTIME_P0: 0
NEW_RUNTIME_P1: 0
P2:             2
P3:            12
```

`FRESH-CONTROL-005` reconoce correctamente `licitar`, selecciona procesos,
declara `opportunity_search + oferta_abierta` y no fuga control al tema. La
única discrepancia restante es `uniformes` en oracle frente a `uniforme`
canónico, por lo que queda `ORACLE_SINGULARIZATION_ONLY`, no bug de runtime.

Los otros catorce FAIL crudos conservan sus clases anteriores. El inventario
case-by-case con `CURRENT_BEHAVIOR`, `PRIOR_CLASS`, `CURRENT_CLASS` y `CHANGED`
está en [fresh_original_results.json](evidence/2026-08-27_HCA_FINAL_CLOSEOUT_V2/fresh_original_results.json).

## 7. Fresh Surprise V2

### Diseño blind

El plan contiene 36 escenarios / 43 turnos en seis grupos: state, money,
spell, buyer, control y cruces dimensionales. Cada escenario fija antes de
ejecutar `QUERY`, `USER_INTENT`, `EXPECTED_DIMENSIONS`, `EXECUTE/CLARIFY` y
`SAFETY_EXPECTATION`. Hubo cero coincidencias exactas de query contra HCA,
fresh original o predictive.

### Resultado

| Métrica | Resultado |
|---|---:|
| Raw PASS | 21 |
| Raw FAIL | 15 |
| ERROR | 0 |
| Coincidencias semánticas | 22/36 |
| P0 runtime | 2 |
| P1 runtime | 12 |
| P2 | 0 |
| P3 oracle | 2 |

Los dos P3 son representaciones legítimas: `obra` como topic en procesos y la
misma representación después de corregir `obrra`. El typo real se preserva; el
oracle V2 exigía erróneamente `contract_type_include`.

### Hallazgos que bloquean

| Causa | Sev. | Casos | Conducta observada |
|---|---:|---|---|
| `FLEXIBLE_NEGATED_STATE_FAIL_OPEN_OR_DIMENSION_COLLAPSE` | P0 | STATE-006, CROSS-007 | “jamás hayan sido publicados” ejecuta el filtro positivo Publicado; “no estén seleccionados” se degrada a exclusión textual de objeto y ejecuta. |
| `MONEY_RANGE_GRAMMAR_FAIL_OPEN` | P1 | MONEY-002/003/005, CROSS-012 | Rango heterogéneo se transforma en `1M..950M`; otros rangos pierden ambas cotas y ejecutan la consulta restante. |
| `UNRESOLVED_STRUCTURAL_CONTROL_EXECUTES_AS_TOPIC` | P1 | SPELL-001, CONTROL-001/005, CROSS-008 | Reset/paginación no resueltos heredan el universo y consultan palabras de control como contenido. |
| `BIDDER_LEAD_FORM_DROPS_OPPORTUNITY_CONSTRAINT` | P1 | CONTROL-003/004, CROSS-009 | `concursar/competir` con nuevos introductores ejecuta procesos históricos sin `opportunity_search` ni `oferta_abierta`. |
| `STATE_ASPECT_TOKEN_LEAKS_INTO_TOPIC` | P1 | STATE-004 | El estado activo es correcto, pero `continúen` se vuelve filtro textual obligatorio; fue un PASS crudo engañoso. |

No se exige soporte universal para español. El defecto adjudicado es más
acotado: cuando la forma estructural no se resuelve, el runtime ejecuta una
consulta distinta en vez de fallar cerrado. Eso viola directamente el hard
gate posterior a ADR-069.

Evidencia completa, incluida la conducta actual por caso, en
[fresh_v2_results.json](evidence/2026-08-27_HCA_FINAL_CLOSEOUT_V2/fresh_v2_results.json).

## 8. Canario vivo

El canario repitió el caso que antes declaró `oferta_abierta` mientras
consultaba Cerrado/Seleccionado. Esta vez fue concluyente:

| Métrica | Resultado |
|---|---:|
| Fuente disponible | YES |
| Canarios ejecutados | 3 |
| Canarios no vacuos | 3 |
| Filas vistas | 12 |
| Checks semánticos ejecutados | 3 |
| Violaciones semánticas | 0 |
| Violaciones hard-filter | 0 |
| Veredicto | PASS |

El canario de estado vio cinco filas y proyectó únicamente la familia abierta.
Los otros dos verificaron el rango pequeño canónico y buyer action. Esto prueba
los casos vivos consultados; no neutraliza los hallazgos Fresh V2.

## 9. Focales, corpora y suite

| Gate | Resultado |
|---|---:|
| Final P1 | 40/40 |
| Actor | 36/36 |
| Control | 42/42 |
| Time | 32/32 |
| Negation | 22/24 (`NEG-N15`, `NEG-ANTI-002` basales) |
| Query corpus | 89/89 |
| Dialogue corpus | 60/61 (`GOB-DEPTO-001` basal) |
| Targeted systemic tests Python 3.12 | 736/736 |
| Targeted parity Python 3.14.5 | 736/736 |
| Full pytest Python 3.12 | 9393 pass, 2 skip, 24 xfail, 0 fail |

El primer intento de full suite habilitó por entorno la telemetría opcional y
falló únicamente el test que exige que esa opción esté deshabilitada por
default. Se corrigió el entorno externo, no código; el rerun completo anterior
es el gate autoritativo. Ambos intentos se registran en
[full_suite_python312.json](evidence/2026-08-27_HCA_FINAL_CLOSEOUT_V2/full_suite_python312.json).

## 10. F601 y arquitectura

```text
F601_CONFIGURED:               YES
F601_MANUAL_INTEGRATION_GATE:  YES
F601_CI_ENFORCED:              NO
F601_FINDINGS:                 0
```

`CI_LINT_ENFORCEMENT` permanece deuda de tooling no bloqueante para HCA.

`app/core/query_plan.py` conserva **371 líneas físicas**, **299 LoC efectivas**
y límite 300. `QUERY_PLAN: AT_LIMIT`; `NEXT_TOUCH_RULE: ACTIVE`. La remediación
introdujo `state_vocabulary.py` y `stopwords.py`, y reforzó autoridades
separadas de estado, dinero, intención, control, follow-up, parsing y spell. No
se introdujo ni se afirma un “universal semantic manager”.

## 11. Cuatro principios finales

| Principio | Veredicto | Deuda concreta |
|---|---|---|
| `USER_INTENT_SURVIVES_NORMALIZATION` | PARTIAL | Rangos V2 y formas flexibles de estado/control cambian o pierden intención. |
| `DIMENSIONS_DO_NOT_COLLAPSE_INTO_TOPIC` | FAIL | Estado negado, aspecto de estado, reset y paginación se degradan a filtros de objeto. |
| `FOLLOWUPS_PRESERVE_UNIVERSE_UNLESS_CHANGED` | PARTIAL | Ordering soportado conserva universo; reset/paginación no resueltos heredan y mutan el tema. |
| `UNSUPPORTED_OR_UNRESOLVED_STRUCTURAL_INTENT_FAILS_CLOSED` | FAIL | Fresh V2 demuestra ejecución en 14 escenarios P0/P1. |

## 12. Delta de campaña

| Etapa | HCA PASS | Naturaleza |
|---|---:|---|
| Inicial | 156 | Baseline |
| Mejora de runtime HCA | 182 | `+26` runtime |
| Corrección de oracle | 188 | `+6` oracle; no puntos de runtime |
| Remediación sistémica fresh | 188 | HCA sin cambio; P0/P1 del fresh original `>0 → 0` |
| Reaudit Fresh V2 | 188 | HCA sin cambio; aparecen 2 P0 + 12 P1 nuevos |

No se combinan scores ni se presenta un porcentaje agregado.

## 13. Deuda separada

Decisiones de producto todavía pendientes:

- `ONLY_MODALITY_SCOPE_POLICY`
- `BUYER_SUBJECT_VS_TERRITORY_POLICY`
- `LIQUIDATION_QUERY_SEMANTICS`
- `LIQUIDATION_SOURCE_DATASET_CAPABILITY`
- `CONTROL_LIMIT_SEMANTICS`
- `DEFERRED_FOLLOWUP_SCOPE_TOPIC`

Deuda conocida no bloqueante:

- `NEG-N15`
- `NEG-ANTI-002`
- `GOB-DEPTO-001`
- `change_scope topic loss`
- `SEMANTIC_AUTHORITY_CLEANUP`
- `QUERY_PLAN_AT_LIMIT`
- `CI_LINT_ENFORCEMENT`

Ninguna de esas deudas explica el bloqueo. Lo explican los nuevos hallazgos de
runtime V2 enumerados en la sección 7.

## 14. Aislamiento

Todos los runners reportaron `outside_sandbox: []`. Los SHA-256 de
`data/secoppal.db` y `data/feedback.jsonl` fueron idénticos antes y después. El
mtime de feedback cambió durante la ventana por un proceso concurrente, pero
el contenido fue byte-idéntico y las corridas de auditoría estaban redirigidas.

```text
AUDIT_ISOLATION: PASS
AUDIT_ATTRIBUTABLE_PRODUCTION_WRITES: 0
RUNTIME_FILES_CHANGED_BY_AUDIT: 0
FIXTURES_CHANGED: NO
USER_DEVELOPMENT_PROCESSES_STOPPED: NO
```

## 15. Veredicto final

```text
HCA_FINAL_REAUDIT: BLOCKED_FOR_NEW_RUNTIME_BUG

HCA:             188 / 195
PASS_TO_FAIL:      0
HCA_RUNTIME_BUGS:  0
HCA_ORACLE_DEBT:   0

PREDICTIVE:       12 / 12
SENTINELS:          7 / 7
MUTATIONS:          5 / 5

FRESH_ORIGINAL:    37 / 52
ORIGINAL_P0/P1:     0 / 0

FRESH_V2:          22 / 36 semantically matched
NEW_RUNTIME_P0:     2
NEW_RUNTIME_P1:    12

LIVE_CANARY:       PASS, 12 rows, 0 semantic, 0 hard-filter
FULL_SUITE_312:    9393 pass, 2 skip, 24 xfail, 0 fail

HCA_CAMPAIGN: OPEN
PUSH: NO
NEXT: NEW_RUNTIME_REMEDIATION_REQUIRED
```

No se declara 97% accuracy, 195/195, complete language coverage ni cierre de
campaña. Los gates canónicos prueban estabilidad; Fresh V2 prueba que la
generalización exigida para cerrar todavía no está satisfecha.
