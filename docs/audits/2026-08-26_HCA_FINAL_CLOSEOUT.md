# SECOPPAL — HCA Final Audit: Holistic Conversational Campaign Closeout

**Fecha:** 2026-08-26

**Rama:** `audit/hca-final-closeout`

**Base canónica:** `92476c5f2edba5be91a60c3592a5da6f86f3b1ab`

**Modo:** audit-only; sin correcciones de runtime, fixtures ni oráculos

**Veredicto:** `HCA_FINAL_AUDIT: BLOCKED_FOR_NEW_RUNTIME_BUG`

## 1. Executive Summary

El estado canónico es reproducible: la corrida nueva obtuvo **188 PASS / 7 FAIL / 0 ERROR** sobre 195 casos, con SHA canónico `a341405a…`, exactamente los siete fallos adjudicados y **cero PASS→FAIL** frente al baseline posterior al Oracle Patch V1. En ese universo siguen siendo semánticamente sostenidos **189/195**: 188 PASS del runner más `HCA-TOPIC-012`, que falla cerrado de forma correcta. Esto no es un score de 189 del runner ni una estimación estadística de accuracy.

Las focales mantienen sus baselines; query corpus da 89/89, dialogue corpus conserva únicamente `GOB-DEPTO-001`, y la suite completa termina con **8657 passed, 2 skipped, 24 xfailed, 0 failed**. El runner y todas las focales reportan `outside_sandbox: []`; no se atribuye ninguna escritura en datos productivos a la auditoría. No se modificó runtime, tests productivos, fixtures ni el corpus HCA.

La campaña, sin embargo, **no puede cerrarse**. El desafío fresco pre-registrado de 52 escenarios y 58 turnos deja 41 coincidencias semánticas, un P0, ocho P1, dos P2 y doce P3. Hay **nueve escenarios con bug de runtime agrupados en siete causas raíz**. El P0 ignora una negación de estado y ejecuta el universo positivo. Además, un canario de fuente descubrió que `oportunidades vigentes` declara `oferta_abierta` pero consulta `Cerrado/Seleccionado`. Por la política explícita del encargo, cualquier P0/P1 nuevo obliga a `BLOCKED_FOR_NEW_RUNTIME_BUG`; no se corrigió ninguno.

## 2. Scope

La auditoría partió del `main` real `92476c5…` y creó `audit/hca-final-closeout`. Se preservaron los numerosos untracked históricos y los worktrees existentes. El alcance de escritura quedó limitado a este informe, su evidencia y la bitácora canónica.

Gates de alcance:

| Gate | Resultado |
|---|---:|
| Runtime (`app/**`) modificado | 0 |
| Tests productivos modificados | 0 |
| Fixtures modificados | 0 |
| Corpus HCA modificado | NO |
| Merge / push | NO / NO |

La evidencia completa está en [2026-08-26_HCA_FINAL_CLOSEOUT](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/).

## 3. Method

La secuencia fue: preflight y hash; corrida HCA nueva y aislada; comparación por los 195 `CASE_ID`; revalidación individual de los siete fallos; cinco suites focales; corpora query/dialogue; plan adversarial escrito antes de ejecutar; adjudicación manual de discrepancias; seis GET de solo lectura y `LIMIT 5` contra Socrata; comparación de universos en follow-ups; suite completa; lint/static; medición arquitectónica; atribución de escrituras; gates Git.

El runner estructural deshabilita el LLM remoto (`deepseek_api_key=None`), simula las filas de SECOP y stubbea narración; su propósito es verificar parser, políticas, follow-up, resolutores y SoQL de forma determinística. `PYTHONHASHSEED=0`; Python 3.14.5; zona civil America/Bogota. Las rutas efectivas de feedback, SQLite, conversaciones, Radar y telemetría de modalidad quedaron dentro del temporal indicado por [isolation.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/isolation.json).

El fresh challenge no miró la implementación al formular sus consultas: [fresh_adversarial_plan.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/fresh_adversarial_plan.json) registra primero consulta, semántica pretendida, dimensiones esperadas y expectativa de ejecutar/aclarar. La adjudicación posterior separa el resultado crudo del runner de la coincidencia semántica.

## 4. Final Metrics

### HCA canónico

| Métrica | Resultado |
|---|---:|
| Casos | 195 |
| Runner PASS | 188 |
| Runner FAIL | 7 |
| ERROR | 0 |
| Cambios de estado frente al baseline | 0 |
| PASS→FAIL | 0 |
| Semánticamente sostenidos | 189/195 |

Definición: **189 = 188 runner PASS + 1 SAFE_UNSUPPORTED que aclara sin ejecutar**. Los resultados completos están en [hca_verdicts.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/hca_verdicts.json) y [hca_summary.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/hca_summary.json).

El SHA del corpus coincide con el baseline actual:

```text
a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e
```

El SHA `262c225c…` se conserva únicamente como procedencia pre-patch.

### Cuatro principios finales

| Principio | Veredicto | Evidencia |
|---|---|---|
| `USER_INTENT_SURVIVES_NORMALIZATION` | PARTIAL | El HCA, dinero unitario y tiempo focal pasan, y el monto verbal compuesto aclara sin parcializar. Pero dos rangos pequeños pierden silenciosamente su cota inferior y los verbos de compra alteran dataset/tema. |
| `DIMENSIONS_DO_NOT_SILENTLY_COLLAPSE_INTO_TOPIC` | PARTIAL | Modalidad, tiempo, estados y tipos contractuales canónicos se estructuran. En fresh, `compró`, `pagina` y la frase de reset terminan como contenido. |
| `FOLLOWUPS_PRESERVE_UNIVERSE_UNLESS_USER_CHANGES_IT` | PARTIAL | Ordering-only conserva WHERE en Meta e ICBF. La variante cortés de paginación y el reset natural mutan o heredan indebidamente el universo. |
| `UNSUPPORTED_OR_AMBIGUOUS_INTENT_FAILS_CLOSED` | FAIL | TOPIC-012, monto compuesto, buyer negativo y tiempo negativo aclaran con seguridad; la negación `no estén publicados` se ignora y ejecuta el universo positivo. |

## 5. Historical Delta

| Punto | PASS/195 | Delta | Naturaleza |
|---|---:|---:|---|
| Initial | 156 | — | Baseline |
| Post R1/R3/R4 | 164 | +8 | Runtime |
| Time R5 | 171 | +7 | Runtime |
| Control R6 | 175 | +4 | Runtime |
| Actor R7 | 177 | +2 | Runtime |
| Final Runtime | 182 | +5 raw | Runtime; también cerró FOLLOWUP-016 sin mover el raw score |
| Oracle Patch V1 | 188 | +6 | Correcciones de oracle, no mejora de runtime |

El runtime movió el score crudo en 26 casos; el Oracle Patch corrigió seis expectativas después de adjudicación independiente. [campaign_delta.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/campaign_delta.json) conserva los IDs exactos de cada etapa.

## 6. Root Causes Closed

“Cerrado” en esta tabla se refiere a la causa y casos HCA originales. Cuando el fresh challenge expuso una variante adyacente, se declara de forma explícita; no se usa el cierre canónico para ocultarla.

| Root cause | Estado final | Invariante | Autoridad | Casos HCA corregidos |
|---|---|---|---|---|
| `DIRECT_LOOKUP_HIJACK` | CLOSED | La referencia requiere estructura numérica; lenguaje natural no entra por prefijo corto | `direct_lookup.py`, handler | INTENT-010 |
| `NORMALIZATION_ERASES_INTENT` | CLOSED original; variante abierta | La normalización no vuelve plausible una contradicción ni cierra un rango temporal abierto | `query_router.py`, `temporal_semantics.py`, `money.py` | MONEY-021, ANTI-004 |
| `DIMENSIONAL_NEGATION` | CLOSED V1; variante P0 abierta | La negación se bindea antes del extractor positivo; dimensiones no soportadas aclaran | `negation_binding.py`, `negation_policy.py` | NEG-003/005/006 |
| `TEMPORAL_SEMANTICS` | CLOSED para R5 | Gana el span específico; granularidad y extremos se conservan | `temporal_semantics.py`, `temporal_gate.py` | TIME-003/004/008/010/012/014/015 |
| `CONTROL_LANGUAGE_TO_TOPIC` | CLOSED V1; variantes P1 abiertas | Control no es contenido; ordering-only no cambia WHERE | `control_language.py` | INTENT-007/008, STATE-006, ORDER-004 |
| `ACTOR_ROUTING_HYGIENE` | CLOSED | EXACT > selección > fuzzy; uppercase no es evidencia; fuzzy no autoriza identidad | actor resolution/arbitration | ACTOR-011, META-002B |
| `MONEY_ADMISSION` | CLOSED canónico; rango pequeño abierto | Dinero explícito no se descarta por magnitud; compuesto no soportado no ejecuta parcial | `money_admission.py`, `money_resolution.py`, `money.py` | MONEY-020/026 |
| `STATE_PROJECTION` | CLOSED canónico; oportunidad abierta | Estado exacto no se ensancha; familia declarada se proyecta completa y coherente | `estado_families.py`, `estado_projection.py` | STATE-005/007 |
| `GENERIC_BUYER_AS_TOPIC` | CLOSED no calificado; verbo de compra abierto | Comprador genérico sin calificar aclara y no se vuelve tema | `generic_buyer.py`, router | ENTITY-005 |
| `DERIVED_EXECUTION_STATE_PERSISTENCE` | CLOSED para ordering | Estado derivado no persiste; ordenar sólo cambia ORDER BY | `derived_execution_state.py`, `conversation_store.py` | FOLLOWUP-016 |

El catálogo machine-readable, con fixtures y módulos, está en [root_causes_closed.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/root_causes_closed.json).

## 7. Remaining Seven

Los siete FAIL son exactamente los esperados; ninguno es un bug de runtime conocido ni deuda confirmada de oracle dentro del HCA actual.

| Caso | Clasificación | Revalidación / decisión pendiente |
|---|---|---|
| `HCA-TOPIC-012` | SAFE_UNSUPPORTED | Conserva salud y Amazonas, reconoce colisión actor/tema, aclara, no ejecuta. `SAFE_FAIL_CLOSED_STILL_CORRECT: YES`. |
| `HCA-MODALITY-002` | KNOWN_DEFERRED_PRODUCT_DEBT | Modalidad, dataset y 2026 correctos; sin fuga temática; guard aclara. `ONLY_MODALITY_SCOPE_POLICY`. |
| `HCA-MODALITY-012` | KNOWN_DEFERRED_PRODUCT_DEBT | Igual frontera only-modality; no consulta global. |
| `HCA-TYPO-003` | KNOWN_DEFERRED_PRODUCT_DEBT | Régimen especial se reconoce; scope insuficiente aclara. |
| `HCA-ENTITY-004` | INSUFFICIENT_EVIDENCE | Ejecuta Atlántico territorial; falta `BUYER_SUBJECT_VS_TERRITORY_POLICY`. |
| `HCA-STATE-003` | INSUFFICIENT_EVIDENCE | No hay autoridad para equiparar liquidado con Cerrado. `LIQUIDATION_QUERY_SEMANTICS: UNDECIDED`. |
| `HCA-SOURCE-002` | INSUFFICIENT_EVIDENCE | El pivot actual a contratos no decide semántica, fecha ni fuente de liquidación. |

Totales: **1 safe, 3 deferred, 3 insufficient, 0 runtime bug, 0 oracle debt**. Detalle en [remaining_cases.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/remaining_cases.json).

## 8. Fresh Adversarial Challenge

El lote contiene 52 escenarios, 58 turnos y diez categorías: dinero, estado, buyer, follow-up, actor, negación, tiempo, control, lookup y cruces dimensionales. El runner crudo dio 30 PASS / 22 FAIL / 0 ERROR. La adjudicación semántica dio **41 coincidencias**, porque doce FAIL crudos eran errores P3 de expectativa y un PASS crudo —`FRESH-STATE-002`— escondía una proyección semántica contradictoria descubierta por el canario.

| Severidad | Cantidad | Interpretación |
|---|---:|---|
| P0 | 1 | Bug de runtime: negación de estado fail-open |
| P1 | 8 | Bugs de runtime en ocho escenarios |
| P2 | 2 | Evidencia/autoridad insuficiente |
| P3 | 12 | Expectativa exploratoria errónea; conducta primaria correcta o segura |

### Bugs nuevos que bloquean

| Causa raíz | Sev. | Escenarios | Conducta observada |
|---|---:|---|---|
| `NEGATIVE_STATE_CONJUGATION_FAIL_OPEN` | P0 | NEGATION-005 | Ignora “no estén publicados” y consulta obra/Caldas sin exclusión ni aclaración. |
| `SMALL_MONEY_RANGE_LOWER_BOUND_LOSS` | P1 | MONEY-003/005 | `entre 120 mil y 350 mil` y `desde 300 mil hasta 900 mil` pierden `valor_min`. |
| `OPPORTUNITY_STATE_FAMILY_PROJECTION_CONTRADICTION` | P1 | STATE-002 | Declara `oferta_abierta`, consulta `Cerrado/Seleccionado`. |
| `BUYER_PURCHASE_VERB_TO_TOPIC_AND_DATASET` | P1 | BUYER-002, CROSS-004 | `compró` selecciona procesos y aparece como tema. |
| `PAGINATION_POLITE_VARIANT_NOT_RECOGNIZED` | P1 | FOLLOWUP-004 | “muéstrame la siguiente página” se vuelve `objeto=pagina`. |
| `RESET_NATURAL_VARIANT_NOT_RECOGNIZED` | P1 | FOLLOWUP-005 | La frase de limpiar conserva Caldas/2024 y ejecuta palabras de control como tema. |
| `BIDDER_LICITAR_INTENT_NOT_ENFORCED` | P1 | CONTROL-005 | `licitar` usa procesos sin oportunidad ni familia abierta. |

Los P2 son la frontera buyer/territorio ya indecisa y una referencia sintética cuya gramática no tiene autoridad. Los doce P3 incluyen representaciones canónicas legítimas (`obra` como tipo, Bogotá como distrito, escuelas→educación), bloqueo seguro de dinero compuesto/negaciones y direct lookup válido. [fresh_adversarial_results.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/fresh_adversarial_results.json) contiene cada adjudicación.

El score fresco es exploratorio y **no se suma** a 188/195.

## 9. Source Canaries

Se hicieron seis GET de solo lectura contra Socrata, un intento por caso, `LIMIT 5`, sin persistir filas:

| Canario | Infraestructura | Filas | Violación hard-filter | Contrato semántico |
|---|---|---:|---:|---|
| Small money | OK | 5 | 0 | PASS |
| Published exact | OK | 5 | 0 | PASS |
| Opportunity family | OK | 5 | 0 | **FAIL** |
| Complete buyer | OK | 1 | 0 | PASS |
| Follow-up ordering | OK | 5 | 0 | PASS; WHERE idéntico |
| Direct lookup real | OK | 1 | 0 | PASS |

Los seis endpoints respondieron y las filas respetaron los filtros efectivamente enviados: **hard-filter violations = 0**. El estado agregado es, no obstante, **FAIL**, porque el canario de oportunidades envió un SoQL semánticamente equivocado: `estado_family=oferta_abierta` coexistió con `Cerrado/Seleccionado`. No es un fallo de Socrata; es un P1 del runtime antes de la fuente. Evidencia en [source_canaries.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/source_canaries.json).

### Universo de follow-up

Meta/Obra/2025 conservó exactamente el WHERE entre T1 y T2 y sólo cambió el orden. El caso ICBF/Antioquia también conservó el ecosistema explícito:

```text
departamento = 'Antioquia'
AND unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%ICBF%'))
```

Esto prueba el cierre para las variantes de ordering cubiertas, no para paginación o reset recién descubiertas. Véase [followup_universe.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/followup_universe.json).

## 10. Regression Suites

| Suite / corpus | Resultado | Lectura |
|---|---:|---|
| Final P1 | 40/40 | PASS |
| Actor | 36/36 | PASS |
| Control | 42/42 | PASS |
| Time | 32/32 | PASS |
| Negation | 22/24 | NEG-N15 y NEG-ANTI-002 basales; `NEGATION_FOCAL_ORACLE_DEBT_REMAINS: YES` |
| Query corpus | 89/89 | PASS |
| Dialogue corpus | 60/61 | Sólo `GOB-DEPTO-001`, `PREEXISTING_OUTSIDE_HCA` |
| Full pytest | 8657 pass, 2 skip, 24 xfail, 0 fail | PASS |

`make lint-core` pasa. Ruff, según la invocación E/W/F vigente, reporta 264 hallazgos preexistentes (181 autofixables); no se modificaron en un bloque audit-only y no hay regresión de archivos cambiados. Semgrep no está disponible: `SEMGREP: TOOL_UNAVAILABLE`; no se instaló ni se afirma PASS. `git diff --check` pasa.

Detalles en [focal_results.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/focal_results.json), [full_suite_summary.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/full_suite_summary.json) y [static_results.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/static_results.json).

## 11. Architectural Health

`app/core/query_plan.py` mide **299 LoC efectivas de un máximo de 300** (371 físicas). Su estado permanece `QUERY_PLAN: AT_LIMIT`; `NEXT_TOUCH_RULE: ACTIVE`. Cualquier futura modificación funcional debe extraer primero responsabilidad suficiente; el límite no se sube.

Otros módulos medidos: dialogue policy 288 efectivas, control language 286, temporal semantics 394, actor resolution 214, actor arbitration 261 y la acción workflow de actor resolution 270. Esta auditoría no los modificó. Snapshot en [architecture_snapshot.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/architecture_snapshot.json).

## 12. Known Non-Blocking Debt

| Deuda | HCA blocking | Estado |
|---|---|---|
| `NEG-N15` | NO | Deuda focal/oracle fuera del HCA holistic |
| `NEG-ANTI-002` | NO | Deuda focal/oracle fuera del HCA holistic |
| `GOB-DEPTO-001` | NO | Mismatch histórico del dialogue corpus |
| change_scope topic loss | NO | Implementación diferida ya documentada |
| `CONTROL_LIMIT_SEMANTICS` | NO | Capacidad de display limit no decidida |
| `SEMANTIC_AUTHORITY_CLEANUP` | NO | Deuda arquitectónica |
| `QUERY_PLAN_AT_LIMIT` | NO por sí sola | Restricción activa para el próximo touch |

Estos elementos no son la razón del bloqueo actual. La razón son los siete bugs de implementación frescos, incluido un P0.

## 13. Product Decisions Still Needed

Se mantienen separadas de bugs de implementación:

| Decisión/capacidad | Tipo | Casos o alcance |
|---|---|---|
| `ONLY_MODALITY_SCOPE_POLICY` | Product decision | MODALITY-002/012, TYPO-003 |
| `BUYER_SUBJECT_VS_TERRITORY_POLICY` | Product decision | ENTITY-004 |
| `LIQUIDATION_QUERY_SEMANTICS` | Product decision | STATE-003 |
| `LIQUIDATION_SOURCE_DATASET_CAPABILITY` | Product decision | SOURCE-002 |
| `CONTROL_LIMIT_SEMANTICS` | Deferred capability | Fuera de los siete HCA |
| `DEFERRED_FOLLOWUP_SCOPE_TOPIC` | Deferred capability | Change-scope con tema nuevo |

No se decidió ninguna durante esta auditoría. El inventario separado está en [debt_and_decisions.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/debt_and_decisions.json).

## 14. Final Verdict

```text
HCA_FINAL_AUDIT: BLOCKED_FOR_NEW_RUNTIME_BUG
HCA_CAMPAIGN: OPEN
RAW_HCA: 188 / 195
SEMANTICALLY_SUPPORTED: 189 / 195
CANONICAL_PASS_TO_FAIL: 0
NEW_FRESH_RUNTIME_BUG_SCENARIOS: 9
NEW_FRESH_RUNTIME_ROOT_CAUSES: 7
RUNTIME_FILES_CHANGED_BY_AUDIT: 0
```

La base canónica sí es estable respecto de sus 195 identidades y sus suites. Lo que falla es la condición de generalización exigida para cerrar: el lote nuevo encuentra P0/P1 reales. No se declara `COMPLETE_FEATURE_COVERAGE`, 195/195 ni accuracy de producto. La campaña permanece abierta y este documento registra un bloqueo honesto, no un cierre artificial.

El análisis de atribución confirma `AUDIT_ATTRIBUTABLE_PRODUCTION_WRITES: 0`; `data/feedback.jsonl` tuvo actividad de un proceso Streamlit ajeno cuyos eventos más recientes preceden la corrida HCA y no usan ningún prefijo de auditoría. Véase [production_writes.json](evidence/2026-08-26_HCA_FINAL_CLOSEOUT/production_writes.json).

## 15. Next Recommended Product Work

Abrir un sprint acotado para las siete causas frescas, empezando por el P0 de negación de estado y la contradicción de oportunidades; después, repetir **sin cambiar sus oráculos** el mismo plan fresh y todos los gates de esta auditoría.
