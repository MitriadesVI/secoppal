# M3C-E — Closeout del contrato conversacional de modalidad

- `DOCUMENT_STATUS: CURRENT_CANONICAL`
- `AUTHORITY_KIND: AS_BUILT`
- Fecha de cierre: 2026-07-25
- Modelo usado: GPT-5 Codex (sesión actual)
- Worktree: `/Users/rodrigoortiz/Documents/secoppal-modality`
- Branch: `search/modality-contract-v1`
- Base HEAD auditado: `3ea5dc671c2feb45e885c81dc1120d86f8a533fe`
- Alcance: auditoría, corpus, ataques, paridad, telemetría y cierre.
- Historia Git: no modificada. No se hizo staging, commit, merge, rebase ni push.

Este closeout es la única autoridad operacional as-built de M3C. Los documentos
anteriores conservan valor histórico o de diseño, pero ante cualquier
contradicción manda este documento.

## Resumen ejecutivo

M3C-B, su corrección R3 de asociación control↔target, M3C-C, el cierre
monotónico del allowlist LLM, la telemetría C→D y ER1A cumplen el contrato
documentado. La suite completa pasa sin regresiones: `6592 passed, 2 skipped,
24 xfailed`. El incremento de diez tests respecto del baseline (`6582`) proviene
exclusivamente de las diez secuencias M3C-E añadidas al corpus E2E.

La auditoría encontró un solo hueco contractual real: las diez secuencias
multi-turn obligatorias estaban cubiertas como tests E2E individuales, pero no
estaban congeladas en `dialogue_e2e_corpus.yaml`. Se añadieron 10 secuencias
(20 turnos) con expectativas declarativas de operación, filtro, topic, dataset y
forma del SoQL. El fixture queda en 29 secuencias / 64 turnos y sus invariantes
pasan `29/29`; conserva además un canario `xfail` del fallo basal ajeno a M3C.

La muestra de producto de modalidad sigue siendo inexistente:
`sample_is_real=false`, `modality_followup_turns_total=0`,
`m3c_d_recommendation=NOT_AUTHORIZED`. Por tanto:

```text
M3C-D DEFERRED
```

No se implementó `PendingModalityClarification`, sidecar, TTL, consumo
single-use ni override contextual de `QueryRouter`.

## Preflight

| control | esperado | observado | estado |
| --- | --- | --- | --- |
| branch | `search/modality-contract-v1` | igual | PASS |
| HEAD | `3ea5dc671c2feb45e885c81dc1120d86f8a533fe` | igual | PASS |
| staging previo | ninguno | ninguno | PASS |
| archivos productivos dirty | ninguno | ninguno | PASS |
| documentos no rastreados al inicio de M3C-E | al inicio de M3C-E, antes de crear este closeout, existían exactamente tres documentos no rastreados | exactamente los tres documentos históricos clasificados en este cierre | PASS |

## Commits incluidos

| hash | mensaje | archivos principales | sub-bloque | contrato cerrado | pruebas registradas |
| --- | --- | --- | --- | --- | --- |
| `f94ffbb` | `feat(modality): integrate canonical query pipeline` | 16 módulos `app/core`; 15 archivos de test | base M3B integrada | filtro canónico, proyección por dataset, precedencia, SoQL, frame/fingerprint y compatibilidad | integración de parser, ejecución, SoQL, frame, precedencia, policy y lectores |
| `33bbdd3` | `feat(modality): add pure conversational mutation algebra` | `modality_followup.py`; `test_modality_followup.py` | M3C-B motor puro | siete operaciones, construcciones cerradas, gate M2, fail-closed, no-op y trace | matriz algebraica, status, conflictos, propiedades e IDs desconocidos |
| `16da02e` | `fix(modality): bind follow-up controls to resolved target spans` | `modality_followup.py`; `test_modality_followup.py` | M3C-B R3 target-span | control asociado a spans M2; protección de verbos descriptivos y del `sin` jurídico; offsets | descriptivos, coordinaciones, construcciones ancladas, spans y fallback acotado |
| `4096a2c` | `feat(modality): wire conversational modality mutations` | `modality_followup_wiring.py`; `workflow/actions/parsing.py`; 2 tests E2E/wiring | M3C-C + allowlist | M2 una vez, operación fuera del merge genérico, rollback ortogonal, metadata efímera, allowlist monotónica LLM | E2E de operaciones/status/SoQL, persistencia, fingerprint, dataset, LLM hostil y una llamada M2 |
| `ec55a81` | `feat(modality): add follow-up product gate telemetry` | productor, integración `normal_flow`, analizador, 2 tests, diseño CG | telemetría / product gate | evento agregado privado, fail-open, deduplicación del analizador, procedencia explícita y umbral C→D | 56 tests del productor y 44 tests del analizador en el HEAD final |
| `3ea5dc6` | `fix(modality): preserve closed replacement target polarity` | wiring, precedence y 3 tests | ER1A | las construcciones `replace_all_except` y `clear_then_replace` preservan polaridad y masking | variantes cerradas, contrastes de exclusión, SoQL y precedencia |

No se reescribió historia.

## Arquitectura final

```text
QueryRouter.parse (una vez)
  └─ M2 resolve (una vez) → ModalityInterpretation + consumed_spans
       └─ classify_control (puro, consciente de target spans)
            └─ ModalityFollowupInstruction efímera

FollowupEngine clasifica intent y arma deltas ortogonales
  └─ apply_context
       ├─ gate M2 + apply_modality_followup_operation (puro)
       ├─ rollback total si la decisión bloquea
       └─ escribe solo include/exclude canónicos en params

QueryPlan → SoQLBuilder
  └─ proyecta procedure_ids al dataset final

normal_flow (post-persistencia)
  └─ productor opcional de telemetría agregada, fail-open
```

La instrucción, spans, operación y trace de control son efímeros. Solo el filtro
final canónico puede entrar a `parsed_params`, `QueryFrame`, fingerprint,
persistencia y SoQL.

## Operaciones soportadas

| operación | semántica cerrada |
| --- | --- |
| `REPLACE` | reemplaza el filtro previo por el target actual, preservando ambas polaridades |
| `ADD_INCLUDE` | une targets al include y los retira de exclude |
| `ADD_EXCLUDE` | une targets al exclude y los retira de include |
| `REMOVE_MEMBER` | retira targets de include y exclude |
| `REMOVE_EXCLUDE` | retira targets solo de exclude |
| `RESTORE_INCLUDE` | retira targets de exclude; los agrega a include solo si ya existía whitelist |
| `CLEAR` | vacía include/exclude únicamente mediante frases explícitas autorizadas |

`sin modalidad` no es `CLEAR`: queda ambiguo y bloquea. Una mutación idempotente
es éxito `no_op`, no error. Un target no ejecutable nunca se aplica parcialmente.

## Fronteras de autoridad

- M2 es la única autoridad jurídica sobre modalidad, procedure IDs, status,
  polaridad y consumed spans.
- El clasificador conversacional solo decide una operación del enum cerrado; no
  reconoce aliases jurídicos ni crea targets.
- El motor puro solo aplica álgebra sobre filtros canónicos.
- `FollowupEngine` conserva autoridad sobre intent y deltas ortogonales, pero no
  muta modalidad en paralelo.
- El builder es la única capa que proyecta procedure IDs a raws SECOP según el
  dataset final.
- El LLM no puede escribir modalidad, dataset, estado, fecha, monto, orden,
  intent ni campos futuros; solo completa campos asistidos aún vacíos y válidos.
- Los bloqueos de status M2, `target_missing` y `control_conflict` producen cero
  ejecución SECOP y cero SoQL.

## Allowlist LLM

La política es monotónica: parte de los params determinísticos y solo puede
llenar un hueco permitido. Campos asistidos: una única representación de topic
(`objeto` u `objeto_or`), `entidad`, `ciudad` o `departamento`, siempre con forma
canónica. Un campo resuelto no puede sobrescribirse. Propuestas múltiples de la
misma dimensión, formas inválidas, whitespace no canónico, campos cerrados,
campos desconocidos y borrados se rechazan. La telemetría del allowlist registra
solo nombres de claves, nunca valores.

## Precedencia y masking

1. Las construcciones cerradas ancladas se evalúan antes de señales atómicas.
2. No existe ranking numérico ni regla “primera señal gana”.
3. Señales equivalentes colapsan a una operación; señales incompatibles producen
   `control_conflict`.
4. Un candidato dentro o solapado con un consumed span M2 nunca es control.
5. Un `sin` dentro del nombre jurídico conserva la polaridad jurídica.
6. Solo spans de control exactos se enmascaran; topic sustantivo y offsets se
   conservan.
7. ER1A garantiza que el masking de una construcción cerrada no invierte el
   target negativo ni expande el contrato fuera de sus frases autorizadas.

## Matriz de cobertura

Los conteos son mínimos de casos independientes localizados durante la auditoría;
varios property/parametrized tests cubren combinaciones adicionales y un mismo
test puede proteger más de una categoría.

Abreviaturas: `pure` = `tests/test_modality_followup.py`; `pipe` =
`tests/test_modality_followup_pipeline.py`; `wire` =
`tests/test_modality_followup_wiring.py`; `integ` =
`tests/test_modality_pipeline_integration.py`; `prec` =
`tests/test_modality_precedence.py`; `dlg` =
`tests/test_dialogue_e2e_invariants.py`.

| categoría | casos existentes | cantidad mínima | archivos | hueco | acción/justificación |
| --- | --- | ---: | --- | --- | --- |
| REPLACE explícito | señales, reemplazo con scope, construcciones cerradas | 9 | pure, pipe, dlg | ninguno | E2E + puro |
| REPLACE implícito | target ejecutable, guards, `sin` jurídico descriptivo | 10 | pure, pipe | ninguno | E2E y matriz pura |
| ADD_INCLUDE | señales, unión, SoQL y corpus | 6 | pure, pipe, dlg | ninguno | E2E |
| ADD_EXCLUDE | señales, `sin` residual, exclusión y SoQL | 7 | pure, pipe, dlg | ninguno | E2E |
| REMOVE_MEMBER | señales, whitelist, no-op y corpus | 6 | pure, pipe, dlg | ninguno | E2E |
| REMOVE_EXCLUDE | señales, universo abierto y corpus | 5 | pure, pipe, dlg | ninguno | E2E |
| RESTORE_INCLUDE con whitelist | álgebra + E2E | 3 | pure, pipe | ninguno | E2E |
| RESTORE_INCLUDE en universo abierto | álgebra + E2E + corpus | 4 | pure, pipe, dlg | ninguno | E2E |
| CLEAR aplicado | seis frases, SoQL y corpus | 9 | pure, pipe, dlg | ninguno | E2E |
| CLEAR no-op | álgebra + no-op del motor | 2 | pure | ninguno | suficiente puro: estado final y trace son la totalidad del contrato |
| mutation no-op | add presente, remove ausente, remove-exclude ausente, clear vacío | 5 | pure, pipe | ninguno | incluye E2E |
| `control_conflict` | cuatro frases, pipeline y corpus | 7 | pure, pipe, dlg | ninguno | E2E bloqueado |
| sin modalidad ambiguo | puro, pipeline, telemetría | 5 | pure, pipe, telemetría | ninguno | E2E bloqueado |
| `target_missing` | gate, operación sin target y telemetría | 4 | pure, telemetría | ninguno | suficiente puro + productor: no depende del workflow |
| PARTIAL | gate puro, clasificador→motor, E2E | 4 | pure, pipe | ninguno | E2E bloqueado |
| AMBIGUOUS | gate puro, clasificador→motor, E2E + corpus | 6 | pure, pipe, dlg | ninguno | E2E bloqueado |
| CONFLICT M2 | gate puro + E2E | 2 | pure, pipe | ninguno | E2E bloqueado |
| NOT_SEARCHABLE sintético | fallback sintético, gate y capa pura | 3 | pure, pipe | ninguno | puro suficiente: hoy es inalcanzable desde texto y se construye el status exacto |
| DATASET_INAPPLICABLE | gate puro, clasificador→motor, E2E | 4 | pure, pipe | ninguno | E2E bloqueado |
| cambio de dataset | preservación de IDs, reproyección SoQL, corpus | 4 | pipe, dlg | ninguno | E2E |
| modalidad + topic | reemplazo descriptivo, topic compartido, corpus | 6 | pipe, integ, dlg | ninguno | E2E |
| modalidad + scope | consulta combinada + herencia de Barranquilla | 3 | pipe, integ | ninguno | E2E |
| modalidad + fecha | parser ortogonal + corpus M2 real | 2 | integ, corpus M2 | ninguno | prueba de parser suficiente; M3C no escribe fechas |
| modalidad + estado | colisiones `abierto/publicado` y precedencia | 8 | integ, prec | ninguno | integración real de parser/builder |
| modalidad + orden | allowlist cierra orden + follow-up de orden conserva frame | 2 | pipe, dlg | ninguno | E2E del frame; operación de modalidad no toca orden |
| `new_search` | reset con modalidad + corpus general | 3 | pipe, dlg | ninguno | E2E |
| `pagination_more` | no implicit replace + persistencia de contexto | 2 | pipe, dlg | ninguno | E2E |
| frame leak | params/resolved/persisted/blocked | 10 | pipe | ninguno | E2E + store real |
| fingerprint | mismo filtro final, orden estable, golden previo | 14 | pipe, integ | ninguno | E2E + 12 goldens |
| persistencia | turnos resueltos, clear, no-op y bloqueado | 7 | pipe | ninguno | ConversationStore real |
| SoQL | include, exclude, clear, dataset y construcción cerrada | 8 | pipe, integ, dlg | ninguno | E2E con cliente SECOP mockeado |
| LLM adversarial | modalidad, dataset, estados, borrado, formas y campos cerrados | 40+ | pipe, integ | ninguno | frontera ejecutada directamente y E2E |
| allowlist monotónica | fill vacío, no overwrite, dimensión topic, forma canónica | 45+ | pipe | ninguno | parametrizados exhaustivos |
| construcciones cerradas | replace-all-except, clear-then-replace, anclaje y ER1A | 18+ | pure, pipe, prec, dlg | ninguno | E2E + SoQL |
| verbos descriptivos | `incluye`, `permite`, `usa`, `excluye`, `elimina`, `agrega` | 15+ | pure, pipe, dlg | ninguno | E2E |
| offsets | spans válidos, masking exacto, unicode/puntuación | 20+ | pure, wire, prec | ninguno | puro suficiente: contrato posicional determinístico |
| M2 una sola llamada | seis caminos parametrizados + construcción cerrada + LLM | 8 | pipe | ninguno | E2E con spy |

## Corpus multi-turn congelado

Se añadieron exactamente estas diez secuencias:

| id | T1 → T2 | contrato |
| --- | --- | --- |
| `DLG-M3C-ADD-INCLUDE` | licitación pública → además contratación directa | unión + topic heredado |
| `DLG-M3C-REMOVE-MEMBER` | LP o CD → quita CD | queda LP |
| `DLG-M3C-REMOVE-EXCLUDE` | alimentación excepto CD → permite CD | universo abierto |
| `DLG-M3C-RESTORE-OPEN` | alimentación excepto CD → vuelve a incluir CD | restaura sin whitelist |
| `DLG-M3C-CLEAR` | LP mantenimiento → sin filtro de modalidad | clear + topic |
| `DLG-M3C-CONTROL-CONFLICT` | CD alimentación → agrega y quita CD | bloqueo + rollback |
| `DLG-M3C-AMBIGUOUS-TARGET` | CD alimentación → además subasta | bloqueo M2 + rollback |
| `DLG-M3C-DATASET-PIVOT` | subasta en procesos → ahora contratos | IDs sobreviven y se reproyectan |
| `DLG-M3C-MODALITY-PLUS-TOPIC` | LP mantenimiento → CD que incluye suministro | verbo descriptivo + topic nuevo |
| `DLG-M3C-CLEAR-THEN-REPLACE` | LP mantenimiento → no importa modalidad, solo subasta | include positivo, exclude vacío, topic heredado, sin `NOT IN` |

Resultado focal del corpus E2E: `29 passed, 1 xfailed`; el `xfail` no es una
secuencia, sino el canario `test_known_bug_detect_direct_lookup_false_positive`.

## Matriz adversarial

| ataque | clasificación | aserción contractual |
| --- | --- | --- |
| `además alimentación` | NONE | NO_MATCH no crea target ni operación |
| `sin alimentación` | NONE | no confunde topic con exclusión de modalidad |
| `solo alimentación` | NONE | no activa REPLACE |
| `quita estado abierto` | NONE | no toca estado |
| `permite procesos cerrados` | NONE | no toca dataset/estado |
| `quita licitación y agrega mínima cuantía` | BLOCKED | `control_conflict`, filtro previo intacto |
| `sin modalidad` | BLOCKED | ambiguo; nunca CLEAR implícito |
| `elimina todas` | NONE | no es frase CLEAR autorizada |
| `solo esas` | NONE | no hay target M2 |
| `quita la anterior` | NONE | no existe referencia jurídica inventada |
| modalidad jurídica con `sin` | SAFE_OPERATION | `sin` dentro del span conserva include positivo |
| descriptivo `incluye` | SAFE_OPERATION | no se vuelve ADD_INCLUDE |
| descriptivo `permite` | SAFE_OPERATION | no se vuelve REMOVE_EXCLUDE |
| descriptivo `usa` | SAFE_OPERATION | no se vuelve control espurio |
| LLM cambia dataset | FAIL_CLOSED | dataset determinístico preservado |
| LLM cambia modalidad | FAIL_CLOSED | filtro canónico preservado |
| LLM agrega estado incompatible | FAIL_CLOSED | estado cerrado rechazado |
| LLM intenta borrar params | FAIL_CLOSED | merge parte del incoming |
| LLM devuelve campo futuro | FAIL_CLOSED | clave desconocida descartada |
| metadata efímera corrupta | FAIL_CLOSED | deserialización estricta; no muta |
| spans corruptos | FAIL_CLOSED | tipo/forma/rango rechazados |
| IDs desconocidos | FAIL_CLOSED | `unknown_procedure_id`; filtro previo intacto |

En todos los casos BLOCKED/FAIL_CLOSED se comprueba estado, operación, filtro,
SoQL/ejecución o excepción contractual específica; no se usa “no lanzó
excepción” como única aserción.

## Paridad sin operación

| camino | evidencia | paridad congelada |
| --- | --- | --- |
| consulta sin modalidad | 12 goldens pre/post | fingerprint, params, route y marcadores SoQL idénticos |
| consulta inicial con modalidad | integración canónica | mismos params ortogonales; solo se añade filtro canónico |
| follow-up de topic | `DLG-TOPIC-CHANGE-KEEPS-SCOPE` | intent/plan/ejecución conservan scope |
| follow-up ciudad/departamento | `DLG-SCOPE-CHANGE-KEEPS-TOPIC`, `DLG-CITY-TO-DEPT` | topic y ejecución conservados |
| follow-up fecha | `DLG-BLOCK-RECOVER-BY-DATE` | fecha sigue gobernada por policy existente |
| follow-up monto | `DLG-BLOCK-RECOVER-BY-VALUE` | monto califica sin operación de modalidad |
| follow-up orden | `DLG-VALUE-PERSISTS-ON-ORDER` | valor, territorio y ejecución sobreviven |
| paginación | `DLG-PAGINATION-KEEPS-CONTEXT` + test dedicado | intent `pagination_more`; no REPLACE implícito |
| `new_search` | dos secuencias + test de modalidad | reset de contexto conforme a policy |

Canarios transversales verifican que metadata M3C no entra a params,
`QueryPlan`, fingerprint ni persistencia. Cuando existe filtro previo, se hereda;
cuando la policy define reset/new search, se limpia según la conducta ya
congelada.

## Telemetría instalada

### Productor

- `source="runtime"` fijo.
- `event_id` UUID v4 aleatorio.
- desactivado por defecto (`modality_followup_telemetry_path=None`);
- emite solo follow-ups con señal de modalidad, una vez por turno;
- no contiene `user_query`, `chat_id`, procedure IDs, params, SOQL, resultados,
  URL ni valores de usuario;
- corre después de la persistencia y no modifica la respuesta;
- errores de proyección/escritura fallan abiertos y el warning no filtra datos.

### Analizador

- read-only, stdlib pura, sin red ni imports de `app`;
- valida esquema, UUID y timestamp UTC;
- deduplica por `event_id` antes de cualquier conteo;
- solo cuenta `source=runtime`;
- la procedencia real exige declaración explícita y nunca se infiere;
- códigos fuera de allowlist colapsan a `unknown_code`;
- `target_missing` y `control_conflict` tienen buckets propios y no entran al
  bucket jurídico M2.

### Probes ejecutados

| escenario | test/ejecución | resultado |
| --- | --- | --- |
| fixture sintético | `test_synthetic_provenance_is_preserved` | sintético, no autoriza |
| archivo vacío | `test_zero_records_is_insufficient_sample` + CLI sobre ruta ausente | 0 turnos, no autoriza |
| duplicados | `test_duplicate_record_is_excluded` | excluido antes del conteo |
| UUID inválido | `test_invalid_event_id_is_excluded` | registro inválido excluido |
| código desconocido | `test_unknown_clarification_code_is_bucketed_not_echoed` | `unknown_code` |
| muestra marcada no real | `test_synthetic_sample_never_authorizes_without_explicit_real` | no autoriza |
| muestra real insuficiente | `test_99_real_records_never_meet_minimum` | no autoriza |

Reporte actual:

```text
sample_is_real = false
modality_followup_turns_total = 0
modality_followup_blocked_total = 0
m3c_d_recommendation = NOT_AUTHORIZED
```

## Compuerta C→D y decisión M3C-D

Condición necesaria de reapertura:

```text
sample_is_real = true
turns_total >= 100
```

Y además al menos una:

```text
blocked_total >= 10
```

o:

```text
blocked_by_m2_status_total / turns_total >= 0.05
blocked_by_m2_status_total >= 5
```

`target_missing` y `control_conflict` no cuentan dentro de
`blocked_by_m2_status_total`.

Decisión formal:

```text
M3C-D DEFERRED
```

M3C-D no se reabre hasta satisfacer la compuerta con muestra real.

## Resultados de validación

| validación | comando/resultado |
| --- | --- |
| py_compile | `python -m py_compile tests/test_dialogue_e2e_invariants.py scripts/analyze_modality_followup_gate.py app/core/modality_followup_telemetry.py` — PASS (ejecutado con el Python del venv) |
| focal del archivo modificado | `pytest tests/test_dialogue_e2e_invariants.py -q` — `29 passed, 1 xfailed` |
| probes del analizador | siete tests seleccionados — `7 passed` |
| lint core | `make lint-core` — `app/core/ limpio` |
| focal contractual | ocho archivos — `626 passed, 1 xfailed` |
| suite completa | `pytest -q` — `6592 passed, 2 skipped, 24 xfailed` |
| query corpus | `run_query_corpus.py --mode all` — `89/89 PASS` |
| dialogue policy corpus | `run_dialogue_corpus.py --mode all` — `60/61 PASS` |

En este entorno `python` no existe en `PATH`; todos los comandos Python se
ejecutaron mediante
`/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.

## Fallo basal

Único fallo aceptado del corpus:

```text
GOB-DEPTO-001
entidad_resolved: expected 'GOBERNACIÓN DE BOYACÁ', got None
```

Firma idéntica al baseline registrado. No está relacionado con modalidad y no se
corrige en M3C-E.

## Clasificación documental del eventual commit

| documento | clasificación | razón | recomendación |
| --- | --- | --- | --- |
| `2026-07-23_M3A_modality_integration_contract_design.md` | HISTORICAL_USEFUL | registra el mapa pre-M3B, decisiones de migración y riesgos que explican la arquitectura final | versionar como linaje auditable |
| `2026-07-23_M3C_modality_conversation_contract.md` | HISTORICAL_DESIGN_BASELINE | registra el diseño aprobado al inicio de M3C; la implementación incorporó enmiendas posteriores | versionar como baseline histórico de diseño |
| `2026-07-24_M3C_B_R3_target_span_control.md` | HISTORICAL_DECISION_RECORD | registra las enmiendas bloqueantes y la geometría control↔target que produjo R3 | versionar como registro histórico de decisión |
| `2026-07-25_M3C_modality_conversation_closeout.md` | CURRENT_CANONICAL | consolida el comportamiento implementado, validado y diferido; es la única autoridad operacional | versionar como autoridad as-built |

Ninguno de los tres documentos históricos es `SUPERSEDED` ni `REDUNDANT`; por
ello se recomiendan los tres para el eventual commit de cierre, junto con este
closeout y los dos archivos de corpus. Esta recomendación no hace staging.

## Deuda conocida

Se registra sin corregir:

- `GOB-DEPTO-001`;
- ruido de verbos descriptivos en topic (`incluye`, `permite`, `quien`);
- seis causas reales del fallback LLM:
  1. `Colombia` queda como objeto;
  2. `Cali` requiere desambiguación ciudad vs. departamento;
  3. `Barranquilla` requiere desambiguación ciudad vs. departamento;
  4. typo `cundinamarcar`;
  5. saludos `hola` / `estimado` quedan como topic;
  6. `algo` queda como topic;
- muestra real de modalidad todavía inexistente;
- M3C-D diferido.

Estas deudas no forman parte del cierre y no justifican cambios productivos en
M3C-E.

## Recomendación del próximo bloque

1. Revisar para commit el closeout, el fixture y su test, junto con los tres
   documentos de diseño clasificados.
2. Tras el eventual despliegue de M3C-C, habilitar explícitamente la ruta local
   de telemetría en el canal real y acumular muestra.
3. Volver a ejecutar el analizador con procedencia real auditable.
4. No planificar M3C-D hasta que el reporte satisfaga literalmente la compuerta.

## Veredicto

```text
M3C_B_STATUS: COMPLETE
M3C_C_STATUS: COMPLETE
M3C_ER1A_STATUS: COMPLETE
M3C_D_DECISION: M3C-D DEFERRED
PRODUCTION_FILES_MODIFIED: NONE
COMMIT_MERGE_REBASE_PUSH: NO

FINAL_VERDICT:
M3C_E_READY_FOR_COMMIT_REVIEW
```
