# M2A-4 — Aclaración pendiente: gate de entrada y cierre

**Rama:** `mission/m2a-4-pending` (base `d7f53e9b` = `main` `5a829d5d` + registro del DAG de la ola 2).
**Contrato:** R3d sellado (`7dd39b4b…`). No se modifica su texto ni se re-sella: las deudas se cierran en el código y en
las pruebas de M2A-4 y se registran aquí (precedente U29). **Deudas de entrada:** `M2A4_M2A6_ENTRY_DEBTS.md` (U28).

Esta primera sección es el **gate de entrada** (escrita antes de implementar). Las secciones de implementación,
mutantes y gates se añaden al final de la rama y no cambian estas decisiones.

## 1. Gate de entrada: las deudas de M2A-4 y su cierre

Ninguna deuda exige cambiar texto sellado, re-sellar ni deja un estado alcanzable sin respuesta representable. Cada una se
cierra del lado que falla cerrado (defaults del orquestador). Dónde vive cada decisión: `app/core/foundation_pending*.py`
y `tests/test_foundation_pending*.py`.

| Id | Deuda | Decisión | Cierre |
|---|---|---|---|
| **M2A4-D1** | P1-1: cancelar `CONTEXT_EXPIRED_CONFIRM` no exige ¬Continuación(P) | Con esa pendiente, «reset + fragmento»: el resto de P es búsqueda nueva **sólo si ¬Continuación(P) ∧ Admisible sola(P)**; si no, `CLARIFY NO_ANCHOR`. Nunca se compone sobre el `frame` vencido. Motivo: A9 («con Continuación(P) nunca es búsqueda nueva», contrato §7.2) prevalece sobre la redacción de la fila 2 y de §17.3 | regla en la fachada (`foundation_pending.respond`); test por clase de P × tipo de pendiente; mutantes «sin ¬Continuación» y «compone sobre el frame vencido» |
| **M2A4-D2** | P1-2: «continuar» se alcanza en lenguaje añadiendo un `KEEP` | Toda selección **en lenguaje** sobre una pendiente `CONTEXT_EXPIRED_CONFIRM` es la fila 7 (`REASK ANSWER_UNRESOLVED`): «continuar» sólo se confirma por evento (contrato §8.3 filas 1 y «Selección» 2, §10.1; U27). Las filas 5 y 8 no cambian (también son `REASK`) | barrido de **todas** las pendientes H1 que crea `delta_outcome` × respuestas en lenguaje con `KEEP` de relleno: 0 `EXECUTE`; mutante «F24 antes de normalizar sólo» |
| **M2A4-D3** | P1-3 (parte M2A-4): qué cancelaciones eliminan el ancla vencida | Lectura literal de U27: **toda** causa de §17.2 (a, b, c, d, e) que cancela una pendiente `CONTEXT_EXPIRED_CONFIRM` elimina el ancla vencida. M2A-4 lo expresa en el resultado de la cancelación (`Cancellation.eliminates_expired_anchor`); el efecto en runtime (`anchor_trace_id = None`) es de M2A-6 | tabla exhaustiva causa × tipo de pendiente; mutante «sólo a, c y d» (la lectura de §10.1) |
| **M2A4-D4** | P2-4: un `KEEP` en una respuesta sin candidatos acaba en la fila 5 | **Coste declarado, no se cambia.** U27 fijó que la normalización sólo se aplica con candidatos. Garantía limitada: el resultado es un `REASK ANSWER_EDITS_OTHER_SLOT`, que falla seguro (0 llamadas, pendiente intacta). Un test lo fija para que relajarlo sea una decisión | `test_declared_cost_keep_without_candidates` |
| **M2A4-D5** | P2-6: F24 en H5 depende de la etiqueta del productor (y el coste de F24 en H6) | **Coste declarado, reescrito por D20** (el texto de la primera entrega, «B se alcanza con un `KEEP` del comprador», lo refutó ZEUS con una sonda y era falso). F24 compara con el parche que preguntó **también tras quitar los `KEEP` de identidad**: la opción igual a él (y toda parte suya, D20) sólo se alcanza **por evento**, con o sin relleno. En H5 cuál es depende de la etiqueta provisional del productor; en H6, «buscar con ambos» (B) es el parche que preguntó (sólo por evento) y «cambiar de comprador» (A) sólo se alcanza repitiendo sus dos operaciones. Falla seguro (`REASK`) | `test_declared_cost_h6_b_only_by_event_a_by_repeating_both_operations`, `test_declared_cost_h5_the_option_the_producers_label_names_is_reached_only_by_event` y el barrido |
| **M2A4-D6** | H5/H7/H9/H10 sin punto de entrada | `ask_authority`: construye la pendiente de una regla cuyo disparo es de la autoridad o del host (INV-M2A-07, M2A-3b); M2A-4 no evalúa el disparo, aplica §8.4 sobre el `frame`, los candidatos y las `answer_slots`. Mismo punto de entrada para `UNRESOLVED_VALUE:<DIM>` con candidatos y `VALUE_WITHOUT_COMPARATOR` | `foundation_pending_create.ask_authority`; casos por regla + ley |
| **M2A4-D7** | `classify_turn`, fila 2 con `CONTEXT_EXPIRED_CONFIRM` (`TurnFacts` no lleva el tipo de pendiente) | Lo decide la fachada, que sí conoce la pendiente. `foundation_turn_scope.py` no se toca | D1 |
| **M2A4-D8** | P3: `laundering_problems` no mira las respuestas de una operación | El barrido de respuestas compara cada respuesta compuesta sobre el `frame` con `frame_ops + respuesta` compuesto desde la base: una dependencia de §19 no se pierde por pasar por el `frame` | propiedad en el barrido |
| **M2A4-D9** | Gates de §8.4: barrer las respuestas a TODAS las pendientes; una respuesta se compone sobre `frame`, nunca sobre la base | Barrido diferencial contra el modelo simbólico del contrato (`respond`/`answer` de `tests/test_m2a_contract_r3_multiop.py`) proyectado a valores tipados, más el barrido sobre las pendientes que crea `delta_outcome`. La respuesta no recibe la base: `compose_answer(frame, …)` es la costura, y el mutante `answer_on_base_not_frame` la sustituye por la base | barrido + mutante |

El cierre de las filas D1–D9 está en los tests citados; la tabla de arriba no cambió desde el gate de entrada. Lo que se
añadió después es lo que el contrato calla (§2), lo que se declara como coste (§3) y lo que M2A-6 / HERA deben saber (§7).

## 2. Lo implementado (cinco módulos de una responsabilidad, registrados en `tests/m2a_steps/m2a_4.py`)

`MODULES` y `CLOSURE` exactos; `DEPENDS_ON = ("M2A-3",)` (M2A-1 llega por M2A-3). Ningún módulo fuera de M2a los importa.

| Módulo | Responsabilidad |
|---|---|
| `foundation_pending_clock` | plazo de 30 minutos desde la **creación** (§8.2, U26) y vencimiento sobre un instante **inyectado**; nunca `datetime.now()` |
| `foundation_pending_cancel` | §17: las cinco causas, `control = CANCEL_PENDING`, base tras cancelar (`fragment_base`), eliminación del ancla vencida (M2A4-D3) |
| `foundation_pending_answer` | §8.3 filas 1 y 4–8 con su «Selección»: la respuesta se compone sobre el `frame` (`compose_answer(frame, …)` no recibe la base) |
| `foundation_pending_create` | el único sitio donde se construye una `PendingClarification`, detrás de la ley de §8.4: `pending_from_question`, `ask_authority` (H5/H7/H9/H10, M2A4-D6), `ask_fresh`, `pending_without_anchor` |
| `foundation_pending` | la decisión de un turno que encuentra una pendiente (`respond`): eventos, `PendingTurn`, filas 2 y 3 sobre `classify_turn`, tope de `asks`, vencimiento |

## 3. Decisiones donde el contrato calla (todas del lado que falla cerrado)

| Id | Decisión | Dónde |
|---|---|---|
| **M2A4-D10** | `asks` cuenta cada `REASK` (filas 5, 6, 7 y 8): la pendiente se mantiene **entera** (marco, pregunta, candidatos, ids y plazo) con `asks + 1`. Con `asks = 2` y un nuevo fallo se **cancela** (causa d): `resolution = REASK` con el código del fallo, `control = CANCEL_PENDING`, nada queda. A.3 no tiene otra etiqueta cerrada para «falló y se canceló»; el registro no guarda código y es `control` quien lo dice. **Consecuencia declarada (§7.6):** una lista vacía con el tope alcanzado acaba cancelando la pendiente, por la causa d y no por ser vacía | `_answered` |
| **M2A4-D11** | Vencida = desde el plazo (`now >= expires_at_utc`, la misma frontera que el oráculo del ancla); el plazo es el **menor** entre el persistido y `created + 30 min`. Un turno de texto sobre una pendiente vencida escribe la causa e y sigue **como si no hubiera pendiente** (`EXPIRED`); un evento recibe `INFORM STALE_OPTION` y la misma cancelación. Una opción de otra pendiente, o de una que nunca ofreció ese candidato, es `STALE_OPTION` y deja la pendiente vigente intacta (sin contar `asks`) | `respond`, `_event` |
| **M2A4-D12** | Una cancelación sin nada que aplicar (sin operaciones, sin búsqueda nueva, o un «reset») cancela y **no ejecuta nada**: `INFORM`, sin código (A.3 no tiene uno), ancla como estaba (salvo D3). Con un resto de P que dice un valor que ninguna autoridad resolvió: `INFORM UNRESOLVED_VALUE:<DIM>` con la cancelación y sin pendiente (no hay span sobre el que preguntar) | `_pure_cancel`, `_cancel_turn` |
| **M2A4-D13** | **F24 necesita las operaciones de la pregunta y los tipos no las guardan** (§7.1). `respond` recibe `asked_ops` sin valor por defecto, y `Creation.asked_ops` las entrega al crear. Endurecido por D22 | `respond`, `Creation` |
| **M2A4-D14** | Una pendiente **sin `frame`**: las de §7.1 fila 1 (`NO_ANCHOR`, `ANCHOR_UNREADABLE`) no tienen candidatos ni `answer_slots`: un fragmento es fila 8 y la búsqueda completa es fila 3. Una pendiente de M1 (primer turno) sin `frame` ni forma de componerlo es el rojo declarado `DEP-M1-PENDING-FRAME` (M2A-1b) al crearla y al responderla; nunca se inventa el `frame` | `pending_without_anchor`, `ask_fresh`, `_compose_on_frame` |
| **M2A4-D15** | `effective.base_trace_id` de una respuesta = `asked_trace_id` (el registro que guarda el marco, §3.1), nunca el `base_trace_id` de la pendiente (el ancla). Se compone el parche **completo del candidato** tal como se resolvió al preguntar (su origen es el span del turno que preguntó); no se re-origina a `OPTION_EVENT` | `answer_by_event`, `_compose_on_frame` |
| **M2A4-D16** | «Reset» sólo existe ante `CONTEXT_EXPIRED_CONFIRM` (§10.1): contra otra pendiente `cancel_pending` falla (`ContractError`, §7.5). El evento de opción compara `pending_id` y `candidate_id`, nunca texto ni `display_key` | `cancel_pending`, `_event` |
| **M2A4-D17** | Forma de lo que construye `create`: `pending_id = pending-<asked_trace_id>` (una pendiente por turno que pregunta), candidatos `c1…cn` por posición, `display_key = <ops por ranura>#<digest de su identidad>`, `ambiguous_span` = envolvente de las citas de las operaciones abiertas (o el origen de la pregunta; `None` si no nace de un span: `CONTEXT_EXPIRED_CONFIRM`, `NO_ANCHOR`, un evento, citas de dos textos) | `create` |
| **M2A4-D18** | «Autoridad no RESOLVED» (fila 7) no es un `PatchOp` (no hay valor tipado que llevar): `PendingTurn.unresolved` son las ranuras que P dijo sin resolver. Tocan su ranura (filas 5 y 8 las cuentan), la escriben (un segundo escritor es conflicto), no seleccionan nada (fila 7) y un P con una de ellas no es «idéntico» ni «parte» del parche que preguntó (F24) | `answer_in_language` |
| **M2A4-D19** | `within_answer_slots` (fila 3) lo da el **llamador** (una búsqueda nueva lleva sus condiciones fuera de `ops`); sólo se comprueba que las operaciones tipadas no lo contradigan. Los hechos de P siguen siendo trivaluados (`classify_turn`): ninguno se rellena por defecto | `_text` |

### 3.1 Decisiones de HERA tras el ZEUS fresco sobre `1c621c6a` (NO_GO; M2A4-D20…D25)

| Id | Hallazgo | Decisión | Dónde |
|---|---|---|---|
| **M2A4-D20** | **P1-1 (fondo).** F24 se esquivaba con `KEEP` de relleno en H5/H6: la comparación iba antes de normalizar y D2 sólo cubría `CONTEXT_EXPIRED_CONFIRM` (§8.4: «un candidato idéntico al parche que preguntó sólo se alcanza por evento»). Agravante: si el turno que preguntó traía otra operación resuelta (SET TIME + SET GEO), repetir sólo «en Antioquia» elegía B | El texto sellado se aplica a **toda** pendiente, no por instancia. (1) F24 se juzga además sobre la respuesta **sin sus `KEEP` de identidad**: idéntica al parche que preguntó ⇒ fila 7 (acotado por D26). (2) Con candidatos, una respuesta normalizada que es una **parte** (subconjunto como multiconjunto de identidades, no vacío) del parche que preguntó ⇒ fila 7: no aporta nada que distinga candidatos. Una respuesta que además dice algo sin resolver no es «parte» (fila 5). Lo que se **compone** pierde los `KEEP` sólo con candidatos (U27, D4). El contrato y el sello no se tocan; el modelo simbólico y `spec_row` se corrigen para que el diferencial siga siendo independiente. **Acotado por D26:** (1) vale sólo para pendientes con candidatos | `adds_nothing`, `answer_in_language`; `tests/test_m2a_contract_r3_multiop.py::answer`; `spec_row` |
| **M2A4-D21** | **P2-1.** Una fila 6 (relativo) con la directiva `CANCEL_PENDING` perdía la cancelación: `_INFORM_ROWS` se evaluaba antes (§8.4: «la pendiente intacta, salvo que §8.3 la cancele antes (filas 2 y 3)») | La directiva cancela (causa a) y el resto, la condición relativa (sin origen tipado ni `answer_slots` aquí), se **dice**, no se pregunta: `INFORM RELATIVE_TO_PREVIOUS_VALUE` sin pendiente. (La primera versión dejaba intactas las filas 4, 4b y 5 con directiva; **D27 lo corrige**: 4 y 5 cancelan, 4b no) | `_text` |
| **M2A4-D22** | **P2-2.** `respond` aceptaba `asked_ops=()` en silencio | F24 sin fuente en runtime es **gate de activación de M2A-6** (§7.1). `respond` ya no acepta `()` como «ninguno»: un turno de texto que encuentra una pendiente **con candidatos** y no trae `asked_ops` debe declarar `no_asked_patch=True` o se rechaza (`ASKED_OPS_UNDECLARED`); declarar y dar a la vez es incoherente. `CONTEXT_EXPIRED_CONFIRM` no lo necesita (su candidato ES el parche que preguntó, y D2 no mira `asked_ops`); un evento tampoco. El arnés Gold da `asked_ops` donde el Gold lo permite (la pendiente de §6.7: `CLEAR(ORDER)`) y declara su ausencia en las demás | `respond`; `test_respond_never_takes_no_asked_patch_in_silence` |
| **M2A4-D23** | **P2-3.** El arnés Gold derivaba los hechos de entrada de la etiqueta esperada (NEW_SEARCH ⇒ `new_search_facts()`, INFORM ⇒ el motivo del código esperado, DELTA ⇒ continuación) | Esos casos **no cuentan como verdes**: «no medibles sin hechos del productor». Una respuesta (`ANSWER_TO_CLARIFICATION`) toma los hechos neutros declarados de una respuesta. Tres estados cerrados (MEASURED / NO_MEDIBLE / RED_DECLARED), denominadores en §8, anclados por un test | `tests/test_foundation_pending_gold.py` |
| **M2A4-D24** | **P3.** (a) suelo de D7 sin base: CANCEL + `CLEAR(TIME)` con `own_conditions = 0` y `admissible_alone` era NEW_SEARCH; (b) `ask_fresh` con una dependencia de §19 en el `frame` daba INFORM; (c) el mutante Z7 (envolvente del span → primer span) vivía; (d) SF199 se afirmaba verde en el pytest de DEV | (a) `own_conditions = 0` nunca es búsqueda, como en `classify_turn` (trivaluado: `None` rechaza); (b) `ask_fresh` pasa por el mismo `_judge`: la dependencia es `DEPENDENCY` (U29); (c) prueba de §8.2 con varias operaciones abiertas, en cualquier orden, con citas de dos textos y con un evento, y dos mutantes; (d) SF199 es rojo declarado: no se afirma verde | `_starts_a_search_alone`, `ask_fresh`, tests |
| **M2A4-D25** | **P3 (e).** Estados y costes que M2A-4 declara y no resuelve | Ver §7.5 (dos estados que acaban en excepción), §7.6 (lista vacía con el tope) y §7.7 (`REASK` sin pendiente frente a §5.4, para M2A-7). **D3 queda validada por ZEUS**; sólo se corrige la frase que hablaba de «contradicción» (§4.4) | este documento |
| **M2A4-D26** | **P3-2 (HERA, corrige su propia D20).** La comparación de F24 sobre la respuesta normalizada se aplicó a toda pendiente; sin candidatos no bloquea ninguna ejecución y sólo cambiaba códigos sellados (fila 5 `ANSWER_EDITS_OTHER_SLOT` y fila 8 `PENDING_UNANSWERED` → fila 7) en 162 casos de la sonda de ZEUS | La comparación normalizada y la regla de la «parte» sólo aplican a pendientes **con candidatos** (donde protegen la selección). Sin candidatos vuelven los códigos sellados de las filas 5 y 8. El F24 sellado, sobre la respuesta tal como está, sigue para **todas** (incluido el parche que preguntó con un `KEEP` propio). El modelo simbólico y `spec_row` vuelven a este alcance | `answer_in_language`; `test_the_normalized_f24_is_only_for_pendings_with_candidates_the_sealed_codes_stand_elsewhere`; mutante `f24_normalized_for_every_pending` |
| **M2A4-D27** | **P3-3 (HERA, por texto sellado).** D21 sólo cancelaba la fila 6 con directiva; §17.2 (a) dice que la directiva `CANCEL_PENDING` cancela y §7.1 exime explícitamente sólo la fila 4b («no se consumen ni se cancelan») | Filas 4 (cambio de operación) y 5 (referente extensional) **con directiva cancelan**: `control = CANCEL_PENDING`, pendiente eliminada, el resto del turno como estaba (`INFORM`, el mismo código, 0 llamadas); la fila 6 también (D21); la **4b con directiva deja la pendiente intacta**. Sin directiva, las cuatro la dejan intacta. INV-M2A-23 se re-verificó (sonda de ZEUS `probe_inv23_d21.py`: 0 violaciones) y el test del INV-23 incluye ahora las cuatro filas con directiva | `_CANCELLED_BY_DIRECTIVE`, `_text`; `test_a_turn_of_rows_4_5_or_6_that_carries_the_cancel_directive_cancels_the_pending`; mutantes `directive_cancels_no_out_row`, `relative_turn_cancel_ignored`, `rows_4_and_5_cancel_ignored`, `row_4b_cancelled_by_the_directive` |
| **M2A4-D28** | **P2-B y P3-1.** (1) `ask_fresh` tenía `asked_ops=()` por defecto y reabría la omisión que D22 cerró; (2) el mutante R4 (quitar `own_conditions` de los hechos trivaluados de la regla sin base) no lo mataba ningún test | (1) `asked_ops` es **obligatorio** en `ask_fresh`: quien crea una búsqueda nueva que pregunta entrega las operaciones tipadas del turno, o una tupla vacía que escribió. (2) Con `own_conditions` indeterminado y Marco(P) falso el alcance no depende de él (`classify_turn` no rechaza) y es la regla quien debe rechazar: `UNDETERMINED_FACT:own_conditions`; el test lo fija (con 0 es `NO_ANCHOR`, con 1 búsqueda nueva) | `ask_fresh`; `test_ask_fresh_has_no_default_for_the_asking_patch`, `test_the_facts_a_cancel_over_no_base_needs_are_three_valued_and_never_defaulted`; mutantes `ask_fresh_asked_ops_defaults_to_nothing`, `own_conditions_none_accepted` |
| **M2A4-D29** | **P3-5.** SF061 tomaba su entrada decisiva (`no_selection`) de `gap_codes` de la etiqueta | Pasa a «no medible sin hechos del productor» (la etiqueta `no_selection` del wire no es una op del Gold sino una nota del etiquetador). DEV queda **2/2** medidos | `status_of`; `test_every_case_has_one_status_and_the_denominators_are_these` |
| **M2A4-D30** | **P3-4 y P2-A (sólo documento).** Desviaciones declaradas | Ver §7.1 (`no_asked_patch`) y §7.2 (el testigo R3c y §8.4). No se toca el contrato | este documento |

## 4. Costos declarados (garantía limitada, ninguno cambia el contrato)

1. **P2-4 (M2A4-D4).** Sin candidatos, un `KEEP` de otra ranura da `REASK ANSWER_EDITS_OTHER_SLOT`. Falla seguro. (F24 normalizado
   no se juzga sin candidatos: D26; el F24 sellado sí, para todas.)
2. **F24 y el relleno (M2A4-D5, reescrito por D20).** La opción igual al parche que preguntó, **y toda parte suya**, sólo se
   alcanza por evento, **con o sin `KEEP` de relleno** (la primera entrega afirmaba lo contrario y la sonda de ZEUS lo refutó). En
   H6, B («buscar con ambos») sólo por evento y A («cambiar de comprador») repitiendo sus dos operaciones; en H5, la opción que
   nombra la etiqueta provisional del productor sólo por evento y la otra en lenguaje. Falla seguro (`REASK ANSWER_UNRESOLVED`).
3. **Coste de D20: más `REASK`.** Sin botones (un canal que no los tenga) ciertas opciones no se alcanzan nunca en lenguaje; el
   tope de `asks` cancela la pendiente tras dos fallos. La ley de §8.4 (3) pide una respuesta que ejecute y «un candidato» por
   evento lo es; M2A-6 debe ofrecer el evento para toda pendiente con candidatos.
4. **El tope de `asks` cuenta también la fila 8** (F29): dos respuestas que no tocan la pregunta cancelan la pendiente.
5. **M2A4-D3, validada por ZEUS.** Con una `CONTEXT_EXPIRED_CONFIRM` cancelada por (b) el ancla vencida se elimina aunque la
   búsqueda nueva pregunte. No contradice la tabla de §10.2 («búsqueda nueva que queda en pregunta → el anterior, hasta que la
   pendiente se resuelva o se cancele»): esa fila habla de un ancla **vigente**; la de un ancla vencida la rigen §10.1 y §17.4
   (U27). M2A-6 lo escribe.
6. **F24 apagable por omisión de datos**: cerrado por D22 (la omisión se rechaza); la fuente en runtime sigue siendo del gate de M2A-6.

## 5. Mutantes (`tests/test_foundation_pending.py`, `tests/test_foundation_pending_sweep.py`)

77 tests directos son además la suite que mata a los mutantes: un mutante es un cambio de una línea del código fuente de un
módulo, cargado con nombres privados y juzgado con **los mismos tests**. **54 mutantes, 0 vivos**; 17 de ellos los matan además
los barridos de §8.4. Tras la microrreparación de ZEUS R1 se retiran tres mutantes cuyo ancla desaparece con D26
(`f24_not_judged_on_the_normalized_answer_of_every_pending`, `f24_normalized_accepts_an_empty_answer`,
`f24_normalized_counts_what_is_unresolved`: juzgaban la comparación normalizada de toda pendiente) y `f24_normalized_for_every_pending`
pasa a ser la regresión de ese estrechamiento; se redefinen `relative_turn_cancel_ignored`, `f24_only_before_normalizing` y
`subset_of_the_asking_patch_accepted`, y entran los de D27 y D28. Un mutante equivalente se descartó en vez de «matarlo»: contar el
multiconjunto de operaciones o sólo el conjunto da el mismo desenlace observable (dos operaciones iguales no pueden elegir un
candidato distinto). Los de ZEUS R1 (`R1`…`R7` de su `mutate.py`) se corrieron con su arnés: ver §6.

| Mutante | Regla | Tests que lo matan | Barrido |
|---|---|---|---|
| `answer_on_base_not_frame` | la respuesta se compone sobre el `frame` | 4 | sí |
| `answer_with_the_anchors_trace` | la respuesta nombra al turno que preguntó | 2 | sí |
| `answered_rule_not_exempt` | H6 no se vuelve a levantar al elegir su respuesta | 4 | sí |
| `any_cancellation_removes_the_anchor` | D3 | 7 | no |
| `ask_fresh_asked_ops_defaults_to_nothing` | **D28:** `ask_fresh` sin valor por defecto | 1 | no |
| `asked_ops_omission_is_silent` | D22: `asked_ops=()` no es «ninguno» | 1 | no |
| `asks_cap_plus_one` | tope de `asks` | 1 | no |
| `asks_never_counted` | `asks + 1` por `REASK` | 8 | no |
| `cancelled_pending_replaced_by_nothing` | una pendiente nueva sustituye a la cancelada | 1 | no |
| `conflict_not_decided_on_p` | causas del paso 2 sobre P | 1 | sí |
| `deadline_trusts_the_record` | el plazo es el menor | 1 | no |
| `delta_after_cancel_on_a_stale_base` | A7: la base es el marco | 2 | no |
| `dependency_swallowed` | una dependencia de §19 no se pierde | 1 | no |
| `directive_cancels_no_out_row` | **D27:** la directiva cancela en las filas 4, 5 y 6 | 1 | no |
| `empty_list_cancels` | una lista vacía nunca cancela (§17.1) | 5 | no |
| `exhausted_pending_stays` | INV-M2A-23 (d) | 1 | no |
| `expired_confirm_selected_in_language` | D2 (sin `asked_ops`, o con unos erróneos) | 2 | sí |
| `expired_pending_still_answers` | causa e | 2 | no |
| `expiry_off_by_one` | vence desde el plazo | 3 | no |
| `f24_normalized_for_every_pending` | **D26:** la comparación normalizada, sólo con candidatos | 1 | sí |
| `f24_off` | F24 sellado, sobre P tal como está (también con un `KEEP` en el parche que preguntó) | 2 | sí |
| `f24_only_before_normalizing` | **D20:** F24 sólo antes de normalizar | 4 | sí |
| `f24_part_accepts_an_empty_answer` | D20: lo vacío es fila 8 (F29) | 4 | no |
| `f24_part_counts_what_is_unresolved` | D20 / D18: decir algo más no es «el mismo parche» | 2 | sí |
| `f25_executes` | F25 | 9 | sí |
| `f29_executes` | F29 | 11 | sí |
| `frame_dependency_laundered` | U29 en la creación | 2 | no |
| `fresh_frame_dependency_as_inform` | D24 (b): U29 también en `ask_fresh` | 1 | no |
| `fresh_frame_not_judged` | ley sobre un marco nuevo | 2 | no |
| `impossible_candidate_accepted` | ley de §8.4 | 5 | no |
| `new_search_without_the_cancellation` | INV-M2A-23 (b) | 5 | no |
| `no_base_search_ignores_the_floor` | D24 (a): suelo de D7 sin base | 2 | no |
| `normalization_drops_every_keep` | sólo los `KEEP` de identidad | 1 | sí |
| `normalization_without_candidates` | lo que se compone pierde los `KEEP` sólo con candidatos (D4) | 3 | sí |
| `only_a_c_d_remove_the_anchor` | D3 | 3 | no |
| `own_conditions_none_accepted` | **D28:** `own_conditions` trivaluado (R4 de ZEUS) | 1 | no |
| `p_composed_instead_of_the_candidate` | el parche completo del candidato | 1 | sí |
| `relative_turn_cancel_ignored` | D21: la fila 6 con directiva cancela | 1 | no |
| `reset_for_any_pending` | D16 | 2 | no |
| `reset_fragment_composed_on_the_expired_frame` | D1 | 4 | no |
| `reset_fragment_ignores_continuation` | D1 | 1 | no |
| `row_4b_cancelled_by_the_directive` | **D27:** la 4b con directiva deja la pendiente intacta | 1 | no |
| `row_5_before_row_6_swapped` | orden de las filas 5 y 6 | 1 | no |
| `rows_4_and_5_cancel_ignored` | **D27:** las filas 4 y 5 con directiva cancelan | 1 | no |
| `selection_by_the_slot_only` | identidad con el valor | 4 | sí |
| `span_ends_at_the_first_citation` | Z7 / D24 (c): el final de la envolvente | 1 | no |
| `span_is_the_first_citation` | Z7 / D24 (c): el span es la envolvente | 1 | no |
| `stale_event_executes` | `STALE_OPTION` | 1 | no |
| `still_asking_executes` | «sigue ambiguo» es fila 7 | 1 | sí |
| `subset_of_the_asking_patch_accepted` | **D20:** una parte del parche que preguntó responde | 1 | sí |
| `ttl_thirty_one_minutes` | 30 minutos | 7 | no |
| `two_candidates_selected` | uno exactamente | 1 | no |
| `unresolved_rest_of_a_cancel_dropped` | D12 | 1 | no |
| `unresolved_value_ignored` | D18 | 3 | no |

## 6. Gates (salida literal)

Entorno: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` (3.14.7), `app` resuelve desde el worktree. **Código congelado en
`5730aabb`** (microrreparación de ZEUS R1 sobre `37289d85`); después de él sólo cambia este documento. El candidato a auditar es el SHA
de este documento + `e269af2f` (rama `mission/m2a-wave2-prep`; no se hace merge aquí).

1. `ruff check app/core/foundation_pending*.py tests/test_foundation_pending*.py tests/m2a_steps/m2a_4.py
   tests/test_m2a_contract_r3_multiop.py`: `All checks passed!`
2. Focales (**no la suite global**), `--hypothesis-seed=0`, sin `M2A_GOLD_REAL_DIR`:
   `pytest tests/test_foundation_pending.py tests/test_foundation_pending_sweep.py tests/test_foundation_pending_gold.py
   tests/test_m2a_boundaries.py tests/test_foundation_delta_*.py tests/test_m2a3_*.py tests/test_m2a_*.py
   tests/test_foundation_accepted_*.py tests/test_module_size_limits.py` →
   **`1 failed, 2269 passed, 10 skipped in 178.02s`**. El único fallo es
   `tests/test_m2a_boundaries.py::test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` (rojo conocido **externo**, §7.4):
   con el archivo de `e269af2f` en una copia desechable (borrada), `tests/test_m2a_boundaries.py`: `308 passed`.
3. `olympus-semgrep-delta --base d7f53e9b --project-dir <worktree>`: `TOTAL=42 BASELINE=42 NEW_DELTA=0 FIXED=0`.
4. `git diff --stat d7f53e9b..HEAD`: sólo `app/core/foundation_pending*.py` (5), `tests/test_foundation_pending*.py` (3),
   `tests/m2a_steps/m2a_4.py`, `tests/test_m2a_contract_r3_multiop.py` (único escritor en esta ola, para D20/D26) y este documento.

Sondas de ZEUS R1 (`scratchpad/zeus_m2a4_r1/`), re-ejecutadas sobre `5730aabb`:

- `probe_f24_exhaustive.py`: `A violations 0 []` y `B false REASK 0 []` (9 pendientes, todos los multiconjuntos de hasta 4 operaciones
  del conjunto y todas las permutaciones con relleno).
- `probe_p22.py`: `H6 repeat asking patch {'asked_ops': 1} -> REASK ANSWER_UNRESOLVED`; `asked_ops=() -> ContractError
  ASKED_OPS_UNDECLARED`; `asked_ops=(), no_asked_patch=True -> PENDING_ANSWER c2` (la mentira de §7.1). La parte de `ask_fresh`
  ahora da `TypeError: ask_fresh() missing 1 required keyword-only argument: 'asked_ops'` (D28).
- `probe_nocand.py` (su `R1` es ahora el código real; se corrió contra el comportamiento anterior, D20 en toda pendiente): 166
  respuestas difieren y **todas** son un código de `REASK` por otro, nunca una ejecución: el código real da los sellados (fila 5
  `ANSWER_EDITS_OTHER_SLOT` ×158 y fila 8 `PENDING_UNANSWERED` ×4 donde antes salía fila 7 `ANSWER_UNRESOLVED`).
- `probe_d21.py`: filas 4 (`OPERATION_CHANGE:COUNT`) y 5 (`REFERENT_EXTENSIONAL`) con directiva ⇒ `control=CANCEL_PENDING cause=DIRECTIVE`
  (`elim_anchor=True` sólo en H1); fila 6 ⇒ igual; **4b (`UNSUPPORTED_PROPOSAL:MULTI_REQUEST`) con directiva ⇒ `control=None`**, pendiente intacta.
- `probe_inv23_d21.py` (INV-M2A-23, 20.000 turnos al azar con las cuatro filas y la directiva): `violations 0 []`,
  `exceptions {'UNDETERMINED_FACT': 84}` (hechos trivaluados sin determinar: lo correcto, nunca un valor por defecto).
- `mutate.py R4_floor_none_accepted`: `R4_floor_none_accepted: KILLED ['1 failed, 62 passed, 71 deselected in 0.82s']
  ['tests/test_foundation_pending.py::test_the_facts_a_cancel_over_no_base_needs_are_three_valued_and_never_defaulted']`.

Barridos de §8.4 (D8, D9), dentro de la suite: diferencial contra el modelo simbólico del contrato (corregido para D20 y D26) levantado a
valores tipados, **255 pendientes, 71.812 respuestas, 0 discrepancias** (70.085 `REASK`, 1.726 `EXECUTE`, 1 `DEPENDENCY`); las
pendientes que crea `delta_outcome` sobre bases ricas y los cinco casos semilla (H6, R3-1, H1, H11, conflicto), con `spec_row`
escrito desde el contrato (también corregido) y la propiedad de lavado de §19; las pendientes `CONTEXT_EXPIRED_CONFIRM` × respuesta en
lenguaje × `KEEP` de relleno × con el parche que preguntó, sin él y con uno erróneo: 0 `EXECUTE`.

## 7. Hallazgos para M2A-6 y HERA (riesgos pendientes)

1. **GATE DE ACTIVACIÓN DE M2A-6 (HERA, D22): F24 no tiene de dónde leer el parche que preguntó.** El contrato compara con «el parche
   del registro `asked_trace_id`», pero `AcceptedTurnRecord` fuerza `patch is None` para `CLARIFY` (`PATCH_RESOLUTION_MISMATCH`) y
   `PendingClarification` no lo guarda. M2A-4 lo recibe por parámetro (`asked_ops`, sin valor por defecto, y declarando la ausencia
   con `no_asked_patch`) y `Creation.asked_ops` lo entrega, pero **nadie lo persiste**. M2A-6 no se activa mientras F24 dependa de
   que el llamador lo recuerde. Reparación mínima propuesta (toca `foundation_accepted.py`, protegido, y el esquema `m2a-pending/1`;
   decisión de HERA): un campo `asked_identity_sha256` con el digest de `ops_identity` del parche que preguntó (sin datos
   personales; D20 necesita además la parte: el multiconjunto, no sólo el digest, así que el campo debe ser la lista de
   identidades). Mientras no exista, M2A-4 es correcto bajo la condición «el llamador da `asked_ops`»; no bajo «F24 está activa».
   **Y (M2A4-D30, ZEUS P2-A):** `no_asked_patch=True` **apaga F24 si el llamador miente**: M2A-4 no puede verificarlo. M2A-6 sólo puede
   declarar `no_asked_patch=True` cuando la pendiente persistida no tiene parche que preguntó, y eso exige persistir las identidades en
   `m2a-pending/1` (el mismo campo): sin él no hay con qué distinguir «no había parche» de «no lo recuerdo». Es parte del gate de
   activación de M2A-6.
2. **Desviación declarada del texto sellado (M2A4-D30, ZEUS P3-4): el testigo R3c y §8.4.** Para `(KEEP BUYER, SET GEO)` ante H6 el
   modelo simbólico (`test_r3c_zeus_witnesses`) y el código siguen §8.4 (~632: «un candidato idéntico al parche que preguntó sólo se
   alcanza por evento»): `REASK ANSWER_UNRESOLVED`. Los pasos 2→3→5 de «Selección» (§8.3), leídos al pie de la letra (F24 antes de
   normalizar; después se quitan los `KEEP`; se compara con los candidatos), lo ejecutarían como B. La lectura de §8.4 es la decisión de
   HERA (D20); el texto del contrato no se toca y queda **pendiente de registrar en su próxima versión** (§20.4: sólo por inconsistencia
   propia o por versionado).
3. **No hay código A.3 para «cancelación sin nada que aplicar»** ni para «falló y se canceló» (M2A4-D10, D12). Se representan con
   `INFORM` / `REASK` y `control = CANCEL_PENDING`, y el registro no guarda código; si la presentación necesita uno, es un delta de
   contrato, no de M2A-4.
4. **Rojo conocido externo.** `test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` cae en cuanto un carril de la ola 2
   llena `MODULES` (su mundo sintético registra sólo los pasos de la ola 1). Lo corrige `e269af2f` (rama
   `mission/m2a-wave2-prep`, que deriva los pasos de la ola 2 del registro real; antes `1d498be4`); no se edita
   `tests/test_m2a_boundaries.py` ni se hace merge. Cualquier otro fallo de ese archivo es de este carril: no hay ninguno.
5. **Dos estados que acaban en excepción (deuda de M2A-6; no son alcanzables por un llamador correcto):** una pendiente vigente con un
   ancla `LEGACY` (`SCOPE_NOT_DEFINED_UNDER_PENDING`: `classify_turn` da `OUT_ANCHOR_NOT_SEARCH` o `OUT_NO_ANCHOR` y la pendiente es de
   Foundation) y un `ResetEvent` contra una pendiente que no es `CONTEXT_EXPIRED_CONFIRM` (`RESET_OUTSIDE_EXPIRED_CONFIRM`: no hay
   botón de «reset» para otra, M2A4-D16). M2A-6 no debe producirlos; si pueden darse, hace falta una regla de contrato.
6. **Lista vacía con el tope alcanzado (M2A4-D10 frente a §17.1).** §17.1 dice que una lista vacía de operaciones nunca significa
   cancelar. Con `asks = 2` una respuesta vacía (fila 8) llega al tope y la pendiente se cancela (causa d). No es que «vacía» cancele:
   es el tope (§8.3) el que cancela y la lista vacía el fallo. Se declara porque lo observable es el mismo; lo fija
   `test_the_cap_of_asks_cancels_the_pending_on_the_failure_after_the_second_ask`. Si HERA lo quiere distinto (p. ej. que el tope
   cuente sólo las filas 5–7), es una decisión de contrato.
7. **Nota para M2A-7 (y M2A-6): «`REASK` sin pendiente» frente a §5.4.** §5.4 dice que `CLARIFY`/`REASK` «deja (o mantiene) una
   `PendingClarification`»; la cancelación por tope (D10) deja un registro `REASK` **sin** pendiente y con `control =
   CANCEL_PENDING`. M2A-7 (eco, presentación) y el lector del registro no deben exigir una pendiente a todo `REASK`: el mensaje de
   «se canceló la pregunta» sale de `control`.
8. **Origen de las condiciones que coloca un candidato** = el span del turno que preguntó (M2A4-D15). Si M2A-6 quiere
   `OPTION_EVENT` para una respuesta por evento, es una decisión suya y no cambia identidad ni valor.
9. **Dependientes declarados en rojo** (§19, nunca dispensados): `DEP-M1-PENDING-FRAME` (M2A-1b: el `frame` de una pendiente de
   M1 y de una búsqueda nueva que pregunta, `ask_fresh`); SF063 (`DEP-DERIVED-VIEW`, ola F); SF192, SF199, SF200, SF201
   (`DEP-E-ORDER-CANDIDATES`; SF199 además `DEP-M2A5-WIRE-CARDINALITY`). Sobre la pendiente que el Gold describe, M2A-4 coincide con
   la etiqueta en SF063, SF199, SF200 y SF201 corridos **sólo como información** (§8): lo que está en rojo es la dependencia.
10. **Para M2A-6:** el hecho `within_answer_slots` y los hechos de P los pone el llamador (M2A4-D19); `respond` no lee
   `anchor_expired` (una pendiente vigente con ancla vencida sólo es la de H1); `Cancellation.eliminates_expired_anchor` es lo
   que debe escribir `anchor_trace_id = None`; la precedencia pendiente/ancla vencida con una pendiente que no es H1 sigue siendo
   G-FU-12 (M2b).
11. **Higiene:** `olympus-state own claim` se colgó a los 60 s (terminado por alarma); no hay ownership registrado, el escritor
    único lo fija el worktree. Los commits llevan `Co-Authored-By: Claude Sonnet 5.5` (el modelo real) en lugar de la línea del
    encargo.

## 8. Gold (P01–P10): denominadores honestos

Adaptador de `tests/test_foundation_pending_gold.py`: tipa los casos de M2A-4 con el tipado del propio oráculo (el oráculo los
marca `DEPENDIENTE(M2A-4)` y sin entrada tipada), corre `respond` y compara con la etiqueta sellada (las dos etiquetas A y B). Los
hechos de P **no se derivan del veredicto** (D23): el Gold no los lleva. Disciplina de particiones como M2A-3: pytest mide **DEV**;
**TEST se midió una sola vez**, sobre `d457085f` (`python -m tests.test_foundation_pending_gold TEST`), y **no se re-midió** tras la
microrreparación de ZEUS R1 (`5730aabb`): ningún caso TEST cambió de estado, y el código que esos casos alcanzan no cambia su desenlace
(los casos TEST medidos declaran que no hay parche que preguntó, así que F24, lo único que tocó D26, es vacío para ellos; ni `ask_fresh`,
ni la regla sin base, ni las filas 4/5/6 con directiva intervienen). SF061 era DEV: su cambio de estado (D29) se ve en el pytest de DEV.

Tres estados cerrados por caso (los 22 que el oráculo entrega a M2A-4; ninguno se mueve sin editar el test que lo ancla):

| Partición | MEDIDOS (respuestas, sin dependencia de otro carril) | NO MEDIBLES sin hechos del productor (no cuentan) | ROJOS DECLARADOS §19 (no cuentan) | Total |
|---|---|---|---|---|
| DEV | **2/2**: SF059, SF189 | 5: SF047, SF048, SF050, SF061, SF191 | 1: SF199 | 8 |
| TEST (una vez) | **6/6**: SF051, SF053, SF056, SF058, SF060, SF188 | 5: SF049, SF052, SF055, SF057, SF190 | 3: SF063, SF200, SF201 | 14 |
| Suma | **8/8** | 10 | 4 | 22 |

Los 10 «no medibles» son NEW_SEARCH (SF047, SF048, SF049, SF050, SF052, SF055, SF057, SF190), DELTA (SF191) o SF061: saber que son
búsqueda nueva o delta exige los hechos del productor (Continuación, Marco, Admisible sola) que el Gold no trae, y SF061 toma su
entrada decisiva (`no_selection`) de `gap_codes`, una nota del etiquetador, no de una op (M2A4-D29). Corridos con los hechos que la
etiqueta implica coinciden con ella, pero eso **no es una medición**. SF199 es un INFORM cuyo motivo sale del código esperado, y es
además rojo declarado. Los rojos declarados corridos como información coinciden con la etiqueta en SF063, SF199, SF200 y SF201.

Creación (CLARIFY de M2A-3 → pendiente posible, §8.4): DEV **3/3** (SF138, SF166, SF163 = P09/F12 con sus dos alternativas) y TEST
**4/4** (SF140, SF193, SF194, SF197). P09 (F12/F30): SF123 y SF155 (H6) las declara no medibles el propio oráculo
(`HOST_FACT_NOT_IN_GOLD:CARRIED_BUYER_TERRITORY`) y H6 está cubierta por los tests directos, el barrido y el diferencial.

**Lo que el Gold NO ejerce:** F24 (el Gold no trae el parche que preguntó; sólo la pendiente de §6.7 permite saber que preguntó por
`CLEAR(ORDER)`, y se usa en `test_dev_sf199_is_declared_red_and_never_counted_green`), el tope de `asks`, el vencimiento, el
«reset» ni D1–D3. P10 no tiene caso en el Gold (regla de protocolo). Eso lo ejercen los tests directos, los barridos y el
diferencial contra el modelo del contrato, no el Gold. Los casos SF058–SF060 comparten dueño con M2A-6 (el evento): aquí se mide
la respuesta por evento, no su ruteo de runtime.

El reducer no se tocó: ni `foundation_delta*.py` ni `foundation_turn_scope.py` ni el contrato ni el sello. Se tocó
`tests/test_m2a_contract_r3_multiop.py` (modelo simbólico, único escritor en esta ola) sólo en `answer()` y en un testigo de R3c,
para D20 y D26; la suite focal de M2A-3, incluido el enlace al oráculo en DEV, sigue verde.
