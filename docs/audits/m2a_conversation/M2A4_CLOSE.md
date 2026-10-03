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
| **M2A4-D5** | P2-6: F24 en H5 depende de la etiqueta del productor (y el coste de F24 en H6) | **Coste declarado, no se cambia.** F24 compara con el parche que preguntó: la opción igual a él sólo se alcanza por evento. En H5 qué opción es depende de la etiqueta provisional del productor; en H6, «cambiar de comprador» (A) sólo se alcanza repitiendo sus dos operaciones y «buscar con ambos» (B) con un `KEEP` del comprador. Falla seguro (`REASK`) | tabla de las dos etiquetas de H5 y de A/B de H6 fijada en un test |
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
| **M2A4-D10** | `asks` cuenta cada `REASK` (filas 5, 6, 7 y 8): la pendiente se mantiene **entera** (marco, pregunta, candidatos, ids y plazo) con `asks + 1`. Con `asks = 2` y un nuevo fallo se **cancela** (causa d): `resolution = REASK` con el código del fallo, `control = CANCEL_PENDING`, nada queda. A.3 no tiene otra etiqueta cerrada para «falló y se canceló»; el registro no guarda código y es `control` quien lo dice | `_answered` |
| **M2A4-D11** | Vencida = desde el plazo (`now >= expires_at_utc`, la misma frontera que el oráculo del ancla); el plazo es el **menor** entre el persistido y `created + 30 min`. Un turno de texto sobre una pendiente vencida escribe la causa e y sigue **como si no hubiera pendiente** (`EXPIRED`); un evento recibe `INFORM STALE_OPTION` y la misma cancelación. Una opción de otra pendiente, o de una que nunca ofreció ese candidato, es `STALE_OPTION` y deja la pendiente vigente intacta (sin contar `asks`) | `respond`, `_event` |
| **M2A4-D12** | Una cancelación sin nada que aplicar (sin operaciones, sin búsqueda nueva, o un «reset») cancela y **no ejecuta nada**: `INFORM`, sin código (A.3 no tiene uno), ancla como estaba (salvo D3). Con un resto de P que dice un valor que ninguna autoridad resolvió: `INFORM UNRESOLVED_VALUE:<DIM>` con la cancelación y sin pendiente (no hay span sobre el que preguntar) | `_pure_cancel`, `_cancel_turn` |
| **M2A4-D13** | **F24 necesita las operaciones de la pregunta y los tipos no las guardan** (§7, hallazgo 1). `respond` recibe `asked_ops` sin valor por defecto, y `Creation.asked_ops` las entrega al crear. Sin ellas F24 queda apagada y el producto elegiría por el usuario | `respond`, `Creation` |
| **M2A4-D14** | Una pendiente **sin `frame`**: las de §7.1 fila 1 (`NO_ANCHOR`, `ANCHOR_UNREADABLE`) no tienen candidatos ni `answer_slots`: un fragmento es fila 8 y la búsqueda completa es fila 3. Una pendiente de M1 (primer turno) sin `frame` ni forma de componerlo es el rojo declarado `DEP-M1-PENDING-FRAME` (M2A-1b) al crearla y al responderla; nunca se inventa el `frame` | `pending_without_anchor`, `ask_fresh`, `_compose_on_frame` |
| **M2A4-D15** | `effective.base_trace_id` de una respuesta = `asked_trace_id` (el registro que guarda el marco, §3.1), nunca el `base_trace_id` de la pendiente (el ancla). Se compone el parche **completo del candidato** tal como se resolvió al preguntar (su origen es el span del turno que preguntó); no se re-origina a `OPTION_EVENT` | `answer_by_event`, `_compose_on_frame` |
| **M2A4-D16** | «Reset» sólo existe ante `CONTEXT_EXPIRED_CONFIRM` (§10.1): contra otra pendiente `cancel_pending` falla (`ContractError`). El evento de opción compara `pending_id` y `candidate_id`, nunca texto ni `display_key` | `cancel_pending`, `_event` |
| **M2A4-D17** | Forma de lo que construye `create`: `pending_id = pending-<asked_trace_id>` (una pendiente por turno que pregunta), candidatos `c1…cn` por posición, `display_key = <ops por ranura>#<digest de su identidad>`, `ambiguous_span` = envolvente de las citas de las operaciones abiertas (o el origen de la pregunta; `None` si no nace de un span: `CONTEXT_EXPIRED_CONFIRM`, `NO_ANCHOR`, un evento) | `create` |
| **M2A4-D18** | «Autoridad no RESOLVED» (fila 7) no es un `PatchOp` (no hay valor tipado que llevar): `PendingTurn.unresolved` son las ranuras que P dijo sin resolver. Tocan su ranura (filas 5 y 8 las cuentan), la escriben (un segundo escritor es conflicto), no seleccionan nada (fila 7) y un P con una de ellas no es «idéntico» al parche que preguntó (F24) | `answer_in_language` |
| **M2A4-D19** | `within_answer_slots` (fila 3) lo da el **llamador** (una búsqueda nueva lleva sus condiciones fuera de `ops`); sólo se comprueba que las operaciones tipadas no lo contradigan. Los hechos de P siguen siendo trivaluados (`classify_turn`): ninguno se rellena por defecto | `_text` |

## 4. Costos declarados (garantía limitada, ninguno cambia el contrato)

1. **P2-4 (M2A4-D4).** Sin candidatos, un `KEEP` de otra ranura da `REASK ANSWER_EDITS_OTHER_SLOT`. Falla seguro.
2. **P2-6 y H6 (M2A4-D5).** La opción igual al parche que preguntó sólo se alcanza por evento. En H5 cuál es depende de la
   etiqueta provisional del productor; en H6, A («cambiar de comprador») exige repetir sus dos operaciones y B («buscar con
   ambos») se alcanza con un `KEEP` del comprador. Falla seguro (`REASK ANSWER_UNRESOLVED`).
3. **El tope de `asks` cuenta también la fila 8** (F29): dos respuestas que no tocan la pregunta cancelan la pendiente.
4. **M2A4-D3 leído a la letra** contradice una fila de la tabla de §10.2 («búsqueda nueva que queda en pregunta → el anterior
   hasta que la pendiente se resuelva o se cancele»): con una `CONTEXT_EXPIRED_CONFIRM` cancelada por (b), el ancla vencida
   se elimina aunque la búsqueda nueva pregunte. Es lo que ordenó el orquestador (falla cerrado); M2A-6 lo escribe.
5. **F24 apagable por omisión de datos**: ver §7.1.

## 5. Mutantes (`tests/test_foundation_pending.py`, `tests/test_foundation_pending_sweep.py`)

69 tests directos son además la suite que mata a los mutantes: un mutante es un cambio de una línea del código fuente de un
módulo, cargado con nombres privados y juzgado con **los mismos tests**. **39 mutantes, 0 vivos**; 14 de ellos los matan también
los barridos de §8.4.

| Mutante | Regla | Tests que lo matan | Barrido |
|---|---|---|---|
| `f24_off` | F24 | 3 | sí |
| `f24_after_normalizing` | F24 antes de normalizar (U27) | 1 | sí |
| `f25_executes` | F25 | 6 | sí |
| `f29_executes` | F29 | 9 | sí |
| `empty_list_cancels` | una lista vacía nunca cancela (§17.1) | 5 | no |
| `answer_on_base_not_frame` | la respuesta se compone sobre el `frame` | 3 | sí |
| `answer_with_the_anchors_trace` | la respuesta nombra al turno que preguntó | 2 | sí |
| `expiry_off_by_one` | vence desde el plazo | 3 | no |
| `ttl_thirty_one_minutes` | 30 minutos | 7 | no |
| `deadline_trusts_the_record` | el plazo es el menor | 1 | no |
| `asks_cap_plus_one` | tope de `asks` | 1 | no |
| `asks_never_counted` | `asks + 1` por `REASK` | 8 | no |
| `impossible_candidate_accepted` | ley de §8.4 | 4 | no |
| `frame_dependency_laundered` | U29 en la creación | 1 | no |
| `fresh_frame_not_judged` | ley sobre un marco nuevo | 1 | no |
| `conflict_not_decided_on_p` | causas del paso 2 sobre P | 1 | sí |
| `normalization_without_candidates` | normalización sólo con candidatos (M2A4-D4) | 2 | sí |
| `normalization_drops_every_keep` | sólo los `KEEP` de identidad | 1 | sí |
| `two_candidates_selected` | uno exactamente | 1 | no |
| `p_composed_instead_of_the_candidate` | el parche completo del candidato | 1 | sí |
| `selection_by_the_slot_only` | identidad con el valor | 3 | sí |
| `answered_rule_not_exempt` | H6 no se vuelve a levantar al elegir su respuesta | 3 | sí |
| `still_asking_executes` | «sigue ambiguo» es fila 7 | 1 | sí |
| `dependency_swallowed` | una dependencia de §19 no se pierde | 1 | no |
| `row_5_before_row_6_swapped` | orden de las filas 5 y 6 | 1 | no |
| `unresolved_value_ignored` | M2A4-D18 | 1 | no |
| `expired_confirm_selected_in_language` | M2A4-D2 | 1 | sí |
| `reset_fragment_ignores_continuation` | M2A4-D1 | 1 | no |
| `reset_fragment_composed_on_the_expired_frame` | M2A4-D1 | 3 | no |
| `only_a_c_d_remove_the_anchor` | M2A4-D3 | 3 | no |
| `any_cancellation_removes_the_anchor` | M2A4-D3 | 6 | no |
| `reset_for_any_pending` | M2A4-D16 | 2 | no |
| `new_search_without_the_cancellation` | INV-M2A-23 (b) | 5 | no |
| `exhausted_pending_stays` | INV-M2A-23 (d) | 1 | no |
| `cancelled_pending_replaced_by_nothing` | una pendiente nueva sustituye a la cancelada | 1 | no |
| `stale_event_executes` | `STALE_OPTION` | 1 | no |
| `expired_pending_still_answers` | causa e | 2 | no |
| `delta_after_cancel_on_a_stale_base` | A7: la base es el marco | 2 | no |
| `unresolved_rest_of_a_cancel_dropped` | M2A4-D12 | 1 | no |

## 6. Gates (salida literal)

Entorno: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` (3.14.7), `app` resuelve desde el worktree. Código congelado en
`9b422278`; después de él sólo cambia este documento.

1. `ruff check app/core/foundation_pending*.py tests/test_foundation_pending*.py tests/m2a_steps/m2a_4.py`: `All checks passed!`
2. Focales (**no la suite global**), `--hypothesis-seed=0`, sin `M2A_GOLD_REAL_DIR`:
   `pytest tests/test_foundation_pending.py tests/test_foundation_pending_sweep.py tests/test_foundation_pending_gold.py
   tests/test_m2a_boundaries.py tests/test_foundation_delta_*.py tests/test_m2a3_*.py tests/test_m2a_*.py
   tests/test_foundation_accepted_*.py tests/test_module_size_limits.py` →
   **`1 failed, 2247 passed, 10 skipped in 267.53s`**. El único fallo es
   `tests/test_m2a_boundaries.py::test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` (rojo conocido **externo**, §7.3):
   con el archivo de `1d498be4` en una copia desechable, `tests/test_m2a_boundaries.py`: `308 passed`.
3. `olympus-semgrep-delta --base d7f53e9b --project-dir <worktree>`: `TOTAL=42 BASELINE=42 NEW_DELTA=0 FIXED=0`.
4. `git diff --stat d7f53e9b..HEAD`: sólo `app/core/foundation_pending*.py` (5), `tests/test_foundation_pending*.py` (3),
   `tests/m2a_steps/m2a_4.py` y este documento.

Barridos de §8.4 (D8, D9), dentro de la suite: diferencial contra el modelo simbólico del contrato levantado a valores tipados,
**255 pendientes, 71.500 respuestas, 0 discrepancias** (69.687 `REASK`, 1.812 `EXECUTE`, 1 `DEPENDENCY`); las pendientes que crea
`delta_outcome` sobre bases ricas (≈40 % de los casos generados son pendientes; H1: 355 de 599), con `spec_row` escrito desde el
contrato y la propiedad de lavado de §19; las pendientes `CONTEXT_EXPIRED_CONFIRM` × respuesta en lenguaje × `KEEP` de relleno:
0 `EXECUTE`.

## 7. Hallazgos para M2A-6 y HERA (riesgos pendientes)

1. **F24 no tiene de dónde leer el parche que preguntó.** El contrato compara con «el parche del registro `asked_trace_id`», pero
   `AcceptedTurnRecord` fuerza `patch is None` para `CLARIFY` (`PATCH_RESOLUTION_MISMATCH`) y `PendingClarification` no lo guarda.
   M2A-4 recibe `asked_ops` por parámetro (sin valor por defecto) y `Creation.asked_ops` las entrega, pero **nadie las persiste**.
   Reparación mínima propuesta (toca `foundation_accepted.py`, protegido, y el esquema `m2a-pending/1`; decisión de HERA): un campo
   `asked_identity_sha256` con el digest de `ops_identity` del parche que preguntó (sin datos personales; basta para la igualdad
   que F24 pregunta). Hasta entonces F24 queda a cargo de que M2A-6 las conserve por otro medio.
2. **No hay código A.3 para «cancelación sin nada que aplicar»** ni para «falló y se canceló» (M2A4-D10, D12). Se representan con
   `INFORM` / `REASK` y `control = CANCEL_PENDING`, y el registro no guarda código; si la presentación necesita uno, es un delta
   de contrato, no de M2A-4.
3. **Rojo conocido externo.** `test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` cae en cuanto un carril de la ola 2
   llena `MODULES` (su mundo sintético registra sólo los pasos de la ola 1). Lo corrige `1d498be4` (rama
   `mission/m2a-wave2-prep`); no se edita `tests/test_m2a_boundaries.py` ni se hace merge. El candidato a auditar es este SHA +
   `1d498be4`. Cualquier otro fallo de ese archivo es de este carril: no hay ninguno.
4. **Origen de las condiciones que coloca un candidato** = el span del turno que preguntó (M2A4-D15). Si M2A-6 quiere
   `OPTION_EVENT` para una respuesta por evento, es una decisión suya y no cambia identidad ni valor.
5. **Dependientes declarados en rojo** (§19, nunca dispensados): `DEP-M1-PENDING-FRAME` (M2A-1b: el `frame` de una pendiente de
   M1 y de una búsqueda nueva que pregunta, `ask_fresh`); SF063 (`DEP-DERIVED-VIEW`, ola F); SF192, SF199, SF200, SF201
   (`DEP-E-ORDER-CANDIDATES`; SF199 además `DEP-M2A5-WIRE-CARDINALITY`). Sobre la pendiente que el Gold describe, M2A-4 se
   comporta como pide la etiqueta en SF063, SF199, SF200 y SF201 (§8): lo que está en rojo es la dependencia.
6. **Para M2A-6:** el hecho `within_answer_slots` y los hechos de P los pone el llamador (M2A4-D19); `respond` no lee
   `anchor_expired` (una pendiente vigente con ancla vencida sólo es la de H1); `Cancellation.eliminates_expired_anchor` es lo
   que debe escribir `anchor_trace_id = None`; la precedencia pendiente/ancla vencida con una pendiente que no es H1 sigue siendo
   G-FU-12 (M2b).
7. **Higiene:** `olympus-state own claim` se colgó a los 60 s (terminado por alarma); no hay ownership registrado, el escritor
   único lo fija el worktree. Los commits llevan `Co-Authored-By: Claude Sonnet 5.5` (el modelo real) en lugar de la línea del
   encargo.

## 8. Gold (P01–P10)

Adaptador de `tests/test_foundation_pending_gold.py`: tipa los casos de M2A-4 con el tipado del propio oráculo (el oráculo los
marca `DEPENDIENTE(M2A-4)` y sin entrada tipada), corre `respond` y compara con la etiqueta sellada (las dos etiquetas A y B).
Disciplina de particiones como M2A-3: pytest mide **DEV**; **TEST se midió una sola vez** sobre el código congelado
(`python -m tests.test_foundation_pending_gold TEST`).

| Partición | Casos de M2A-4 sin dependencia de otro carril | Declarados en rojo (§19) | Creación (CLARIFY de M2A-3 → pendiente posible) |
|---|---|---|---|
| DEV | **7/7** (SF047, SF048, SF050, SF059, SF061, SF189, SF191) | SF199: coincide con la etiqueta (4b, pendiente intacta) | **3/3** (SF138, SF166, SF163 = P09/F12 con sus dos alternativas) |
| TEST (una vez) | **11/11** (SF049, SF051, SF052, SF053, SF055, SF056, SF057, SF058, SF060, SF188, SF190) | SF063, SF200, SF201: coinciden con la etiqueta | **4/4** (SF140, SF193, SF194, SF197) |

P09 (F12/F30): SF163 está medido por el adaptador; SF123 y SF155 (H6) las declara no medibles el propio oráculo
(`HOST_FACT_NOT_IN_GOLD:CARRIED_BUYER_TERRITORY`) y H6 está cubierta por los tests directos y por el barrido. P10 no tiene caso
en el Gold (regla de protocolo): `test_an_empty_list_of_operations_is_never_a_cancellation` y `f29_executes`. Los casos
SF058–SF060 comparten dueño con M2A-6 (el evento): aquí se mide la respuesta por evento, no su ruteo de runtime.

El reducer no se tocó: ni `foundation_delta*.py` ni `foundation_turn_scope.py` ni el modelo `tests/test_m2a_contract_r3_multiop.py`
(la suite focal de M2A-3, incluido el enlace al oráculo en DEV, sigue verde).
