# M2A-3 — Fase 2: `compose` y `classify_turn` contra el oráculo congelado

**Fecha:** 2026-10-02. **Rama:** `mission/m2a-3-reducer`. **Contrato:** R3d sellado (`57530a07`, sello `7dd39b4b…`,
fusionado en `10c0c576`). **Oráculo:** M2A-8a0 congelado, primero `f7d1b8b8` (artefacto `4aa343c7…`, fusionado en
`b2ad0a62`) y después `f95275b4` (artefacto `a753efad…`, fusionado en `434ab1bd`), más el arreglo de su test de lector
estricto `9535b41f` (fusionado en `0ef76f97`) y el de A8.1 con unidad desconocida `9e54d2f5` (mismo artefacto,
fusionado en `013e4429`). Todo de sólo lectura aquí. **La reparación del NO_GO de ZEUS sobre `15e72829` está en §6;
la del NO_GO del ZEUS conjunto sobre la integración `8066e03d`, en §8.**

**Estado: DEV y TEST al 100 % de los APLICABLES, sintético y REAL (§3).** TEST se midió una sola vez.

## 1. El BLOCKER de la fase 2, resuelto por el oráculo

En `f7d1b8b8`, SF103, SF104, SF106, SF167 y SF183 (DEV) y SF101 (TEST) llegaban con una unidad heredada
(`unit_source = PREVIOUS_ACCEPTED_VALUE`) sin `inherited_from`. §15.4 y §11 exigen ese origen, y el tipo lo sigue
exigiendo. El carril del oráculo lo corrigió en `f95275b4`: el origen es la traza del ancla y ninguna expectativa
cambió. Los xfail estrictos del enlace pasaron a XPASS y se retiraron en `edce7d69`. El tipo no se relajó para la
unidad.

En `95d9e828` se habían corregido en el tipo dos exigencias de 1a sin cláusula:

- el acoplamiento `year ⇔ year_source`. §3.2: `year` es la vigencia de una conversión SMMLV; `year_source` es la
  procedencia de un año, también el de un TIME (§11);
- el origen obligatorio de un **año** heredado. §11 `YEAR_INHERITED` sólo pide `year_source`.

Con esa corrección, 23 de los 29 casos aplicables cuyas entradas el tipo rechazaba pasaron a construirse. Desde
`f95275b4` el oráculo entrega además año y fuente juntos y el origen de todo año heredado: sus datos cumplen las dos
lecturas. En `9535b41f` su test de lector estricto dejó de fijar el código `YEAR_WITHOUT_SOURCE` y está en verde con
este tipo. **Estrechado por ZEUS (P2-5, §6):** de la relajación sólo quedan esas dos lecturas (un TIME lleva
`year_source` sin `year`; un año heredado no exige origen). Una vigencia SMMLV vuelve a exigir su fuente, y
`year_source` / `inherited_from` vuelven a vivir sólo en VALUE y TIME.

## 2. Lo implementado (tres módulos de una responsabilidad, registrados en `tests/m2a_steps/m2a_3.py`)

- **`app/core/foundation_delta.py`** — `compose(base, patch, *, base_trace_id) -> Composed | DeltaRefusal`. El
  parámetro `base_trace_id` existe porque una `AcceptedInterpretation` no lleva su propia traza.
  - **Causas de §6 paso 3b** con su precedencia cerrada y desempate por el orden de ranuras de §4.2. Cada causa lleva su
    cláusula.
  - **TARGET_NOT_ADDRESSABLE** (§4.2), también si la víctima de F16 o de A8.1 está dentro de un grupo.
  - **DELTA_SLOT_CONFLICT**, incluido `KEEP` + escritura aunque los valores coincidan (U22).
  - **KEEP_ON_EMPTY_SLOT.**
  - **OP_NOT_ACCREDITED:\<DIM\>**, una causa por OPERACIÓN (§6 paso 3b): F16 fail-closed y P-3; A8 con unidades no
    comparables o desconocidas; O1B (`REPLACE_UNIVERSE` a oportunidades ⇒ `UNIVERSE`, §19 DEP-O1B); política sin dueño
    (§6.6, inventario de la fase 1): una condición colocada que la necesita (MODALITY `family`, −Terminado sobre
    contratos), un cambio de universo que arrastra una política o una condición así (⇒ `UNIVERSE`), la lente O1B.
  - **DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER** (R3-1), decidido sobre el estado.
  - **Composición:** CLEAR → SET/SET_ORDER/REPLACE_UNIVERSE; arrastre byte-idéntico con la excepción P-1;
    sustituciones A8.1 y F16; efectos `VALUE_RANGE_REPLACED`, `POLARITY_INVERTED` y `TOPIC_REPLACED`.
  - **REPLACE_UNIVERSE** con ancla dual: se queda la petición nombrada, con `universe_origin` = el de la op y `dataset`
    = el del nuevo sujeto (tabla fijada contra `SoQLBuilder`).
  - Una política se va con su condición; la lógica OR/NOT se conserva y se poda sólo el AND de primer nivel.
  - El único camino de construcción es `canonical_interpretation`. Los ids colocados nunca reutilizan un id de la base.
- **`app/core/foundation_delta_outcome.py`** — `delta_outcome(base, patch, *, base_trace_id, anchor_expired)`.
  - **Causas** antes que las reglas del host. Con dos o más, o con el ancla vencida ⇒ `INFORM`. Con una ⇒ `CLARIFY`
    sólo si §8.4 lo permite.
  - **R3-1** sin E-ORDER lleva sólo C1 (§19).
  - **Reglas del host puras:**
    - H1: el vencimiento es un hecho inyectado; §10 es de M2A-6.
    - H3: catálogo SMMLV 2024–2026, fijado contra `app/data/smmlv.py`.
    - H6: `buyer_scope` arrastrado frente a la DIVIPOLA nueva.
    - H8: literales de STATE del dataset resultante; reducir un ancla dual también cambia el universo (§14, §8).
    - H11 y H12.
  - **Dependencias declaradas de §19:** DEP-SMMLV-RECALC y DEP-MODALITY-UNIVERSE.
  - **Ley §8.4:**
    - el marco, leído DESDE LA BASE, es válido, no tiene preguntas abiertas y no levanta ninguna dependencia
      `COMPOSICION` de §19; si la levanta, el turno es esa `DEPENDENCY` (§8);
    - los candidatos componen sin causas del paso 3b (§8.4.2; nunca se los juzga por las preguntas del host);
    - hay al menos una respuesta que ejecuta, buscada con testigos abstractos de una operación, como en el modelo del
      contrato; un candidato ejecuta con la regla a la que responde exenta (`executes(..., answered=...)`).
  - **Pendiente:** se devuelve como datos (`PendingQuestion`); la construye M2A-4.
  - **`relative_outcome`:** §7.1 fila 6.
- **`app/core/foundation_turn_scope.py`** — `classify_turn(TurnFacts)`: §7.1 filas 1–6 (incluida 4b) y §7.2 / §8 filas
  2–3 para el alcance.
  - Los hechos se leen en tres valores: el alcance sólo sale si no depende de los que faltan.
  - `ABSENT` se rechaza (`REFERENT_SIGNAL_ABSENT`). «Nuevo ≠ ∅ ∧ Marco ∧ ¬Admisible sola» no tiene fila en §7.2, así que
    §7.2 no dice búsqueda nueva y la fila 8 de §7.1 («lo demás: delta») la cierra: `IN` / `DELTA` (INV-M2A-20).
  - `search_effects` da los efectos de una búsqueda nueva (`CONTEXT_RESET`, `TOPIC_REPLACED`).
- **`app/core/foundation_accepted.py`** — la superficie de decodificación de R1 (`canonical_value`, `money_bound`,
  `unit_class`, `time_years`, `geo_divipola`, `state_datasets`, `state_class`, `modality_family_policy`) y las dos
  correcciones de §1, estrechadas en §6.

Fuera, declarado:

- **H2** (pendiente vigente): M2A-4.
- **H5, H7, H9, H10:** lecturas de una autoridad que se resuelven al tipar el valor; un valor resuelto ya las pasó.
  Nadie construye todavía su pendiente: deuda de M2A-4 (§7, punto 8).
- **§8.3, §10 y §17.**
- **`HostViewDecision`:** el sujeto `unspecified` sigue fallando cerrado en el tipo.

## 3. Oráculo: DEV y TEST

Medido con `python -m tests.test_m2a3_oracle_link <DEV|TEST>`, con y sin `M2A_GOLD_REAL_DIR`, sobre `434ab1bd` más el
enlace de `edce7d69`. La fusión posterior `0ef76f97` sólo toca un test del oráculo; el artefacto sigue en `a753efad`.

| Partición | Gold `audit` | `audit_transitions` | `audit_classification` |
|---|---|---|---|
| DEV sintético | **54/54**, 0 discrepancias | 0 violaciones (47 + 3 rechazos) | 52/52 |
| DEV + REAL | **72/72** (54 + 18) | 0 violaciones (64 + 4) | 69/69 |
| TEST sintético (una vez) | **53/53**, 0 discrepancias | 0 violaciones (46 + 6) | 51/51 |
| TEST + REAL (una vez) | **71/71** (53 + 18) | 0 violaciones (62 + 8) | 68/68 |

TEST no necesitó ninguna corrección. El oráculo no verifica las efectivas de búsqueda nueva: las construye
`from_acceptance`, de M2A-1b (`LG-FRESH-ACCEPTANCE`).

Rojos declarados (DEPENDIENTE, con su dueño):

| Partición | Dueño | Casos |
|---|---|---|
| DEV | M2A-4 | SF047, SF048, SF050, SF059, SF061, SF189, SF191, SF199 |
| DEV | M2A-6 | SF145, SF146, SF150, SF151, SF152 |
| DEV | E-MODALITY | SF007, SF160 |
| DEV | F | SF082, SF126, R0106, R0107, R0529 |
| DEV | `FOUNDATION_O1B_EXPLICIT_TIME_PARITY` | SF075 |
| TEST | M2A-4 | SF049, SF051, SF052, SF053, SF055, SF056, SF057, SF058, SF060, SF063, SF188, SF190, SF200, SF201 |
| TEST | M2A-6 | SF149 |
| TEST | E-MODALITY | SF008, SF080, R0459 |
| TEST | E-ORDER | SF192 |
| TEST | F | SF081, SF185, R0531, R0532 |
| TEST | D-MONEY | R0304 |

No medibles:

- DEV: SF031, SF033 (`TIME_MONTH_HINT`); SF025, SF163, R0281 (`OPS_AB_DISAGREE`); SF159 (`REGION_CLOSED_LIST`); R0094
  (`STATE_LITERALS_OF_THE_UNIVERSE`); R0282 (`PRIOR_AB_DISAGREE`).
- TEST: SF026, SF030, SF036, SF161, R0274 (`OPS_AB_DISAGREE`); SF123, SF155 (`CARRIED_BUYER_TERRITORY`); SF078, SF079
  (`STATE_LITERALS_OF_THE_UNIVERSE`); SF168 (`SMMLV_YEAR_NOT_SINGLE`); SF102 (`VALUE_UNIT_FROM_RESULT`); R0075, R0336
  (`PRIOR_AB_DISAGREE`).

## 4. Propiedades y mutantes

- **`tests/test_foundation_delta_properties.py`** (Hypothesis con semilla fija en los gates). Las bases generadas
  llevan políticas, `receipt_rules`, procedencia rellena, ancla dual, oportunidades y grupos OR.
  - P1: `check_transition` del oráculo con 0 violaciones. Para las causas que el oráculo no ve (§4.2, O1B, §6.6) se
    exige además que las causas visibles sean exactamente las suyas.
  - P2: invariancia a la permutación de las ops. P3: determinismo. P4: forma canónica.
  - P5: propiedad universal; nunca excepción.
  - P6: políticas. P7: TARGET. P8: efectos.
  - Comprobaciones propias de H1, H3, H6, H8, H11, H12, «dos causas ⇒ INFORM» y DEP-O1B, escritas desde el contrato.
  - Estrés local: 6 semillas × 4.000 casos sin violaciones.
- **32 mutantes del reducer, todos muertos** (27 de la fase 2 y 5 de la reparación, §6). Incluyen los que el
  manifiesto del oráculo declara indiscriminables por el Gold: `f16_any_value`, `set_clears_whole_dimension`,
  `clear_dimension_only_positive`, `redirect_keeps_old_kind` y el sobredisparo de H6. El resto es uno por regla.
- **`tests/test_foundation_delta_rules.py`** (reparación, §6): un test directo por regla, para que cada mutante de una
  línea muera con la configuración del gate y no sólo cuando el generador llega a él.
- **`tests/test_foundation_delta_tables.py`**, tablas exhaustivas del dominio cerrado:
  - INV-M2A-18 (A8.1): 900 filas (408 antes de la reparación; ahora con la unidad desconocida).
  - INV-M2A-19 (F16, P-3, F17, F3): 15 filas.
  - §7.2 sobre las 768 combinaciones (con INV-M2A-05 e INV-M2A-20) y §7.1 filas 1–6.
  - Pines de las constantes copiadas.
- **`tests/test_foundation_accepted_m2a3_canonical.py`:** 4 mutantes nuevos de procedencia. Siguen vivos los de las
  fases 1b y 1c.

## 5. Gates (resultado literal)

**Fase 2 (`4a68e2bc`):** 18 archivos con `M2A_GOLD_REAL_DIR`, **1536 passed**; `ruff` limpio; semgrep delta
`TOTAL=0 BASELINE=0 NEW_DELTA=0`; `seal OK 7dd39b4b…`; `oracle OK a753efad…`.

**Reparación de ZEUS (`00ee88e5`, código y tests; este registro va en el commit siguiente):**

- `pytest -p no:cacheprovider --hypothesis-seed=0` con `M2A_GOLD_REAL_DIR`, 17 archivos (`test_foundation_accepted_*` ×3,
  `test_foundation_delta_*` ×3, `test_m2a3_*` ×2, `test_m2a_boundaries`, `test_m2a_contract_r3_multiop`, `…_r3_sweep`,
  `…_s19_extraction`, `test_m2a_gold_seal`, `test_m2a_oracle_inputs_strict_reader`, `test_m2a_oracle_vs_contract_model`,
  `test_m2a_reducer_oracle`, `test_m2a_transition_oracle`): **1785 passed**, 0 skipped, 0 xfailed.
- DEV (`python -m tests.test_m2a3_oracle_link DEV`): sintético **54/54** (transiciones 47 + 3, 0 violaciones;
  clasificación 52/52); con REAL **72/72** (64 + 4; 69/69). TEST se mide una sola vez, sobre el SHA congelado, y se
  reporta en la entrega.
- Mutantes: los 32 del suite muertos; arnés externo (35 de ZEUS re-anclados + 12 propios, `pytest -x
  --hypothesis-seed=0` sobre propiedades, tablas, enlace, observabilidad y reglas, con control sin mutar en verde):
  **46 muertos, 1 equivalente declarado** (§6).
- `ruff --select E,W,F --ignore E501` (la selección del `Makefile`) sobre los archivos del carril: limpio. El repo
  entero da 789 errores preexistentes, ninguno en archivos del carril.
- `olympus-semgrep-delta --base fa0b6de8`: archivos del carril `TOTAL=0 BASELINE=0 NEW_DELTA=0`; repo entero
  `TOTAL=42 BASELINE=42 NEW_DELTA=0`.
- `m2a_gold_seal.py verify --real-dir`: `seal OK 7dd39b4b…`. `m2a_reducer_oracle.py check` (con y sin `--real-dir`):
  `oracle OK a753efad…`.

Corrí esos tests, no la suite global.

## 6. Reparación del NO_GO de ZEUS sobre `15e72829` (≡ `4a68e2bc` en `app/`)

| Hallazgo | Reparación | Tests directos (`tests/test_foundation_delta_rules.py` salvo indicación) |
|---|---|---|
| **P1 (fondo)** H6 nunca daba `CLARIFY`: la ley de §8.4 juzgaba los candidatos por las preguntas del host y «buscar con ambos» volvía a disparar H6 | §8.4.2 juzga cada candidato sólo por las causas del paso 3b; §8.4.3 hace ejecutar un candidato con la regla que responde exenta (`executes(frame, ops, trace, answered=rule)`). Misma clase revisada: H1 «continuar» y R3-1 C1 no responden a una regla del host y no cambian | `test_h6_asks_with_its_two_candidates_and_buscar_con_ambos_executes` (forma de `e6.py`), `…_for_a_region_without_a_territorial_code` (SF123), `…_reads_only_a_carried_territorial_buyer…`, `…_with_a_candidate_that_does_not_compose_informs`, `test_a_frame_never_holds_a_second_question`, `test_answer_slots_must_cover_the_candidates` |
| **P2-3** condición que necesita política compone sin ella | `needs_host_policy`: SET de MODALITY `family` (operador o `family_policy`) y STATE −Terminado sobre una petición de contratos (la inversión F16 de +Terminado incluida) ⇒ `OP_NOT_ACCREDITED:<DIM>`; un `REPLACE_UNIVERSE` que arrastra una condición así ⇒ `…:UNIVERSE` | `test_a_condition_that_needs_a_host_policy_is_not_accredited`, `test_a_negated_terminado_over_contracts_needs_the_complement_policy`, `test_a_change_of_universe_carrying_a_condition_that_needs_a_policy_is_refused` |
| **P2-4** una op contaba como varias causas | Una causa por operación para `TARGET_NOT_ADDRESSABLE` y `OP_NOT_ACCREDITED` (como el modelo del contrato); la ranura es la de la operación (`UNIVERSE` para `REPLACE_UNIVERSE`) y la cláusula la de su primera razón | `test_a_change_of_universe_with_carried_policies_is_one_cause`, `test_the_sf075_shape_is_dep_o1b_on_the_universe_with_or_without_time`, `test_the_o1b_lens_counts_only_a_change_of_the_state`, `test_an_operation_on_a_group_is_one_target_cause…` |
| **P2-5** relajación del tipo demasiado ancha | `year` exige `year_source`; `year_source` e `inherited_from` sólo en VALUE/TIME (`check_dimension`); en VALUE, `year_source` exige `year`. Quedan sólo: TIME con `year_source` sin `year`, y año heredado sin origen | `provenance_failures` (`tests/test_foundation_accepted_m2a3_canonical.py`) con 5 mutantes de procedencia nuevos |
| **P2-6** A8.1 trataba una unidad `None` como comparable | `_comparable` exige las dos unidades conocidas y de la misma clase. El oráculo lo aplicó por su lado en `9e54d2f5` | tabla A8.1 con la unidad desconocida (`tests/test_foundation_delta_tables.py`), `test_a81_never_compares_an_unknown_unit`; el generador produce `unit=None` |
| **P2-7** 12 mutantes de ZEUS vivos | tests directos por regla (arriba y H1, R3-1, H3, H8, H11/H12, §7.1–§7.2, `search_effects`); el generador produce además regiones sin código, MODALITY `family`; propiedades nuevas (forma de cada pendiente, §6.6, una causa por op, dimensión de `UNRESOLVED_VALUE` dentro de `answer_slots`) | los 35 de ZEUS, re-anclados, y 12 propios mueren con la configuración del gate (semilla 0); 5 entran además en `REDUCER_MUTANTS` |
| **P3** `UNDECIDED:ADMISSIBLE_ALONE` sin `TurnScope` | §7.1 fila 8: `IN` / `DELTA`; `is_new_content` eliminado (sin uso) | la tabla de §7.2 (las 768 combinaciones, 0 rechazos), `test_a_new_framed_but_not_admissible_turn_is_a_delta` |

Lecturas que esta reparación fija y conviene que ZEUS mire:

- **Unidad desconocida ARRASTRADA ⇒ `INFORM UNRESOLVED_VALUE:VALUE`**, no `CLARIFY`: §8.4 (3) no encuentra respuesta sobre
  `answer_slots = {la cota nueva}` que componga, porque ninguna unidad elegida se compara con una desconocida. Con la
  unidad desconocida en la cota NUEVA y la arrastrada conocida, sí: `CLARIFY`.
- **Ancla dual reducida** a una de sus peticiones: sus políticas se arrastran (P-1: «nada más cambia») y no hay causa
  §6.6. Para H8, en cambio, sí es un cambio de universo: §8 (F4) corrige la lectura de esta sección.
- **Mutante equivalente declarado:** quitar `answered` de la llamada de H6 a la ley no cambia ningún desenlace, porque
  «cambiar de comprador» ejecuta siempre que compone. El parámetro se mantiene porque es la semántica del contrato y la
  que usará M2A-4.

## 7. Deuda registrada para M2A-4 (U28; ZEUS puntos 8 y 9; no se tocan aquí)

- **(8)** H5, H7, H9 y H10 no tienen punto de entrada que construya su pendiente bajo §8.4. `compose` no las evalúa
  (§15.4, §16.1, INV-M2A-07: son lecturas de la autoridad), pero alguien tiene que levantar su `PendingClarification`
  (H5 con dos candidatos).
- **(9)** `classify_turn`, fila 2 de §8.3 con una pendiente `CONTEXT_EXPIRED_CONFIRM`: `TurnFacts` no lleva el tipo de
  pendiente, así que «reset + fragmento» sin marco se clasifica `IN` / `DELTA` sobre el `frame` (el ancla vencida).
  R3d manda búsqueda nueva si el resto es admisible solo y, si no, `CLARIFY NO_ANCHOR`; nunca componer sobre el
  `frame` vencido.
- **Composición de la opción elegida:** al aplicar un candidato (§8.3 filas 1 y 4), M2A-4 compone con la regla que la
  pendiente responde exenta (`executes(..., answered=...)`); re-juzgar «buscar con ambos» como un turno nuevo volvería a
  disparar H6.

## 8. Reparación del NO_GO del ZEUS conjunto sobre la integración `8066e03d` (contiene `e96ca3c5`)

Sondas de ZEUS: `scratchpad/zeus_int/probes/` (`p1_h6_dep.py`, `p2_dep_launder.py`, `p4_sweep.py`, `p6_misc.py`,
`p9_more.py`), corridas con `PYTHONPATH` en este worktree.

| Hallazgo | Reparación | Tests directos (`tests/test_foundation_delta_rules.py`) |
|---|---|---|
| **F1 (fondo)** una dependencia §19 `COMPOSICION` se perdía al pasar por el `frame` de una pendiente: el `frame` sólo se juzgaba por preguntas y, una vez compuesto, ya no veía el cambio de universo (H8) ni el TIME que quitó (DEP-SMMLV-RECALC) | `frame_obstacle` lee el `frame` compuesto DESDE LA BASE, en el orden del paso 4: una pregunta (salvo H11) o una dependencia de §19. La ley lo nombra («frame raises a §19 dependency») y `_ask` responde esa `DEPENDENCY` antes de preguntar, como H1 («decide esa dependencia»). Cubre toda pendiente que construyo: una causa del paso 3b (incluida R3-1), H3–H11 (H6) y H4 (`relative_outcome`). H1 ya lo hacía (su `frame` es la base). Los candidatos de H6, C1 y «continuar» no cambian el universo ni tocan TIME/VALUE, así que no pueden blanquear una dependencia sobre un `frame` lícito; la propiedad nueva `laundering_problems` lo comprueba (candidato sobre el `frame` frente a `frame` + candidato desde la base) | `test_h6_with_a_change_of_universe_that_carries_a_modality_is_its_dependency` (p1), `test_r3_1_with_a_dependency_in_its_frame_is_that_dependency` (p2 A, B, B2), `test_one_cause_whose_frame_drops_the_time_of_a_smmlv_bound_is_the_recalc` (p9), `test_h6_with_a_dependency_first_is_that_dependency` (p2 C), `test_a_relative_turn_whose_rest_raises_a_dependency_is_that_dependency` (p9, H4), `test_the_law_names_a_frame_that_raises_a_dependency` |
| **F3 (A)** mutante vivo `subjects_after = {r.subject for r in base.requests}` | test directo con la redirección en el mismo parche, y su contraste (salir de contratos) | `test_the_policy_a_placed_condition_needs_is_judged_on_the_universe_the_patch_leaves` (p6) |
| **F4 (P3)** ancla dual reducida con modalidad arrastrada: `EXECUTE` aquí, `DEPENDENCY` en el modelo congelado | **No sostengo la lectura «la petición que queda ya ejecutó su modalidad»:** §14 pone la reducción del ancla dual entre los cambios de universo (anuncia `UNIVERSE_CHANGED`) y dice «tras cambiar el universo, F27 (H8) comprueba STATE y MODALITY sobre el dataset resultante»; P-1 («nada más cambia») habla de los bytes de lo arrastrado, no de H8. **Me alineo al modelo:** `Composed.universe_changed` es verdadero también al reducir un ancla dual. Efecto: MODALITY arrastrada ⇒ `DEPENDENCY DEP-MODALITY-UNIVERSE`; un STATE sin literales en el dataset que queda (+Terminado al quedarse con procesos) ⇒ `CLARIFY STATE_NOT_IN_UNIVERSE` en vez de ejecutarse. Las políticas siguen arrastrándose (P-1) | `test_a_dual_anchor_reduced_is_a_change_of_universe_for_h8` (p6, p8) |
| **F7 (P3)** | **Q-R8-3 queda CERRADA**: «Nuevo ≠ ∅ ∧ Marco ∧ ¬Admisible sola» no tiene fila en §7.2, así que §7.2 no dice búsqueda nueva y la fila 8 de §7.1 («lo demás: delta o respuesta») da `IN` / `DELTA` (INV-M2A-20: un `TurnScope` por turno). Ya no es pregunta abierta de la ola F | `test_a_new_framed_but_not_admissible_turn_is_a_delta`; tabla de §7.2 con 0 rechazos |

Además (sólo tests): el mutante de ZEUS «la lente O1B cuenta un `CLEAR` de nada» sobrevivía; ahora lo mata un caso en
`test_the_o1b_lens_counts_only_a_change_of_the_state`.

**Divergencia con el modelo congelado (para HERA; no se toca, es del carril del contrato):** en
`tests/test_m2a_contract_r3_multiop.py`, `law_violations` sólo mira `open_operations` del `frame`, no sus dependencias.
Para `CLEAR(ORDER)` + `REPLACE_UNIVERSE` con la modalidad arrastrada (sonda `p3_model.py`) el modelo da `CLARIFY` R3-1
y C1 `EXECUTE`; el reducer da ahora `DEPENDENCY DEP-MODALITY-UNIVERSE`, como pide F1. El oráculo no juzga
`DEPENDENCY` y el Gold no cambia (DEV/TEST iguales).

**Fuera, por orden del coordinador:** F2 (H6 con `buyer_scope` `None`) queda como gate de activación de M2A-6/D-BUYER.

Gates: se corren sobre el SHA congelado tras este registro (focales y propiedades con `--hypothesis-seed=0` con y sin
REAL, Gold-oracle DEV/TEST, mutantes, ruff, semgrep delta `--base fa0b6de8`) y se reportan en la entrega; este
documento no se toca después del freeze para no invalidarlo.

