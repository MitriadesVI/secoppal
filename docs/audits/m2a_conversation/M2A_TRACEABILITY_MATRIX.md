# M2A — Matriz de trazabilidad

**Fecha:** 2026-10-01; **enmendada 2026-10-02 (R3**, contrato Anexo D: filas T14–T17, N06, P11, M10, M11, X03–X05 y la reconciliación «CLEAR(ORDER) con CARDINALITY»; casos asignados en B4, `379ca3db`; **R3b-1**: filas T18, T19, P12, P13, R07 y X06, y revisadas P11 y X05; **R3b-2** (U25): controles SF196–SF201 asignados, Gold 209). **Contrato:** `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` (§ = sección del contrato).
**Gold:** `docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json` (209 casos, release r3b: r2 con U1/U2/D1 + U3 + U25; ids `R…` = REAL, texto
fuera de git; `SF…` = sintético).

Cada fila responde: *este comportamiento existe porque <decisión> + <casos>, lo representa <estructura> y lo
comprobará <gate>*. Los ids salen mecánicamente del Gold (unión de las etiquetas A y B) y del informe
`docs/audits/m2a_conversation/gold/REPRESENTABILITY_REPORT.json`; ninguno se eligió a mano. «Gate futuro» nombra el paso del plan
(`M2A_IMPLEMENTATION_PLAN.md`: M2A-4 = pendiente, M2A-5 = productor/wire, M2A-7 = eco) y el invariante (§18) que lo
verificará.

## 1. Tipos, composición y atomicidad

| # | Regla (§) | Decisión de origen | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| T01 | La efectiva aceptada es el único estado; `Turn.parsed_params = {}` (§3.1, §3.7) | ADR-103 D7 Persistencia; H1 | los 199 (gate G2: ninguno requiere params) | `AcceptedTurnRecord`, `AcceptedInterpretation` | M2A-1/M2A-2: INV-M2A-04, -09; pin de imports |
| T02 | Delta = conjunto atómico de `SET/CLEAR/KEEP/SET_ORDER/REPLACE_UNIVERSE` (§5.2, §6.1) | F1; D7 «deltas»; G-FU-03 | 158 deltas ejecutados con arrastre comprobado (`delta_executed_carry_checked`); 56 etiquetas con 2–3 ops | `SemanticPatch.ops` | M2A-3: INV-M2A-01 (mutante «aplica la primera y falla la segunda») |
| T03 | Arrastre por identidad: lo no tocado pasa byte-idéntico (§6.3) | F1 | 158 (como T02) | `compose` | M2A-3: INV-M2A-02, -16 |
| T04 | `SET` sobre ranura ocupada reemplaza; unión de valores no expresable (§5.2) | F1; clasificación R0075 | R0075 (+141 con `SET`) | `PatchOp(SET)` | M2A-3: INV-M2A-16 |
| T05 | `CLEAR` citado quita la ranura (§5.2) | F1, D8 «remoción sólo citada» | R0202, SF023, SF024, SF100, SF109, SF110, SF111, SF112, SF113, SF115, SF156, SF158 | `PatchOp(CLEAR)` | M2A-3: INV-M2A-16 |
| T06 | `KEEP` da dueño a la cita deíctica (§5.2) | F1, O5 | R0099, R0336, SF024, SF041 | `PatchOp(KEEP)` | M2A-3: `KEEP_ON_EMPTY_SLOT` ⇒ `CLARIFY KEEP_ON_EMPTY_SLOT` (R3-2) |
| T07 | `SET_ORDER` re-consulta y conserva CARDINALITY (F10) (§5.2) | F4, F10; ADR-103 Enm. 1 (superlativo singular) | R0079, R0083, R0087, R0089, R0099, R0515, SF015–SF022, SF117–SF120, SF122, SF140, SF160, SF161, SF171, SF172; `TOPN_CARRIED`: SF018, SF019, SF172 | `PatchOp(SET_ORDER)` + `cardinality@2` | M2A-3; M2A-6 (cursor) |
| T08 | Top-N > 100 ejecuta 100 y anuncia (§2, §11) | A3 = P8 (ADR-103 D9) | SF021 | `cardinality@2(n, n_requested)` | M2A-7: INV-M2A-21 |
| T09 | Orden por fecha sin dirección = descendente, anunciado (§11) | F21; T11 | SF140, SF171, SF172 | `Provenance.order_direction_source = DEFAULTED` | M2A-7: INV-M2A-21 |
| T10 | `REPLACE_UNIVERSE`; ancla dual se queda con el nombrado; `UNIVERSE_CHANGED` (§5.2, §14) | F1 (`SET_UNIVERSE`), D7 «REPLACE de universo» | R0094, SF073–SF080, SF131, SF186, SF187 | `PatchOp(REPLACE_UNIVERSE)` | M2A-3: INV-M2A-11 |
| T11 | Estado/modalidad inexistente en el universo resultante ⇒ CLARIFY (§6 H8) | F27 | SF079 | regla H8 sobre `state@1.literals` por dataset | M2A-3 |
| T12 | Re-consulta, no filtro de lo mostrado («los de mayor valor», «los que…») (§9) | F4; clasificación SF017 | 47 con `refers_to_result_set` (p. ej. R0079, R0083, R0087, R0089, R0099, R0223, R0459, SF003, SF017…) | `semantic_ref` (no `result_set_ref`) | M2A-3/M2A-6: INV-M2A-22 |
| T14 | CARDINALITY sin ORDER tras componer ⇒ rechazo `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` y `CLARIFY`, base intacta; decidido sobre el estado (§6.7) | usuario 2026-10-02 (STOP-2 de M2A-3; D-1, D-2); `M2A_DESIGN_v2.md:240` | SF192 (+ SF195, control: `CLEAR(ORDER)`+`SET_ORDER` conserva la CARDINALITY) | `DeltaRefusal` + pendiente con un candidato | M2A-3: INV-M2A-01, -16, -25; mutantes «conserva la CARDINALITY sin ORDER» y «la borra sin cita» |
| T15 | Todo `DeltaRefusal` tiene código de A.3 (§6, tabla) | barrido R3 (R3-2) | SF193 (`KEEP_ON_EMPTY_SLOT`) | tabla `DeltaRefusal` → A.3 | M2A-3/M2A-4: INV-M2A-25 |
| T16 | `<DIM>` recorre las claves de dimensión de las ranuras, incluida UNIVERSE (A.3) | barrido R3 (R3-3); STOP-1 de M2A-3 | SF075 (rojo declarado, dependencia `FOUNDATION_O1B_EXPLICIT_TIME_PARITY`) | `UNRESOLVED_VALUE:UNIVERSE`, `OP_NOT_ACCREDITED:UNIVERSE` | M2A-3 fase 1c (tipo): INV-M2A-25 |
| T17 | `REPLACE_UNIVERSE` con ancla dual estrecha `request_ids` de lo arrastrado (§6.3) | barrido R3 (P-1); R5 de M2A-3 | SF076, SF077 | excepción de §6.3 | M2A-3: INV-M2A-02 |
| T18 | Causas de rechazo con precedencia cerrada, antes de H1: `TARGET_NOT_ADDRESSABLE` > `DELTA_SLOT_CONFLICT` > `KEEP_ON_EMPTY_SLOT` > `OP_NOT_ACCREDITED:<DIM>` > `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`; dos o más ⇒ `INFORM` con la de mayor precedencia, sin pendiente (§6 paso 3b) | usuario 2026-10-02 (R3b, U20; ZEUS P1-1) | SF196 (`CLEAR(ORDER)` con top-N + `KEEP` de ranura vacía ⇒ `INFORM KEEP_ON_EMPTY_SLOT`) | `DeltaRefusal` + A.3 «INFORM con un código de CLARIFY» | barrido S10; `tests/test_m2a_contract_r3_multiop.py`; M2A-3 fase 2 sobre el `compose` real |
| T19 | `KEEP` + `SET`/`SET_ORDER`/`REPLACE_UNIVERSE` de la misma ranura ⇒ `DELTA_SLOT_CONFLICT`, aunque los valores coincidan; un `CLEAR` de la dimensión entera cubre sus ranuras (§6 paso 2) | usuario 2026-10-02 (R3b, U22; ZEUS P2) | SF197 (`KEEP` + `SET` del comprador ⇒ `CLARIFY DELTA_SLOT_CONFLICT`); con el mismo valor, sólo barrido y verificador (SF202 retirado, U25) | regla de §6 paso 2 | barrido S13; verificador G9 y su mutante; barrido multi-op |
| T13 | Composición determinista; `interpretation_hash` de la efectiva (§4.3, §12) | D7 «Eco y hashes» | los 199 | `_digest(AcceptedInterpretation)` | M2A-1: propiedad de determinismo; INV-M2A-06 (`plan_hash` del primer turno igual) |

## 2. Búsqueda nueva frente a delta

| # | Regla (§) | Decisión | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| N01 | Sólo condiciones/modificadores con SEARCH activa ⇒ DELTA aunque pase A9 (§7.2 b) | **A9.1**; G-FU-02b | R0459, SF187 (adjudicados A9.1) + 160 deltas que no traen TOPIC/ACTOR nuevos | dimensiones de P vs ancla | M2A-3/M2A-5: INV-M2A-05; mutante «condición suelta admisible ⇒ NEW_SEARCH» |
| N02 | TOPIC/ACTOR nuevo + marco + admisible sola ⇒ NEW_SEARCH + `CONTEXT_RESET` (§7.2 c) | **A9**; A4 | SF124, SF126, SF128, SF130, SF134, SF184, SF185 | `TurnResolution.NEW_SEARCH`, `base_trace_id = None` | M2A-3/M2A-5: INV-M2A-05 |
| N03 | Continuación estructural gana (§7.2 a; también ante una pendiente, §8.3 filas 2–3) | A9 («gana la marca»); G-FU-02 | R0094 (adjudicado A9: universo explícito + estado, pero con marca de continuación ⇒ DELTA); SF041 (KEEP), SF120, SF121 (referente de conjunto); con pendiente: SF051, SF053 (⇒ REASK F25, no búsqueda nueva) | `referent_evidence`, directivas `KEEP`/`CLEAR`, job `FOLLOWUP_DELTA` | M2A-3/M2A-5: garantía limitada declarada; prompt v5.1 |
| N04 | Tema desnudo (una condición sin marco) ⇒ DELTA, TOPIC sustituye (§7.2 d) | F22; F3 | R0286, R0484, SF002, SF133; `TOPIC_REPLACED`: SF041, SF133 | `Marco(P)` | M2A-3/M2A-5 |
| N06 | Propuesta no admitida con ancla o pendiente ⇒ `INFORM UNSUPPORTED_PROPOSAL:<motivo>`, 0 llamadas, ancla y pendiente intactas (§7.1 fila 4b) | usuario 2026-10-02 (D-6) | — (sin caso en Gold; regla genérica) | `TurnScope.OUT_UNSUPPORTED_PROPOSAL`, lista cerrada de motivos con dueño | M2A-3 (fase 1c el tipo); M2A-5/M2A-6: INV-M2A-20, -25 |
| N05 | Sin ancla ni pendiente: fragmento ⇒ CLARIFY NO_ANCHOR (§7.1) | F28 | — (casos en frontera G-FU-17: SF137, SF142) | `TurnScope.OUT_NO_ANCHOR` | M2A-3/M2A-5: regresión fail-closed |

## 3. Pendiente, respuestas y cancelación

| # | Regla (§) | Decisión | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| P01 | `PendingClarification{ambiguous_span, candidate_patches[]}` genérico (§8.1) | **G-FU-03**; corrección de diseño (span) | 19 con previo pendiente (SF047–SF061, SF063 con preestado normalizado por U2, SF188–SF191); 2 con dos ranuras candidatas (SF059, SF060) | `PendingClarification`, `CandidatePatch` | M2A-4: INV-M2A-08 |
| P02 | Respuesta por evento selecciona un candidato, sin LLM (§8.3.1) | A7; O11 | SF058, SF059, SF060 | `PENDING_ANSWER` por evento | M2A-4 |
| P03 | Respuesta abierta completa el marco (§8.3.4) | A7 | SF056, SF188 | `answer_slots` | M2A-4 |
| P04 | Respuesta + otra edición ⇒ REASK (§8.3.5) | F25 | SF051, SF053 | regla de §8.3 | M2A-4: mutante «F25 que ejecuta» |
| P05 | «Cualquiera» ⇒ REASK, el producto no elige (§8.3.6) | F24 | SF061 | etiqueta `no_selection` | M2A-4: mutante «F24 que elige» |
| P06 | Respuesta que no resuelve ⇒ REASK (§8.3.7) | diseño (ANSWER_UNRESOLVED) | SF189 | regla de §8.3 | M2A-4 |
| P07 | Consulta autosuficiente con pendiente ⇒ NEW_SEARCH + `CANCEL_PENDING` (§8.3.3, §17.2b) | A7, A9; clasificación SF063 (O12/O13) | SF047, SF048, SF049, SF050, SF052, SF055, SF057, SF063, SF190 | `control = CANCEL_PENDING` | M2A-4: INV-M2A-23 |
| P08 | Rechazo citado + fragmento ⇒ cancela y aplica sobre el marco (§8.3.2, §17.3) | F23, A7 (base = marco) | SF191 (y SF063 con búsqueda nueva) | directiva `CANCEL_PENDING` | M2A-4: INV-M2A-08, -23 |
| P09 | Ambigüedad con dos lecturas ⇒ CLARIFY con 2 `candidate_patches` (§6 H5/H6) | F12, F30 | SF163 (BUYER/lugar); SF123, SF155 (territorio) | `CandidatePatch` (pueden tocar varias ranuras) | M2A-4 |
| P10 | Sin pregunta respondida ni rechazo ⇒ REASK (§8.3.8) | F29 | — (sin caso en Gold; regla de protocolo) | regla de §8.3 | M2A-4 |
| P11 | Retirar el ORDER con top-N ⇒ pendiente con C1 «quitar orden y límite» + un `SET_ORDER` por cada criterio de orden alternativo del dataset (resueltos al preguntar, sin el vigente), `answer_slots = {ORDER, CARDINALITY}`; «por fecha» selecciona su candidato por identidad semántica (§8.3 «Selección», R3b), «dame 20» no llega a §8.3: el wire lo rechaza ⇒ §7.1 fila 4b `INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL` con la pendiente intacta (R3b, P1-2), «déjalo como estaba» = `CANCEL_PENDING`; sin E-ORDER, sólo C1 (§6.7, §19). Controles r3b: SF199 («dame 20» ⇒ 4b), SF200 (C1), SF201 (fila 5) | usuario 2026-10-02 (D-1, opción 1 del orquestador) | SF192 (sellado sólo C1 y declarado parcial: `partial_alternatives`, `DEP-E-ORDER-CANDIDATES`, R3b U23; los `SET_ORDER` son de E-ORDER, §19) | `CandidatePatch` (C1 + `SET_ORDER`) | M2A-4: INV-M2A-08, -25 |
| P12 | Ley de la pendiente posible: `frame` válido y sin operaciones abiertas, cada candidato compone; si no, `INFORM` sin pendiente; con una pendiente vigente no nace otra (§8.4, INV-M2A-26) | usuario 2026-10-02 (R3b, U20; ZEUS P1-1) | SF196, SF198 (causa sin pendiente posible ⇒ `INFORM`) | `PendingClarification` (frame, candidatos) | barrido S11; barrido multi-op; M2A-3 fase 2 (`check_transition` del oráculo) |
| P13 | Selección de candidato por identidad semántica (op, ranura, valor canónico / destino), nunca por texto ni ids; compone el parche completo; edición de otra ranura ⇒ fila 5 (§8.3 «Selección») | usuario 2026-10-02 (R3b, U22; ZEUS P2) | SF192 (alternativa C1); SF200 (selecciona C1 por identidad); SF201 (respuesta + edición de otra ranura ⇒ fila 5) | `CandidatePatch.patch` | barrido (`test_d1_option1_answers_go_through_the_wire_and_select_by_semantic_identity`); M2A-4 |

## 4. Referentes, vigencia y fallo de fuente

| # | Regla (§) | Decisión | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| R01 | Tras fallo de fuente la efectiva es ancla y refinable (§9, §13) | **A6**; D7 Referentes | SF135, SF136, SF178 | `semantic_ref` sin `result_set_ref` | M2A-6: INV-M2A-22 |
| R02 | Filas/página sin `result_set_ref` ⇒ INFORM, 0 llamadas (§9.1) | A6; G-FU-04 (código diferido) | — (casos en frontera: R0276, SF179, SF180, SF181) | `ResultSetRef` ausente | M2A-6 |
| R03 | Paginar = mismo plan, instante guardado (§9.2) | D14 (diseño); INV-M2A-10 | SF145, SF146 | `ResultSetRef.groups[].plan_hash` | M2A-6: INV-M2A-10 |
| R04 | Eventos de orden y «quitar filtro» como parche sin LLM (§5.3) | P7 / Enm. 3 (botones a M2) | SF149, SF150, SF151, SF152 | `SemanticPatch(origin_kind = EVENT)` | M2A-6 |
| R05 | Vencido ⇒ CLARIFY `CONTEXT_EXPIRED_CONFIRM` (continuar / reset) (§10) | F11 | R0223, R0286, SF138, SF140 | `PendingClarification` con un candidato + `CANCEL_PENDING` | M2A-4/M2A-6 |
| R06 | `CLEAR` de ranura vacía ⇒ INFORM, ancla intacta (§6 H12) | F18 | SF113 | regla H12 | M2A-3: INV-M2A-20 |
| R07 | Ancla vencida: `CONTEXT_EXPIRED_CONFIRM` sólo si «continuar» compone y ejecuta (sin causa y sin H3–H12); si no, `INFORM` con la causa o la regla, sin bucle (§6 H1, §10.1) | usuario 2026-10-02 (R3b, U20; ZEUS P1-1) | SF198 (ancla vencida + `KEEP` de ranura vacía ⇒ `INFORM`, sin bucle) | `PendingClarification` con un candidato | barrido S12; barrido multi-op; verificador G9 (pendiente sobre ancla vencida) |
| R08 | Vencimiento pegajoso: el turno que encuentra el ancla vencida y no la confirma ni la reemplaza lleva `CONTEXT_EXPIRED` en su registro; el ancla sigue vencida hasta «continuar», «reset»/cancelación o búsqueda nueva (§10.1, §11) | usuario 2026-10-02 (R3c, U26; ZEUS P1-1) | SF198 (eco `CONTEXT_EXPIRED`) | eco del último `AcceptedTurnRecord` (sin campos nuevos) | M2A-6/8b por replay (propiedad de varios turnos) |

## 5. VALUE

| # | Regla (§) | Decisión | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| V01 | VALUE por lado; la opuesta se conserva (§15.1) | A1; F2 | todos los `SET VALUE.*` (p. ej. SF087, SF093, SF098, SF107, SF129) | ranuras `VALUE.lower/upper` por comparador tipado | M2A-3: INV-M2A-18 |
| V02 | **A8**: intervalo vacío generado por follow-up ⇒ sustituye la restricción y anuncia (§15.2) | **A8**; G-FU-01 | SF090, SF094, SF099, SF103, SF182, SF183 (cálculo numérico) ; R0187 adjudicado A8 pero en frontera por otra causa (G-FU-18) | `money@1.amount_cop` + origen de las cotas | M2A-3: INV-M2A-18 (tabla exhaustiva) |
| V03 | **A8.1**: degenerado a un punto ⇒ igual que A8; igualdad pedida explícita vale (§15.2) | **A8.1**; G-FU-01b | R0205, R0531, SF091, SF095 | origen de las cotas (mismo turno = pedida) | M2A-3: INV-M2A-18; mutante «degenerado de follow-up ejecutado» |
| V04 | «Cambia el valor a…» = `CLEAR(VALUE)` + `SET` (§15.1) | F2 | R0202, SF100 | `CLEAR` + `SET` | M2A-3 |
| V05 | Unidad compartida en un rango (§15.3) | F6 | R0180, R0282, R0311, R0529, SF087, SF098, SF107 | `unit.sibling` | E-VALUE kit; M2A-7 eco |
| V06 | Herencia de unidad única (incl. SMMLV); puede seguir A8.1 (§15.4) | A1, A2; F5; F26 retirada | R0531, R0532, SF101, SF103, SF104, SF106, SF167, SF183 | `unit.inherited`, `Provenance.unit_source` | M2A-3b/3c: INV-M2A-17 |
| V07 | Unidad no heredable ⇒ CLARIFY (§6 H9) | F5 | SF102 | regla H9 | M2A-3 |
| V08 | SMMLV ejecutable con año único; recálculo al cambiar el año (§15.5) | A2; OQ-1; D10 | R0301, R0304, SF165, SF167 | `Provenance.source_amount/year` | E-VALUE (D10) + M2A-3c |
| V09 | SMMLV sin año único o fuera del catálogo D10 (2024–2026) ⇒ CLARIFY (§6 H3, §15.5) | A2; D10; **U1** (control de no extrapolación) | R0303, SF168 (D1: estado persistido inválido declarado), SF166 (U1: 2023 fuera del catálogo) | regla H3 + catálogo | M2A-3 |
| V12 | Cotas no comparables (SMMLV frente a pesos) ⇒ no se aplica A8.1, CLARIFY (§15.2) | A8 («no adjudicado»); G-FU-06 | — (caso en frontera: SF170) | `Provenance.unit` | M2A-3: INV-M2A-18 |
| V10 | «en adelante» = `>=` (Anexo B) | contrato VALUE vigente (`money_comparators._QUANTIFIER_SUFFIXES`) | R0205 | autoridad VALUE del primer turno | ya en `main` |
| V11 | Formas de superficie leídas por la autoridad VALUE (§19 dependencias) | contrato M1 v1.4 (O4) | SF087, SF093, SF098, SF106, SF107, SF129, SF136, SF191 | `money@1` | E-VALUE |

## 6. TIME, vista y polaridad

| # | Regla (§) | Decisión | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| M01 | Mes sin año hereda el año único (§16.1) | F7 | R0496, R0497, R0498, SF032 | `year.inherited` | E-TIME + M2A-3c: INV-M2A-17 |
| M02 | Mes sin año sin TIME previo: reloj, anunciado (§16.1) | F7 | SF034, SF035 | `year.inferred` | E-TIME |
| M03 | TIME previo plurianual/relativo ⇒ CLARIFY (§6 H10) | F7 | SF031, SF033 | regla H10 | M2A-3 |
| M04 | Relativo al referente previo único (§16.3) | **A5**; ADR-103 D7 (OQ-3 = b) | SF037, SF042, SF174 | entrada separada de TIME, `time.relative_to_previous` (Enm. 5) | E-TIME + M2A-3c; eco `YEAR_INHERITED` + `RELATIVE_WINDOW_RESOLVED` (§11) |
| M05 | Relativo al reloj (§16.4) | contrato M1 (TIME) | SF025–SF030, SF036, SF161 | `relative_period` | E-TIME |
| M06 | Vista derivada se recalcula y se anuncia (§14) | F9 | R0106, SF082 (`VIEW_REDERIVED`); `VIEW_DERIVED`: SF063, SF082, SF126, SF185 | `HostViewDecision` (ola F) | M2A-3 + F: INV-M2A-11 |
| M07 | Negar el único positivo lo invierte (§6.5) | F16 | SF011, SF012 | sustitución declarada `POLARITY_INVERTED` | M2A-3: INV-M2A-19 |
| M08 | Negar otro valor añade la exclusión (§6.5) | F17 | SF001, SF003, SF005, SF007, SF009, SF014 | `SET` negativo | M2A-3: INV-M2A-19 |
| M09 | Región fuera de la lista ⇒ CLARIFY (§6 H7) | F13 | SF159 | regla H7 | E-GEO + M2A-3 |
| M10 | Negar sobre varios positivos o un valor contenido ⇒ `OP_NOT_ACCREDITED:<DIM>` ⇒ `CLARIFY UNRESOLVED_VALUE:<DIM>` (§6.5) | barrido R3 (R3-4; D-3) | SF194 | regla F16, rama fail-closed | M2A-3: INV-M2A-19, -25 |
| M11 | F16 sólo invierte un positivo **arrastrado**; +X y −X en el mismo parche ⇒ `OP_NOT_ACCREDITED:<DIM>` ⇒ `CLARIFY UNRESOLVED_VALUE:<DIM>` (§6.5) | usuario 2026-10-02 (P-3, U18) | — (sin caso; mutante del verificador `R3_P3_SF001_same_patch_both_polarities`) | regla F16, rama fail-closed | M2A-3: INV-M2A-19, -25 |

## 7. Frontera (lo que M2a NO hace)

| # | Regla (§) | Decisión | Casos | Estructura | Gate futuro |
|---|---|---|---|---|---|
| X01 | 57 casos FRONTIER fuera de los gates de M2a; provisional fail-closed (§19) | ADJUDICATION_T3 punto 11–12, 16; ledger G-FU-04…21 | los 57 del sello (`slices.FOLLOWUP_FRONTIER`) | `TurnScope.OUT_*` + `INFORM`/`CLARIFY` | M2A-8: GOLD ∩ FRONTIER = ∅ en el replay; INV-M2A-20 |
| X02 | Nada cruza a legacy con ancla Foundation (§3.7) | D7; R-BARRERA | los 199 | — | M2A-1…M2A-9: INV-M2A-04, -15 |
| X03 | Toda respuesta alcanzable es representable (INV-M2A-25, invariante mayor) | usuario 2026-10-02 (R3-5) | todos | códigos de A.3 admitidos por el tipo | `tests/test_m2a_contract_r3_sweep.py` (barrido generado; lo que falte en el tipo es un xfail estricto) |
| X04 | Congelación tras R3 (§20.4) | usuario 2026-10-02 | — | — | revisión: toda enmienda posterior cita una inconsistencia del contrato o una extensión/versionado del lenguaje |
| X05 | Una autoridad para las dependencias: §19 ⇒ extracción generada y sellada; ningún consumidor tiene lista ni override propio. Cada fila: ids explícitos (también `[]`), conteo verificable, dueño de lista cerrada, `impacto_m2a3` = `LECTURA` / `COMPOSICION` (§19) | usuario 2026-10-02 (U19; R3b, U23) | SF075 (`DEP-O1B-OPPORTUNITIES`); R0301, SF165, SF167 (`DEP-SMMLV-READ`); R0304 (`DEP-SMMLV-RECALC`); SF192 (`DEP-E-ORDER-CANDIDATES`) | `gold/CONTRACT_S19_DEPENDENCIES.json` (`extract_contract_dependencies.py`) | `m2a_gold_seal.py verify` (gate `contract_dependencies_reproduce`); `tests/test_m2a_gold_seal.py` |
| X06 | INV-M2A-25 con varias operaciones: barrido exhaustivo por parejas × ocupación de la base + Hypothesis con N operaciones e invariancia a la permutación, sobre un modelo simbólico del contrato (§8.4, §18) | usuario 2026-10-02 (R3b, U21) | — (modelo simbólico; no lee casos) | modelo de ocupación de ranuras generado de `PatchOpKind` × ranura | `tests/test_m2a_contract_r3_multiop.py`; sobre el `compose` real, gate de M2A-3 fase 2 |

## 8. Reconciliaciones A↔B registradas

| Cuestión | A (`main`) | B (asesoría) | Resolución | Dónde |
|---|---|---|---|---|
| Rango sin interior | D7: CLARIFY con dos opciones (OQ-4 = a) | A8/A8.1: sustituir y anunciar; mixtos SMMLV/pesos sin adjudicar | B supersede sólo con unidades comparables; mixtos ⇒ CLARIFY (G-FU-06) | ADR-103 Enm. 5 punto 2; contrato §15.2 |
| Búsqueda nueva | D7: decide el productor | A9/A9.1: forma de la IR | B supersede; el suelo del host de D7 se conserva | Enm. 5 punto 2; §7.2 |
| Gold del gate M2a | «2.1 con A1–A7» | `M2A_GOLD` con A1–A9.1 | B supersede | Enm. 5 punto 2 |
| Eco del relativo al referente | D7: `RELATIVE_WINDOW_RESOLVED` | Gold deriva `YEAR_INHERITED` | ambos (no se contradicen) | §11 regla de cobertura |
| Regla de recibo del relativo | §4.3 no la lista | A5 exige aritmética de periodo | se añade `time.relative_to_previous` (aditiva, sin tipo nuevo) | Enm. 5 punto 3 |
| Nombre de la op de universo | D7: «REPLACE de universo» | F1/Gold: `SET_UNIVERSE` | `REPLACE_UNIVERSE`, misma semántica | §2, §5.2 |
| `CANCEL_PENDING` con búsqueda nueva | — | O12/O13 abierto (A: `[]`, B: `CANCEL_PENDING`) | explícito siempre (`control`), sin re-etiquetar | §17.2, Anexo B |
| Igualdad explícita «exactamente X» (A8.1) | sin operador EQ en VALUE | A8.1: válida | dependencia de D-MONEY (par `>=`/`<=` del mismo turno); fail-closed hasta entonces | §15.2, §19 |
| Primer turno con valor ambiguo | M1: `CLARIFY` con su código | — | admisible ≠ ejecutable; M2a no cambia el primer turno | §7.2, INV-M2A-06 |
| Preestados del Gold | — | SF063 con `asks = 3`; SF168 ancla SMMLV plurianual | U2 normaliza SF063; D1 declara SF168 estado persistido inválido; G11 exige preestados alcanzables | `gold/ADJUDICATION_2026-10-02.md` |
| Referente extensional/intensional | C0 l.300 (ambigüedad heredada) | G-FU-14 | señal estructural diferida a M2A-5 (gate de M2A-5 y precondición de activar M2A-6); sin regla provisional por defecto | §7.1 fila 5, §19 |
| Top-N > 100 | C1.2 (`main`) ya topa y anuncia | A3 | sin conflicto (el mismatch que T3 vio en el worktree c12 ya está corregido en `main`) | §2 |
| `CLEAR(ORDER)` con CARDINALITY | contrato v1: sin regla ni código (STOP-2 de M2A-3) | `M2A_DESIGN_v2.md:240`: `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | recuperada por el usuario (2026-10-02), decidida sobre el estado | §6.7 (R3-1) |
