# Etiquetador SB (ciego) — slice sintético, ronda `m2a-followup-gold/2.1`

191 registros en `labels_v2_1.jsonl` (ordenados por `gold_id`, `labeler = "SB"`, los campos de entrada byte-idénticos).
Normas: kit_v2_2 (ADJUDICATED_OVERRIDES A1–A7 > SCHEMA_v2_1 > CONTRACT_ADDENDUM_FOLLOWUPS_v2), contrato M1 v1.4–v1.9.
Hoy = 2026-10-01. Validador: `self_check_v2_1.py` -> `self_check.json`: **0 errores, 0 avisos**.
Generador: `build_labels_v2_1.py` (etiqueta escrita a mano por caso); motivo por cambio: `change_rules.json`.

`effects` y `expected_echo` **no** se etiquetaron a mano: los calcula `build_labels_v2_1.py` mecánicamente con §6/§7
(van en el registro porque el formato los lista; el validador oficial los recalcula).

## Distribuciones

### turn_kind

| valor | n |
|---|---|
| DELTA | 146 |
| NEW_SEARCH | 15 |
| OTHER_OPERATION | 11 |
| ANSWER_TO_CLARIFICATION | 10 |
| PROTOCOL | 9 |

### m2a_scope

| valor | n |
|---|---|
| IN | 142 |
| IN_NEW_SEARCH | 15 |
| IN_PROTOCOL | 12 |
| OUT_REFERENT_EXTENSIONAL | 7 |
| OUT_RELATIVE_TO_PREVIOUS | 7 |
| OUT_OPERATION_CHANGE | 4 |
| OUT_ANCHOR_NOT_SEARCH | 2 |
| OUT_NO_ANCHOR | 2 |

### expected_disposition

| valor | n |
|---|---|
| EXECUTE | 131 |
| CLARIFY | 26 |
| NEW_SEARCH | 15 |
| INFORM | 13 |
| REASK | 4 |
| HANDOFF_LEGACY | 2 |

### expected_disposition / disposition_reason

| valor | n |
|---|---|
| EXECUTE / None | 131 |
| NEW_SEARCH / None | 15 |
| INFORM / REFERENT_EXTENSIONAL | 8 |
| CLARIFY / RELATIVE_TO_PREVIOUS_VALUE | 7 |
| CLARIFY / CONTEXT_EXPIRED_CONFIRM | 4 |
| CLARIFY / EMPTY_SEARCH_AFTER_DELTA | 2 |
| CLARIFY / MONTH_YEAR_AMBIGUOUS | 2 |
| CLARIFY / SMMLV_YEAR_AMBIGUOUS | 2 |
| CLARIFY / TERRITORY_VS_CARRIED_BUYER | 2 |
| INFORM / OPERATION_CHANGE:FOLLOW | 2 |
| REASK / ANSWER_EDITS_OTHER_SLOT | 2 |
| CLARIFY / ANCHOR_UNREADABLE | 1 |
| CLARIFY / BUYER_OR_PLACE_AFTER_ACTOR | 1 |
| CLARIFY / NO_ANCHOR | 1 |
| CLARIFY / REGION_NOT_IN_LIST | 1 |
| CLARIFY / STATE_NOT_IN_UNIVERSE | 1 |
| CLARIFY / UNIT_AMBIGUOUS | 1 |
| CLARIFY / UNRESOLVED_VALUE:UNIVERSE | 1 |
| HANDOFF_LEGACY / ANCHOR_NOT_SEARCH:COUNT | 1 |
| HANDOFF_LEGACY / ANCHOR_NOT_SEARCH:RANK_SUPPLIERS | 1 |
| INFORM / CLEAR_ON_EMPTY_SLOT | 1 |
| INFORM / OPERATION_CHANGE:EXPORT | 1 |
| INFORM / OPERATION_CHANGE:SUM_VALUE | 1 |
| REASK / ANSWER_UNRESOLVED | 1 |
| REASK / OPTION_NOT_CHOSEN | 1 |

### ops por ranura

| valor | n |
|---|---|
| TIME | 49 |
| VALUE.lower | 27 |
| ORDER | 20 |
| VALUE.upper | 18 |
| BUYER | 15 |
| GEOGRAPHY | 15 |
| UNIVERSE | 13 |
| CARDINALITY | 8 |
| TOPIC | 8 |
| STATE | 7 |
| MODALITY | 5 |
| CONTRACT_TYPE_OR_NATURE | 3 |
| None | 2 |
| VALUE | 1 |

### ops por op · ranura

| valor | n |
|---|---|
| SET · TIME | 41 |
| SET · VALUE.lower | 27 |
| SET · VALUE.upper | 18 |
| SET_ORDER · ORDER | 18 |
| SET · GEOGRAPHY | 12 |
| SET_UNIVERSE · UNIVERSE | 12 |
| SET · BUYER | 8 |
| SET · TOPIC | 7 |
| SET · CARDINALITY | 6 |
| SET · STATE | 6 |
| ANSWER · TIME | 5 |
| ANSWER · BUYER | 4 |
| SET · MODALITY | 4 |
| CLEAR · TIME | 3 |
| SET · CONTRACT_TYPE_OR_NATURE | 3 |
| CANCEL_PENDING · None | 2 |
| CLEAR · BUYER | 2 |
| CLEAR · CARDINALITY | 2 |
| CLEAR · GEOGRAPHY | 2 |
| ANSWER · GEOGRAPHY | 1 |
| CLEAR · MODALITY | 1 |
| CLEAR · ORDER | 1 |
| CLEAR · STATE | 1 |
| CLEAR · TOPIC | 1 |
| CLEAR · VALUE | 1 |
| KEEP · BUYER | 1 |
| KEEP · ORDER | 1 |
| KEEP · UNIVERSE | 1 |

Ops: 191 en total; `unresolved` 23; `refers_to_result_set` 55; `depends_on_prior_value` 25.

### effects (derivado)

| valor | n |
|---|---|
| (vacío) | 144 |
| PENDING_CANCELLED | 10 |
| UNIVERSE_CHANGED | 10 |
| VALUE_RANGE_REPLACED | 9 |
| CONTEXT_RESET | 6 |
| ORDER_DIRECTION_DEFAULTED | 3 |
| TOPIC_REPLACED | 3 |
| TOPN_CARRIED | 3 |
| POLARITY_INVERTED | 2 |
| TOPN_CAPPED | 1 |
| VIEW_REDERIVED | 1 |

### expected_echo (derivado)

| valor | n |
|---|---|
| CARRIED | 124 |
| (vacío) | 56 |
| UNIVERSE_CHANGED | 10 |
| VALUE_RANGE_REPLACED | 9 |
| RELATIVE_WINDOW_RESOLVED | 8 |
| YEAR_INHERITED | 8 |
| CONTEXT_RESET | 6 |
| UNIT_INHERITED | 6 |
| CONTEXT_EXPIRED | 4 |
| TOPIC_REPLACED | 3 |
| TOPN_CARRIED | 3 |
| UNIT_SHARED_IN_RANGE | 3 |
| VIEW_DERIVED | 3 |
| ORDER_DIRECTION_DEFAULTED | 2 |
| POLARITY_INVERTED | 2 |
| YEAR_CLOCK_INFERRED | 2 |
| TOPN_CAPPED | 1 |
| VIEW_REDERIVED | 1 |

### ambiguity

| valor | n |
|---|---|
| CLEAR | 164 |
| AMBIGUOUS | 27 |

### confidence

| valor | n |
|---|---|
| HIGH | 156 |
| MEDIUM | 20 |
| LOW | 15 |

### gap_codes

| valor | n |
|---|---|
| UNIVERSE_FRAGMENT | 18 |
| UNIT | 10 |
| RELATIVE_ANCHOR | 9 |
| RANGE_NO_INTERIOR | 8 |
| POLARITY_OTHER_VALUE | 7 |
| PRIOR_NOT_EXECUTED | 6 |
| SMMLV | 6 |
| GEOBUYER | 5 |
| MONTHYEAR | 5 |
| REGION_VOCAB | 5 |
| TTL | 5 |
| COUNTERPART | 4 |
| TOPIC | 4 |
| OPSWITCH | 3 |
| ORDER_DIRECTION | 3 |
| SET | 3 |
| ANSWER_PLUS_EDIT | 2 |
| EMPTY_AFTER_DELTA | 2 |
| PENDING_REJECTED | 2 |
| POLARITY_SAME_VALUE | 2 |
| PV | 2 |
| TOPN_CAP | 2 |
| UNIVTIME | 2 |
| ANY_OPTION | 1 |
| CLEAR_ABSENT | 1 |
| NEGATED_COMPARATOR | 1 |
| NO_ANCHOR | 1 |
| OPPORTUNITY_STATE | 1 |
| PAGE_VS_TOPN | 1 |
| PENDING_OPSWITCH | 1 |
| STATE_UNIVERSE | 1 |
| TOTALES | 1 |

## Cambios respecto de mi ronda anterior (162 comunes)

Disposición, razón o `resulting_ir` (comparada por significado: universo, vista, condiciones, orden, n). 63 registros.

| id | tipo | antes | ahora | regla que lo causa |
|---|---|---|---|---|
| SF021 | resulting_ir | EXECUTE/None | EXECUTE/None | cardinalidad 250 -> n=100/n_requested=250; AMBIGUOUS -> CLEAR (A3/F20) |
| SF025 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF026 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF027 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF028 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF029 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF030 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF031 | resulting_ir | CLARIFY/MONTH_YEAR_MULTI_YEAR_PRIOR | CLARIFY/MONTH_YEAR_AMBIGUOUS | resulting_ir -> null (SCHEMA §5.2: CLARIFY no F11); razón MONTH_YEAR_MULTI_YEAR_PRIOR -> MONTH_YEAR_AMBIGUOUS; AMBIGUOUS -> CLEAR |
| SF033 | resulting_ir | CLARIFY/MONTH_YEAR_RELATIVE_PRIOR | CLARIFY/MONTH_YEAR_AMBIGUOUS | resulting_ir -> null (§5.2); razón -> MONTH_YEAR_AMBIGUOUS |
| SF036 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF037 | disposición | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | EXECUTE/None | CLARIFY/OUT_RELATIVE_TO_PREVIOUS -> EXECUTE/IN (A5) |
| SF038 | disposición | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | EXECUTE/None | CLARIFY/OUT_RELATIVE_TO_PREVIOUS -> EXECUTE/IN 2023 (A5) |
| SF039 | resulting_ir | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | resulting_ir -> null (§5.2 / O14) |
| SF040 | resulting_ir | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | resulting_ir -> null (§5.2 / O14: sin aritmética) |
| SF042 | disposición | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | EXECUTE/None | CLARIFY -> EXECUTE (A5) |
| SF043 | resulting_ir | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | resulting_ir -> null (§5.2) |
| SF044 | resulting_ir | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | resulting_ir -> null (§5.2) |
| SF045 | disposición | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | EXECUTE/None | CLARIFY -> EXECUTE (A5) |
| SF046 | resulting_ir | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | CLARIFY/RELATIVE_TO_PREVIOUS_VALUE | resulting_ir -> null (§5.2) |
| SF047 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null); value_hint en formato §3 |
| SF048 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null); value_hint en formato §3 |
| SF049 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null) |
| SF050 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null) |
| SF051 | disposición | EXECUTE/None | REASK/ANSWER_EDITS_OTHER_SLOT | EXECUTE -> REASK/ANSWER_EDITS_OTHER_SLOT (F25) |
| SF052 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null); value_hint en formato §3 |
| SF053 | disposición | EXECUTE/None | REASK/ANSWER_EDITS_OTHER_SLOT | EXECUTE -> REASK (F25); AMBIGUOUS -> CLEAR (A4: anafórico no es autosuficiente) |
| SF054 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF055 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | AMBIGUOUS -> CLEAR (A7) |
| SF056 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF057 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null); value_hint en formato §3 |
| SF059 | formato | EXECUTE/None | EXECUTE/None | resulting_ir: valor X ahora disponible en pending (Alcaldía de Soledad); scope -> IN_PROTOCOL |
| SF060 | resulting_ir | EXECUTE/None | EXECUTE/None | resulting_ir: GEOGRAPHY Soledad MUNICIPALITY (antes UNRESOLVED/UNKNOWN); scope -> IN_PROTOCOL |
| SF061 | disposición | EXECUTE/None | REASK/OPTION_NOT_CHOSEN | EXECUTE (OR de opciones) -> REASK/OPTION_NOT_CHOSEN (F24) |
| SF062 | disposición | HANDOFF_LEGACY/OPERATION_HANDOFF:SUM_VALUE | INFORM/OPERATION_CHANGE:SUM_VALUE | HANDOFF_LEGACY -> INFORM/OPERATION_CHANGE:SUM_VALUE (§5.2: con ancla Foundation nada va a legacy); resulting_ir -> null |
| SF063 | disposición | EXECUTE/None | NEW_SEARCH/None | DELTA/EXECUTE -> NEW_SEARCH UNSPECIFIED (A4 + A7); la lectura anterior pasa a alternativa |
| SF064 | formato | CLARIFY/CONTEXT_EXPIRED_CONFIRM | CLARIFY/CONTEXT_EXPIRED_CONFIRM | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |
| SF065 | disposición | HANDOFF_LEGACY/REFERENT_EXTENSIONAL | INFORM/REFERENT_EXTENSIONAL | HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5) |
| SF066 | disposición | CLARIFY/REFERENT_UNRESOLVED | INFORM/REFERENT_EXTENSIONAL | HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5) |
| SF067 | disposición | HANDOFF_LEGACY/REFERENT_EXTENSIONAL | INFORM/REFERENT_EXTENSIONAL | HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5) |
| SF070 | disposición | HANDOFF_LEGACY/REFERENT_EXTENSIONAL | INFORM/REFERENT_EXTENSIONAL | HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5) |
| SF071 | disposición | HANDOFF_LEGACY/REFERENT_EXTENSIONAL | INFORM/REFERENT_EXTENSIONAL | HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5) |
| SF072 | disposición | HANDOFF_LEGACY/REFERENT_EXTENSIONAL | INFORM/REFERENT_EXTENSIONAL | HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5) |
| SF079 | resulting_ir | CLARIFY/STATE_NOT_IN_UNIVERSE | CLARIFY/STATE_NOT_IN_UNIVERSE | resulting_ir -> null (§5.2); AMBIGUOUS -> CLEAR (F27) |
| SF102 | resulting_ir | CLARIFY/UNIT_AMBIGUOUS | CLARIFY/UNIT_AMBIGUOUS | resulting_ir -> null; AMBIGUOUS -> CLEAR (A1: «única unidad activa inequívoca en el VALUE previo») |
| SF103 | disposición | CLARIFY/SMMLV_UNSUPPORTED | EXECUTE/None | texto nuevo (antes «mínimo 800» sobre SMMLV): caso distinto; EXECUTE >=350 millones, AMBIGUOUS + RANGE_NO_INTERIOR (A1) |
| SF105 | resulting_ir | EXECUTE/None | EXECUTE/None | unit PESOS -> MILES (lista cerrada §3) |
| SF113 | disposición | EXECUTE/None | INFORM/CLEAR_ON_EMPTY_SLOT | EXECUTE (no-op) -> INFORM/CLEAR_ON_EMPTY_SLOT (F18); AMBIGUOUS -> CLEAR |
| SF114 | disposición | EXECUTE/None | CLARIFY/EMPTY_SEARCH_AFTER_DELTA | EXECUTE -> CLARIFY/EMPTY_SEARCH_AFTER_DELTA (F19); resulting_ir -> null |
| SF116 | disposición | EXECUTE/None | CLARIFY/EMPTY_SEARCH_AFTER_DELTA | EXECUTE -> CLARIFY/EMPTY_SEARCH_AFTER_DELTA (F19); resulting_ir -> null |
| SF123 | disposición | EXECUTE/None | CLARIFY/TERRITORY_VS_CARRIED_BUYER | EXECUTE (AMBIGUOUS) -> CLARIFY/TERRITORY_VS_CARRIED_BUYER con dos alternativas (F30); resulting_ir -> null |
| SF124 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null) |
| SF128 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null) |
| SF130 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null) |
| SF134 | razón | NEW_SEARCH/SELF_CONTAINED_REQUEST | NEW_SEARCH/None | sólo código: §5.3 (NEW_SEARCH lleva razón null) |
| SF137 | resulting_ir | CLARIFY/NO_ANCHOR | CLARIFY/NO_ANCHOR | m2a_scope OUT_ANCHOR_NOT_SEARCH -> OUT_NO_ANCHOR; resulting_ir -> null; AMBIGUOUS -> CLEAR (F28) |
| SF143 | razón | HANDOFF_LEGACY/ANCHOR_OWNER_LEGACY | HANDOFF_LEGACY/ANCHOR_NOT_SEARCH:COUNT | sólo código: §5.3 ANCHOR_NOT_SEARCH:<op> |
| SF144 | razón | HANDOFF_LEGACY/ANCHOR_OWNER_LEGACY | HANDOFF_LEGACY/ANCHOR_NOT_SEARCH:RANK_SUPPLIERS | sólo código: §5.3 ANCHOR_NOT_SEARCH:<op> |
| SF148 | disposición | EXECUTE/None | CLARIFY/UNRESOLVED_VALUE:UNIVERSE | EXECUTE -> CLARIFY (§5.2 #3, ancla dual al paginar) |
| SF153 | disposición | HANDOFF_LEGACY/OPERATION_HANDOFF:FOLLOW | INFORM/OPERATION_CHANGE:FOLLOW | HANDOFF_LEGACY -> INFORM/OPERATION_CHANGE:FOLLOW (§5.2 #4); resulting_ir -> null |
| SF154 | disposición | HANDOFF_LEGACY/OPERATION_HANDOFF:FOLLOW | INFORM/OPERATION_CHANGE:FOLLOW | HANDOFF_LEGACY -> INFORM (§5.2 #4); resulting_ir -> null; AMBIGUOUS -> CLEAR |
| SF155 | disposición | EXECUTE/None | CLARIFY/TERRITORY_VS_CARRIED_BUYER | EXECUTE (AMBIGUOUS) -> CLARIFY/TERRITORY_VS_CARRIED_BUYER con dos alternativas (F30); resulting_ir -> null |
| SF159 | resulting_ir | CLARIFY/REGION_NOT_IN_LIST | CLARIFY/REGION_NOT_IN_LIST | resulting_ir -> null (§5.2) |
| SF161 | formato | EXECUTE/None | EXECUTE/None | sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD) |

Otros cambios (sólo alcance, ambigüedad u ops, misma disposición e IR): 21.

| id | cambio y regla |
|---|---|
| SF011 | etiqueta igual; la ambigüedad desaparece por F16 (AMBIGUOUS -> CLEAR) |
| SF012 | igual; AMBIGUOUS -> CLEAR por F16 |
| SF058 | m2a_scope IN -> IN_PROTOCOL (input_kind EVENT) |
| SF075 | AMBIGUOUS -> CLEAR (UNIVTIME cerrado por F1) |
| SF084 | CLEAR -> AMBIGUOUS (OPPORTUNITY_STATE vivo en §8) |
| SF090 | CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1 retira la sustitución automática); IR igual |
| SF091 | CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual |
| SF094 | CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual |
| SF095 | CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual |
| SF099 | CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual |
| SF100 | ops: se añade CLEAR(VALUE) por O4; IR igual |
| SF104 | AMBIGUOUS -> CLEAR (A1 retira F26 / magnitud absurda) |
| SF140 | AMBIGUOUS -> CLEAR (F21) |
| SF142 | m2a_scope IN -> OUT_NO_ANCHOR |
| SF145 | m2a_scope -> IN_PROTOCOL |
| SF146 | m2a_scope -> IN_PROTOCOL |
| SF147 | m2a_scope -> IN_PROTOCOL; CLEAR -> AMBIGUOUS (PAGE_VS_TOPN vivo) |
| SF149 | ops SET_ORDER -> [] (O15); turn_kind DELTA -> PROTOCOL; scope -> IN_PROTOCOL |
| SF150 | ops -> [] (O15); PROTOCOL / IN_PROTOCOL |
| SF151 | ops -> [] (O15); PROTOCOL / IN_PROTOCOL |
| SF152 | ops -> [] (O15); PROTOCOL / IN_PROTOCOL |

Casos nuevos SF163–SF191: 29, etiquetados desde cero.

## AMBIGUOUS (27), agrupados por la regla que falta

- **A1 — rango sin interior tras la cota por lado (RANGE_NO_INTERIOR)**: SF090, SF091, SF094, SF095, SF099, SF103, SF182, SF183
- **A5 — referente temporal relativo: al valor previo o al reloj / granularidad compatible (RELATIVE_ANCHOR, X18)**: SF038, SF173, SF175, SF177
- **A4 — frontera de autosuficiencia: fragmento vs consulta nueva (UNIVERSE_FRAGMENT)**: SF126, SF134, SF185, SF187
- **A4 + A7 — «no, mejor X» con X que se interpreta sola (PENDING_REJECTED + UNIVERSE_FRAGMENT)**: SF063
- **A2 — SMMLV sin ningún TIME (SMMLV)**: SF169
- **X9 — qué cuenta como «sin condición» en F19 (EMPTY_AFTER_DELTA)**: SF114, SF116
- **X12 + contrato D7 — territorio contenido en el del comprador / «de <lugar>» comprador o lugar (GEOBUYER)**: SF068, SF069
- **X7 — negar un valor contenido en el positivo vigente (POLARITY_OTHER_VALUE + GEOBUYER)**: SF162
- **Forma de la cota negada (NEGATED_COMPARATOR, vivo)**: SF013
- **Estado redundante con la lente de oportunidad (OPPORTUNITY_STATE, vivo)**: SF084
- **Paginación vs cardinalidad (PAGE_VS_TOPN, vivo)**: SF147
- **Sustantivo de universo que repite el vigente («licitaciones» sobre procesos): sin gap_code en §8**: SF132

## Dónde no alcanzaron el esquema o las reglas

1. **INFORM sin conjunto de resultados (A6).** «muéstrame más resultados» sobre un previo NOT_EXECUTED (SF179) debe ser `INFORM`,
   pero §5.3 no tiene un código para «no hay resultados que paginar». Usé `REFERENT_EXTENSIONAL`, el más cercano. Falta un código propio.
2. **Paginar un ancla dual (§5.2 #3).** El esquema manda `CLARIFY` (SF148), pero §5.3 no tiene código. Usé `UNRESOLVED_VALUE:UNIVERSE`
   (UNIVERSE no es una dimensión del contrato). Falta un código propio.
3. **O12 frente a O13.** «No, mejor X» con X autosuficiente (SF063) es `NEW_SEARCH`: O13 pide `ops = []` y O12 pide `CANCEL_PENDING`.
   Conservé el `CANCEL_PENDING`; el validador lo admite.
4. **SMMLV con TIME plurianual arrastrado (SF168).** §5.2 pide `unresolved = true` en la operación que bloquea, pero aquí no la causa
   ninguna operación del turno (la duda viene del previo). Todas quedan en `unresolved = false`.
5. **A1 y la lectura literal por lado.** En los ocho casos RANGE_NO_INTERIOR, la lectura «conservar las dos cotas» deja un rango vacío
   que §9-F2 prohíbe. Por eso la alternativa es `CLARIFY`, sin IR, y va en `notes`, no en `alternatives`. El derivado marca
   `VALUE_RANGE_REPLACED` porque una cota desaparece sin operación en su lado, aunque A1 retiró el inciso (a): si la mejor lectura es
   la sustitución, hace falta ese anuncio.
6. **`alternatives` sólo admite IR.** Las alternativas de disposición con la misma IR (CLARIFY o REASK frente a EXECUTE) no tienen forma
   propia; van en `notes`.
7. **`UNIVERSE_CHANGED` y la vista.** El efecto se define por `universe`; pasar de PROCESSES/RECORDS a PROCESSES/OPPORTUNITIES
   (SF084) no lo dispara, aunque cambia lo recuperado.
8. **Recálculo SMMLV al cambiar de año (A2, SF166).** A2 exige un «eco de la conversión», pero §7 no tiene esa clase.
9. **Unidad MILES.** «900 mil pesos» pasa a `MILES` por la lista de §3 (SF105); antes era `PESOS`.
10. **Tensiones declaradas sin resolver:** X7 (SF162), X9 (SF114, SF116), X12 (SF068, SF069), X15 (SF064: gana F11 por §5.3), X16 (SF087, SF098, SF107: el eco derivado
    lleva UNIT_SHARED_IN_RANGE), X18 (SF038, SF173). X8, X13 y X17 no aparecen en este slice.
11. **F29 frente a cambio de operación (SF062).** La precedencia §5.2 (#4 antes de IN) da `INFORM / OPERATION_CHANGE:SUM_VALUE`
    aunque la pregunta BUYER siga abierta. Ninguna regla dice si el comprador ambiguo se arrastra al nuevo dueño.
