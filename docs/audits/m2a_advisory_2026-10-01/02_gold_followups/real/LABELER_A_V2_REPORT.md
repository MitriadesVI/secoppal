# LABELER A (ciego) - re-etiquetado v2 del Gold de follow-ups

Registros: **65** (FOLLOWUP_DELTA REAL del Gold sellado: 65). Esquema `m2a-followup-gold/2`; contrato M1_0 v1.4 (+ enmiendas v1.5-v1.9) + adenda F1-F15. Sin texto REAL: solo `gold_id`.

### turn_kind

| valor | n |
|---|---|
| DELTA | 61 |
| ANSWER_TO_CLARIFICATION | 2 |
| OTHER_OPERATION | 1 |
| UNCLEAR | 1 |


### m2a_scope

| valor | n |
|---|---|
| IN | 56 |
| OUT_ANCHOR_NOT_SEARCH | 9 |


### expected_disposition

| valor | n |
|---|---|
| EXECUTE | 49 |
| HANDOFF_LEGACY / ANCHOR_NOT_SEARCH:PROFILE_ACTOR | 5 |
| CLARIFY / SMMLV_UNSUPPORTED | 4 |
| HANDOFF_LEGACY / ANCHOR_NOT_SEARCH:COUNT | 3 |
| CLARIFY / CONTEXT_EXPIRED_CONFIRM | 2 |
| CLARIFY / WITH_AFTER_ACTOR_BUYER_OR_PLACE | 1 |
| HANDOFF_LEGACY / ANCHOR_NOT_SEARCH:RANK_SUPPLIERS | 1 |


### confidence

| valor | n |
|---|---|
| HIGH | 35 |
| MEDIUM | 24 |
| LOW | 6 |


### source_kind

| valor | n |
|---|---|
| ORGANIC | 42 |
| SCRIPTED | 23 |


### prior.selection_rule

| valor | n |
|---|---|
| IMMEDIATE_SUBSTANTIVE | 44 |
| DOMINANT_IN_REPLAY | 11 |
| CHAIN_LABELED | 10 |


### prior.owner

| valor | n |
|---|---|
| FOUNDATION_SEARCH | 56 |
| LEGACY:PROFILE_ACTOR | 5 |
| LEGACY:COUNT | 3 |
| LEGACY:RANK_SUPPLIERS | 1 |


### prior.state

| valor | n |
|---|---|
| EXECUTED | 57 |
| PENDING | 3 |
| EXPIRED | 2 |
| NOT_EXECUTED | 2 |
| EXECUTED_EMPTY | 1 |


### prior.chain (turnos de protocolo intermedios)

| valor | n |
|---|---|
| (ninguno) | 60 |
| PROTOCOL:option | 4 |
| PROTOCOL:page,PROTOCOL:page | 1 |


### ops por ranura (operaciones | registros distintos)

| slot | op | operaciones | registros |
|---|---|---|---|
| ACTOR | SET | 1 | 1 |
| BUYER | SET | 4 | 4 |
| GEOGRAPHY | ANSWER | 1 | 1 |
| GEOGRAPHY | SET | 7 | 7 |
| MODALITY | SET | 2 | 2 |
| None | ANSWER | 1 | 1 |
| ORDER | SET_ORDER | 7 | 7 |
| STATE | KEEP | 1 | 1 |
| STATE | SET | 1 | 1 |
| TIME | KEEP | 2 | 2 |
| TIME | SET | 20 | 20 |
| TOPIC | SET | 2 | 2 |
| UNIVERSE | SET_UNIVERSE | 1 | 1 |
| VALUE | CLEAR | 1 | 1 |
| VALUE.lower | SET | 17 | 17 |
| VALUE.upper | SET | 8 | 8 |

Total operaciones: 76; registros sin operaciones: 1 (R0483).

### operaciones por cantidad en un registro

| n ops | registros |
|---|---|
| 1 | 52 |
| 2 | 12 |
| 0 | 1 |


### effects

| effect | registros |
|---|---|
| VALUE_RANGE_REPLACED | 9 |
| VIEW_REDERIVED | 3 |
| TOPIC_REPLACED | 1 |
| UNIVERSE_CHANGED | 1 |


Registros con effects: TOPIC_REPLACED: R0286; UNIVERSE_CHANGED: R0094; VALUE_RANGE_REPLACED: R0180, R0187, R0192, R0205, R0308, R0309, R0311, R0531, R0532; VIEW_REDERIVED: R0081, R0106, R0276

### expected_echo (clases exigidas)

| clase | registros |
|---|---|
| CARRIED | 64 |
| VIEW_DERIVED | 10 |
| VALUE_RANGE_REPLACED | 9 |
| UNIT_INHERITED | 7 |
| YEAR_INHERITED | 4 |
| CONTEXT_EXPIRED | 2 |
| TOPIC_REPLACED | 1 |


Clases no usadas en ningun registro: TOPN_CARRIED (ningun caso con top-N previo; F10 no se ejercita). No se declaro ninguna clase nueva.

### procedencia de ano (operaciones TIME)

| year_source | ops |
|---|---|
| STATED | 18 |
| PREVIOUS_ACCEPTED_TIME | 4 |


### procedencia de unidad (operaciones VALUE)

| unit_source | ops |
|---|---|
| STATED | 18 |
| SIBLING_IN_RANGE | 5 |
| PREVIOUS_ACCEPTED_VALUE | 2 |


Registros con year_source no STATED: R0099, R0496, R0497, R0498.

Registros con unit_source no STATED: R0180, R0192, R0282, R0311, R0529, R0531, R0532.

### Casos con gap_seconds > 1800 (F11)

| gold_id | gap_seconds | disposicion | razon |
|---|---|---|---|
| R0223 | 14498 | CLARIFY | CONTEXT_EXPIRED_CONFIRM |
| R0286 | 45148 | CLARIFY | CONTEXT_EXPIRED_CONFIRM |


Siguientes huecos mas largos (todos <= 1800 s): R0202=389s, R0192=294s, R0189=184s. Hueco medido entre la marca de tiempo del follow-up y la del ultimo turno anterior (incluidos turnos de protocolo intermedios); en corridas scripted es 0-1 s.

### gap_codes (registros por codigo)

| codigo | registros |
|---|---|
| SET | 10 |
| UNIT | 7 |
| YEARRANGE | 5 |
| OPSWITCH | 4 |
| SELECT | 4 |
| SMMLV | 4 |
| COUNTERPART | 3 |
| MONTHYEAR | 3 |
| PV | 3 |
| VCOMP | 3 |
| ACTORTIME | 2 |
| REGION_VOCAB | 2 |
| TTL | 2 |
| B5RANGE | 1 |
| COMPARATOR_FORM | 1 |
| ENADELANTE | 1 |
| FIRMADOS | 1 |
| GEOBUYER | 1 |
| TOPIC | 1 |
| TOTALES | 1 |
| UNIVERSE_FRAGMENT | 1 |
| UNIVTIME | 1 |


### gap_codes por id

| gold_id | gap_codes |
|---|---|
| R0079 | SET |
| R0081 | SET, PV |
| R0083 | SET |
| R0087 | SET |
| R0089 | SET |
| R0094 | UNIVTIME, UNIVERSE_FRAGMENT |
| R0099 | SET |
| R0106 | PV |
| R0180 | UNIT |
| R0187 | COMPARATOR_FORM |
| R0189 | OPSWITCH |
| R0192 | UNIT, SET |
| R0202 | GEOBUYER |
| R0205 | ENADELANTE |
| R0223 | SET, TTL |
| R0274 | REGION_VOCAB |
| R0276 | PV |
| R0281 | REGION_VOCAB |
| R0282 | UNIT |
| R0286 | TTL, TOPIC |
| R0301 | SMMLV |
| R0303 | YEARRANGE, SMMLV |
| R0304 | SMMLV |
| R0306 | SMMLV |
| R0310 | VCOMP |
| R0311 | UNIT |
| R0313 | YEARRANGE |
| R0314 | YEARRANGE |
| R0319 | OPSWITCH, ACTORTIME |
| R0320 | COUNTERPART |
| R0321 | COUNTERPART |
| R0375 | B5RANGE, OPSWITCH |
| R0418 | COUNTERPART, SELECT |
| R0431 | SELECT |
| R0432 | YEARRANGE |
| R0435 | SELECT |
| R0441 | OPSWITCH, ACTORTIME, SELECT |
| R0450 | YEARRANGE |
| R0459 | SET |
| R0464 | VCOMP |
| R0483 | FIRMADOS |
| R0496 | MONTHYEAR |
| R0497 | MONTHYEAR |
| R0498 | MONTHYEAR |
| R0515 | SET |
| R0529 | UNIT |
| R0531 | UNIT, VCOMP |
| R0532 | UNIT |
| R0560 | TOTALES |

Sin gap_codes: R0075, R0092, R0093, R0107, R0197, R0201, R0233, R0295, R0308, R0309, R0312, R0336, R0409, R0484, R0556, R0561.


Nota: un gap_code se coloca cuando el caso toca el tema aunque la adenda ya lo decida (p.ej. MONTHYEAR con F7, SET con F4, PV con F9, SMMLV con F14, REGION_VOCAB con F13); la columna de notas de cada registro dice cual regla lo cierra. Realmente sin decidir tras la adenda: B5RANGE (R0375), COMPARATOR_FORM (R0187), OPSWITCH (R0189, R0319, R0375, R0441), TOTALES (R0560), FIRMADOS (R0483), ENADELANTE (R0205), GEOBUYER (R0202), UNIVTIME/UNIVERSE_FRAGMENT (R0094), TOPIC combinado con TTL.


### Ambiguedad declarada (18)

R0075, R0081, R0094, R0187, R0189, R0192, R0202, R0205, R0306, R0319, R0320, R0321, R0375, R0418, R0435, R0441, R0483, R0560


## Convenciones propias declaradas

- `prior.gap_seconds`: diferencia de marcas de tiempo (traces) entre el follow-up y el ULTIMO turno anterior (incluye turnos de protocolo). Para el estado EXPIRED se usa ese mismo hueco.
- `prior.state`: observado en el trace del previo (EXECUTED si hubo resultados; EXECUTED_EMPTY si cero; NOT_EXECUTED con error de fuente; PENDING con aclaracion; EXPIRED si gap > 1800 s). Excepcion declarada: R0321 modela el flujo ideal (R0320 deja pendiente la aclaracion comprador/lugar).
- `prior.ir` de un previo no busqueda (C7): conserva `operation` y las condiciones transportadas, `product_view = null`.
- `unit`: lista PESOS, MILES, MILLONES, MIL_MILLONES, SMMLV (no estaba enumerada en el esquema; solo se usan MILLONES y SMMLV).
- `effects`: VALUE_RANGE_REPLACED cuando el previo tenia VALUE y ninguna de sus cotas sobrevive en la IR resultante (y no hubo CLEAR); VIEW_REDERIVED solo cuando la vista derivada cambia de valor; UNIVERSE_CHANGED con SET_UNIVERSE que cambia el universo; TOPIC_REPLACED con SET TOPIC sobre TOPIC ocupado.
- `expected_echo`: CARRIED si sobrevive alguna condicion no tocada; UNIT_INHERITED tambien para SIBLING_IN_RANGE (F6 pide anunciar toda herencia); VIEW_DERIVED siempre que el resultado sea SEARCH UNSPECIFIED (la vista por defecto se anuncia). En registros HANDOFF_LEGACY el eco se calcula igual (informativo).
- `ANSWER` con `slot = null` (R0560): no existe ranura de aspecto en la lista cerrada.
- IR: campos extra `logic` (OR de TOPIC en R0187), `aspect` (COUNT) y `host_notice` (ACTOR_TIME_ON_SIGNATURE_DATE, v1.9) ademas del esquema.
- Precedencia: la regla 7 (ancla no busqueda -> OUT_ANCHOR_NOT_SEARCH + HANDOFF_LEGACY) prevalece sobre OUT_OPERATION_CHANGE y sobre F12/B5 (CLARIFY); OUT_OPERATION_CHANGE, OUT_REFERENT_EXTENSIONAL y OUT_RELATIVE_TO_PREVIOUS (F15) no aparecen.
- F4 (re-consulta): ninguna etiqueta `OUT_REFERENT_EXTENSIONAL`; `refers_to_result_set = true` solo donde el fragmento trae «los/aquellos/alguno/alguna».


## Cambios v1 -> v2 y regla que los motiva

| regla | registros | v1 -> v2 |
|---|---|---|
| F11 (vigencia 30 min) | R0223, R0286 | EXECUTE -> CLARIFY `CONTEXT_EXPIRED_CONFIRM`; R0223 no tenia el hueco medido al ultimo turno (dos paginaciones); R0286 pasaba de TTL abierto a regla |
| F14 (SMMLV) | R0301, R0303, R0304, R0306 | EXECUTE -> CLARIFY `SMMLV_UNSUPPORTED` (en R0303/R0304/R0306 el monto SMMLV se arrastra del previo y la IR completa lo contiene) |
| F12 («con X» tras ACTOR) | R0418 (R0320, R0321 en handoff) | EXECUTE -> CLARIFY `WITH_AFTER_ACTOR_BUYER_OR_PLACE`; R0320/R0321 mantienen BUYER/GEOGRAPHY como mejor lectura pero su disposicion es HANDOFF_LEGACY por ancla |
| Regla 7 (ancla no busqueda) | R0189, R0319, R0320, R0321, R0375, R0441, R0483, R0560, R0561 | EXECUTE (R0189, R0319, R0320, R0321, R0441, R0561) o CLARIFY (R0375, R0483, R0560) -> HANDOFF_LEGACY + OUT_ANCHOR_NOT_SEARCH; `resulting_ir` ahora tambien para R0483 y R0560 (v1 la dejaba nula) |
| 2.2.1 (toda condicion citada es operacion) | R0099, R0189, R0336 | nuevas operaciones KEEP (TIME «ese ano», STATE «en convocatoria», TIME «en 2026» repetido); v1 no las listaba |
| F2 + 2.3 (VALUE por lado) | R0180, R0187, R0192, R0205, R0282, R0308, R0309, R0310, R0311, R0464, R0529, R0531, R0532 | la IR resultante es la misma que en v1; cambia la representacion (SET por lado, rango = 2 SET con misma quote, `VALUE_RANGE_REPLACED` donde ninguna cota previa sobrevive: R0180, R0187, R0192, R0205, R0308, R0309, R0311, R0531, R0532); VCOMP (R0310, R0464, R0531) queda decidido: el lado nuevo completa el rango |
| F5/F6 (unidad) | R0180, R0192, R0282, R0311, R0529, R0531, R0532 | `unit_source` explicito (SIBLING_IN_RANGE / PREVIOUS_ACCEPTED_VALUE); R0531/R0532 dejan de ser CLARIFY-posible: se hereda la unidad unica del VALUE previo |
| F7 (mes sin ano) | R0496, R0497, R0498 | `year_source = PREVIOUS_ACCEPTED_TIME` y eco `YEAR_INHERITED`; MONTHYEAR deja de ser duda de producto |
| F8 (rango de anos inclusivo) | R0303, R0313, R0314, R0432, R0450 | `between_dates` con fecha fin 12-31 del ano final (R0313 con cota abierta) |
| F9 (vista derivada) | R0081, R0106, R0276 | `VIEW_REDERIVED` explicito (PV6 -> PV5/PV1 -> HISTORICAL_PROCESSES) y eco `VIEW_DERIVED`; ya no es un hueco PV de contrato; resto de registros UNSPECIFIED llevan `view_source = DERIVED` |
| F3 (TOPIC) | R0286 | `TOPIC_REPLACED` (v1: duda TOPIC) |
| F4 (re-consulta) | R0079, R0081, R0083, R0087, R0089, R0099, R0192, R0223, R0459, R0515 | sin cambio de IR; el gap SET queda decidido (re-consulta, no filtro de lo mostrado); `refers_to_result_set` y `depends_on_prior_value` ahora separados |
| F13 (regiones) | R0274, R0281 | EXECUTE (ambas estan en la lista cerrada de regiones) + gap REGION_VOCAB; sin cambio de disposicion |
| Identidad del previo | R0560, R0321, R0107, R0498 | R0560 UNCLEAR -> ANSWER_TO_CLARIFICATION (el previo COUNT quedo pendiente de aclaracion); R0321 DELTA -> ANSWER_TO_CLARIFICATION (flujo ideal tras F12); R0107 prior PENDING; R0498 prior EXECUTED_EMPTY |
| Operacion/universo en handoff | R0189, R0375 | R0189 resulting universo PROCESSES -> UNSPECIFIED + PV6 (la IR previa C7 no guarda el universo); R0375 resulting PROFILE_ACTOR -> SEARCH (VALUE solo filtra registros; PV3 -> CONTRACTS) |
| Sin cambio | resto | misma IR, misma disposicion; se anaden prior.*, m2a_scope, effects y expected_echo |
