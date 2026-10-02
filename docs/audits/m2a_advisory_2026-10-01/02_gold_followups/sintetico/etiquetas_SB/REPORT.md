# Etiquetador SB (ciego) — slice sintético del Gold de follow-ups

Esquema `m2a-followup-gold/2`. 162 registros (`labels_v2.jsonl`, ordenados por `gold_id`), `labeler = "SB"`.
Hoy = 2026-10-01. Validador propio: `self_check.py` -> `self_check.json` (**0 errores**, 1 aviso: SF142 lleva
`resulting_ir = null` porque `prior.ir` es null). Generador de las etiquetas: `build_labels.py` (una etiqueta escrita a mano por caso).

## Convenciones propias (para la adjudicación)

- `quote`: en VALUE y TIME, la expresión entera con su comparador («más de 500 millones», «de enero a junio de 2025»);
  en las exclusiones, sólo el valor («papelería», «Eje Cafetero»), y la polaridad lleva la negación.
- Una exclusión NEGATIVE se **suma** como átomo a la ranura; sólo **reemplaza** al positivo cuando niega exactamente el
  mismo valor (SF011, SF012), porque si no el AND sería vacío.
- VALUE_RANGE_REPLACED sólo cuando F2 lo pide (rango sin interior o instrucción explícita «cambia el valor a…»). Reemplazar
  las dos cotas con un rango nuevo que tiene interior (SF098, SF106) no lleva el efecto (duda anotada).
- Fragmento con sustantivo de universo y contenido propio (tema/comprador) = `NEW_SEARCH`; sin sustantivo de universo = delta
  (F1/F3). No está escrito en el contrato ni en la adenda: va como `UNIVERSE_FRAGMENT`.
- «este año», «este mes», «últimos N meses»: `relative_period` con `year_source = CLOCK_INFERRED`, sin eco YEAR_INHERITED
  (no es herencia F7). Mes sin año y sin TIME previo (F7): CLOCK_INFERRED + YEAR_INHERITED.
- PROTOCOL (paginación) y referentes extensionales («el segundo»): sin ops, `resulting_ir` = ancla sin cambios.
- Cambio de operación (SUM_VALUE, FOLLOW): `HANDOFF_LEGACY` como única disposición de traspaso disponible; la IR lleva la
  operación nueva y las condiciones arrastradas (sin `aspect`, que la forma de `prior.ir` no tiene).
- F15: el valor relativo se calcula en `resulting_ir` cuando es determinable (el doble, el mes siguiente); si no («un poco»),
  queda como `UNRESOLVED: …`.
- Clase de eco fuera de la lista: `TOPN_CAPPED` (SF021, declarada en `notes`, no en `expected_echo`).

## Distribuciones

### turn_kind

| valor | n |
|---|---|
| DELTA | 130 |
| NEW_SEARCH | 11 |
| OTHER_OPERATION | 9 |
| ANSWER_TO_CLARIFICATION | 8 |
| PROTOCOL | 4 |

### m2a_scope

| valor | n |
|---|---|
| IN | 137 |
| OUT_RELATIVE_TO_PREVIOUS | 9 |
| OUT_OPERATION_CHANGE | 7 |
| OUT_REFERENT_EXTENSIONAL | 6 |
| OUT_ANCHOR_NOT_SEARCH | 3 |

### expected_disposition

| valor | n |
|---|---|
| EXECUTE | 119 |
| CLARIFY | 22 |
| NEW_SEARCH | 11 |
| HANDOFF_LEGACY | 10 |

### disposition_reason (no EXECUTE)

| valor | n |
|---|---|
| SELF_CONTAINED_REQUEST | 11 |
| RELATIVE_TO_PREVIOUS_VALUE | 9 |
| REFERENT_EXTENSIONAL | 5 |
| CONTEXT_EXPIRED_CONFIRM | 4 |
| ANCHOR_OWNER_LEGACY | 2 |
| OPERATION_HANDOFF:FOLLOW | 2 |
| ANCHOR_UNREADABLE | 1 |
| MONTH_YEAR_MULTI_YEAR_PRIOR | 1 |
| MONTH_YEAR_RELATIVE_PRIOR | 1 |
| NO_ANCHOR | 1 |
| OPERATION_HANDOFF:SUM_VALUE | 1 |
| REFERENT_UNRESOLVED | 1 |
| REGION_NOT_IN_LIST | 1 |
| SMMLV_UNSUPPORTED | 1 |
| STATE_NOT_IN_UNIVERSE | 1 |
| UNIT_AMBIGUOUS | 1 |

### ops por op · ranura

| valor | n |
|---|---|
| SET · TIME | 41 |
| SET · VALUE.lower | 26 |
| SET_UNIVERSE · UNIVERSE | 22 |
| SET · TOPIC | 19 |
| SET_ORDER · ORDER | 18 |
| SET · BUYER | 16 |
| SET · VALUE.upper | 15 |
| SET · GEOGRAPHY | 12 |
| SET · CARDINALITY | 6 |
| SET · STATE | 6 |
| ANSWER · TIME | 4 |
| CLEAR · TIME | 4 |
| SET · MODALITY | 4 |
| ANSWER · BUYER | 3 |
| CLEAR · BUYER | 3 |
| CLEAR · CARDINALITY | 2 |
| CLEAR · GEOGRAPHY | 2 |
| ANSWER · GEOGRAPHY | 1 |
| CLEAR · MODALITY | 1 |
| CLEAR · ORDER | 1 |
| CLEAR · STATE | 1 |
| CLEAR · TOPIC | 1 |
| KEEP · BUYER | 1 |
| KEEP · ORDER | 1 |
| KEEP · UNIVERSE | 1 |

### ops por ranura

| valor | n |
|---|---|
| TIME | 49 |
| VALUE.lower | 26 |
| BUYER | 23 |
| UNIVERSE | 23 |
| ORDER | 20 |
| TOPIC | 20 |
| GEOGRAPHY | 15 |
| VALUE.upper | 15 |
| CARDINALITY | 8 |
| STATE | 7 |
| MODALITY | 5 |

Otras cifras de ops: total 211; `refers_to_result_set = true` en 52; `depends_on_prior_value = true` en 15.

### inherited.year_source (ops TIME no CLEAR)

| valor | n |
|---|---|
| STATED | 28 |
| CLOCK_INFERRED | 10 |
| PREVIOUS_ACCEPTED_TIME | 5 |
| None | 2 |

### inherited.unit_source (ops VALUE no CLEAR)

| valor | n |
|---|---|
| STATED | 26 |
| PREVIOUS_ACCEPTED_VALUE | 11 |
| SIBLING_IN_RANGE | 3 |
| None | 1 |

### effects

| valor | n |
|---|---|
| (vacío) | 139 |
| UNIVERSE_CHANGED | 10 |
| VALUE_RANGE_REPLACED | 7 |
| TOPIC_REPLACED | 4 |
| VIEW_REDERIVED | 2 |

### expected_echo

| valor | n |
|---|---|
| CARRIED | 115 |
| (vacío) | 43 |
| VALUE_RANGE_REPLACED | 7 |
| CONTEXT_EXPIRED | 4 |
| TOPIC_REPLACED | 4 |
| UNIT_INHERITED | 3 |
| YEAR_INHERITED | 3 |
| TOPN_CARRIED | 2 |
| VIEW_DERIVED | 1 |

### ambiguity

| valor | n |
|---|---|
| CLEAR | 132 |
| AMBIGUOUS | 30 |

### confidence

| valor | n |
|---|---|
| HIGH | 104 |
| MEDIUM | 46 |
| LOW | 12 |

### gap_codes

| valor | n |
|---|---|
| UNIVERSE_FRAGMENT | 12 |
| GEOBUYER | 6 |
| SELECT | 6 |
| TTL | 4 |
| OPSWITCH | 3 |
| UNIT | 3 |
| COUNTERPART | 2 |
| MONTHYEAR | 2 |
| TOPIC | 2 |
| UNIVTIME | 2 |
| REGION_VOCAB | 1 |
| SET | 1 |
| SMMLV | 1 |
| TOTALES | 1 |
| VCOMP | 1 |

## Casos AMBIGUOUS (30), agrupados por la regla que falta

- **Negar el único valor positivo vigente de la misma ranura (¿invertir o aclarar?) — NEW_GAP POLARITY_FLIP_SAME_VALUE**: SF011, SF012
- **Tope de top-N: P8 dice «máximo 100 anunciado» y «por encima, aclarar» — NEW_GAP TOPN_CAP_ABOVE_MAX**: SF021
- **Mes sin año con TIME previo de varios años: F7 manda CLARIFY, pero «solo marzo» admite «marzo de cada año» — MONTHYEAR**: SF031
- **«anterior» anclado al valor previo (F15) o al reloj — NEW_GAP ANTERIOR_PRIOR_VS_CLOCK**: SF038
- **Fragmento vs petición autocontenida (con/sin sustantivo de universo; qué se arrastra) — UNIVERSE_FRAGMENT / NEW_GAP SELF_CONTAINED_RESTATEMENT_DURING_PENDING**: SF053, SF055, SF126, SF134
- **Respuesta «cualquiera» a una aclaración de opciones — NEW_GAP ANY_OPTION_ANSWER**: SF061
- **Cambio de operación con una aclaración pendiente sin resolver — OPSWITCH/TOTALES**: SF062
- **Rechazo + reformulación durante una aclaración pendiente — NEW_GAP PENDING_ABANDONED_BY_DELTA**: SF063
- **Referente extensional no identificable / posicional vs cardinalidad — SELECT**: SF066, SF072
- **«de <lugar>» comprador vs lugar, y lugar nuevo junto a un comprador conservado — GEOBUYER**: SF068, SF069, SF123, SF155, SF162
- **Universo nuevo con TIME arrastrado incompatible (oportunidades / alertas de nuevos con año cerrado) — UNIVTIME**: SF075, SF154
- **Estado de procesos pedido sobre contratos en un delta sin ACTOR — NEW_GAP STATE_NOT_IN_TARGET_UNIVERSE**: SF079
- **Herencia de unidad: F5 «condición que se modifica» por lado o por restricción; unidad heredada con escala absurda — UNIT / NEW_GAP UNIT_INHERITANCE_IMPLAUSIBLE_SCALE**: SF102, SF104
- **Remoción de una ranura vacía — NEW_GAP CLEAR_ABSENT_SLOT**: SF113
- **Delta que deja la IR sin condiciones — NEW_GAP EMPTY_IR_AFTER_CLEAR**: SF114, SF116
- **«licitaciones» sobre un ancla que ya es de procesos (O6 deja la IR igual) — UNIVERSE_FRAGMENT**: SF132
- **Fragmento sin ancla (owner NONE) vs regla 2.2.7 — NEW_GAP NO_ANCHOR_FRAGMENT**: SF137
- **Orden «por fecha» sin dirección — NEW_GAP ORDER_DIRECTION_UNSTATED**: SF140

## NEW_GAP

| código | ids | qué falta |
|---|---|---|
| `POLARITY_FLIP_SAME_VALUE` | SF011, SF012 | Negar exactamente el valor positivo vigente de una ranura: ¿se invierte el filtro (lectura elegida) o se aclara la contradicción? |
| `TOPN_CAP_ABOVE_MAX` | SF021 | P8 es contradictoria: «máximo 100, anunciado» con ejemplo de 500 -> 100, y a la vez «por encima, aclarar». Falta además la clase de eco TOPN_CAPPED. |
| `ANTERIOR_PRIOR_VS_CLOCK` | SF038 | «el año anterior» puede anclarse al año previo (F15 -> CLARIFY) o al reloj (2025 -> EXECUTE). |
| `SELF_CONTAINED_RESTATEMENT_DURING_PENDING` | SF055 | Una reformulación completa durante una aclaración pendiente: ¿NEW_SEARCH o ANSWER_TO_CLARIFICATION? Misma IR, distinta disposición. |
| `PENDING_VALUE_NOT_IN_PRIOR` | SF059, SF060 | prior.pending no transporta el valor X en disputa (F12), así que la IR resultante de «Como comprador/lugar» no se puede escribir completa. |
| `ANY_OPTION_ANSWER` | SF061 | «cualquiera, el que sea» ante una aclaración de opciones: ¿OR de todas, quitar la dimensión o volver a preguntar? |
| `PENDING_ABANDONED_BY_DELTA` | SF063 | «no, mejor …» durante una aclaración pendiente: ¿delta sobre el ancla (lectura elegida) o búsqueda nueva? |
| `STATE_NOT_IN_TARGET_UNIVERSE` | SF079 | Un delta que cambia a CONTRATOS con un estado que sólo existe en procesos («abiertos»), sin ACTOR: §19 no lo cubre (sólo ACTOR×STATE). |
| `UNIT_INHERITANCE_IMPLAUSIBLE_SCALE` | SF104 | F5 hereda PESOS y produce «650 pesos»; no hay guarda de escala inverosímil. |
| `CLEAR_ABSENT_SLOT` | SF113 | Quitar un filtro que no existe: la IR no cambia; ¿ejecutar, avisar o aclarar? |
| `EMPTY_IR_AFTER_CLEAR` | SF114, SF116 | Un CLEAR que deja la búsqueda sin ninguna condición: ¿ejecutar todo SECOP o aclarar? |
| `NO_ANCHOR_FRAGMENT` | SF137 | Fragmento sin turno previo (owner NONE): la regla 2.2.7 manda HANDOFF_LEGACY, pero NONE no es legacy; la lectura elegida es CLARIFY. |
| `ORDER_DIRECTION_UNSTATED` | SF140 | «ordénalos por fecha» sin dirección: el contrato exige date_desc o date_asc. |
| `ANCHOR_UNREADABLE` | SF142 | Ancla Foundation EXECUTED con prior.ir null: no hay regla de disposición (se eligió CLARIFY ANCHOR_UNREADABLE con resulting_ir null). |

## Observaciones sobre los insumos (no son etiquetas)

- SF047–SF057, SF063, SF064: `prior.pending = PERIOD_WITHOUT_YEAR`, pero el periodo sin año original no viaja en `prior.ir`.
  La respuesta se etiqueta como el periodo que trae el follow-up.
- SF059/SF060: el valor X de la aclaración F12 no está en `prior` (ver `PENDING_VALUE_NOT_IN_PRIOR`).
- SF103: el ancla está EXECUTED con un VALUE en SMMLV, aunque F14 dice que SMMLV no se ejecuta hoy.
- La regla 2.2.7 (owner ≠ FOUNDATION_SEARCH -> HANDOFF_LEGACY) choca con owner `NONE` (SF137).
