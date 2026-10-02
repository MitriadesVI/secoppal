# REPORT SA v2.1 - slice sintetico de follow-ups (191 casos)

Etiquetador SA (ciego). Fecha de referencia 2026-10-01. Esquema `m2a-followup-gold/2.1` con A1-A7 (prevalecen) y F1-F30.
`labels_v2_1.jsonl` (191 registros), `self_check.json` (0 errores). `effects` y `expected_echo` van vacios: son derivados, no se etiquetan.
Convenciones propias: rangos de fechas y relativos con fechas absolutas segun §3 (la ventana relativa termina en hoy, 2026-10-01, como en los `prior`); `during_month` en la IR como `AAAA-MM`; las IR alternativas van en `alternatives[]` y la alternativa de disposicion (sin IR) en `notes`.

## Distribuciones

### turn_kind

| valor | n |
|---|---|
| DELTA | 144 |
| NEW_SEARCH | 17 |
| OTHER_OPERATION | 11 |
| ANSWER_TO_CLARIFICATION | 10 |
| PROTOCOL | 9 |

### m2a_scope

| valor | n |
|---|---|
| IN | 139 |
| IN_NEW_SEARCH | 17 |
| IN_PROTOCOL | 12 |
| OUT_RELATIVE_TO_PREVIOUS | 8 |
| OUT_REFERENT_EXTENSIONAL | 7 |
| OUT_OPERATION_CHANGE | 4 |
| OUT_ANCHOR_NOT_SEARCH | 2 |
| OUT_NO_ANCHOR | 2 |

### expected_disposition

| valor | n |
|---|---|
| EXECUTE | 128 |
| CLARIFY | 27 |
| NEW_SEARCH | 17 |
| INFORM | 13 |
| REASK | 4 |
| HANDOFF_LEGACY | 2 |

### disposicion / razon

| valor | n |
|---|---|
| EXECUTE / None | 128 |
| NEW_SEARCH / None | 17 |
| CLARIFY / RELATIVE_TO_PREVIOUS_VALUE | 8 |
| INFORM / REFERENT_EXTENSIONAL | 8 |
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

### ops por op:ranura

| valor | n |
|---|---|
| SET:TIME | 39 |
| SET:VALUE.lower | 27 |
| SET:VALUE.upper | 18 |
| SET_ORDER:ORDER | 18 |
| SET_UNIVERSE:UNIVERSE | 12 |
| SET:GEOGRAPHY | 11 |
| SET:BUYER | 9 |
| SET:CARDINALITY | 6 |
| SET:STATE | 6 |
| SET:TOPIC | 6 |
| ANSWER:TIME | 5 |
| ANSWER:BUYER | 4 |
| SET:MODALITY | 4 |
| CLEAR:TIME | 3 |
| SET:CONTRACT_TYPE_OR_NATURE | 3 |
| CLEAR:BUYER | 2 |
| CLEAR:CARDINALITY | 2 |
| CLEAR:GEOGRAPHY | 2 |
| ANSWER:GEOGRAPHY | 1 |
| CANCEL_PENDING:None | 1 |
| CLEAR:MODALITY | 1 |
| CLEAR:ORDER | 1 |
| CLEAR:STATE | 1 |
| CLEAR:TOPIC | 1 |
| CLEAR:VALUE | 1 |
| KEEP:BUYER | 1 |
| KEEP:ORDER | 1 |

### ambiguity

| valor | n |
|---|---|
| CLEAR | 160 |
| AMBIGUOUS | 31 |

### confidence

| valor | n |
|---|---|
| HIGH | 154 |
| MEDIUM | 35 |
| LOW | 2 |

### gap_codes

| valor | n |
|---|---|
| UNIVERSE_FRAGMENT | 17 |
| UNIT | 13 |
| RANGE_NO_INTERIOR | 9 |
| RELATIVE_ANCHOR | 9 |
| GEOBUYER | 7 |
| PRIOR_NOT_EXECUTED | 6 |
| SMMLV | 6 |
| MONTHYEAR | 5 |
| REGION_VOCAB | 5 |
| TTL | 5 |
| B5RANGE | 4 |
| COMPARATOR_FORM | 4 |
| COUNTERPART | 4 |
| OPSWITCH | 3 |
| ORDER_DIRECTION | 3 |
| POLARITY_SAME_VALUE | 3 |
| PV | 3 |
| TOPIC | 3 |
| ANSWER_PLUS_EDIT | 2 |
| EMPTY_AFTER_DELTA | 2 |
| PENDING_REJECTED | 2 |
| SET | 2 |
| TOPN_CAP | 2 |
| ANY_OPTION | 1 |
| CLEAR_ABSENT | 1 |
| NEGATED_COMPARATOR | 1 |
| NO_ANCHOR | 1 |
| OPPORTUNITY_STATE | 1 |
| PAGE_VS_TOPN | 1 |
| PENDING_OPSWITCH | 1 |
| POLARITY_OTHER_VALUE | 1 |
| STATE_UNIVERSE | 1 |
| TOTALES | 1 |
| UNIVTIME | 1 |

## Cambios respecto de mi ronda anterior (disposicion, `resulting_ir` nulo o tipo de turno)

| Regla | Ids | Cambio |
|---|---|---|
| A3 / F20 | SF021 | CLARIFY TOPN_CAP_EXCEEDED -> EXECUTE con n=100, n_requested=250 |
| A5 | SF037, SF038, SF042, SF045 | CLARIFY (F15) -> EXECUTE, SET TIME relativo al unico referente, `m2a_scope = IN`; SF038 y SF045 AMBIGUOUS |
| A5 (nuevos) | SF173, SF174 EXECUTE; SF175, SF176, SF177 CLARIFY | referente unico / sin referente / rango / ventana relativa |
| F15 + O14 | SF039, SF040, SF043, SF044, SF046 | `resulting_ir` pasa a null; ops con `unresolved`, sin aritmetica |
| A2 | SF103 (texto cambiado), SF165-SF170 | SMMLV ejecutable con un unico ano; SF168 CLARIFY SMMLV_YEAR_AMBIGUOUS (TIME plurianual); SF169 sin TIME: AMBIGUOUS |
| A1 | SF090, SF091, SF094, SF095, SF099, SF103, SF182, SF183 | sin interior: AMBIGUOUS + `RANGE_NO_INTERIOR`; mejor lectura = la cota nueva sustituye; IR alternativa = composicion literal por lados. SF098 deja de ser AMBIGUOUS; SF104 deja de ser AMBIGUOUS (sin clausula de magnitud) |
| A7 / F25 | SF051, SF053 | EXECUTE -> REASK ANSWER_EDITS_OTHER_SLOT (turno ANSWER_TO_CLARIFICATION) |
| A7 | SF055 | deja de ser AMBIGUOUS: consulta completa que cancela la pendiente = NEW_SEARCH |
| F24 | SF061 | NON_SELECTION -> REASK OPTION_NOT_CHOSEN, `resulting_ir` null |
| Pendiente con span (v2.3) | SF058-SF060, SF188-SF191 | respuestas por evento (O11, `IN_PROTOCOL`); SF059/SF060 ya tienen `resulting_ir` (antes null) |
| §5.2 fila 4 | SF062, SF153, SF154 | HANDOFF_LEGACY -> INFORM OPERATION_CHANGE:*, `resulting_ir` null |
| §5.2 fila 5 | SF065-SF067, SF070-SF072 | HANDOFF_LEGACY -> INFORM REFERENT_EXTENSIONAL, null |
| F27 | SF079 | queda CLARIFY STATE_NOT_IN_UNIVERSE, null (ya no AMBIGUOUS) |
| F18 | SF113 | EXECUTE -> INFORM CLEAR_ON_EMPTY_SLOT |
| F19 | SF114, SF116 | EXECUTE -> CLARIFY EMPTY_SEARCH_AFTER_DELTA (AMBIGUOUS por X9) |
| F30 | SF123, SF155 | EXECUTE -> CLARIFY TERRITORY_VS_CARRIED_BUYER, null, dos alternatives |
| F28 / UNREADABLE | SF137, SF142 | OUT_NO_ANCHOR, `resulting_ir` null; turn_kind DELTA (decide el texto) |
| §2.1 legacy | SF143, SF144 | razon renombrada a ANCHOR_NOT_SEARCH:<op> |
| §5.2 fila 3 | SF149-SF152 | DELTA -> PROTOCOL, `IN_PROTOCOL`, ops [] |
| §5.2 fila 3 (dual) | SF148 | EXECUTE -> CLARIFY (ancla BOTH) con razon provisional (ver huecos) |
| F16 | SF011, SF012 | dejan de ser AMBIGUOUS (inversion cerrada) |
| F21 | SF140 | deja de ser AMBIGUOUS (por fecha sin direccion = date_desc DEFAULTED) |
| A6 / F29 | SF135, SF136 | siguen EXECUTE pero ahora por A6 (previo aceptado con fuente fallida) |
| Forma 2.1 | todos | IR con `aspect`, `logic`, `host_notice`, `BOTH`, `order{direction_source}`, `cardinality{n,n_requested}`; `unit` MILES para «900 mil pesos» (SF105) |

## AMBIGUOUS (31) por regla faltante

| Regla / tension | Ids | Lectura elegida y alternativa |
|---|---|---|
| A1 rango sin interior (`RANGE_NO_INTERIOR`) | SF090, SF091, SF094, SF095, SF099, SF103, SF182, SF183 | la cota explicita nueva sustituye; alt: ambas cotas conservadas (intervalo vacio) |
| Representacion del comparador negado (`NEGATED_COMPARATOR`) | SF013 | NEGATIVE greater_than en VALUE.upper; alt: less_or_equal |
| A5 con lectura posible contra el reloj (`RELATIVE_ANCHOR`) | SF038, SF173, SF045 | referente unico; alt: reloj (2025) o trimestre calendario |
| F15: referente vago de un relativo no temporal | SF046 | ambos lados no resueltos; alt: solo el superior |
| A4 frontera de autosuficiencia (`UNIVERSE_FRAGMENT`) | SF053, SF063, SF126, SF185, SF187 | NEW_SEARCH (SF063, SF126, SF185, SF187) o delta/REASK (SF053); alt: delta sobre el marco |
| Operacion nueva con pendiente abierta (`PENDING_OPSWITCH`) | SF062 | INFORM OPERATION_CHANGE; alt: REASK PENDING_UNANSWERED |
| X15 contexto vencido con pendiente | SF064 | F11 gana; alt: REASK |
| X12 territorio contenido en el comprador (`GEOBUYER`) | SF068, SF069, SF139 | EXECUTE (SF139 bajo F11); alt: comprador Alcaldia / CLARIFY F30 |
| X7 negar un valor contenido en el vigente | SF162 | F17 (suma exclusion); alt: inversion F16 |
| `OPPORTUNITY_STATE` | SF084 | estado y lente; alt: solo lente |
| X9 que cuenta como «sin condicion» | SF114, SF116 | CLARIFY; alt: ejecutar la busqueda desnuda |
| SMMLV sin TIME (`SMMLV`) | SF169 | CLARIFY SMMLV_YEAR_AMBIGUOUS; alt: ejecutar con salario del ano en curso |
| `PAGE_VS_TOPN` | SF147 | pagina; alt: cardinalidad |
| Ancla BOTH al paginar | SF148 | CLARIFY; alt: paginar ambos |
| Superlativo singular interrogativo (sin gap) | SF017 | re-consulta orden + top 1; alt: pregunta sobre las filas |

## Donde el esquema o las reglas no alcanzaron

1. `disposition_reason` para paginar con ancla dual (§5.2 fila 3 pide CLARIFY pero no hay codigo): uso `UNRESOLVED_VALUE:UNIVERSE` (SF148).
2. `INFORM` por falta de pagina tras fuente fallida (A6, SF179): no hay codigo; uso `REFERENT_EXTENSIONAL`. `resulting_ir` = previo (fila 3) aunque la disposicion sea INFORM.
3. Cuando una respuesta compatible edita otra ranura y el edit tambien crearia un rango sin interior (SF051): declaro ambos gaps.
4. Rango de unidades mezcladas (SF170: >=200 millones, <=900 SMMLV): A2 no lo cubre; lo ejecuto con un unico ano.
5. Ventana relativa «este mes» con fin en hoy (2026-10-01..2026-10-01): convencion mia, el esquema no dice si termina en hoy o en fin de mes.
6. Mejor lectura de A1: A1 conserva el lado opuesto, pero eso produce un intervalo vacio; elegi la intencion del usuario (la cota explicita sustituye) y deje la composicion literal como alternativa.
7. `unresolved` en CLARIFY causado por estado arrastrado (SF168, SF169) o por un F30/F12 con dos opciones: la operacion que se cita lleva `unresolved = true` aunque la causa este en el previo.
8. SF163: la operacion «con Soacha» se etiqueta con ranura BUYER (primera opcion) sin que el esquema diga cual ranura usar cuando son dos.
9. SF146 y SF145 (paginacion): igual al previo, EXECUTE; sin `observed` del producto.
