# REPORT SA - slice sintetico de follow-ups (162 casos)

Etiquetador: SA (ciego). Fecha de referencia 2026-10-01. Archivos: `labels_v2.jsonl`, `self_check.json` (0 errores).

## Distribuciones

### turn_kind

| valor | n |
|---|---|
| DELTA | 128 |
| NEW_SEARCH | 13 |
| OTHER_OPERATION | 9 |
| ANSWER_TO_CLARIFICATION | 6 |
| PROTOCOL | 4 |
| UNCLEAR | 2 |

### m2a_scope

| valor | n |
|---|---|
| IN | 136 |
| OUT_RELATIVE_TO_PREVIOUS | 9 |
| OUT_OPERATION_CHANGE | 7 |
| OUT_REFERENT_EXTENSIONAL | 6 |
| OUT_ANCHOR_NOT_SEARCH | 4 |

### expected_disposition

| valor | n |
|---|---|
| EXECUTE | 115 |
| CLARIFY | 22 |
| NEW_SEARCH | 13 |
| HANDOFF_LEGACY | 11 |
| REASK | 1 |

### disposition + reason

| valor | n |
|---|---|
| EXECUTE / None | 115 |
| NEW_SEARCH / None | 13 |
| CLARIFY / RELATIVE_TO_PREVIOUS_VALUE | 9 |
| HANDOFF_LEGACY / REFERENT_EXTENSIONAL | 6 |
| CLARIFY / CONTEXT_EXPIRED_CONFIRM | 4 |
| CLARIFY / MONTH_YEAR_AMBIGUOUS | 2 |
| HANDOFF_LEGACY / OPERATION_SWITCH:FOLLOW | 2 |
| CLARIFY / ANCHOR_UNREADABLE | 1 |
| CLARIFY / NO_ANCHOR | 1 |
| CLARIFY / REGION_NOT_IN_LIST | 1 |
| CLARIFY / SMMLV_UNSUPPORTED | 1 |
| CLARIFY / STATE_NOT_IN_UNIVERSE | 1 |
| CLARIFY / TOPN_CAP_EXCEEDED | 1 |
| CLARIFY / UNIT_AMBIGUOUS | 1 |
| HANDOFF_LEGACY / ANCHOR_OWNER_LEGACY:COUNT | 1 |
| HANDOFF_LEGACY / ANCHOR_OWNER_LEGACY:RANK_SUPPLIERS | 1 |
| HANDOFF_LEGACY / OPERATION_SWITCH:SUM_VALUE | 1 |
| REASK / NON_SELECTION | 1 |

### ops por tipo

| valor | n |
|---|---|
| SET | 113 |
| SET_ORDER | 18 |
| CLEAR | 15 |
| SET_UNIVERSE | 10 |
| ANSWER | 5 |
| KEEP | 3 |

### ops por ranura (op:slot)

| valor | n |
|---|---|
| SET:TIME | 34 |
| SET:VALUE.lower | 25 |
| SET_ORDER:ORDER | 18 |
| SET:VALUE.upper | 14 |
| SET:GEOGRAPHY | 11 |
| SET_UNIVERSE:UNIVERSE | 10 |
| SET:BUYER | 7 |
| SET:CARDINALITY | 6 |
| SET:STATE | 6 |
| SET:TOPIC | 6 |
| CLEAR:TIME | 4 |
| SET:MODALITY | 4 |
| CLEAR:BUYER | 3 |
| ANSWER:BUYER | 2 |
| ANSWER:TIME | 2 |
| CLEAR:CARDINALITY | 2 |
| CLEAR:GEOGRAPHY | 2 |
| ANSWER:GEOGRAPHY | 1 |
| CLEAR:MODALITY | 1 |
| CLEAR:ORDER | 1 |
| CLEAR:STATE | 1 |
| CLEAR:TOPIC | 1 |
| KEEP:BUYER | 1 |
| KEEP:ORDER | 1 |
| KEEP:UNIVERSE | 1 |

### effects

| valor | n |
|---|---|
| UNIVERSE_CHANGED | 10 |
| VALUE_RANGE_REPLACED | 8 |
| TOPIC_REPLACED | 2 |
| VIEW_REDERIVED | 1 |

### expected_echo

| valor | n |
|---|---|
| CARRIED | 109 |
| (none) | 47 |
| VALUE_RANGE_REPLACED | 8 |
| UNIT_INHERITED | 6 |
| CONTEXT_EXPIRED | 4 |
| VIEW_DERIVED | 3 |
| YEAR_INHERITED | 3 |
| TOPIC_REPLACED | 2 |
| TOPN_CARRIED | 2 |

### ambiguity

| valor | n |
|---|---|
| CLEAR | 125 |
| AMBIGUOUS | 37 |

### confidence

| valor | n |
|---|---|
| HIGH | 124 |
| MEDIUM | 34 |
| LOW | 4 |

### gap_codes usados

| valor | n |
|---|---|
| UNIT | 10 |
| MONTHYEAR | 6 |
| SELECT | 6 |
| UNIVERSE_FRAGMENT | 6 |
| GEOBUYER | 5 |
| PV | 5 |
| REGION_VOCAB | 5 |
| TTL | 5 |
| B5RANGE | 4 |
| SET | 4 |
| COMPARATOR_FORM | 3 |
| OPSWITCH | 3 |
| TOPIC | 3 |
| COUNTERPART | 2 |
| SMMLV | 1 |
| TOTALES | 1 |

## Ids AMBIGUOUS (37) agrupados por la regla que falta

| Regla faltante | Ids | Lectura elegida / alternativa |
|---|---|---|
| Composicion de un SET de polaridad opuesta o distinto valor sobre una ranura ocupada (voltear, coexistir, limpiar) | SF011, SF012, SF162 | Mismo valor: volteo de polaridad (SF011, SF012). Valor distinto dentro del departamento (SF162): coexisten. Alt: conjuncion vacia / CLEAR / reemplazo |
| Representacion de un comparador de valor negado | SF013 | NEGATIVE greater_than (E2/D4). Alt: less_or_equal POSITIVE |
| Superlativo singular interrogativo: re-consulta vs pregunta sobre lo mostrado | SF017 | Re-consulta ORDER + top 1 (F4). Alt: solo responder sobre filas |
| Tope P8 de top-N: truncar y anunciar vs aclarar por encima de 100 | SF021 | CLARIFY TOPN_CAP_EXCEEDED. Alt: ejecutar con 100 anunciado |
| F15 sin cuantia o con referente vago ("un trimestre antes", "eso", "el doble") | SF040, SF045, SF046 | Mejor lectura aritmetica, CLARIFY igualmente |
| Respuesta con campo de pendiente mas ediciones adicionales: ANSWER vs DELTA, y NEW_SEARCH vs ANSWER cuando el texto repite la consulta pendiente | SF051, SF053, SF055 | DELTA (SF051, SF053), NEW_SEARCH (SF055); mismo IR resultante en SF055 |
| Valor del span pendiente no registrado en prior.ir (respuesta por boton "Como comprador/lugar") | SF059, SF060 | resulting_ir null; Alt: ACTOR + span como BUYER / GEOGRAPHY |
| Operacion nueva mientras hay una aclaracion pendiente (SUM_VALUE con comprador ambiguo) | SF062 | HANDOFF_LEGACY. Alt: REASK del comprador |
| "no, mejor X" completo: reinicio vs delta (se arrastran valor y universo no mencionados) | SF063, SF126, SF133 | NEW_SEARCH (SF063, SF126); SF133 tema desnudo = TOPIC_REPLACED. Alt: DELTA / NEW_SEARCH UNSPECIFIED |
| D7 "de <lugar>" = GEOGRAPHY dominante vs BUYER | SF068, SF069, SF139 | GEOGRAPHY MUNICIPALITY. Alt: BUYER Alcaldia de Cali/Palmira |
| Oportunidades con TIME arrastrado | SF075 | TIME 2025 se arrastra (F1). Alt: soltar el TIME |
| Estado solo-proceso con universo CONTRATOS explicito y sin ACTOR | SF079 | CLARIFY STATE_NOT_IN_UNIVERSE. Alt: ejecutar leyendo "abiertos" como activos |
| Convocatorias + "siguen recibiendo ofertas": estado redundante con la lente de oportunidad | SF084 | SET_UNIVERSE + STATE family open. Alt: solo la lente |
| VALUE_RANGE_REPLACED cuando el reemplazo por lados sustituye ambas cotas | SF098 | Con efecto (§2.3). Alt: sin efecto |
| F5 aplicado a un monto implausible (650 pesos tras 40.000.000 pesos) | SF104 | Hereda PESOS (F5). Alt: CLARIFY |
| CLEAR sobre ranura vacia / dejar la busqueda sin ninguna condicion | SF113, SF114, SF116 | EXECUTE con ancla sin cambio (SF113) o busqueda desnuda (SF114, SF116). Alt: CLARIFY/informar |
| "y para/en <lugar>" con un BUYER arrastrado de otro territorio (resultado vacio probable) | SF123, SF155 | Se conserva el BUYER (F1). Alt: CLEAR BUYER (cambio de territorio) |
| prior.state NOT_EXECUTED | SF135, SF136 | EXECUTE sobre el IR aceptado. Alt: REASK/CLARIFY |
| Sin ancla y fragmento solo de año | SF137 | CLARIFY NO_ANCHOR. Alt: NEW_SEARCH solo-año |
| Orden por fecha sin direccion | SF140 | date_desc por defecto. Alt: date_asc |
| "otros diez más": paginacion vs cardinalidad | SF147 | PROTOCOL. Alt: DELTA top_n |
| FOLLOW con universo cambiado por "procesos nuevos" | SF154 | FOLLOW sobre PROCESSES. Alt: universo CONTRACTS |

## NEW_GAP (no encajan en la lista cerrada de gap_codes)

1. **NEW_GAP m2a_scope para NEW_SEARCH y PROTOCOL:** el esquema no tiene valor para un turno que no es delta. Uso `IN` con `ops = []` para NEW_SEARCH (resulting_ir = IR nueva completa) y `OUT_OPERATION_CHANGE` para PROTOCOL (SF145-SF148; PROTOCOL es una operacion distinta, §2).
2. **NEW_GAP polaridad opuesta sobre ranura ocupada** (SF011, SF012, SF162): ver tabla; ademas SF013 para el comparador negado.
3. **NEW_GAP tope top-N** (SF021): el texto de C0 P8 admite dos lecturas (truncar y anunciar vs aclarar por encima).
4. **NEW_GAP estado solo-proceso con CONTRATOS explicito sin ACTOR** (SF079).
5. **NEW_GAP CLEAR sobre ranura vacia** (SF113).
6. **NEW_GAP prior.state NOT_EXECUTED** (SF135, SF136).
7. **NEW_GAP orden por fecha sin direccion** (SF140).
8. **NEW_GAP "no, mejor X" / tema desnudo**: reinicio vs delta (SF063, SF126, SF133).
9. **NEW_GAP clase de eco `UNIVERSE_CHANGED`** (ademas de VIEW_DERIVED): los casos con `effects: UNIVERSE_CHANGED` (SF073-SF080, SF084, SF131) lo llevan en `notes` como `ECHO_EXTRA`, no en `expected_echo`. Tambien se echo-anuncia la ventana relativa resuelta con el reloj ("este anio", "ultimos N meses", CLOCK_INFERRED) sin clase propia: no la puse en `expected_echo`.
10. **NEW_GAP pendiente sin valor** (SF059, SF060): `prior.pending` no guarda el span ambiguo; no se puede completar resulting_ir.
11. **NEW_GAP plausibilidad de unidad heredada** (SF104).

## Decisiones de lectura que conviene revisar

- `relative_period` para "este anio", "este mes", "ano en curso", "ultimos N meses" (listados bajo ese operador en el contrato); `year_source = CLOCK_INFERRED` en todas.
- F15 (SF037-SF046): `resulting_ir` se etiqueta con la aritmetica del continuar; null solo donde la cuantia no existe (SF039, SF043).
- F11: SF138 (1860 s) y SF064, SF139, SF140 CLARIFY CONTEXT_EXPIRED_CONFIRM; SF141 (1740 s) EXECUTE.
- Rango §2.3 con unidad compartida: la cota inferior lleva `SIBLING_IN_RANGE` y la superior `STATED` (SF087, SF098, SF107); en SF106 las dos cotas heredan PREVIOUS_ACCEPTED_VALUE.
- Los casos con owner LEGACY (SF143, SF144) cumplen la regla 7: OUT_ANCHOR_NOT_SEARCH + HANDOFF_LEGACY.
- Los fragmentos de seleccion extensional (SF065-067, SF070-072) y las operaciones nuevas (SF153, SF154) van a HANDOFF_LEGACY (contrato §6: follow-up fuera de la paridad M1).
