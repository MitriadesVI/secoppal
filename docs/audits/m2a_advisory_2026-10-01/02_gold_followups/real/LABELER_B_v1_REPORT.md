# LABELER_B_REPORT — Gold de follow-ups (etiquetador ciego B)

Registros FOLLOWUP_DELTA REAL en el Gold sellado: 65 (verificado: coincide). Etiquetados: 65.

Sin texto REAL literal en este informe: se cita por `gold_id`. El texto vive sólo en `labels.jsonl`.


## turn_kind

| valor | n |
|---|---|
| DELTA | 63 |
| ANSWER_TO_CLARIFICATION | 1 |
| UNCLEAR | 1 |

## expected_disposition

| valor | n |
|---|---|
| EXECUTE | 64 |
| CLARIFY | 1 |

## confidence

| valor | n |
|---|---|
| HIGH | 34 |
| MEDIUM | 20 |
| LOW | 11 |

## prior_status

| valor | n |
|---|---|
| SEALED_GOLD | 55 |
| LABELED_BY_ME | 10 |

UNAVAILABLE: 0. Todos los previos se recuperaron del corpus (traces de producción, mismo user_id o batería scripted); los de baja certeza van con `confidence: LOW` y `prior_gap_seconds` (hueco temporal).

prior_gap_seconds > 600: ['R0223', 'R0286']

Origen del contexto: organic=45, scripted (batería de pruebas)=20.

## dimensión x delta_op (elementos de delta; un rango = 2 elementos)

| dimensión | ADD | REPLACE | REMOVE | total |
|---|---|---|---|---|
| TIME | 11 | 11 | 0 | 22 |
| GEOGRAPHY | 4 | 4 | 0 | 8 |
| VALUE | 13 | 12 | 2 | 27 |
| ORDER | 7 | 0 | 0 | 7 |
| CARDINALITY | 0 | 0 | 0 | 0 |
| STATE | 1 | 1 | 0 | 2 |
| MODALITY | 2 | 0 | 0 | 2 |
| CONTRACT_TYPE_OR_NATURE | 0 | 0 | 0 | 0 |
| BUYER | 4 | 0 | 0 | 4 |
| ACTOR | 0 | 1 | 0 | 1 |
| TOPIC | 1 | 1 | 0 | 2 |
| UNIVERSE | 0 | 1 | 0 | 1 |
| OTHER | 0 | 2 | 0 | 2 |

## registros que tocan cada dimensión (registro contado una vez por dimensión)

| valor | n |
|---|---|
| TIME | 22 |
| VALUE | 20 |
| GEOGRAPHY | 8 |
| ORDER | 7 |
| BUYER | 4 |
| MODALITY | 2 |
| OTHER | 2 |
| STATE | 2 |
| TOPIC | 2 |
| ACTOR | 1 |
| UNIVERSE | 1 |

## cambios combinados (>=2 dimensiones distintas en un mismo follow-up): 7

- R0079: ['ORDER', 'TIME']
- R0094: ['STATE', 'UNIVERSE']
- R0099: ['ORDER', 'TIME']
- R0189: ['OTHER', 'STATE']
- R0202: ['BUYER', 'VALUE']
- R0336: ['TIME', 'VALUE']
- R0441: ['BUYER', 'TIME']

## needs_referent

- registros con algún delta needs_referent=true (valor tomado del turno previo: año/unidad): 6 -> ['R0099', 'R0496', 'R0497', 'R0498', 'R0531', 'R0532']
- registros con anáfora textual (determinante con núcleo elidido, «aquellos», «ese año», sujeto elidido): 12 -> ['R0079', 'R0081', 'R0083', 'R0087', 'R0089', 'R0099', 'R0192', 'R0223', 'R0319', 'R0418', 'R0441', 'R0515']
- cadena de aclaración/selección entre previo y follow-up («opción N»): ['R0418', 'R0431', 'R0435', 'R0441']
- cambio de operación respecto del previo (operation_change): ['R0189', 'R0319', 'R0375', 'R0441', 'R0560']
- cambia la Product View calculada por §5 al aplicar el delta (prior UNSPECIFIED): 3 -> [('R0081', 'CURR->HIST'), ('R0106', 'CURR->HIST'), ('R0276', 'CURR->HIST')]

## Cobertura de las clases pedidas (existen en los 65)

| clase | registros | ids |
|---|---|---|
| tiempo | 22 | R0079, R0092, R0099, R0106, R0197, R0276, R0303, R0304, R0312, R0313, R0314, R0319, R0336, R0409, R0431, R0432, R0441, R0450, R0496, R0497, R0498, R0556 |
| geografía | 8 | R0075, R0093, R0201, R0233, R0274, R0281, R0306, R0321 |
| valor | 20 | R0107, R0180, R0187, R0192, R0202, R0205, R0223, R0282, R0295, R0301, R0308, R0309, R0310, R0311, R0336, R0375, R0464, R0529, R0531, R0532 |
| orden | 7 | R0079, R0081, R0083, R0087, R0089, R0099, R0515 |
| estado | 2 | R0094, R0189 |
| universo (cambia procesos/contratos/vista) | 1 | R0094 |
| cambios combinados | 7 | R0079, R0094, R0099, R0189, R0202, R0336, R0441 |

Clases del contrato SIN casos en los 65: CARDINALITY (ningún follow-up trae cifra «los 10…»), CONTRACT_TYPE_OR_NATURE, polaridad NEGATIVE (ninguna negación), delta_op REMOVE sólo en R0202 (VALUE), ORDER con date_asc/value_asc, TIME relative_period, referentes de fila («el segundo», «ese contrato»), follow-ups de selección sobre resultados (todo es refinamiento de filtros). Ningún caso se ha fabricado.

### operadores usados en deltas

| dimensión.operador | n |
|---|---|
| ACTOR.is | 1 |
| BUYER.is | 4 |
| GEOGRAPHY.is | 8 |
| MODALITY.family | 2 |
| ORDER.date_desc | 1 |
| ORDER.value_desc | 6 |
| OTHER.aspect_is | 1 |
| OTHER.operation_is | 1 |
| STATE.family | 2 |
| TIME.between_dates | 5 |
| TIME.during_month | 3 |
| TIME.during_year | 14 |
| TOPIC.contains | 2 |
| UNIVERSE.subject_is | 1 |
| VALUE.greater_or_equal | 9 |
| VALUE.greater_than | 9 |
| VALUE.less_or_equal | 6 |
| VALUE.less_than | 3 |

## contract_gap agrupados (27 textos distintos en 30 registros)

- (2) R0081, R0106 :: Prior UNSPECIFIED: el delta (TIME/ORDER por valor) cambia lo que la Product View Policy §5 calcularía (PV6 oportunidades vigentes -> PV1/PV5 históricos). El contrato no dice si en un follow-up la vista se recalcula sobre el IR resultante o se conserva la del turno previo (¿se anuncia el cambio?).
- (2) R0303, R0314 :: Rango de años «de A a B»: inclusividad del año final no fijada.
- (2) R0432, R0450 :: Rango de años «entre A y B»: inclusividad del año final no fijada.
- (1) R0094 :: Fragmento con sustantivo de universo («procesos abiertos») tras una búsqueda: ¿delta que conserva tema/geografía/tiempo, o búsqueda nueva? El contrato no da criterio.
- (1) R0099 :: Referente deíctico «ese año»: el contrato no define si se resuelve por el IR previo (TIME) o por el conjunto de resultados.
- (1) R0187 :: Comparador mal formado («de mas 500 millones»): el contrato no dice si un comparador desordenado se proyecta o aclara.
- (1) R0189 :: Cambio de operación COUNT->SEARCH en un follow-up («alguno…?»): el contrato no dice cuándo el follow-up hereda operación y cuándo la cambia ni cómo se ancla el universo del previo no-SEARCH.
- (1) R0202 :: Cuando se añade un BUYER cuya sede está dentro de la GEOGRAPHY previa: ¿se conserva la geografía previa o la subsume el comprador?
- (1) R0223 :: Vigencia del turno previo: ¿tras cuánto tiempo/inactividad un fragmento deja de ser follow-up y pasa a ser CLARIFY?
- (1) R0276 :: Prior UNSPECIFIED: el delta (TIME/ORDER por valor) cambia lo que la Product View Policy §5 calcularía (PV6 oportunidades vigentes -> PV1/PV5 históricos). El contrato no dice si en un follow-up la vista se recalcula sobre el IR resultante o se conserva la del turno previo (¿se anuncia el cambio?). Aquí además TIME convive con STATE open.
- (1) R0281 :: «Costa Atlántica» no es una región administrativa DIVIPOLA (¿REGION == Caribe?). El contrato no fija el vocabulario de REGION.
- (1) R0286 :: Vigencia del turno previo (ver R0223); y: ¿«sobre X» reemplaza o añade TOPIC?
- (1) R0301 :: Unidad VALUE en SMMLV: el contrato no fija año de conversión ni si SMMLV es VALUE ejecutable o UNSUPPORTED.
- (1) R0310 :: Cota VALUE de dirección opuesta a la previa en un follow-up: ¿ADD (rango) o REPLACE? El contrato solo trata «entre X y Y» dentro de un mismo turno.
- (1) R0319 :: Follow-up que añade un filtro de registros a un PROFILE_ACTOR: ¿se convierte en SEARCH·CONTRACTS con ACTOR? (§6 lo dice para búsquedas, no para follow-ups de perfil).
- (1) R0320 :: Preposición «con» + lugar tras un ACTOR: ¿BUYER (contraparte) o GEOGRAPHY? D7 solo cubre «de <lugar>».
- (1) R0321 :: Reformulación del mismo referente con otra preposición: el contrato no dice si reemplaza la condición de otra dimensión.
- (1) R0375 :: B5 (monto sin comparador) vs cota inferior elidida en un rango («de A y menos de B»); y VALUE sobre un PROFILE_ACTOR previo (ver R0319).
- (1) R0418 :: Preposición «con» + lugar tras un ACTOR (ver R0320).
- (1) R0435 :: Turno que reproduce la etiqueta de un candidato tras una aclaración (ANSWER_TO_CLARIFICATION vs reemplazo de ACTOR) con prefijo «y ahora:».
- (1) R0441 :: Follow-up que añade filtros de registros a un PROFILE_ACTOR (ver R0319).
- (1) R0459 :: Fragmento que es una modalidad en singular («alguna selección abreviada?»): ¿filtro del previo o búsqueda nueva?
- (1) R0464 :: Cota VALUE de dirección opuesta en follow-up: ADD vs REPLACE (ver R0310).
- (1) R0483 :: Palabra de estado sola tras un RANK_SUPPLIERS: ¿delta STATE, no-op por Y1, o aclaración?
- (1) R0531 :: Unidad VALUE omitida en un follow-up: ¿se hereda del previo o es VALUE sin unidad -> CLARIFY? (B5 solo cubre falta de comparador).
- (1) R0532 :: Unidad VALUE omitida en un follow-up (ver R0531).
- (1) R0560 :: Respuesta de una palabra a una aclaración de operación analítica: el contrato no define turn_kind ni su IR.

## Decisiones de etiquetado a revisar (resumen)

- LOW: ['R0189', 'R0223', 'R0286', 'R0310', 'R0320', 'R0321', 'R0375', 'R0418', 'R0464', 'R0483', 'R0560']
- Prior de baterías scripted: elegido por el orden de la batería y adyacencia de ids del Gold (los turnos de una batería comparten milisegundo y se entrelazan).
- R0106/R0107: el Gold sella «pavimentacion» como UNSPECIFIED; si se leyera como respuesta a la aclaración de «contratos» el universo sería CONTRACTS (ver ambiguity_note).
- Reglas internas de etiquetado usadas donde el contrato calla: cota VALUE del mismo sentido = REPLACE; de sentido opuesto = ADD si el rango resultante no es vacío (R0310, R0464), REPLACE si lo sería (R0531); lugar introducido por «en» = GEOGRAPHY; por «con» tras un ACTOR = BUYER (contraparte, LOW).
