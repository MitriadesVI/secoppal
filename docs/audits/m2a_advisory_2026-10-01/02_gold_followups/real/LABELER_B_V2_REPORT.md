# LABELER_B_V2_REPORT — re-etiquetado v2 del Gold de follow-ups (etiquetador ciego B)

Esquema `m2a-followup-gold/2` · universo: 65 registros REAL `FOLLOWUP_DELTA` del Gold sellado (sha256 del archivo privado
verificado contra `REAL_SEAL_v1.json`: coincide) · 65 etiquetados · `self_check.json`: **PASS, 0 errores**.
Sin texto REAL en este informe: todo se cita por `gold_id` (el texto vive sólo en `labels_v2.jsonl`).

No leí `m2a_design/`, `hades/`, `labeler_A*/`, `comparison/` ni código/tests de follow-up del producto. Del worktree
`secoppal-m1-int-par` sólo leí `geo_regions.py` y las tablas de alias que importa (lista cerrada de regiones, F13).

## Convenciones propias (declaradas porque el kit no las fija)

1. **`prior.state` describe el previo como lo dejaría el producto ideal** (igual que `resulting_ir`), no lo observado en
   la traza: un fallo del host (SECOP no disponible, aclaración de legacy) no cambia el estado ideal. `EXPIRED` si
   `gap_seconds > 1800` (F11); `PENDING` si el previo ideal es una aclaración (F14 sobre R0302; F12 sobre R0320).
   `EXECUTED_EMPTY`/`NOT_EXECUTED` no se usan: no son conocibles en el ideal. Lo observado va en `notes`.
2. **`gap_seconds`**: primera ocurrencia del follow-up en `traces` (sólo lectura) menos el último turno anterior del mismo
   usuario (canal si es anónimo) con el texto del previo, redondeado al segundo. Con cadena de protocolo se mide desde el
   previo sustantivo (en R0223 la última paginación también queda a >1800 s).
3. **Selección del previo**: `DOMINANT_IN_REPLAY` en las baterías de mayo con decenas de réplicas (pareja base→follow-up a
   ≈1–20 ms en todas); `CHAIN_LABELED` cuando el previo es otro follow-up cuyo IR etiqueto yo; `IMMEDIATE_SUBSTANTIVE`
   en el resto (incluida la ocurrencia original orgánica de R0079, R0083 y R0099, cuyas réplicas emparejan con un
   registro de IR idéntico).
4. **IR**: forma de `prior.ir` del esquema, sin `quote`. `value_hint` de VALUE legible (cifra + escala) con `unit`
   (`MILLONES`, `SMMLV`); TIME de mes «<mes> de <año>»; rangos de años `AAAA-01-01..AAAA-12-31` (F8). Extensiones
   mínimas: `logic` sólo cuando el previo tiene OR (R0187) y `aspect` sólo en COUNT. `order = {"operator": …}`.
5. **`expected_echo`** se calcula igual para toda disposición (lo que anunciaría la ejecución de `resulting_ir`).
   `UNIT_INHERITED` incluye la unidad compartida de un rango (F6), por la cabecera «Herencia … siempre anunciada».
   Clases nuevas declaradas en `notes`: `UNIVERSE_CHANGED` (R0094) y `ACTOR_TIME_ON_SIGNATURE_DATE` (aviso §19 de
   ACTOR+TIME sin universo: R0319, R0320, R0321, R0441).
6. Ante una F12 (CLARIFY comprador/lugar) `resulting_ir` muestra la opción 1 (comprador) y `alternatives` la opción 2.
7. `disposition_reason` para `HANDOFF_LEGACY`: `ANCHOR_NOT_SEARCH`; para F12: `CON_X_BUYER_OR_PLACE` (la adenda no
   fija código).

## Distribuciones

### turn_kind

| valor | n |
|---|---|
| DELTA | 58 |
| OTHER_OPERATION | 4 |
| UNCLEAR | 2 |
| ANSWER_TO_CLARIFICATION | 1 |

### m2a_scope

| valor | n |
|---|---|
| IN | 58 |
| OUT_ANCHOR_NOT_SEARCH | 7 |

### expected_disposition (disposition_reason)

| valor | n |
|---|---|
| EXECUTE (None) | 50 |
| HANDOFF_LEGACY (ANCHOR_NOT_SEARCH) | 7 |
| CLARIFY (SMMLV_UNSUPPORTED) | 4 |
| CLARIFY (CONTEXT_EXPIRED_CONFIRM) | 2 |
| CLARIFY (CON_X_BUYER_OR_PLACE) | 2 |

### confidence

| valor | n |
|---|---|
| HIGH | 43 |
| MEDIUM | 18 |
| LOW | 4 |

### ambiguity

| valor | n |
|---|---|
| CLEAR | 49 |
| AMBIGUOUS | 16 |

### prior.selection_rule

| valor | n |
|---|---|
| IMMEDIATE_SUBSTANTIVE | 47 |
| CHAIN_LABELED | 10 |
| DOMINANT_IN_REPLAY | 8 |

### prior.owner

| valor | n |
|---|---|
| FOUNDATION_SEARCH | 58 |
| LEGACY:COUNT | 3 |
| LEGACY:PROFILE_ACTOR | 3 |
| LEGACY:RANK_SUPPLIERS | 1 |

### prior.state

| valor | n |
|---|---|
| EXECUTED | 59 |
| PENDING | 4 |
| EXPIRED | 2 |

### prior.chain

| valor | n |
|---|---|
| (vacía) | 60 |
| PROTOCOL:option | 4 |
| PROTOCOL:page,PROTOCOL:page | 1 |

### source_kind

| valor | n |
|---|---|
| ORGANIC | 45 |
| SCRIPTED | 20 |

### ops por ranura (op × slot)

| slot | SET | KEEP | CLEAR | SET_ORDER | SET_UNIVERSE | ANSWER | total |
|---|---|---|---|---|---|---|---|
| ACTOR | 1 | 0 | 0 | 0 | 0 | 0 | 1 |
| BUYER | 4 | 0 | 0 | 0 | 0 | 0 | 4 |
| GEOGRAPHY | 7 | 0 | 0 | 0 | 0 | 1 | 8 |
| MODALITY | 2 | 0 | 0 | 0 | 0 | 0 | 2 |
| ORDER | 0 | 0 | 0 | 7 | 0 | 0 | 7 |
| STATE | 1 | 1 | 0 | 0 | 0 | 0 | 2 |
| TIME | 20 | 2 | 0 | 0 | 0 | 0 | 22 |
| TOPIC | 2 | 0 | 0 | 0 | 0 | 0 | 2 |
| UNIVERSE | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| VALUE | 0 | 0 | 1 | 0 | 0 | 0 | 1 |
| VALUE.lower | 17 | 0 | 0 | 0 | 0 | 0 | 17 |
| VALUE.upper | 8 | 0 | 0 | 0 | 0 | 0 | 8 |

Total ops: 75; registros sin ops: ['R0483', 'R0560']

### operador por ranura

| valor | n |
|---|---|
| TIME.during_year | 14 |
| VALUE.lower.greater_than | 9 |
| GEOGRAPHY.is | 8 |
| VALUE.lower.greater_or_equal | 8 |
| ORDER.value_desc | 6 |
| TIME.between_dates | 5 |
| VALUE.upper.less_or_equal | 5 |
| BUYER.is | 4 |
| TIME.during_month | 3 |
| VALUE.upper.less_than | 3 |
| MODALITY.family | 2 |
| STATE.family | 2 |
| TOPIC.contains | 2 |
| ACTOR.is | 1 |
| ORDER.date_desc | 1 |
| UNIVERSE.None | 1 |
| VALUE.None | 1 |

### effects (registros)

| valor | n |
|---|---|
| VALUE_RANGE_REPLACED | 3 |
| VIEW_REDERIVED | 3 |
| TOPIC_REPLACED | 1 |
| UNIVERSE_CHANGED | 1 |

Ids por effect: TOPIC_REPLACED: ['R0286']; UNIVERSE_CHANGED: ['R0094']; VALUE_RANGE_REPLACED: ['R0187', 'R0205', 'R0531']; VIEW_REDERIVED: ['R0081', 'R0106', 'R0276']

### expected_echo (registros)

| valor | n |
|---|---|
| CARRIED | 64 |
| VIEW_DERIVED | 12 |
| UNIT_INHERITED | 7 |
| ACTOR_TIME_ON_SIGNATURE_DATE | 4 |
| YEAR_INHERITED | 4 |
| VALUE_RANGE_REPLACED | 3 |
| CONTEXT_EXPIRED | 2 |
| TOPIC_REPLACED | 1 |
| UNIVERSE_CHANGED | 1 |

Ids por clase de eco (sin CARRIED): ACTOR_TIME_ON_SIGNATURE_DATE: ['R0319', 'R0320', 'R0321', 'R0441']; CONTEXT_EXPIRED: ['R0223', 'R0286']; TOPIC_REPLACED: ['R0286']; UNIT_INHERITED: ['R0180', 'R0192', 'R0282', 'R0311', 'R0529', 'R0531', 'R0532']; UNIVERSE_CHANGED: ['R0094']; VALUE_RANGE_REPLACED: ['R0187', 'R0205', 'R0531']; VIEW_DERIVED: ['R0081', 'R0106', 'R0107', 'R0276', 'R0319', 'R0320', 'R0321', 'R0375', 'R0441', 'R0529', 'R0531', 'R0532']; YEAR_INHERITED: ['R0099', 'R0496', 'R0497', 'R0498']

### inherited.year_source (ops TIME)

| valor | n |
|---|---|
| STATED | 18 |
| PREVIOUS_ACCEPTED_TIME | 4 |

### inherited.unit_source (ops VALUE con cota)

| valor | n |
|---|---|
| STATED | 18 |
| SIBLING_IN_RANGE | 5 |
| PREVIOUS_ACCEPTED_VALUE | 2 |

Ids con año heredado:  ['R0099', 'R0496', 'R0497', 'R0498']
Ids con unidad heredada del previo (F5):  ['R0531', 'R0532']
Ids con unidad de rango (F6):  ['R0180', 'R0192', 'R0282', 'R0311', 'R0529']
Ids con refers_to_result_set:  ['R0079', 'R0081', 'R0083', 'R0087', 'R0089', 'R0099', 'R0192', 'R0223']
Ids con depends_on_prior_value:  ['R0099', 'R0496', 'R0497', 'R0498', 'R0531', 'R0532']

### prior.gap_seconds > 1800

- R0223: 16177 s → CLARIFY / CONTEXT_EXPIRED_CONFIRM
- R0286: 45148 s → CLARIFY / CONTEXT_EXPIRED_CONFIRM

Distribución de gap_seconds: 0 s (réplica/batería) = 20; 1–60 s = 32; 61–600 s = 11; 601–1800 s = 0; > 1800 s = 2.

### gap_codes por id

| gold_id | gap_codes | confidence |
|---|---|---|
| R0094 | UNIVERSE_FRAGMENT, UNIVTIME | MEDIUM |
| R0106 | SELECT | MEDIUM |
| R0187 | COMPARATOR_FORM | MEDIUM |
| R0189 | OPSWITCH | LOW |
| R0205 | ENADELANTE | MEDIUM |
| R0301 | SMMLV | MEDIUM |
| R0303 | SMMLV | MEDIUM |
| R0304 | SMMLV | MEDIUM |
| R0306 | SMMLV | MEDIUM |
| R0319 | OPSWITCH | MEDIUM |
| R0375 | B5RANGE, OPSWITCH | LOW |
| R0441 | OPSWITCH | MEDIUM |
| R0459 | UNIVERSE_FRAGMENT | MEDIUM |
| R0483 | FIRMADOS | LOW |
| R0560 | TOTALES | LOW |

### gap_codes (frecuencia)

| valor | n |
|---|---|
| OPSWITCH | 4 |
| SMMLV | 4 |
| UNIVERSE_FRAGMENT | 2 |
| B5RANGE | 1 |
| COMPARATOR_FORM | 1 |
| ENADELANTE | 1 |
| FIRMADOS | 1 |
| SELECT | 1 |
| TOTALES | 1 |
| UNIVTIME | 1 |

Registros AMBIGUOUS:  ['R0094', 'R0106', 'R0187', 'R0189', 'R0276', 'R0319', 'R0320', 'R0321', 'R0375', 'R0418', 'R0435', 'R0441', 'R0459', 'R0483', 'R0560', 'R0561']
Registros LOW:  ['R0189', 'R0375', 'R0483', 'R0560']

## Cambios v1 → v2 y regla que los motiva

Diferencias de `gap_seconds` de ±1 s en 22 registros: sólo redondeo (v1 truncaba); no se listan.

| gold_id | Cambio | Regla |
|---|---|---|
| R0306 | Previo R0305 → **R0302** (la pareja de la batería a ≈20 ms es la base con año); `resulting_ir` gana TIME 2026 | trazas re-leídas a nivel de microsegundo |
| R0301, R0303, R0304, R0306 | EXECUTE → **CLARIFY / SMMLV_UNSUPPORTED**; R0303/R0304/R0306 con `prior.state = PENDING` (el previo ya llevaba SMMLV) | F14 (provisional) → `gap_codes: SMMLV` |
| R0223, R0286 | EXECUTE → **CLARIFY / CONTEXT_EXPIRED_CONFIRM**, `prior.state = EXPIRED`, eco `CONTEXT_EXPIRED`; confianza LOW → HIGH | F11 |
| R0286 | `effects/echo: TOPIC_REPLACED` (v1 ya reemplazaba) | F3 |
| R0320, R0418 | EXECUTE (BUYER) → **CLARIFY** comprador/lugar; confianza LOW → HIGH | F12 |
| R0321 | DELTA → **ANSWER_TO_CLARIFICATION** (op `ANSWER` GEOGRAPHY sobre el previo pendiente de F12); level UNKNOWN → DEPARTMENT | F12 + D7 |
| R0189 | DELTA/EXECUTE → **OTHER_OPERATION / HANDOFF_LEGACY**, `OUT_ANCHOR_NOT_SEARCH`; STATE pasa a `KEEP` (mismo valor) | §2.2.7, §2.2.1, `OPSWITCH` |
| R0319, R0375, R0441 | DELTA/EXECUTE → **OTHER_OPERATION / HANDOFF_LEGACY**; universo CONTRACTS → **UNSPECIFIED con `product_view` CONTRACTS** | §2.2.7; contrato §19 (ACTOR+TIME) y PV3; `OPSWITCH` (+`B5RANGE` en R0375) |
| R0483 | CLARIFY con IR nula → **HANDOFF_LEGACY** con `resulting_ir` = ranking sin cambios (C4/Y1) | §2.2.5/§2.2.7, `FIRMADOS` |
| R0560 | ANSWER_TO_CLARIFICATION/EXECUTE → **UNCLEAR / HANDOFF_LEGACY** (el previo ideal R0559 ejecuta; la aclaración fue de legacy) | §2.2.7, convención 1, `TOTALES` |
| R0561 | EXECUTE → **HANDOFF_LEGACY** (ancla COUNT) | §2.2.7 |
| R0081, R0106, R0276 | El cambio de vista PV6→PV1/PV5 deja de ser `contract_gap`: `effects: VIEW_REDERIVED`, eco `VIEW_DERIVED` | F9 |
| R0107, R0529, R0531, R0532 (+R0319…R0441 por ACTOR) | eco `VIEW_DERIVED` (vista derivada sin cambio de valor) | F9 / §5 |
| R0187, R0205, R0531 | Reemplazo de la restricción entera ahora por regla, con `effects/echo: VALUE_RANGE_REPLACED` | F2 |
| R0310, R0464 | La cota opuesta completa el rango (igual que v1) pero deja de ser gap; confianza LOW → HIGH | F2 |
| R0180, R0192, R0282, R0311, R0529 | Rango = dos `SET` con la misma cita; cota sin unidad `SIBLING_IN_RANGE`; eco `UNIT_INHERITED` | §2.3, F6 |
| R0531, R0532 | Unidad heredada `PREVIOUS_ACCEPTED_VALUE`, `depends_on_prior_value`; eco `UNIT_INHERITED`; confianza → HIGH | F5 |
| R0496, R0497, R0498 | `value_hint` de mes con año explícito, `PREVIOUS_ACCEPTED_TIME`; eco `YEAR_INHERITED` | F7, §2.4 |
| R0099 | El deíctico de año pasa de REPLACE no-op a `KEEP` con `depends_on_prior_value`; eco `YEAR_INHERITED`; sin gap | §2.1, §2.2.1 |
| R0336 | El año repetido pasa de REPLACE no-op a `KEEP` (valor dicho) | §2.2.1 |
| R0202 | Dos REMOVE por cota → **un `CLEAR` VALUE**; GEOGRAPHY se conserva | §2.3, F1 |
| R0303, R0314, R0432, R0450 | `between_dates` con el año final completo; deja de ser gap; R0314/R0432/R0450 → HIGH | F8 |
| R0274, R0281 | Región validada contra la lista cerrada del producto (ambas están); R0281 deja de ser gap | F13 |
| R0079, R0081, R0083, R0087, R0089, R0099, R0192, R0223 | `refers_to_result_set = true`; re-consulta, no filtro de lo mostrado | F4 |
| R0094 | Igual IR; gap reetiquetado `UNIVERSE_FRAGMENT` + `UNIVTIME`; `effects: UNIVERSE_CHANGED` | F1 + lista §4 |
| R0106 | Igual IR; ambigüedad del ancla (¿respuesta a la aclaración previa?) como `SELECT` | lista §4 |
| R0187 | Igual IR; gap `COMPARATOR_FORM` | adenda «sin decidir» |
| R0205 | Igual IR; gap `ENADELANTE`; confianza HIGH → MEDIUM | lista §4 |
| R0450 | `prior.gold_id` = null (el previo es una ocurrencia de texto igual a R0431 en otra conversación); IR igual | convención 3 |

Sin cambios de significado en los demás registros (sólo reexpresión al esquema v2: ops `SET`/`SET_ORDER`, `value_hint`
completo, procedencias, `CARRIED`).

## Riesgos y dudas declaradas

- Los significados de `SELECT`, `UNIVTIME`, `UNIVERSE_FRAGMENT` y `ENADELANTE` los inferí del nombre (el kit no trae
  definiciones); cada uso lleva su explicación en `notes`.
- F14 es un default provisional: los 4 registros SMMLV dependen de él.
- `expected_echo` con `UNIT_INHERITED` en rangos (F6) es una lectura literal de la cabecera de la adenda; si el diseño
  sólo exige anunciar herencias entre turnos, sobra en R0180, R0192, R0282, R0311 y R0529.
