# Esquema `m2a-followup-gold/2.1` — extracto para etiquetadores

Kit de etiquetado de follow-ups, versión 2.1. Contiene el esquema, las definiciones, las reglas y las listas cerradas.
Se usa junto con el contrato M1_0 v1.4 (y enmiendas v1.5–v1.9) y la adenda `CONTRACT_ADDENDUM_FOLLOWUPS_v2.md` (F1–F30).
No contiene la etiqueta de ningún caso. Si una regla no alcanza, usa el `gap_code`, etiqueta tu mejor lectura y explica
la alternativa en `notes`; no inventes campos, valores ni códigos fuera de las listas cerradas.

## 1. Registro

Un registro JSON por línea. Los campos marcados **(derivado)** NO los escribe el etiquetador: los calcula el validador
con las reglas de §6 y §7; un etiquetador que crea que falta un anuncio lo dice en `notes`.

```json
{
  "schema": "m2a-followup-gold/2.1",
  "gold_id": "…",
  "slice": "REAL_FOLLOWUP | SYNTHETIC_FOLLOWUP",
  "followup_text": "…",                         // REAL: fuera de git
  "followup_sha256": "…",
  "source_kind": "ORGANIC | SCRIPTED | SYNTHETIC",
  "input_kind": "TEXT | EVENT",                 // EVENT: el turno es la pulsación de una opción o de un botón

  "prior": {
    "gold_id": "… | null",
    "selection_rule": "IMMEDIATE_SUBSTANTIVE | DOMINANT_IN_REPLAY | CHAIN_LABELED | SYNTHETIC_AUTHORED",
    "chain": ["PROTOCOL:option", "PROTOCOL:page", "PROTOCOL:view", "PROTOCOL:reask"],
    "gap_seconds": 22,
    "owner": "FOUNDATION_SEARCH | LEGACY:PROFILE_ACTOR | LEGACY:PROFILE_BUYER | LEGACY:COUNT | LEGACY:SUM_VALUE | LEGACY:GROUP_BY | LEGACY:RANK_SUPPLIERS | LEGACY:FOLLOW | NONE",
    "state": "EXECUTED | EXECUTED_EMPTY | NOT_EXECUTED | PENDING | UNREADABLE | NONE",
    "observed": { "owner": "…", "state": "…" }, // lo que muestra la traza; sólo REAL; informativo
    "ir": { … §3 … } ,                          // null sólo con state UNREADABLE o NONE
    "pending": null
  },

  "turn_kind": "DELTA | NEW_SEARCH | ANSWER_TO_CLARIFICATION | PROTOCOL | OTHER_OPERATION | UNCLEAR",
  "m2a_scope": "IN | IN_NEW_SEARCH | IN_PROTOCOL | OUT_NO_ANCHOR | OUT_ANCHOR_NOT_SEARCH | OUT_OPERATION_CHANGE | OUT_REFERENT_EXTENSIONAL | OUT_RELATIVE_TO_PREVIOUS",

  "ops": [ { … §4 … } ],
  "resulting_ir": { … §3 … } ,                  // o null, según §5.2
  "alternatives": [],                           // IR alternativas; obligatorio con dos opciones (p. ej. comprador / lugar)
  "expected_disposition": "EXECUTE | NEW_SEARCH | CLARIFY | REASK | INFORM | HANDOFF_LEGACY",
  "disposition_reason": null,                   // lista cerrada §5.3
  "effects": [],                                // (derivado) lista cerrada §6
  "expected_echo": [],                          // (derivado) lista cerrada §7

  "ambiguity": "CLEAR | AMBIGUOUS",
  "confidence": "HIGH | MEDIUM | LOW",
  "gap_codes": [],                              // lista cerrada §8
  "labeler": "…", "notes": "…"
}
```

## 2. El turno previo (`prior`)

### 2.1 Convención única

`prior.*` describe **el estado en que queda la conversación según quién es dueño del turno previo después del CUTOVER**:

| Dueño del turno previo | Qué describe `prior.state`, `prior.ir`, `prior.pending` |
|---|---|
| `FOUNDATION_SEARCH` (el turno previo es una búsqueda, o un follow-up dentro de M2a) | El **producto bajo el contrato vigente** (M1_0 + adenda F1–F30): lo que el contrato dice que pasa con ese turno, no lo que hizo legacy en la traza |
| `LEGACY:<operación>` (perfil, conteo, ranking, radar…) | Lo **observado** en la traza: ahí el producto es legacy y M2a no lo modela |

Lo que muestra la traza se guarda siempre en `prior.observed` (sólo REAL) y nunca decide una etiqueta por sí solo.

Por qué: el Gold es el oráculo del producto que se va a construir. Un ancla de búsqueda que el contrato no permite
(por ejemplo, una búsqueda ejecutada con un monto que el contrato declara no ejecutable, o una aclaración que sólo hizo
legacy) es un estado que el producto no puede alcanzar: no se puede montar como caso de prueba y haría que reglas como
F29 o F11 se dispararan por artefactos de otro sistema. En cambio, para un turno de dueño legacy el comportamiento de
legacy **es** el producto.

Derivación mecánica (el validador la comprueba cuando `prior.gold_id` no es null):

| Campo | Regla |
|---|---|
| `owner` | Si el turno previo es una búsqueda (`SEARCH`) o un follow-up con `m2a_scope` que empieza por `IN`: `FOUNDATION_SEARCH`. Si es otra operación, o un follow-up con `m2a_scope = OUT_ANCHOR_NOT_SEARCH`: `LEGACY:<operación del hilo>`. Sin turno previo: `NONE`. |
| `state` con dueño Foundation | Según la disposición esperada del turno previo: `EXECUTE`/`NEW_SEARCH` → `EXECUTED`; `CLARIFY`/`REASK` con una pregunta abierta → `PENDING`; `INFORM` → el estado que ya tenía la conversación. `EXECUTED_EMPTY` y `NOT_EXECUTED` no se pueden saber del contrato: sólo aparecen en casos sintéticos, donde el autor los fija. |
| `state` con dueño legacy | El observado. |
| `UNREADABLE` | Sólo sintético: hay turno previo de Foundation pero su registro no se puede leer (`ir = null`). |
| `NONE` | No hay turno previo (`owner = NONE`, `ir = null`, `gap_seconds = null`). |

La vigencia **no** es un estado: un previo vencido es `gap_seconds > 1800` con el estado que tenga. No existe
`state = EXPIRED`.

### 2.2 `selection_rule`, `chain`, `gap_seconds`

| Campo | Definición |
|---|---|
| `selection_rule` | `IMMEDIATE_SUBSTANTIVE`: el último turno no protocolario anterior, en la misma conversación. `DOMINANT_IN_REPLAY`: en baterías repetidas, el previo más frecuente. `CHAIN_LABELED`: el previo es otro follow-up de este Gold. `SYNTHETIC_AUTHORED`: escrito por el autor. |
| `chain` | Turnos de protocolo entre el previo sustantivo y el follow-up, en orden: `PROTOCOL:option`, `PROTOCOL:page`, `PROTOCOL:view` (vista elegida por botón), `PROTOCOL:reask`. Si hubo una selección, `prior.ir` es el estado **después** de ella. |
| `gap_seconds` | Segundos entre la marca de tiempo del follow-up y la del **turno inmediatamente anterior de la misma conversación, sea del tipo que sea** (incluidos los de `chain`). Entero, redondeo al más cercano (0,5 hacia arriba). En baterías con marcas iguales: 0. Con varias ocurrencias, la que fija `selection_rule`. `null` sólo con `owner = NONE`. |

### 2.3 `pending` (obligatorio sí y sólo sí `state = PENDING`)

```json
{ "reason": "<código de §5.3>",
  "span": "texto exacto del turno previo que quedó sin resolver",      // null sólo si la pregunta no nace de un span (p. ej. CONTEXT_EXPIRED_CONFIRM)
  "candidate_slots": ["BUYER", "GEOGRAPHY"],      // 1 ranura, o 2 cuando la duda es de dimensión
  "candidate_value": "lectura humana del valor en disputa",          // lo que entraría en la ranura elegida
  "options": [ {"option_id": "o1", "slot": "BUYER", "value_hint": "…"} ],   // [] si la pregunta es abierta
  "asks": 1 }
```

`prior.ir` de un previo `PENDING` contiene sólo lo ya resuelto; el span abierto **no** está en `prior.ir.conditions`.

## 3. Forma de la IR (`prior.ir`, `resulting_ir`, `alternatives[]`)

```json
{ "operation": "SEARCH",                         // otra operación sólo en previos de dueño legacy o resultados informativos
  "aspect": null,                                // sólo si operation ∈ {COUNT, SUM_VALUE, GROUP_BY}; prohibido con SEARCH
  "universe": "CONTRACTS | PROCESSES | BOTH | UNSPECIFIED",
  "view": "RECORDS | OPPORTUNITIES",
  "product_view": null,                          // obligatorio sí y sólo sí universe = UNSPECIFIED
  "view_source": "USER | DERIVED | null",
  "conditions": [ {"id": "c1", "dimension": "…", "operator": "…", "polarity": "POSITIVE | NEGATIVE",
                   "value_hint": "…", "level": null, "unit": null} ],
  "logic": null,                                 // null = conjunción de todas; si no, expresión del contrato M1 («AND(c1,OR(c2,c3))»)
  "order": null,                                 // {"operator": "value_desc|value_asc|date_desc|date_asc", "direction_source": "STATED|DEFAULTED"}
  "cardinality": null,                           // {"operator": "top_n", "n": 10, "n_requested": 10}
  "host_notice": [] }                            // avisos del contrato M1 (v1.7–v1.9), p. ej. ACTOR_TIME_ON_SIGNATURE_DATE
```

| Elemento | Decisión 2.1 |
|---|---|
| Ancla dual | `universe = "BOTH"`: las mismas condiciones sobre procesos y contratos. No se usa otro literal ni una lista aparte. |
| Universo dicho | `universe` ≠ `UNSPECIFIED`, `product_view = null`, `view_source = USER`. |
| Universo no dicho | `universe = UNSPECIFIED`, `product_view` calculada con PV1–PV6, `view_source = DERIVED`. |
| Vista elegida por botón | `universe = UNSPECIFIED`, `product_view` = la elegida, `view_source = USER`, y `PROTOCOL:view` en `chain`. Cuenta como dicha (F9). |
| `order` | Objeto con `operator` de la tabla cerrada. `direction_source = DEFAULTED` cuando el texto pide orden por fecha sin dirección (F21); en lo demás `STATED`. Sin `polarity`. |
| `cardinality` | `n` = lo que se ejecuta (1..100); `n_requested` = lo que se pidió. Difieren sólo con el tope (F20). Exige `order`. |
| `logic` | **Entra**, opcional. Sólo se escribe si hay OR o NOT de grupo. Una operación cuyo objetivo está dentro de un OR/NOT no es direccionable: `CLARIFY` / `TARGET_NOT_ADDRESSABLE`. |
| `aspect` | **Entra**, sólo fuera de `SEARCH`. |
| `host_notice` | **Entra** como lista cerrada del contrato M1. No es una clase de `expected_echo`. |
| Otros campos | **Prohibidos** (`universes`, `logic_note`, `universe_quote`, `quote` dentro de la IR…). El validador rechaza claves desconocidas. |
| `unit` (sólo VALUE) | Lista cerrada: `PESOS` (cifra completa o con «pesos», sin palabra de escala), `MILES`, `MILLONES`, `MIL_MILLONES`, `BILLONES` (10¹²), `SMMLV`. `null` fuera de VALUE. |
| `value_hint` | Lectura humana **completa**: monto con su unidad; mes con su año; rango de años como `AAAA-01-01..AAAA-12-31` (F8); periodo relativo con sus fechas absolutas entre paréntesis. |

## 4. Operaciones (`ops`)

```json
{ "op": "SET | CLEAR | KEEP | SET_ORDER | SET_UNIVERSE | ANSWER | CANCEL_PENDING",
  "slot": "TIME | VALUE.lower | VALUE.upper | VALUE | GEOGRAPHY | BUYER | ACTOR | TOPIC | STATE | MODALITY | CONTRACT_TYPE_OR_NATURE | ORDER | CARDINALITY | UNIVERSE | null",
  "operator": "…", "polarity": "POSITIVE | NEGATIVE", "level": null,
  "quote": "…",                                  // subcadena exacta del follow-up; null sólo con input_kind = EVENT
  "value_hint": "…",                             // completo, como en §3
  "unresolved": false,                           // true: esta operación es la que impide ejecutar (queda sin valor cierto)
  "refers_to_result_set": false,
  "depends_on_prior_value": false,
  "inherited": { "year_source": null, "unit_source": null } }
```

| Regla | Contenido |
|---|---|
| O1 | Toda condición **citada** en el follow-up es una operación, aunque el valor resultante no cambie. Lo no mencionado no lleva operación. |
| O2 | `slot` siempre es de la lista cerrada. `slot = null` sólo con `CANCEL_PENDING`. Una respuesta sobre algo que no es ranura (aspecto, operación) deja `ops = []` y se explica en `notes`. |
| O3 | `SET` no dice si añade o reemplaza: el validador lo deriva (`slot_before`). |
| O4 | VALUE por lado (F2). «entre X y Y»: dos `SET` (`VALUE.lower` con `greater_or_equal`, `VALUE.upper` con `less_or_equal`), misma `quote`. Una cota: un `SET` en su lado. El **lado** es el del intervalo efectivo: una cota negada («excluye los de más de X») restringe por arriba y va en `VALUE.upper`. Quitar el valor: un `CLEAR` con `slot = VALUE`. «Cambia el valor a…»: `CLEAR` (`slot = VALUE`, `quote` = la instrucción) + `SET`. |
| O5 | `KEEP`: el follow-up cita, sin cambiarla, una condición del previo (deíctico o valor repetido idéntico). `inherited` va a null: un `KEEP` no hereda nada. |
| O6 | `inherited.year_source` (sólo en `SET`/`ANSWER` de TIME): `STATED` (el año está en el follow-up); `PREVIOUS_ACCEPTED_TIME` (F7); `CLOCK_INFERRED` (mes o día sin año y sin TIME previo, F7); `RELATIVE_TO_CLOCK` (operador `relative_period`: «este año», «últimos N meses»). |
| O7 | `inherited.unit_source` (sólo en `SET`/`ANSWER` de VALUE): `STATED` (la unidad acompaña a esa cifra); `SIBLING_IN_RANGE` (la unidad está una vez en el mismo rango y acompaña a la otra cifra, F6); `PREVIOUS_ACCEPTED_VALUE` (F5). |
| O8 | `refers_to_result_set = true` sí y sólo sí el follow-up trae un pronombre o determinante que apunta al conjunto mostrado («los», «aquellos», «alguno», «esos»). Con F4 sigue siendo una re-consulta. |
| O9 | `depends_on_prior_value = true` sí y sólo sí el valor de la operación no puede escribirse sin un valor del previo (año o unidad heredados, `KEEP` deíctico, relativo al valor previo). |
| O10 | Respuesta a una aclaración: la operación que da el valor de la ranura pendiente es `ANSWER` (con su `slot`). Cualquier **otra** condición citada es `SET`/`CLEAR` normal (y activa F25). Lo que se conserva del marco no lleva operación. |
| O11 | Respuesta por evento (`input_kind = EVENT`): un `ANSWER` con `slot` = la ranura de la opción elegida, `quote = null`, `value_hint` = `pending.options[].value_hint` o `pending.candidate_value`. |
| O12 | «No, mejor X» con pendiente (F23): un `CANCEL_PENDING` (`slot = null`, `quote` = el rechazo) más las operaciones de X. |
| O13 | `NEW_SEARCH` lleva `ops = []`. |
| O14 | Relativo al valor previo (F15): una operación sobre la ranura afectada con `unresolved = true`, `depends_on_prior_value = true` y `value_hint` = la expresión tal cual. No se hace la aritmética. |
| O15 | Turno de protocolo, operación distinta o referente de fila: `ops = []`. |

## 5. Clase de turno, alcance y disposición

### 5.1 `turn_kind`

| Valor | Cuándo se marca | Cuándo NO |
|---|---|---|
| `NEW_SEARCH` | El texto **nombra un universo** (sustantivo de universo citado) **y** trae al menos una condición de contenido propia (TOPIC, BUYER o ACTOR), y no hay pronombre de conjunto ni `KEEP`. | Tema, comprador o periodo sin sustantivo de universo (F22: es `DELTA`). Sustantivo de universo sin contenido propio (es `DELTA` con `SET_UNIVERSE`). |
| `DELTA` | Cualquier otro turno de texto que fija, quita o cita condiciones, orden o universo respecto del previo. Se decide por el texto y la forma del previo, aunque el ancla no sirva (eso va en `m2a_scope`). | Si alguna operación da el valor de la ranura pendiente (es `ANSWER_TO_CLARIFICATION`). |
| `ANSWER_TO_CLARIFICATION` | `prior.state = PENDING` y el turno (texto o evento) da un valor o elige una opción para la ranura pendiente; también «cualquiera» (F24) y la respuesta que además edita otra ranura (F25). | Rechazo con reformulación (F23): es `DELTA` o `NEW_SEARCH` con `CANCEL_PENDING`. Turno que ignora la pregunta (F29): `DELTA`. |
| `PROTOCOL` | Paginación, botón de vista, de orden o de quitar filtro, reset; frases cerradas de paginación. | Una opción que responde una aclaración (es `ANSWER_TO_CLARIFICATION` con `input_kind = EVENT`). |
| `OTHER_OPERATION` | El **texto** pide otra operación (contar, sumar, desglosar, ranking, perfil, seguir, exportar) o pregunta por una fila concreta. | Añadir filtros a un previo de dueño legacy: es `DELTA` (el dueño va en `m2a_scope`). |
| `UNCLEAR` | Ninguno de los anteriores; exige `notes`. | — |

«Consulta completa» (F23) = lo que esta tabla llama `NEW_SEARCH`. Convención 2.1, pendiente de confirmación de producto.

### 5.2 `m2a_scope`: valores, precedencia y disposición

Se asigna **el primero que aplique**, en este orden:

| # | Valor | Cuándo | Disposición | `resulting_ir` |
|---|---|---|---|---|
| 1 | `OUT_NO_ANCHOR` | `prior.state ∈ {NONE, UNREADABLE}` y el turno no es `NEW_SEARCH` | `CLARIFY` / `NO_ANCHOR` (F28) o `ANCHOR_UNREADABLE` | null |
| 2 | `OUT_ANCHOR_NOT_SEARCH` | `prior.owner` empieza por `LEGACY:` | `HANDOFF_LEGACY` / `ANCHOR_NOT_SEARCH:<op>` | mejor lectura, **informativa** (no entra en el acuerdo primario ni en gates) |
| 3 | `IN_PROTOCOL` | `turn_kind = PROTOCOL`, o `input_kind = EVENT` | `EXECUTE` (o `CLARIFY` si el protocolo lo exige, p. ej. ancla dual al paginar) | página: igual al previo; evento: lo que produce |
| 4 | `OUT_OPERATION_CHANGE` | `turn_kind = OTHER_OPERATION` por cambio de operación | `INFORM` / `OPERATION_CHANGE:<op>` | null |
| 5 | `OUT_REFERENT_EXTENSIONAL` | El turno se refiere a una fila o a un subconjunto de lo mostrado por posición o identidad | `INFORM` / `REFERENT_EXTENSIONAL` | null |
| 6 | `OUT_RELATIVE_TO_PREVIOUS` | Alguna operación es relativa a un valor previo (F15) | `CLARIFY` / `RELATIVE_TO_PREVIOUS_VALUE` | null |
| 7 | `IN_NEW_SEARCH` | `turn_kind = NEW_SEARCH` | `NEW_SEARCH` | la IR nueva completa |
| 8 | `IN` | Lo demás: delta o respuesta por texto sobre un ancla o un pendiente de Foundation | la que resulte de las reglas F | según la disposición (abajo) |

`HANDOFF_LEGACY` se reserva para `prior.owner = LEGACY:*`. Con ancla de Foundation nada se entrega a legacy.

`resulting_ir` dentro de `IN`:

| Disposición | `resulting_ir` |
|---|---|
| `EXECUTE` | La IR ejecutada, completa. |
| `CLARIFY` / `CONTEXT_EXPIRED_CONFIRM` | La que resultaría al continuar (F11). |
| `CLARIFY` con dos opciones enumerables (F12, F30) | null; las dos lecturas van completas en `alternatives`. |
| Otro `CLARIFY`, `REASK`, `INFORM` | null. La operación que lo causa lleva `unresolved = true`. |

### 5.3 `expected_disposition` y lista cerrada de `disposition_reason`

`EXECUTE` y `NEW_SEARCH` llevan `disposition_reason = null`.

| Disposición | Código | Regla | Significado |
|---|---|---|---|
| `CLARIFY` | `CONTEXT_EXPIRED_CONFIRM` | F11 | `gap_seconds > 1800`; ofrece continuar o reset |
| `CLARIFY` | `BUYER_OR_PLACE_AFTER_ACTOR` | F12 | «con X» tras un ACTOR; dos opciones |
| `CLARIFY` | `REGION_NOT_IN_LIST` | F13 | región fuera de la lista cerrada |
| `CLARIFY` | `SMMLV_UNSUPPORTED` | F14 | monto en SMMLV, en cualquier turno |
| `CLARIFY` | `RELATIVE_TO_PREVIOUS_VALUE` | F15 | operador relativo a un valor previo |
| `CLARIFY` | `UNIT_AMBIGUOUS` | F5 | monto sin unidad y el VALUE previo no tiene una única unidad activa (o no hay VALUE previo) |
| `CLARIFY` | `INHERITED_UNIT_IMPLAUSIBLE` | F26 | la unidad heredada deja el rango sin interior o da una magnitud absurda |
| `CLARIFY` | `MONTH_YEAR_AMBIGUOUS` | F7 | mes sin año y el TIME previo abarca varios años o es relativo |
| `CLARIFY` | `EMPTY_SEARCH_AFTER_DELTA` | F19 | el delta dejaría la búsqueda sin ninguna condición |
| `CLARIFY` | `STATE_NOT_IN_UNIVERSE` | F27 | el estado no existe en el universo pedido |
| `CLARIFY` | `TERRITORY_VS_CARRIED_BUYER` | F30 | territorio nuevo con comprador de otro territorio arrastrado; dos opciones |
| `CLARIFY` | `NO_ANCHOR` | F28 | fragmento sin búsqueda previa |
| `CLARIFY` | `ANCHOR_UNREADABLE` | diseño | hay turno previo pero su registro no se puede leer |
| `CLARIFY` | `TARGET_NOT_ADDRESSABLE` | diseño | el objetivo está dentro de un OR/NOT |
| `CLARIFY` | `VALUE_WITHOUT_COMPARATOR` | contrato M1 B5 | monto sin comparador |
| `CLARIFY` | `UNRESOLVED_VALUE:<DIMENSIÓN>` | contrato M1 | el propio texto no basta para la dimensión (homónimo, nombre ambiguo) |
| `REASK` | `OPTION_NOT_CHOSEN` | F24 | «cualquiera» ante opciones |
| `REASK` | `ANSWER_EDITS_OTHER_SLOT` | F25 | la respuesta además edita otra ranura |
| `REASK` | `PENDING_UNANSWERED` | F29 | el turno no responde la pregunta pendiente |
| `REASK` | `ANSWER_UNRESOLVED` | diseño | la respuesta no resuelve la ranura |
| `INFORM` | `CLEAR_ON_EMPTY_SLOT` | F18 | se pide quitar un filtro que no estaba; no re-consulta |
| `INFORM` | `OPERATION_CHANGE:<op>` | alcance | otra operación sobre un ancla de Foundation (incluye guardar radar: `FOLLOW`) |
| `INFORM` | `REFERENT_EXTENSIONAL` | alcance | referencia a una fila o subconjunto mostrado |
| `HANDOFF_LEGACY` | `ANCHOR_NOT_SEARCH:<op>` | alcance | el ancla es de un dueño legacy |

Códigos retirados (no usar): `TOPN_CAP_EXCEEDED` (F20 ejecuta), `SELF_CONTAINED_REQUEST`, `OPERATION_SWITCH:*`,
`OPERATION_HANDOFF:*`, `ANCHOR_OWNER_LEGACY*`, `REFERENT_UNRESOLVED`, `MONTH_YEAR_MULTI_YEAR_PRIOR`,
`MONTH_YEAR_RELATIVE_PRIOR`, `NON_SELECTION`, `WITH_AFTER_ACTOR_BUYER_OR_PLACE`, `CON_X_BUYER_OR_PLACE`.

Precedencia cuando aplican varias reglas dentro de `IN` (la primera que aplique fija disposición y código):
F11 (vencido) → F29/F24/F25 (hay pendiente) → F14 → F15 → F12 → F30 → F13 → F27 → F5/F26 → F7 → F19 → F18 → ejecutar.
Si el previo `PENDING` además está vencido, gana F11.

## 6. `effects` (derivado; lista cerrada)

Lo que el delta **hace además** de lo que cada operación dice. Se calcula sólo cuando la disposición es `EXECUTE`, o
`CLARIFY` / `CONTEXT_EXPIRED_CONFIRM` (sobre la IR que resultaría); en lo demás es `[]`.

| Efecto | Se marca sí y sólo sí | No se marca |
|---|---|---|
| `VALUE_RANGE_REPLACED` | Una cota VALUE del previo **desaparece sin que haya una operación sobre su propio lado**: (a) la regla F2 de rango sin interior (cota inferior ≥ cota superior tras aplicar el `SET`), o (b) `CLEAR(VALUE)` por instrucción explícita seguido de un `SET` que no repone ese lado. | Cota sustituida por otra del **mismo** lado. Rango nuevo que fija los **dos** lados. `CLEAR(VALUE)` sin `SET`. |
| `TOPIC_REPLACED` | `SET` de TOPIC positivo sobre un previo con TOPIC positivo de valor distinto (F3, F22). | TOPIC sobre ranura vacía. Exclusión de tema (F17). |
| `POLARITY_INVERTED` | `SET` negativo cuyo valor es **el único** positivo vigente de esa ranura: el positivo sale y entra la exclusión (F16). | Negar un valor distinto (F17). |
| `VIEW_REDERIVED` | El previo tenía vista derivada (`view_source = DERIVED`) y `product_view` del resultado es **distinta** (F9). | Vista derivada que no cambia. Universo dicho o elegido por botón. |
| `UNIVERSE_CHANGED` | `SET_UNIVERSE` cuyo `universe` resultante difiere del previo (incluye `BOTH` → uno). | `SET_UNIVERSE` que repite el universo vigente. |
| `TOPN_CARRIED` | La `cardinality` del previo sobrevive y hay un `SET_ORDER` (F10). | Cardinalidad nueva en el mismo turno. |
| `TOPN_CAPPED` | `cardinality.n_requested > 100` y `n = 100` (F20). | — |
| `ORDER_DIRECTION_DEFAULTED` | `order.direction_source = DEFAULTED` (F21). | — |
| `PENDING_CANCELLED` | Hay `CANCEL_PENDING` (F23), o un `NEW_SEARCH` llega con `prior.state = PENDING`. | — |
| `CONTEXT_RESET` | `m2a_scope = IN_NEW_SEARCH` con `prior.owner = FOUNDATION_SEARCH` y `prior.state ∈ {EXECUTED, EXECUTED_EMPTY, NOT_EXECUTED}`. | Sin ancla previa. |

## 7. `expected_echo` (derivado; lista cerrada)

Lo que la respuesta **debe anunciar**. Describe la respuesta que corresponde a `expected_disposition`: con
`EXECUTE`/`NEW_SEARCH`, el eco del resultado; con `CLARIFY` / `CONTEXT_EXPIRED_CONFIRM`, sólo `CONTEXT_EXPIRED`; con
cualquier otro `CLARIFY`, `REASK`, `INFORM` o `HANDOFF_LEGACY`, `[]`.

| Clase | Se exige sí y sólo sí | No |
|---|---|---|
| `CARRIED` | Turno `DELTA` o `ANSWER_TO_CLARIFICATION` ejecutado y al menos una condición, el orden, la cardinalidad o un universo dicho del previo **sobrevive sin que ninguna operación lo toque**. | Nada sobrevive. `NEW_SEARCH`. Protocolo de página. |
| `UNIT_INHERITED` | Alguna operación tiene `unit_source = PREVIOUS_ACCEPTED_VALUE` (F5). Anuncia el valor completo. | Unidad repartida dentro del rango. |
| `UNIT_SHARED_IN_RANGE` | Alguna operación tiene `unit_source = SIBLING_IN_RANGE` (F6): el eco muestra las dos cotas con su unidad. | — |
| `YEAR_INHERITED` | Alguna operación tiene `year_source = PREVIOUS_ACCEPTED_TIME` (F7). | `KEEP` de un año (es `CARRIED`). |
| `YEAR_CLOCK_INFERRED` | Alguna operación tiene `year_source = CLOCK_INFERRED` (F7). | — |
| `RELATIVE_WINDOW_RESOLVED` | Alguna operación tiene `year_source = RELATIVE_TO_CLOCK`: el eco muestra las fechas absolutas. | — |
| `VALUE_RANGE_REPLACED`, `TOPIC_REPLACED`, `POLARITY_INVERTED`, `UNIVERSE_CHANGED`, `TOPN_CARRIED`, `TOPN_CAPPED`, `ORDER_DIRECTION_DEFAULTED`, `CONTEXT_RESET` | El efecto del mismo nombre está en `effects`. | — |
| `VIEW_DERIVED` | El resultado tiene `view_source = DERIVED` (toda vista por defecto se anuncia, se recalcule o no). | Universo dicho o elegido por botón. |
| `VIEW_REDERIVED` | El efecto `VIEW_REDERIVED` está en `effects` (además de `VIEW_DERIVED`). | — |
| `CONTEXT_EXPIRED` | Disposición `CLARIFY` / `CONTEXT_EXPIRED_CONFIRM`. | — |

No son clases de eco: los avisos del contrato M1 (`host_notice`), `PENDING_CANCELLED` (no se anuncia: se ejecuta lo
nuevo) y las exclusiones añadidas por F17 (son condiciones nuevas corrientes).

## 8. `gap_codes` (lista cerrada)

Se marca un código cuando el caso **toca** el tema, aunque una regla ya lo cierre. «Cerrado por» dice qué regla lo
decide; «vivo» = sigue sin decisión.

| Código | Tema | Estado |
|---|---|---|
| `SET` | re-consultar o filtrar lo mostrado | cerrado por F4 |
| `PV` | recálculo de la vista | cerrado por F9 |
| `VCOMP` | cota de sentido opuesto | cerrado por F2 |
| `UNIT` | unidad omitida | cerrado por F5, F6, F26 |
| `MONTHYEAR` | mes sin año | cerrado por F7 |
| `YEARRANGE` | inclusividad de rangos de años | cerrado por F8 |
| `TTL` | vigencia del contexto | cerrado por F11 |
| `TOPIC` | tema nuevo: añade o reemplaza | cerrado por F3, F22 |
| `COUNTERPART` | «con X» tras un actor | cerrado por F12 |
| `REGION_VOCAB` | vocabulario de región | cerrado por F13 |
| `SMMLV` | monto en SMMLV | cerrado por F14 |
| `POLARITY_SAME_VALUE` | negar el valor vigente | cerrado por F16 |
| `POLARITY_OTHER_VALUE` | negar otro valor | cerrado por F17 |
| `CLEAR_ABSENT` | quitar un filtro que no está | cerrado por F18 |
| `EMPTY_AFTER_DELTA` | delta que deja la búsqueda vacía | cerrado por F19 |
| `TOPN_CAP` | top-N por encima del tope | cerrado por F20 |
| `ORDER_DIRECTION` | orden por fecha sin dirección | cerrado por F21 |
| `UNIVERSE_FRAGMENT` | fragmento o consulta completa | cerrado por F22, F23 y la definición de §5.1 (pendiente de confirmar) |
| `PENDING_REJECTED` | «no, mejor X» con pendiente | cerrado por F23 (base del delta: vivo) |
| `ANY_OPTION` | «cualquiera» ante opciones | cerrado por F24 |
| `ANSWER_PLUS_EDIT` | respuesta que edita otra ranura | cerrado por F25 |
| `UNIT_IMPLAUSIBLE` | unidad heredada inverosímil o que vacía el rango | cerrado por F26 (umbral: vivo) |
| `STATE_UNIVERSE` | estado que no existe en el universo | cerrado por F27 |
| `NO_ANCHOR` | fragmento sin previo | cerrado por F28 |
| `PRIOR_NOT_EXECUTED` | follow-up sobre un turno no ejecutado | cerrado por F29 cuando hay pregunta pendiente; **vivo** cuando el previo se aceptó y falló la fuente |
| `GEOBUYER` | comprador y territorio que no coinciden | cerrado por F30 (territorio nuevo) y F1 (comprador nuevo dentro del territorio); **vivo** para compradores nacionales y contención |
| `UNIVTIME` | TIME arrastrado al cambiar de universo | cerrado por F1 (se conserva) |
| `SELECT` | previo reconstruido tras una selección | cerrado por `prior.chain` (§2.2) |
| `ENADELANTE` | «X en adelante» | **vivo** (contrato M1: añadir a los inclusivos) |
| `B5RANGE` | cota inferior sin comparador en un rango | **vivo** |
| `COMPARATOR_FORM` | comparador con las palabras desordenadas | **vivo** |
| `NEGATED_COMPARATOR` | cota negada: operador negado o comparador complementario | **vivo** (el lado sí está fijado, O4) |
| `RELATIVE_ANCHOR` | «el año anterior»: al valor previo o al reloj | **vivo** |
| `OPPORTUNITY_STATE` | estado redundante con la lente de oportunidad | **vivo** |
| `PAGE_VS_TOPN` | «otros diez más»: paginación o cardinalidad | **vivo** |
| `OPSWITCH`, `TOTALES`, `FIRMADOS`, `ACTORTIME`, `PENDING_OPSWITCH` | cambio de operación por follow-up y afines | fuera de M2a |

Un código nuevo exige añadirlo aquí antes de usarlo.

## 9. Regla F# → campos que la reflejan (para el validador)

| Regla | Comprobación |
|---|---|
| F1 | Toda condición de `prior.ir` ausente de `resulting_ir` tiene una operación (`CLEAR` o `SET`) sobre su dimensión, o el efecto `VALUE_RANGE_REPLACED` / `POLARITY_INVERTED`. Toda condición de `prior.ir` sin operación sobre su ranura está idéntica en `resulting_ir`. |
| F2 | `VALUE.lower`/`VALUE.upper` según O4; `effects: VALUE_RANGE_REPLACED` según §6; nunca quedan dos cotas con inferior ≥ superior. |
| F3, F22 | `SET TOPIC` sobre TOPIC ocupado ⇒ un solo TOPIC positivo en el resultado + `TOPIC_REPLACED`; texto sin universo con tema ⇒ `turn_kind = DELTA`. |
| F4 | `refers_to_result_set = true` nunca implica `OUT_REFERENT_EXTENSIONAL` por sí solo; el resultado es una re-consulta. |
| F5 | `unit_source = PREVIOUS_ACCEPTED_VALUE` ⇒ todas las cotas VALUE de `prior.ir` tienen la misma `unit` y no es `SMMLV`; si no, `CLARIFY` / `UNIT_AMBIGUOUS`. |
| F6 | Rango con una sola palabra de unidad ⇒ una cota `STATED`, la otra `SIBLING_IN_RANGE`, misma `unit`. |
| F7 | `year_source = PREVIOUS_ACCEPTED_TIME` ⇒ `prior.ir` tiene un único TIME positivo de un solo año, no relativo; TIME previo plurianual o relativo ⇒ `CLARIFY` / `MONTH_YEAR_AMBIGUOUS`; sin TIME previo ⇒ `CLOCK_INFERRED`. |
| F8 | Rango de años ⇒ `value_hint` termina en `-12-31` del año final. |
| F9 | `VIEW_REDERIVED` según §6; con `view_source = USER` en el previo, `universe`/`product_view` no cambian sin `SET_UNIVERSE`. |
| F10 | `SET_ORDER` con `cardinality` previa y sin operación sobre CARDINALITY ⇒ misma `n` + `TOPN_CARRIED`. |
| F11 | `gap_seconds > 1800` (y `m2a_scope = IN`) ⇒ `CLARIFY` / `CONTEXT_EXPIRED_CONFIRM`, `resulting_ir` no nulo. |
| F12 | Previo con ACTOR y operación de lugar introducida por «con» ⇒ `CLARIFY` / `BUYER_OR_PLACE_AFTER_ACTOR`, dos `alternatives`. |
| F13 | GEOGRAPHY de nivel REGION fuera de la lista ⇒ `CLARIFY` / `REGION_NOT_IN_LIST`. |
| F14 | Cualquier VALUE con `unit = SMMLV` en una operación ⇒ `CLARIFY` / `SMMLV_UNSUPPORTED`; ningún `prior.ir` de dueño Foundation con `state = EXECUTED` contiene `unit = SMMLV`. |
| F15 | Operación con `unresolved` y `depends_on_prior_value` relativa ⇒ `OUT_RELATIVE_TO_PREVIOUS`. |
| F16, F17 | `SET` negativo: si su valor es el único positivo de la ranura ⇒ sale el positivo, `POLARITY_INVERTED`; si no ⇒ se añade y el positivo sigue. |
| F18 | `CLEAR` sobre dimensión ausente de `prior.ir` como única operación ⇒ `INFORM` / `CLEAR_ON_EMPTY_SLOT`. |
| F19 | El resultado no tendría ninguna condición ⇒ `CLARIFY` / `EMPTY_SEARCH_AFTER_DELTA`. |
| F20 | `n_requested > 100` ⇒ `n = 100`, `EXECUTE`, `TOPN_CAPPED`. |
| F21 | Orden por fecha sin dirección ⇒ `date_desc`, `direction_source = DEFAULTED`. |
| F23 | `CANCEL_PENDING` ⇒ `PENDING_CANCELLED`; el resto del turno se etiqueta como `DELTA` o `NEW_SEARCH`. |
| F24 | Pendiente con opciones y respuesta que no elige ⇒ `REASK` / `OPTION_NOT_CHOSEN`. |
| F25 | `ANSWER` + cualquier otra operación sobre otra ranura ⇒ `REASK` / `ANSWER_EDITS_OTHER_SLOT`. |
| F26 | `unit_source = PREVIOUS_ACCEPTED_VALUE` y (rango sin interior, o monto por debajo del piso de montos del producto) ⇒ `CLARIFY` / `INHERITED_UNIT_IMPLAUSIBLE`; nunca junto con `VALUE_RANGE_REPLACED`. |
| F27 | STATE (citado o arrastrado) que no existe en el universo resultante ⇒ `CLARIFY` / `STATE_NOT_IN_UNIVERSE`. |
| F28 | `prior.state = NONE` y no `NEW_SEARCH` ⇒ `OUT_NO_ANCHOR`, `CLARIFY` / `NO_ANCHOR`. |
| F29 | `prior.state = PENDING` y ninguna operación `ANSWER` ni `CANCEL_PENDING` ni `NEW_SEARCH` ⇒ `REASK` / `PENDING_UNANSWERED`. |
| F30 | `SET GEOGRAPHY` con BUYER territorial arrastrado de otro territorio ⇒ `CLARIFY` / `TERRITORY_VS_CARRIED_BUYER`, dos `alternatives`. |

## 10. Reglas generales de etiquetado

1. El Gold describe el producto ideal bajo el contrato; un hueco del host no es `CLARIFY`.
2. No se escriben `effects` ni `expected_echo`.
3. `resulting_ir` se escribe a mano y entero cuando §5.2 lo pide; el validador lo compara con el de un reductor de
   referencia independiente del código del producto. Una diferencia abre adjudicación; no se corrige sola.
4. Donde una regla F admite dos lecturas (ver «Tensiones» de la adenda), se etiqueta la mejor lectura, se pone el
   `gap_code` y se explica la alternativa en `notes` con `ambiguity = AMBIGUOUS`.
5. El texto idéntico en dos registros no implica la misma etiqueta: manda `prior`.
