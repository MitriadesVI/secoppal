# Gold de follow-ups — esquema v2 para re-etiquetar, hoja de adjudicación propuesta y slice sintético

Fecha: 2026-10-01. Autor: METIS. **Propuesta**: no re-etiqueta nada ni modifica ningún `labels.jsonl`.
Base: los dos etiquetados ciegos de los 65 `FOLLOWUP_DELTA` REAL (`gold_followups/labeler_A`, `labeler_B`), su
comparación (`comparison/summary.json`, `disagreements.jsonl`) y `M2A_DESIGN_v2.md`. El texto REAL no se cita: sólo ids.
Regla que rige todo el documento: **el Gold precede al código**. Donde una etiqueta contradice el diseño, cambia el
diseño o se eleva a producto; nunca se cambia la etiqueta para que encaje.

---

## 1. Qué corrige v2 respecto del esquema usado por A y B

| Problema observado | Evidencia | Corrección en v2 |
|---|---|---|
| `needs_referent` mezcla dos cosas: A lo usó para «el fragmento se refiere al conjunto mostrado»; B, para «el valor sale del turno previo». 14 desacuerdos, todos de definición. | `summary.json` → `needs_referent`: 14 ids | Dos campos por operación: `refers_to_result_set` y `depends_on_prior_value` |
| `delta_op` ADD/REPLACE se etiqueta a mano y diverge sin que cambie el resultado (rangos: A = REPLACE+ADD, B = REPLACE+REPLACE). | R0180, R0192, R0311 | No se etiqueta: `slot_before` lo **calcula el validador** a partir de la IR previa |
| Rango de valor sin convención (una operación, dos, o una por cota existente). | R0180, R0192, R0202, R0311 | Convención única (§2.3) |
| Una condición citada cuyo valor no cambia se omite (A) o se etiqueta (B). | R0099, R0336 | Toda condición **citada** es una operación; la referencia a un valor previo es `KEEP` |
| Año o unidad heredados van en texto libre (`value_note`). | R0496–R0498, R0531, R0532 | Campo de procedencia `inherited` |
| El ancla no-SEARCH se descubre leyendo `prior_ir.operation`. | 7 ids por acuerdo, 2 sólo en A | Campo `prior.owner` y campo `m2a_scope` |
| El previo se eligió con reglas distintas (cadenas, baterías de replay). | `prior_text`: R0079, R0083, R0306, R0561 | Campo `prior.selection_rule` y `prior.gap_seconds` obligatorio |
| La comparación se hizo por forma del delta (56/65) cuando el acuerdo real es por estado resultante (58/65). | `summary.json`; medición E12 de la revisión adversarial | El campo primario de comparación y de gate es `resulting_ir` |

---

## 2. Esquema `m2a-followup-gold/2`

Un registro JSON por línea. Validador determinista en `scripts/audits/m1_search/` (HERA), escrito sin importar `app/`.

```json
{
  "gold_id": "R0092",
  "slice": "REAL_FOLLOWUP | SYNTHETIC_FOLLOWUP",
  "followup_text": "…",                       // REAL: fuera de git
  "followup_sha256": "…",
  "source_kind": "ORGANIC | SCRIPTED | SYNTHETIC",

  "prior": {
    "gold_id": "R0091 | null",                // registro Gold del turno previo, si existe
    "selection_rule": "IMMEDIATE_SUBSTANTIVE | DOMINANT_IN_REPLAY | CHAIN_LABELED | SYNTHETIC_AUTHORED",
    "chain": ["PROTOCOL:option", "PROTOCOL:page"],   // turnos de protocolo entre el previo sustantivo y el follow-up
    "gap_seconds": 68,
    "owner": "FOUNDATION_SEARCH | LEGACY:PROFILE_ACTOR | LEGACY:COUNT | LEGACY:RANK_SUPPLIERS | LEGACY:PROFILE_BUYER | NONE",
    "state": "EXECUTED | EXECUTED_EMPTY | NOT_EXECUTED | PENDING | EXPIRED | NONE",
    "ir": { "operation": "SEARCH", "universe": "CONTRACTS | PROCESSES | UNSPECIFIED", "view": "RECORDS | OPPORTUNITIES",
            "product_view": null, "view_source": "USER | DERIVED | null",
            "conditions": [ {"id": "c1", "dimension": "TIME", "operator": "during_year", "polarity": "POSITIVE",
                             "value_hint": "2025", "level": null, "unit": null} ],
            "order": null, "cardinality": null },
    "pending": null                             // {"dimension": "BUYER", "reason": "AMBIGUOUS_HOST_DIMENSION:BUYER", "options": 3}
  },

  "turn_kind": "DELTA | NEW_SEARCH | ANSWER_TO_CLARIFICATION | PROTOCOL | OTHER_OPERATION | UNCLEAR",
  "m2a_scope": "IN | OUT_ANCHOR_NOT_SEARCH | OUT_OPERATION_CHANGE | OUT_REFERENT_EXTENSIONAL | OUT_RELATIVE_TO_PREVIOUS",

  "ops": [
    { "op": "SET | CLEAR | KEEP | SET_ORDER | SET_UNIVERSE | ANSWER",
      "slot": "TIME | VALUE.lower | VALUE.upper | VALUE | GEOGRAPHY | BUYER | ACTOR | TOPIC | STATE | MODALITY | CONTRACT_TYPE_OR_NATURE | ORDER | CARDINALITY | UNIVERSE",
      "operator": "during_month", "polarity": "POSITIVE", "level": null,
      "quote": "…",                             // subcadena exacta del follow-up
      "value_hint": "julio de 2026",            // lectura humana COMPLETA: con año y con unidad
      "refers_to_result_set": false,            // el fragmento apunta al conjunto mostrado («los», «aquellos», «alguno»)
      "depends_on_prior_value": true,           // el valor no se puede leer sin un valor del turno previo
      "inherited": { "year_source": "STATED | PREVIOUS_ACCEPTED_TIME | CLOCK_INFERRED | null",
                     "unit_source": "STATED | PREVIOUS_ACCEPTED_VALUE | SIBLING_IN_RANGE | null" } }
  ],

  "resulting_ir": { … misma forma que prior.ir, COMPLETA … },      // campo primario
  "effects": ["VALUE_RANGE_REPLACED", "VIEW_REDERIVED", "TOPIC_REPLACED", "UNIVERSE_CHANGED"],   // lista cerrada; puede ser vacía
  "expected_disposition": "EXECUTE | CLARIFY | REASK | NEW_SEARCH | HANDOFF_LEGACY",
  "disposition_reason": null,                   // código corto si no es EXECUTE
  "expected_echo": ["CARRIED", "YEAR_INHERITED"],                  // clases de anuncio exigidas (lista cerrada del diseño)

  "ambiguity": "CLEAR | AMBIGUOUS", "alternatives": [],
  "confidence": "HIGH | MEDIUM | LOW",
  "gap_codes": ["MONTHYEAR"],                   // lista cerrada de §4
  "labeler": "…", "notes": "…"
}
```

### 2.1 Operaciones alineadas con el álgebra por ranuras

| Etiqueta v1 | Etiqueta v2 | Nota |
|---|---|---|
| `ADD` | `SET` con `slot_before = EMPTY` | `slot_before` lo deriva el validador, no el etiquetador |
| `REPLACE` | `SET` con `slot_before = OCCUPIED` | ídem |
| `REMOVE` | `CLEAR` | sobre la dimensión entera; exige `quote` |
| (sin etiqueta) referencia a un valor previo | `KEEP` | con `depends_on_prior_value = true` |
| `ORDER` ADD/REPLACE | `SET_ORDER` | — |
| `UNIVERSE` REPLACE (`subject_is`) | `SET_UNIVERSE` | `value_hint` = universo nuevo |
| `OTHER` (`operation_switch`, `aspect_is`, `operation_is`) | no es operación de M2a | va en `turn_kind = OTHER_OPERATION` y `m2a_scope = OUT_OPERATION_CHANGE` |
| `OTHER` (`value_without_comparator`) | condición `UNSUPPORTED` en `resulting_ir` con disposición `CLARIFY` | regla B5 del contrato |

### 2.2 Reglas de etiquetado

1. **Toda condición citada en el fragmento es una operación**, aunque el valor resultante sea igual al previo.
2. Lo no mencionado no lleva operación: se conserva. Nunca se etiqueta una remoción sin `quote`.
3. `refers_to_result_set` y `depends_on_prior_value` son independientes; los dos pueden ser verdaderos.
4. `value_hint` se escribe completo; lo que no esté en el fragmento se declara en `inherited`.
5. `resulting_ir` se escribe entero y a mano; el validador lo compara con el que calcula un reductor de referencia
   **propio de HERA** (independiente del código de `app/`). Una diferencia abre adjudicación; no se corrige sola.
6. `expected_disposition` describe el producto ideal, como en el Gold M1; un hueco del host no es `CLARIFY`.
7. Un registro con `prior.owner ≠ FOUNDATION_SEARCH` lleva `m2a_scope = OUT_ANCHOR_NOT_SEARCH` y disposición
   `HANDOFF_LEGACY`; su `resulting_ir` se etiqueta igualmente (sirve a M2b/M3).

### 2.3 Convención única para rango de valor

- «entre X y Y», «de X a Y», «X-Y»: **exactamente dos** operaciones `SET`, `VALUE.lower` con `greater_or_equal` y
  `VALUE.upper` con `less_or_equal`, con la **misma** `quote`.
- Una sola cota: una operación `SET` sobre su lado.
- «sin importar el valor», «quita el rango»: **una** operación `CLEAR` con `slot = VALUE` (no una por cota).
- El efecto sobre la cota del otro lado no se etiqueta como operación: queda en `resulting_ir` y, si la restricción se
  sustituyó entera, en `effects: ["VALUE_RANGE_REPLACED"]`.

### 2.4 Procedencia de año y de unidad

| Campo | Valores | Cuándo |
|---|---|---|
| `inherited.year_source` | `STATED` · `PREVIOUS_ACCEPTED_TIME` · `CLOCK_INFERRED` | en toda operación TIME |
| `inherited.unit_source` | `STATED` · `PREVIOUS_ACCEPTED_VALUE` · `SIBLING_IN_RANGE` | en toda operación VALUE |
| `prior.ir.conditions[].unit` | etiqueta de escala (lista cerrada) o `null` | en toda condición VALUE del previo |

---

## 3. Hoja de adjudicación propuesta

Columnas: qué decide cada etiquetador, qué decisión del registro D1–D15 lo resuelve, y cómo. «Mecánico» = se resuelve
aplicando una regla ya decidida, sin juicio nuevo. Nada de esto modifica los `labels.jsonl`.

### 3.1 Los 9 desacuerdos de fondo

| Id | A | B | ¿Misma IR resultante? | Propuesta | Resuelve |
|---|---|---|---|---|---|
| R0099 | `ORDER` añadido | `ORDER` añadido + `TIME` reemplazado por el mismo año | sí | `SET_ORDER` + `KEEP` sobre TIME (`depends_on_prior_value = true`) | **Mecánico**: D1 + regla 2.2-1 |
| R0180 | VALUE: REPLACE + ADD | VALUE: REPLACE + REPLACE | sí | dos `SET` (`VALUE.lower`, `VALUE.upper`); `slot_before` derivado | **Mecánico**: D1, D2, §2.3 |
| R0192 | ídem | ídem | sí | ídem; además `unit_source` de la cota sin unidad (ver P3 del diseño) | **Mecánico** en la forma; la unidad repartida dentro del rango **necesita decisión** |
| R0311 | ídem | ídem | sí | ídem | **Mecánico**: D1, D2, §2.3 |
| R0336 | sólo VALUE | VALUE + TIME (año ya presente) | sí | `SET VALUE.lower` + `SET TIME` (el año está citado) | **Mecánico**: regla 2.2-1 |
| R0189 | `OTHER_OPERATION` (conteo → lista), sin delta | `DELTA` STATE + cambio de operación | sí en condiciones | `m2a_scope = OUT_ANCHOR_NOT_SEARCH`, `HANDOFF_LEGACY` | **Fuera de M2a** (D9). Cuándo un follow-up cambia de operación: producto, M3 |
| R0375 | `CLARIFY` (monto sin comparador dentro del rango) | `EXECUTE` (rango) | no | `m2a_scope = OUT_ANCHOR_NOT_SEARCH` | **Fuera de M2a** (D9). La laguna [B5RANGE] **necesita decisión** de contrato; se cubre con un gemelo sintético de ancla SEARCH |
| R0441 | añade cambio de operación | sólo BUYER + TIME | — | `m2a_scope = OUT_ANCHOR_NOT_SEARCH` | **Fuera de M2a** (D9); producto, M3 |
| R0560 | `UNCLEAR` / `CLARIFY` | `ANSWER_TO_CLARIFICATION` / `EXECUTE` | no | `m2a_scope = OUT_ANCHOR_NOT_SEARCH` | **Fuera de M2a** (D9); laguna [TOTALES], producto, M3 |

Balance: 5 mecánicos, 4 fuera de M2a por ancla no-SEARCH (de los cuales 3 dejan una pregunta de producto para M3 y
1 una laguna de contrato).

### 3.2 Desacuerdos que no son de fondo

| Campo | Ids | Propuesta | Resuelve |
|---|---|---|---|
| `needs_referent` | R0079, R0081, R0083, R0087, R0089, R0192, R0223, R0418, R0459 (A sí, B no) | `refers_to_result_set = true` donde el fragmento lleva pronombre o determinante de conjunto; R0459 a revisar (no lo lleva) | **Mecánico** con el campo partido |
| `needs_referent` | R0496, R0497, R0498, R0531, R0532 (B sí, A no) | `depends_on_prior_value = true` + `inherited` | **Mecánico**: D4, D5 |
| `resulting_operation` | R0319, R0320, R0321 | ancla no-SEARCH (R0319 por acuerdo; R0320 y R0321 sólo según A, por previo de cadena) | **Fuera de M2a** (D9) una vez fijado el previo; la operación resultante es producto, M3 |
| `prior_text` | R0079, R0083, R0306, R0561 | fijar el previo con `prior.selection_rule`; en R0306 el previo cambia la IR resultante (con o sin TIME) | **Dato, no producto**: re-determinar en las trazas; hasta entonces `confidence = LOW` |
| `delta_operators` | R0202 | un `CLEAR` con `slot = VALUE` | **Mecánico**: §2.3 |

### 3.3 Lagunas del contrato declaradas por los etiquetadores

| Código | Ids | Qué preguntan | Propuesta | Resuelve |
|---|---|---|---|---|
| [SET] | R0079, R0081, R0083, R0087, R0089, R0099, R0223, R0515 | ¿re-consultar o reordenar lo mostrado? | re-consultar (referente = la consulta); `refers_to_result_set = true` | **Mecánico**: diseño §5.7 (ambos etiquetadores re-consultan) |
| [PV] | R0081, R0106, R0276 | ¿se recalcula la vista? | sí, porque era derivada; `effects: VIEW_REDERIVED`, `expected_echo: VIEW_DERIVED` | **Mecánico**: D6 |
| [VCOMP] | R0310, R0464 | cota de sentido opuesto: ¿rango o reemplazo? | rango | **Mecánico**: D2 |
| — | R0187, R0205, R0531 | cota que deja el rango sin interior | reemplazo, `effects: VALUE_RANGE_REPLACED`. R0205 es un rango de un solo punto: la regla debe ser «sin interior», no «vacío» | **Mecánico**: D2 con la precisión T3 del diseño |
| [UNIT] | R0531, R0532 | unidad omitida | heredada, `unit_source = PREVIOUS_ACCEPTED_VALUE`, `expected_echo: UNIT_INHERITED` | **Mecánico**: D4 |
| [UNIT] | R0192 | cifra sin unidad junto a otra con unidad en el mismo rango | `unit_source = SIBLING_IN_RANGE` | **Necesita decisión** (regla de la autoridad VALUE en primer turno) |
| [MONTHYEAR] | R0496, R0497, R0498 | año del mes | `year_source = PREVIOUS_ACCEPTED_TIME`, `expected_echo: YEAR_INHERITED` | **Mecánico**: D5 |
| [TOPIC] | R0286 (y R0484) | ¿añade o reemplaza? | reemplaza, `effects: TOPIC_REPLACED` | **Mecánico**: D3 (reversible) |
| [GEOBUYER] | R0202 | ¿se conserva la geografía al añadir comprador? | se conserva | **Mecánico**: D8 |
| [UNIVTIME] | R0094 | ¿se conserva TIME al cambiar de universo? | se conserva; `effects: UNIVERSE_CHANGED`; el anuncio dice a qué fecha aplica | **Mecánico**: D8 + eco (D11) |
| (B) | R0094, R0459 | fragmento con sustantivo de universo o de modalidad: ¿delta o búsqueda nueva? | delta: no trae condición de contenido | **Mecánico**: guarda de fragmento del diseño §5.2 |
| [TTL] | R0223, R0286 | vigencia del contexto | ambos etiquetados EXECUTE; el diseño propone 30 min + un toque | **Necesita decisión de producto** (P1 del diseño). No se cambia la etiqueta |
| [SELECT] | R0418, R0431, R0435, R0441 | la IR tras «opción N» es una reconstrucción | `prior.chain` + `prior.ir` = estado tras la selección; R0418, R0431 y R0435 tienen ancla SEARCH con ACTOR (ambos etiquetadores) | **Dato**: regla de `prior.chain`; dependen de la autoridad ACTOR. R0441: fuera (D9) |
| [OPSWITCH] | R0189, R0319, R0441 | cambio de operación por follow-up | fuera | **Fuera de M2a** (D9); producto, M3 |
| [COUNTERPART] | R0320, R0321, R0418 | «con X» tras un actor: ¿comprador o lugar? | R0418 tiene ancla SEARCH y ambos etiquetan comprador (confianza media/baja). R0320 y R0321: A los ancla en un perfil, B en una búsqueda | **Necesita decisión** de contrato (la preposición no está cubierta) para R0418; R0320 y R0321 dependen además de fijar el previo |
| [ACTORTIME] | R0441 | actor + periodo sobre un perfil previo | fuera | **Fuera de M2a** (D9) |
| [FIRMADOS] | R0483 | palabra de estado sola tras un ranking | ambos `CLARIFY`; ancla no-SEARCH | **Fuera de M2a** (D9); sin desacuerdo |
| [TOTALES] | R0560 | palabra sola tras un conteo | fuera | **Fuera de M2a** (D9); producto, M3 |
| [B5RANGE] | R0375 | cota inferior sin comparador dentro de un rango | — | **Necesita decisión** de contrato (y fuera de M2a por ancla) |
| [ENADELANTE] | R0205 | «X en adelante» | ambos inclusivo | **Contrato Gold**: añadir la locución a la lista de inclusivos; sin desacuerdo |
| (B) | R0303, R0314, R0432, R0450 | inclusividad del año final en «de A a B» / «entre A y B» | — | **Necesita decisión** de contrato (regla de la autoridad TIME, no de M2a) |
| (B) | R0187 | comparador con las palabras desordenadas | — | **Necesita decisión** de contrato (proyectar o aclarar) |
| (B) | R0281 | nombre de región que no es DIVIPOLA | — | **Necesita decisión** (vocabulario de REGION, autoridad GEO) |
| (B) | R0301 | monto en SMMLV | — | **Necesita decisión** (¿ejecutable sin año de conversión?); hoy la autoridad VALUE lo pregunta |
| (B) | R0435 | etiqueta de candidato pegada tras una aclaración | ancla SEARCH; `SET ACTOR` sobre ranura ocupada | **Mecánico** en M2a; depende de la autoridad ACTOR |
| (B) | R0106, R0107 | el previo sellado como universo no especificado podría leerse como respuesta a una aclaración | — | **Dato**: revisar el previo en las trazas |

Resumen de lo que sigue necesitando decisión (ninguna bloquea el diseño; bloquean cerrar etiquetas): vigencia (R0223,
R0286); unidad repartida en un rango (R0192); [B5RANGE]; inclusividad de rangos de años; comparador desordenado; región
no DIVIPOLA; SMMLV; «con X» tras un actor (R0418). Para M3: cambio de operación por follow-up y palabra sola tras conteo.

---

## 4. Lista cerrada de `gap_codes` para el re-etiquetado

`SET`, `SELECT`, `PV`, `OPSWITCH`, `UNIT`, `COUNTERPART`, `MONTHYEAR`, `VCOMP`, `UNIVTIME`, `GEOBUYER`, `ENADELANTE`,
`TTL`, `TOPIC`, `B5RANGE`, `ACTORTIME`, `FIRMADOS`, `TOTALES`, `YEARRANGE`, `COMPARATOR_FORM`, `REGION_VOCAB`, `SMMLV`,
`UNIVERSE_FRAGMENT`. Un código nuevo exige añadirlo aquí antes de usarlo.

---

## 5. Slice sintético (separado del REAL)

Archivo propio (`followup_synthetic.jsonl`), en git, con textos **inventados** (ninguno parecido a un texto REAL:
misma comprobación de n-gramas que el Gold M1) y sin datos personales. No se mezcla con el REAL ni en conteos ni en
el sello: son dos entradas del sello.

Cada caso es un par (previo, follow-up) con el previo escrito como IR Gold completa y `prior.selection_rule =
SYNTHETIC_AUTHORED`. Donde tiene sentido, cada caso lleva un **gemelo** que cambia un solo rasgo.

| Clase (ausente o casi en los 65) | Qué cubre | Casos |
|---|---|---|
| Polaridad negativa | `SET` con `NEGATIVE` por dimensión (TOPIC, GEOGRAPHY, BUYER, MODALITY, STATE); negación de un valor ya arrastrado en positivo (debe aclarar); negación en VALUE y TIME | 14 |
| Top-N | cifra explícita, superlativo singular, orden nuevo con top-N arrastrado (P2 del diseño), tope de 100, `CLEAR` de orden con cardinalidad | 10 |
| Tiempo relativo | «este año», «últimos N meses» como `SET TIME`; relativo sobre un TIME arrastrado; mes sin año con previo de varios años y con previo relativo (deben aclarar); mes sin año sin TIME previo (regla del reloj, anunciada) | 12 |
| Relativo al valor previo (D7) | «el mes siguiente», «el año anterior», «bájalo», «el doble», «el mismo comprador»: todos `OUT_RELATIVE_TO_PREVIOUS`, `CLARIFY` | 10 |
| Respuesta a aclaración | las 11 filas de `R5_REPAIR.md` §P2 reexpresadas; opción por evento; respuesta que no resuelve; segundo cambio de marco; pendiente caducado | 18 |
| Referentes ordinales y de fila | «el segundo», «ese contrato», «de esos, los de…»: `OUT_REFERENT_EXTENSIONAL` | 8 |
| Cambio explícito de universo | contratos ↔ procesos, a oportunidades, ancla dual que se queda con uno, universo dicho + condición dependiente de dataset (STATE, MODALITY), vista elegida por botón que **no** se recalcula tras un delta | 12 |
| VALUE por lado | tabla completa: lado previo (ninguno, inferior, superior, ambos) × lado nuevo (inferior, superior, ambos) × relación de cifras (con interior, sin interior, igual); «cambia el valor a…» | 16 |
| Herencia de unidad | unidad única; dos unidades en el previo; previo en SMMLV; previo en pesos sin escala; cifra bajo el piso con unidad propia; cadena de dos herencias | 8 |
| `CLEAR` distinto de VALUE | quitar periodo, lugar, estado, comprador; quitar lo que no existe; quitar lo único que hace segura la consulta | 8 |
| Orden | ascendente por valor y por fecha; orden nuevo sobre orden previo; orden con filtro por token | 6 |
| Búsqueda nueva con historia | gemelos «fragmento / consulta completa» por dimensión (como `P159`–`P164`); fragmento con sustantivo de universo y sin contenido; tema solo tras una búsqueda con comprador | 12 |
| Ancla inválida | no ejecutada, vacía, vencida (a 31 min, 5 h, 13 h), ilegible, de dueño legacy | 10 |
| Protocolo sobre resultado Foundation | página tras búsqueda, tras delta y tras cambio de orden; ancla dual; botón de orden; quitar filtro; guardar radar (mensaje) | 10 |
| Dos o más dimensiones | combinaciones no vistas en los 65, incluida una con `CLEAR` + `SET` | 8 |
| **Total** | | **162** |

### 5.1 Quién escribe y quién etiqueta

| Rol | Quién | Restricción |
|---|---|---|
| Autor de los casos | Un agente de HERA dedicado, con este documento y el contrato Gold; **no** el implementador ni METIS | No ve el prompt v5 ni el código de `foundation_delta` |
| Etiquetador A y etiquetador B | Dos modelos distintos, ciegos entre sí (como en el Gold M1) | No ven `m2a_design/` salvo este esquema; no ven el código de follow-up; no ejecutan el producto |
| Adjudicación | El orquestador, con registro por id y regla aplicada | Sin dispensas: una diferencia deliberada se resuelve en el contrato Gold y se vuelve a sellar |
| Validador y reductor de referencia | HERA, en `scripts/audits/m1_search/` | Independiente de `app/` |
| Partición | `sha256(gold_id + "m2a-split-v1") mod 4 == 0` → DEV; resto → TEST; fijada antes de tocar el prompt | El autor del prompt sólo ve DEV |

### 5.2 Orden de trabajo

1. Decidir o aparcar las preguntas de §3 marcadas «necesita decisión» que afectan a registros con ancla SEARCH (vigencia,
   unidad repartida).
2. Re-etiquetar los 65 REAL con el esquema v2 (los dos etiquetadores, ciegos), partiendo de sus etiquetas actuales
   sólo para `prior`.
3. Escribir y doble-etiquetar el slice sintético.
4. Adjudicar, validar, sellar (dos entradas: REAL y sintético) y fijar la partición.
5. Sólo entonces empieza M2A-3 (`compose`) con IR oráculo.
