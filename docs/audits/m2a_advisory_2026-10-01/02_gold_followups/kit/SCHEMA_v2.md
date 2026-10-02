# Esquema m2a-followup-gold/2 — kit del etiquetador (extracto sin hoja de adjudicación)

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

## 4. Lista cerrada de `gap_codes` para el re-etiquetado

`SET`, `SELECT`, `PV`, `OPSWITCH`, `UNIT`, `COUNTERPART`, `MONTHYEAR`, `VCOMP`, `UNIVTIME`, `GEOBUYER`, `ENADELANTE`,
`TTL`, `TOPIC`, `B5RANGE`, `ACTORTIME`, `FIRMADOS`, `TOTALES`, `YEARRANGE`, `COMPARATOR_FORM`, `REGION_VOCAB`, `SMMLV`,
`UNIVERSE_FRAGMENT`. Un código nuevo exige añadirlo aquí antes de usarlo.
