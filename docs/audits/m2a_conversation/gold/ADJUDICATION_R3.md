# M2A_GOLD r3: adjudicación U3 (enmienda R3, 2026-10-02)

**Autor de la decisión:** el usuario (2026-10-02), transmitida por el orquestador. **Registro:** ARES, paso B4 de la
enmienda R3 (contrato, Anexo D). **Mecanismo:** el mismo que U1/U2/D1 (`ADJUDICATION_2026-10-02.md`). La copia sellada
la genera `scripts/audits/m2a_conversation/build_sealed_gold.py` (`u3_additions`) y el sello comprueba que se reproduce
byte a byte. El candidato asesor no cambia: U3 **añade** cuatro casos y no toca ningún registro existente.

## Procedencia

Las etiquetas se conservan literales: no se editan; la adjudicación se aplica al construir.

| Pieza | Ruta | sha256 |
|---|---|---|
| Etiquetas del etiquetador A (Sonnet, ciego) | `docs/audits/m2a_conversation/labeling_r3/labels_A.jsonl` | `cfc932e8c17b5d34beede7537c3fa408ece31d6a9384eba0ebdaa6ef3c70c68e` |
| Etiquetas del etiquetador B (Opus, ciego) | `docs/audits/m2a_conversation/labeling_r3/labels_B.jsonl` | `025d1b3044808449a46bca8516e605bdffca985458de3027da8b4f2217f68299` |
| Contrato contra el que etiquetaron | `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` en `a90d6ad0` | `6eb5ca00c0307f78…` |
| Entrada que recibieron | `labeling_r3/R3_LABELER_INPUT.md` en `a90d6ad0` | `4766d610cd8d1d92…` |
| Esquema | `docs/audits/m2a_advisory_2026-10-01/02_gold_followups/kit_v2_2/SCHEMA_v2_1.md` | `0e96ada47d513a92…` |

**Acuerdo primario.** En los cuatro casos, A y B coinciden en `turn_kind`, `m2a_scope`, `expected_disposition`,
`disposition_reason`, `resulting_ir`, `effects` y `expected_echo`, por lo que `primary_agreement = true`. Ninguno marcó
ambigüedad (`ambiguous_by = []`). Las confianzas se registran tal cual.

## Decisiones

| Caso | Disposición (A = B) | Diferencia A/B | Adjudicación U3 | Digest del registro sellado |
|---|---|---|---|---|
| SF192 | `CLARIFY DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | `alternatives`: A `[]`, B dos IR (C1 y `SET_ORDER` fecha). Forma de la op `CLEAR(ORDER)` | Se sella **sólo C1** («quitar orden y límite»: el preestado sin ORDER ni CARDINALITY). Los candidatos `SET_ORDER` por criterio alternativo los determina E-ORDER (§19, `DEP-E-ORDER`) y no se sellan. `CLEAR` sin `polarity` ni `value_hint` | `852b7da9da9ce079…` |
| SF193 | `CLARIFY KEEP_ON_EMPTY_SLOT` | `refers_to_result_set` de la op TIME (A `true`, B `false`); `gap_codes` (A `["SET"]`, B `[]`); `value_hint` del KEEP | `refers_to_result_set = true` en «los de 2024» (convención del Gold sellado, matriz T12); `gap_codes = []`; KEEP sin `value_hint` | `68f60fea5dc268b8…` |
| SF194 | `CLARIFY UNRESOLVED_VALUE:TOPIC` | sólo confianza (A MEDIUM, B HIGH) y notas | ninguna; confianzas tal cual | `207f1dc2269e6ef0…` |
| SF195 | `EXECUTE` (CARDINALITY conservada, F10) | orden de `gap_codes` (mismo conjunto); forma de la op `CLEAR(ORDER)` | `gap_codes = ["ORDER_DIRECTION", "SET"]`, el orden canónico del Gold (alfabético: 20 de 33 listas de varios códigos); `CLEAR` sin `polarity` ni `value_hint` | `b767eb83d05e8bac…` |

Cada cambio deja su rastro en las notas de la etiqueta afectada (`| U3 (usuario, 2026-10-02): …`).

## Precisiones del registro (para la revisión)

1. **SF193 `gap_codes`.** La decisión se aplica tal como se dio (`[]`). Pero su motivo declarado («"SET" no es de la
   lista cerrada») no es exacto: `SET` **sí** está en la lista cerrada (`SCHEMA_v2_1.md` §8: «re-consultar o filtrar lo
   mostrado», cerrado por F4), y A y B lo usan en SF192 y SF195. El resultado coincide con la convención dominante del
   Gold sellado: 74 etiquetas con `refers_to_result_set = true` sin `SET`, frente a 2 con `SET`.
2. **Forma de las ops.** «CLEAR/KEEP con `polarity` null y `value_hint` null (forma de B)»:
   - se aplica íntegra a `CLEAR`, que es la forma de B y la del Gold;
   - en `KEEP` se aplica a `value_hint`. Su `polarity` sigue siendo `POSITIVE`, porque A y B coinciden en ella y es la
     convención del Gold (8 de 8 KEEP). Escribirla nula contradiría la «forma de B».
3. **SF195, `polarity` de `SET_ORDER`.** A pone `POSITIVE` y B `null`. No se adjudicó, así que queda tal cual en cada
   etiqueta. La convención del Gold es `POSITIVE` (48 de 48), y ninguna regla del verificador la lee.
4. **SF192 `gap_codes = ["SET"]`.** A y B coinciden; queda tal cual.

## Partición DEV/TEST

Regla fija del sello: estrato = slice + disposición; orden `sha256("m2a-gold-partition/1:" + id)`; posición par →
TEST, impar → DEV; pegajosa. Calculada con los cuatro ids dentro y **0 movimientos** de ids existentes:

| Id | Disposición | Lado |
|---|---|---|
| SF192 | CLARIFY | TEST |
| SF193 | CLARIFY | TEST |
| SF194 | CLARIFY | TEST |
| SF195 | EXECUTE | DEV |

## Totales tras r3

`M2A_GOLD` = 49 REAL + 154 SYNTHETIC = 203. `FOLLOWUP_FRONTIER` = 16 + 41 = 57, sin cambios. Disposiciones del
sintético: EXECUTE 118, CLARIFY 15, NEW_SEARCH 16, REASK 4, INFORM 1.
